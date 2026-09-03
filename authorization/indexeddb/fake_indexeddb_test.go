package indexeddb

import (
	"context"
	"encoding/json"
	"sort"
	"strings"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

type fakeIndexedDB struct {
	createdStores  []string
	createdIndexes []indexeddb.IndexDefinition
	commitErr      error
	closed         bool
	stores         map[string]*fakeObjectStore
	nilGetAllCalls int
}

func (db *fakeIndexedDB) CreateObjectStore(_ context.Context, name string, _ indexeddb.ObjectStoreOptions) (indexeddb.ObjectStore, error) {
	db.createdStores = append(db.createdStores, name)
	return db.objectStore(name), nil
}

func (db *fakeIndexedDB) DeleteObjectStore(context.Context, string) error {
	return indexeddb.ErrUnsupported
}

func (db *fakeIndexedDB) CreateIndex(_ context.Context, storeName string, definition indexeddb.IndexDefinition) error {
	store := db.objectStore(storeName).(*fakeObjectStore)
	if _, exists := store.indexes[definition.Name]; exists {
		return indexeddb.ErrAlreadyExists
	}
	store.indexes[definition.Name] = definition
	db.createdIndexes = append(db.createdIndexes, definition)
	return nil
}

func (db *fakeIndexedDB) DeleteIndex(_ context.Context, storeName, name string) error {
	delete(db.objectStore(storeName).(*fakeObjectStore).indexes, name)
	return nil
}

var _ indexeddb.IndexManager = (*fakeIndexedDB)(nil)

func (db *fakeIndexedDB) Transaction(_ context.Context, stores []string, mode indexeddb.TransactionMode, _ indexeddb.TransactionOptions) (indexeddb.Transaction, error) {
	if mode != indexeddb.TransactionReadwrite {
		return nil, indexeddb.ErrUnsupported
	}
	txStores := make(map[string]*fakeObjectStore, len(stores))
	for _, name := range stores {
		txStores[name] = db.objectStore(name).(*fakeObjectStore).clone()
	}
	return &fakeTransaction{db: db, stores: txStores}, nil
}

func (db *fakeIndexedDB) ObjectStore(name string) indexeddb.ObjectStore {
	return db.objectStore(name)
}

func (db *fakeIndexedDB) Close() error {
	db.closed = true
	return nil
}

func (db *fakeIndexedDB) objectStore(name string) indexeddb.ObjectStore {
	if db.stores == nil {
		db.stores = make(map[string]*fakeObjectStore)
	}
	if store, ok := db.stores[name]; ok {
		return store
	}
	store := &fakeObjectStore{records: make(map[string]indexeddb.Record), indexes: make(map[string]indexeddb.IndexDefinition), nilGetAllCalls: &db.nilGetAllCalls}
	db.stores[name] = store
	return store
}

type fakeObjectStore struct {
	records        map[string]indexeddb.Record
	indexes        map[string]indexeddb.IndexDefinition
	nilGetAllCalls *int
}

func (s *fakeObjectStore) Add(context.Context, indexeddb.Record) error {
	return indexeddb.ErrUnsupported
}

func (s *fakeObjectStore) Put(_ context.Context, record indexeddb.Record) error {
	s.records[fakeRecordID(record)] = cloneFakeRecord(record)
	return nil
}

func (s *fakeObjectStore) Get(_ context.Context, id string) (indexeddb.Record, error) {
	record, ok := s.records[id]
	if !ok {
		return nil, gestalt.ErrNotFound
	}
	return cloneFakeRecord(record), nil
}

func (s *fakeObjectStore) GetKey(_ context.Context, id string) (string, error) {
	return "", indexeddb.ErrUnsupported
}

func (s *fakeObjectStore) Delete(_ context.Context, id string) error {
	delete(s.records, id)
	return nil
}

func (s *fakeObjectStore) Clear(context.Context) error {
	s.records = make(map[string]indexeddb.Record)
	return nil
}

func (s *fakeObjectStore) GetAll(_ context.Context, query any, _ ...uint32) ([]indexeddb.Record, error) {
	if query == nil && s.nilGetAllCalls != nil {
		(*s.nilGetAllCalls)++
	}
	ids := make([]string, 0, len(s.records))
	for id := range s.records {
		ok, err := indexeddb.MatchQuery(id, indexeddb.ToQuery(query))
		if err != nil {
			return nil, err
		}
		if ok {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	records := make([]indexeddb.Record, 0, len(ids))
	for _, id := range ids {
		records = append(records, cloneFakeRecord(s.records[id]))
	}
	return records, nil
}

func (s *fakeObjectStore) GetAllKeys(context.Context, any, ...uint32) ([]string, error) {
	ids := make([]string, 0, len(s.records))
	for id := range s.records {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

func (s *fakeObjectStore) Count(context.Context, any) (int64, error) {
	return 0, indexeddb.ErrUnsupported
}

func (s *fakeObjectStore) DeleteRange(context.Context, any) (int64, error) {
	return 0, indexeddb.ErrUnsupported
}

func (s *fakeObjectStore) Index(name string) indexeddb.Index {
	if _, ok := s.indexes[name]; !ok {
		return nil
	}
	return &fakeIndex{store: s, name: name}
}

func (s *fakeObjectStore) OpenCursor(context.Context, any, indexeddb.CursorDirection) (indexeddb.Cursor, error) {
	return nil, indexeddb.ErrUnsupported
}

func (s *fakeObjectStore) OpenKeyCursor(context.Context, any, indexeddb.CursorDirection) (indexeddb.Cursor, error) {
	return nil, indexeddb.ErrUnsupported
}

func (s *fakeObjectStore) clone() *fakeObjectStore {
	clone := &fakeObjectStore{records: make(map[string]indexeddb.Record, len(s.records)), indexes: make(map[string]indexeddb.IndexDefinition, len(s.indexes)), nilGetAllCalls: s.nilGetAllCalls}
	for id, record := range s.records {
		clone.records[id] = cloneFakeRecord(record)
	}
	for name, definition := range s.indexes {
		clone.indexes[name] = definition
	}
	return clone
}

type fakeTransaction struct {
	db     *fakeIndexedDB
	stores map[string]*fakeObjectStore
	done   bool
}

func (tx *fakeTransaction) ObjectStore(name string) indexeddb.TransactionObjectStore {
	return fakeTransactionObjectStore{store: tx.stores[name]}
}

func (tx *fakeTransaction) Commit(context.Context) error {
	if tx.done {
		return indexeddb.ErrTransactionDone
	}
	if tx.db.commitErr != nil {
		return tx.db.commitErr
	}
	if tx.db.stores == nil {
		tx.db.stores = make(map[string]*fakeObjectStore)
	}
	for name, store := range tx.stores {
		tx.db.stores[name] = store.clone()
	}
	tx.done = true
	return nil
}

func (tx *fakeTransaction) Abort(context.Context) error {
	if tx.done {
		return indexeddb.ErrTransactionDone
	}
	tx.done = true
	return nil
}

type fakeTransactionObjectStore struct {
	store *fakeObjectStore
}

func (s fakeTransactionObjectStore) Get(ctx context.Context, id string) (indexeddb.Record, error) {
	return s.store.Get(ctx, id)
}

func (fakeTransactionObjectStore) GetKey(context.Context, string) (string, error) {
	return "", indexeddb.ErrUnsupported
}

func (fakeTransactionObjectStore) Add(context.Context, indexeddb.Record) error {
	return indexeddb.ErrUnsupported
}

func (s fakeTransactionObjectStore) Put(ctx context.Context, record indexeddb.Record) error {
	return s.store.Put(ctx, record)
}

func (s fakeTransactionObjectStore) Delete(ctx context.Context, id string) error {
	return s.store.Delete(ctx, id)
}

func (s fakeTransactionObjectStore) Clear(ctx context.Context) error {
	return s.store.Clear(ctx)
}

func (s fakeTransactionObjectStore) GetAll(ctx context.Context, query any, count ...uint32) ([]indexeddb.Record, error) {
	return s.store.GetAll(ctx, query, count...)
}

func (s fakeTransactionObjectStore) GetAllKeys(ctx context.Context, query any, count ...uint32) ([]string, error) {
	return s.store.GetAllKeys(ctx, query, count...)
}

func (fakeTransactionObjectStore) Count(context.Context, any) (int64, error) {
	return 0, indexeddb.ErrUnsupported
}

func (fakeTransactionObjectStore) DeleteRange(context.Context, any) (int64, error) {
	return 0, indexeddb.ErrUnsupported
}

func (s fakeTransactionObjectStore) Index(name string) indexeddb.TransactionIndex {
	if _, ok := s.store.indexes[name]; !ok {
		return nil
	}
	return &fakeIndex{store: s.store, name: name}
}

type fakeIndex struct {
	store *fakeObjectStore
	name  string
}

func (*fakeIndex) Get(context.Context, any) (indexeddb.Record, error) {
	return nil, indexeddb.ErrUnsupported
}

func (*fakeIndex) GetKey(context.Context, any) (string, error) {
	return "", indexeddb.ErrUnsupported
}

func (i *fakeIndex) GetAll(_ context.Context, query any, _ ...uint32) ([]indexeddb.Record, error) {
	definition := i.store.indexes[i.name]
	nativeQuery := indexeddb.ToQuery(query)
	ids := make([]string, 0, len(i.store.records))
	for id, record := range i.store.records {
		key, ok := fakeIndexKey(record, definition.KeyPath)
		if !ok {
			continue
		}
		matched, err := indexeddb.MatchQuery(key, nativeQuery)
		if err != nil {
			return nil, err
		}
		if matched {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	result := make([]indexeddb.Record, 0, len(ids))
	for _, id := range ids {
		result = append(result, cloneFakeRecord(i.store.records[id]))
	}
	return result, nil
}

func (*fakeIndex) GetAllKeys(context.Context, any, ...uint32) ([]string, error) {
	return nil, indexeddb.ErrUnsupported
}

func (*fakeIndex) Count(context.Context, any) (int64, error) {
	return 0, indexeddb.ErrUnsupported
}

func (*fakeIndex) Delete(context.Context, any) (int64, error) {
	return 0, indexeddb.ErrUnsupported
}

func (*fakeIndex) OpenCursor(context.Context, any, indexeddb.CursorDirection) (indexeddb.Cursor, error) {
	return nil, indexeddb.ErrUnsupported
}

func (*fakeIndex) OpenKeyCursor(context.Context, any, indexeddb.CursorDirection) (indexeddb.Cursor, error) {
	return nil, indexeddb.ErrUnsupported
}

func fakeIndexKey(record indexeddb.Record, path []string) ([]any, bool) {
	key := make([]any, 0, len(path))
	for _, fieldPath := range path {
		var value any = record
		for _, part := range strings.Split(fieldPath, ".") {
			values, ok := value.(map[string]any)
			if !ok {
				return nil, false
			}
			value, ok = values[part]
			if !ok {
				return nil, false
			}
		}
		key = append(key, value)
	}
	return key, true
}

func fakeRecordID(record indexeddb.Record) string {
	id, _ := record["id"].(string)
	return id
}

func cloneFakeRecord(record indexeddb.Record) indexeddb.Record {
	data, err := json.Marshal(record)
	if err != nil {
		panic(err)
	}
	var out indexeddb.Record
	if err := json.Unmarshal(data, &out); err != nil {
		panic(err)
	}
	return out
}
