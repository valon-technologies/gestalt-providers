package indexeddb

import (
	"context"
	"encoding/json"
	"sort"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

type fakeIndexedDB struct {
	createdStores []string
	closed        bool
	stores        map[string]*fakeObjectStore
}

func (db *fakeIndexedDB) CreateObjectStore(_ context.Context, name string, _ indexeddb.ObjectStoreOptions) (indexeddb.ObjectStore, error) {
	db.createdStores = append(db.createdStores, name)
	return db.objectStore(name), nil
}

func (db *fakeIndexedDB) DeleteObjectStore(context.Context, string) error {
	return indexeddb.ErrUnsupported
}

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
	store := &fakeObjectStore{records: make(map[string]indexeddb.Record)}
	db.stores[name] = store
	return store
}

type fakeObjectStore struct {
	records map[string]indexeddb.Record
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

func (s *fakeObjectStore) Delete(context.Context, string) error {
	return indexeddb.ErrUnsupported
}

func (s *fakeObjectStore) Clear(context.Context) error {
	s.records = make(map[string]indexeddb.Record)
	return nil
}

func (s *fakeObjectStore) GetAll(_ context.Context, r *indexeddb.KeyRange) ([]indexeddb.Record, error) {
	ids := make([]string, 0, len(s.records))
	for id := range s.records {
		if r == nil || r.Includes(id) {
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

func (s *fakeObjectStore) GetAllKeys(context.Context, *indexeddb.KeyRange) ([]string, error) {
	return nil, indexeddb.ErrUnsupported
}

func (s *fakeObjectStore) Count(context.Context, *indexeddb.KeyRange) (int64, error) {
	return 0, indexeddb.ErrUnsupported
}

func (s *fakeObjectStore) DeleteRange(context.Context, indexeddb.KeyRange) (int64, error) {
	return 0, indexeddb.ErrUnsupported
}

func (s *fakeObjectStore) Index(string) indexeddb.Index {
	return nil
}

func (s *fakeObjectStore) OpenCursor(context.Context, *indexeddb.KeyRange, indexeddb.CursorDirection) (indexeddb.Cursor, error) {
	return nil, indexeddb.ErrUnsupported
}

func (s *fakeObjectStore) OpenKeyCursor(context.Context, *indexeddb.KeyRange, indexeddb.CursorDirection) (indexeddb.Cursor, error) {
	return nil, indexeddb.ErrUnsupported
}

func (s *fakeObjectStore) clone() *fakeObjectStore {
	clone := &fakeObjectStore{records: make(map[string]indexeddb.Record, len(s.records))}
	for id, record := range s.records {
		clone.records[id] = cloneFakeRecord(record)
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

func (fakeTransactionObjectStore) Get(context.Context, string) (indexeddb.Record, error) {
	return nil, indexeddb.ErrUnsupported
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

func (fakeTransactionObjectStore) Delete(context.Context, string) error {
	return indexeddb.ErrUnsupported
}

func (s fakeTransactionObjectStore) Clear(ctx context.Context) error {
	return s.store.Clear(ctx)
}

func (fakeTransactionObjectStore) GetAll(context.Context, *indexeddb.KeyRange) ([]indexeddb.Record, error) {
	return nil, indexeddb.ErrUnsupported
}

func (fakeTransactionObjectStore) GetAllKeys(context.Context, *indexeddb.KeyRange) ([]string, error) {
	return nil, indexeddb.ErrUnsupported
}

func (fakeTransactionObjectStore) Count(context.Context, *indexeddb.KeyRange) (int64, error) {
	return 0, indexeddb.ErrUnsupported
}

func (fakeTransactionObjectStore) DeleteRange(context.Context, indexeddb.KeyRange) (int64, error) {
	return 0, indexeddb.ErrUnsupported
}

func (s fakeTransactionObjectStore) Index(string) indexeddb.TransactionIndex {
	return nil
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
