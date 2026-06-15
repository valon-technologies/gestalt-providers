package fake

import (
	"context"
	"reflect"
	"sync"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

// IndexedDB is an in-memory indexeddb.Database for OIDC provider tests.
type IndexedDB struct {
	mu     sync.Mutex
	stores map[string]*ObjectStore

	TransactionAddHook    func(storeName string, record gestalt.Record) error
	TransactionCommitHook func() error
	CloseHook             func() error
	closed                bool
}

// ObjectStore holds in-memory records for one object store.
type ObjectStore struct {
	mu      sync.Mutex
	records map[string]gestalt.Record
	schema  gestalt.ObjectStoreOptions
}

func NewIndexedDB() *IndexedDB {
	return &IndexedDB{stores: make(map[string]*ObjectStore)}
}

func (db *IndexedDB) CreateObjectStore(_ context.Context, name string, schema gestalt.ObjectStoreOptions) (indexeddb.ObjectStore, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, ok := db.stores[name]; ok {
		return nil, gestalt.ErrAlreadyExists
	}
	store := db.storeLocked(name)
	store.schema = schema
	return store, nil
}

func (db *IndexedDB) DeleteObjectStore(_ context.Context, name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.stores, name)
	return nil
}

func (db *IndexedDB) Transaction(_ context.Context, stores []string, mode indexeddb.TransactionMode, _ indexeddb.TransactionOptions) (indexeddb.Transaction, error) {
	if mode != indexeddb.TransactionReadwrite {
		return nil, indexeddb.ErrUnsupported
	}
	return &transaction{
		db:     db,
		stores: append([]string(nil), stores...),
		adds:   make(map[string]map[string]gestalt.Record),
	}, nil
}

func (db *IndexedDB) ObjectStore(name string) indexeddb.ObjectStore {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.storeLocked(name)
}

func (db *IndexedDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.closed = true
	if db.CloseHook != nil {
		return db.CloseHook()
	}
	return nil
}

func (db *IndexedDB) storeLocked(name string) *ObjectStore {
	if store, ok := db.stores[name]; ok {
		return store
	}
	store := &ObjectStore{records: make(map[string]gestalt.Record)}
	db.stores[name] = store
	return store
}

func (s *ObjectStore) Get(_ context.Context, id string) (gestalt.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[id]
	if !ok {
		return nil, gestalt.ErrNotFound
	}
	return cloneRecord(record), nil
}

func (s *ObjectStore) GetKey(_ context.Context, id string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[id]; !ok {
		return "", gestalt.ErrNotFound
	}
	return id, nil
}

func (s *ObjectStore) Put(_ context.Context, record gestalt.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if conflict := s.uniqueIndexConflictLocked(record); conflict {
		return gestalt.ErrAlreadyExists
	}
	s.records[recordID(record)] = cloneRecord(record)
	return nil
}

func (s *ObjectStore) Add(_ context.Context, record gestalt.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := recordID(record)
	if _, ok := s.records[id]; ok {
		return gestalt.ErrAlreadyExists
	}
	if conflict := s.uniqueIndexConflictLocked(record); conflict {
		return gestalt.ErrAlreadyExists
	}
	s.records[id] = cloneRecord(record)
	return nil
}

func (s *ObjectStore) uniqueIndexConflictLocked(record gestalt.Record) bool {
	id := recordID(record)
	for _, index := range s.schema.Indexes {
		if !index.Unique {
			continue
		}
		values := make([]any, 0, len(index.KeyPath))
		for _, field := range index.KeyPath {
			values = append(values, record[field])
		}
		for existingID, existing := range s.records {
			if existingID == id {
				continue
			}
			if matchesIndex(existing, s.schema, index.Name, values) {
				return true
			}
		}
	}
	return false
}

func (s *ObjectStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[id]; !ok {
		return gestalt.ErrNotFound
	}
	delete(s.records, id)
	return nil
}

func (s *ObjectStore) Clear(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = make(map[string]gestalt.Record)
	return nil
}

func (s *ObjectStore) GetAll(_ context.Context, _ *gestalt.KeyRange) ([]gestalt.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneRecords(s.records), nil
}

func (s *ObjectStore) GetAllKeys(_ context.Context, _ *gestalt.KeyRange) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.records))
	for id := range s.records {
		keys = append(keys, id)
	}
	return keys, nil
}

func (s *ObjectStore) Count(_ context.Context, _ *gestalt.KeyRange) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int64(len(s.records)), nil
}

func (s *ObjectStore) DeleteRange(_ context.Context, _ gestalt.KeyRange) (int64, error) {
	return 0, indexeddb.ErrUnsupported
}

func (s *ObjectStore) Index(name string) indexeddb.Index {
	return Index{store: s, name: name}
}

func (s *ObjectStore) OpenCursor(_ context.Context, _ *gestalt.KeyRange, _ gestalt.CursorDirection) (indexeddb.Cursor, error) {
	return nil, indexeddb.ErrUnsupported
}

func (s *ObjectStore) OpenKeyCursor(_ context.Context, _ *gestalt.KeyRange, _ gestalt.CursorDirection) (indexeddb.Cursor, error) {
	return nil, indexeddb.ErrUnsupported
}

type Index struct {
	store *ObjectStore
	name  string
}

func (idx Index) Get(_ context.Context, values ...any) (gestalt.Record, error) {
	idx.store.mu.Lock()
	defer idx.store.mu.Unlock()
	for _, record := range idx.store.records {
		if matchesIndex(record, idx.store.schema, idx.name, values) {
			return cloneRecord(record), nil
		}
	}
	return nil, gestalt.ErrNotFound
}

func (idx Index) GetKey(_ context.Context, _ ...any) (string, error) {
	return "", indexeddb.ErrUnsupported
}

func (idx Index) GetAll(_ context.Context, _ *gestalt.KeyRange, values ...any) ([]gestalt.Record, error) {
	idx.store.mu.Lock()
	defer idx.store.mu.Unlock()
	records := make([]gestalt.Record, 0, len(idx.store.records))
	for _, record := range idx.store.records {
		if matchesIndex(record, idx.store.schema, idx.name, values) {
			records = append(records, cloneRecord(record))
		}
	}
	return records, nil
}

func (idx Index) GetAllKeys(_ context.Context, _ *gestalt.KeyRange, _ ...any) ([]string, error) {
	return nil, indexeddb.ErrUnsupported
}

func (idx Index) Count(_ context.Context, _ *gestalt.KeyRange, _ ...any) (int64, error) {
	return 0, indexeddb.ErrUnsupported
}

func (idx Index) Delete(_ context.Context, _ ...any) (int64, error) {
	return 0, indexeddb.ErrUnsupported
}

func (idx Index) DeleteRange(_ context.Context, _ *gestalt.KeyRange, _ ...any) (int64, error) {
	return 0, indexeddb.ErrUnsupported
}

func (idx Index) OpenCursor(_ context.Context, _ *gestalt.KeyRange, _ gestalt.CursorDirection, _ ...any) (indexeddb.Cursor, error) {
	return nil, indexeddb.ErrUnsupported
}

func (idx Index) OpenKeyCursor(_ context.Context, _ *gestalt.KeyRange, _ gestalt.CursorDirection, _ ...any) (indexeddb.Cursor, error) {
	return nil, indexeddb.ErrUnsupported
}

func matchesIndex(record gestalt.Record, schema gestalt.ObjectStoreOptions, indexName string, values []any) bool {
	for _, index := range schema.Indexes {
		if index.Name != indexName {
			continue
		}
		for i, field := range index.KeyPath {
			if i >= len(values) {
				return false
			}
			if !reflect.DeepEqual(record[field], values[i]) {
				return false
			}
		}
		return true
	}
	return false
}

func cloneRecords(records map[string]gestalt.Record) []gestalt.Record {
	out := make([]gestalt.Record, 0, len(records))
	for _, record := range records {
		out = append(out, cloneRecord(record))
	}
	return out
}

func cloneRecord(record gestalt.Record) gestalt.Record {
	out := make(gestalt.Record, len(record))
	for key, value := range record {
		out[key] = value
	}
	return out
}

func recordID(record gestalt.Record) string {
	id, _ := record["id"].(string)
	return id
}

type transaction struct {
	db      *IndexedDB
	stores  []string
	adds    map[string]map[string]gestalt.Record
	done    bool
	aborted bool
}

type txObjectStore struct {
	tx   *transaction
	name string
}

type txIndex struct {
	store *txObjectStore
	name  string
}

func (tx *transaction) ObjectStore(name string) indexeddb.TransactionObjectStore {
	return &txObjectStore{tx: tx, name: name}
}

func (tx *transaction) Commit(_ context.Context) error {
	if tx.done {
		return indexeddb.ErrTransactionDone
	}
	if tx.aborted {
		return indexeddb.ErrTransactionDone
	}
	if tx.db.TransactionCommitHook != nil {
		if err := tx.db.TransactionCommitHook(); err != nil {
			tx.done = true
			tx.aborted = true
			return err
		}
	}
	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()
	for storeName, records := range tx.adds {
		store := tx.db.storeLocked(storeName)
		for id, record := range records {
			if err := store.addLocked(record); err != nil {
				tx.done = true
				tx.aborted = true
				return err
			}
			_ = id
		}
	}
	tx.done = true
	return nil
}

func (tx *transaction) Abort(_ context.Context) error {
	if tx.done {
		return nil
	}
	tx.aborted = true
	tx.done = true
	return nil
}

func (s *txObjectStore) Get(ctx context.Context, id string) (gestalt.Record, error) {
	return s.tx.db.ObjectStore(s.name).Get(ctx, id)
}

func (s *txObjectStore) GetKey(ctx context.Context, id string) (string, error) {
	return s.tx.db.ObjectStore(s.name).GetKey(ctx, id)
}

func (s *txObjectStore) Add(_ context.Context, record gestalt.Record) error {
	if s.tx.done {
		return indexeddb.ErrTransactionDone
	}
	if s.tx.db.TransactionAddHook != nil {
		if err := s.tx.db.TransactionAddHook(s.name, record); err != nil {
			return err
		}
	}
	id := recordID(record)
	if s.tx.adds[s.name] == nil {
		s.tx.adds[s.name] = make(map[string]gestalt.Record)
	}
	store := s.tx.db.storeLocked(s.name)
	store.mu.Lock()
	_, exists := store.records[id]
	store.mu.Unlock()
	if exists {
		return gestalt.ErrAlreadyExists
	}
	if _, ok := s.tx.adds[s.name][id]; ok {
		return gestalt.ErrAlreadyExists
	}
	s.tx.adds[s.name][id] = cloneRecord(record)
	return nil
}

func (s *txObjectStore) Put(ctx context.Context, record gestalt.Record) error {
	return s.tx.db.ObjectStore(s.name).Put(ctx, record)
}

func (s *txObjectStore) Delete(ctx context.Context, id string) error {
	return s.tx.db.ObjectStore(s.name).Delete(ctx, id)
}

func (s *txObjectStore) Clear(ctx context.Context) error {
	return s.tx.db.ObjectStore(s.name).Clear(ctx)
}

func (s *txObjectStore) GetAll(ctx context.Context, r *gestalt.KeyRange) ([]gestalt.Record, error) {
	return s.tx.db.ObjectStore(s.name).GetAll(ctx, r)
}

func (s *txObjectStore) GetAllKeys(ctx context.Context, r *gestalt.KeyRange) ([]string, error) {
	return s.tx.db.ObjectStore(s.name).GetAllKeys(ctx, r)
}

func (s *txObjectStore) Count(ctx context.Context, r *gestalt.KeyRange) (int64, error) {
	return s.tx.db.ObjectStore(s.name).Count(ctx, r)
}

func (s *txObjectStore) DeleteRange(ctx context.Context, r gestalt.KeyRange) (int64, error) {
	return s.tx.db.ObjectStore(s.name).DeleteRange(ctx, r)
}

func (s *txObjectStore) Index(name string) indexeddb.TransactionIndex {
	return &txIndex{store: s, name: name}
}

func (idx txIndex) Get(ctx context.Context, values ...any) (gestalt.Record, error) {
	return idx.store.tx.db.ObjectStore(idx.store.name).Index(idx.name).Get(ctx, values...)
}

func (idx txIndex) GetKey(ctx context.Context, values ...any) (string, error) {
	return idx.store.tx.db.ObjectStore(idx.store.name).Index(idx.name).GetKey(ctx, values...)
}

func (idx txIndex) GetAll(ctx context.Context, r *gestalt.KeyRange, values ...any) ([]gestalt.Record, error) {
	return idx.store.tx.db.ObjectStore(idx.store.name).Index(idx.name).GetAll(ctx, r, values...)
}

func (idx txIndex) GetAllKeys(ctx context.Context, r *gestalt.KeyRange, values ...any) ([]string, error) {
	return idx.store.tx.db.ObjectStore(idx.store.name).Index(idx.name).GetAllKeys(ctx, r, values...)
}

func (idx txIndex) Count(ctx context.Context, r *gestalt.KeyRange, values ...any) (int64, error) {
	return idx.store.tx.db.ObjectStore(idx.store.name).Index(idx.name).Count(ctx, r, values...)
}

func (idx txIndex) Delete(ctx context.Context, values ...any) (int64, error) {
	return idx.store.tx.db.ObjectStore(idx.store.name).Index(idx.name).Delete(ctx, values...)
}

func (idx txIndex) DeleteRange(ctx context.Context, r *gestalt.KeyRange, values ...any) (int64, error) {
	return idx.store.tx.db.ObjectStore(idx.store.name).Index(idx.name).DeleteRange(ctx, r, values...)
}

func (s *ObjectStore) addLocked(record gestalt.Record) error {
	id := recordID(record)
	if _, ok := s.records[id]; ok {
		return gestalt.ErrAlreadyExists
	}
	if conflict := s.uniqueIndexConflictLocked(record); conflict {
		return gestalt.ErrAlreadyExists
	}
	s.records[id] = cloneRecord(record)
	return nil
}
