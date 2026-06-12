package fake

import (
	"context"
	"reflect"
	"sync"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

// IndexedDB is an in-memory indexeddb.Database for external credential provider tests.
type IndexedDB struct {
	mu     sync.Mutex
	stores map[string]*ObjectStore
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

func (db *IndexedDB) Transaction(_ context.Context, _ []string, _ indexeddb.TransactionMode, _ indexeddb.TransactionOptions) (indexeddb.Transaction, error) {
	return nil, indexeddb.ErrUnsupported
}

func (db *IndexedDB) ObjectStore(name string) indexeddb.ObjectStore {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.storeLocked(name)
}

func (db *IndexedDB) Close() error { return nil }

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

// uniqueIndexConflictLocked reports whether another record already claims one
// of the schema's unique index keys, mirroring the relational provider's
// transactional unique-index enforcement.
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
