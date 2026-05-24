package fake

import (
	"context"
	"reflect"
	"sync"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

type IndexedDB struct {
	mu     sync.Mutex
	stores map[string]*ObjectStore
}

type ObjectStore struct {
	records map[string]gestalt.Record
	schema  gestalt.ObjectStoreSchema
}

func New() *IndexedDB {
	return &IndexedDB{stores: make(map[string]*ObjectStore)}
}

func (db *IndexedDB) CreateObjectStore(_ context.Context, name string, schema gestalt.ObjectStoreSchema) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, ok := db.stores[name]; ok {
		return gestalt.ErrAlreadyExists
	}
	store := db.storeLocked(name)
	store.schema = schema
	return nil
}

func (db *IndexedDB) ObjectStore(name string) *ObjectStore {
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
	record, ok := s.records[id]
	if !ok {
		return nil, gestalt.ErrNotFound
	}
	return cloneRecord(record), nil
}

func (s *ObjectStore) Put(_ context.Context, record gestalt.Record) error {
	s.records[recordID(record)] = cloneRecord(record)
	return nil
}

func (s *ObjectStore) Add(_ context.Context, record gestalt.Record) error {
	id := recordID(record)
	if _, ok := s.records[id]; ok {
		return gestalt.ErrAlreadyExists
	}
	s.records[id] = cloneRecord(record)
	return nil
}

func (s *ObjectStore) Delete(_ context.Context, id string) error {
	if _, ok := s.records[id]; !ok {
		return gestalt.ErrNotFound
	}
	delete(s.records, id)
	return nil
}

func (s *ObjectStore) GetAll(_ context.Context, _ *gestalt.KeyRange) ([]gestalt.Record, error) {
	return cloneRecords(s.records), nil
}

func (s *ObjectStore) Count(_ context.Context, _ *gestalt.KeyRange) (int64, error) {
	return int64(len(s.records)), nil
}

func (s *ObjectStore) Index(name string) Index {
	return Index{store: s, name: name}
}

type Index struct {
	store *ObjectStore
	name  string
}

func (idx Index) GetAll(_ context.Context, _ *gestalt.KeyRange, values ...any) ([]gestalt.Record, error) {
	records := make([]gestalt.Record, 0, len(idx.store.records))
	for _, record := range idx.store.records {
		if matchesIndex(record, idx.store.schema, idx.name, values) {
			records = append(records, cloneRecord(record))
		}
	}
	return records, nil
}

func matchesIndex(record gestalt.Record, schema gestalt.ObjectStoreSchema, indexName string, values []any) bool {
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
