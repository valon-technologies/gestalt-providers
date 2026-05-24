package externalcredentials

import (
	"context"
	"errors"
	"reflect"
	"sync"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

type fakeIndexedDB struct {
	mu     sync.Mutex
	stores map[string]*fakeObjectStore
}

type fakeObjectStore struct {
	records map[string]gestalt.Record
	schema  gestalt.ObjectStoreSchema
}

func newFakeIndexedDB() *fakeIndexedDB {
	return &fakeIndexedDB{stores: make(map[string]*fakeObjectStore)}
}

func (db *fakeIndexedDB) CreateObjectStore(_ context.Context, name string, schema gestalt.ObjectStoreSchema) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, ok := db.stores[name]; ok {
		return gestalt.ErrAlreadyExists
	}
	store := db.storeLocked(name)
	store.schema = schema
	return nil
}

func (db *fakeIndexedDB) ObjectStore(name string) objectStore {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.storeLocked(name)
}

func (db *fakeIndexedDB) Close() error { return nil }

func (db *fakeIndexedDB) storeLocked(name string) *fakeObjectStore {
	if store, ok := db.stores[name]; ok {
		return store
	}
	store := &fakeObjectStore{records: make(map[string]gestalt.Record)}
	db.stores[name] = store
	return store
}

func (s *fakeObjectStore) Get(_ context.Context, id string) (gestalt.Record, error) {
	record, ok := s.records[id]
	if !ok {
		return nil, gestalt.ErrNotFound
	}
	return cloneFakeRecord(record), nil
}

func (s *fakeObjectStore) Put(_ context.Context, record gestalt.Record) error {
	s.records[fakeRecordID(record)] = cloneFakeRecord(record)
	return nil
}

func (s *fakeObjectStore) Add(_ context.Context, record gestalt.Record) error {
	id := fakeRecordID(record)
	if _, ok := s.records[id]; ok {
		return gestalt.ErrAlreadyExists
	}
	s.records[id] = cloneFakeRecord(record)
	return nil
}

func (s *fakeObjectStore) Delete(_ context.Context, id string) error {
	if _, ok := s.records[id]; !ok {
		return gestalt.ErrNotFound
	}
	delete(s.records, id)
	return nil
}

func (s *fakeObjectStore) GetAll(_ context.Context, _ *gestalt.KeyRange) ([]gestalt.Record, error) {
	return cloneFakeRecords(s.records), nil
}

func (s *fakeObjectStore) Count(_ context.Context, _ *gestalt.KeyRange) (int64, error) {
	return int64(len(s.records)), nil
}

func (s *fakeObjectStore) Index(name string) index {
	return fakeIndex{store: s, name: name}
}

type fakeIndex struct {
	store *fakeObjectStore
	name  string
}

func (idx fakeIndex) GetAll(_ context.Context, _ *gestalt.KeyRange, values ...any) ([]gestalt.Record, error) {
	records := make([]gestalt.Record, 0, len(idx.store.records))
	for _, record := range idx.store.records {
		if matchesFakeIndex(record, idx.store.schema, idx.name, values) {
			records = append(records, cloneFakeRecord(record))
		}
	}
	return records, nil
}

func matchesFakeIndex(record gestalt.Record, schema gestalt.ObjectStoreSchema, indexName string, values []any) bool {
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

func cloneFakeRecords(records map[string]gestalt.Record) []gestalt.Record {
	out := make([]gestalt.Record, 0, len(records))
	for _, record := range records {
		out = append(out, cloneFakeRecord(record))
	}
	return out
}

func cloneFakeRecord(record gestalt.Record) gestalt.Record {
	out := make(gestalt.Record, len(record))
	for key, value := range record {
		out[key] = value
	}
	return out
}

func fakeRecordID(record gestalt.Record) string {
	id, _ := record["id"].(string)
	return id
}

func seedExternalCredentialStoreOnConn(ctx context.Context, db indexedDBConn) error {
	if err := db.CreateObjectStore(ctx, storeName, externalCredentialSchema()); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return err
	}
	return nil
}
