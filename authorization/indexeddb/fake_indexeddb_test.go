package indexeddb

import (
	"context"
	"encoding/json"

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

func (db *fakeIndexedDB) Transaction(context.Context, []string, indexeddb.TransactionMode, indexeddb.TransactionOptions) (indexeddb.Transaction, error) {
	return nil, indexeddb.ErrUnsupported
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
	return indexeddb.ErrUnsupported
}

func (s *fakeObjectStore) GetAll(context.Context, *indexeddb.KeyRange) ([]indexeddb.Record, error) {
	return nil, indexeddb.ErrUnsupported
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
