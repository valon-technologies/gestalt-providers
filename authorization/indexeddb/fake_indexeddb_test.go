package indexeddb

import (
	"context"

	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

type fakeIndexedDB struct {
	createdStores []string
	closed        bool
}

func (db *fakeIndexedDB) CreateObjectStore(_ context.Context, name string, _ indexeddb.ObjectStoreOptions) (indexeddb.ObjectStore, error) {
	db.createdStores = append(db.createdStores, name)
	return nil, nil
}

func (db *fakeIndexedDB) DeleteObjectStore(context.Context, string) error {
	return nil
}

func (db *fakeIndexedDB) Transaction(context.Context, []string, indexeddb.TransactionMode, indexeddb.TransactionOptions) (indexeddb.Transaction, error) {
	return nil, nil
}

func (db *fakeIndexedDB) ObjectStore(string) indexeddb.ObjectStore {
	return nil
}

func (db *fakeIndexedDB) Close() error {
	db.closed = true
	return nil
}
