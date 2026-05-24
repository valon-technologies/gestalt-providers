package indexeddb

import (
	"context"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

type indexedDBConn interface {
	CreateObjectStore(ctx context.Context, name string, schema gestalt.ObjectStoreSchema) error
	ObjectStore(name string) objectStore
	Close() error
}

type objectStore interface {
	Get(ctx context.Context, id string) (gestalt.Record, error)
	Put(ctx context.Context, record gestalt.Record) error
	Add(ctx context.Context, record gestalt.Record) error
	Delete(ctx context.Context, id string) error
	GetAll(ctx context.Context, r *gestalt.KeyRange) ([]gestalt.Record, error)
	Count(ctx context.Context, r *gestalt.KeyRange) (int64, error)
	Index(name string) index
}

type index interface {
	GetAll(ctx context.Context, r *gestalt.KeyRange, values ...any) ([]gestalt.Record, error)
}

var connectIndexedDB = func(binding string) (indexedDBConn, error) {
	if binding == "" {
		client, err := gestalt.IndexedDB()
		return sdkDB{client}, err
	}
	client, err := gestalt.IndexedDB(binding)
	return sdkDB{client}, err
}

type sdkDB struct {
	*gestalt.IndexedDBClient
}

func (db sdkDB) ObjectStore(name string) objectStore {
	if db.IndexedDBClient == nil {
		return nil
	}
	return sdkStore{db.IndexedDBClient.ObjectStore(name)}
}

type sdkStore struct {
	*gestalt.ObjectStoreClient
}

func (s sdkStore) Index(name string) index {
	if s.ObjectStoreClient == nil {
		return nil
	}
	return sdkIndex{s.ObjectStoreClient.Index(name)}
}

type sdkIndex struct {
	*gestalt.IndexClient
}
