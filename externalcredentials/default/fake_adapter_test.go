package externalcredentials

import (
	"context"

	idbfake "github.com/valon-technologies/gestalt-providers/externalcredentials/default/internal/fake"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func wrapFakeIndexedDB(db *idbfake.IndexedDB) indexedDBClient {
	return fakeIndexedDBClient{db: db}
}

type fakeIndexedDBClient struct {
	db *idbfake.IndexedDB
}

func (c fakeIndexedDBClient) CreateObjectStore(ctx context.Context, name string, schema gestalt.ObjectStoreSchema) error {
	return c.db.CreateObjectStore(ctx, name, schema)
}

func (c fakeIndexedDBClient) ObjectStore(name string) objectStore {
	return fakeObjectStore{store: c.db.ObjectStore(name)}
}

func (c fakeIndexedDBClient) Close() error {
	return c.db.Close()
}

type fakeObjectStore struct {
	store *idbfake.ObjectStore
}

func (s fakeObjectStore) Get(ctx context.Context, id string) (gestalt.Record, error) {
	return s.store.Get(ctx, id)
}

func (s fakeObjectStore) Put(ctx context.Context, record gestalt.Record) error {
	return s.store.Put(ctx, record)
}

func (s fakeObjectStore) Add(ctx context.Context, record gestalt.Record) error {
	return s.store.Add(ctx, record)
}

func (s fakeObjectStore) Delete(ctx context.Context, id string) error {
	return s.store.Delete(ctx, id)
}

func (s fakeObjectStore) GetAll(ctx context.Context, r *gestalt.KeyRange) ([]gestalt.Record, error) {
	return s.store.GetAll(ctx, r)
}

func (s fakeObjectStore) Count(ctx context.Context, r *gestalt.KeyRange) (int64, error) {
	return s.store.Count(ctx, r)
}

func (s fakeObjectStore) Index(name string) index {
	return fakeIndex{idx: s.store.Index(name)}
}

type fakeIndex struct {
	idx idbfake.Index
}

func (idx fakeIndex) GetAll(ctx context.Context, r *gestalt.KeyRange, values ...any) ([]gestalt.Record, error) {
	return idx.idx.GetAll(ctx, r, values...)
}

func wrapProviderExternalCredClient(provider *Provider) externalCredClient {
	return externalCredClientAdapter{
		ProviderExternalCredClient: idbfake.NewProviderExternalCredClient(provider),
	}
}

type externalCredClientAdapter struct {
	idbfake.ProviderExternalCredClient
}
