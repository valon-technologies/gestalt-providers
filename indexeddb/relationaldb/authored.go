package relationaldb

import (
	"context"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func (p *Provider) CreateObjectStore(ctx context.Context, name string, schema gestalt.ObjectStoreOptions) error {
	return p.Store.CreateObjectStore(ctx, name, schema)
}

func (p *Provider) DeleteObjectStore(ctx context.Context, name string) error {
	return p.Store.DeleteObjectStore(ctx, name)
}

func (p *Provider) Get(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) (gestalt.Record, error) {
	return p.Store.Get(ctx, req)
}

func (p *Provider) GetKey(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) (string, error) {
	return p.Store.GetKey(ctx, req)
}

func (p *Provider) Add(ctx context.Context, req gestalt.IndexedDBRecordRequest) error {
	return p.Store.Add(ctx, req)
}

func (p *Provider) Put(ctx context.Context, req gestalt.IndexedDBRecordRequest) error {
	return p.Store.Put(ctx, req)
}

func (p *Provider) Delete(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) error {
	return p.Store.Delete(ctx, req)
}

func (p *Provider) Clear(ctx context.Context, store string) error {
	return p.Store.Clear(ctx, store)
}

func (p *Provider) GetAll(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]gestalt.Record, error) {
	return p.Store.GetAll(ctx, req)
}

func (p *Provider) GetAllKeys(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]string, error) {
	return p.Store.GetAllKeys(ctx, req)
}

func (p *Provider) Count(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) (int64, error) {
	return p.Store.Count(ctx, req)
}

func (p *Provider) DeleteRange(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) (int64, error) {
	return p.Store.DeleteRange(ctx, req)
}

func (p *Provider) IndexGet(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (gestalt.Record, error) {
	return p.Store.IndexGet(ctx, req)
}

func (p *Provider) IndexGetKey(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (string, error) {
	return p.Store.IndexGetKey(ctx, req)
}

func (p *Provider) IndexGetAll(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]gestalt.Record, error) {
	return p.Store.IndexGetAll(ctx, req)
}

func (p *Provider) IndexGetAllKeys(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]string, error) {
	return p.Store.IndexGetAllKeys(ctx, req)
}

func (p *Provider) IndexCount(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (int64, error) {
	return p.Store.IndexCount(ctx, req)
}

func (p *Provider) IndexDelete(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (int64, error) {
	return p.Store.IndexDelete(ctx, req)
}

func (p *Provider) OpenCursor(ctx context.Context, req gestalt.IndexedDBOpenCursorRequest) (gestalt.IndexedDBCursor, error) {
	return p.Store.OpenCursor(ctx, req)
}

func (p *Provider) BeginTransaction(ctx context.Context, req gestalt.IndexedDBBeginTransactionRequest) (gestalt.IndexedDBTransaction, error) {
	return p.Store.BeginTransaction(ctx, req)
}
