package dynamodb

import (
	"context"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func (p *Provider) CreateObjectStore(ctx context.Context, name string, schema gestalt.ObjectStoreSchema) error {
	return p.providerCore.CreateObjectStore(ctx, name, schema)
}

func (p *Provider) DeleteObjectStore(ctx context.Context, name string) error {
	return p.providerCore.DeleteObjectStore(ctx, name)
}

func (p *Provider) Get(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) (gestalt.Record, error) {
	return p.providerCore.Get(ctx, req)
}

func (p *Provider) GetKey(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) (string, error) {
	return p.providerCore.GetKey(ctx, req)
}

func (p *Provider) Add(ctx context.Context, req gestalt.IndexedDBRecordRequest) error {
	return p.providerCore.Add(ctx, req)
}

func (p *Provider) Put(ctx context.Context, req gestalt.IndexedDBRecordRequest) error {
	return p.providerCore.Put(ctx, req)
}

func (p *Provider) Delete(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) error {
	return p.providerCore.Delete(ctx, req)
}

func (p *Provider) Clear(ctx context.Context, store string) error {
	return p.providerCore.Clear(ctx, store)
}

func (p *Provider) GetAll(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]gestalt.Record, error) {
	return p.providerCore.GetAll(ctx, req)
}

func (p *Provider) GetAllKeys(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]string, error) {
	return p.providerCore.GetAllKeys(ctx, req)
}

func (p *Provider) Count(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) (int64, error) {
	return p.providerCore.Count(ctx, req)
}

func (p *Provider) DeleteRange(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) (int64, error) {
	return p.providerCore.DeleteRange(ctx, req)
}

func (p *Provider) IndexGet(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (gestalt.Record, error) {
	return p.providerCore.IndexGet(ctx, req)
}

func (p *Provider) IndexGetKey(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (string, error) {
	return p.providerCore.IndexGetKey(ctx, req)
}

func (p *Provider) IndexGetAll(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]gestalt.Record, error) {
	return p.providerCore.IndexGetAll(ctx, req)
}

func (p *Provider) IndexGetAllKeys(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]string, error) {
	return p.providerCore.IndexGetAllKeys(ctx, req)
}

func (p *Provider) IndexCount(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (int64, error) {
	return p.providerCore.IndexCount(ctx, req)
}

func (p *Provider) IndexDelete(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (int64, error) {
	return p.providerCore.IndexDelete(ctx, req)
}

func (p *Provider) OpenCursor(ctx context.Context, req gestalt.IndexedDBOpenCursorRequest) (gestalt.IndexedDBCursor, error) {
	return p.providerCore.OpenCursor(ctx, req)
}

func (p *Provider) BeginTransaction(ctx context.Context, req gestalt.IndexedDBBeginTransactionRequest) (gestalt.IndexedDBTransaction, error) {
	return p.providerCore.BeginTransaction(ctx, req)
}
