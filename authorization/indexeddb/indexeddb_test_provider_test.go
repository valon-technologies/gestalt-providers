package indexeddb

import (
	"context"
	"reflect"
	"sync"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

type testIndexedDBProvider struct {
	mu            sync.Mutex
	stores        map[string]*testObjectStore
	createdStores []string
}

type testObjectStore struct {
	records map[string]gestalt.Record
	schema  gestalt.ObjectStoreSchema
}

func newTestIndexedDBProvider() *testIndexedDBProvider {
	return &testIndexedDBProvider{stores: make(map[string]*testObjectStore)}
}

func (p *testIndexedDBProvider) Configure(context.Context, string, map[string]any) error {
	return nil
}

func (p *testIndexedDBProvider) CreateObjectStore(_ context.Context, name string, schema gestalt.ObjectStoreSchema) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.stores[name]; ok {
		return gestalt.AlreadyExists("already exists")
	}
	store := p.storeLocked(name)
	store.schema = schema
	p.createdStores = append(p.createdStores, name)
	return nil
}

func (p *testIndexedDBProvider) DeleteObjectStore(_ context.Context, name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.stores, name)
	return nil
}

func (p *testIndexedDBProvider) Get(_ context.Context, req gestalt.IndexedDBObjectStoreRequest) (gestalt.Record, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	record, ok := p.storeLocked(req.Store).records[req.ID]
	if !ok {
		return nil, gestalt.NotFound("not found")
	}
	return cloneTestRecord(record), nil
}

func (p *testIndexedDBProvider) GetKey(_ context.Context, req gestalt.IndexedDBObjectStoreRequest) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.storeLocked(req.Store).records[req.ID]; !ok {
		return "", gestalt.NotFound("not found")
	}
	return req.ID, nil
}

func (p *testIndexedDBProvider) Add(_ context.Context, req gestalt.IndexedDBRecordRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	store := p.storeLocked(req.Store)
	id := recordID(req.Record)
	if _, ok := store.records[id]; ok {
		return gestalt.AlreadyExists("already exists")
	}
	store.records[id] = cloneTestRecord(req.Record)
	return nil
}

func (p *testIndexedDBProvider) Put(_ context.Context, req gestalt.IndexedDBRecordRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.storeLocked(req.Store).records[recordID(req.Record)] = cloneTestRecord(req.Record)
	return nil
}

func (p *testIndexedDBProvider) Delete(_ context.Context, req gestalt.IndexedDBObjectStoreRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	store := p.storeLocked(req.Store)
	if _, ok := store.records[req.ID]; !ok {
		return gestalt.NotFound("not found")
	}
	delete(store.records, req.ID)
	return nil
}

func (p *testIndexedDBProvider) Clear(_ context.Context, store string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.storeLocked(store).records = make(map[string]gestalt.Record)
	return nil
}

func (p *testIndexedDBProvider) GetAll(_ context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]gestalt.Record, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return cloneTestRecords(p.storeLocked(req.Store).records), nil
}

func (p *testIndexedDBProvider) GetAllKeys(_ context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	records := p.storeLocked(req.Store).records
	keys := make([]string, 0, len(records))
	for key := range records {
		keys = append(keys, key)
	}
	return keys, nil
}

func (p *testIndexedDBProvider) Count(_ context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return int64(len(p.storeLocked(req.Store).records)), nil
}

func (p *testIndexedDBProvider) DeleteRange(_ context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	store := p.storeLocked(req.Store)
	deleted := int64(len(store.records))
	store.records = make(map[string]gestalt.Record)
	return deleted, nil
}

func (p *testIndexedDBProvider) IndexGet(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (gestalt.Record, error) {
	records, err := p.IndexGetAll(ctx, req)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, gestalt.NotFound("not found")
	}
	return records[0], nil
}

func (p *testIndexedDBProvider) IndexGetKey(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (string, error) {
	record, err := p.IndexGet(ctx, req)
	if err != nil {
		return "", err
	}
	return recordID(record), nil
}

func (p *testIndexedDBProvider) IndexGetAll(_ context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]gestalt.Record, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	store := p.storeLocked(req.Store)
	records := make([]gestalt.Record, 0, len(store.records))
	for _, record := range store.records {
		if matchesTestIndex(record, store.schema, req.Index, req.Values) {
			records = append(records, cloneTestRecord(record))
		}
	}
	return records, nil
}

func (p *testIndexedDBProvider) IndexGetAllKeys(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]string, error) {
	records, err := p.IndexGetAll(ctx, req)
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(records))
	for i, record := range records {
		keys[i] = recordID(record)
	}
	return keys, nil
}

func (p *testIndexedDBProvider) IndexCount(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (int64, error) {
	records, err := p.IndexGetAll(ctx, req)
	if err != nil {
		return 0, err
	}
	return int64(len(records)), nil
}

func (p *testIndexedDBProvider) IndexDelete(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	store := p.storeLocked(req.Store)
	var deleted int64
	for id, record := range store.records {
		if matchesTestIndex(record, store.schema, req.Index, req.Values) {
			delete(store.records, id)
			deleted++
		}
	}
	return deleted, nil
}

func (p *testIndexedDBProvider) OpenCursor(context.Context, gestalt.IndexedDBOpenCursorRequest) (gestalt.IndexedDBCursor, error) {
	return nil, gestalt.Unimplemented("cursor not implemented")
}

func (p *testIndexedDBProvider) BeginTransaction(context.Context, gestalt.IndexedDBBeginTransactionRequest) (gestalt.IndexedDBTransaction, error) {
	return nil, gestalt.Unimplemented("transaction not implemented")
}

func (p *testIndexedDBProvider) createdStoreNames() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.createdStores...)
}

func (p *testIndexedDBProvider) storeSchema(name string) (gestalt.ObjectStoreSchema, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	store, ok := p.stores[name]
	if !ok {
		return gestalt.ObjectStoreSchema{}, false
	}
	return store.schema, true
}

func (p *testIndexedDBProvider) storeLocked(name string) *testObjectStore {
	if store, ok := p.stores[name]; ok {
		return store
	}
	store := &testObjectStore{records: make(map[string]gestalt.Record)}
	p.stores[name] = store
	return store
}

func matchesTestIndex(record gestalt.Record, schema gestalt.ObjectStoreSchema, indexName string, values []any) bool {
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

func cloneTestRecords(records map[string]gestalt.Record) []gestalt.Record {
	out := make([]gestalt.Record, 0, len(records))
	for _, record := range records {
		out = append(out, cloneTestRecord(record))
	}
	return out
}

func cloneTestRecord(record gestalt.Record) gestalt.Record {
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
