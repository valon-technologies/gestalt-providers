package relationaldb

import (
	"context"

	"github.com/valon-technologies/gestalt-providers/indexeddb/internal/sdkcompat"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func (p *Provider) CreateObjectStore(ctx context.Context, name string, schema gestalt.ObjectStoreSchema) error {
	_, err := p.Store.CreateObjectStore(ctx, sdkcompat.CreateObjectStoreRequest(name, schema))
	return err
}

func (p *Provider) DeleteObjectStore(ctx context.Context, name string) error {
	_, err := p.Store.DeleteObjectStore(ctx, sdkcompat.DeleteObjectStoreRequest(name))
	return err
}

func (p *Provider) Get(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) (gestalt.Record, error) {
	resp, err := p.Store.Get(ctx, sdkcompat.ObjectStoreRequest(req))
	if err != nil {
		return nil, err
	}
	return sdkcompat.RecordFromProto(resp.GetRecord())
}

func (p *Provider) GetKey(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) (string, error) {
	resp, err := p.Store.GetKey(ctx, sdkcompat.ObjectStoreRequest(req))
	if err != nil {
		return "", err
	}
	return resp.GetKey(), nil
}

func (p *Provider) Add(ctx context.Context, req gestalt.IndexedDBRecordRequest) error {
	pbReq, err := sdkcompat.RecordRequest(req)
	if err != nil {
		return err
	}
	_, err = p.Store.Add(ctx, pbReq)
	return err
}

func (p *Provider) Put(ctx context.Context, req gestalt.IndexedDBRecordRequest) error {
	pbReq, err := sdkcompat.RecordRequest(req)
	if err != nil {
		return err
	}
	_, err = p.Store.Put(ctx, pbReq)
	return err
}

func (p *Provider) Delete(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) error {
	_, err := p.Store.Delete(ctx, sdkcompat.ObjectStoreRequest(req))
	return err
}

func (p *Provider) Clear(ctx context.Context, store string) error {
	_, err := p.Store.Clear(ctx, sdkcompat.ObjectStoreNameRequest(store))
	return err
}

func (p *Provider) GetAll(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]gestalt.Record, error) {
	pbReq, err := sdkcompat.ObjectStoreRangeRequest(req)
	if err != nil {
		return nil, err
	}
	resp, err := p.Store.GetAll(ctx, pbReq)
	if err != nil {
		return nil, err
	}
	return sdkcompat.RecordsFromProto(resp.GetRecords())
}

func (p *Provider) GetAllKeys(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]string, error) {
	pbReq, err := sdkcompat.ObjectStoreRangeRequest(req)
	if err != nil {
		return nil, err
	}
	resp, err := p.Store.GetAllKeys(ctx, pbReq)
	if err != nil {
		return nil, err
	}
	return resp.GetKeys(), nil
}

func (p *Provider) Count(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) (int64, error) {
	pbReq, err := sdkcompat.ObjectStoreRangeRequest(req)
	if err != nil {
		return 0, err
	}
	resp, err := p.Store.Count(ctx, pbReq)
	if err != nil {
		return 0, err
	}
	return resp.GetCount(), nil
}

func (p *Provider) DeleteRange(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) (int64, error) {
	pbReq, err := sdkcompat.ObjectStoreRangeRequest(req)
	if err != nil {
		return 0, err
	}
	resp, err := p.Store.DeleteRange(ctx, pbReq)
	if err != nil {
		return 0, err
	}
	return resp.GetDeleted(), nil
}

func (p *Provider) IndexGet(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (gestalt.Record, error) {
	pbReq, err := sdkcompat.IndexQueryRequest(req)
	if err != nil {
		return nil, err
	}
	resp, err := p.Store.IndexGet(ctx, pbReq)
	if err != nil {
		return nil, err
	}
	return sdkcompat.RecordFromProto(resp.GetRecord())
}

func (p *Provider) IndexGetKey(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (string, error) {
	pbReq, err := sdkcompat.IndexQueryRequest(req)
	if err != nil {
		return "", err
	}
	resp, err := p.Store.IndexGetKey(ctx, pbReq)
	if err != nil {
		return "", err
	}
	return resp.GetKey(), nil
}

func (p *Provider) IndexGetAll(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]gestalt.Record, error) {
	pbReq, err := sdkcompat.IndexQueryRequest(req)
	if err != nil {
		return nil, err
	}
	resp, err := p.Store.IndexGetAll(ctx, pbReq)
	if err != nil {
		return nil, err
	}
	return sdkcompat.RecordsFromProto(resp.GetRecords())
}

func (p *Provider) IndexGetAllKeys(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]string, error) {
	pbReq, err := sdkcompat.IndexQueryRequest(req)
	if err != nil {
		return nil, err
	}
	resp, err := p.Store.IndexGetAllKeys(ctx, pbReq)
	if err != nil {
		return nil, err
	}
	return resp.GetKeys(), nil
}

func (p *Provider) IndexCount(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (int64, error) {
	pbReq, err := sdkcompat.IndexQueryRequest(req)
	if err != nil {
		return 0, err
	}
	resp, err := p.Store.IndexCount(ctx, pbReq)
	if err != nil {
		return 0, err
	}
	return resp.GetCount(), nil
}

func (p *Provider) IndexDelete(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (int64, error) {
	pbReq, err := sdkcompat.IndexQueryRequest(req)
	if err != nil {
		return 0, err
	}
	resp, err := p.Store.IndexDelete(ctx, pbReq)
	if err != nil {
		return 0, err
	}
	return resp.GetDeleted(), nil
}

func (p *Provider) OpenCursor(ctx context.Context, req gestalt.IndexedDBOpenCursorRequest) (gestalt.IndexedDBCursor, error) {
	pbReq, err := sdkcompat.OpenCursorRequest(req)
	if err != nil {
		return nil, err
	}
	cursor, err := p.Store.openCursorSnapshot(ctx, pbReq)
	if err != nil {
		return nil, err
	}
	return sdkcompat.NewCursor(cursor), nil
}

func (p *Provider) BeginTransaction(ctx context.Context, req gestalt.IndexedDBBeginTransactionRequest) (gestalt.IndexedDBTransaction, error) {
	tx, err := p.Store.beginTransaction(ctx, sdkcompat.BeginTransactionRequest(req))
	if err != nil {
		return nil, err
	}
	return sdkcompat.NewTransaction(tx), nil
}
