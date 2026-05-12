package dynamodb

import (
	"context"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func (p *Provider) CreateObjectStore(ctx context.Context, name string, schema gestalt.ObjectStoreSchema) error {
	_, err := p.providerCore.CreateObjectStore(ctx, CreateObjectStoreRequest(name, schema))
	return Error(err)
}

func (p *Provider) DeleteObjectStore(ctx context.Context, name string) error {
	_, err := p.providerCore.DeleteObjectStore(ctx, DeleteObjectStoreRequest(name))
	return Error(err)
}

func (p *Provider) Get(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) (gestalt.Record, error) {
	resp, err := p.providerCore.Get(ctx, ObjectStoreRequest(req))
	if err != nil {
		return nil, Error(err)
	}
	return RecordFromProto(resp.GetRecord())
}

func (p *Provider) GetKey(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) (string, error) {
	resp, err := p.providerCore.GetKey(ctx, ObjectStoreRequest(req))
	if err != nil {
		return "", Error(err)
	}
	return resp.GetKey(), nil
}

func (p *Provider) Add(ctx context.Context, req gestalt.IndexedDBRecordRequest) error {
	pbReq, err := RecordRequest(req)
	if err != nil {
		return err
	}
	_, err = p.providerCore.Add(ctx, pbReq)
	return Error(err)
}

func (p *Provider) Put(ctx context.Context, req gestalt.IndexedDBRecordRequest) error {
	pbReq, err := RecordRequest(req)
	if err != nil {
		return err
	}
	_, err = p.providerCore.Put(ctx, pbReq)
	return Error(err)
}

func (p *Provider) Delete(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) error {
	_, err := p.providerCore.Delete(ctx, ObjectStoreRequest(req))
	return Error(err)
}

func (p *Provider) Clear(ctx context.Context, store string) error {
	_, err := p.providerCore.Clear(ctx, ObjectStoreNameRequest(store))
	return Error(err)
}

func (p *Provider) GetAll(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]gestalt.Record, error) {
	pbReq, err := ObjectStoreRangeRequest(req)
	if err != nil {
		return nil, err
	}
	resp, err := p.providerCore.GetAll(ctx, pbReq)
	if err != nil {
		return nil, Error(err)
	}
	return RecordsFromProto(resp.GetRecords())
}

func (p *Provider) GetAllKeys(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]string, error) {
	pbReq, err := ObjectStoreRangeRequest(req)
	if err != nil {
		return nil, err
	}
	resp, err := p.providerCore.GetAllKeys(ctx, pbReq)
	if err != nil {
		return nil, Error(err)
	}
	return resp.GetKeys(), nil
}

func (p *Provider) Count(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) (int64, error) {
	pbReq, err := ObjectStoreRangeRequest(req)
	if err != nil {
		return 0, err
	}
	resp, err := p.providerCore.Count(ctx, pbReq)
	if err != nil {
		return 0, Error(err)
	}
	return resp.GetCount(), nil
}

func (p *Provider) DeleteRange(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) (int64, error) {
	pbReq, err := ObjectStoreRangeRequest(req)
	if err != nil {
		return 0, err
	}
	resp, err := p.providerCore.DeleteRange(ctx, pbReq)
	if err != nil {
		return 0, Error(err)
	}
	return resp.GetDeleted(), nil
}

func (p *Provider) IndexGet(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (gestalt.Record, error) {
	pbReq, err := IndexQueryRequest(req)
	if err != nil {
		return nil, err
	}
	resp, err := p.providerCore.IndexGet(ctx, pbReq)
	if err != nil {
		return nil, Error(err)
	}
	return RecordFromProto(resp.GetRecord())
}

func (p *Provider) IndexGetKey(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (string, error) {
	pbReq, err := IndexQueryRequest(req)
	if err != nil {
		return "", err
	}
	resp, err := p.providerCore.IndexGetKey(ctx, pbReq)
	if err != nil {
		return "", Error(err)
	}
	return resp.GetKey(), nil
}

func (p *Provider) IndexGetAll(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]gestalt.Record, error) {
	pbReq, err := IndexQueryRequest(req)
	if err != nil {
		return nil, err
	}
	resp, err := p.providerCore.IndexGetAll(ctx, pbReq)
	if err != nil {
		return nil, Error(err)
	}
	return RecordsFromProto(resp.GetRecords())
}

func (p *Provider) IndexGetAllKeys(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]string, error) {
	pbReq, err := IndexQueryRequest(req)
	if err != nil {
		return nil, err
	}
	resp, err := p.providerCore.IndexGetAllKeys(ctx, pbReq)
	if err != nil {
		return nil, Error(err)
	}
	return resp.GetKeys(), nil
}

func (p *Provider) IndexCount(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (int64, error) {
	pbReq, err := IndexQueryRequest(req)
	if err != nil {
		return 0, err
	}
	resp, err := p.providerCore.IndexCount(ctx, pbReq)
	if err != nil {
		return 0, Error(err)
	}
	return resp.GetCount(), nil
}

func (p *Provider) IndexDelete(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (int64, error) {
	pbReq, err := IndexQueryRequest(req)
	if err != nil {
		return 0, err
	}
	resp, err := p.providerCore.IndexDelete(ctx, pbReq)
	if err != nil {
		return 0, Error(err)
	}
	return resp.GetDeleted(), nil
}

func (p *Provider) OpenCursor(ctx context.Context, req gestalt.IndexedDBOpenCursorRequest) (gestalt.IndexedDBCursor, error) {
	pbReq, err := OpenCursorRequest(req)
	if err != nil {
		return nil, err
	}
	cursor, err := p.providerCore.openCursorSnapshot(ctx, pbReq)
	if err != nil {
		return nil, Error(err)
	}
	return NewCursor(cursor), nil
}

func (p *Provider) BeginTransaction(ctx context.Context, req gestalt.IndexedDBBeginTransactionRequest) (gestalt.IndexedDBTransaction, error) {
	tx, err := p.providerCore.beginTransaction(ctx, BeginTransactionRequest(req))
	if err != nil {
		return nil, Error(err)
	}
	return NewTransaction(tx), nil
}
