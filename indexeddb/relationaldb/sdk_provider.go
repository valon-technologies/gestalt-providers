package relationaldb

import (
	"context"

	"github.com/valon-technologies/gestalt-providers/indexeddb/internal/txstream"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
)

const (
	transactionDurabilityDefault = 0
	transactionDurabilityStrict  = 1
	transactionDurabilityRelaxed = 2
)

var _ gestalt.IndexedDBProvider = (*Provider)(nil)

func (p *Provider) CreateObjectStore(ctx context.Context, name string, schema gestalt.ObjectStoreSchema) error {
	_, err := p.Store.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name:   name,
		Schema: objectStoreSchemaToProto(schema),
	})
	return err
}

func (p *Provider) DeleteObjectStore(ctx context.Context, name string) error {
	_, err := p.Store.DeleteObjectStore(ctx, &proto.DeleteObjectStoreRequest{Name: name})
	return err
}

func (p *Provider) OpenCursor(ctx context.Context, req gestalt.IndexedDBOpenCursorRequest) (gestalt.IndexedDBCursor, error) {
	pbReq, err := openCursorRequestToProto(req)
	if err != nil {
		return nil, err
	}
	cursor, err := p.Store.openCursorSnapshot(ctx, pbReq)
	if err != nil {
		return nil, err
	}
	return &sdkCursor{cursor: cursor, indexCursor: req.Index != ""}, nil
}

func (p *Provider) BeginTransaction(ctx context.Context, req gestalt.IndexedDBBeginTransactionRequest) (gestalt.IndexedDBTransaction, error) {
	pbReq := &proto.BeginTransactionRequest{
		Stores: req.Stores,
		Mode:   transactionModeToProto(req.Mode),
	}
	applyDurabilityHint(pbReq, req.DurabilityHint)
	tx, err := p.Store.beginTransaction(ctx, pbReq)
	if err != nil {
		return nil, err
	}
	return &sdkTransaction{
		sdkBackend: sdkBackend{backend: tx},
		tx:         tx,
	}, nil
}

type sdkBackend struct {
	backend txstream.Backend
}

func (b sdkBackend) Get(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) (gestalt.Record, error) {
	resp, err := b.backend.Get(ctx, objectStoreRequestToProto(req))
	if err != nil {
		return nil, err
	}
	return gestalt.RecordFromProto(resp.GetRecord())
}

func (b sdkBackend) GetKey(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) (string, error) {
	resp, err := b.backend.GetKey(ctx, objectStoreRequestToProto(req))
	if err != nil {
		return "", err
	}
	return resp.GetKey(), nil
}

func (b sdkBackend) Add(ctx context.Context, req gestalt.IndexedDBRecordRequest) error {
	pbReq, err := recordRequestToProto(req)
	if err != nil {
		return err
	}
	_, err = b.backend.Add(ctx, pbReq)
	return err
}

func (b sdkBackend) Put(ctx context.Context, req gestalt.IndexedDBRecordRequest) error {
	pbReq, err := recordRequestToProto(req)
	if err != nil {
		return err
	}
	_, err = b.backend.Put(ctx, pbReq)
	return err
}

func (b sdkBackend) Delete(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) error {
	_, err := b.backend.Delete(ctx, objectStoreRequestToProto(req))
	return err
}

func (b sdkBackend) Clear(ctx context.Context, store string) error {
	_, err := b.backend.Clear(ctx, &proto.ObjectStoreNameRequest{Store: store})
	return err
}

func (b sdkBackend) GetAll(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]gestalt.Record, error) {
	pbReq, err := objectStoreRangeRequestToProto(req)
	if err != nil {
		return nil, err
	}
	resp, err := b.backend.GetAll(ctx, pbReq)
	if err != nil {
		return nil, err
	}
	return recordsFromProto(resp.GetRecords())
}

func (b sdkBackend) GetAllKeys(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]string, error) {
	pbReq, err := objectStoreRangeRequestToProto(req)
	if err != nil {
		return nil, err
	}
	resp, err := b.backend.GetAllKeys(ctx, pbReq)
	if err != nil {
		return nil, err
	}
	return resp.GetKeys(), nil
}

func (b sdkBackend) Count(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) (int64, error) {
	pbReq, err := objectStoreRangeRequestToProto(req)
	if err != nil {
		return 0, err
	}
	resp, err := b.backend.Count(ctx, pbReq)
	if err != nil {
		return 0, err
	}
	return resp.GetCount(), nil
}

func (b sdkBackend) DeleteRange(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) (int64, error) {
	pbReq, err := objectStoreRangeRequestToProto(req)
	if err != nil {
		return 0, err
	}
	resp, err := b.backend.DeleteRange(ctx, pbReq)
	if err != nil {
		return 0, err
	}
	return resp.GetDeleted(), nil
}

func (b sdkBackend) IndexGet(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (gestalt.Record, error) {
	pbReq, err := indexQueryRequestToProto(req)
	if err != nil {
		return nil, err
	}
	resp, err := b.backend.IndexGet(ctx, pbReq)
	if err != nil {
		return nil, err
	}
	return gestalt.RecordFromProto(resp.GetRecord())
}

func (b sdkBackend) IndexGetKey(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (string, error) {
	pbReq, err := indexQueryRequestToProto(req)
	if err != nil {
		return "", err
	}
	resp, err := b.backend.IndexGetKey(ctx, pbReq)
	if err != nil {
		return "", err
	}
	return resp.GetKey(), nil
}

func (b sdkBackend) IndexGetAll(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]gestalt.Record, error) {
	pbReq, err := indexQueryRequestToProto(req)
	if err != nil {
		return nil, err
	}
	resp, err := b.backend.IndexGetAll(ctx, pbReq)
	if err != nil {
		return nil, err
	}
	return recordsFromProto(resp.GetRecords())
}

func (b sdkBackend) IndexGetAllKeys(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]string, error) {
	pbReq, err := indexQueryRequestToProto(req)
	if err != nil {
		return nil, err
	}
	resp, err := b.backend.IndexGetAllKeys(ctx, pbReq)
	if err != nil {
		return nil, err
	}
	return resp.GetKeys(), nil
}

func (b sdkBackend) IndexCount(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (int64, error) {
	pbReq, err := indexQueryRequestToProto(req)
	if err != nil {
		return 0, err
	}
	resp, err := b.backend.IndexCount(ctx, pbReq)
	if err != nil {
		return 0, err
	}
	return resp.GetCount(), nil
}

func (b sdkBackend) IndexDelete(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (int64, error) {
	pbReq, err := indexQueryRequestToProto(req)
	if err != nil {
		return 0, err
	}
	resp, err := b.backend.IndexDelete(ctx, pbReq)
	if err != nil {
		return 0, err
	}
	return resp.GetDeleted(), nil
}

type sdkCursor struct {
	cursor      *relationalCursor
	indexCursor bool
}

func (c *sdkCursor) Next(context.Context) (*gestalt.IndexedDBCursorEntry, error) {
	entry, ok, err := c.cursor.ContinueNext()
	if err != nil || !ok {
		return nil, err
	}
	return indexedDBCursorEntryFromProto(entry, c.indexCursor)
}

func (c *sdkCursor) ContinueToKey(_ context.Context, key any) (*gestalt.IndexedDBCursorEntry, error) {
	entry, ok, err := c.cursor.ContinueToKey(key)
	if err != nil || !ok {
		return nil, err
	}
	return indexedDBCursorEntryFromProto(entry, c.indexCursor)
}

func (c *sdkCursor) Advance(_ context.Context, count int) (*gestalt.IndexedDBCursorEntry, error) {
	entry, ok, err := c.cursor.Advance(count)
	if err != nil || !ok {
		return nil, err
	}
	return indexedDBCursorEntryFromProto(entry, c.indexCursor)
}

func (c *sdkCursor) Delete(ctx context.Context) error {
	return c.cursor.DeleteCurrent(ctx)
}

func (c *sdkCursor) Update(ctx context.Context, record gestalt.Record) (*gestalt.IndexedDBCursorEntry, error) {
	pbRecord, err := gestalt.RecordToProto(record)
	if err != nil {
		return nil, err
	}
	entry, err := c.cursor.UpdateCurrent(ctx, pbRecord)
	if err != nil {
		return nil, err
	}
	return indexedDBCursorEntryFromProto(entry, c.indexCursor)
}

func (c *sdkCursor) Close() error {
	return nil
}

type sdkTransaction struct {
	sdkBackend
	tx txstream.Transaction
}

func (t *sdkTransaction) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

func (t *sdkTransaction) Abort(ctx context.Context) error {
	return t.tx.Abort(ctx)
}

func objectStoreRequestToProto(req gestalt.IndexedDBObjectStoreRequest) *proto.ObjectStoreRequest {
	return &proto.ObjectStoreRequest{Store: req.Store, Id: req.ID}
}

func recordRequestToProto(req gestalt.IndexedDBRecordRequest) (*proto.RecordRequest, error) {
	record, err := gestalt.RecordToProto(req.Record)
	if err != nil {
		return nil, err
	}
	return &proto.RecordRequest{Store: req.Store, Record: record}, nil
}

func objectStoreRangeRequestToProto(req gestalt.IndexedDBObjectStoreRangeRequest) (*proto.ObjectStoreRangeRequest, error) {
	keyRange, err := keyRangeToProto(req.Range)
	if err != nil {
		return nil, err
	}
	return &proto.ObjectStoreRangeRequest{Store: req.Store, Range: keyRange}, nil
}

func indexQueryRequestToProto(req gestalt.IndexedDBIndexQueryRequest) (*proto.IndexQueryRequest, error) {
	values, err := gestalt.TypedValuesFromAny(req.Values)
	if err != nil {
		return nil, err
	}
	keyRange, err := keyRangeToProto(req.Range)
	if err != nil {
		return nil, err
	}
	return &proto.IndexQueryRequest{Store: req.Store, Index: req.Index, Values: values, Range: keyRange}, nil
}

func openCursorRequestToProto(req gestalt.IndexedDBOpenCursorRequest) (*proto.OpenCursorRequest, error) {
	values, err := gestalt.TypedValuesFromAny(req.Values)
	if err != nil {
		return nil, err
	}
	keyRange, err := keyRangeToProto(req.Range)
	if err != nil {
		return nil, err
	}
	out := &proto.OpenCursorRequest{
		Store:    req.Store,
		Range:    keyRange,
		KeysOnly: req.KeysOnly,
		Index:    req.Index,
		Values:   values,
	}
	applyCursorDirection(out, req.Direction)
	return out, nil
}

func keyRangeToProto(keyRange *gestalt.KeyRange) (*proto.KeyRange, error) {
	if keyRange == nil {
		return nil, nil
	}
	out := &proto.KeyRange{LowerOpen: keyRange.LowerOpen, UpperOpen: keyRange.UpperOpen}
	if keyRange.Lower != nil {
		lower, err := gestalt.TypedValueFromAny(keyRange.Lower)
		if err != nil {
			return nil, err
		}
		out.Lower = lower
	}
	if keyRange.Upper != nil {
		upper, err := gestalt.TypedValueFromAny(keyRange.Upper)
		if err != nil {
			return nil, err
		}
		out.Upper = upper
	}
	return out, nil
}

func objectStoreSchemaToProto(schema gestalt.ObjectStoreSchema) *proto.ObjectStoreSchema {
	indexes := make([]*proto.IndexSchema, 0, len(schema.Indexes))
	for _, index := range schema.Indexes {
		indexes = append(indexes, &proto.IndexSchema{
			Name:    index.Name,
			KeyPath: append([]string(nil), index.KeyPath...),
			Unique:  index.Unique,
		})
	}
	columns := make([]*proto.ColumnDef, 0, len(schema.Columns))
	for _, column := range schema.Columns {
		columns = append(columns, &proto.ColumnDef{
			Name:       column.Name,
			Type:       int32(column.Type),
			PrimaryKey: column.PrimaryKey,
			NotNull:    column.NotNull,
			Unique:     column.Unique,
		})
	}
	return &proto.ObjectStoreSchema{Indexes: indexes, Columns: columns}
}

func recordsFromProto(records []*proto.Record) ([]gestalt.Record, error) {
	out := make([]gestalt.Record, 0, len(records))
	for _, record := range records {
		decoded, err := gestalt.RecordFromProto(record)
		if err != nil {
			return nil, err
		}
		out = append(out, decoded)
	}
	return out, nil
}

func indexedDBCursorEntryFromProto(entry *proto.CursorEntry, indexCursor bool) (*gestalt.IndexedDBCursorEntry, error) {
	if entry == nil {
		return nil, nil
	}
	keys, err := gestalt.KeyValuesToAny(entry.GetKey())
	if err != nil {
		return nil, err
	}
	var key any = keys
	if !indexCursor && len(keys) == 1 {
		key = keys[0]
	}
	var record gestalt.Record
	if entry.GetRecord() != nil {
		record, err = gestalt.RecordFromProto(entry.GetRecord())
		if err != nil {
			return nil, err
		}
	}
	return &gestalt.IndexedDBCursorEntry{
		Key:        key,
		PrimaryKey: entry.GetPrimaryKey(),
		Record:     record,
	}, nil
}

func applyCursorDirection(req *proto.OpenCursorRequest, direction gestalt.CursorDirection) {
	switch direction {
	case gestalt.CursorNextUnique:
		req.Direction = proto.CursorDirection_CURSOR_NEXT_UNIQUE
	case gestalt.CursorPrev:
		req.Direction = proto.CursorDirection_CURSOR_PREV
	case gestalt.CursorPrevUnique:
		req.Direction = proto.CursorDirection_CURSOR_PREV_UNIQUE
	default:
		req.Direction = proto.CursorDirection_CURSOR_NEXT
	}
}

func transactionModeToProto(mode gestalt.TransactionMode) proto.TransactionMode {
	if mode == gestalt.TransactionReadwrite {
		return proto.TransactionMode_TRANSACTION_READWRITE
	}
	return proto.TransactionMode_TRANSACTION_READONLY
}

func applyDurabilityHint(req *proto.BeginTransactionRequest, hint gestalt.TransactionDurabilityHint) {
	switch hint {
	case gestalt.TransactionDurabilityStrict:
		req.DurabilityHint = transactionDurabilityStrict
	case gestalt.TransactionDurabilityRelaxed:
		req.DurabilityHint = transactionDurabilityRelaxed
	default:
		req.DurabilityHint = transactionDurabilityDefault
	}
}
