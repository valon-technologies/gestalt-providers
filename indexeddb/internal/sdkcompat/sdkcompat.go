package sdkcompat

import (
	"context"
	"fmt"
	"strings"

	"github.com/valon-technologies/gestalt-providers/indexeddb/internal/cursorutil"
	"github.com/valon-technologies/gestalt-providers/indexeddb/internal/txstream"
	"github.com/valon-technologies/gestalt-providers/indexeddb/internal/wirecodec"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type statusSentinelError struct {
	err    error
	target error
}

func (e statusSentinelError) Error() string {
	return e.err.Error()
}

func (e statusSentinelError) Unwrap() error {
	return e.err
}

func (e statusSentinelError) Is(target error) bool {
	return target == e.target
}

func (e statusSentinelError) GRPCStatus() *status.Status {
	st, _ := status.FromError(e.err)
	return st
}

func Error(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	var target error
	switch st.Code() {
	case codes.NotFound:
		target = gestalt.ErrNotFound
	case codes.AlreadyExists:
		target = gestalt.ErrAlreadyExists
	case codes.InvalidArgument:
		if strings.Contains(st.Message(), "invalid transaction") {
			target = gestalt.ErrInvalidTransaction
		}
	case codes.FailedPrecondition:
		switch {
		case strings.Contains(st.Message(), "readonly"):
			target = gestalt.ErrReadOnly
		case strings.Contains(st.Message(), "already finished"):
			target = gestalt.ErrTransactionDone
		}
	}
	if target == nil {
		return err
	}
	return statusSentinelError{err: err, target: target}
}

const (
	transactionDurabilityDefault = 0
	transactionDurabilityStrict  = 1
	transactionDurabilityRelaxed = 2
)

func RecordToProto(record gestalt.Record) (*proto.Record, error) {
	out := &proto.Record{Fields: make(map[string]*proto.TypedValue, len(record))}
	for key, value := range record {
		typed, err := wirecodec.TypedValueFromAny(value)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", key, err)
		}
		out.Fields[key] = typed
	}
	return out, nil
}

func RecordFromProto(record *proto.Record) (gestalt.Record, error) {
	if record == nil {
		return nil, nil
	}
	out := make(gestalt.Record, len(record.GetFields()))
	for key, typed := range record.GetFields() {
		value, err := wirecodec.AnyFromTypedValue(typed)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", key, err)
		}
		out[key] = value
	}
	return out, nil
}

func RecordsFromProto(records []*proto.Record) ([]gestalt.Record, error) {
	out := make([]gestalt.Record, len(records))
	for i, record := range records {
		decoded, err := RecordFromProto(record)
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
		out[i] = decoded
	}
	return out, nil
}

func CreateObjectStoreRequest(name string, schema gestalt.ObjectStoreSchema) *proto.CreateObjectStoreRequest {
	indexes := make([]*proto.IndexSchema, len(schema.Indexes))
	for i, idx := range schema.Indexes {
		indexes[i] = &proto.IndexSchema{Name: idx.Name, KeyPath: idx.KeyPath, Unique: idx.Unique}
	}
	columns := make([]*proto.ColumnDef, len(schema.Columns))
	for i, col := range schema.Columns {
		columns[i] = &proto.ColumnDef{
			Name:       col.Name,
			Type:       int32(col.Type),
			PrimaryKey: col.PrimaryKey,
			NotNull:    col.NotNull,
			Unique:     col.Unique,
		}
	}
	return &proto.CreateObjectStoreRequest{Name: name, Schema: &proto.ObjectStoreSchema{Indexes: indexes, Columns: columns}}
}

func DeleteObjectStoreRequest(name string) *proto.DeleteObjectStoreRequest {
	return &proto.DeleteObjectStoreRequest{Name: name}
}

func ObjectStoreRequest(req gestalt.IndexedDBObjectStoreRequest) *proto.ObjectStoreRequest {
	return &proto.ObjectStoreRequest{Store: req.Store, Id: req.ID}
}

func RecordRequest(req gestalt.IndexedDBRecordRequest) (*proto.RecordRequest, error) {
	record, err := RecordToProto(req.Record)
	if err != nil {
		return nil, err
	}
	return &proto.RecordRequest{Store: req.Store, Record: record}, nil
}

func ObjectStoreNameRequest(store string) *proto.ObjectStoreNameRequest {
	return &proto.ObjectStoreNameRequest{Store: store}
}

func ObjectStoreRangeRequest(req gestalt.IndexedDBObjectStoreRangeRequest) (*proto.ObjectStoreRangeRequest, error) {
	rng, err := KeyRange(req.Range)
	if err != nil {
		return nil, err
	}
	return &proto.ObjectStoreRangeRequest{Store: req.Store, Range: rng}, nil
}

func IndexQueryRequest(req gestalt.IndexedDBIndexQueryRequest) (*proto.IndexQueryRequest, error) {
	values, err := TypedValues(req.Values)
	if err != nil {
		return nil, err
	}
	rng, err := KeyRange(req.Range)
	if err != nil {
		return nil, err
	}
	return &proto.IndexQueryRequest{Store: req.Store, Index: req.Index, Values: values, Range: rng}, nil
}

func OpenCursorRequest(req gestalt.IndexedDBOpenCursorRequest) (*proto.OpenCursorRequest, error) {
	values, err := TypedValues(req.Values)
	if err != nil {
		return nil, err
	}
	rng, err := KeyRange(req.Range)
	if err != nil {
		return nil, err
	}
	direction := proto.CursorDirection_CURSOR_NEXT
	switch req.Direction {
	case gestalt.CursorNextUnique:
		direction = proto.CursorDirection_CURSOR_NEXT_UNIQUE
	case gestalt.CursorPrev:
		direction = proto.CursorDirection_CURSOR_PREV
	case gestalt.CursorPrevUnique:
		direction = proto.CursorDirection_CURSOR_PREV_UNIQUE
	}
	return &proto.OpenCursorRequest{
		Store:     req.Store,
		Range:     rng,
		Direction: direction,
		KeysOnly:  req.KeysOnly,
		Index:     req.Index,
		Values:    values,
	}, nil
}

func BeginTransactionRequest(req gestalt.IndexedDBBeginTransactionRequest) *proto.BeginTransactionRequest {
	pbReq := &proto.BeginTransactionRequest{
		Stores: req.Stores,
		Mode:   TransactionMode(req.Mode),
	}
	switch req.DurabilityHint {
	case gestalt.TransactionDurabilityStrict:
		pbReq.DurabilityHint = transactionDurabilityStrict
	case gestalt.TransactionDurabilityRelaxed:
		pbReq.DurabilityHint = transactionDurabilityRelaxed
	default:
		pbReq.DurabilityHint = transactionDurabilityDefault
	}
	return pbReq
}

func KeyRange(r *gestalt.KeyRange) (*proto.KeyRange, error) {
	if r == nil {
		return nil, nil
	}
	out := &proto.KeyRange{LowerOpen: r.LowerOpen, UpperOpen: r.UpperOpen}
	if r.Lower != nil {
		lower, err := wirecodec.TypedValueFromAny(r.Lower)
		if err != nil {
			return nil, fmt.Errorf("lower bound: %w", err)
		}
		out.Lower = lower
	}
	if r.Upper != nil {
		upper, err := wirecodec.TypedValueFromAny(r.Upper)
		if err != nil {
			return nil, fmt.Errorf("upper bound: %w", err)
		}
		out.Upper = upper
	}
	return out, nil
}

func TypedValues(values []any) ([]*proto.TypedValue, error) {
	out := make([]*proto.TypedValue, len(values))
	for i, value := range values {
		typed, err := wirecodec.TypedValueFromAny(value)
		if err != nil {
			return nil, fmt.Errorf("value %d: %w", i, err)
		}
		out[i] = typed
	}
	return out, nil
}

func TransactionMode(mode gestalt.TransactionMode) proto.TransactionMode {
	if mode == gestalt.TransactionReadwrite {
		return proto.TransactionMode_TRANSACTION_READWRITE
	}
	return proto.TransactionMode_TRANSACTION_READONLY
}

type Cursor struct {
	runtime cursorutil.Runtime
}

func NewCursor(runtime cursorutil.Runtime) *Cursor {
	return &Cursor{runtime: runtime}
}

func (c *Cursor) Next(ctx context.Context) (*gestalt.IndexedDBCursorEntry, error) {
	entry, ok, err := c.runtime.SnapshotState().ContinueNext()
	return cursorEntry(entry, ok, err)
}

func (c *Cursor) ContinueToKey(ctx context.Context, key any) (*gestalt.IndexedDBCursorEntry, error) {
	entry, ok, err := c.runtime.SnapshotState().ContinueToKey(key)
	return cursorEntry(entry, ok, err)
}

func (c *Cursor) Advance(ctx context.Context, count int) (*gestalt.IndexedDBCursorEntry, error) {
	entry, ok, err := c.runtime.SnapshotState().Advance(count)
	return cursorEntry(entry, ok, err)
}

func (c *Cursor) Delete(ctx context.Context) error {
	return Error(c.runtime.DeleteCurrent(ctx))
}

func (c *Cursor) Update(ctx context.Context, record gestalt.Record) (*gestalt.IndexedDBCursorEntry, error) {
	pbRecord, err := RecordToProto(record)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "marshal cursor update: %v", err)
	}
	entry, err := c.runtime.UpdateCurrent(ctx, pbRecord)
	return cursorEntry(entry, true, err)
}

func (c *Cursor) Close() error {
	return nil
}

func cursorEntry(entry *proto.CursorEntry, ok bool, err error) (*gestalt.IndexedDBCursorEntry, error) {
	if err != nil {
		return nil, Error(err)
	}
	if !ok || entry == nil {
		return nil, nil
	}
	key, err := cursorKey(entry.GetKey(), len(entry.GetKey()) > 1)
	if err != nil {
		return nil, err
	}
	record, err := RecordFromProto(entry.GetRecord())
	if err != nil {
		return nil, err
	}
	return &gestalt.IndexedDBCursorEntry{Key: key, PrimaryKey: entry.GetPrimaryKey(), Record: record}, nil
}

func cursorKey(values []*proto.KeyValue, indexCursor bool) (any, error) {
	parts, err := wirecodec.KeyValuesToAny(values)
	if err != nil {
		return nil, err
	}
	if indexCursor {
		return parts, nil
	}
	if len(parts) == 1 {
		return parts[0], nil
	}
	return parts, nil
}

type Transaction struct {
	tx txstream.Transaction
}

func NewTransaction(tx txstream.Transaction) *Transaction {
	return &Transaction{tx: tx}
}

func (t *Transaction) Commit(ctx context.Context) error {
	return Error(t.tx.Commit(ctx))
}

func (t *Transaction) Abort(ctx context.Context) error {
	return Error(t.tx.Abort(ctx))
}

func (t *Transaction) Get(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) (gestalt.Record, error) {
	resp, err := t.tx.Get(ctx, ObjectStoreRequest(req))
	if err != nil {
		return nil, Error(err)
	}
	return RecordFromProto(resp.GetRecord())
}

func (t *Transaction) GetKey(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) (string, error) {
	resp, err := t.tx.GetKey(ctx, ObjectStoreRequest(req))
	if err != nil {
		return "", Error(err)
	}
	return resp.GetKey(), nil
}

func (t *Transaction) Add(ctx context.Context, req gestalt.IndexedDBRecordRequest) error {
	pbReq, err := RecordRequest(req)
	if err != nil {
		return err
	}
	_, err = t.tx.Add(ctx, pbReq)
	return Error(err)
}

func (t *Transaction) Put(ctx context.Context, req gestalt.IndexedDBRecordRequest) error {
	pbReq, err := RecordRequest(req)
	if err != nil {
		return err
	}
	_, err = t.tx.Put(ctx, pbReq)
	return Error(err)
}

func (t *Transaction) Delete(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) error {
	_, err := t.tx.Delete(ctx, ObjectStoreRequest(req))
	return Error(err)
}

func (t *Transaction) Clear(ctx context.Context, store string) error {
	_, err := t.tx.Clear(ctx, ObjectStoreNameRequest(store))
	return Error(err)
}

func (t *Transaction) GetAll(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]gestalt.Record, error) {
	pbReq, err := ObjectStoreRangeRequest(req)
	if err != nil {
		return nil, err
	}
	resp, err := t.tx.GetAll(ctx, pbReq)
	if err != nil {
		return nil, Error(err)
	}
	return RecordsFromProto(resp.GetRecords())
}

func (t *Transaction) GetAllKeys(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]string, error) {
	pbReq, err := ObjectStoreRangeRequest(req)
	if err != nil {
		return nil, err
	}
	resp, err := t.tx.GetAllKeys(ctx, pbReq)
	if err != nil {
		return nil, Error(err)
	}
	return resp.GetKeys(), nil
}

func (t *Transaction) Count(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) (int64, error) {
	pbReq, err := ObjectStoreRangeRequest(req)
	if err != nil {
		return 0, err
	}
	resp, err := t.tx.Count(ctx, pbReq)
	if err != nil {
		return 0, Error(err)
	}
	return resp.GetCount(), nil
}

func (t *Transaction) DeleteRange(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) (int64, error) {
	pbReq, err := ObjectStoreRangeRequest(req)
	if err != nil {
		return 0, err
	}
	resp, err := t.tx.DeleteRange(ctx, pbReq)
	if err != nil {
		return 0, Error(err)
	}
	return resp.GetDeleted(), nil
}

func (t *Transaction) IndexGet(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (gestalt.Record, error) {
	pbReq, err := IndexQueryRequest(req)
	if err != nil {
		return nil, err
	}
	resp, err := t.tx.IndexGet(ctx, pbReq)
	if err != nil {
		return nil, Error(err)
	}
	return RecordFromProto(resp.GetRecord())
}

func (t *Transaction) IndexGetKey(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (string, error) {
	pbReq, err := IndexQueryRequest(req)
	if err != nil {
		return "", err
	}
	resp, err := t.tx.IndexGetKey(ctx, pbReq)
	if err != nil {
		return "", Error(err)
	}
	return resp.GetKey(), nil
}

func (t *Transaction) IndexGetAll(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]gestalt.Record, error) {
	pbReq, err := IndexQueryRequest(req)
	if err != nil {
		return nil, err
	}
	resp, err := t.tx.IndexGetAll(ctx, pbReq)
	if err != nil {
		return nil, Error(err)
	}
	return RecordsFromProto(resp.GetRecords())
}

func (t *Transaction) IndexGetAllKeys(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]string, error) {
	pbReq, err := IndexQueryRequest(req)
	if err != nil {
		return nil, err
	}
	resp, err := t.tx.IndexGetAllKeys(ctx, pbReq)
	if err != nil {
		return nil, Error(err)
	}
	return resp.GetKeys(), nil
}

func (t *Transaction) IndexCount(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (int64, error) {
	pbReq, err := IndexQueryRequest(req)
	if err != nil {
		return 0, err
	}
	resp, err := t.tx.IndexCount(ctx, pbReq)
	if err != nil {
		return 0, Error(err)
	}
	return resp.GetCount(), nil
}

func (t *Transaction) IndexDelete(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (int64, error) {
	pbReq, err := IndexQueryRequest(req)
	if err != nil {
		return 0, err
	}
	resp, err := t.tx.IndexDelete(ctx, pbReq)
	if err != nil {
		return 0, Error(err)
	}
	return resp.GetDeleted(), nil
}
