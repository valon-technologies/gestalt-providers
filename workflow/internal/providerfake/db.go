package providerfake

import (
	"context"
	"errors"
	"strings"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ProviderDB implements indexeddb.Database by delegating to gestalt.IndexedDBProvider.
type ProviderDB struct {
	provider gestalt.IndexedDBProvider
}

func NewProviderDB(provider gestalt.IndexedDBProvider) ProviderDB {
	return ProviderDB{provider: provider}
}

func rpcError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, indexeddb.ErrNotFound):
		return indexeddb.ErrNotFound
	case errors.Is(err, indexeddb.ErrAlreadyExists):
		return indexeddb.ErrAlreadyExists
	case errors.Is(err, indexeddb.ErrInvalidTransaction):
		return indexeddb.ErrInvalidTransaction
	case errors.Is(err, indexeddb.ErrReadOnly):
		return indexeddb.ErrReadOnly
	case errors.Is(err, indexeddb.ErrTransactionDone):
		return indexeddb.ErrTransactionDone
	}
	if code, ok := gestalt.StatusCodeOf(err); ok {
		if mapped := statusCodeError(code, err.Error()); mapped != nil {
			return mapped
		}
		return err
	}
	if st, ok := status.FromError(err); ok {
		if mapped := statusCodeError(statusCodeFromGRPC(st.Code()), st.Message()); mapped != nil {
			return mapped
		}
		return err
	}
	return err
}

func statusCodeError(code gestalt.StatusCode, message string) error {
	normalized := strings.ToLower(message)
	switch code {
	case gestalt.CodeNotFound:
		return indexeddb.ErrNotFound
	case gestalt.CodeAlreadyExists:
		return indexeddb.ErrAlreadyExists
	case gestalt.CodeInvalidArgument:
		if strings.Contains(normalized, "invalid transaction") {
			return indexeddb.ErrInvalidTransaction
		}
	case gestalt.CodeFailedPrecondition:
		if strings.Contains(normalized, "readonly") {
			return indexeddb.ErrReadOnly
		}
		if strings.Contains(normalized, "already finished") {
			return indexeddb.ErrTransactionDone
		}
	}
	return nil
}

func statusCodeFromGRPC(code codes.Code) gestalt.StatusCode {
	switch code {
	case codes.NotFound:
		return gestalt.CodeNotFound
	case codes.AlreadyExists:
		return gestalt.CodeAlreadyExists
	case codes.InvalidArgument:
		return gestalt.CodeInvalidArgument
	case codes.FailedPrecondition:
		return gestalt.CodeFailedPrecondition
	default:
		return gestalt.CodeUnknown
	}
}

func (db ProviderDB) CreateObjectStore(ctx context.Context, name string, schema indexeddb.ObjectStoreOptions) (indexeddb.ObjectStore, error) {
	if err := rpcError(db.provider.CreateObjectStore(ctx, name, schema)); err != nil {
		return nil, err
	}
	return db.ObjectStore(name), nil
}

func (db ProviderDB) DeleteObjectStore(ctx context.Context, name string) error {
	return rpcError(db.provider.DeleteObjectStore(ctx, name))
}

func (db ProviderDB) ObjectStore(name string) indexeddb.ObjectStore {
	return ProviderStore{provider: db.provider, store: name}
}

func (db ProviderDB) Transaction(ctx context.Context, stores []string, mode gestalt.TransactionMode, opts gestalt.TransactionOptions) (indexeddb.Transaction, error) {
	tx, err := db.provider.BeginTransaction(ctx, gestalt.IndexedDBBeginTransactionRequest{
		Stores:         stores,
		Mode:           mode,
		DurabilityHint: opts.DurabilityHint,
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return ProviderTx{tx: tx}, nil
}

func (db ProviderDB) Close() error { return nil }

type ProviderStore struct {
	provider gestalt.IndexedDBProvider
	store    string
}

func (s ProviderStore) Get(ctx context.Context, id string) (gestalt.Record, error) {
	record, err := s.provider.Get(ctx, gestalt.IndexedDBObjectStoreRequest{Store: s.store, ID: id})
	return record, rpcError(err)
}

func (s ProviderStore) GetKey(ctx context.Context, id string) (string, error) {
	if _, err := s.Get(ctx, id); err != nil {
		return "", err
	}
	return id, nil
}

func (s ProviderStore) Put(ctx context.Context, record gestalt.Record) error {
	return rpcError(s.provider.Put(ctx, gestalt.IndexedDBRecordRequest{Store: s.store, Record: record}))
}

func (s ProviderStore) Add(ctx context.Context, record gestalt.Record) error {
	return rpcError(s.provider.Add(ctx, gestalt.IndexedDBRecordRequest{Store: s.store, Record: record}))
}

func (s ProviderStore) Delete(ctx context.Context, id string) error {
	return rpcError(s.provider.Delete(ctx, gestalt.IndexedDBObjectStoreRequest{Store: s.store, ID: id}))
}

func (s ProviderStore) Clear(ctx context.Context) error {
	return indexeddb.ErrUnsupported
}

func (s ProviderStore) GetAll(ctx context.Context, r *gestalt.KeyRange) ([]gestalt.Record, error) {
	records, err := s.provider.GetAll(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: s.store, Range: r})
	return records, rpcError(err)
}

func (s ProviderStore) GetAllKeys(ctx context.Context, r *gestalt.KeyRange) ([]string, error) {
	return nil, indexeddb.ErrUnsupported
}

func (s ProviderStore) Count(ctx context.Context, r *gestalt.KeyRange) (int64, error) {
	count, err := s.provider.Count(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: s.store, Range: r})
	return count, rpcError(err)
}

func (s ProviderStore) DeleteRange(ctx context.Context, r gestalt.KeyRange) (int64, error) {
	return 0, indexeddb.ErrUnsupported
}

func (s ProviderStore) Index(name string) indexeddb.Index {
	return ProviderIndex{provider: s.provider, store: s.store, index: name}
}

func (s ProviderStore) OpenCursor(ctx context.Context, r *gestalt.KeyRange, dir gestalt.CursorDirection) (indexeddb.Cursor, error) {
	cur, err := s.provider.OpenCursor(ctx, gestalt.IndexedDBOpenCursorRequest{
		Store:     s.store,
		Range:     r,
		Direction: dir,
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return &ProviderCursor{ctx: ctx, cur: cur}, nil
}

func (s ProviderStore) OpenKeyCursor(ctx context.Context, r *gestalt.KeyRange, dir gestalt.CursorDirection) (indexeddb.Cursor, error) {
	cur, err := s.provider.OpenCursor(ctx, gestalt.IndexedDBOpenCursorRequest{
		Store:     s.store,
		Range:     r,
		Direction: dir,
		KeysOnly:  true,
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return &ProviderCursor{ctx: ctx, cur: cur}, nil
}

type ProviderIndex struct {
	provider gestalt.IndexedDBProvider
	store    string
	index    string
}

func (idx ProviderIndex) Get(ctx context.Context, values ...any) (gestalt.Record, error) {
	return nil, indexeddb.ErrUnsupported
}

func (idx ProviderIndex) GetKey(ctx context.Context, values ...any) (string, error) {
	return "", indexeddb.ErrUnsupported
}

func (idx ProviderIndex) GetAll(ctx context.Context, r *gestalt.KeyRange, values ...any) ([]gestalt.Record, error) {
	records, err := idx.provider.IndexGetAll(ctx, gestalt.IndexedDBIndexQueryRequest{
		Store:  idx.store,
		Index:  idx.index,
		Values: values,
		Range:  r,
	})
	return records, rpcError(err)
}

func (idx ProviderIndex) GetAllKeys(ctx context.Context, r *gestalt.KeyRange, values ...any) ([]string, error) {
	return nil, indexeddb.ErrUnsupported
}

func (idx ProviderIndex) Count(ctx context.Context, r *gestalt.KeyRange, values ...any) (int64, error) {
	count, err := idx.provider.IndexCount(ctx, gestalt.IndexedDBIndexQueryRequest{
		Store:  idx.store,
		Index:  idx.index,
		Values: values,
		Range:  r,
	})
	return count, rpcError(err)
}

func (idx ProviderIndex) Delete(ctx context.Context, values ...any) (int64, error) {
	deleted, err := idx.provider.IndexDelete(ctx, gestalt.IndexedDBIndexQueryRequest{
		Store:  idx.store,
		Index:  idx.index,
		Values: values,
	})
	return deleted, rpcError(err)
}

func (idx ProviderIndex) DeleteRange(ctx context.Context, r *gestalt.KeyRange, values ...any) (int64, error) {
	return 0, indexeddb.ErrUnsupported
}

func (idx ProviderIndex) OpenCursor(ctx context.Context, r *gestalt.KeyRange, dir gestalt.CursorDirection, values ...any) (indexeddb.Cursor, error) {
	return nil, indexeddb.ErrUnsupported
}

func (idx ProviderIndex) OpenKeyCursor(ctx context.Context, r *gestalt.KeyRange, dir gestalt.CursorDirection, values ...any) (indexeddb.Cursor, error) {
	cur, err := idx.provider.OpenCursor(ctx, gestalt.IndexedDBOpenCursorRequest{
		Store:     idx.store,
		Index:     idx.index,
		Range:     r,
		Direction: dir,
		KeysOnly:  true,
		Values:    values,
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return &ProviderCursor{ctx: ctx, cur: cur}, nil
}

type ProviderCursor struct {
	ctx   context.Context
	cur   gestalt.IndexedDBCursor
	entry *gestalt.IndexedDBCursorEntry
	err   error
	done  bool
}

func (c *ProviderCursor) ContinueToKey(key any) bool {
	_ = key
	return false
}

func (c *ProviderCursor) Advance(count int) bool {
	for i := 0; i < count; i++ {
		if !c.Continue() {
			return false
		}
	}
	return true
}

func (c *ProviderCursor) Key() any {
	if c.entry == nil {
		return nil
	}
	return c.entry.PrimaryKey
}

func (c *ProviderCursor) Delete() error { return indexeddb.ErrUnsupported }

func (c *ProviderCursor) Update(value gestalt.Record) error {
	_ = value
	return indexeddb.ErrUnsupported
}

func (c *ProviderCursor) Continue() bool {
	if c.done || c.err != nil {
		return false
	}
	entry, err := c.cur.Next(c.ctx)
	if err != nil {
		c.err = rpcError(err)
		return false
	}
	if entry == nil {
		c.done = true
		c.entry = nil
		return false
	}
	c.entry = entry
	return true
}

func (c *ProviderCursor) PrimaryKey() string {
	if c.entry == nil {
		return ""
	}
	return c.entry.PrimaryKey
}

func (c *ProviderCursor) Value() (gestalt.Record, error) {
	if c.err != nil {
		return nil, c.err
	}
	if c.done || c.entry == nil {
		return nil, gestalt.ErrNotFound
	}
	return c.entry.Record, nil
}

func (c *ProviderCursor) Err() error { return c.err }

func (c *ProviderCursor) Close() error { return c.cur.Close() }

type ProviderTx struct {
	tx gestalt.IndexedDBTransaction
}

func (tx ProviderTx) Commit(ctx context.Context) error {
	return rpcError(tx.tx.Commit(ctx))
}

func (tx ProviderTx) Abort(ctx context.Context) error {
	return rpcError(tx.tx.Abort(ctx))
}

func (tx ProviderTx) ObjectStore(name string) indexeddb.TransactionObjectStore {
	return ProviderTxStore{tx: tx.tx, store: name}
}

type ProviderTxStore struct {
	tx    gestalt.IndexedDBTransaction
	store string
}

func (s ProviderTxStore) Get(ctx context.Context, id string) (gestalt.Record, error) {
	record, err := s.tx.Get(ctx, gestalt.IndexedDBObjectStoreRequest{Store: s.store, ID: id})
	return record, rpcError(err)
}

func (s ProviderTxStore) GetKey(ctx context.Context, id string) (string, error) {
	if _, err := s.Get(ctx, id); err != nil {
		return "", err
	}
	return id, nil
}

func (s ProviderTxStore) Put(ctx context.Context, record gestalt.Record) error {
	return rpcError(s.tx.Put(ctx, gestalt.IndexedDBRecordRequest{Store: s.store, Record: record}))
}

func (s ProviderTxStore) Add(ctx context.Context, record gestalt.Record) error {
	return rpcError(s.tx.Add(ctx, gestalt.IndexedDBRecordRequest{Store: s.store, Record: record}))
}

func (s ProviderTxStore) Delete(ctx context.Context, id string) error {
	return rpcError(s.tx.Delete(ctx, gestalt.IndexedDBObjectStoreRequest{Store: s.store, ID: id}))
}

func (s ProviderTxStore) Clear(ctx context.Context) error {
	return indexeddb.ErrUnsupported
}

func (s ProviderTxStore) GetAll(ctx context.Context, r *gestalt.KeyRange) ([]gestalt.Record, error) {
	records, err := s.tx.GetAll(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: s.store, Range: r})
	return records, rpcError(err)
}

func (s ProviderTxStore) GetAllKeys(ctx context.Context, r *gestalt.KeyRange) ([]string, error) {
	return nil, indexeddb.ErrUnsupported
}

func (s ProviderTxStore) Count(ctx context.Context, r *gestalt.KeyRange) (int64, error) {
	return 0, indexeddb.ErrUnsupported
}

func (s ProviderTxStore) DeleteRange(ctx context.Context, r gestalt.KeyRange) (int64, error) {
	return 0, indexeddb.ErrUnsupported
}

func (s ProviderTxStore) Index(name string) indexeddb.TransactionIndex {
	return ProviderTxIndex{tx: s.tx, store: s.store, index: name}
}

type ProviderTxIndex struct {
	tx    gestalt.IndexedDBTransaction
	store string
	index string
}

func (idx ProviderTxIndex) Get(ctx context.Context, values ...any) (gestalt.Record, error) {
	return nil, indexeddb.ErrUnsupported
}

func (idx ProviderTxIndex) GetKey(ctx context.Context, values ...any) (string, error) {
	return "", indexeddb.ErrUnsupported
}

func (idx ProviderTxIndex) GetAll(ctx context.Context, r *gestalt.KeyRange, values ...any) ([]gestalt.Record, error) {
	records, err := idx.tx.IndexGetAll(ctx, gestalt.IndexedDBIndexQueryRequest{
		Store:  idx.store,
		Index:  idx.index,
		Values: values,
		Range:  r,
	})
	return records, rpcError(err)
}

func (idx ProviderTxIndex) Count(ctx context.Context, r *gestalt.KeyRange, values ...any) (int64, error) {
	count, err := idx.tx.IndexCount(ctx, gestalt.IndexedDBIndexQueryRequest{
		Store:  idx.store,
		Index:  idx.index,
		Values: values,
		Range:  r,
	})
	return count, rpcError(err)
}

func (idx ProviderTxIndex) GetAllKeys(ctx context.Context, r *gestalt.KeyRange, values ...any) ([]string, error) {
	return nil, indexeddb.ErrUnsupported
}

func (idx ProviderTxIndex) Delete(ctx context.Context, values ...any) (int64, error) {
	deleted, err := idx.tx.IndexDelete(ctx, gestalt.IndexedDBIndexQueryRequest{
		Store:  idx.store,
		Index:  idx.index,
		Values: values,
	})
	return deleted, rpcError(err)
}

func (idx ProviderTxIndex) DeleteRange(ctx context.Context, r *gestalt.KeyRange, values ...any) (int64, error) {
	return 0, indexeddb.ErrUnsupported
}
