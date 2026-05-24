package fake

import (
	"context"
	"strings"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func MapProviderError(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	switch st.Code() {
	case codes.NotFound:
		return gestalt.ErrNotFound
	case codes.AlreadyExists:
		return gestalt.ErrAlreadyExists
	case codes.InvalidArgument:
		if strings.Contains(st.Message(), "invalid transaction") {
			return gestalt.ErrInvalidTransaction
		}
		return err
	case codes.FailedPrecondition:
		if strings.Contains(st.Message(), "readonly") {
			return gestalt.ErrReadOnly
		}
		if strings.Contains(st.Message(), "already finished") {
			return gestalt.ErrTransactionDone
		}
		return err
	default:
		return err
	}
}

type ProviderDB struct {
	provider gestalt.IndexedDBProvider
}

func (db ProviderDB) CreateObjectStore(ctx context.Context, name string, schema gestalt.ObjectStoreSchema) (indexeddb.ObjectStore, error) {
	if err := MapProviderError(db.provider.CreateObjectStore(ctx, name, schema)); err != nil {
		return nil, err
	}
	return db.ObjectStore(name), nil
}

func (db ProviderDB) DeleteObjectStore(ctx context.Context, name string) error {
	return MapProviderError(db.provider.DeleteObjectStore(ctx, name))
}

func NewProviderDB(provider gestalt.IndexedDBProvider) ProviderDB {
	return ProviderDB{provider: provider}
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
		return nil, MapProviderError(err)
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
	return record, MapProviderError(err)
}

func (s ProviderStore) GetKey(ctx context.Context, id string) (string, error) {
	if _, err := s.Get(ctx, id); err != nil {
		return "", err
	}
	return id, nil
}

func (s ProviderStore) Put(ctx context.Context, record gestalt.Record) error {
	return MapProviderError(s.provider.Put(ctx, gestalt.IndexedDBRecordRequest{Store: s.store, Record: record}))
}

func (s ProviderStore) Add(ctx context.Context, record gestalt.Record) error {
	return MapProviderError(s.provider.Add(ctx, gestalt.IndexedDBRecordRequest{Store: s.store, Record: record}))
}

func (s ProviderStore) Delete(ctx context.Context, id string) error {
	return MapProviderError(s.provider.Delete(ctx, gestalt.IndexedDBObjectStoreRequest{Store: s.store, ID: id}))
}

func (s ProviderStore) Clear(_ context.Context) error {
	return indexeddb.ErrUnsupported
}

func (s ProviderStore) GetAll(ctx context.Context, r *gestalt.KeyRange) ([]gestalt.Record, error) {
	records, err := s.provider.GetAll(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: s.store, Range: r})
	return records, MapProviderError(err)
}

func (s ProviderStore) GetAllKeys(_ context.Context, _ *gestalt.KeyRange) ([]string, error) {
	return nil, indexeddb.ErrUnsupported
}

func (s ProviderStore) Count(ctx context.Context, r *gestalt.KeyRange) (int64, error) {
	count, err := s.provider.Count(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: s.store, Range: r})
	return count, MapProviderError(err)
}

func (s ProviderStore) DeleteRange(_ context.Context, _ gestalt.KeyRange) (int64, error) {
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
		return nil, MapProviderError(err)
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
		return nil, MapProviderError(err)
	}
	return &ProviderCursor{ctx: ctx, cur: cur}, nil
}

type ProviderIndex struct {
	provider gestalt.IndexedDBProvider
	store    string
	index    string
}

func (idx ProviderIndex) Get(_ context.Context, _ ...any) (gestalt.Record, error) {
	return nil, indexeddb.ErrUnsupported
}

func (idx ProviderIndex) GetKey(_ context.Context, _ ...any) (string, error) {
	return "", indexeddb.ErrUnsupported
}

func (idx ProviderIndex) GetAll(ctx context.Context, r *gestalt.KeyRange, values ...any) ([]gestalt.Record, error) {
	records, err := idx.provider.IndexGetAll(ctx, gestalt.IndexedDBIndexQueryRequest{
		Store:  idx.store,
		Index:  idx.index,
		Values: values,
		Range:  r,
	})
	return records, MapProviderError(err)
}

func (idx ProviderIndex) GetAllKeys(_ context.Context, _ *gestalt.KeyRange, _ ...any) ([]string, error) {
	return nil, indexeddb.ErrUnsupported
}

func (idx ProviderIndex) Count(ctx context.Context, r *gestalt.KeyRange, values ...any) (int64, error) {
	count, err := idx.provider.IndexCount(ctx, gestalt.IndexedDBIndexQueryRequest{
		Store:  idx.store,
		Index:  idx.index,
		Values: values,
		Range:  r,
	})
	return count, MapProviderError(err)
}

func (idx ProviderIndex) Delete(_ context.Context, _ ...any) (int64, error) {
	return 0, indexeddb.ErrUnsupported
}

func (idx ProviderIndex) DeleteRange(_ context.Context, _ *gestalt.KeyRange, _ ...any) (int64, error) {
	return 0, indexeddb.ErrUnsupported
}

func (idx ProviderIndex) OpenCursor(_ context.Context, _ *gestalt.KeyRange, _ gestalt.CursorDirection, _ ...any) (indexeddb.Cursor, error) {
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
		return nil, MapProviderError(err)
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

func (c *ProviderCursor) ContinueToKey(_ any) bool { return false }

func (c *ProviderCursor) Advance(_ int) bool { return false }

func (c *ProviderCursor) Key() any {
	if c.entry == nil {
		return nil
	}
	return c.entry.PrimaryKey
}

func (c *ProviderCursor) Delete() error { return indexeddb.ErrUnsupported }

func (c *ProviderCursor) Update(_ gestalt.Record) error { return indexeddb.ErrUnsupported }

func (c *ProviderCursor) Continue() bool {
	if c.done || c.err != nil {
		return false
	}
	entry, err := c.cur.Next(c.ctx)
	if err != nil {
		c.err = MapProviderError(err)
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
	return MapProviderError(tx.tx.Commit(ctx))
}

func (tx ProviderTx) Abort(ctx context.Context) error {
	return MapProviderError(tx.tx.Abort(ctx))
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
	return record, MapProviderError(err)
}

func (s ProviderTxStore) GetKey(ctx context.Context, id string) (string, error) {
	if _, err := s.Get(ctx, id); err != nil {
		return "", err
	}
	return id, nil
}

func (s ProviderTxStore) Put(ctx context.Context, record gestalt.Record) error {
	return MapProviderError(s.tx.Put(ctx, gestalt.IndexedDBRecordRequest{Store: s.store, Record: record}))
}

func (s ProviderTxStore) Add(ctx context.Context, record gestalt.Record) error {
	return MapProviderError(s.tx.Add(ctx, gestalt.IndexedDBRecordRequest{Store: s.store, Record: record}))
}

func (s ProviderTxStore) Delete(ctx context.Context, id string) error {
	return MapProviderError(s.tx.Delete(ctx, gestalt.IndexedDBObjectStoreRequest{Store: s.store, ID: id}))
}

func (s ProviderTxStore) Clear(_ context.Context) error {
	return indexeddb.ErrUnsupported
}

func (s ProviderTxStore) GetAll(ctx context.Context, r *gestalt.KeyRange) ([]gestalt.Record, error) {
	records, err := s.tx.GetAll(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: s.store, Range: r})
	return records, MapProviderError(err)
}

func (s ProviderTxStore) GetAllKeys(_ context.Context, _ *gestalt.KeyRange) ([]string, error) {
	return nil, indexeddb.ErrUnsupported
}

func (s ProviderTxStore) Count(_ context.Context, _ *gestalt.KeyRange) (int64, error) {
	return 0, indexeddb.ErrUnsupported
}

func (s ProviderTxStore) DeleteRange(_ context.Context, _ gestalt.KeyRange) (int64, error) {
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

func (idx ProviderTxIndex) Get(_ context.Context, _ ...any) (gestalt.Record, error) {
	return nil, indexeddb.ErrUnsupported
}

func (idx ProviderTxIndex) GetKey(_ context.Context, _ ...any) (string, error) {
	return "", indexeddb.ErrUnsupported
}

func (idx ProviderTxIndex) GetAll(ctx context.Context, r *gestalt.KeyRange, values ...any) ([]gestalt.Record, error) {
	records, err := idx.tx.IndexGetAll(ctx, gestalt.IndexedDBIndexQueryRequest{
		Store:  idx.store,
		Index:  idx.index,
		Values: values,
		Range:  r,
	})
	return records, MapProviderError(err)
}

func (idx ProviderTxIndex) Count(ctx context.Context, r *gestalt.KeyRange, values ...any) (int64, error) {
	count, err := idx.tx.IndexCount(ctx, gestalt.IndexedDBIndexQueryRequest{
		Store:  idx.store,
		Index:  idx.index,
		Values: values,
		Range:  r,
	})
	return count, MapProviderError(err)
}

func (idx ProviderTxIndex) GetAllKeys(_ context.Context, _ *gestalt.KeyRange, _ ...any) ([]string, error) {
	return nil, indexeddb.ErrUnsupported
}

func (idx ProviderTxIndex) Delete(ctx context.Context, values ...any) (int64, error) {
	deleted, err := idx.tx.IndexDelete(ctx, gestalt.IndexedDBIndexQueryRequest{
		Store:  idx.store,
		Index:  idx.index,
		Values: values,
	})
	return deleted, MapProviderError(err)
}

func (idx ProviderTxIndex) DeleteRange(_ context.Context, _ *gestalt.KeyRange, _ ...any) (int64, error) {
	return 0, indexeddb.ErrUnsupported
}
