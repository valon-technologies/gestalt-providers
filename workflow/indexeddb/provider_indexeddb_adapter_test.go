package indexeddb

import (
	"context"
	"strings"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func providerIndexedDBErr(err error) error {
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

type providerWorkflowDB struct {
	provider gestalt.IndexedDBProvider
}

func (db providerWorkflowDB) CreateObjectStore(ctx context.Context, name string, schema gestalt.ObjectStoreSchema) error {
	return providerIndexedDBErr(db.provider.CreateObjectStore(ctx, name, schema))
}

func (db providerWorkflowDB) DeleteObjectStore(ctx context.Context, name string) error {
	return providerIndexedDBErr(db.provider.DeleteObjectStore(ctx, name))
}

func (db providerWorkflowDB) ObjectStore(name string) workflowObjectStore {
	return providerWorkflowStore{provider: db.provider, store: name}
}

func (db providerWorkflowDB) Transaction(ctx context.Context, stores []string, mode gestalt.TransactionMode, opts gestalt.TransactionOptions) (workflowTx, error) {
	tx, err := db.provider.BeginTransaction(ctx, gestalt.IndexedDBBeginTransactionRequest{
		Stores:         stores,
		Mode:           mode,
		DurabilityHint: opts.DurabilityHint,
	})
	if err != nil {
		return nil, providerIndexedDBErr(err)
	}
	return providerWorkflowTx{tx: tx}, nil
}

func (db providerWorkflowDB) Close() error { return nil }

type providerWorkflowStore struct {
	provider gestalt.IndexedDBProvider
	store    string
}

func (s providerWorkflowStore) Get(ctx context.Context, id string) (gestalt.Record, error) {
	record, err := s.provider.Get(ctx, gestalt.IndexedDBObjectStoreRequest{Store: s.store, ID: id})
	return record, providerIndexedDBErr(err)
}

func (s providerWorkflowStore) Put(ctx context.Context, record gestalt.Record) error {
	return providerIndexedDBErr(s.provider.Put(ctx, gestalt.IndexedDBRecordRequest{Store: s.store, Record: record}))
}

func (s providerWorkflowStore) Add(ctx context.Context, record gestalt.Record) error {
	return providerIndexedDBErr(s.provider.Add(ctx, gestalt.IndexedDBRecordRequest{Store: s.store, Record: record}))
}

func (s providerWorkflowStore) Delete(ctx context.Context, id string) error {
	return providerIndexedDBErr(s.provider.Delete(ctx, gestalt.IndexedDBObjectStoreRequest{Store: s.store, ID: id}))
}

func (s providerWorkflowStore) GetAll(ctx context.Context, r *gestalt.KeyRange) ([]gestalt.Record, error) {
	records, err := s.provider.GetAll(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: s.store, Range: r})
	return records, providerIndexedDBErr(err)
}

func (s providerWorkflowStore) Count(ctx context.Context, r *gestalt.KeyRange) (int64, error) {
	count, err := s.provider.Count(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: s.store, Range: r})
	return count, providerIndexedDBErr(err)
}

func (s providerWorkflowStore) Index(name string) workflowIndex {
	return providerWorkflowIndex{provider: s.provider, store: s.store, index: name}
}

func (s providerWorkflowStore) OpenCursor(ctx context.Context, r *gestalt.KeyRange, dir gestalt.CursorDirection) (workflowCursor, error) {
	cur, err := s.provider.OpenCursor(ctx, gestalt.IndexedDBOpenCursorRequest{
		Store:     s.store,
		Range:     r,
		Direction: dir,
	})
	if err != nil {
		return nil, providerIndexedDBErr(err)
	}
	return &providerWorkflowCursor{ctx: ctx, cur: cur}, nil
}

func (s providerWorkflowStore) OpenKeyCursor(ctx context.Context, r *gestalt.KeyRange, dir gestalt.CursorDirection) (workflowCursor, error) {
	cur, err := s.provider.OpenCursor(ctx, gestalt.IndexedDBOpenCursorRequest{
		Store:     s.store,
		Range:     r,
		Direction: dir,
		KeysOnly:  true,
	})
	if err != nil {
		return nil, providerIndexedDBErr(err)
	}
	return &providerWorkflowCursor{ctx: ctx, cur: cur}, nil
}

type providerWorkflowIndex struct {
	provider gestalt.IndexedDBProvider
	store    string
	index    string
}

func (idx providerWorkflowIndex) GetAll(ctx context.Context, r *gestalt.KeyRange, values ...any) ([]gestalt.Record, error) {
	records, err := idx.provider.IndexGetAll(ctx, gestalt.IndexedDBIndexQueryRequest{
		Store:  idx.store,
		Index:  idx.index,
		Values: values,
		Range:  r,
	})
	return records, providerIndexedDBErr(err)
}

func (idx providerWorkflowIndex) Count(ctx context.Context, r *gestalt.KeyRange, values ...any) (int64, error) {
	count, err := idx.provider.IndexCount(ctx, gestalt.IndexedDBIndexQueryRequest{
		Store:  idx.store,
		Index:  idx.index,
		Values: values,
		Range:  r,
	})
	return count, providerIndexedDBErr(err)
}

func (idx providerWorkflowIndex) OpenKeyCursor(ctx context.Context, r *gestalt.KeyRange, dir gestalt.CursorDirection, values ...any) (workflowCursor, error) {
	cur, err := idx.provider.OpenCursor(ctx, gestalt.IndexedDBOpenCursorRequest{
		Store:     idx.store,
		Index:     idx.index,
		Range:     r,
		Direction: dir,
		KeysOnly:  true,
		Values:    values,
	})
	if err != nil {
		return nil, providerIndexedDBErr(err)
	}
	return &providerWorkflowCursor{ctx: ctx, cur: cur}, nil
}

type providerWorkflowCursor struct {
	ctx   context.Context
	cur   gestalt.IndexedDBCursor
	entry *gestalt.IndexedDBCursorEntry
	err   error
	done  bool
}

func (c *providerWorkflowCursor) Continue() bool {
	if c.done || c.err != nil {
		return false
	}
	entry, err := c.cur.Next(c.ctx)
	if err != nil {
		c.err = providerIndexedDBErr(err)
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

func (c *providerWorkflowCursor) PrimaryKey() string {
	if c.entry == nil {
		return ""
	}
	return c.entry.PrimaryKey
}

func (c *providerWorkflowCursor) Value() (gestalt.Record, error) {
	if c.err != nil {
		return nil, c.err
	}
	if c.done || c.entry == nil {
		return nil, gestalt.ErrNotFound
	}
	return c.entry.Record, nil
}

func (c *providerWorkflowCursor) Err() error { return c.err }

func (c *providerWorkflowCursor) Close() error { return c.cur.Close() }

type providerWorkflowTx struct {
	tx gestalt.IndexedDBTransaction
}

func (tx providerWorkflowTx) Commit(ctx context.Context) error {
	return providerIndexedDBErr(tx.tx.Commit(ctx))
}

func (tx providerWorkflowTx) Abort(ctx context.Context) error {
	return providerIndexedDBErr(tx.tx.Abort(ctx))
}

func (tx providerWorkflowTx) ObjectStore(name string) workflowTxObjectStore {
	return providerWorkflowTxStore{tx: tx.tx, store: name}
}

type providerWorkflowTxStore struct {
	tx    gestalt.IndexedDBTransaction
	store string
}

func (s providerWorkflowTxStore) Get(ctx context.Context, id string) (gestalt.Record, error) {
	record, err := s.tx.Get(ctx, gestalt.IndexedDBObjectStoreRequest{Store: s.store, ID: id})
	return record, providerIndexedDBErr(err)
}

func (s providerWorkflowTxStore) Put(ctx context.Context, record gestalt.Record) error {
	return providerIndexedDBErr(s.tx.Put(ctx, gestalt.IndexedDBRecordRequest{Store: s.store, Record: record}))
}

func (s providerWorkflowTxStore) Add(ctx context.Context, record gestalt.Record) error {
	return providerIndexedDBErr(s.tx.Add(ctx, gestalt.IndexedDBRecordRequest{Store: s.store, Record: record}))
}

func (s providerWorkflowTxStore) Delete(ctx context.Context, id string) error {
	return providerIndexedDBErr(s.tx.Delete(ctx, gestalt.IndexedDBObjectStoreRequest{Store: s.store, ID: id}))
}

func (s providerWorkflowTxStore) GetAll(ctx context.Context, r *gestalt.KeyRange) ([]gestalt.Record, error) {
	records, err := s.tx.GetAll(ctx, gestalt.IndexedDBObjectStoreRangeRequest{Store: s.store, Range: r})
	return records, providerIndexedDBErr(err)
}

func (s providerWorkflowTxStore) Index(name string) workflowTxIndex {
	return providerWorkflowTxIndex{tx: s.tx, store: s.store, index: name}
}

type providerWorkflowTxIndex struct {
	tx    gestalt.IndexedDBTransaction
	store string
	index string
}

func (idx providerWorkflowTxIndex) GetAll(ctx context.Context, r *gestalt.KeyRange, values ...any) ([]gestalt.Record, error) {
	records, err := idx.tx.IndexGetAll(ctx, gestalt.IndexedDBIndexQueryRequest{
		Store:  idx.store,
		Index:  idx.index,
		Values: values,
		Range:  r,
	})
	return records, providerIndexedDBErr(err)
}

func (idx providerWorkflowTxIndex) Count(ctx context.Context, r *gestalt.KeyRange, values ...any) (int64, error) {
	count, err := idx.tx.IndexCount(ctx, gestalt.IndexedDBIndexQueryRequest{
		Store:  idx.store,
		Index:  idx.index,
		Values: values,
		Range:  r,
	})
	return count, providerIndexedDBErr(err)
}

func (idx providerWorkflowTxIndex) Delete(ctx context.Context, values ...any) (int64, error) {
	deleted, err := idx.tx.IndexDelete(ctx, gestalt.IndexedDBIndexQueryRequest{
		Store:  idx.store,
		Index:  idx.index,
		Values: values,
	})
	return deleted, providerIndexedDBErr(err)
}
