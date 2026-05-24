package temporal

import (
	"context"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

type workflowDB interface {
	CreateObjectStore(ctx context.Context, name string, schema gestalt.ObjectStoreSchema) error
	DeleteObjectStore(ctx context.Context, name string) error
	ObjectStore(name string) workflowObjectStore
	Transaction(ctx context.Context, stores []string, mode gestalt.TransactionMode, opts gestalt.TransactionOptions) (workflowTx, error)
	Close() error
}

type workflowObjectStore interface {
	Get(ctx context.Context, id string) (gestalt.Record, error)
	Put(ctx context.Context, record gestalt.Record) error
	Add(ctx context.Context, record gestalt.Record) error
	Delete(ctx context.Context, id string) error
	GetAll(ctx context.Context, r *gestalt.KeyRange) ([]gestalt.Record, error)
	Index(name string) workflowIndex
	OpenCursor(ctx context.Context, r *gestalt.KeyRange, dir gestalt.CursorDirection) (workflowCursor, error)
}

type workflowIndex interface {
	GetAll(ctx context.Context, r *gestalt.KeyRange, values ...any) ([]gestalt.Record, error)
	Delete(ctx context.Context, values ...any) (int64, error)
}

type workflowCursor interface {
	Continue() bool
	Value() (gestalt.Record, error)
	Err() error
	Close() error
}

type workflowTx interface {
	Commit(ctx context.Context) error
	Abort(ctx context.Context) error
	ObjectStore(name string) workflowTxObjectStore
}

type workflowTxObjectStore interface {
	Get(ctx context.Context, id string) (gestalt.Record, error)
	Put(ctx context.Context, record gestalt.Record) error
	Add(ctx context.Context, record gestalt.Record) error
	Delete(ctx context.Context, id string) error
	GetAll(ctx context.Context, r *gestalt.KeyRange) ([]gestalt.Record, error)
	Index(name string) workflowTxIndex
}

type workflowTxIndex interface {
	Delete(ctx context.Context, values ...any) (int64, error)
}

var connectIndexedDB = func() (workflowDB, error) {
	client, err := gestalt.IndexedDB()
	if err != nil {
		return nil, err
	}
	return sdkWorkflowDB{client}, nil
}

type sdkWorkflowDB struct {
	*gestalt.IndexedDBClient
}

func (db sdkWorkflowDB) ObjectStore(name string) workflowObjectStore {
	if db.IndexedDBClient == nil {
		return nil
	}
	return sdkWorkflowStore{db.IndexedDBClient.ObjectStore(name)}
}

func (db sdkWorkflowDB) Transaction(ctx context.Context, stores []string, mode gestalt.TransactionMode, opts gestalt.TransactionOptions) (workflowTx, error) {
	tx, err := db.IndexedDBClient.Transaction(ctx, stores, mode, opts)
	if err != nil {
		return nil, err
	}
	return sdkWorkflowTx{tx}, nil
}

type sdkWorkflowStore struct {
	*gestalt.ObjectStoreClient
}

func (s sdkWorkflowStore) Index(name string) workflowIndex {
	if s.ObjectStoreClient == nil {
		return nil
	}
	return sdkWorkflowIndex{s.ObjectStoreClient.Index(name)}
}

func (s sdkWorkflowStore) OpenCursor(ctx context.Context, r *gestalt.KeyRange, dir gestalt.CursorDirection) (workflowCursor, error) {
	cur, err := s.ObjectStoreClient.OpenCursor(ctx, r, dir)
	if err != nil {
		return nil, err
	}
	return sdkWorkflowCursor{cur}, nil
}

type sdkWorkflowIndex struct {
	*gestalt.IndexClient
}

type sdkWorkflowCursor struct {
	*gestalt.Cursor
}

type sdkWorkflowTx struct {
	*gestalt.Transaction
}

func (tx sdkWorkflowTx) ObjectStore(name string) workflowTxObjectStore {
	if tx.Transaction == nil {
		return nil
	}
	return sdkWorkflowTxStore{tx.Transaction.ObjectStore(name)}
}

type sdkWorkflowTxStore struct {
	*gestalt.TransactionObjectStore
}

func (s sdkWorkflowTxStore) Index(name string) workflowTxIndex {
	if s.TransactionObjectStore == nil {
		return nil
	}
	return sdkWorkflowTxIndex{s.TransactionObjectStore.Index(name)}
}

type sdkWorkflowTxIndex struct {
	*gestalt.TransactionIndex
}
