package relationaldb

import (
	"context"
	"sync"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type relationalDatabase struct {
	factory  *relationalFactory
	store    *Store
	name     string
	version  uint64
	handleID uint64
	closed   bool
}

func (db *relationalDatabase) Name() string { return db.name }

func (db *relationalDatabase) Version() uint64 {
	return db.version
}

func (db *relationalDatabase) ObjectStoreNames(ctx context.Context) ([]string, error) {
	if err := db.ensureOpen(); err != nil {
		return nil, err
	}
	return db.store.ObjectStoreNames(ctx)
}

func (db *relationalDatabase) Transaction(ctx context.Context, stores []string, mode gestalt.TransactionMode, opts gestalt.TransactionOptions) (gestalt.IndexedDBTransaction, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if mode == "" {
		mode = gestalt.TransactionReadonly
	}
	if mode != gestalt.TransactionReadonly && mode != gestalt.TransactionReadwrite {
		return nil, status.Error(codes.InvalidArgument, "invalid transaction mode")
	}
	if len(stores) == 0 {
		return nil, status.Error(codes.InvalidArgument, "transaction scope is required")
	}
	db.factory.mu.Lock()
	if db.closed || db.factory.closed {
		db.factory.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "database is closed")
	}
	state := db.factory.states[db.name]
	if state == nil || state.version == 0 {
		db.factory.mu.Unlock()
		return nil, status.Error(codes.NotFound, "database not found")
	}
	handle := state.handles[db.handleID]
	if handle == nil || handle.closed {
		db.factory.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "database handle is closed")
	}
	if state.phase != dbPhaseIdle {
		db.factory.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "database has a pending version change or delete")
	}
	state.activeTx++
	handle.activeTx++
	db.factory.mu.Unlock()

	if err := db.factory.updateActiveTransactions(ctx, db.handleID, 1); err != nil {
		db.releaseLocalTx()
		return nil, err
	}
	tx, err := db.store.beginTransaction(ctx, gestalt.IndexedDBBeginTransactionRequest{
		Stores:         stores,
		Mode:           mode,
		DurabilityHint: opts.DurabilityHint,
	})
	if err != nil {
		db.releaseTx()
		return nil, err
	}
	return &trackedTransaction{inner: tx, mode: mode, release: db.releaseTx}, nil
}

func (db *relationalDatabase) Close() error {
	db.factory.mu.Lock()
	if db.closed {
		db.factory.mu.Unlock()
		return nil
	}
	unregister := true
	if state := db.factory.states[db.name]; state != nil {
		if handle := state.handles[db.handleID]; handle != nil {
			handle.closed = true
			if handle.activeTx > 0 {
				unregister = false
			} else {
				delete(state.handles, db.handleID)
			}
			db.factory.notifyLocked()
		}
	}
	db.closed = true
	db.factory.mu.Unlock()
	if unregister {
		return db.factory.unregisterConnection(context.Background(), db.handleID)
	}
	return db.factory.closeConnection(context.Background(), db.handleID)
}

func (db *relationalDatabase) ensureOpen() error {
	db.factory.mu.Lock()
	defer db.factory.mu.Unlock()
	if db.closed || db.factory.closed {
		return status.Error(codes.FailedPrecondition, "database is closed")
	}
	if state := db.factory.states[db.name]; state == nil || state.version == 0 {
		return status.Error(codes.NotFound, "database not found")
	}
	return nil
}

func (db *relationalDatabase) releaseTx() {
	_ = db.factory.updateActiveTransactions(context.Background(), db.handleID, -1)
	db.releaseLocalTx()
}

func (db *relationalDatabase) releaseLocalTx() {
	var unregister bool
	var finalizeFactory bool
	db.factory.mu.Lock()
	if state := db.factory.states[db.name]; state != nil && state.activeTx > 0 {
		state.activeTx--
		if handle := state.handles[db.handleID]; handle != nil {
			if handle.activeTx > 0 {
				handle.activeTx--
			}
			if handle.closed && handle.activeTx == 0 {
				delete(state.handles, db.handleID)
				unregister = true
			}
		}
		if db.factory.closed && db.factory.activeTransactionsLocked() == 0 && !db.factory.finalized {
			db.factory.finalized = true
			db.factory.states = make(map[string]*databaseState)
			close(db.factory.stopHeartbeat)
			finalizeFactory = true
		}
		db.factory.notifyLocked()
	}
	db.factory.mu.Unlock()
	if unregister {
		_ = db.factory.unregisterConnection(context.Background(), db.handleID)
	}
	if finalizeFactory {
		_ = db.factory.cleanupInactiveConnections(context.Background())
		if db.factory.ownsDB {
			_ = db.factory.db.Close()
		}
	}
}

type trackedTransaction struct {
	inner    gestalt.IndexedDBTransaction
	mode     gestalt.TransactionMode
	release  func()
	mu       sync.Mutex
	released bool
}

func (t *trackedTransaction) Commit(ctx context.Context) error {
	err := t.inner.Commit(ctx)
	t.releaseOnce()
	return err
}

func (t *trackedTransaction) Abort(ctx context.Context) error {
	err := t.inner.Abort(ctx)
	t.releaseOnce()
	return err
}

func (t *trackedTransaction) releaseOnce() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.released {
		return
	}
	t.released = true
	t.release()
}

func (t *trackedTransaction) ensureWritable() error {
	if t.mode == gestalt.TransactionReadonly {
		return status.Error(codes.FailedPrecondition, "transaction is readonly")
	}
	return nil
}

func (t *trackedTransaction) Get(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) (gestalt.Record, error) {
	return t.inner.Get(ctx, req)
}

func (t *trackedTransaction) GetKey(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) (string, error) {
	return t.inner.GetKey(ctx, req)
}

func (t *trackedTransaction) Add(ctx context.Context, req gestalt.IndexedDBRecordRequest) error {
	if err := t.ensureWritable(); err != nil {
		return err
	}
	return t.inner.Add(ctx, req)
}

func (t *trackedTransaction) Put(ctx context.Context, req gestalt.IndexedDBRecordRequest) error {
	if err := t.ensureWritable(); err != nil {
		return err
	}
	return t.inner.Put(ctx, req)
}

func (t *trackedTransaction) Delete(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) error {
	if err := t.ensureWritable(); err != nil {
		return err
	}
	return t.inner.Delete(ctx, req)
}

func (t *trackedTransaction) Clear(ctx context.Context, store string) error {
	if err := t.ensureWritable(); err != nil {
		return err
	}
	return t.inner.Clear(ctx, store)
}

func (t *trackedTransaction) GetAll(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]gestalt.Record, error) {
	return t.inner.GetAll(ctx, req)
}

func (t *trackedTransaction) GetAllKeys(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]string, error) {
	return t.inner.GetAllKeys(ctx, req)
}

func (t *trackedTransaction) Count(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) (int64, error) {
	return t.inner.Count(ctx, req)
}

func (t *trackedTransaction) DeleteRange(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) (int64, error) {
	if err := t.ensureWritable(); err != nil {
		return 0, err
	}
	return t.inner.DeleteRange(ctx, req)
}

func (t *trackedTransaction) IndexGet(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (gestalt.Record, error) {
	return t.inner.IndexGet(ctx, req)
}

func (t *trackedTransaction) IndexGetKey(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (string, error) {
	return t.inner.IndexGetKey(ctx, req)
}

func (t *trackedTransaction) IndexGetAll(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]gestalt.Record, error) {
	return t.inner.IndexGetAll(ctx, req)
}

func (t *trackedTransaction) IndexGetAllKeys(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]string, error) {
	return t.inner.IndexGetAllKeys(ctx, req)
}

func (t *trackedTransaction) IndexCount(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (int64, error) {
	return t.inner.IndexCount(ctx, req)
}

func (t *trackedTransaction) IndexDelete(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (int64, error) {
	if err := t.ensureWritable(); err != nil {
		return 0, err
	}
	return t.inner.IndexDelete(ctx, req)
}
