package relationaldb

import (
	"context"
	"database/sql"
	"sync"

	"github.com/valon-technologies/gestalt-providers/indexeddb/internal/txstream"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Store) Transaction(stream proto.IndexedDB_TransactionServer) error {
	return txstream.Serve(stream, s.beginTransaction)
}

func (s *Store) beginTransaction(ctx context.Context, req *proto.BeginTransactionRequest) (txstream.Transaction, error) {
	s.mu.RLock()

	scope := make(map[string]struct{}, len(req.GetStores()))
	meta := make(map[string]*storeMeta, len(req.GetStores()))
	for _, store := range req.GetStores() {
		if _, ok := scope[store]; !ok {
			storeMeta, found, err := s.loadStoreMetadata(ctx, store)
			if err != nil {
				s.mu.RUnlock()
				return nil, status.Errorf(codes.Internal, "load metadata for %q: %v", store, err)
			}
			if !found {
				s.mu.RUnlock()
				return nil, status.Errorf(codes.NotFound, "object store not found: %s", store)
			}
			meta[store] = storeMeta
		}
		scope[store] = struct{}{}
	}

	sqlTx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.mu.RUnlock()
		return nil, status.Errorf(codes.Internal, "begin transaction: %v", err)
	}

	return &relationalTransaction{
		store: s,
		tx:    sqlTx,
		scope: scope,
		meta:  meta,
		unlock: func() {
			s.mu.RUnlock()
		},
	}, nil
}

type relationalTransaction struct {
	store  *Store
	tx     *sql.Tx
	scope  map[string]struct{}
	meta   map[string]*storeMeta
	mu     sync.Mutex
	done   bool
	unlock func()
}

func (t *relationalTransaction) Commit(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return status.Error(codes.FailedPrecondition, "transaction is already finished")
	}
	t.done = true
	defer t.unlock()
	if err := t.tx.Commit(); err != nil {
		return status.Errorf(codes.Internal, "commit transaction: %v", err)
	}
	return nil
}

func (t *relationalTransaction) Abort(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return status.Error(codes.FailedPrecondition, "transaction is already finished")
	}
	t.done = true
	defer t.unlock()
	if err := t.tx.Rollback(); err != nil && err != sql.ErrTxDone {
		return status.Errorf(codes.Internal, "abort transaction: %v", err)
	}
	return nil
}

func (t *relationalTransaction) txContext(ctx context.Context) context.Context {
	return contextWithTx(ctx, t.tx, t.meta)
}

func (t *relationalTransaction) requireStore(name string) error {
	if _, ok := t.scope[name]; !ok {
		return status.Errorf(codes.InvalidArgument, "invalid transaction: object store not in scope: %s", name)
	}
	return nil
}

func (t *relationalTransaction) Get(ctx context.Context, req *proto.ObjectStoreRequest) (*proto.RecordResponse, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.store.Get(t.txContext(ctx), req)
}

func (t *relationalTransaction) GetKey(ctx context.Context, req *proto.ObjectStoreRequest) (*proto.KeyResponse, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.store.GetKey(t.txContext(ctx), req)
}

func (t *relationalTransaction) Add(ctx context.Context, req *proto.RecordRequest) (*gestalt.Empty, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.store.Add(t.txContext(ctx), req)
}

func (t *relationalTransaction) Put(ctx context.Context, req *proto.RecordRequest) (*gestalt.Empty, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.store.Put(t.txContext(ctx), req)
}

func (t *relationalTransaction) Delete(ctx context.Context, req *proto.ObjectStoreRequest) (*gestalt.Empty, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.store.Delete(t.txContext(ctx), req)
}

func (t *relationalTransaction) Clear(ctx context.Context, req *proto.ObjectStoreNameRequest) (*gestalt.Empty, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.store.Clear(t.txContext(ctx), req)
}

func (t *relationalTransaction) GetAll(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.RecordsResponse, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.store.GetAll(t.txContext(ctx), req)
}

func (t *relationalTransaction) GetAllKeys(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.KeysResponse, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.store.GetAllKeys(t.txContext(ctx), req)
}

func (t *relationalTransaction) Count(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.CountResponse, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.store.Count(t.txContext(ctx), req)
}

func (t *relationalTransaction) DeleteRange(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.DeleteResponse, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.store.DeleteRange(t.txContext(ctx), req)
}

func (t *relationalTransaction) IndexGet(ctx context.Context, req *proto.IndexQueryRequest) (*proto.RecordResponse, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.store.IndexGet(t.txContext(ctx), req)
}

func (t *relationalTransaction) IndexGetKey(ctx context.Context, req *proto.IndexQueryRequest) (*proto.KeyResponse, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.store.IndexGetKey(t.txContext(ctx), req)
}

func (t *relationalTransaction) IndexGetAll(ctx context.Context, req *proto.IndexQueryRequest) (*proto.RecordsResponse, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.store.IndexGetAll(t.txContext(ctx), req)
}

func (t *relationalTransaction) IndexGetAllKeys(ctx context.Context, req *proto.IndexQueryRequest) (*proto.KeysResponse, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.store.IndexGetAllKeys(t.txContext(ctx), req)
}

func (t *relationalTransaction) IndexCount(ctx context.Context, req *proto.IndexQueryRequest) (*proto.CountResponse, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.store.IndexCount(t.txContext(ctx), req)
}

func (t *relationalTransaction) IndexDelete(ctx context.Context, req *proto.IndexQueryRequest) (*proto.DeleteResponse, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.store.IndexDelete(t.txContext(ctx), req)
}
