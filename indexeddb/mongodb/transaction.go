package mongodb

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/valon-technologies/gestalt-providers/indexeddb/internal/txstream"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const mongoCommitRetryTimeout = 120 * time.Second

type indexMetaContextKey struct{}

func contextWithIndexMeta(ctx context.Context, schemas map[string]map[string]indexMeta) context.Context {
	return context.WithValue(ctx, indexMetaContextKey{}, schemas)
}

func (p *Provider) Transaction(stream proto.IndexedDB_TransactionServer) error {
	return txstream.Serve(stream, p.beginTransaction)
}

func (p *Provider) beginTransaction(ctx context.Context, req *proto.BeginTransactionRequest) (txstream.Transaction, error) {
	s, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	s.mu.RLock()
	scope := make(map[string]struct{}, len(req.GetStores()))
	schemas := make(map[string]map[string]indexMeta, len(req.GetStores()))
	for _, store := range req.GetStores() {
		storeSchemas, ok := s.schemas[store]
		if !ok {
			s.mu.RUnlock()
			return nil, status.Errorf(codes.NotFound, "object store not found: %s", store)
		}
		scope[store] = struct{}{}
		schemas[store] = copyMongoIndexMeta(storeSchemas)
	}

	session, err := s.client.StartSession()
	if err != nil {
		s.mu.RUnlock()
		return nil, status.Errorf(codes.Internal, "start session: %v", err)
	}
	if err := session.StartTransaction(); err != nil {
		session.EndSession(ctx)
		s.mu.RUnlock()
		return nil, status.Errorf(codes.Internal, "start transaction: %v", err)
	}

	return &mongoTransaction{
		provider: p,
		session:  session,
		scope:    scope,
		schemas:  schemas,
		unlock: func() {
			s.mu.RUnlock()
		},
	}, nil
}

func copyMongoIndexMeta(in map[string]indexMeta) map[string]indexMeta {
	out := make(map[string]indexMeta, len(in))
	for name, meta := range in {
		out[name] = indexMeta{
			keyPath: append([]string(nil), meta.keyPath...),
			unique:  meta.unique,
		}
	}
	return out
}

type mongoTransaction struct {
	provider *Provider
	session  *mongo.Session
	scope    map[string]struct{}
	schemas  map[string]map[string]indexMeta
	mu       sync.Mutex
	done     bool
	unlock   func()
}

func (t *mongoTransaction) Commit(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return status.Error(codes.FailedPrecondition, "transaction is already finished")
	}
	t.done = true
	defer t.unlock()
	defer t.session.EndSession(ctx)
	if err := commitMongoTransaction(ctx, t.session); err != nil {
		return status.Errorf(codes.Internal, "commit transaction: %v", err)
	}
	return nil
}

func (t *mongoTransaction) Abort(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return status.Error(codes.FailedPrecondition, "transaction is already finished")
	}
	t.done = true
	defer t.unlock()
	defer t.session.EndSession(ctx)
	if err := t.session.AbortTransaction(ctx); err != nil {
		return status.Errorf(codes.Internal, "abort transaction: %v", err)
	}
	return nil
}

func (t *mongoTransaction) txContext(ctx context.Context) context.Context {
	return contextWithIndexMeta(mongo.NewSessionContext(ctx, t.session), t.schemas)
}

func (t *mongoTransaction) requireStore(name string) error {
	if _, ok := t.scope[name]; !ok {
		return status.Errorf(codes.InvalidArgument, "invalid transaction: object store not in scope: %s", name)
	}
	return nil
}

func (t *mongoTransaction) Get(ctx context.Context, req *proto.ObjectStoreRequest) (*proto.RecordResponse, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.provider.Get(t.txContext(ctx), req)
}

func (t *mongoTransaction) GetKey(ctx context.Context, req *proto.ObjectStoreRequest) (*proto.KeyResponse, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.provider.GetKey(t.txContext(ctx), req)
}

func (t *mongoTransaction) Add(ctx context.Context, req *proto.RecordRequest) (*emptypb.Empty, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.provider.Add(t.txContext(ctx), req)
}

func (t *mongoTransaction) Put(ctx context.Context, req *proto.RecordRequest) (*emptypb.Empty, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.provider.Put(t.txContext(ctx), req)
}

func (t *mongoTransaction) Delete(ctx context.Context, req *proto.ObjectStoreRequest) (*emptypb.Empty, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.provider.Delete(t.txContext(ctx), req)
}

func (t *mongoTransaction) Clear(ctx context.Context, req *proto.ObjectStoreNameRequest) (*emptypb.Empty, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.provider.Clear(t.txContext(ctx), req)
}

func (t *mongoTransaction) GetAll(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.RecordsResponse, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.provider.GetAll(t.txContext(ctx), req)
}

func (t *mongoTransaction) GetAllKeys(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.KeysResponse, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.provider.GetAllKeys(t.txContext(ctx), req)
}

func (t *mongoTransaction) Count(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.CountResponse, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.provider.Count(t.txContext(ctx), req)
}

func (t *mongoTransaction) DeleteRange(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.DeleteResponse, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.provider.DeleteRange(t.txContext(ctx), req)
}

func (t *mongoTransaction) IndexGet(ctx context.Context, req *proto.IndexQueryRequest) (*proto.RecordResponse, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.provider.IndexGet(t.txContext(ctx), req)
}

func (t *mongoTransaction) IndexGetKey(ctx context.Context, req *proto.IndexQueryRequest) (*proto.KeyResponse, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.provider.IndexGetKey(t.txContext(ctx), req)
}

func (t *mongoTransaction) IndexGetAll(ctx context.Context, req *proto.IndexQueryRequest) (*proto.RecordsResponse, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.provider.IndexGetAll(t.txContext(ctx), req)
}

func (t *mongoTransaction) IndexGetAllKeys(ctx context.Context, req *proto.IndexQueryRequest) (*proto.KeysResponse, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.provider.IndexGetAllKeys(t.txContext(ctx), req)
}

func (t *mongoTransaction) IndexCount(ctx context.Context, req *proto.IndexQueryRequest) (*proto.CountResponse, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.provider.IndexCount(t.txContext(ctx), req)
}

func (t *mongoTransaction) IndexDelete(ctx context.Context, req *proto.IndexQueryRequest) (*proto.DeleteResponse, error) {
	if err := t.requireStore(req.GetStore()); err != nil {
		return nil, err
	}
	return t.provider.IndexDelete(t.txContext(ctx), req)
}

func getMongoIndexMetaForContext(ctx context.Context, s *Store, store, index string) (indexMeta, error) {
	if schemas, ok := ctx.Value(indexMetaContextKey{}).(map[string]map[string]indexMeta); ok {
		storeSchemas, ok := schemas[store]
		if !ok {
			return indexMeta{}, status.Errorf(codes.NotFound, "object store %q has no registered schema", store)
		}
		meta, ok := storeSchemas[index]
		if !ok {
			return indexMeta{}, status.Errorf(codes.NotFound, "index %q not found on store %q", index, store)
		}
		return meta, nil
	}
	return s.getIndexMeta(store, index)
}

func commitMongoTransaction(ctx context.Context, session *mongo.Session) error {
	timer := time.NewTimer(mongoCommitRetryTimeout)
	defer timer.Stop()
	for {
		err := session.CommitTransaction(ctx)
		if err == nil {
			return nil
		}
		if !mongoHasErrorLabel(err, "UnknownTransactionCommitResult") || mongoIsMaxTimeMSExpired(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return err
		default:
		}
	}
}

func mongoHasErrorLabel(err error, label string) bool {
	var labeled mongo.LabeledError
	return errors.As(err, &labeled) && labeled.HasErrorLabel(label)
}

func mongoIsMaxTimeMSExpired(err error) bool {
	var cmd mongo.CommandError
	return errors.As(err, &cmd) && cmd.IsMaxTimeMSExpiredError()
}
