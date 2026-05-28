package indexeddb

import (
	"context"
	"fmt"
	"strings"
	"sync"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Provider struct {
	mu sync.Mutex
	db indexeddb.Database
}

type openIndexedDBFunc func(context.Context, ...string) (indexeddb.Database, error)

func New() *Provider {
	return &Provider{}
}

func (p *Provider) Configure(ctx context.Context, _ string, raw map[string]any) error {
	return configure(ctx, raw, gestalt.IndexedDB, p)
}

func configure(ctx context.Context, raw map[string]any, openIndexedDB openIndexedDBFunc, provider *Provider) error {
	cfg, err := decodeConfig(raw)
	if err != nil {
		return newAuthorizationProviderError(err)
	}
	if provider == nil {
		return newAuthorizationProviderError(fmt.Errorf("provider is required"))
	}
	stores := getStoreNames()

	var db indexeddb.Database
	if cfg.IndexedDB != "" {
		db, err = openIndexedDB(ctx, cfg.IndexedDB)
	} else {
		db, err = openIndexedDB(ctx)
	}
	if err != nil {
		return newAuthorizationProviderError(fmt.Errorf("connect indexeddb: %w", err))
	}

	if err := ensureAuthorizationStores(ctx, db, stores); err != nil {
		_ = db.Close()
		return newAuthorizationProviderError(err)
	}

	provider.configureDatabase(db)
	return nil
}

func (p *Provider) configureDatabase(db indexeddb.Database) {
	p.mu.Lock()
	oldDB := p.db
	p.db = db
	p.mu.Unlock()

	if oldDB != nil {
		_ = oldDB.Close()
	}
}

func (p *Provider) getDbWithLock() (indexeddb.Database, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.db == nil {
		return nil, fmt.Errorf("provider is not configured")
	}
	return p.db, nil
}

func (p *Provider) GetActiveModelRef(ctx context.Context, _ *emptypb.Empty) (*GetActiveModelRefResponse, error) {
	db, err := p.getDbWithLock()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	stores := getStoreNames()
	keys := getStateKeys()

	ref, err := getActiveModelRef(ctx, db.ObjectStore(stores.state), keys.activeModel)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get active model ref: %v", err)
	}
	if ref == nil {
		return nil, status.Error(codes.NotFound, "active model is not set")
	}

	return &GetActiveModelRefResponse{Model: cloneAuthorizationModelRef(ref)}, nil
}

func (p *Provider) SetActiveModel(ctx context.Context, req *SetActiveModelRequest) (*SetActiveModelResponse, error) {
	if req == nil || req.Model == nil {
		return nil, status.Error(codes.InvalidArgument, "model is required")
	}
	model := cloneAuthorizationModel(req.Model)
	model.Id = strings.TrimSpace(model.Id)
	if model.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "model id is required")
	}

	db, err := p.getDbWithLock()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	stores := getStoreNames()
	keys := getStateKeys()
	if err := putModel(ctx, db.ObjectStore(stores.models), model); err != nil {
		return nil, status.Errorf(codes.Internal, "set active model: %v", err)
	}
	ref := model.toRef()
	if err := putActiveModelRef(ctx, db.ObjectStore(stores.state), keys.activeModel, ref); err != nil {
		return nil, status.Errorf(codes.Internal, "set active model state: %v", err)
	}

	return &SetActiveModelResponse{Model: cloneAuthorizationModelRef(ref)}, nil
}

func (p *Provider) ListActiveModelResourceTypes(ctx context.Context, req *ListActiveModelResourceTypesRequest) (*ListActiveModelResourceTypesResponse, error) {
	db, err := p.getDbWithLock()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	stores := getStoreNames()
	keys := getStateKeys()

	modelID := ""
	if req != nil {
		modelID = strings.TrimSpace(req.ModelID)
	}
	if modelID == "" {
		ref, err := getActiveModelRef(ctx, db.ObjectStore(stores.state), keys.activeModel)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "get active model: %v", err)
		}
		if ref == nil {
			return nil, status.Error(codes.NotFound, "active model is not set")
		}
		modelID = ref.Id
	}

	model, err := getModel(ctx, db.ObjectStore(stores.models), modelID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get model %q: %v", modelID, err)
	}
	if model == nil {
		return nil, status.Errorf(codes.NotFound, "model %q not found", modelID)
	}

	return &ListActiveModelResourceTypesResponse{
		ResourceTypes: cloneAuthorizationModelResourceTypes(model.ResourceTypes),
	}, nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindAuthorization,
		Name:        "indexeddb",
		DisplayName: "IndexedDB Authorization",
		Description: "Stub authorization provider.",
		Version:     "0.0.1-alpha.2",
	}
}

func (p *Provider) HealthCheck(context.Context) error {
	return nil
}

func (p *Provider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.db == nil {
		return nil
	}
	err := p.db.Close()
	p.db = nil
	return err
}

var _ AuthorizationProvider = (*Provider)(nil)
var _ gestalt.MetadataProvider = (*Provider)(nil)
var _ gestalt.HealthChecker = (*Provider)(nil)
var _ gestalt.Closer = (*Provider)(nil)
