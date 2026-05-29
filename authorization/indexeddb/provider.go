package indexeddb

import (
	"context"
	"fmt"
	"sort"
	"strconv"
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

func (p *Provider) SetRelationships(ctx context.Context, req *SetRelationshipsRequest) (*SetRelationshipsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	relationships := make([]*Relationship, 0, len(req.Relationships))
	records := make([]indexeddb.Record, 0, len(req.Relationships))
	seenIDs := make(map[string]struct{}, len(req.Relationships))
	for _, relationship := range req.Relationships {
		cloned := cloneRelationship(relationship)
		if err := normalizeRelationship(cloned); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "relationship is invalid: %v", err)
		}
		record, err := relationshipToRecord(cloned)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "relationship is invalid: %v", err)
		}
		id := stringField(record, "id")
		if _, ok := seenIDs[id]; ok {
			continue
		}
		seenIDs[id] = struct{}{}
		relationships = append(relationships, cloned)
		records = append(records, record)
	}

	db, err := p.getDbWithLock()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	stores := getStoreNames()
	tx, err := db.Transaction(ctx, []string{stores.relationships}, indexeddb.TransactionReadwrite, indexeddb.TransactionOptions{})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "start relationships transaction: %v", err)
	}
	defer func() {
		_ = tx.Abort(ctx)
	}()

	store := tx.ObjectStore(stores.relationships)
	if err := store.Clear(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "clear relationships: %v", err)
	}
	for _, record := range records {
		if err := store.Put(ctx, record); err != nil {
			return nil, status.Errorf(codes.Internal, "set relationship: %v", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "commit relationships: %v", err)
	}

	return &SetRelationshipsResponse{Relationships: cloneRelationships(relationships)}, nil
}

func (p *Provider) ListRelationships(ctx context.Context, req *ListRelationshipsRequest) (*ListRelationshipsResponse, error) {
	db, err := p.getDbWithLock()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	records, err := db.ObjectStore(getStoreNames().relationships).GetAll(ctx, nil)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list relationships: %v", err)
	}
	sort.Slice(records, func(i, j int) bool {
		return stringField(records[i], "id") < stringField(records[j], "id")
	})

	filter := (*RelationshipFilter)(nil)
	pageSize := int32(defaultRelationshipPageSize)
	pageToken := ""
	if req != nil {
		filter = req.Filter
		if req.PageSize < 0 {
			return nil, status.Error(codes.InvalidArgument, "page size must be non-negative")
		}
		if req.PageSize > 0 {
			pageSize = req.PageSize
		}
		pageToken = strings.TrimSpace(req.PageToken)
	}

	offset, err := parseRelationshipPageToken(pageToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "page token is invalid: %v", err)
	}

	matches := make([]*Relationship, 0, len(records))
	for _, record := range records {
		relationship, err := relationshipFromRecord(record)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "decode relationship: %v", err)
		}
		if relationshipMatchesFilter(relationship, filter) {
			matches = append(matches, relationship)
		}
	}
	if offset > len(matches) {
		return nil, status.Error(codes.InvalidArgument, "page token is out of range")
	}

	limit := int(pageSize)
	if limit == 0 {
		limit = len(matches)
	}
	end := offset + limit
	if end > len(matches) {
		end = len(matches)
	}
	nextPageToken := ""
	if end < len(matches) {
		nextPageToken = strconv.Itoa(end)
	}

	return &ListRelationshipsResponse{
		Relationships: cloneRelationships(matches[offset:end]),
		NextPageToken: nextPageToken,
	}, nil
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
