package indexeddb

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	"github.com/valon-technologies/gestalt/sdk/go/migrations"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func (p *Provider) MigrationOptions(_ context.Context, _ string, raw map[string]any) (migrations.RunOptions, string, error) {
	cfg, err := decodeConfig(raw)
	if err != nil {
		return migrations.RunOptions{}, "", err
	}
	return migrations.RunOptions{Revisions: authorizationMigrations()}, cfg.IndexedDB, nil
}

func configure(ctx context.Context, raw map[string]any, openIndexedDB openIndexedDBFunc, provider *Provider) error {
	cfg, err := decodeConfig(raw)
	if err != nil {
		return newAuthorizationProviderError(err)
	}
	if provider == nil {
		return newAuthorizationProviderError(fmt.Errorf("provider is required"))
	}

	var db indexeddb.Database
	if cfg.IndexedDB != "" {
		db, err = openIndexedDB(ctx, cfg.IndexedDB)
	} else {
		db, err = openIndexedDB(ctx)
	}
	if err != nil {
		return newAuthorizationProviderError(fmt.Errorf("connect indexeddb: %w", err))
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

func (p *Provider) CheckAccess(ctx context.Context, req *CheckAccessRequest) (*CheckAccessResponse, error) {
	snapshot, err := p.loadAuthorizationSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return evaluateAccess(snapshot, req)
}

func (p *Provider) CheckAccessMany(ctx context.Context, req *CheckAccessManyRequest) (*CheckAccessManyResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	snapshot, err := p.loadAuthorizationSnapshot(ctx)
	if err != nil {
		return nil, err
	}

	decisions := make([]*CheckAccessResponse, 0, len(req.Requests))
	for _, check := range req.Requests {
		decision, err := evaluateAccess(snapshot, check)
		if err != nil {
			return nil, err
		}
		decisions = append(decisions, decision)
	}

	return &CheckAccessManyResponse{Decisions: decisions}, nil
}

func (p *Provider) AddRelationship(ctx context.Context, req *AddRelationshipRequest) (*AddRelationshipResponse, error) {
	if req == nil || req.Relationship == nil {
		return nil, status.Error(codes.InvalidArgument, "relationship is required")
	}
	relationship := cloneRelationship(req.Relationship)
	if err := normalizeRelationship(relationship); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "relationship is invalid: %v", err)
	}
	record, err := relationshipToRecord(relationship)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "relationship is invalid: %v", err)
	}

	db, err := p.getDbWithLock()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if err := db.ObjectStore(getStoreNames().relationships).Put(ctx, record); err != nil {
		return nil, status.Errorf(codes.Internal, "add relationship: %v", err)
	}

	return &AddRelationshipResponse{Relationship: cloneRelationship(relationship)}, nil
}

func (p *Provider) DeleteRelationship(ctx context.Context, req *DeleteRelationshipRequest) (*DeleteRelationshipResponse, error) {
	if req == nil || req.RelationshipTuple == nil {
		return nil, status.Error(codes.InvalidArgument, "relationship tuple is required")
	}
	relationship := &Relationship{Tuple: cloneRelationshipTuple(req.RelationshipTuple)}
	if err := normalizeRelationship(relationship); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "relationship tuple is invalid: %v", err)
	}

	db, err := p.getDbWithLock()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if err := db.ObjectStore(getStoreNames().relationships).Delete(ctx, relationshipID(relationship.Tuple)); err != nil {
		return nil, status.Errorf(codes.Internal, "delete relationship: %v", err)
	}

	return &DeleteRelationshipResponse{}, nil
}

func (p *Provider) SetAuthorizationState(ctx context.Context, req *SetAuthorizationStateRequest) (*SetAuthorizationStateResponse, error) {
	if req == nil || req.Model == nil {
		return nil, status.Error(codes.InvalidArgument, "model is required")
	}
	model := cloneAuthorizationModel(req.Model)
	if err := normalizeAuthorizationModel(model); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "model is invalid: %v", err)
	}
	modelRecord, err := modelToRecord(model)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "model is invalid: %v", err)
	}
	_, relationshipRecords, err := normalizeRelationshipRecords(req.Relationships)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ref := authorizationModelToRef(model, time.Now().UTC())
	refRecord, err := modelRefToRecord(getStateKeys().activeModel, ref)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "active model ref is invalid: %v", err)
	}

	db, err := p.getDbWithLock()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	stores := getStoreNames()
	tx, err := db.Transaction(ctx, stores.all(), indexeddb.TransactionReadwrite, indexeddb.TransactionOptions{})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "start authorization state transaction: %v", err)
	}
	defer func() {
		_ = tx.Abort(ctx)
	}()

	modelStore := tx.ObjectStore(stores.models)
	stateStore := tx.ObjectStore(stores.state)
	relationshipStore := tx.ObjectStore(stores.relationships)

	if err := modelStore.Put(ctx, modelRecord); err != nil {
		return nil, status.Errorf(codes.Internal, "set authorization model: %v", err)
	}
	if err := stateStore.Put(ctx, refRecord); err != nil {
		return nil, status.Errorf(codes.Internal, "set active model state: %v", err)
	}
	if err := relationshipStore.Clear(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "clear relationships: %v", err)
	}
	for _, record := range relationshipRecords {
		if err := relationshipStore.Put(ctx, record); err != nil {
			return nil, status.Errorf(codes.Internal, "set relationship: %v", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "commit authorization state: %v", err)
	}

	return &SetAuthorizationStateResponse{ActiveModel: cloneAuthorizationModelRef(ref)}, nil
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

func (p *Provider) GetActiveModelRef(ctx context.Context) (*GetActiveModelRefResponse, error) {
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
	if err := normalizeAuthorizationModel(model); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "model is invalid: %v", err)
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
	ref := authorizationModelToRef(model, time.Now().UTC())
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

	var filter *AuthorizationModelResourceTypeFilter
	pageSize := int32(defaultModelResourceTypePageSize)
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

	ref, err := getActiveModelRef(ctx, db.ObjectStore(stores.state), keys.activeModel)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get active model: %v", err)
	}
	if ref == nil {
		return nil, status.Error(codes.NotFound, "active model is not set")
	}
	modelID := ref.Id

	model, err := getModel(ctx, db.ObjectStore(stores.models), modelID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get model %q: %v", modelID, err)
	}
	if model == nil {
		return nil, status.Errorf(codes.NotFound, "model %q not found", modelID)
	}

	offset, err := parseModelResourceTypePageToken(pageToken)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "page token is invalid: %v", err)
	}
	resourceTypes := filterAuthorizationModelResourceTypes(model.ResourceTypes, filter)
	if offset > len(resourceTypes) {
		return nil, status.Error(codes.InvalidArgument, "page token is out of range")
	}

	limit := int(pageSize)
	if limit == 0 {
		limit = len(resourceTypes)
	}
	end := offset + limit
	if end > len(resourceTypes) {
		end = len(resourceTypes)
	}
	nextPageToken := ""
	if end < len(resourceTypes) {
		nextPageToken = strconv.Itoa(end)
	}

	return &ListActiveModelResourceTypesResponse{
		ResourceTypes: resourceTypes[offset:end],
		NextPageToken: nextPageToken,
		ModelId:       modelID,
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

var _ gestalt.AuthorizationProvider = (*Provider)(nil)
var _ gestalt.MetadataProvider = (*Provider)(nil)
var _ gestalt.HealthChecker = (*Provider)(nil)
var _ gestalt.Closer = (*Provider)(nil)
