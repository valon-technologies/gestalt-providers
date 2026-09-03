package indexeddb

import (
	"context"
	"errors"
	"fmt"
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
	if _, err := p.WriteRelationships(ctx, &WriteRelationshipsRequest{Updates: []*RelationshipUpdate{{
		Operation:    RelationshipUpdateOperationTouch,
		Relationship: relationship,
	}}}); err != nil {
		return nil, err
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

	if _, err := p.WriteRelationships(ctx, &WriteRelationshipsRequest{Updates: []*RelationshipUpdate{{
		Operation:    RelationshipUpdateOperationDelete,
		Relationship: relationship,
	}}}); err != nil {
		return nil, err
	}

	return &DeleteRelationshipResponse{}, nil
}

func (p *Provider) WriteRelationships(ctx context.Context, req *WriteRelationshipsRequest) (*WriteRelationshipsResponse, error) {
	prepared, err := prepareRelationshipWrite(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	db, err := p.getDbWithLock()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	storeName := getStoreNames().relationships
	tx, err := db.Transaction(ctx, []string{storeName}, indexeddb.TransactionReadwrite, indexeddb.TransactionOptions{})
	if err != nil {
		return nil, relationshipStorageError("start relationship transaction", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Abort(ctx)
		}
	}()
	store := tx.ObjectStore(storeName)
	for _, precondition := range prepared.preconditions {
		matches, err := relationshipRecordsForFilter(ctx, store, precondition.filter)
		if err != nil {
			return nil, relationshipStorageError("evaluate relationship precondition", err)
		}
		matched := false
		for _, record := range matches {
			relationship, err := relationshipFromRecord(record)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "decode relationship precondition: %v", err)
			}
			if relationshipMatchesFilter(relationship, precondition.filter) {
				matched = true
				break
			}
		}
		if (precondition.operation == PreconditionOperationMustMatch && !matched) || (precondition.operation == PreconditionOperationMustNotMatch && matched) {
			return nil, status.Error(codes.FailedPrecondition, "relationship precondition failed")
		}
	}
	for _, update := range prepared.updates {
		switch update.operation {
		case RelationshipUpdateOperationCreate:
			_, err := store.Get(ctx, update.id)
			if err != nil && !errors.Is(err, gestalt.ErrNotFound) && !errors.Is(err, indexeddb.ErrNotFound) {
				return nil, relationshipStorageError("read relationship", err)
			}
			if err == nil {
				return nil, status.Error(codes.AlreadyExists, "relationship already exists")
			}
			if err := store.Put(ctx, update.record); err != nil {
				return nil, relationshipStorageError("create relationship", err)
			}
		case RelationshipUpdateOperationTouch:
			if err := store.Put(ctx, update.record); err != nil {
				return nil, relationshipStorageError("touch relationship", err)
			}
		case RelationshipUpdateOperationDelete:
			if update.relationship.SourceLayer == SourceLayerUnspecified {
				if err := store.Delete(ctx, update.id); err != nil && !errors.Is(err, gestalt.ErrNotFound) && !errors.Is(err, indexeddb.ErrNotFound) {
					return nil, relationshipStorageError("delete relationship", err)
				}
				continue
			}
			existing, err := store.Get(ctx, update.id)
			if errors.Is(err, gestalt.ErrNotFound) || errors.Is(err, indexeddb.ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, relationshipStorageError("read relationship", err)
			}
			storedRelationship, err := relationshipFromRecord(existing)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "decode relationship for delete: %v", err)
			}
			if storedRelationship.SourceLayer != update.relationship.SourceLayer {
				continue
			}
			if err := store.Delete(ctx, update.id); err != nil && !errors.Is(err, gestalt.ErrNotFound) && !errors.Is(err, indexeddb.ErrNotFound) {
				return nil, relationshipStorageError("delete relationship", err)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, relationshipStorageError("commit relationship transaction", err)
	}
	committed = true
	return &WriteRelationshipsResponse{}, nil
}

func relationshipStorageError(operation string, err error) error {
	code := codes.Internal
	if status.Code(err) == codes.ResourceExhausted {
		code = codes.ResourceExhausted
	}
	return status.Errorf(code, "%s: %v", operation, err)
}

type preparedRelationshipUpdate struct {
	operation    RelationshipUpdateOperation
	relationship *Relationship
	record       indexeddb.Record
	id           string
}

type preparedRelationshipPrecondition struct {
	operation PreconditionOperation
	filter    *RelationshipFilter
}

type preparedRelationshipWrite struct {
	updates       []preparedRelationshipUpdate
	preconditions []preparedRelationshipPrecondition
}

func prepareRelationshipWrite(req *WriteRelationshipsRequest) (preparedRelationshipWrite, error) {
	if req == nil {
		return preparedRelationshipWrite{}, fmt.Errorf("request is required")
	}
	if len(req.Updates) == 0 {
		return preparedRelationshipWrite{}, fmt.Errorf("updates are required")
	}
	prepared := preparedRelationshipWrite{updates: make([]preparedRelationshipUpdate, 0, len(req.Updates)), preconditions: make([]preparedRelationshipPrecondition, 0, len(req.OptionalPreconditions))}
	seen := map[string]struct{}{}
	for i, update := range req.Updates {
		if update == nil || update.Relationship == nil {
			return preparedRelationshipWrite{}, fmt.Errorf("updates[%d].relationship is required", i)
		}
		if update.Operation != RelationshipUpdateOperationCreate && update.Operation != RelationshipUpdateOperationTouch && update.Operation != RelationshipUpdateOperationDelete {
			return preparedRelationshipWrite{}, fmt.Errorf("updates[%d].operation is invalid", i)
		}
		relationship := cloneRelationship(update.Relationship)
		if err := normalizeRelationship(relationship); err != nil {
			return preparedRelationshipWrite{}, fmt.Errorf("updates[%d].relationship is invalid: %w", i, err)
		}
		record, err := relationshipToRecord(relationship)
		if err != nil {
			return preparedRelationshipWrite{}, fmt.Errorf("updates[%d].relationship is invalid: %w", i, err)
		}
		id := stringField(record, "id")
		if _, ok := seen[id]; ok {
			return preparedRelationshipWrite{}, fmt.Errorf("updates[%d] duplicates a relationship identity", i)
		}
		seen[id] = struct{}{}
		prepared.updates = append(prepared.updates, preparedRelationshipUpdate{operation: update.Operation, relationship: relationship, record: record, id: id})
	}
	for i, precondition := range req.OptionalPreconditions {
		if precondition == nil || precondition.Filter == nil {
			return preparedRelationshipWrite{}, fmt.Errorf("optional_preconditions[%d].filter is required", i)
		}
		if precondition.Operation != PreconditionOperationMustMatch && precondition.Operation != PreconditionOperationMustNotMatch {
			return preparedRelationshipWrite{}, fmt.Errorf("optional_preconditions[%d].operation is invalid", i)
		}
		filter, err := normalizeRelationshipFilter(precondition.Filter)
		if err != nil {
			return preparedRelationshipWrite{}, fmt.Errorf("optional_preconditions[%d].filter is invalid: %w", i, err)
		}
		prepared.preconditions = append(prepared.preconditions, preparedRelationshipPrecondition{operation: precondition.Operation, filter: filter})
	}
	return prepared, nil
}

func normalizeRelationshipFilter(filter *RelationshipFilter) (*RelationshipFilter, error) {
	if filter == nil {
		return nil, fmt.Errorf("filter is required")
	}
	switch filter.TargetType {
	case RelationshipTargetTypeUnspecified, RelationshipTargetTypeSubject, RelationshipTargetTypeResource, RelationshipTargetTypeSubjectSet:
	default:
		return nil, fmt.Errorf("target type is invalid")
	}
	switch filter.SourceLayer {
	case SourceLayerUnspecified, SourceLayerStaticConfig, SourceLayerRuntime:
	default:
		return nil, fmt.Errorf("source layer is invalid")
	}
	if filter.Target == nil && strings.TrimSpace(filter.Relation) == "" && filter.Resource == nil && filter.TargetType == RelationshipTargetTypeUnspecified && strings.TrimSpace(filter.TargetEntityType) == "" && strings.TrimSpace(filter.ResourceType) == "" && filter.SourceLayer == SourceLayerUnspecified {
		return nil, fmt.Errorf("filter must not be empty")
	}
	result := &RelationshipFilter{Relation: strings.TrimSpace(filter.Relation), TargetType: filter.TargetType, TargetEntityType: strings.TrimSpace(filter.TargetEntityType), ResourceType: strings.TrimSpace(filter.ResourceType), SourceLayer: filter.SourceLayer}
	if filter.Target != nil {
		result.Target = cloneRelationshipTarget(filter.Target)
		if err := normalizeRelationshipTarget(result.Target); err != nil {
			return nil, err
		}
	}
	if filter.Resource != nil {
		result.Resource = cloneResource(filter.Resource)
		if err := normalizeResource(result.Resource, "resource"); err != nil {
			return nil, err
		}
	}
	return result, nil
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
var _ gestalt.AuthorizationRelationshipWriter = (*Provider)(nil)
var _ gestalt.MetadataProvider = (*Provider)(nil)
var _ gestalt.HealthChecker = (*Provider)(nil)
var _ gestalt.Closer = (*Provider)(nil)
