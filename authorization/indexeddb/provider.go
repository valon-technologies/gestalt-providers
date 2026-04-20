package indexeddb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const providerVersion = "0.0.1-alpha.2"

type Provider struct {
	mu    sync.RWMutex
	cfg   config
	store *store
	now   func() time.Time
}

func New() *Provider {
	return &Provider{now: time.Now}
}

func (p *Provider) Configure(ctx context.Context, _ string, raw map[string]any) error {
	cfg, err := decodeConfig(raw)
	if err != nil {
		return fmt.Errorf("indexeddb authorization: %w", err)
	}
	st, err := openStore(ctx, cfg)
	if err != nil {
		return fmt.Errorf("indexeddb authorization: %w", err)
	}

	p.mu.Lock()
	oldStore := p.store
	p.cfg = cfg
	p.store = st
	p.mu.Unlock()

	if oldStore != nil {
		_ = oldStore.Close()
	}
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindAuthorization,
		Name:        "indexeddb",
		DisplayName: "IndexedDB Authorization",
		Description: "Authorization provider backed by the host IndexedDB service.",
		Version:     providerVersion,
	}
}

func (p *Provider) HealthCheck(ctx context.Context) error {
	st, err := p.configuredStore()
	if err != nil {
		return err
	}
	_, err = st.state.Count(ctx, nil)
	return err
}

func (p *Provider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.store == nil {
		return nil
	}
	err := p.store.Close()
	p.store = nil
	return err
}

func (p *Provider) configuredStore() (*store, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "indexeddb authorization: provider is not configured")
	}
	return p.store, nil
}

func (p *Provider) GetMetadata(ctx context.Context) (*gestalt.AuthorizationMetadata, error) {
	st, err := p.configuredStore()
	if err != nil {
		return nil, err
	}
	activeModelID, err := st.activeModelID(ctx)
	if err != nil {
		return nil, err
	}
	return &gestalt.AuthorizationMetadata{
		Capabilities:  []string{"decision_plane", "relationship_control_plane", "model_control_plane"},
		ActiveModelId: activeModelID,
	}, nil
}

func (p *Provider) GetActiveModel(ctx context.Context) (*gestalt.GetActiveModelResponse, error) {
	model, err := p.resolveModel(ctx, "", false)
	if err != nil {
		return nil, err
	}
	if model == nil {
		return &gestalt.GetActiveModelResponse{}, nil
	}
	return &gestalt.GetActiveModelResponse{Model: model.ref}, nil
}

func (p *Provider) ListModels(ctx context.Context, req *gestalt.ListModelsRequest) (*gestalt.ListModelsResponse, error) {
	st, err := p.configuredStore()
	if err != nil {
		return nil, err
	}
	models, err := st.listModels(ctx)
	if err != nil {
		return nil, err
	}
	start, end, nextToken, err := paginate(len(models), req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}
	return &gestalt.ListModelsResponse{
		Models:        append([]*gestalt.AuthorizationModelRef(nil), models[start:end]...),
		NextPageToken: nextToken,
	}, nil
}

func (p *Provider) WriteModel(ctx context.Context, req *gestalt.WriteModelRequest) (*gestalt.AuthorizationModelRef, error) {
	st, err := p.configuredStore()
	if err != nil {
		return nil, err
	}
	model, normalized, err := compileAuthorizationModel(req.GetModel())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid authorization model: %v", err)
	}
	modelID, err := modelIDForDefinition(normalized)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build model id: %v", err)
	}
	if existing, err := st.loadModel(ctx, modelID); err == nil {
		if err := st.setActiveModelID(ctx, existing.ref.GetId()); err != nil {
			return nil, err
		}
		return existing.ref, nil
	} else if status.Code(err) != codes.NotFound {
		return nil, err
	}
	modelJSON, err := marshalStoredModel(normalized)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal authorization model: %v", err)
	}
	ref := &gestalt.AuthorizationModelRef{
		Id:        modelID,
		Version:   model.Version,
		CreatedAt: timestamppb.New(p.now().UTC()),
	}
	if err := st.writeModel(ctx, ref, modelJSON); err != nil {
		return nil, err
	}
	return ref, nil
}

func (p *Provider) ReadRelationships(ctx context.Context, req *gestalt.ReadRelationshipsRequest) (*gestalt.ReadRelationshipsResponse, error) {
	st, err := p.configuredStore()
	if err != nil {
		return nil, err
	}
	if req.GetSubject() != nil {
		if err := validateSubject(req.GetSubject()); err != nil {
			return nil, err
		}
	}
	if req.GetResource() != nil {
		if err := validateResource(req.GetResource()); err != nil {
			return nil, err
		}
	}

	model, err := p.resolveModel(ctx, req.GetModelId(), false)
	if err != nil {
		return nil, err
	}
	relationships, err := st.candidateRelationships(ctx, req.GetSubject(), req.GetResource())
	if err != nil {
		return nil, err
	}
	filtered := make([]*gestalt.Relationship, 0, len(relationships))
	for _, relationship := range relationships {
		if req.GetRelation() != "" && relationship.GetRelation() != req.GetRelation() {
			continue
		}
		filtered = append(filtered, relationship)
	}
	sortRelationships(filtered)

	start, end, nextToken, err := paginate(len(filtered), req.GetPageSize(), req.GetPageToken())
	if err != nil {
		return nil, err
	}
	modelID := ""
	if model != nil {
		modelID = model.ref.GetId()
	}
	return &gestalt.ReadRelationshipsResponse{
		Relationships: append([]*gestalt.Relationship(nil), filtered[start:end]...),
		NextPageToken: nextToken,
		ModelId:       modelID,
	}, nil
}

func (p *Provider) WriteRelationships(ctx context.Context, req *gestalt.WriteRelationshipsRequest) error {
	st, err := p.configuredStore()
	if err != nil {
		return err
	}
	model, err := p.resolveModel(ctx, req.GetModelId(), false)
	if err != nil {
		return err
	}

	deleteIDs := make([]string, 0, len(req.GetDeletes()))
	for _, key := range req.GetDeletes() {
		id, err := relationshipKeyID(key)
		if err != nil {
			return err
		}
		deleteIDs = append(deleteIDs, id)
	}

	writeRecords := make([]gestalt.Record, 0, len(req.GetWrites()))
	for _, relationship := range req.GetWrites() {
		record, err := relationshipRecord(relationship)
		if err != nil {
			return err
		}
		if model != nil {
			if err := model.compiled.validateRelationship(relationship.GetSubject().GetType(), relationship.GetRelation(), relationship.GetResource().GetType()); err != nil {
				return status.Errorf(codes.InvalidArgument, "relationship rejected by model %q: %v", model.ref.GetId(), err)
			}
		}
		writeRecords = append(writeRecords, record)
	}

	snapshots := make(map[string]relationshipSnapshot, len(deleteIDs)+len(writeRecords))
	for _, id := range deleteIDs {
		if err := snapshotRelationshipRecord(ctx, st, snapshots, id); err != nil {
			return err
		}
	}
	for _, record := range writeRecords {
		id, _ := record["id"].(string)
		if err := snapshotRelationshipRecord(ctx, st, snapshots, id); err != nil {
			return err
		}
	}

	for _, id := range deleteIDs {
		if err := st.relationships.Delete(ctx, id); err != nil && !errors.Is(err, gestalt.ErrNotFound) {
			if rollbackErr := restoreRelationshipSnapshots(ctx, st, snapshots); rollbackErr != nil {
				return errors.Join(err, fmt.Errorf("rollback relationship batch: %w", rollbackErr))
			}
			return err
		}
	}
	for _, record := range writeRecords {
		if err := st.relationships.Put(ctx, record); err != nil {
			if rollbackErr := restoreRelationshipSnapshots(ctx, st, snapshots); rollbackErr != nil {
				return errors.Join(err, fmt.Errorf("rollback relationship batch: %w", rollbackErr))
			}
			return err
		}
	}
	return nil
}

type relationshipSnapshot struct {
	present bool
	record  gestalt.Record
}

func snapshotRelationshipRecord(ctx context.Context, st *store, snapshots map[string]relationshipSnapshot, id string) error {
	if _, ok := snapshots[id]; ok {
		return nil
	}
	record, err := st.relationships.Get(ctx, id)
	switch {
	case err == nil:
		snapshots[id] = relationshipSnapshot{present: true, record: cloneRecord(record)}
		return nil
	case errors.Is(err, gestalt.ErrNotFound):
		snapshots[id] = relationshipSnapshot{}
		return nil
	default:
		return err
	}
}

func restoreRelationshipSnapshots(ctx context.Context, st *store, snapshots map[string]relationshipSnapshot) error {
	var restoreErr error
	for id, snapshot := range snapshots {
		if snapshot.present {
			if err := st.relationships.Put(ctx, cloneRecord(snapshot.record)); err != nil {
				restoreErr = errors.Join(restoreErr, err)
			}
			continue
		}
		if err := st.relationships.Delete(ctx, id); err != nil && !errors.Is(err, gestalt.ErrNotFound) {
			restoreErr = errors.Join(restoreErr, err)
		}
	}
	return restoreErr
}

func cloneRecord(record gestalt.Record) gestalt.Record {
	if record == nil {
		return nil
	}
	cloned := make(gestalt.Record, len(record))
	for key, value := range record {
		cloned[key] = cloneRecordValue(value)
	}
	return cloned
}

func cloneRecordValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		cloned := make(map[string]any, len(typed))
		for key, nested := range typed {
			cloned[key] = cloneRecordValue(nested)
		}
		return cloned
	case []any:
		cloned := make([]any, len(typed))
		for i, nested := range typed {
			cloned[i] = cloneRecordValue(nested)
		}
		return cloned
	default:
		return typed
	}
}

func (p *Provider) resolveModel(ctx context.Context, requestedModelID string, requireActive bool) (*storedModel, error) {
	st, err := p.configuredStore()
	if err != nil {
		return nil, err
	}
	modelID := strings.TrimSpace(requestedModelID)
	if modelID == "" {
		modelID, err = st.activeModelID(ctx)
		if err != nil {
			return nil, err
		}
	}
	if modelID == "" {
		if requireActive {
			return nil, status.Error(codes.FailedPrecondition, "indexeddb authorization: no active model is configured")
		}
		return nil, nil
	}
	return st.loadModel(ctx, modelID)
}
