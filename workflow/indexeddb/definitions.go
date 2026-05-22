package indexeddb

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type workflowDefinitionRecord struct {
	ID        string
	Target    *gestalt.BoundWorkflowTarget
	CreatedBy *gestalt.WorkflowActor
	CreatedAt time.Time
}

func (p *Provider) CreateDefinition(ctx context.Context, req *gestalt.CreateWorkflowProviderDefinitionRequest) (*gestalt.BoundWorkflowDefinition, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	target, err := normalizeTarget(workflowTargetInput(req.Target))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	key := strings.TrimSpace(req.IdempotencyKey)
	definitionID := uuid.NewString()
	if key != "" {
		definitionID = idempotentDefinitionID(target.OwnerKey, key)
	}

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if key != "" {
		existing, found, err := loadDefinitionRecord(ctx, state.definitionStore, definitionID)
		if err != nil {
			p.mu.Unlock()
			return nil, status.Errorf(codes.Internal, "load idempotent workflow definition: %v", err)
		}
		if found {
			resp := existing.toInput(p.name)
			p.mu.Unlock()
			return resp, nil
		}
	}
	record := workflowDefinitionRecord{
		ID:        definitionID,
		Target:    cloneTarget(target.Target),
		CreatedAt: p.clock().UTC(),
	}
	if err := state.definitionStore.Add(ctx, record.toRecord()); err != nil {
		if key != "" && errors.Is(err, gestalt.ErrAlreadyExists) {
			existing, found, loadErr := loadDefinitionRecord(ctx, state.definitionStore, definitionID)
			if loadErr != nil {
				p.mu.Unlock()
				return nil, status.Errorf(codes.Internal, "load existing workflow definition: %v", loadErr)
			}
			if found {
				resp := existing.toInput(p.name)
				p.mu.Unlock()
				return resp, nil
			}
		}
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "create workflow definition: %v", err)
	}
	resp := record.toInput(p.name)
	p.mu.Unlock()
	return resp, nil
}

func (p *Provider) GetDefinition(ctx context.Context, req *gestalt.GetWorkflowProviderDefinitionRequest) (*gestalt.BoundWorkflowDefinition, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	definitionID := strings.TrimSpace(req.DefinitionID)
	if definitionID == "" {
		return nil, status.Error(codes.InvalidArgument, "definition_id is required")
	}

	p.mu.RLock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.RUnlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	record, found, err := loadDefinitionRecord(ctx, state.definitionStore, definitionID)
	providerName := p.name
	p.mu.RUnlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get workflow definition: %v", err)
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow definition %q not found", definitionID)
	}
	return record.toInput(providerName), nil
}

func (p *Provider) UpdateDefinition(ctx context.Context, req *gestalt.UpdateWorkflowProviderDefinitionRequest) (*gestalt.BoundWorkflowDefinition, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	definitionID := strings.TrimSpace(req.DefinitionID)
	if definitionID == "" {
		return nil, status.Error(codes.InvalidArgument, "definition_id is required")
	}
	target, err := normalizeTarget(workflowTargetInput(req.Target))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	existing, found, err := loadDefinitionRecord(ctx, state.definitionStore, definitionID)
	if err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "load workflow definition: %v", err)
	}
	if !found {
		p.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "workflow definition %q not found", definitionID)
	}
	record := workflowDefinitionRecord{
		ID:        definitionID,
		Target:    cloneTarget(target.Target),
		CreatedBy: cloneActor(existing.CreatedBy),
		CreatedAt: existing.CreatedAt,
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = p.clock().UTC()
	}
	if err := state.definitionStore.Put(ctx, record.toRecord()); err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "update workflow definition: %v", err)
	}
	resp := record.toInput(p.name)
	p.mu.Unlock()
	return resp, nil
}

func (p *Provider) DeleteDefinition(ctx context.Context, req *gestalt.DeleteWorkflowProviderDefinitionRequest) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}
	definitionID := strings.TrimSpace(req.DefinitionID)
	if definitionID == "" {
		return status.Error(codes.InvalidArgument, "definition_id is required")
	}

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	_, found, err := loadDefinitionRecord(ctx, state.definitionStore, definitionID)
	if err != nil {
		p.mu.Unlock()
		return status.Errorf(codes.Internal, "load workflow definition: %v", err)
	}
	if !found {
		p.mu.Unlock()
		return status.Errorf(codes.NotFound, "workflow definition %q not found", definitionID)
	}
	if err := state.definitionStore.Delete(ctx, definitionID); err != nil {
		p.mu.Unlock()
		return status.Errorf(codes.Internal, "delete workflow definition: %v", err)
	}
	p.mu.Unlock()
	return nil
}

func loadDefinitionRecord(ctx context.Context, store *gestalt.ObjectStoreClient, definitionID string) (workflowDefinitionRecord, bool, error) {
	record, err := store.Get(ctx, strings.TrimSpace(definitionID))
	if err != nil {
		if errors.Is(err, gestalt.ErrNotFound) {
			return workflowDefinitionRecord{}, false, nil
		}
		return workflowDefinitionRecord{}, false, err
	}
	definition, err := definitionRecordFromRecord(record)
	if err != nil {
		return workflowDefinitionRecord{}, false, err
	}
	if strings.TrimSpace(definition.ID) == "" {
		return workflowDefinitionRecord{}, false, nil
	}
	return definition, true, nil
}

func (r workflowDefinitionRecord) toRecord() gestalt.Record {
	return gestalt.Record{
		"id":          r.ID,
		"target_json": targetJSON(r.Target),
		"created_by":  actorToMap(r.CreatedBy),
		"created_at":  r.CreatedAt.UTC(),
	}
}

func definitionRecordFromRecord(record gestalt.Record) (workflowDefinitionRecord, error) {
	value := map[string]any(record)
	id := stringField(value, "id")
	target, err := targetFromRecordValue("workflow definition", id, value["target_json"])
	if err != nil {
		return workflowDefinitionRecord{}, err
	}
	out := workflowDefinitionRecord{
		ID:        id,
		Target:    target,
		CreatedBy: actorFromAny(value["created_by"]),
	}
	if createdAt := timeField(value, "created_at"); createdAt != nil {
		out.CreatedAt = createdAt.UTC()
	}
	return out, nil
}

func (r workflowDefinitionRecord) toInput(providerName string) *gestalt.BoundWorkflowDefinition {
	return cloneWorkflowDefinition(&gestalt.BoundWorkflowDefinition{
		ID:           r.ID,
		Target:       workflowTargetInput(r.Target),
		CreatedBy:    workflowActorInput(r.CreatedBy),
		CreatedAt:    r.CreatedAt,
		ProviderName: strings.TrimSpace(providerName),
	})
}

func cloneWorkflowDefinition(definition *gestalt.BoundWorkflowDefinition) *gestalt.BoundWorkflowDefinition {
	if definition == nil {
		return nil
	}
	out := cloneJSONValue(*definition)
	return &out
}

func idempotentDefinitionID(ownerKey, key string) string {
	return hashScopedID("definition", ownerKey, key)
}
