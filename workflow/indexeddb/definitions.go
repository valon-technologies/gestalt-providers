package indexeddb

import (
	"context"
	"errors"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type workflowDefinitionRecord struct {
	ID                 string
	Generation         int64
	Target             *gestalt.BoundWorkflowTarget
	Activations        []gestalt.WorkflowActivation
	Paused             bool
	CreatedBySubjectID string
	CreatedAt          time.Time
	UpdatedAt          time.Time
	RunAs              *gestalt.Subject
}

func (p *Provider) ApplyDefinition(ctx context.Context, req *gestalt.ApplyWorkflowProviderDefinitionRequest) (*gestalt.WorkflowDefinition, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.Spec == nil {
		return nil, status.Error(codes.InvalidArgument, "spec is required")
	}
	spec := req.Spec
	definitionID := strings.TrimSpace(spec.ID)
	if definitionID == "" {
		return nil, status.Error(codes.InvalidArgument, "definition_id is required")
	}
	target, err := normalizeTarget(workflowTargetInput(spec.Target))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	activations, err := normalizeWorkflowActivations(spec.Activations)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validateWorkflowActivationRunAs(activations, spec.RunAs); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	now := p.clock().UTC()
	existing, found, err := loadDefinitionRecord(ctx, state.definitionStore, definitionID)
	if err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "load workflow definition: %v", err)
	}
	record := workflowDefinitionRecord{
		ID:                 definitionID,
		Generation:         1,
		Target:             cloneTarget(target.Target),
		Activations:        cloneWorkflowActivations(activations),
		Paused:             spec.Paused,
		CreatedBySubjectID: cloneCreatedBySubjectID(req.RequestedBySubjectID),
		CreatedAt:          now,
		UpdatedAt:          now,
		RunAs:              cloneSubject(spec.RunAs),
	}
	if found {
		record.Generation = existing.Generation + 1
		if record.Generation <= 0 {
			record.Generation = 1
		}
		record.CreatedAt = existing.CreatedAt
		record.CreatedBySubjectID = createdByForUpsert(existing.CreatedBySubjectID, record.CreatedBySubjectID)
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = now
	}
	if err := state.definitionStore.Put(ctx, record.toRecord()); err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "apply workflow definition: %v", err)
	}
	if err := syncDefinitionScheduleActivations(ctx, state.scheduleStore, record, now); err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "sync workflow definition schedules: %v", err)
	}
	resp := record.toInput(p.name)
	p.signalWorkerLocked("")
	p.mu.Unlock()
	return resp, nil
}

func (p *Provider) GetDefinition(ctx context.Context, req *gestalt.GetWorkflowProviderDefinitionRequest) (*gestalt.WorkflowDefinition, error) {
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

func (p *Provider) ListDefinitions(ctx context.Context, req *gestalt.ListWorkflowProviderDefinitionsRequest) (*gestalt.ListWorkflowProviderDefinitionsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	p.mu.RLock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.RUnlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	records, err := listDefinitionRecords(ctx, state.definitionStore)
	providerName := p.name
	p.mu.RUnlock()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list workflow definitions: %v", err)
	}
	resp := &gestalt.ListWorkflowProviderDefinitionsResponse{Definitions: make([]gestalt.WorkflowDefinition, 0, len(records))}
	for _, record := range records {
		resp.Definitions = append(resp.Definitions, *record.toInput(providerName))
	}
	return resp, nil
}

func (p *Provider) SetDefinitionPaused(ctx context.Context, req *gestalt.SetWorkflowProviderDefinitionPausedRequest) (*gestalt.WorkflowDefinition, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	definitionID := strings.TrimSpace(req.DefinitionID)
	if definitionID == "" {
		return nil, status.Error(codes.InvalidArgument, "definition_id is required")
	}
	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	record, found, err := loadDefinitionRecord(ctx, state.definitionStore, definitionID)
	if err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "load workflow definition: %v", err)
	}
	if !found {
		p.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "workflow definition %q not found", definitionID)
	}
	record.Paused = req.Paused
	record.UpdatedAt = p.clock().UTC()
	if err := state.definitionStore.Put(ctx, record.toRecord()); err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "set workflow definition paused: %v", err)
	}
	if err := syncDefinitionScheduleActivations(ctx, state.scheduleStore, record, record.UpdatedAt); err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "sync workflow definition schedules: %v", err)
	}
	resp := record.toInput(p.name)
	p.signalWorkerLocked("")
	p.mu.Unlock()
	return resp, nil
}

func (p *Provider) SetActivationPaused(ctx context.Context, req *gestalt.SetWorkflowProviderActivationPausedRequest) (*gestalt.WorkflowDefinition, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	definitionID := strings.TrimSpace(req.DefinitionID)
	if definitionID == "" {
		return nil, status.Error(codes.InvalidArgument, "definition_id is required")
	}
	activationID := strings.TrimSpace(req.ActivationID)
	if activationID == "" {
		return nil, status.Error(codes.InvalidArgument, "activation_id is required")
	}
	p.mu.Lock()
	state, err := p.requireConfiguredLocked()
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	record, found, err := loadDefinitionRecord(ctx, state.definitionStore, definitionID)
	if err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "load workflow definition: %v", err)
	}
	if !found {
		p.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "workflow definition %q not found", definitionID)
	}
	updated := false
	for i := range record.Activations {
		if strings.TrimSpace(record.Activations[i].ID) == activationID {
			record.Activations[i].Paused = req.Paused
			updated = true
			break
		}
	}
	if !updated {
		p.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "workflow activation %q not found", activationID)
	}
	record.UpdatedAt = p.clock().UTC()
	if err := state.definitionStore.Put(ctx, record.toRecord()); err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "set workflow activation paused: %v", err)
	}
	if err := syncDefinitionScheduleActivations(ctx, state.scheduleStore, record, record.UpdatedAt); err != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.Internal, "sync workflow definition schedules: %v", err)
	}
	resp := record.toInput(p.name)
	p.signalWorkerLocked("")
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

func loadDefinitionRecord(ctx context.Context, store indexeddb.ObjectStore, definitionID string) (workflowDefinitionRecord, bool, error) {
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

func listDefinitionRecords(ctx context.Context, store indexeddb.ObjectStore) ([]workflowDefinitionRecord, error) {
	records, err := store.GetAll(ctx, nil)
	if err != nil {
		return nil, err
	}
	out := make([]workflowDefinitionRecord, 0, len(records))
	for _, record := range records {
		definition, err := definitionRecordFromRecord(record)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(definition.ID) == "" {
			continue
		}
		out = append(out, definition)
	}
	return out, nil
}

func (r workflowDefinitionRecord) toRecord() gestalt.Record {
	return gestalt.Record{
		"id":               r.ID,
		"generation":       r.Generation,
		"target_json":      targetJSON(r.Target),
		"activations_json": jsonValueString(r.Activations),
		"paused":           r.Paused,
		"created_by":       createdByToMap(r.CreatedBySubjectID),
		"created_at":       r.CreatedAt.UTC(),
		"updated_at":       r.UpdatedAt.UTC(),
		"run_as":           subjectToMap(r.RunAs),
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
		ID:                 id,
		Generation:         intField(value, "generation"),
		Target:             target,
		Activations:        workflowActivationsFromRecordValue(value["activations_json"]),
		Paused:             boolField(value, "paused"),
		CreatedBySubjectID: createdByFromAny(value["created_by"]),
		RunAs:              subjectFromAny(value["run_as"]),
	}
	if out.Generation <= 0 {
		out.Generation = 1
	}
	if createdAt := timeField(value, "created_at"); createdAt != nil {
		out.CreatedAt = createdAt.UTC()
	}
	if updatedAt := timeField(value, "updated_at"); updatedAt != nil {
		out.UpdatedAt = updatedAt.UTC()
	}
	return out, nil
}

func (r workflowDefinitionRecord) toInput(providerName string) *gestalt.WorkflowDefinition {
	return cloneWorkflowDefinition(&gestalt.WorkflowDefinition{
		ID:                 r.ID,
		Generation:         r.Generation,
		Target:             workflowTargetInput(r.Target),
		Activations:        cloneWorkflowActivations(r.Activations),
		Paused:             r.Paused,
		CreatedBySubjectID: cloneCreatedBySubjectID(r.CreatedBySubjectID),
		CreatedAt:          r.CreatedAt,
		UpdatedAt:          r.UpdatedAt,
		ProviderName:       strings.TrimSpace(providerName),
		RunAs:              cloneSubject(r.RunAs),
	})
}

func cloneWorkflowDefinition(definition *gestalt.WorkflowDefinition) *gestalt.WorkflowDefinition {
	if definition == nil {
		return nil
	}
	out := cloneJSONValue(*definition)
	return &out
}
