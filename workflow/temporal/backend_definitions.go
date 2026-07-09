package temporal

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (b *temporalBackend) ApplyDefinition(ctx context.Context, req *gestalt.ApplyWorkflowProviderDefinitionRequest) (*gestalt.WorkflowDefinition, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	spec, ownerKey, err := normalizeDefinitionSpec(req.Spec)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	definitionID := strings.TrimSpace(spec.ID)
	if definitionID == "" {
		key := strings.TrimSpace(req.IdempotencyKey)
		if key != "" {
			definitionID = idempotentDefinitionID(ownerKey, key)
		} else {
			definitionID = uuid.NewString()
		}
	}
	now := time.Now().UTC()
	existing, found, err := b.state.getDefinition(ctx, definitionID)
	if err != nil {
		return nil, err
	}
	createdAt := now
	createdBy := requestSubjectID(ctx)
	generation := int64(1)
	if found {
		createdAt = existing.CreatedAt
		if createdAt.IsZero() {
			createdAt = now
		}
		createdBy = createdByForUpsert(existing.CreatedBySubjectID, createdBy)
		generation = existing.Generation + 1
		if generation <= 0 {
			generation = 1
		}
	}
	definition := &gestalt.WorkflowDefinition{
		ID:                 definitionID,
		Generation:         generation,
		Target:             spec.Target,
		Activations:        spec.Activations,
		Paused:             spec.Paused,
		CreatedBySubjectID: createdBy,
		CreatedAt:          createdAt,
		UpdatedAt:          now,
		ProviderName:       b.providerName,
		RunAs:              cloneSubjectInput(spec.RunAs),
	}
	if err := b.syncDefinitionSchedules(ctx, existing, definition); err != nil {
		return nil, err
	}
	if found {
		if err := b.state.putDefinition(ctx, definition); err != nil {
			return nil, err
		}
	} else if err := b.state.addDefinition(ctx, definition); err != nil {
		if errors.Is(err, gestalt.ErrAlreadyExists) {
			return nil, status.Errorf(codes.AlreadyExists, "workflow definition %q already exists", definitionID)
		}
		return nil, err
	}
	return cloneWorkflowDefinitionInput(definition), nil
}

func (b *temporalBackend) GetDefinition(ctx context.Context, req *gestalt.GetWorkflowProviderDefinitionRequest) (*gestalt.WorkflowDefinition, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	definitionID := strings.TrimSpace(req.DefinitionID)
	if definitionID == "" {
		return nil, status.Error(codes.InvalidArgument, "definition_id is required")
	}
	definition, found, err := b.state.getDefinition(ctx, definitionID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow definition %q not found", definitionID)
	}
	return cloneWorkflowDefinitionInput(definition), nil
}

func (b *temporalBackend) ListDefinitions(ctx context.Context, _ *gestalt.ListWorkflowProviderDefinitionsRequest) (*gestalt.ListWorkflowProviderDefinitionsResponse, error) {
	definitions, err := b.state.listDefinitions(ctx)
	if err != nil {
		return nil, err
	}
	sortWorkflowDefinitions(definitions)
	out := make([]gestalt.WorkflowDefinition, 0, len(definitions))
	for _, definition := range definitions {
		if definition != nil {
			out = append(out, *cloneWorkflowDefinitionInput(definition))
		}
	}
	return &gestalt.ListWorkflowProviderDefinitionsResponse{Definitions: out}, nil
}

func (b *temporalBackend) SetDefinitionPaused(ctx context.Context, req *gestalt.SetWorkflowProviderDefinitionPausedRequest) (*gestalt.WorkflowDefinition, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	definitionID := strings.TrimSpace(req.DefinitionID)
	if definitionID == "" {
		return nil, status.Error(codes.InvalidArgument, "definition_id is required")
	}
	definition, found, err := b.state.getDefinition(ctx, definitionID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow definition %q not found", definitionID)
	}
	next := *cloneWorkflowDefinitionInput(definition)
	next.Paused = req.Paused
	next.Generation++
	if next.Generation <= 0 {
		next.Generation = 1
	}
	next.UpdatedAt = time.Now().UTC()
	if createdBySubjectIDSet(requestSubjectID(ctx)) && isConfigManagedSubjectID(next.CreatedBySubjectID) {
		next.CreatedBySubjectID = requestSubjectID(ctx)
	}
	if err := b.syncDefinitionSchedules(ctx, definition, &next); err != nil {
		return nil, err
	}
	if err := b.state.putDefinition(ctx, &next); err != nil {
		return nil, err
	}
	return cloneWorkflowDefinitionInput(&next), nil
}

func (b *temporalBackend) SetActivationPaused(ctx context.Context, req *gestalt.SetWorkflowProviderActivationPausedRequest) (*gestalt.WorkflowDefinition, error) {
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
	definition, found, err := b.state.getDefinition(ctx, definitionID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow definition %q not found", definitionID)
	}
	next := *cloneWorkflowDefinitionInput(definition)
	updated := false
	for i := range next.Activations {
		if strings.TrimSpace(next.Activations[i].ID) == activationID {
			next.Activations[i].Paused = req.Paused
			updated = true
			break
		}
	}
	if !updated {
		return nil, status.Errorf(codes.NotFound, "workflow activation %q not found", activationID)
	}
	next.Generation++
	if next.Generation <= 0 {
		next.Generation = 1
	}
	next.UpdatedAt = time.Now().UTC()
	if err := b.syncDefinitionSchedules(ctx, definition, &next); err != nil {
		return nil, err
	}
	if err := b.state.putDefinition(ctx, &next); err != nil {
		return nil, err
	}
	return cloneWorkflowDefinitionInput(&next), nil
}

func (b *temporalBackend) DeleteDefinition(ctx context.Context, req *gestalt.DeleteWorkflowProviderDefinitionRequest) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}
	definitionID := strings.TrimSpace(req.DefinitionID)
	if definitionID == "" {
		return status.Error(codes.InvalidArgument, "definition_id is required")
	}
	definition, found, err := b.state.getDefinition(ctx, definitionID)
	if err != nil {
		return err
	}
	if !found {
		return status.Errorf(codes.NotFound, "workflow definition %q not found", definitionID)
	}
	if err := b.syncDefinitionSchedules(ctx, definition, nil); err != nil {
		return err
	}
	return b.state.deleteDefinition(ctx, definitionID)
}

func normalizeDefinitionSpec(spec *gestalt.WorkflowDefinitionSpec) (gestalt.WorkflowDefinitionSpec, string, error) {
	if spec == nil {
		return gestalt.WorkflowDefinitionSpec{}, "", fmt.Errorf("spec is required")
	}
	target, err := normalizeTarget(spec.Target)
	if err != nil {
		return gestalt.WorkflowDefinitionSpec{}, "", err
	}
	activations, err := normalizeWorkflowActivations(spec.Activations)
	if err != nil {
		return gestalt.WorkflowDefinitionSpec{}, "", err
	}
	if err := validateWorkflowActivationRunAsInput(activations, spec.RunAs); err != nil {
		return gestalt.WorkflowDefinitionSpec{}, "", err
	}
	return gestalt.WorkflowDefinitionSpec{
		ID:          strings.TrimSpace(spec.ID),
		Target:      target.Target,
		Activations: activations,
		Paused:      spec.Paused,
		RunAs:       cloneSubjectInput(spec.RunAs),
	}, target.OwnerKey, nil
}

func normalizeWorkflowActivations(inputs []gestalt.WorkflowActivation) ([]gestalt.WorkflowActivation, error) {
	if len(inputs) == 0 {
		return nil, nil
	}
	out := make([]gestalt.WorkflowActivation, 0, len(inputs))
	seen := map[string]struct{}{}
	for i, input := range inputs {
		activation := input
		path := fmt.Sprintf("spec.activations[%d]", i)
		activation.ID = strings.TrimSpace(activation.ID)
		if activation.ID == "" {
			return nil, fmt.Errorf("%s.id is required", path)
		}
		if _, exists := seen[activation.ID]; exists {
			return nil, fmt.Errorf("%s.id duplicates %q", path, activation.ID)
		}
		switch {
		case activation.Schedule != nil && activation.Event != nil:
			return nil, fmt.Errorf("%s must set exactly one of schedule or event", path)
		case activation.Schedule != nil:
			schedule := *activation.Schedule
			schedule.Cron = strings.TrimSpace(schedule.Cron)
			if schedule.Cron == "" {
				return nil, fmt.Errorf("%s.schedule.cron is required", path)
			}
			schedule.Timezone = strings.TrimSpace(schedule.Timezone)
			if schedule.Timezone == "" {
				schedule.Timezone = defaultTimezone
			}
			activation.Schedule = &schedule
		case activation.Event != nil:
			event := *activation.Event
			match := normalizeWorkflowEventMatch(event.Match)
			if match == nil || strings.TrimSpace(match.Type) == "" {
				return nil, fmt.Errorf("%s.event.match.type is required", path)
			}
			event.Match = match
			activation.Event = &event
		default:
			return nil, fmt.Errorf("%s must set schedule or event", path)
		}
		seen[activation.ID] = struct{}{}
		out = append(out, activation)
	}
	return out, nil
}

func normalizeWorkflowEventMatch(match *gestalt.WorkflowEventMatch) *gestalt.WorkflowEventMatch {
	if match == nil {
		return nil
	}
	return &gestalt.WorkflowEventMatch{
		Type:    strings.TrimSpace(match.Type),
		Source:  strings.TrimSpace(match.Source),
		Subject: strings.TrimSpace(match.Subject),
	}
}

func sortWorkflowDefinitions(definitions []*gestalt.WorkflowDefinition) {
	sort.SliceStable(definitions, func(i, j int) bool {
		left := definitions[i]
		right := definitions[j]
		if left == nil {
			return false
		}
		if right == nil {
			return true
		}
		if !left.CreatedAt.Equal(right.CreatedAt) {
			return left.CreatedAt.Before(right.CreatedAt)
		}
		return strings.TrimSpace(left.ID) < strings.TrimSpace(right.ID)
	})
}
