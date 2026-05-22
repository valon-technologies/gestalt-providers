package temporal

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

func (b *temporalBackend) CreateDefinition(ctx context.Context, req *gestalt.CreateWorkflowProviderDefinitionRequest) (*gestalt.BoundWorkflowDefinition, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	target, err := normalizeTarget(req.Target)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	key := strings.TrimSpace(req.IdempotencyKey)
	definitionID := uuid.NewString()
	if key != "" {
		definitionID = idempotentDefinitionID(target.OwnerKey, key)
	}
	if key != "" {
		existing, found, err := b.state.getDefinition(ctx, definitionID)
		if err != nil {
			return nil, err
		}
		if found {
			return cloneWorkflowDefinitionInput(existing), nil
		}
	}
	definition := &gestalt.BoundWorkflowDefinition{
		ID:           definitionID,
		Target:       target.Target,
		CreatedAt:    time.Now().UTC(),
		ProviderName: b.providerName,
	}
	if err := b.state.addDefinition(ctx, definition); err != nil {
		if key != "" && errors.Is(err, gestalt.ErrAlreadyExists) {
			existing, found, loadErr := b.state.getDefinition(ctx, definitionID)
			if loadErr != nil {
				return nil, loadErr
			}
			if found {
				return cloneWorkflowDefinitionInput(existing), nil
			}
		}
		return nil, err
	}
	return cloneWorkflowDefinitionInput(definition), nil
}

func (b *temporalBackend) GetDefinition(ctx context.Context, req *gestalt.GetWorkflowProviderDefinitionRequest) (*gestalt.BoundWorkflowDefinition, error) {
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

func (b *temporalBackend) UpdateDefinition(ctx context.Context, req *gestalt.UpdateWorkflowProviderDefinitionRequest) (*gestalt.BoundWorkflowDefinition, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	definitionID := strings.TrimSpace(req.DefinitionID)
	if definitionID == "" {
		return nil, status.Error(codes.InvalidArgument, "definition_id is required")
	}
	target, err := normalizeTarget(req.Target)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	existing, found, err := b.state.getDefinition(ctx, definitionID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow definition %q not found", definitionID)
	}
	definition := &gestalt.BoundWorkflowDefinition{
		ID:           definitionID,
		Target:       target.Target,
		CreatedBy:    cloneActorInput(existing.CreatedBy),
		CreatedAt:    existing.CreatedAt,
		ProviderName: b.providerName,
	}
	if definition.CreatedAt.IsZero() {
		definition.CreatedAt = time.Now().UTC()
	}
	if err := b.state.putDefinition(ctx, definition); err != nil {
		return nil, err
	}
	return cloneWorkflowDefinitionInput(definition), nil
}

func (b *temporalBackend) DeleteDefinition(ctx context.Context, req *gestalt.DeleteWorkflowProviderDefinitionRequest) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}
	definitionID := strings.TrimSpace(req.DefinitionID)
	if definitionID == "" {
		return status.Error(codes.InvalidArgument, "definition_id is required")
	}
	if _, found, err := b.state.getDefinition(ctx, definitionID); err != nil {
		return err
	} else if !found {
		return status.Errorf(codes.NotFound, "workflow definition %q not found", definitionID)
	}
	return b.state.deleteDefinition(ctx, definitionID)
}
