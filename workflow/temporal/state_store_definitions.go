package temporal

import (
	"context"
	"errors"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func temporalDefinitionSchema() gestalt.ObjectStoreOptions {
	return gestalt.ObjectStoreOptions{
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true},
			{Name: "scope_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "owner_key", Type: gestalt.TypeString},
			{Name: "created_at", Type: gestalt.TypeTime},
			{Name: "payload", Type: gestalt.TypeBytes, NotNull: true},
		},
	}
}

func (s *workflowStateStore) putDefinition(ctx context.Context, definition *gestalt.WorkflowDefinition) error {
	if definition == nil || strings.TrimSpace(definition.ID) == "" {
		return nil
	}
	return s.definitions.Put(ctx, s.definitionRecord(definition))
}

func (s *workflowStateStore) addDefinition(ctx context.Context, definition *gestalt.WorkflowDefinition) error {
	if definition == nil || strings.TrimSpace(definition.ID) == "" {
		return nil
	}
	return s.definitions.Add(ctx, s.definitionRecord(definition))
}

func (s *workflowStateStore) getDefinition(ctx context.Context, id string) (*gestalt.WorkflowDefinition, bool, error) {
	record, err := s.definitions.Get(ctx, s.scopedID(strings.TrimSpace(id)))
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	definition, err := definitionFromRecord(record)
	if err != nil {
		return nil, false, err
	}
	return definition, strings.TrimSpace(definition.ID) != "", nil
}

func (s *workflowStateStore) listDefinitions(ctx context.Context) ([]*gestalt.WorkflowDefinition, error) {
	records, err := s.definitions.GetAll(ctx, nil)
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	definitions := make([]*gestalt.WorkflowDefinition, 0, len(records))
	for _, record := range records {
		if recordString(record, "scope_id") != s.scopeID {
			continue
		}
		definition, err := definitionFromRecord(record)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(definition.ID) != "" {
			definitions = append(definitions, definition)
		}
	}
	return definitions, nil
}

func (s *workflowStateStore) matchEventActivations(ctx context.Context, event *gestalt.WorkflowEvent) ([]matchedWorkflowActivation, error) {
	definitions, err := s.listDefinitions(ctx)
	if err != nil {
		return nil, err
	}
	matches := make([]matchedWorkflowActivation, 0)
	for _, definition := range definitions {
		if definition == nil || definition.Paused {
			continue
		}
		for _, activation := range definition.Activations {
			if strings.TrimSpace(activation.ID) == "" || activation.Event == nil {
				continue
			}
			if !eventMatchesActivationInput(event, activation) {
				continue
			}
			matches = append(matches, matchedWorkflowActivation{
				Definition: cloneWorkflowDefinitionInput(definition),
				Activation: activation,
			})
		}
	}
	return matches, nil
}

func (s *workflowStateStore) deleteDefinition(ctx context.Context, id string) error {
	err := s.definitions.Delete(ctx, s.scopedID(strings.TrimSpace(id)))
	if errors.Is(err, gestalt.ErrNotFound) {
		return nil
	}
	return err
}

func (s *workflowStateStore) definitionRecord(definition *gestalt.WorkflowDefinition) gestalt.Record {
	payload := nativePayload(definition)
	now := time.Now().UTC()
	createdAt := definition.CreatedAt
	if createdAt.IsZero() {
		createdAt = now
	}
	return gestalt.Record{
		"id":         s.scopedID(definition.ID),
		"scope_id":   s.scopeID,
		"owner_key":  targetOwnerKeyInput(definition.Target),
		"created_at": createdAt.UTC(),
		"payload":    payload,
	}
}

func definitionFromRecord(record gestalt.Record) (*gestalt.WorkflowDefinition, error) {
	return decodeNativePayload[gestalt.WorkflowDefinition](recordBytes(record, "payload"), "workflow definition")
}
