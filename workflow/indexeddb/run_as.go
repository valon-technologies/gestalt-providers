package indexeddb

import (
	"errors"
	"fmt"
	"strings"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func cloneRunAsID(runAs string) string {
	return strings.TrimSpace(runAs)
}

func runAsFromSubject(subject *gestalt.Subject) string {
	if subject == nil {
		return ""
	}
	return cloneRunAsID(subject.ID)
}

func runAsToSubject(runAs string) *gestalt.Subject {
	runAs = cloneRunAsID(runAs)
	if runAs == "" {
		return nil
	}
	return &gestalt.Subject{ID: runAs}
}

func runAsFromAny(value any) string {
	switch typed := value.(type) {
	case string:
		return cloneRunAsID(typed)
	case map[string]any:
		if subjectID := cloneRunAsID(stringField(typed, "id")); subjectID != "" {
			return subjectID
		}
		if nested, ok := typed["subject"].(map[string]any); ok {
			return cloneRunAsID(stringField(nested, "id"))
		}
	}
	return ""
}

func validateWorkflowRunAs(runAs string) error {
	if cloneRunAsID(runAs) == "" {
		return errors.New("run_as is required")
	}
	return nil
}

func validateWorkflowActivationRunAs(activations []gestalt.WorkflowActivation, runAs string) error {
	for _, activation := range activations {
		if activation.Event == nil && activation.Schedule == nil {
			continue
		}
		if err := validateWorkflowRunAs(runAs); err != nil {
			return fmt.Errorf("activation %q run_as: %w", activation.ID, err)
		}
	}
	return nil
}
