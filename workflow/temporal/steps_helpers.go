package temporal

import (
	"fmt"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

const (
	workflowStepPluginActionSuffix   = "plugin"
	workflowStepAgentActionSuffix    = "agent-turn"
	workflowStepDeliveryActionSuffix = "delivery"
)

func normalizeWorkflowSteps(steps []gestalt.WorkflowStep) ([]gestalt.WorkflowStep, error) {
	out := make([]gestalt.WorkflowStep, 0, len(steps))
	seen := map[string]struct{}{}
	for i := range steps {
		step, err := normalizeWorkflowStep(steps[i], i, seen)
		if err != nil {
			return nil, err
		}
		seen[step.ID] = struct{}{}
		out = append(out, step)
	}
	return out, nil
}

func normalizeWorkflowStep(step gestalt.WorkflowStep, index int, seen map[string]struct{}) (gestalt.WorkflowStep, error) {
	step.ID = strings.TrimSpace(step.ID)
	if !validWorkflowStepID(step.ID) {
		return gestalt.WorkflowStep{}, fmt.Errorf("target.steps[%d].id is required and may contain only letters, numbers, _, -, or .", index)
	}
	if _, ok := seen[step.ID]; ok {
		return gestalt.WorkflowStep{}, fmt.Errorf("target.steps[%d].id %q is duplicated", index, step.ID)
	}
	if step.TimeoutSeconds < 0 {
		return gestalt.WorkflowStep{}, fmt.Errorf("target.steps[%d].timeout_seconds must not be negative", index)
	}
	actions := 0
	if step.Plugin != nil {
		actions++
		plugin, err := normalizeWorkflowStepPluginCall(step.Plugin, fmt.Sprintf("target.steps[%d].plugin", index))
		if err != nil {
			return gestalt.WorkflowStep{}, err
		}
		step.Plugin = plugin
	}
	if step.Agent != nil {
		actions++
		agent := *step.Agent
		agent.Provider = strings.TrimSpace(agent.Provider)
		agent.Model = strings.TrimSpace(agent.Model)
		agent.SessionKey = strings.TrimSpace(agent.SessionKey)
		step.Agent = &agent
	}
	if actions != 1 {
		return gestalt.WorkflowStep{}, fmt.Errorf("target.steps[%d] must set exactly one action", index)
	}
	step.Inputs = cloneWorkflowValueMap(step.Inputs)
	step.When = cloneWorkflowStepWhen(step.When)
	if step.OutputDelivery != nil {
		delivery := *step.OutputDelivery
		plugin, err := normalizeWorkflowStepPluginCall(delivery.Plugin, fmt.Sprintf("target.steps[%d].output_delivery.plugin", index))
		if err != nil {
			return gestalt.WorkflowStep{}, err
		}
		delivery.Plugin = plugin
		step.OutputDelivery = &delivery
	}
	return step, nil
}

func normalizeWorkflowStepPluginCall(input *gestalt.WorkflowStepPluginCall, fieldName string) (*gestalt.WorkflowStepPluginCall, error) {
	if input == nil {
		return nil, fmt.Errorf("%s is required", fieldName)
	}
	out := *input
	out.Name = strings.TrimSpace(out.Name)
	out.Operation = strings.TrimSpace(out.Operation)
	out.Connection = strings.TrimSpace(out.Connection)
	out.Instance = strings.TrimSpace(out.Instance)
	out.CredentialMode = strings.ToLower(strings.TrimSpace(out.CredentialMode))
	if out.Name == "" {
		return nil, fmt.Errorf("%s.name is required", fieldName)
	}
	if out.Operation == "" {
		return nil, fmt.Errorf("%s.operation is required", fieldName)
	}
	switch out.CredentialMode {
	case "", "none", "user":
	default:
		return nil, fmt.Errorf("%s.credential_mode %q is not supported", fieldName, input.CredentialMode)
	}
	out.Input = cloneWorkflowValue(out.Input)
	return &out, nil
}

func validWorkflowStepID(id string) bool {
	if strings.TrimSpace(id) == "" {
		return false
	}
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '_' || r == '-' || r == '.':
		default:
			return false
		}
	}
	return true
}

func workflowStepActionID(stepID, suffix string) string {
	if !validWorkflowStepID(stepID) {
		return ""
	}
	switch suffix {
	case workflowStepPluginActionSuffix, workflowStepAgentActionSuffix, workflowStepDeliveryActionSuffix:
		return "step/" + strings.TrimSpace(stepID) + "/" + suffix
	default:
		return ""
	}
}

func initialWorkflowStepStates(target *gestalt.BoundWorkflowTarget, now time.Time) []gestalt.WorkflowStepState {
	if target == nil || len(target.Steps) == 0 {
		return nil
	}
	out := make([]gestalt.WorkflowStepState, 0, len(target.Steps))
	for i, step := range target.Steps {
		state := gestalt.WorkflowStepState{
			StepID:        strings.TrimSpace(step.ID),
			StepIndex:     int32(i),
			Status:        gestalt.WorkflowStepStatusValuePending,
			AttemptNumber: 1,
		}
		if !now.IsZero() {
			updatedAt := now.UTC()
			state.UpdatedAt = &updatedAt
		}
		out = append(out, state)
	}
	return out
}

func cloneWorkflowValueMap(values map[string]gestalt.WorkflowValue) map[string]gestalt.WorkflowValue {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]gestalt.WorkflowValue, len(values))
	for key, value := range values {
		out[strings.TrimSpace(key)] = cloneWorkflowValue(value)
	}
	return out
}

func cloneWorkflowStepWhen(when *gestalt.WorkflowStepWhen) *gestalt.WorkflowStepWhen {
	if when == nil {
		return nil
	}
	out := *when
	out.Value = cloneWorkflowValue(out.Value)
	return &out
}

func cloneWorkflowValue(value gestalt.WorkflowValue) gestalt.WorkflowValue {
	out := value
	out.RunInput = strings.TrimSpace(out.RunInput)
	out.SignalPayload = strings.TrimSpace(out.SignalPayload)
	out.SignalMetadata = strings.TrimSpace(out.SignalMetadata)
	out.WorkflowContext = strings.TrimSpace(out.WorkflowContext)
	if out.Object != nil {
		out.Object = cloneWorkflowValueMap(out.Object)
	}
	if out.Array != nil {
		out.Array = make([]gestalt.WorkflowValue, len(value.Array))
		for i := range value.Array {
			out.Array[i] = cloneWorkflowValue(value.Array[i])
		}
	}
	if out.Template != nil {
		text := *out.Template
		out.Template = &text
	}
	if out.StepOutput != nil {
		source := *out.StepOutput
		source.StepID = strings.TrimSpace(source.StepID)
		source.Path = strings.TrimSpace(source.Path)
		out.StepOutput = &source
	}
	return out
}
