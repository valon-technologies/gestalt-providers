package indexeddb

import (
	"strings"
	"testing"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func workflowTarget(t *testing.T, appName, operation string, input map[string]any) *gestalt.BoundWorkflowTarget {
	t.Helper()
	target := &gestalt.BoundWorkflowTarget{Steps: []gestalt.WorkflowStep{{
		ID: strings.TrimSpace(operation),
		App: &gestalt.WorkflowStepAppCall{
			Name:      appName,
			Operation: operation,
		},
	}}}
	if len(input) > 0 {
		fields := make(map[string]gestalt.WorkflowValue, len(input))
		for key, value := range input {
			fields[key] = gestalt.WorkflowValue{Literal: value, LiteralSet: true}
		}
		target.Steps[0].App.Input = gestalt.WorkflowValue{Object: fields}
	}
	return target
}

func workflowAgentTarget(providerName, model, prompt string) *gestalt.BoundWorkflowTarget {
	return &gestalt.BoundWorkflowTarget{Steps: []gestalt.WorkflowStep{{
		ID: "run",
		Agent: &gestalt.WorkflowStepAgentTurn{
			Provider: providerName,
			Model:    model,
			Prompt:   gestalt.WorkflowText{Template: prompt},
			Tools: []gestalt.AgentToolRef{
				{App: "slack", Operation: "chat.postMessage"},
				{App: "linear"},
			},
		},
	}}}
}

func workflowAgentTargetWithTools(providerName, model, prompt string, tools []gestalt.AgentToolRef) *gestalt.BoundWorkflowTarget {
	return &gestalt.BoundWorkflowTarget{Steps: []gestalt.WorkflowStep{{
		ID: "run",
		Agent: &gestalt.WorkflowStepAgentTurn{
			Provider: providerName,
			Model:    model,
			Prompt:   gestalt.WorkflowText{Template: prompt},
			Tools:    tools,
		},
	}}}
}

func testAppStep(target *gestalt.BoundWorkflowTarget) *gestalt.WorkflowStepAppCall {
	return firstWorkflowAppStep(target)
}

func testAgentStep(target *gestalt.BoundWorkflowTarget) *gestalt.WorkflowStepAgentTurn {
	agent, _ := firstWorkflowAgentStep(target)
	return agent
}

func workflowValueObjectField(target *gestalt.BoundWorkflowTarget, key string) any {
	app := firstWorkflowAppStep(target)
	if app == nil || app.Input.Object == nil {
		return nil
	}
	value, ok := app.Input.Object[key]
	if !ok || !value.LiteralSet {
		return nil
	}
	return value.Literal
}
