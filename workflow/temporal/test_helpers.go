package temporal

import (
	"strings"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func nativePluginTargetInput(app, operation string) *gestalt.BoundWorkflowTarget {
	return &gestalt.BoundWorkflowTarget{Steps: []gestalt.WorkflowStep{{
		ID: strings.TrimSpace(operation),
		App: &gestalt.WorkflowStepAppCall{
			Name:      strings.TrimSpace(app),
			Operation: strings.TrimSpace(operation),
		},
	}}}
}

func nativePluginTargetInputWithObject(app, operation string, input map[string]any) *gestalt.BoundWorkflowTarget {
	target := nativePluginTargetInput(app, operation)
	if len(input) > 0 && len(target.Steps) > 0 && target.Steps[0].App != nil {
		fields := make(map[string]gestalt.WorkflowValue, len(input))
		for key, value := range input {
			fields[key] = gestalt.WorkflowValue{Literal: value, LiteralSet: true}
		}
		target.Steps[0].App.Input = gestalt.WorkflowValue{Object: fields}
	}
	return target
}

func pluginTarget(app, operation string) *gestalt.BoundWorkflowTarget {
	return nativePluginTargetInputWithObject(app, operation, map[string]any{"text": "hello"})
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
