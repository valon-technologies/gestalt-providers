package temporal

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type workflowStepProjector func(workflow.Context, gestalt.BoundWorkflowRun)

type workflowStepEvalContext struct {
	runInput        any
	workflowContext map[string]any
	signals         []gestalt.WorkflowSignal
	inputs          map[string]any
	stepOutputs     map[string]any
}

type missingWorkflowPath struct {
	path string
}

func executeWorkflowStepsV4(ctx workflow.Context, input runWorkflowV4Input, run gestalt.BoundWorkflowRun, signals []gestalt.WorkflowSignal, project workflowStepProjector) (gestalt.BoundWorkflowRun, error) {
	if run.Target == nil || len(run.Target.Steps) == 0 {
		return run, nil
	}
	if len(run.Steps) != len(run.Target.Steps) {
		run.Steps = initialWorkflowStepStates(run.Target, workflow.Now(ctx).UTC())
	}
	stepOutputs := map[string]any{}
	baseEval := workflowStepEvalContext{
		runInput:        input.RunInput,
		workflowContext: workflowContextValue(run),
		signals:         signals,
		stepOutputs:     stepOutputs,
	}
	for i, step := range run.Target.Steps {
		now := workflow.Now(ctx).UTC()
		if step.When != nil {
			ok, err := evaluateWorkflowStepWhen(step.When, baseEval)
			if err != nil {
				return failWorkflowStep(ctx, run, i, "when_evaluation_failed", err.Error(), project), nil
			}
			if !ok {
				run = updateWorkflowStepState(run, i, gestalt.WorkflowStepState{
					Status:        gestalt.WorkflowStepStatusValueSkipped,
					SkippedReason: "when evaluated false",
					AttemptNumber: 1,
					UpdatedAt:     &now,
				})
				project(ctx, run)
				continue
			}
		}
		inputs, err := evaluateWorkflowStepInputs(step.Inputs, baseEval)
		if err != nil {
			return failWorkflowStep(ctx, run, i, "input_evaluation_failed", err.Error(), project), nil
		}
		stepEval := baseEval
		stepEval.inputs = inputs
		run = updateWorkflowStepState(run, i, gestalt.WorkflowStepState{
			Status:        gestalt.WorkflowStepStatusValueRunning,
			AttemptNumber: 1,
			UpdatedAt:     &now,
		})
		project(ctx, run)

		resp, err := invokeWorkflowStepAction(ctx, input, run, step, stepEval, signals)
		if err != nil {
			return failWorkflowStep(ctx, run, i, "action_failed", err.Error(), project), nil
		}
		if actionFailed(resp) {
			return failWorkflowStep(ctx, run, i, "action_failed", workflowHostActionFailureMessage(resp), project), nil
		}
		if resp != nil {
			stepOutputs[step.ID] = workflowStepOutputValue(resp.Body)
		}
		if step.OutputDelivery != nil {
			deliveryResp, err := invokeWorkflowStepDelivery(ctx, input, run, step, stepEval, signals)
			if err != nil {
				return failWorkflowStep(ctx, run, i, "delivery_failed", err.Error(), project), nil
			}
			if actionFailed(deliveryResp) {
				return failWorkflowStep(ctx, run, i, "delivery_failed", workflowHostActionFailureMessage(deliveryResp), project), nil
			}
		}
		now = workflow.Now(ctx).UTC()
		run.ResultBody = ""
		if resp != nil {
			run.ResultBody = resp.Body
		}
		run = updateWorkflowStepState(run, i, gestalt.WorkflowStepState{
			Status:        gestalt.WorkflowStepStatusValueSucceeded,
			AttemptNumber: 1,
			OutputSummary: workflowHostActionOutputSummary(resp),
			OutputRef:     workflowHostActionOutputRef(resp),
			UpdatedAt:     &now,
		})
		project(ctx, run)
	}
	completedAt := workflow.Now(ctx).UTC()
	run.Status = gestalt.WorkflowRunStatusValueSucceeded
	run.CompletedAt = &completedAt
	run.StatusMessage = ""
	run.Error = nil
	return run, nil
}

func invokeWorkflowStepAction(ctx workflow.Context, input runWorkflowV4Input, run gestalt.BoundWorkflowRun, step gestalt.WorkflowStep, eval workflowStepEvalContext, signals []gestalt.WorkflowSignal) (*gestalt.WorkflowHostActionResponse, error) {
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: stepActivityTimeout(input, step),
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	switch {
	case step.Plugin != nil:
		body, err := evaluateWorkflowStepPluginInput(step.Plugin.Input, eval)
		if err != nil {
			return nil, err
		}
		req := gestalt.InvokeWorkflowActionInput{
			Selector: workflowStepSelector(input.PlanBinding, run.ID, run.ExecutionRef, step.ID, workflowStepActionID(step.ID, workflowStepPluginActionSuffix)),
			Plugin:   &gestalt.WorkflowPluginActionPayload{Input: body},
			Metadata: step.Metadata,
			Trigger:  run.Trigger,
			Signals:  signals,
		}
		var resp gestalt.WorkflowHostActionResponse
		err = workflow.ExecuteActivity(activityCtx, (*workflowActivities).InvokeWorkflowAction, req).Get(activityCtx, &resp)
		return &resp, err
	case step.Agent != nil:
		prompt, messages, err := evaluateWorkflowStepAgentTurn(step.Agent, eval)
		if err != nil {
			return nil, err
		}
		req := gestalt.InvokeWorkflowActionInput{
			Selector:  workflowStepSelector(input.PlanBinding, run.ID, run.ExecutionRef, step.ID, workflowStepActionID(step.ID, workflowStepAgentActionSuffix)),
			AgentTurn: &gestalt.WorkflowAgentTurnPayload{Prompt: prompt, Messages: messages},
			Metadata:  step.Metadata,
			Trigger:   run.Trigger,
			Signals:   signals,
		}
		var resp gestalt.WorkflowHostActionResponse
		err = workflow.ExecuteActivity(activityCtx, (*workflowActivities).InvokeWorkflowAction, req).Get(activityCtx, &resp)
		return &resp, err
	default:
		return nil, fmt.Errorf("step %q has no action", step.ID)
	}
}

func invokeWorkflowStepDelivery(ctx workflow.Context, input runWorkflowV4Input, run gestalt.BoundWorkflowRun, step gestalt.WorkflowStep, eval workflowStepEvalContext, signals []gestalt.WorkflowSignal) (*gestalt.WorkflowHostActionResponse, error) {
	if step.OutputDelivery == nil || step.OutputDelivery.Plugin == nil {
		return nil, nil
	}
	body, err := evaluateWorkflowStepPluginInput(step.OutputDelivery.Plugin.Input, eval)
	if err != nil {
		return nil, err
	}
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: stepActivityTimeout(input, step),
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	req := gestalt.InvokeWorkflowActionInput{
		Selector: workflowStepSelector(input.PlanBinding, run.ID, run.ExecutionRef, step.ID, workflowStepActionID(step.ID, workflowStepDeliveryActionSuffix)),
		Plugin:   &gestalt.WorkflowPluginActionPayload{Input: body},
		Metadata: step.Metadata,
		Trigger:  run.Trigger,
		Signals:  signals,
	}
	var resp gestalt.WorkflowHostActionResponse
	err = workflow.ExecuteActivity(activityCtx, (*workflowActivities).InvokeWorkflowAction, req).Get(activityCtx, &resp)
	return &resp, err
}

func workflowStepSelector(binding *gestalt.WorkflowPlanBinding, runID, runExecutionRef, stepID, actionID string) *gestalt.WorkflowHostActionSelector {
	binding = clonePlanBindingInput(binding)
	selector := &gestalt.WorkflowHostActionSelector{
		RunID:          strings.TrimSpace(runID),
		ExecutionRef:   strings.TrimSpace(runExecutionRef),
		StepID:         strings.TrimSpace(stepID),
		ActionID:       strings.TrimSpace(actionID),
		AttemptNumber:  1,
		IdempotencyKey: "workflow-step:" + hashID(runID, actionID, "1"),
	}
	if binding != nil {
		if selector.ExecutionRef == "" || selector.ExecutionRef == binding.ExecutionRef {
			selector.ExecutionRef = binding.ExecutionRef
			selector.ExecutionRefGeneration = binding.ExecutionRefGeneration
			selector.ExecutionRefSeal = binding.ExecutionRefSeal
		}
		selector.TargetDigest = binding.TargetDigest
		selector.ProviderPlanDigest = binding.ProviderPlanDigest
		if binding.IdempotencyKey != "" {
			selector.IdempotencyKey = "workflow-step:" + hashID(binding.ID, binding.IdempotencyKey, runID, actionID, "1")
		}
	}
	return selector
}

func stepActivityTimeout(input runWorkflowV4Input, step gestalt.WorkflowStep) time.Duration {
	if step.TimeoutSeconds > 0 {
		return time.Duration(step.TimeoutSeconds) * time.Second
	}
	return input.ActivityStartToCloseTimeoutNS
}

func evaluateWorkflowStepWhen(when *gestalt.WorkflowStepWhen, eval workflowStepEvalContext) (bool, error) {
	value, err := evaluateWorkflowValue(when.Value, eval, true)
	if err != nil {
		return false, err
	}
	if _, missing := value.(missingWorkflowPath); missing {
		return false, nil
	}
	return workflowValuesEqual(value, when.Equals), nil
}

func evaluateWorkflowStepInputs(values map[string]gestalt.WorkflowValue, eval workflowStepEvalContext) (map[string]any, error) {
	if len(values) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make(map[string]any, len(values))
	for _, key := range keys {
		value, err := evaluateWorkflowValue(values[key], eval, false)
		if err != nil {
			return nil, fmt.Errorf("inputs.%s: %w", key, err)
		}
		out[key] = value
	}
	return out, nil
}

func evaluateWorkflowStepPluginInput(value gestalt.WorkflowValue, eval workflowStepEvalContext) (map[string]any, error) {
	body, err := evaluateWorkflowValue(value, eval, false)
	if err != nil {
		return nil, err
	}
	if body == nil {
		return nil, nil
	}
	object, ok := body.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("plugin input must evaluate to an object")
	}
	return object, nil
}

func evaluateWorkflowStepAgentTurn(agent *gestalt.WorkflowStepAgentTurn, eval workflowStepEvalContext) (gestalt.WorkflowText, []gestalt.WorkflowAgentMessage, error) {
	if agent == nil {
		return gestalt.WorkflowText{}, nil, fmt.Errorf("agent is required")
	}
	prompt, err := evaluateWorkflowText(agent.Prompt, eval)
	if err != nil {
		return gestalt.WorkflowText{}, nil, fmt.Errorf("prompt: %w", err)
	}
	messages := make([]gestalt.WorkflowAgentMessage, 0, len(agent.Messages))
	for i, message := range agent.Messages {
		text, err := evaluateWorkflowText(message.Text, eval)
		if err != nil {
			return gestalt.WorkflowText{}, nil, fmt.Errorf("messages[%d].text: %w", i, err)
		}
		message.Text = text
		messages = append(messages, message)
	}
	return prompt, messages, nil
}

func evaluateWorkflowText(text gestalt.WorkflowText, eval workflowStepEvalContext) (gestalt.WorkflowText, error) {
	rendered, err := renderWorkflowTemplate(text.Template, eval)
	if err != nil {
		return gestalt.WorkflowText{}, err
	}
	return gestalt.WorkflowText{Template: rendered}, nil
}

func evaluateWorkflowValue(value gestalt.WorkflowValue, eval workflowStepEvalContext, allowMissing bool) (any, error) {
	switch {
	case value.LiteralSet:
		return value.Literal, nil
	case value.Object != nil:
		keys := make([]string, 0, len(value.Object))
		for key := range value.Object {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out := make(map[string]any, len(value.Object))
		for _, key := range keys {
			resolved, err := evaluateWorkflowValue(value.Object[key], eval, allowMissing)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", key, err)
			}
			out[key] = resolved
		}
		return out, nil
	case value.Array != nil:
		out := make([]any, 0, len(value.Array))
		for i := range value.Array {
			resolved, err := evaluateWorkflowValue(value.Array[i], eval, allowMissing)
			if err != nil {
				return nil, fmt.Errorf("[%d]: %w", i, err)
			}
			out = append(out, resolved)
		}
		return out, nil
	case value.Template != nil:
		return renderWorkflowTemplate(value.Template.Template, eval)
	case strings.TrimSpace(value.RunInput) != "":
		return lookupWorkflowPathSource(eval.runInput, value.RunInput, allowMissing)
	case strings.TrimSpace(value.SignalPayload) != "":
		payload, _ := latestWorkflowSignalPayload(eval.signals)
		return lookupWorkflowPathSource(payload, value.SignalPayload, allowMissing)
	case strings.TrimSpace(value.SignalMetadata) != "":
		metadata, _ := latestWorkflowSignalMetadata(eval.signals)
		return lookupWorkflowPathSource(metadata, value.SignalMetadata, allowMissing)
	case strings.TrimSpace(value.WorkflowContext) != "":
		return lookupWorkflowPathSource(eval.workflowContext, value.WorkflowContext, allowMissing)
	case value.StepOutput != nil:
		stepID := strings.TrimSpace(value.StepOutput.StepID)
		output, ok := eval.stepOutputs[stepID]
		if !ok {
			return handleMissingWorkflowPath("step_output."+stepID, allowMissing)
		}
		return lookupWorkflowPathSource(output, value.StepOutput.Path, allowMissing)
	default:
		return nil, nil
	}
}

func renderWorkflowTemplate(template string, eval workflowStepEvalContext) (string, error) {
	if template == "" {
		return "", nil
	}
	var b strings.Builder
	for {
		start := strings.Index(template, "${")
		if start < 0 {
			b.WriteString(template)
			return b.String(), nil
		}
		b.WriteString(template[:start])
		rest := template[start+2:]
		end := strings.Index(rest, "}")
		if end < 0 {
			return "", fmt.Errorf("unterminated template expression")
		}
		expr := strings.TrimSpace(rest[:end])
		value, ok := lookupWorkflowTemplatePath(workflowTemplateRoot(eval), expr)
		if !ok {
			return "", fmt.Errorf("missing template path %q", expr)
		}
		b.WriteString(workflowTemplateString(value))
		template = rest[end+1:]
	}
}

func lookupWorkflowPathSource(root any, path string, allowMissing bool) (any, error) {
	value, ok := lookupWorkflowPath(root, strings.TrimSpace(path))
	if !ok {
		return handleMissingWorkflowPath(path, allowMissing)
	}
	return value, nil
}

func handleMissingWorkflowPath(path string, allowMissing bool) (any, error) {
	if allowMissing {
		return missingWorkflowPath{path: strings.TrimSpace(path)}, nil
	}
	return nil, fmt.Errorf("missing path %q", strings.TrimSpace(path))
}

func lookupWorkflowTemplatePath(root map[string]any, path string) (any, bool) {
	return lookupWorkflowPath(root, path)
}

func lookupWorkflowPath(root any, path string) (any, bool) {
	value := normalizeWorkflowJSONValue(root)
	path = strings.TrimSpace(path)
	if path == "" {
		return value, true
	}
	for _, part := range strings.Split(path, ".") {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, false
		}
		switch typed := value.(type) {
		case map[string]any:
			next, ok := typed[part]
			if !ok {
				return nil, false
			}
			value = next
		case []any:
			index, err := strconv.Atoi(part)
			if err != nil || index < 0 || index >= len(typed) {
				return nil, false
			}
			value = typed[index]
		default:
			return nil, false
		}
	}
	return value, true
}

func workflowTemplateRoot(eval workflowStepEvalContext) map[string]any {
	payload, _ := latestWorkflowSignalPayload(eval.signals)
	metadata, _ := latestWorkflowSignalMetadata(eval.signals)
	return map[string]any{
		"inputs":          eval.inputs,
		"runInput":        eval.runInput,
		"signalPayload":   payload,
		"signalMetadata":  metadata,
		"signal":          map[string]any{"payload": payload, "metadata": metadata},
		"context":         eval.workflowContext,
		"workflowContext": eval.workflowContext,
		"stepOutput":      eval.stepOutputs,
	}
}

func workflowContextValue(run gestalt.BoundWorkflowRun) map[string]any {
	return map[string]any{
		"runID":              run.ID,
		"runId":              run.ID,
		"workflowKey":        run.WorkflowKey,
		"executionRef":       run.ExecutionRef,
		"targetDigest":       run.TargetDigest,
		"providerPlanDigest": run.ProviderPlanDigest,
		"run": map[string]any{
			"id":                 run.ID,
			"workflowKey":        run.WorkflowKey,
			"executionRef":       run.ExecutionRef,
			"targetDigest":       run.TargetDigest,
			"providerPlanDigest": run.ProviderPlanDigest,
		},
	}
}

func latestWorkflowSignalPayload(signals []gestalt.WorkflowSignal) (any, bool) {
	if len(signals) == 0 {
		return nil, false
	}
	return signals[len(signals)-1].Payload, true
}

func latestWorkflowSignalMetadata(signals []gestalt.WorkflowSignal) (any, bool) {
	if len(signals) == 0 {
		return nil, false
	}
	return signals[len(signals)-1].Metadata, true
}

func workflowTemplateString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case bool:
		if typed {
			return "true"
		}
		return "false"
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(typed), 'f', -1, 32)
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case int32:
		return strconv.FormatInt(int64(typed), 10)
	default:
		data, err := json.Marshal(normalizeWorkflowJSONValue(value))
		if err != nil {
			return fmt.Sprint(value)
		}
		return string(data)
	}
}

func normalizeWorkflowJSONValue(value any) any {
	if value == nil {
		return nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var out any
	if err := json.Unmarshal(data, &out); err != nil {
		return value
	}
	return out
}

func workflowValuesEqual(left, right any) bool {
	return reflect.DeepEqual(normalizeWorkflowJSONValue(left), normalizeWorkflowJSONValue(right))
}

func workflowStepOutputValue(body string) any {
	body = strings.TrimSpace(body)
	if body == "" {
		return nil
	}
	var out any
	if err := json.Unmarshal([]byte(body), &out); err == nil {
		return out
	}
	return body
}

func actionFailed(resp *gestalt.WorkflowHostActionResponse) bool {
	if resp == nil {
		return false
	}
	return resp.Error != nil || resp.Status >= http.StatusBadRequest
}

func workflowHostActionFailureMessage(resp *gestalt.WorkflowHostActionResponse) string {
	if resp == nil {
		return "workflow host action failed"
	}
	if resp.Error != nil && strings.TrimSpace(resp.Error.Message) != "" {
		return resp.Error.Message
	}
	if resp.Status >= http.StatusBadRequest {
		return fmt.Sprintf("workflow host action returned status %d", resp.Status)
	}
	return "workflow host action failed"
}

func workflowHostActionOutputSummary(resp *gestalt.WorkflowHostActionResponse) *gestalt.WorkflowOutputSummary {
	if resp == nil || resp.OutputSummary == nil {
		return nil
	}
	out := *resp.OutputSummary
	return &out
}

func workflowHostActionOutputRef(resp *gestalt.WorkflowHostActionResponse) string {
	if resp == nil {
		return ""
	}
	return strings.TrimSpace(resp.OutputRef)
}

func failWorkflowStep(ctx workflow.Context, run gestalt.BoundWorkflowRun, index int, code, message string, project workflowStepProjector) gestalt.BoundWorkflowRun {
	now := workflow.Now(ctx).UTC()
	stepID := ""
	if run.Target != nil && index >= 0 && index < len(run.Target.Steps) {
		stepID = strings.TrimSpace(run.Target.Steps[index].ID)
	}
	err := &gestalt.WorkflowRunError{
		Code:    strings.TrimSpace(code),
		Message: strings.TrimSpace(message),
		StepID:  stepID,
	}
	run.Status = gestalt.WorkflowRunStatusValueFailed
	run.CompletedAt = &now
	run.StatusMessage = err.Message
	run.Error = err
	run = updateWorkflowStepState(run, index, gestalt.WorkflowStepState{
		Status:        gestalt.WorkflowStepStatusValueFailed,
		AttemptNumber: 1,
		Error:         err,
		UpdatedAt:     &now,
	})
	project(ctx, run)
	return run
}

func updateWorkflowStepState(run gestalt.BoundWorkflowRun, index int, patch gestalt.WorkflowStepState) gestalt.BoundWorkflowRun {
	if index < 0 {
		return run
	}
	if len(run.Steps) <= index {
		run.Steps = initialWorkflowStepStates(run.Target, time.Time{})
	}
	if len(run.Steps) <= index {
		return run
	}
	current := run.Steps[index]
	if current.StepID == "" && run.Target != nil && index < len(run.Target.Steps) {
		current.StepID = strings.TrimSpace(run.Target.Steps[index].ID)
	}
	current.StepIndex = int32(index)
	if patch.Status != gestalt.WorkflowStepStatusValueUnspecified {
		current.Status = patch.Status
	}
	if patch.SkippedReason != "" {
		current.SkippedReason = patch.SkippedReason
	}
	if patch.AttemptNumber != 0 {
		current.AttemptNumber = patch.AttemptNumber
	}
	if patch.OutputSummary != nil {
		current.OutputSummary = patch.OutputSummary
	}
	if patch.OutputRef != "" {
		current.OutputRef = patch.OutputRef
	}
	if patch.Error != nil {
		current.Error = patch.Error
	}
	if patch.UpdatedAt != nil {
		current.UpdatedAt = patch.UpdatedAt
	}
	run.Steps[index] = current
	return run
}

func cancelPendingSteps(steps []gestalt.WorkflowStepState, now time.Time, reason string) {
	for i := range steps {
		switch steps[i].Status {
		case gestalt.WorkflowStepStatusValueSucceeded,
			gestalt.WorkflowStepStatusValueFailed,
			gestalt.WorkflowStepStatusValueSkipped,
			gestalt.WorkflowStepStatusValueCanceled:
			continue
		default:
			updatedAt := now.UTC()
			steps[i].Status = gestalt.WorkflowStepStatusValueCanceled
			steps[i].SkippedReason = strings.TrimSpace(reason)
			steps[i].UpdatedAt = &updatedAt
		}
	}
}

func planBindingMatches(left, right *gestalt.WorkflowPlanBinding) bool {
	left = clonePlanBindingInput(left)
	right = clonePlanBindingInput(right)
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.ID == right.ID &&
		left.ExecutionRef == right.ExecutionRef &&
		left.ExecutionRefGeneration == right.ExecutionRefGeneration &&
		left.ExecutionRefSeal == right.ExecutionRefSeal &&
		left.TargetDigest == right.TargetDigest &&
		left.ProviderPlanDigest == right.ProviderPlanDigest
}
