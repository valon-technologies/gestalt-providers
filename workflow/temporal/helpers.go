package temporal

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	gestaltworkflow "github.com/valon-technologies/gestalt/sdk/go/workflow"
)

const (
	defaultSpecVersion = "1.0"
	defaultTimezone    = "UTC"

	configManagedWorkflowSubject = "system:config"
)

type scopedTarget struct {
	OwnerKey string
	Target   *gestalt.BoundWorkflowTarget
}

const runHandleKindV4 = "temporal-run-v4"

type temporalRunHandle struct {
	Kind             string `json:"kind"`
	RunWorkflowID    string `json:"run_workflow_id"`
	RunTemporalRunID string `json:"run_temporal_run_id,omitempty"`
	WorkflowKey      string `json:"workflow_key,omitempty"`
	OwnerKey         string `json:"owner_key,omitempty"`
}

func encodeTemporalRunHandle(handle temporalRunHandle) string {
	handle.Kind = strings.TrimSpace(handle.Kind)
	if handle.Kind == "" {
		handle.Kind = runHandleKindV4
	}
	handle.RunWorkflowID = strings.TrimSpace(handle.RunWorkflowID)
	handle.RunTemporalRunID = strings.TrimSpace(handle.RunTemporalRunID)
	handle.WorkflowKey = strings.TrimSpace(handle.WorkflowKey)
	handle.OwnerKey = strings.TrimSpace(handle.OwnerKey)
	payload, _ := json.Marshal(handle)
	return base64.RawURLEncoding.EncodeToString(payload)
}

func decodeTemporalRunHandle(id string) (*temporalRunHandle, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, errors.New("run_id is required")
	}
	data, err := base64.RawURLEncoding.DecodeString(id)
	if err != nil {
		return nil, fmt.Errorf("run_id is not a temporal workflow handle")
	}
	var handle temporalRunHandle
	if err := json.Unmarshal(data, &handle); err != nil {
		return nil, fmt.Errorf("run_id is not a temporal workflow handle")
	}
	handle.Kind = strings.TrimSpace(handle.Kind)
	handle.RunWorkflowID = strings.TrimSpace(handle.RunWorkflowID)
	handle.RunTemporalRunID = strings.TrimSpace(handle.RunTemporalRunID)
	handle.WorkflowKey = strings.TrimSpace(handle.WorkflowKey)
	handle.OwnerKey = strings.TrimSpace(handle.OwnerKey)
	if handle.Kind != runHandleKindV4 {
		return nil, fmt.Errorf("run_id is not a supported temporal workflow handle")
	}
	if handle.RunWorkflowID == "" {
		return nil, fmt.Errorf("run_id is missing run_workflow_id")
	}
	return &handle, nil
}

func hashID(parts ...string) string {
	h := sha256.New()
	for i, part := range parts {
		if i > 0 {
			_, _ = h.Write([]byte{0})
		}
		_, _ = h.Write([]byte(strings.TrimSpace(part)))
	}
	sum := h.Sum(nil)
	return hex.EncodeToString(sum[:16])
}

func workflowInvokeMetadataInput(workflowKey, definitionID string) map[string]any {
	workflowKey = strings.TrimSpace(workflowKey)
	definitionID = strings.TrimSpace(definitionID)
	if workflowKey == "" && definitionID == "" {
		return nil
	}
	metadata := map[string]any{}
	if workflowKey != "" {
		metadata[workflowInvokeMetadataWorkflowKey] = workflowKey
	}
	if definitionID != "" {
		metadata[workflowInvokeMetadataDefinitionID] = definitionID
	}
	return metadata
}

func valueHashID(value any) string {
	if value == nil {
		return ""
	}
	data, err := json.Marshal(value)
	if err != nil {
		return hashID("json-marshal-error", err.Error())
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:16])
}

func workflowID(scopeID, kind string, parts ...string) string {
	values := append([]string{scopeID, kind}, parts...)
	return "gestalt/" + hashID("scope", scopeID) + "/" + strings.Trim(strings.ReplaceAll(kind, " ", "-"), "/") + "/" + hashID(values...)
}

func normalizeTarget(target *gestalt.BoundWorkflowTarget) (scopedTarget, error) {
	if target == nil {
		return scopedTarget{}, errors.New("target is required")
	}
	if len(target.Steps) == 0 {
		return scopedTarget{}, errors.New("target.steps is required")
	}
	steps := append([]gestalt.WorkflowStep(nil), target.Steps...)
	seen := map[string]struct{}{}
	ownerKey := ""
	for i := range steps {
		step := &steps[i]
		stepPath := fmt.Sprintf("target.steps[%d]", i)
		step.ID = strings.TrimSpace(step.ID)
		if step.ID == "" {
			return scopedTarget{}, fmt.Errorf("%s.id is required", stepPath)
		}
		if _, exists := seen[step.ID]; exists {
			return scopedTarget{}, fmt.Errorf("%s.id duplicates %q", stepPath, step.ID)
		}
		if step.TimeoutSeconds < 0 {
			return scopedTarget{}, fmt.Errorf("%s.timeout_seconds must not be negative", stepPath)
		}
		switch {
		case step.App != nil && step.Agent != nil:
			return scopedTarget{}, fmt.Errorf("%s must set exactly one of app or agent", stepPath)
		case step.App != nil:
			app, stepOwner, err := normalizeWorkflowStepApp(step.App, stepPath+".app")
			if err != nil {
				return scopedTarget{}, err
			}
			step.App = app
			if ownerKey == "" {
				ownerKey = stepOwner
			}
		case step.Agent != nil:
			agent, stepOwner, err := normalizeWorkflowStepAgent(step.Agent, stepPath+".agent")
			if err != nil {
				return scopedTarget{}, err
			}
			step.Agent = agent
			if ownerKey == "" {
				ownerKey = stepOwner
			}
		default:
			return scopedTarget{}, fmt.Errorf("%s must set app or agent", stepPath)
		}
		seen[step.ID] = struct{}{}
	}
	if ownerKey == "" {
		return scopedTarget{}, errors.New("target owner is required")
	}
	return scopedTarget{
		OwnerKey: ownerKey,
		Target:   &gestalt.BoundWorkflowTarget{Steps: steps},
	}, nil
}

func normalizeWorkflowStepApp(app *gestalt.WorkflowStepAppCall, path string) (*gestalt.WorkflowStepAppCall, string, error) {
	if app == nil {
		return nil, "", fmt.Errorf("%s is required", path)
	}
	out := *app
	appName := strings.TrimSpace(out.Name)
	operation := strings.TrimSpace(out.Operation)
	if appName == "" {
		return nil, "", fmt.Errorf("%s.name is required", path)
	}
	if operation == "" {
		return nil, "", fmt.Errorf("%s.operation is required", path)
	}
	credentialMode := strings.ToLower(strings.TrimSpace(out.CredentialMode))
	switch credentialMode {
	case "", "none", "subject":
	default:
		return nil, "", fmt.Errorf("%s.credential_mode %q is not supported", path, out.CredentialMode)
	}
	out.Name = appName
	out.Operation = operation
	out.Connection = strings.TrimSpace(out.Connection)
	out.Instance = strings.TrimSpace(out.Instance)
	out.CredentialMode = credentialMode
	return &out, appName, nil
}

func normalizeWorkflowStepAgent(agent *gestalt.WorkflowStepAgentTurn, path string) (*gestalt.WorkflowStepAgentTurn, string, error) {
	if agent == nil {
		return nil, "", fmt.Errorf("%s is required", path)
	}
	out := *agent
	providerName := strings.TrimSpace(out.Provider)
	out.Model = strings.TrimSpace(out.Model)
	out.SessionKey = strings.TrimSpace(out.SessionKey)
	out.Prompt = gestalt.WorkflowText{Template: strings.TrimSpace(out.Prompt.Template)}
	if providerName == "" {
		return nil, "", fmt.Errorf("%s.provider is required", path)
	}
	if out.Prompt.Template == "" && len(out.Messages) == 0 {
		return nil, "", fmt.Errorf("%s.prompt or messages is required", path)
	}
	out.Provider = providerName
	return &out, "agent:" + providerName, nil
}

func targetOwnerKeyInput(target *gestalt.BoundWorkflowTarget) string {
	if target == nil || len(target.Steps) == 0 {
		return ""
	}
	for _, step := range target.Steps {
		if step.App != nil {
			if appName := strings.TrimSpace(step.App.Name); appName != "" {
				return appName
			}
		}
		if step.Agent != nil {
			if provider := strings.TrimSpace(step.Agent.Provider); provider != "" {
				return "agent:" + provider
			}
		}
	}
	return ""
}

func firstWorkflowAppStep(target *gestalt.BoundWorkflowTarget) *gestalt.WorkflowStepAppCall {
	if target == nil {
		return nil
	}
	for i := range target.Steps {
		if target.Steps[i].App != nil {
			return target.Steps[i].App
		}
	}
	return nil
}

func firstWorkflowAgentStep(target *gestalt.BoundWorkflowTarget) (*gestalt.WorkflowStepAgentTurn, *gestalt.WorkflowStep) {
	if target == nil {
		return nil, nil
	}
	for i := range target.Steps {
		if target.Steps[i].Agent != nil {
			step := target.Steps[i]
			return step.Agent, &step
		}
	}
	return nil, nil
}

func targetHasAppStep(target *gestalt.BoundWorkflowTarget) bool {
	return firstWorkflowAppStep(target) != nil
}

func targetHasAgentStep(target *gestalt.BoundWorkflowTarget) bool {
	agent, _ := firstWorkflowAgentStep(target)
	return agent != nil
}

func normalizeWorkflowEvent(event *gestalt.WorkflowEvent, now func() time.Time) (*gestalt.WorkflowEvent, error) {
	if event == nil {
		return nil, errors.New("event is required")
	}
	source := strings.TrimSpace(event.Source)
	if source == "" {
		return nil, errors.New("event.source is required")
	}
	eventType := strings.TrimSpace(event.Type)
	if eventType == "" {
		return nil, errors.New("event.type is required")
	}
	specVersion := strings.TrimSpace(event.SpecVersion)
	if specVersion == "" {
		specVersion = defaultSpecVersion
	}
	eventTime := now().UTC()
	if !event.Time.IsZero() {
		eventTime = event.Time.UTC()
	}
	normalized := &gestalt.WorkflowEvent{
		ID:              strings.TrimSpace(event.ID),
		Source:          source,
		SpecVersion:     specVersion,
		Type:            eventType,
		Subject:         strings.TrimSpace(event.Subject),
		Time:            eventTime,
		DataContentType: strings.TrimSpace(event.DataContentType),
		Data:            event.Data,
		Extensions:      event.Extensions,
	}
	return cloneWorkflowEventInput(normalized), nil
}

func cloneWorkflowEventInput(event *gestalt.WorkflowEvent) *gestalt.WorkflowEvent {
	if event == nil {
		return nil
	}
	data, err := json.Marshal(event)
	if err != nil {
		out := *event
		return &out
	}
	var out gestalt.WorkflowEvent
	if err := json.Unmarshal(data, &out); err != nil {
		out := *event
		return &out
	}
	return &out
}

func cloneWorkflowDefinitionInput(definition *gestalt.WorkflowDefinition) *gestalt.WorkflowDefinition {
	if definition == nil {
		return nil
	}
	data, err := json.Marshal(definition)
	if err != nil {
		out := *definition
		return &out
	}
	var out gestalt.WorkflowDefinition
	if err := json.Unmarshal(data, &out); err != nil {
		out := *definition
		return &out
	}
	return &out
}

func cloneBoundWorkflowTargetInput(target *gestalt.BoundWorkflowTarget) *gestalt.BoundWorkflowTarget {
	if target == nil {
		return nil
	}
	data, err := json.Marshal(target)
	if err != nil {
		out := *target
		return &out
	}
	var out gestalt.BoundWorkflowTarget
	if err := json.Unmarshal(data, &out); err != nil {
		out := *target
		return &out
	}
	return &out
}

func normalizeWorkflowSignalInput(signal *gestalt.WorkflowSignal, now time.Time) (*gestalt.WorkflowSignal, error) {
	if signal == nil {
		return nil, errors.New("signal is required")
	}
	out := *signal
	name := strings.TrimSpace(out.Name)
	if name == "" {
		return nil, errors.New("signal.name is required")
	}
	if out.CreatedAt.IsZero() {
		out.CreatedAt = now.UTC()
	} else {
		out.CreatedAt = out.CreatedAt.UTC()
	}
	out.ID = strings.TrimSpace(out.ID)
	out.Name = name
	out.CreatedBySubjectID = cloneCreatedBySubjectID(out.CreatedBySubjectID)
	out.IdempotencyKey = strings.TrimSpace(out.IdempotencyKey)
	return &out, nil
}

func cloneRunInput(run *gestalt.WorkflowRun) *gestalt.WorkflowRun {
	if run == nil {
		return nil
	}
	out := *run
	out.ID = strings.TrimSpace(out.ID)
	out.StatusMessage = strings.TrimSpace(out.StatusMessage)
	out.WorkflowKey = strings.TrimSpace(out.WorkflowKey)
	out.DefinitionID = strings.TrimSpace(out.DefinitionID)
	out.CreatedBySubjectID = cloneCreatedBySubjectID(out.CreatedBySubjectID)
	return &out
}

func cloneSignalInput(signal *gestalt.WorkflowSignal) *gestalt.WorkflowSignal {
	if signal == nil {
		return nil
	}
	out := *signal
	out.ID = strings.TrimSpace(out.ID)
	out.Name = strings.TrimSpace(out.Name)
	out.IdempotencyKey = strings.TrimSpace(out.IdempotencyKey)
	out.CreatedBySubjectID = cloneCreatedBySubjectID(out.CreatedBySubjectID)
	return &out
}

func eventMatchesActivationInput(event *gestalt.WorkflowEvent, activation gestalt.WorkflowActivation) bool {
	if event == nil || activation.Paused || activation.Event == nil || activation.Event.Match == nil {
		return false
	}
	match := activation.Event.Match
	if strings.TrimSpace(event.Type) != strings.TrimSpace(match.Type) {
		return false
	}
	if source := strings.TrimSpace(match.Source); source != "" && strings.TrimSpace(event.Source) != source {
		return false
	}
	if subject := strings.TrimSpace(match.Subject); subject != "" && strings.TrimSpace(event.Subject) != subject {
		return false
	}
	return true
}

func matchKeysInput(match *gestalt.WorkflowEventMatch) []string {
	if match == nil {
		return nil
	}
	typ := strings.TrimSpace(match.Type)
	source := strings.TrimSpace(match.Source)
	subject := strings.TrimSpace(match.Subject)
	if typ == "" {
		return nil
	}
	return []string{
		eventMatchKey(typ, source, subject),
	}
}

func eventLookupKeysInput(event *gestalt.WorkflowEvent) []string {
	if event == nil {
		return nil
	}
	typ := strings.TrimSpace(event.Type)
	source := strings.TrimSpace(event.Source)
	subject := strings.TrimSpace(event.Subject)
	if typ == "" {
		return nil
	}
	keys := []string{
		eventMatchKey(typ, "", ""),
	}
	if source != "" {
		keys = append(keys, eventMatchKey(typ, source, ""))
	}
	if subject != "" {
		keys = append(keys, eventMatchKey(typ, "", subject))
		if source != "" {
			keys = append(keys, eventMatchKey(typ, source, subject))
		}
	}
	return keys
}

func eventMatchKey(typ, source, subject string) string {
	return strings.TrimSpace(typ) + "\x00" + strings.TrimSpace(source) + "\x00" + strings.TrimSpace(subject)
}

func createdBySubjectIDSet(subjectID string) bool {
	return strings.TrimSpace(subjectID) != ""
}

func createdByForUpsert(existing, requested string) string {
	if strings.TrimSpace(existing) == "" || isConfigManagedSubjectID(requested) {
		return cloneCreatedBySubjectID(requested)
	}
	return cloneCreatedBySubjectID(existing)
}

func cloneCreatedBySubjectID(subjectID string) string {
	return strings.TrimSpace(subjectID)
}

func cloneSubjectInput(subject *gestalt.Subject) *gestalt.Subject {
	if subject == nil {
		return nil
	}
	return &gestalt.Subject{
		ID:                  strings.TrimSpace(subject.ID),
		CredentialSubjectID: strings.TrimSpace(subject.CredentialSubjectID),
		Email:               strings.TrimSpace(subject.Email),
	}
}

func isConfigManagedSubjectID(subjectID string) bool {
	return strings.TrimSpace(subjectID) == configManagedWorkflowSubject
}

func manualTriggerInput() *gestalt.WorkflowRunTrigger {
	return &gestalt.WorkflowRunTrigger{Manual: true}
}

func scheduleTriggerInput(scheduleID string, scheduledFor time.Time) *gestalt.WorkflowRunTrigger {
	scheduledFor = scheduledFor.UTC()
	return &gestalt.WorkflowRunTrigger{Schedule: &gestalt.WorkflowScheduleTrigger{
		ActivationID: strings.TrimSpace(scheduleID),
		ScheduledFor: &scheduledFor,
	}}
}

func sortRunInputs(runs []*gestalt.WorkflowRun) {
	sort.SliceStable(runs, func(i, j int) bool {
		a := runs[i].CreatedAt
		b := runs[j].CreatedAt
		if !a.Equal(b) {
			return a.Before(b)
		}
		return runs[i].ID < runs[j].ID
	})
}

func workflowActivationInputMap(value gestalt.WorkflowValue) (map[string]any, error) {
	return workflowActivationInputMapWithSignals(value, nil)
}

func workflowEventActivationInputMap(value gestalt.WorkflowValue, event *gestalt.WorkflowEvent) (map[string]any, error) {
	if event == nil {
		return workflowActivationInputMapWithSignals(value, nil)
	}
	signal := gestalt.WorkflowSignal{Name: strings.TrimSpace(event.Type), Payload: workflowEventMapInput(event)}
	return workflowActivationInputMapWithSignals(value, []gestalt.WorkflowSignal{signal})
}

func workflowActivationInputMapWithSignals(value gestalt.WorkflowValue, signals []gestalt.WorkflowSignal) (map[string]any, error) {
	if workflowValueIsZero(value) {
		return nil, nil
	}
	resolved, ok, err := (gestaltworkflow.EvalContext{Request: gestaltworkflow.Request{Signals: signals}}).EvaluateValue(value)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("input did not resolve")
	}
	if resolved == nil {
		return nil, nil
	}
	input, ok := resolved.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("input must resolve to an object")
	}
	return cloneMapInput(input), nil
}

func workflowEventMapInput(event *gestalt.WorkflowEvent) map[string]any {
	if event == nil {
		return nil
	}
	value := map[string]any{
		"id":              event.ID,
		"source":          event.Source,
		"spec_version":    event.SpecVersion,
		"type":            event.Type,
		"subject":         event.Subject,
		"datacontenttype": event.DataContentType,
		"data":            cloneAnyInput(event.Data),
		"extensions":      cloneMapInput(event.Extensions),
	}
	if !event.Time.IsZero() {
		value["time"] = event.Time.UTC().Format(time.RFC3339Nano)
	}
	return value
}

func workflowValueIsZero(value gestalt.WorkflowValue) bool {
	return !value.LiteralSet &&
		value.Object == nil &&
		value.Array == nil &&
		value.Template == nil &&
		strings.TrimSpace(value.Input) == "" &&
		strings.TrimSpace(value.Signal) == "" &&
		value.StepOutput == nil &&
		value.StepInput == nil
}

func cloneMapInput(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	payload, err := json.Marshal(input)
	if err != nil {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(payload, &out); err != nil {
		return map[string]any{}
	}
	return out
}

func cloneAnyInput(input any) any {
	if input == nil {
		return nil
	}
	payload, err := json.Marshal(input)
	if err != nil {
		return nil
	}
	var out any
	if err := json.Unmarshal(payload, &out); err != nil {
		return nil
	}
	return out
}

func applyWorkflowExecutionProjectionInput(run *gestalt.WorkflowRun, body string, completedAt time.Time) {
	if run == nil {
		return
	}
	result := workflowExecutionResultFromBodyInput(body)
	run.Output = result.FinalOutput
	run.CurrentStepID = result.FinalStepID
	run.Steps = workflowStepExecutionsFromResultInput(result, completedAt)
}

func workflowExecutionResultFromBodyInput(body string) gestaltworkflow.StepsResult {
	var result gestaltworkflow.StepsResult
	if strings.TrimSpace(body) == "" {
		return result
	}
	_ = json.Unmarshal([]byte(body), &result)
	return result
}

func workflowStepExecutionsFromResultInput(result gestaltworkflow.StepsResult, completedAt time.Time) []gestalt.WorkflowStepExecution {
	if len(result.Steps) == 0 {
		return nil
	}
	out := make([]gestalt.WorkflowStepExecution, 0, len(result.Steps))
	for _, step := range result.Steps {
		statusValue := workflowStepStatusFromStringInput(step.Status)
		output := cloneAnyInput(result.Outputs[strings.TrimSpace(step.ID)])
		message := ""
		if step.Error != nil {
			message = strings.TrimSpace(step.Error.Message)
		}
		execution := gestalt.WorkflowStepExecution{
			StepID:        strings.TrimSpace(step.ID),
			Status:        statusValue,
			Output:        output,
			StatusMessage: message,
			SkipReason:    strings.TrimSpace(step.SkippedReason),
			CompletedAt:   timePtrInput(completedAt),
		}
		if statusValue == gestalt.WorkflowStepStatusValueSucceeded ||
			statusValue == gestalt.WorkflowStepStatusValueFailed ||
			statusValue == gestalt.WorkflowStepStatusValueSkipped {
			execution.Attempts = []gestalt.WorkflowStepAttempt{{
				ID:            execution.StepID + ":1",
				Status:        statusValue,
				Output:        output,
				StatusMessage: message,
				CompletedAt:   timePtrInput(completedAt),
			}}
		}
		out = append(out, execution)
	}
	return out
}

func workflowTargetStepCountInput(target *gestalt.BoundWorkflowTarget) int {
	if target == nil {
		return 0
	}
	return len(target.Steps)
}

func workflowStepOutputsFromExecutionsInput(steps []gestalt.WorkflowStepExecution) map[string]any {
	if len(steps) == 0 {
		return nil
	}
	out := map[string]any{}
	for _, step := range steps {
		if step.Status != gestalt.WorkflowStepStatusValueSucceeded {
			continue
		}
		stepID := strings.TrimSpace(step.StepID)
		if stepID == "" {
			continue
		}
		out[stepID] = cloneAnyInput(step.Output)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func workflowStepInputsFromExecutionsInput(steps []gestalt.WorkflowStepExecution) map[string]any {
	if len(steps) == 0 {
		return nil
	}
	out := map[string]any{}
	for _, step := range steps {
		stepID := strings.TrimSpace(step.StepID)
		if stepID == "" {
			continue
		}
		out[stepID] = cloneAnyInput(step.Input)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func workflowSkippedStepIDsFromExecutionsInput(steps []gestalt.WorkflowStepExecution) []string {
	if len(steps) == 0 {
		return nil
	}
	out := make([]string, 0)
	for _, step := range steps {
		if step.Status != gestalt.WorkflowStepStatusValueSkipped {
			continue
		}
		if stepID := strings.TrimSpace(step.StepID); stepID != "" {
			out = append(out, stepID)
		}
	}
	return out
}

func workflowStepExecutionFromStepResponseInput(resp gestaltworkflow.StepResponse, startedAt, completedAt time.Time) gestalt.WorkflowStepExecution {
	statusValue := workflowStepStatusFromStringInput(resp.Step.Status)
	stepID := strings.TrimSpace(resp.Step.ID)
	input := cloneAnyInput(resp.Input)
	output := cloneAnyInput(resp.Output)
	message := ""
	if resp.Step.Error != nil {
		message = strings.TrimSpace(resp.Step.Error.Message)
	}
	execution := gestalt.WorkflowStepExecution{
		StepID:        stepID,
		Status:        statusValue,
		Input:         input,
		Output:        output,
		StatusMessage: message,
		SkipReason:    strings.TrimSpace(resp.Step.SkippedReason),
		StartedAt:     timePtrInput(startedAt),
		CompletedAt:   timePtrInput(completedAt),
	}
	if workflowStepStatusTerminalInput(statusValue) {
		execution.Attempts = []gestalt.WorkflowStepAttempt{{
			ID:            execution.StepID + ":1",
			Status:        statusValue,
			Input:         input,
			Output:        output,
			StatusMessage: message,
			StartedAt:     timePtrInput(startedAt),
			CompletedAt:   timePtrInput(completedAt),
		}}
	}
	return execution
}

func workflowStepStatusTerminalInput(status gestalt.WorkflowStepStatus) bool {
	switch status {
	case gestalt.WorkflowStepStatusValueSkipped,
		gestalt.WorkflowStepStatusValueSucceeded,
		gestalt.WorkflowStepStatusValueFailed:
		return true
	default:
		return false
	}
}

func workflowStepFailureMessageInput(resp *gestaltworkflow.StepResponse, fallback string) string {
	if resp == nil {
		return strings.TrimSpace(fallback)
	}
	if resp.Step.Error != nil && strings.TrimSpace(resp.Step.Error.Message) != "" {
		return strings.TrimSpace(resp.Step.Error.Message)
	}
	return strings.TrimSpace(fallback)
}

func workflowStepStatusFromStringInput(status string) gestalt.WorkflowStepStatus {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "pending":
		return gestalt.WorkflowStepStatusValuePending
	case "running":
		return gestalt.WorkflowStepStatusValueRunning
	case "skipped":
		return gestalt.WorkflowStepStatusValueSkipped
	case "succeeded", "success":
		return gestalt.WorkflowStepStatusValueSucceeded
	case "failed", "failure":
		return gestalt.WorkflowStepStatusValueFailed
	case "unknown":
		return gestalt.WorkflowStepStatusValueUnknown
	default:
		return gestalt.WorkflowStepStatusValueUnspecified
	}
}

func workflowRunEventsFromRun(run *gestalt.WorkflowRun) []gestalt.WorkflowRunEvent {
	if run == nil {
		return nil
	}
	events := []gestalt.WorkflowRunEvent{{
		ID:        run.ID + ":run",
		RunID:     run.ID,
		Type:      "run." + workflowRunStatusName(run.Status),
		Data:      map[string]any{"status": workflowRunStatusName(run.Status)},
		CreatedAt: run.CreatedAt,
	}}
	for _, step := range run.Steps {
		createdAt := run.CreatedAt
		if step.CompletedAt != nil {
			createdAt = *step.CompletedAt
		}
		events = append(events, gestalt.WorkflowRunEvent{
			ID:        run.ID + ":step:" + strings.TrimSpace(step.StepID),
			RunID:     run.ID,
			StepID:    step.StepID,
			Type:      "step." + workflowStepStatusEventNameInput(step.Status),
			Data:      map[string]any{"status": workflowStepStatusEventNameInput(step.Status), "message": step.StatusMessage},
			CreatedAt: createdAt,
		})
	}
	return events
}

func workflowStepStatusEventNameInput(status gestalt.WorkflowStepStatus) string {
	switch status {
	case gestalt.WorkflowStepStatusValuePending:
		return "pending"
	case gestalt.WorkflowStepStatusValueRunning:
		return "running"
	case gestalt.WorkflowStepStatusValueSkipped:
		return "skipped"
	case gestalt.WorkflowStepStatusValueSucceeded:
		return "succeeded"
	case gestalt.WorkflowStepStatusValueFailed:
		return "failed"
	case gestalt.WorkflowStepStatusValueUnknown:
		return "unknown"
	default:
		return "unspecified"
	}
}

func timePtrInput(value time.Time) *time.Time {
	return &value
}
