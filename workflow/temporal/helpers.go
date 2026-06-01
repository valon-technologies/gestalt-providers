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
)

const (
	defaultSpecVersion = "1.0"
	defaultTimezone    = "UTC"

	configManagedWorkflowSubject = "system:config"
	configManagedWorkflowKind    = "system"
	configManagedWorkflowAuth    = "config"
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

func cloneWorkflowDefinitionInput(definition *gestalt.BoundWorkflowDefinition) *gestalt.BoundWorkflowDefinition {
	if definition == nil {
		return nil
	}
	data, err := json.Marshal(definition)
	if err != nil {
		out := *definition
		return &out
	}
	var out gestalt.BoundWorkflowDefinition
	if err := json.Unmarshal(data, &out); err != nil {
		out := *definition
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
	out.CreatedBy = cloneActorInput(out.CreatedBy)
	out.IdempotencyKey = strings.TrimSpace(out.IdempotencyKey)
	return &out, nil
}

func cloneRunInput(run *gestalt.BoundWorkflowRun) *gestalt.BoundWorkflowRun {
	if run == nil {
		return nil
	}
	out := *run
	out.ID = strings.TrimSpace(out.ID)
	out.StatusMessage = strings.TrimSpace(out.StatusMessage)
	out.WorkflowKey = strings.TrimSpace(out.WorkflowKey)
	out.DefinitionID = strings.TrimSpace(out.DefinitionID)
	out.CreatedBy = cloneActorInput(out.CreatedBy)
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
	out.CreatedBy = cloneActorInput(out.CreatedBy)
	return &out
}

func eventMatchesTriggerInput(event *gestalt.WorkflowEvent, trigger *gestalt.BoundWorkflowEventTrigger) bool {
	if event == nil || trigger == nil || trigger.Paused || trigger.Match == nil {
		return false
	}
	if strings.TrimSpace(event.Type) != strings.TrimSpace(trigger.Match.Type) {
		return false
	}
	if source := strings.TrimSpace(trigger.Match.Source); source != "" && strings.TrimSpace(event.Source) != source {
		return false
	}
	if subject := strings.TrimSpace(trigger.Match.Subject); subject != "" && strings.TrimSpace(event.Subject) != subject {
		return false
	}
	return true
}

func matchKeysInput(ownerKey string, match *gestalt.WorkflowEventMatch) []string {
	if match == nil {
		return nil
	}
	ownerKey = strings.TrimSpace(ownerKey)
	typ := strings.TrimSpace(match.Type)
	source := strings.TrimSpace(match.Source)
	subject := strings.TrimSpace(match.Subject)
	if typ == "" {
		return nil
	}
	return []string{
		eventMatchKey(ownerKey, typ, source, subject),
	}
}

func eventLookupKeysInput(ownerKey string, event *gestalt.WorkflowEvent) []string {
	if event == nil {
		return nil
	}
	ownerKey = strings.TrimSpace(ownerKey)
	typ := strings.TrimSpace(event.Type)
	source := strings.TrimSpace(event.Source)
	subject := strings.TrimSpace(event.Subject)
	return []string{
		eventMatchKey(ownerKey, typ, "", ""),
		eventMatchKey(ownerKey, typ, source, ""),
		eventMatchKey(ownerKey, typ, "", subject),
		eventMatchKey(ownerKey, typ, source, subject),
	}
}

func eventMatchKey(ownerKey, typ, source, subject string) string {
	return strings.TrimSpace(ownerKey) + "\x00" + strings.TrimSpace(typ) + "\x00" + strings.TrimSpace(source) + "\x00" + strings.TrimSpace(subject)
}

func actorHasSubject(actor *gestalt.WorkflowActor) bool {
	return actor != nil && strings.TrimSpace(actor.SubjectID) != ""
}

func createdByForUpsertInput(existing, requested *gestalt.WorkflowActor) *gestalt.WorkflowActor {
	if existing == nil || isConfigManagedActorInput(requested) {
		return cloneActorInput(requested)
	}
	return cloneActorInput(existing)
}

func cloneActorInput(actor *gestalt.WorkflowActor) *gestalt.WorkflowActor {
	if actor == nil {
		return nil
	}
	out := *actor
	out.SubjectID = strings.TrimSpace(out.SubjectID)
	out.SubjectKind = strings.TrimSpace(out.SubjectKind)
	out.DisplayName = strings.TrimSpace(out.DisplayName)
	out.AuthSource = strings.TrimSpace(out.AuthSource)
	return &out
}

func isConfigManagedActorInput(actor *gestalt.WorkflowActor) bool {
	if actor == nil {
		return false
	}
	return strings.TrimSpace(actor.SubjectID) == configManagedWorkflowSubject &&
		strings.TrimSpace(actor.SubjectKind) == configManagedWorkflowKind &&
		strings.TrimSpace(actor.AuthSource) == configManagedWorkflowAuth
}

func manualTriggerInput() *gestalt.WorkflowRunTrigger {
	return &gestalt.WorkflowRunTrigger{Manual: true}
}

func scheduleTriggerInput(scheduleID string, scheduledFor time.Time) *gestalt.WorkflowRunTrigger {
	scheduledFor = scheduledFor.UTC()
	return &gestalt.WorkflowRunTrigger{Schedule: &gestalt.WorkflowScheduleTrigger{
		ScheduleID:   strings.TrimSpace(scheduleID),
		ScheduledFor: &scheduledFor,
	}}
}

func sortRunInputs(runs []*gestalt.BoundWorkflowRun) {
	sort.SliceStable(runs, func(i, j int) bool {
		a := runs[i].CreatedAt
		b := runs[j].CreatedAt
		if !a.Equal(b) {
			return a.Before(b)
		}
		return runs[i].ID < runs[j].ID
	})
}

func sortScheduleInputs(schedules []*gestalt.BoundWorkflowSchedule) {
	sort.SliceStable(schedules, func(i, j int) bool {
		a := schedules[i].CreatedAt
		b := schedules[j].CreatedAt
		if !a.Equal(b) {
			return a.Before(b)
		}
		return schedules[i].ID < schedules[j].ID
	})
}

func sortTriggerInputs(triggers []*gestalt.BoundWorkflowEventTrigger) {
	sort.SliceStable(triggers, func(i, j int) bool {
		a := triggers[i].CreatedAt
		b := triggers[j].CreatedAt
		if !a.Equal(b) {
			return a.Before(b)
		}
		return triggers[i].ID < triggers[j].ID
	})
}
