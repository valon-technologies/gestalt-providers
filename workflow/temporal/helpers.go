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

	gestaltInputKey              = "_gestalt"
	eventRunPermissionsKey       = "eventRunPermissions"
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

func workflowInvokeMetadataInput(workflowKey string) any {
	workflowKey = strings.TrimSpace(workflowKey)
	if workflowKey == "" {
		return nil
	}
	return map[string]any{
		workflowInvokeMetadataWorkflowKey: workflowKey,
	}
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
	if target.Agent != nil {
		agent := *target.Agent
		agent.ProviderName = strings.TrimSpace(agent.ProviderName)
		agent.Model = strings.TrimSpace(agent.Model)
		agent.Prompt = strings.TrimSpace(agent.Prompt)
		agent.OutputDelivery = cloneOutputDeliveryInput(agent.OutputDelivery)
		agent.SessionReadyDelivery = cloneOutputDeliveryInput(agent.SessionReadyDelivery)
		if agent.ProviderName == "" {
			return scopedTarget{}, errors.New("target.agent.provider_name is required")
		}
		if agent.Prompt == "" && len(agent.Messages) == 0 {
			return scopedTarget{}, errors.New("target.agent.prompt or messages is required")
		}
		if agent.TimeoutSeconds < 0 {
			return scopedTarget{}, errors.New("target.agent.timeout_seconds must not be negative")
		}
		if err := normalizeAgentOutputDelivery(agent.OutputDelivery); err != nil {
			return scopedTarget{}, err
		}
		if err := normalizeAgentSessionReadyDelivery(agent.SessionReadyDelivery); err != nil {
			return scopedTarget{}, err
		}
		normalized := &gestalt.BoundWorkflowTarget{Agent: &agent}
		if _, err := gestalt.NewBoundWorkflowTarget(*normalized); err != nil {
			return scopedTarget{}, fmt.Errorf("target.agent: %w", err)
		}
		return scopedTarget{
			OwnerKey: "agent:" + agent.ProviderName,
			Target:   normalized,
		}, nil
	}
	if target.Plugin == nil {
		return scopedTarget{}, errors.New("target.plugin.plugin_name is required")
	}
	plugin := *target.Plugin
	pluginName := strings.TrimSpace(plugin.PluginName)
	operation := strings.TrimSpace(plugin.Operation)
	if pluginName == "" {
		return scopedTarget{}, errors.New("target.plugin.plugin_name is required")
	}
	if operation == "" {
		return scopedTarget{}, errors.New("target.plugin.operation is required")
	}
	credentialMode := strings.ToLower(strings.TrimSpace(plugin.CredentialMode))
	switch credentialMode {
	case "", "none", "user":
	default:
		return scopedTarget{}, fmt.Errorf("target.plugin.credential_mode %q is not supported", plugin.CredentialMode)
	}
	plugin.PluginName = pluginName
	plugin.Operation = operation
	plugin.Connection = strings.TrimSpace(plugin.Connection)
	plugin.Instance = strings.TrimSpace(plugin.Instance)
	plugin.CredentialMode = credentialMode
	normalized := &gestalt.BoundWorkflowTarget{Plugin: &plugin}
	if _, err := gestalt.NewBoundWorkflowTarget(*normalized); err != nil {
		return scopedTarget{}, fmt.Errorf("target.plugin.input: %w", err)
	}
	return scopedTarget{
		OwnerKey: pluginName,
		Target:   normalized,
	}, nil
}

func normalizeAgentOutputDelivery(delivery *gestalt.WorkflowOutputDelivery) error {
	return normalizeAgentDelivery(delivery, "output_delivery", false)
}

func normalizeAgentSessionReadyDelivery(delivery *gestalt.WorkflowOutputDelivery) error {
	return normalizeAgentDelivery(delivery, "session_ready_delivery", true)
}

func normalizeAgentDelivery(delivery *gestalt.WorkflowOutputDelivery, fieldName string, beforeTurn bool) error {
	if delivery == nil {
		return nil
	}
	target := delivery.Target
	if target == nil {
		return fmt.Errorf("target.agent.%s.target.plugin_name is required", fieldName)
	}
	targetCopy := *target
	pluginName := strings.TrimSpace(targetCopy.PluginName)
	operation := strings.TrimSpace(targetCopy.Operation)
	if pluginName == "" {
		return fmt.Errorf("target.agent.%s.target.plugin_name is required", fieldName)
	}
	if operation == "" {
		return fmt.Errorf("target.agent.%s.target.operation is required", fieldName)
	}
	targetCredentialMode := strings.ToLower(strings.TrimSpace(targetCopy.CredentialMode))
	if targetCredentialMode != "" {
		return fmt.Errorf("target.agent.%s.target.credential_mode %q is not supported", fieldName, targetCopy.CredentialMode)
	}
	credentialMode := strings.ToLower(strings.TrimSpace(delivery.CredentialMode))
	switch credentialMode {
	case "", "none", "user":
	default:
		return fmt.Errorf("target.agent.%s.credential_mode %q is not supported", fieldName, delivery.CredentialMode)
	}
	targetCopy.PluginName = pluginName
	targetCopy.Operation = operation
	targetCopy.Connection = strings.TrimSpace(targetCopy.Connection)
	targetCopy.Instance = strings.TrimSpace(targetCopy.Instance)
	targetCopy.CredentialMode = ""
	delivery.CredentialMode = credentialMode
	delivery.Target = &targetCopy
	for i := range delivery.InputBindings {
		binding := &delivery.InputBindings[i]
		if binding.Value == nil {
			return fmt.Errorf("target.agent.%s.input_bindings.value is required", fieldName)
		}
		binding.InputField = strings.TrimSpace(binding.InputField)
		if binding.InputField == "" {
			return fmt.Errorf("target.agent.%s.input_bindings.input_field is required", fieldName)
		}
		value := binding.Value
		value.AgentOutput = strings.TrimSpace(value.AgentOutput)
		value.SignalPayload = strings.TrimSpace(value.SignalPayload)
		value.SignalMetadata = strings.TrimSpace(value.SignalMetadata)
		value.AgentSession = strings.TrimSpace(value.AgentSession)
		selected := 0
		if value.AgentOutput != "" {
			selected++
			if beforeTurn {
				return fmt.Errorf("target.agent.%s.input_bindings.value.agent_output is not available before the agent turn starts", fieldName)
			}
		}
		if value.SignalPayload != "" {
			selected++
		}
		if value.SignalMetadata != "" {
			selected++
		}
		if value.AgentSession != "" {
			selected++
		}
		if value.Literal != nil {
			selected++
		}
		if selected == 0 {
			return fmt.Errorf("target.agent.%s.input_bindings.value is required", fieldName)
		}
		if selected > 1 {
			return fmt.Errorf("target.agent.%s.input_bindings.value must set exactly one source", fieldName)
		}
	}
	if _, err := gestalt.NewWorkflowOutputDelivery(*delivery); err != nil {
		return fmt.Errorf("target.agent.%s: %w", fieldName, err)
	}
	return nil
}

func targetOwnerKeyInput(target *gestalt.BoundWorkflowTarget) string {
	if target == nil {
		return ""
	}
	if target.Agent != nil {
		if provider := strings.TrimSpace(target.Agent.ProviderName); provider != "" {
			return "agent:" + provider
		}
		return ""
	}
	if target.Plugin != nil {
		return strings.TrimSpace(target.Plugin.PluginName)
	}
	return ""
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
	if _, err := gestalt.NewWorkflowEvent(*normalized); err != nil {
		return nil, err
	}
	return normalized, nil
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
	if _, err := gestalt.NewWorkflowSignal(out); err != nil {
		return nil, err
	}
	return &out, nil
}

func cloneRunInput(run *gestalt.BoundWorkflowRun) *gestalt.BoundWorkflowRun {
	if run == nil {
		return nil
	}
	out := *run
	out.ID = strings.TrimSpace(out.ID)
	out.StatusMessage = strings.TrimSpace(out.StatusMessage)
	out.ExecutionRef = strings.TrimSpace(out.ExecutionRef)
	out.WorkflowKey = strings.TrimSpace(out.WorkflowKey)
	out.CreatedBy = cloneActorInput(out.CreatedBy)
	return &out
}

func cloneOutputDeliveryInput(delivery *gestalt.WorkflowOutputDelivery) *gestalt.WorkflowOutputDelivery {
	if delivery == nil {
		return nil
	}
	out := *delivery
	if delivery.Target != nil {
		target := *delivery.Target
		out.Target = &target
	}
	if len(delivery.InputBindings) > 0 {
		out.InputBindings = make([]gestalt.WorkflowOutputBinding, len(delivery.InputBindings))
		for i, binding := range delivery.InputBindings {
			out.InputBindings[i] = binding
			if binding.Value != nil {
				value := *binding.Value
				out.InputBindings[i].Value = &value
			}
		}
	}
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

func sortReferenceInputs(refs []*gestalt.WorkflowExecutionReference) {
	sort.SliceStable(refs, func(i, j int) bool {
		a := refs[i].CreatedAt
		b := refs[j].CreatedAt
		if !a.Equal(b) {
			return a.Before(b)
		}
		return refs[i].ID < refs[j].ID
	})
}
