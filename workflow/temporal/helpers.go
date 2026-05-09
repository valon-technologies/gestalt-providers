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
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
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
	Target   *proto.BoundWorkflowTarget
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

func workflowInvokeMetadata(workflowKey string) *structpb.Struct {
	workflowKey = strings.TrimSpace(workflowKey)
	if workflowKey == "" {
		return nil
	}
	meta, err := structpb.NewStruct(map[string]any{
		workflowInvokeMetadataWorkflowKey: workflowKey,
	})
	if err != nil {
		return nil
	}
	return meta
}

func protoHashID(msg gproto.Message) string {
	if msg == nil {
		return ""
	}
	data, err := gproto.MarshalOptions{Deterministic: true}.Marshal(msg)
	if err != nil {
		return hashID("proto-marshal-error", err.Error())
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:16])
}

func protoPayload(msg gproto.Message) []byte {
	data, err := marshalProto(msg)
	if err != nil {
		return nil
	}
	return data
}

func targetFromPayload(data []byte) *proto.BoundWorkflowTarget {
	if len(data) == 0 {
		return nil
	}
	var target proto.BoundWorkflowTarget
	if err := gproto.Unmarshal(data, &target); err != nil {
		return nil
	}
	return cloneTarget(&target)
}

func triggerFromPayload(data []byte) *proto.WorkflowRunTrigger {
	if len(data) == 0 {
		return nil
	}
	var trigger proto.WorkflowRunTrigger
	if err := gproto.Unmarshal(data, &trigger); err != nil {
		return nil
	}
	return cloneRunTrigger(&trigger)
}

func actorFromPayload(data []byte) *proto.WorkflowActor {
	if len(data) == 0 {
		return nil
	}
	var actor proto.WorkflowActor
	if err := gproto.Unmarshal(data, &actor); err != nil {
		return nil
	}
	return cloneActor(&actor)
}

func signalFromPayload(data []byte) *proto.WorkflowSignal {
	if len(data) == 0 {
		return nil
	}
	var signal proto.WorkflowSignal
	if err := gproto.Unmarshal(data, &signal); err != nil {
		return nil
	}
	return cloneSignal(&signal)
}

func runFromPayload(data []byte) *proto.BoundWorkflowRun {
	if len(data) == 0 {
		return nil
	}
	var run proto.BoundWorkflowRun
	if err := gproto.Unmarshal(data, &run); err != nil {
		return nil
	}
	return cloneRun(&run)
}

func signalResponseFromPayload(data []byte) *proto.SignalWorkflowRunResponse {
	if len(data) == 0 {
		return nil
	}
	var resp proto.SignalWorkflowRunResponse
	if err := gproto.Unmarshal(data, &resp); err != nil {
		return nil
	}
	return cloneSignalResponse(&resp)
}

func scopeHash(scopeID string) string {
	return hashID("scope", scopeID)
}

func workflowID(scopeID, kind string, parts ...string) string {
	values := append([]string{scopeID, kind}, parts...)
	return "gestalt/" + scopeHash(scopeID) + "/" + strings.Trim(strings.ReplaceAll(kind, " ", "-"), "/") + "/" + hashID(values...)
}

func normalizeTarget(target *proto.BoundWorkflowTarget) (scopedTarget, error) {
	if target == nil {
		return scopedTarget{}, errors.New("target is required")
	}
	if agentTarget := target.GetAgent(); agentTarget != nil {
		agent := gproto.Clone(agentTarget).(*proto.BoundWorkflowAgentTarget)
		agent.ProviderName = strings.TrimSpace(agent.GetProviderName())
		agent.Model = strings.TrimSpace(agent.GetModel())
		agent.Prompt = strings.TrimSpace(agent.GetPrompt())
		if agent.GetProviderName() == "" {
			return scopedTarget{}, errors.New("target.agent.provider_name is required")
		}
		if agent.GetPrompt() == "" && len(agent.GetMessages()) == 0 {
			return scopedTarget{}, errors.New("target.agent.prompt or messages is required")
		}
		if agent.GetTimeoutSeconds() < 0 {
			return scopedTarget{}, errors.New("target.agent.timeout_seconds must not be negative")
		}
		if err := normalizeAgentOutputDelivery(agent.GetOutputDelivery()); err != nil {
			return scopedTarget{}, err
		}
		if err := normalizeAgentSessionReadyDelivery(agent.GetSessionReadyDelivery()); err != nil {
			return scopedTarget{}, err
		}
		return scopedTarget{
			OwnerKey: "agent:" + agent.GetProviderName(),
			Target:   &proto.BoundWorkflowTarget{Kind: &proto.BoundWorkflowTarget_Agent{Agent: agent}},
		}, nil
	}
	plugin := target.GetPlugin()
	if plugin == nil {
		return scopedTarget{}, errors.New("target.plugin.plugin_name is required")
	}
	pluginName := strings.TrimSpace(plugin.GetPluginName())
	operation := strings.TrimSpace(plugin.GetOperation())
	if pluginName == "" {
		return scopedTarget{}, errors.New("target.plugin.plugin_name is required")
	}
	if operation == "" {
		return scopedTarget{}, errors.New("target.plugin.operation is required")
	}
	credentialMode := strings.ToLower(strings.TrimSpace(plugin.GetCredentialMode()))
	switch credentialMode {
	case "", "none", "user":
	default:
		return scopedTarget{}, fmt.Errorf("target.plugin.credential_mode %q is not supported", plugin.GetCredentialMode())
	}
	input, err := gestalt.StructFromAny(gestalt.MapFromStruct(plugin.GetInput()))
	if err != nil {
		return scopedTarget{}, fmt.Errorf("target.plugin.input: %w", err)
	}
	normalized := &proto.BoundWorkflowPluginTarget{
		PluginName:     pluginName,
		Operation:      operation,
		Input:          input,
		Connection:     strings.TrimSpace(plugin.GetConnection()),
		Instance:       strings.TrimSpace(plugin.GetInstance()),
		CredentialMode: credentialMode,
	}
	return scopedTarget{
		OwnerKey: pluginName,
		Target:   &proto.BoundWorkflowTarget{Kind: &proto.BoundWorkflowTarget_Plugin{Plugin: normalized}},
	}, nil
}

func normalizeAgentOutputDelivery(delivery *proto.WorkflowOutputDelivery) error {
	return normalizeAgentDelivery(delivery, "output_delivery", false)
}

func normalizeAgentSessionReadyDelivery(delivery *proto.WorkflowOutputDelivery) error {
	return normalizeAgentDelivery(delivery, "session_ready_delivery", true)
}

func normalizeAgentDelivery(delivery *proto.WorkflowOutputDelivery, fieldName string, beforeTurn bool) error {
	if delivery == nil {
		return nil
	}
	target := delivery.GetTarget()
	if target == nil {
		return fmt.Errorf("target.agent.%s.target.plugin_name is required", fieldName)
	}
	target.PluginName = strings.TrimSpace(target.GetPluginName())
	target.Operation = strings.TrimSpace(target.GetOperation())
	target.Connection = strings.TrimSpace(target.GetConnection())
	target.Instance = strings.TrimSpace(target.GetInstance())
	target.CredentialMode = strings.ToLower(strings.TrimSpace(target.GetCredentialMode()))
	if target.GetPluginName() == "" {
		return fmt.Errorf("target.agent.%s.target.plugin_name is required", fieldName)
	}
	if target.GetOperation() == "" {
		return fmt.Errorf("target.agent.%s.target.operation is required", fieldName)
	}
	if target.GetCredentialMode() != "" {
		return fmt.Errorf("target.agent.%s.target.credential_mode %q is not supported", fieldName, target.GetCredentialMode())
	}
	credentialMode := strings.ToLower(strings.TrimSpace(delivery.GetCredentialMode()))
	switch credentialMode {
	case "", "none", "user":
		delivery.CredentialMode = credentialMode
	default:
		return fmt.Errorf("target.agent.%s.credential_mode %q is not supported", fieldName, delivery.GetCredentialMode())
	}
	for _, binding := range delivery.GetInputBindings() {
		if binding == nil || binding.GetValue() == nil || binding.GetValue().GetKind() == nil {
			return fmt.Errorf("target.agent.%s.input_bindings.value is required", fieldName)
		}
		binding.InputField = strings.TrimSpace(binding.GetInputField())
		if binding.GetInputField() == "" {
			return fmt.Errorf("target.agent.%s.input_bindings.input_field is required", fieldName)
		}
		switch kind := binding.GetValue().GetKind().(type) {
		case *proto.WorkflowOutputValueSource_AgentOutput:
			if beforeTurn {
				return fmt.Errorf("target.agent.%s.input_bindings.value.agent_output is not available before the agent turn starts", fieldName)
			}
			kind.AgentOutput = strings.TrimSpace(kind.AgentOutput)
			if kind.AgentOutput == "" {
				return fmt.Errorf("target.agent.%s.input_bindings.value.agent_output is required", fieldName)
			}
		case *proto.WorkflowOutputValueSource_SignalPayload:
			kind.SignalPayload = strings.TrimSpace(kind.SignalPayload)
			if kind.SignalPayload == "" {
				return fmt.Errorf("target.agent.%s.input_bindings.value.signal_payload is required", fieldName)
			}
		case *proto.WorkflowOutputValueSource_SignalMetadata:
			kind.SignalMetadata = strings.TrimSpace(kind.SignalMetadata)
			if kind.SignalMetadata == "" {
				return fmt.Errorf("target.agent.%s.input_bindings.value.signal_metadata is required", fieldName)
			}
		case *proto.WorkflowOutputValueSource_AgentSession:
			kind.AgentSession = strings.TrimSpace(kind.AgentSession)
			if kind.AgentSession == "" {
				return fmt.Errorf("target.agent.%s.input_bindings.value.agent_session is required", fieldName)
			}
		case *proto.WorkflowOutputValueSource_Literal:
			if kind.Literal == nil {
				return fmt.Errorf("target.agent.%s.input_bindings.value.literal is required", fieldName)
			}
		default:
			return fmt.Errorf("target.agent.%s.input_bindings.value is required", fieldName)
		}
	}
	return nil
}

func targetOwnerKey(target *proto.BoundWorkflowTarget) string {
	if target == nil {
		return ""
	}
	if agent := target.GetAgent(); agent != nil {
		if provider := strings.TrimSpace(agent.GetProviderName()); provider != "" {
			return "agent:" + provider
		}
		return ""
	}
	if plugin := target.GetPlugin(); plugin != nil {
		return strings.TrimSpace(plugin.GetPluginName())
	}
	return ""
}

func normalizeWorkflowEvent(event *proto.WorkflowEvent, now func() time.Time) (*proto.WorkflowEvent, error) {
	if event == nil {
		return nil, errors.New("event is required")
	}
	source := strings.TrimSpace(event.GetSource())
	if source == "" {
		return nil, errors.New("event.source is required")
	}
	eventType := strings.TrimSpace(event.GetType())
	if eventType == "" {
		return nil, errors.New("event.type is required")
	}
	specVersion := strings.TrimSpace(event.GetSpecVersion())
	if specVersion == "" {
		specVersion = defaultSpecVersion
	}
	eventTime := now().UTC()
	if ts := event.GetTime(); ts != nil && ts.IsValid() {
		eventTime = ts.AsTime().UTC()
	}
	return gestalt.NewWorkflowEvent(gestalt.WorkflowEventInput{
		ID:              strings.TrimSpace(event.GetId()),
		Source:          source,
		SpecVersion:     specVersion,
		Type:            eventType,
		Subject:         strings.TrimSpace(event.GetSubject()),
		Time:            eventTime,
		DataContentType: strings.TrimSpace(event.GetDatacontenttype()),
		Data:            gestalt.MapFromStruct(event.GetData()),
		Extensions:      gestalt.MapFromValues(event.GetExtensions()),
	})
}

func normalizeWorkflowSignal(signal *proto.WorkflowSignal, now time.Time) (*proto.WorkflowSignal, error) {
	if signal == nil {
		return nil, errors.New("signal is required")
	}
	name := strings.TrimSpace(signal.GetName())
	if name == "" {
		return nil, errors.New("signal.name is required")
	}
	createdAt := now.UTC()
	if ts := signal.GetCreatedAt(); ts != nil && ts.IsValid() {
		createdAt = ts.AsTime().UTC()
	}
	return gestalt.NewWorkflowSignal(gestalt.WorkflowSignalInput{
		ID:             strings.TrimSpace(signal.GetId()),
		Name:           name,
		Payload:        gestalt.MapFromStruct(signal.GetPayload()),
		Metadata:       gestalt.MapFromStruct(signal.GetMetadata()),
		CreatedBy:      cloneActor(signal.GetCreatedBy()),
		CreatedAt:      createdAt,
		IdempotencyKey: strings.TrimSpace(signal.GetIdempotencyKey()),
		Sequence:       signal.GetSequence(),
	})
}

func cloneTarget(target *proto.BoundWorkflowTarget) *proto.BoundWorkflowTarget {
	if target == nil {
		return nil
	}
	return gproto.Clone(target).(*proto.BoundWorkflowTarget)
}

func cloneActor(actor *proto.WorkflowActor) *proto.WorkflowActor {
	if actor == nil {
		return nil
	}
	out := gproto.Clone(actor).(*proto.WorkflowActor)
	out.SubjectId = strings.TrimSpace(out.GetSubjectId())
	out.SubjectKind = strings.TrimSpace(out.GetSubjectKind())
	out.DisplayName = strings.TrimSpace(out.GetDisplayName())
	out.AuthSource = strings.TrimSpace(out.GetAuthSource())
	return out
}

func cloneEvent(event *proto.WorkflowEvent) *proto.WorkflowEvent {
	if event == nil {
		return nil
	}
	return gproto.Clone(event).(*proto.WorkflowEvent)
}

func cloneSignal(signal *proto.WorkflowSignal) *proto.WorkflowSignal {
	if signal == nil {
		return nil
	}
	return gproto.Clone(signal).(*proto.WorkflowSignal)
}

func cloneRun(run *proto.BoundWorkflowRun) *proto.BoundWorkflowRun {
	if run == nil {
		return nil
	}
	return gproto.Clone(run).(*proto.BoundWorkflowRun)
}

func cloneSchedule(schedule *proto.BoundWorkflowSchedule) *proto.BoundWorkflowSchedule {
	if schedule == nil {
		return nil
	}
	return gproto.Clone(schedule).(*proto.BoundWorkflowSchedule)
}

func cloneTrigger(trigger *proto.BoundWorkflowEventTrigger) *proto.BoundWorkflowEventTrigger {
	if trigger == nil {
		return nil
	}
	return gproto.Clone(trigger).(*proto.BoundWorkflowEventTrigger)
}

func cloneExecutionReference(ref *proto.WorkflowExecutionReference) *proto.WorkflowExecutionReference {
	if ref == nil {
		return nil
	}
	return gproto.Clone(ref).(*proto.WorkflowExecutionReference)
}

func eventMatchesTrigger(event *proto.WorkflowEvent, trigger *proto.BoundWorkflowEventTrigger) bool {
	if event == nil || trigger == nil || trigger.GetPaused() {
		return false
	}
	match := trigger.GetMatch()
	if strings.TrimSpace(event.GetType()) != strings.TrimSpace(match.GetType()) {
		return false
	}
	if source := strings.TrimSpace(match.GetSource()); source != "" && strings.TrimSpace(event.GetSource()) != source {
		return false
	}
	if subject := strings.TrimSpace(match.GetSubject()); subject != "" && strings.TrimSpace(event.GetSubject()) != subject {
		return false
	}
	return true
}

func matchKeys(ownerKey string, match *proto.WorkflowEventMatch) []string {
	if match == nil {
		return nil
	}
	ownerKey = strings.TrimSpace(ownerKey)
	typ := strings.TrimSpace(match.GetType())
	source := strings.TrimSpace(match.GetSource())
	subject := strings.TrimSpace(match.GetSubject())
	if typ == "" {
		return nil
	}
	return []string{
		eventMatchKey(ownerKey, typ, source, subject),
	}
}

func eventLookupKeys(ownerKey string, event *proto.WorkflowEvent) []string {
	ownerKey = strings.TrimSpace(ownerKey)
	typ := strings.TrimSpace(event.GetType())
	source := strings.TrimSpace(event.GetSource())
	subject := strings.TrimSpace(event.GetSubject())
	keys := []string{
		eventMatchKey(ownerKey, typ, "", ""),
		eventMatchKey(ownerKey, typ, source, ""),
		eventMatchKey(ownerKey, typ, "", subject),
		eventMatchKey(ownerKey, typ, source, subject),
	}
	return keys
}

func eventMatchKey(ownerKey, typ, source, subject string) string {
	return strings.TrimSpace(ownerKey) + "\x00" + strings.TrimSpace(typ) + "\x00" + strings.TrimSpace(source) + "\x00" + strings.TrimSpace(subject)
}

func actorHasSubject(actor *proto.WorkflowActor) bool {
	return strings.TrimSpace(actor.GetSubjectId()) != ""
}

func isConfigManagedActor(actor *proto.WorkflowActor) bool {
	if actor == nil {
		return false
	}
	return strings.TrimSpace(actor.GetSubjectId()) == configManagedWorkflowSubject &&
		strings.TrimSpace(actor.GetSubjectKind()) == configManagedWorkflowKind &&
		strings.TrimSpace(actor.GetAuthSource()) == configManagedWorkflowAuth
}

func createdByForUpsert(existing, requested *proto.WorkflowActor) *proto.WorkflowActor {
	if existing == nil || isConfigManagedActor(requested) {
		return cloneActor(requested)
	}
	return cloneActor(existing)
}

func newManualTrigger() *proto.WorkflowRunTrigger {
	return &proto.WorkflowRunTrigger{Kind: &proto.WorkflowRunTrigger_Manual{Manual: &proto.WorkflowManualTrigger{}}}
}

func scheduleTrigger(scheduleID string, scheduledFor time.Time) *proto.WorkflowRunTrigger {
	return gestalt.NewWorkflowScheduleTrigger(strings.TrimSpace(scheduleID), scheduledFor.UTC())
}

func eventTrigger(triggerID string, event *proto.WorkflowEvent) *proto.WorkflowRunTrigger {
	return &proto.WorkflowRunTrigger{Kind: &proto.WorkflowRunTrigger_Event{Event: &proto.WorkflowEventTriggerInvocation{
		TriggerId: strings.TrimSpace(triggerID),
		Event:     cloneEvent(event),
	}}}
}

func sortRuns(runs []*proto.BoundWorkflowRun) {
	sort.SliceStable(runs, func(i, j int) bool {
		a := runs[i].GetCreatedAt().AsTime()
		b := runs[j].GetCreatedAt().AsTime()
		if !a.Equal(b) {
			return a.Before(b)
		}
		return runs[i].GetId() < runs[j].GetId()
	})
}

func sortSchedules(schedules []*proto.BoundWorkflowSchedule) {
	sort.SliceStable(schedules, func(i, j int) bool {
		a := schedules[i].GetCreatedAt().AsTime()
		b := schedules[j].GetCreatedAt().AsTime()
		if !a.Equal(b) {
			return a.Before(b)
		}
		return schedules[i].GetId() < schedules[j].GetId()
	})
}

func sortTriggers(triggers []*proto.BoundWorkflowEventTrigger) {
	sort.SliceStable(triggers, func(i, j int) bool {
		a := triggers[i].GetCreatedAt().AsTime()
		b := triggers[j].GetCreatedAt().AsTime()
		if !a.Equal(b) {
			return a.Before(b)
		}
		return triggers[i].GetId() < triggers[j].GetId()
	})
}

func sortReferences(refs []*proto.WorkflowExecutionReference) {
	sort.SliceStable(refs, func(i, j int) bool {
		a := refs[i].GetCreatedAt().AsTime()
		b := refs[j].GetCreatedAt().AsTime()
		if !a.Equal(b) {
			return a.Before(b)
		}
		return refs[i].GetId() < refs[j].GetId()
	})
}
