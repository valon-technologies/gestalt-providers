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

func workflowInvokeMetadataInput(workflowKey string) any {
	workflowKey = strings.TrimSpace(workflowKey)
	if workflowKey == "" {
		return nil
	}
	return map[string]any{
		workflowInvokeMetadataWorkflowKey: workflowKey,
	}
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

func workflowID(scopeID, kind string, parts ...string) string {
	values := append([]string{scopeID, kind}, parts...)
	return "gestalt/" + hashID("scope", scopeID) + "/" + strings.Trim(strings.ReplaceAll(kind, " ", "-"), "/") + "/" + hashID(values...)
}

func normalizeTarget(target *proto.BoundWorkflowTarget) (scopedTarget, error) {
	if target == nil {
		return scopedTarget{}, errors.New("target is required")
	}
	if agentTarget := target.GetAgent(); agentTarget != nil {
		agentInput := gestalt.BoundWorkflowAgentTargetInputFromTarget(agentTarget)
		agentInput.ProviderName = strings.TrimSpace(agentInput.ProviderName)
		agentInput.Model = strings.TrimSpace(agentInput.Model)
		agentInput.Prompt = strings.TrimSpace(agentInput.Prompt)
		agent, err := gestalt.NewBoundWorkflowAgentTarget(agentInput)
		if err != nil {
			return scopedTarget{}, fmt.Errorf("target.agent: %w", err)
		}
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
		agentInput = gestalt.BoundWorkflowAgentTargetInputFromTarget(agent)
		normalized, err := gestalt.NewBoundWorkflowTarget(gestalt.BoundWorkflowTargetInput{
			Agent: &agentInput,
		})
		if err != nil {
			return scopedTarget{}, fmt.Errorf("target.agent: %w", err)
		}
		return scopedTarget{
			OwnerKey: "agent:" + agent.GetProviderName(),
			Target:   normalized,
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
	pluginInput := gestalt.BoundWorkflowPluginTargetInputFromTarget(plugin)
	pluginInput.PluginName = pluginName
	pluginInput.Operation = operation
	pluginInput.Connection = strings.TrimSpace(pluginInput.Connection)
	pluginInput.Instance = strings.TrimSpace(pluginInput.Instance)
	pluginInput.CredentialMode = credentialMode
	normalized, err := gestalt.NewBoundWorkflowTarget(gestalt.BoundWorkflowTargetInput{
		Plugin: &pluginInput,
	})
	if err != nil {
		return scopedTarget{}, fmt.Errorf("target.plugin.input: %w", err)
	}
	return scopedTarget{
		OwnerKey: pluginName,
		Target:   normalized,
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
	pluginName := strings.TrimSpace(target.GetPluginName())
	operation := strings.TrimSpace(target.GetOperation())
	if pluginName == "" {
		return fmt.Errorf("target.agent.%s.target.plugin_name is required", fieldName)
	}
	if operation == "" {
		return fmt.Errorf("target.agent.%s.target.operation is required", fieldName)
	}
	targetCredentialMode := strings.ToLower(strings.TrimSpace(target.GetCredentialMode()))
	if targetCredentialMode != "" {
		return fmt.Errorf("target.agent.%s.target.credential_mode %q is not supported", fieldName, target.GetCredentialMode())
	}
	credentialMode := strings.ToLower(strings.TrimSpace(delivery.GetCredentialMode()))
	switch credentialMode {
	case "", "none", "user":
	default:
		return fmt.Errorf("target.agent.%s.credential_mode %q is not supported", fieldName, delivery.GetCredentialMode())
	}
	target.PluginName = pluginName
	target.Operation = operation
	target.Connection = strings.TrimSpace(target.GetConnection())
	target.Instance = strings.TrimSpace(target.GetInstance())
	target.CredentialMode = ""
	delivery.CredentialMode = credentialMode
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
	deliveryInput := gestalt.WorkflowOutputDeliveryInputFromDelivery(delivery)
	normalized, err := gestalt.NewWorkflowOutputDelivery(*deliveryInput)
	if err != nil {
		return fmt.Errorf("target.agent.%s: %w", fieldName, err)
	}
	*delivery = *normalized
	return nil
}

func targetOwnerKeyInput(target *gestalt.BoundWorkflowTargetInput) string {
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
		CreatedBy:      actorInputPtr(signal.GetCreatedBy()),
		CreatedAt:      createdAt,
		IdempotencyKey: strings.TrimSpace(signal.GetIdempotencyKey()),
		Sequence:       signal.GetSequence(),
	})
}

func cloneTarget(target *proto.BoundWorkflowTarget) *proto.BoundWorkflowTarget {
	if target == nil {
		return nil
	}
	out, err := gestalt.NewBoundWorkflowTargetFromTarget(target)
	if err != nil {
		panic(fmt.Sprintf("clone workflow target: %v", err))
	}
	return out
}

func cloneActor(actor *proto.WorkflowActor) *proto.WorkflowActor {
	if actor == nil {
		return nil
	}
	return &proto.WorkflowActor{
		SubjectId:   strings.TrimSpace(actor.GetSubjectId()),
		SubjectKind: strings.TrimSpace(actor.GetSubjectKind()),
		DisplayName: strings.TrimSpace(actor.GetDisplayName()),
		AuthSource:  strings.TrimSpace(actor.GetAuthSource()),
	}
}

func actorInputPtr(actor *proto.WorkflowActor) *gestalt.WorkflowActorInput {
	actor = cloneActor(actor)
	if actor == nil {
		return nil
	}
	input := gestalt.WorkflowActorInputFromActor(actor)
	return &input
}

func workflowTargetInput(target *proto.BoundWorkflowTarget) *gestalt.BoundWorkflowTargetInput {
	if target == nil {
		return nil
	}
	input := gestalt.BoundWorkflowTargetInputFromTarget(target)
	return &input
}

func workflowTriggerInput(trigger *proto.WorkflowRunTrigger) *gestalt.WorkflowRunTriggerInput {
	if trigger == nil {
		return nil
	}
	input, err := gestalt.WorkflowRunTriggerInputFromTrigger(trigger)
	if err != nil {
		panic("workflow trigger input: " + err.Error())
	}
	return &input
}

func workflowSignalInputs(signals []*proto.WorkflowSignal) []gestalt.WorkflowSignalInput {
	if len(signals) == 0 {
		return nil
	}
	out := make([]gestalt.WorkflowSignalInput, 0, len(signals))
	for _, signal := range signals {
		out = append(out, gestalt.WorkflowSignalInputFromSignal(signal))
	}
	return out
}

func workflowTargetProto(input *gestalt.BoundWorkflowTargetInput) (*proto.BoundWorkflowTarget, error) {
	if input == nil {
		return nil, nil
	}
	return gestalt.NewBoundWorkflowTarget(*input)
}

func workflowActorProto(input *gestalt.WorkflowActorInput) *proto.WorkflowActor {
	if input == nil {
		return nil
	}
	return gestalt.NewWorkflowActor(*input)
}

func workflowEventProto(input *gestalt.WorkflowEventInput) (*proto.WorkflowEvent, error) {
	if input == nil {
		return nil, nil
	}
	return gestalt.NewWorkflowEvent(*input)
}

func workflowSignalProto(input *gestalt.WorkflowSignalInput) (*proto.WorkflowSignal, error) {
	if input == nil {
		return nil, nil
	}
	return gestalt.NewWorkflowSignal(*input)
}

func runInputFromProto(run *proto.BoundWorkflowRun, err error) (*gestalt.BoundWorkflowRunInput, error) {
	if err != nil || run == nil {
		return nil, err
	}
	input, err := gestalt.BoundWorkflowRunInputFromRun(run)
	if err != nil {
		return nil, err
	}
	return &input, nil
}

func signalRunResponseInputFromProto(resp *proto.SignalWorkflowRunResponse, err error) (*gestalt.SignalWorkflowRunResponse, error) {
	if err != nil || resp == nil {
		return nil, err
	}
	run, err := runInputFromProto(resp.GetRun(), nil)
	if err != nil {
		return nil, err
	}
	var signal *gestalt.WorkflowSignalInput
	if resp.GetSignal() != nil {
		input := gestalt.WorkflowSignalInputFromSignal(resp.GetSignal())
		signal = &input
	}
	return &gestalt.SignalWorkflowRunResponse{
		Run:         run,
		Signal:      signal,
		StartedRun:  resp.GetStartedRun(),
		WorkflowKey: resp.GetWorkflowKey(),
	}, nil
}

func cloneEvent(event *proto.WorkflowEvent) *proto.WorkflowEvent {
	if event == nil {
		return nil
	}
	out, err := gestalt.NewWorkflowEventFromEvent(event)
	if err != nil {
		panic(fmt.Sprintf("clone workflow event: %v", err))
	}
	return out
}

func cloneSignal(signal *proto.WorkflowSignal) *proto.WorkflowSignal {
	if signal == nil {
		return nil
	}
	out, err := gestalt.NewWorkflowSignalFromSignal(signal)
	if err != nil {
		panic(fmt.Sprintf("clone workflow signal: %v", err))
	}
	if out.GetCreatedBy() != nil {
		out.CreatedBy = cloneActor(out.GetCreatedBy())
	}
	return out
}

func cloneRun(run *proto.BoundWorkflowRun) *proto.BoundWorkflowRun {
	if run == nil {
		return nil
	}
	out, err := gestalt.NewBoundWorkflowRunFromRun(run)
	if err != nil {
		panic(fmt.Sprintf("clone workflow run: %v", err))
	}
	if out.GetCreatedBy() != nil {
		out.CreatedBy = cloneActor(out.GetCreatedBy())
	}
	return out
}

func cloneSchedule(schedule *proto.BoundWorkflowSchedule) *proto.BoundWorkflowSchedule {
	if schedule == nil {
		return nil
	}
	out, err := gestalt.NewBoundWorkflowScheduleFromSchedule(schedule)
	if err != nil {
		panic(fmt.Sprintf("clone workflow schedule: %v", err))
	}
	if out.GetCreatedBy() != nil {
		out.CreatedBy = cloneActor(out.GetCreatedBy())
	}
	return out
}

func cloneTrigger(trigger *proto.BoundWorkflowEventTrigger) *proto.BoundWorkflowEventTrigger {
	if trigger == nil {
		return nil
	}
	out, err := gestalt.NewBoundWorkflowEventTriggerFromTrigger(trigger)
	if err != nil {
		panic(fmt.Sprintf("clone workflow event trigger: %v", err))
	}
	if out.GetCreatedBy() != nil {
		out.CreatedBy = cloneActor(out.GetCreatedBy())
	}
	return out
}

func cloneExecutionReference(ref *proto.WorkflowExecutionReference) *proto.WorkflowExecutionReference {
	if ref == nil {
		return nil
	}
	out, err := gestalt.NewWorkflowExecutionReferenceFromReference(ref)
	if err != nil {
		panic(fmt.Sprintf("clone workflow execution reference: %v", err))
	}
	out.Permissions = clonePermissions(out.GetPermissions())
	return out
}

func eventMatchesTriggerInput(event *gestalt.WorkflowEventInput, trigger *gestalt.BoundWorkflowEventTriggerInput) bool {
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

func matchKeysInput(ownerKey string, match *gestalt.WorkflowEventMatchInput) []string {
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

func eventLookupKeysInput(ownerKey string, event *gestalt.WorkflowEventInput) []string {
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

func createdByForUpsertInput(existing, requested *gestalt.WorkflowActorInput) *gestalt.WorkflowActorInput {
	if existing == nil || isConfigManagedActorInput(requested) {
		return cloneActorInput(requested)
	}
	return cloneActorInput(existing)
}

func cloneActorInput(actor *gestalt.WorkflowActorInput) *gestalt.WorkflowActorInput {
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

func isConfigManagedActorInput(actor *gestalt.WorkflowActorInput) bool {
	if actor == nil {
		return false
	}
	return strings.TrimSpace(actor.SubjectID) == configManagedWorkflowSubject &&
		strings.TrimSpace(actor.SubjectKind) == configManagedWorkflowKind &&
		strings.TrimSpace(actor.AuthSource) == configManagedWorkflowAuth
}

func newManualTrigger() *proto.WorkflowRunTrigger {
	return &proto.WorkflowRunTrigger{Kind: &proto.WorkflowRunTrigger_Manual{Manual: &proto.WorkflowManualTrigger{}}}
}

func scheduleTrigger(scheduleID string, scheduledFor time.Time) *proto.WorkflowRunTrigger {
	return gestalt.NewWorkflowScheduleTrigger(strings.TrimSpace(scheduleID), scheduledFor.UTC())
}

func scheduleTriggerInput(scheduleID string, scheduledFor time.Time) *gestalt.WorkflowRunTriggerInput {
	scheduledFor = scheduledFor.UTC()
	return &gestalt.WorkflowRunTriggerInput{Schedule: &gestalt.WorkflowScheduleTriggerInput{
		ScheduleID:   strings.TrimSpace(scheduleID),
		ScheduledFor: &scheduledFor,
	}}
}

func eventTrigger(triggerID string, event *proto.WorkflowEvent) *proto.WorkflowRunTrigger {
	return &proto.WorkflowRunTrigger{Kind: &proto.WorkflowRunTrigger_Event{Event: &proto.WorkflowEventTriggerInvocation{
		TriggerId: strings.TrimSpace(triggerID),
		Event:     cloneEvent(event),
	}}}
}

func sortRunInputs(runs []*gestalt.BoundWorkflowRunInput) {
	sort.SliceStable(runs, func(i, j int) bool {
		a := runs[i].CreatedAt
		b := runs[j].CreatedAt
		if !a.Equal(b) {
			return a.Before(b)
		}
		return runs[i].ID < runs[j].ID
	})
}

func sortScheduleInputs(schedules []*gestalt.BoundWorkflowScheduleInput) {
	sort.SliceStable(schedules, func(i, j int) bool {
		a := schedules[i].CreatedAt
		b := schedules[j].CreatedAt
		if !a.Equal(b) {
			return a.Before(b)
		}
		return schedules[i].ID < schedules[j].ID
	})
}

func sortTriggerInputs(triggers []*gestalt.BoundWorkflowEventTriggerInput) {
	sort.SliceStable(triggers, func(i, j int) bool {
		a := triggers[i].CreatedAt
		b := triggers[j].CreatedAt
		if !a.Equal(b) {
			return a.Before(b)
		}
		return triggers[i].ID < triggers[j].ID
	})
}

func sortReferenceInputs(refs []*gestalt.WorkflowExecutionReferenceInput) {
	sort.SliceStable(refs, func(i, j int) bool {
		a := refs[i].CreatedAt
		b := refs[j].CreatedAt
		if !a.Equal(b) {
			return a.Before(b)
		}
		return refs[i].ID < refs[j].ID
	})
}
