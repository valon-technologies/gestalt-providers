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

	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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

type runHandle struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
}

func encodeRunHandle(workflowID, runID string) string {
	payload, _ := json.Marshal(runHandle{WorkflowID: strings.TrimSpace(workflowID), RunID: strings.TrimSpace(runID)})
	return base64.RawURLEncoding.EncodeToString(payload)
}

func decodeRunHandle(id string) (runHandle, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return runHandle{}, errors.New("run_id is required")
	}
	data, err := base64.RawURLEncoding.DecodeString(id)
	if err != nil {
		return runHandle{}, fmt.Errorf("run_id is not a temporal workflow handle")
	}
	var out runHandle
	if err := json.Unmarshal(data, &out); err != nil {
		return runHandle{}, fmt.Errorf("run_id is not a temporal workflow handle")
	}
	out.WorkflowID = strings.TrimSpace(out.WorkflowID)
	out.RunID = strings.TrimSpace(out.RunID)
	if out.WorkflowID == "" || out.RunID == "" {
		return runHandle{}, fmt.Errorf("run_id is missing workflow_id or run_id")
	}
	return out, nil
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

func scopeHash(scopeID string) string {
	return hashID("scope", scopeID)
}

func workflowID(scopeID, kind string, parts ...string) string {
	values := append([]string{scopeID, kind}, parts...)
	return "gestalt/" + scopeHash(scopeID) + "/" + strings.Trim(strings.ReplaceAll(kind, " ", "-"), "/") + "/" + hashID(values...)
}

func indexWorkflowID(scopeID string, shard int) string {
	return fmt.Sprintf("gestalt/%s/index/%04d", scopeHash(scopeID), shard)
}

func metadataWorkflowID(scopeID string) string {
	return "gestalt/" + scopeHash(scopeID) + "/metadata"
}

func shardFor(key string, count int) int {
	if count <= 1 {
		return 0
	}
	sum := sha256.Sum256([]byte(strings.TrimSpace(key)))
	n := uint32(sum[0])<<24 | uint32(sum[1])<<16 | uint32(sum[2])<<8 | uint32(sum[3])
	return int(n % uint32(count))
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
	normalized := &proto.BoundWorkflowPluginTarget{
		PluginName:     pluginName,
		Operation:      operation,
		Input:          cloneStruct(plugin.GetInput()),
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
	if delivery == nil {
		return nil
	}
	target := delivery.GetTarget()
	if target == nil {
		return errors.New("target.agent.output_delivery.target.plugin_name is required")
	}
	target.PluginName = strings.TrimSpace(target.GetPluginName())
	target.Operation = strings.TrimSpace(target.GetOperation())
	target.Connection = strings.TrimSpace(target.GetConnection())
	target.Instance = strings.TrimSpace(target.GetInstance())
	target.CredentialMode = strings.ToLower(strings.TrimSpace(target.GetCredentialMode()))
	if target.GetPluginName() == "" {
		return errors.New("target.agent.output_delivery.target.plugin_name is required")
	}
	if target.GetOperation() == "" {
		return errors.New("target.agent.output_delivery.target.operation is required")
	}
	if target.GetCredentialMode() != "" {
		return fmt.Errorf("target.agent.output_delivery.target.credential_mode %q is not supported", target.GetCredentialMode())
	}
	credentialMode := strings.ToLower(strings.TrimSpace(delivery.GetCredentialMode()))
	switch credentialMode {
	case "", "none", "user":
		delivery.CredentialMode = credentialMode
	default:
		return fmt.Errorf("target.agent.output_delivery.credential_mode %q is not supported", delivery.GetCredentialMode())
	}
	for _, binding := range delivery.GetInputBindings() {
		if binding == nil || binding.GetValue() == nil || binding.GetValue().GetKind() == nil {
			return errors.New("target.agent.output_delivery.input_bindings.value is required")
		}
		binding.InputField = strings.TrimSpace(binding.GetInputField())
		if binding.GetInputField() == "" {
			return errors.New("target.agent.output_delivery.input_bindings.input_field is required")
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
	out := gproto.Clone(event).(*proto.WorkflowEvent)
	out.Id = strings.TrimSpace(out.GetId())
	out.Source = strings.TrimSpace(out.GetSource())
	out.SpecVersion = strings.TrimSpace(out.GetSpecVersion())
	out.Type = strings.TrimSpace(out.GetType())
	out.Subject = strings.TrimSpace(out.GetSubject())
	out.Datacontenttype = strings.TrimSpace(out.GetDatacontenttype())
	if out.GetSource() == "" {
		return nil, errors.New("event.source is required")
	}
	if out.GetType() == "" {
		return nil, errors.New("event.type is required")
	}
	if out.GetSpecVersion() == "" {
		out.SpecVersion = defaultSpecVersion
	}
	if out.GetTime() == nil || !out.GetTime().IsValid() {
		out.Time = timestamppb.New(now().UTC())
	}
	out.Data = cloneStruct(out.GetData())
	return out, nil
}

func normalizeWorkflowSignal(signal *proto.WorkflowSignal, now time.Time) (*proto.WorkflowSignal, error) {
	if signal == nil {
		return nil, errors.New("signal is required")
	}
	out := gproto.Clone(signal).(*proto.WorkflowSignal)
	out.Id = strings.TrimSpace(out.GetId())
	out.Name = strings.TrimSpace(out.GetName())
	out.IdempotencyKey = strings.TrimSpace(out.GetIdempotencyKey())
	if out.GetName() == "" {
		return nil, errors.New("signal.name is required")
	}
	if out.GetCreatedAt() == nil || !out.GetCreatedAt().IsValid() {
		out.CreatedAt = timestamppb.New(now.UTC())
	}
	out.Payload = cloneStruct(out.GetPayload())
	out.Metadata = cloneStruct(out.GetMetadata())
	out.CreatedBy = cloneActor(out.GetCreatedBy())
	return out, nil
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

func cloneStruct(value *structpb.Struct) *structpb.Struct {
	if value == nil {
		return nil
	}
	return gproto.Clone(value).(*structpb.Struct)
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
	return &proto.WorkflowRunTrigger{Kind: &proto.WorkflowRunTrigger_Schedule{Schedule: &proto.WorkflowScheduleTrigger{
		ScheduleId:   strings.TrimSpace(scheduleID),
		ScheduledFor: timestamppb.New(scheduledFor.UTC()),
	}}}
}

func eventTrigger(triggerID string, event *proto.WorkflowEvent) *proto.WorkflowRunTrigger {
	return &proto.WorkflowRunTrigger{Kind: &proto.WorkflowRunTrigger_Event{Event: &proto.WorkflowEventTriggerInvocation{
		TriggerId: strings.TrimSpace(triggerID),
		Event:     cloneEvent(event),
	}}}
}

func publicRunID(workflowID, runID string) string {
	return encodeRunHandle(workflowID, runID)
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
