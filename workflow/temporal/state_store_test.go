package temporal

import (
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	gproto "google.golang.org/protobuf/proto"
)

func TestWorkflowStateStoreDecodesLegacyDurablePayloads(t *testing.T) {
	base := time.Unix(1_700_000_000, 0).UTC()
	nextRunAt := base.Add(5 * time.Minute)
	revokedAt := base.Add(24 * time.Hour)

	scheduleInput := gestalt.BoundWorkflowScheduleInput{
		ID:           "schedule-1",
		Cron:         "*/5 * * * *",
		Timezone:     "America/New_York",
		Target:       nativePluginTargetInput("slack", "postMessage"),
		Paused:       true,
		CreatedAt:    base,
		UpdatedAt:    base.Add(time.Minute),
		NextRunAt:    &nextRunAt,
		CreatedBy:    actor("user-1"),
		ExecutionRef: "ref-1",
	}
	legacySchedule, err := gestalt.NewBoundWorkflowSchedule(scheduleInput)
	if err != nil {
		t.Fatalf("NewBoundWorkflowSchedule: %v", err)
	}
	gotSchedule, err := scheduleInputFromRecord(gestalt.Record{"payload": mustMarshalProto(t, legacySchedule)})
	if err != nil {
		t.Fatalf("scheduleInputFromRecord: %v", err)
	}
	if gotSchedule.ID != scheduleInput.ID || gotSchedule.Cron != scheduleInput.Cron || gotSchedule.Timezone != scheduleInput.Timezone || gotSchedule.ExecutionRef != scheduleInput.ExecutionRef {
		t.Fatalf("decoded schedule = %#v, want fields from %#v", gotSchedule, scheduleInput)
	}
	if gotSchedule.NextRunAt == nil || !gotSchedule.NextRunAt.Equal(nextRunAt) {
		t.Fatalf("decoded schedule next run = %v, want %v", gotSchedule.NextRunAt, nextRunAt)
	}
	assertPluginTarget(t, gotSchedule.Target, "slack", "postMessage")

	triggerInput := gestalt.BoundWorkflowEventTriggerInput{
		ID: "trigger-1",
		Match: &gestalt.WorkflowEventMatchInput{
			Type:    "message.created",
			Source:  "slack",
			Subject: "channel-1",
		},
		Target:       nativePluginTargetInput("gmail", "messages.send"),
		Paused:       true,
		CreatedAt:    base,
		UpdatedAt:    base.Add(time.Minute),
		CreatedBy:    actor("user-1"),
		ExecutionRef: "ref-1",
	}
	legacyTrigger, err := gestalt.NewBoundWorkflowEventTrigger(triggerInput)
	if err != nil {
		t.Fatalf("NewBoundWorkflowEventTrigger: %v", err)
	}
	gotTrigger, err := triggerFromRecord(gestalt.Record{"payload": mustMarshalProto(t, legacyTrigger)})
	if err != nil {
		t.Fatalf("triggerFromRecord: %v", err)
	}
	if gotTrigger.ID != triggerInput.ID || gotTrigger.ExecutionRef != triggerInput.ExecutionRef || gotTrigger.Match == nil || gotTrigger.Match.Type != triggerInput.Match.Type {
		t.Fatalf("decoded trigger = %#v, want fields from %#v", gotTrigger, triggerInput)
	}
	assertPluginTarget(t, gotTrigger.Target, "gmail", "messages.send")

	refInput := gestalt.WorkflowExecutionReferenceInput{
		ID:                  "ref-1",
		ProviderName:        "temporal",
		Target:              nativePluginTargetInput("linear", "issues"),
		SubjectID:           "user-1",
		CredentialSubjectID: "credential-user-1",
		Permissions: []gestalt.WorkflowAccessPermissionInput{{
			Plugin:     "linear",
			Operations: []string{"issues"},
		}},
		CreatedAt:          base,
		RevokedAt:          &revokedAt,
		SubjectKind:        "user",
		DisplayName:        "Workflow User",
		AuthSource:         "google",
		CallerPluginName:   "brain",
		SourceDefinitionID: "cfg-source",
	}
	legacyRef, err := gestalt.NewWorkflowExecutionReference(refInput)
	if err != nil {
		t.Fatalf("NewWorkflowExecutionReference: %v", err)
	}
	gotRef, err := executionRefFromRecord(gestalt.Record{"payload": mustMarshalProto(t, legacyRef)})
	if err != nil {
		t.Fatalf("executionRefFromRecord: %v", err)
	}
	if gotRef.ID != refInput.ID || gotRef.ProviderName != refInput.ProviderName || gotRef.SubjectID != refInput.SubjectID || gotRef.CallerPluginName != refInput.CallerPluginName {
		t.Fatalf("decoded execution ref = %#v, want fields from %#v", gotRef, refInput)
	}
	if gotRef.RevokedAt == nil || !gotRef.RevokedAt.Equal(revokedAt) {
		t.Fatalf("decoded execution ref revoked at = %v, want %v", gotRef.RevokedAt, revokedAt)
	}
	assertPluginTarget(t, gotRef.Target, "linear", "issues")
}

func mustMarshalProto(t *testing.T, msg gproto.Message) []byte {
	t.Helper()
	payload, err := gproto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal proto: %v", err)
	}
	return payload
}

func assertPluginTarget(t *testing.T, target *gestalt.BoundWorkflowTargetInput, plugin, operation string) {
	t.Helper()
	if target == nil || target.Plugin == nil {
		t.Fatalf("target = %#v, want plugin target", target)
	}
	if target.Plugin.PluginName != plugin || target.Plugin.Operation != operation {
		t.Fatalf("target plugin = %q operation = %q, want %q %q", target.Plugin.PluginName, target.Plugin.Operation, plugin, operation)
	}
}
