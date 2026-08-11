package temporal

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func TestRunListSummaryMemoRoundTrip(t *testing.T) {
	startedAt := time.Date(2026, 8, 10, 15, 48, 0, 683000000, time.UTC)
	summary := runListSummaryMemo{
		Trigger:   scheduleTriggerInput("weekday_morning", startedAt),
		StartedAt: &startedAt,
		CreatedBy: "subject:system",
	}
	encoded := encodeRunListSummaryMemo(summary)
	if encoded == "" {
		t.Fatal("expected encoded summary")
	}
	got := decodeRunListSummaryMemo(encoded)
	if got.CreatedBy != "subject:system" {
		t.Fatalf("createdBy = %q", got.CreatedBy)
	}
	if got.StartedAt == nil || !got.StartedAt.Equal(startedAt) {
		t.Fatalf("startedAt = %#v", got.StartedAt)
	}
	if got.Trigger == nil || got.Trigger.Schedule == nil || got.Trigger.Schedule.ActivationID != "weekday_morning" {
		t.Fatalf("trigger = %#v", got.Trigger)
	}
}

func TestRunStartMemoIncludesOwnerAndSummary(t *testing.T) {
	now := time.Date(2026, 8, 10, 15, 48, 0, 0, time.UTC)
	input := runWorkflowInput{
		OwnerKey:  "ai-spend-tracker",
		CreatedBy: "subject:system",
		Trigger:   manualTriggerInput(),
	}
	memo := runStartMemo(input.OwnerKey, runListSummaryFromInput(input, now))
	if memo[memoKeyOwnerKey] != "ai-spend-tracker" {
		t.Fatalf("owner memo = %#v", memo[memoKeyOwnerKey])
	}
	raw, _ := memo[memoKeyListSummary].(string)
	got := decodeRunListSummaryMemo(raw)
	if !got.Trigger.Manual || got.CreatedBy != "subject:system" {
		t.Fatalf("summary = %#v", got)
	}
}

func TestListSummaryTriggerStripsEventPayload(t *testing.T) {
	trigger := &gestalt.WorkflowRunTrigger{Event: &gestalt.WorkflowEventTriggerInvocation{
		ActivationID: "slack_alert",
		Event: &gestalt.WorkflowEvent{
			ID:         "event-1",
			Source:     "slack",
			Type:       "message.created",
			Subject:    "channel:alerts",
			Data:       map[string]any{"text": "secret body", "channel": "alerts"},
			Extensions: map[string]any{"token": "should-not-persist"},
		},
	}}
	got := listSummaryTrigger(trigger)
	if got == nil || got.Event == nil || got.Event.Event == nil {
		t.Fatalf("projected = %#v", got)
	}
	if got.Event.ActivationID != "slack_alert" || got.Event.Event.Source != "slack" || got.Event.Event.Type != "message.created" {
		t.Fatalf("projected metadata = %#v", got.Event)
	}
	if got.Event.Event.Data != nil || got.Event.Event.Extensions != nil {
		t.Fatalf("expected payload fields stripped, got %#v", got.Event.Event)
	}
	encoded := encodeRunListSummaryMemo(runListSummaryMemo{Trigger: trigger})
	if strings.Contains(encoded, "secret body") || strings.Contains(encoded, "should-not-persist") {
		t.Fatalf("encoded memo leaked event payload: %s", encoded)
	}
}

func TestDecodeRunListSummaryMemoScrubsLegacyEventPayload(t *testing.T) {
	startedAt := time.Date(2026, 8, 10, 15, 48, 0, 0, time.UTC)
	legacy, err := json.Marshal(runListSummaryMemo{
		StartedAt: &startedAt,
		Trigger: &gestalt.WorkflowRunTrigger{Event: &gestalt.WorkflowEventTriggerInvocation{
			ActivationID: "slack_alert",
			Event: &gestalt.WorkflowEvent{
				ID:     "event-1",
				Source: "slack",
				Type:   "message.created",
				Data:   map[string]any{"text": "legacy secret"},
			},
		}},
	})
	if err != nil {
		t.Fatalf("marshal legacy: %v", err)
	}
	got := decodeRunListSummaryMemo(string(legacy))
	if got.Trigger == nil || got.Trigger.Event == nil || got.Trigger.Event.Event == nil {
		t.Fatalf("decoded = %#v", got)
	}
	if got.Trigger.Event.Event.Data != nil {
		t.Fatalf("expected scrubbed data, got %#v", got.Trigger.Event.Event.Data)
	}
}

func TestRunListSummaryForScheduleActionOmitsScheduledFor(t *testing.T) {
	got := runListSummaryForScheduleAction("weekday_morning", "subject:system")
	if got.Trigger == nil || got.Trigger.Schedule == nil {
		t.Fatalf("summary = %#v", got)
	}
	if got.Trigger.Schedule.ActivationID != "weekday_morning" {
		t.Fatalf("activation = %q", got.Trigger.Schedule.ActivationID)
	}
	if got.Trigger.Schedule.ScheduledFor != nil {
		t.Fatalf("ScheduledFor = %#v, want nil until run starts", got.Trigger.Schedule.ScheduledFor)
	}
	if got.CreatedBy != "subject:system" {
		t.Fatalf("createdBy = %q", got.CreatedBy)
	}
}

func TestRunListSummaryFromRunIncludesStartedAt(t *testing.T) {
	startedAt := time.Date(2026, 8, 10, 15, 48, 0, 0, time.UTC)
	run := &gestalt.WorkflowRun{
		Trigger:   manualTriggerInput(),
		StartedAt: &startedAt,
		CreatedBy: "subject:system",
	}
	got := runListSummaryFromRun(run)
	if got.StartedAt == nil || !got.StartedAt.Equal(startedAt) {
		t.Fatalf("startedAt = %#v", got.StartedAt)
	}
	if !got.Trigger.Manual || got.CreatedBy != "subject:system" {
		t.Fatalf("summary = %#v", got)
	}
}

func TestCloneTriggerInputDeepCopiesSchedule(t *testing.T) {
	scheduledFor := time.Date(2026, 8, 10, 15, 48, 0, 0, time.UTC)
	original := &gestalt.WorkflowRunTrigger{Schedule: &gestalt.WorkflowScheduleTrigger{
		ActivationID: "weekday_morning",
		ScheduledFor: &scheduledFor,
	}}
	cloned := cloneTriggerInput(original)
	if cloned == original || cloned.Schedule == original.Schedule {
		t.Fatal("expected deep copy")
	}
	cloned.Schedule.ActivationID = "other"
	if original.Schedule.ActivationID != "weekday_morning" {
		t.Fatal("mutating clone changed original")
	}
}
