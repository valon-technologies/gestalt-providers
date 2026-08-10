package temporal

import (
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
