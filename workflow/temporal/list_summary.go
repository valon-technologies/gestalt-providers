package temporal

import (
	"encoding/json"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

const memoKeyListSummary = "gestaltListSummary"

// runListSummaryMemo is the visibility-memo projection ListRuns reads so the
// workflows table can show duration and trigger without per-run GetRun calls.
//
// Trigger is a list-safe projection: activation/kind metadata only. Full event
// payloads stay in workflow args / GetRun state and are never copied into
// visibility memos.
type runListSummaryMemo struct {
	Trigger   *gestalt.WorkflowRunTrigger `json:"trigger,omitempty"`
	StartedAt *time.Time                  `json:"startedAt,omitempty"`
	CreatedBy string                      `json:"createdBy,omitempty"`
}

func runListSummaryFromInput(input runWorkflowInput, now time.Time) runListSummaryMemo {
	return runListSummaryMemo{
		Trigger:   listSummaryTrigger(input.triggerInput(now)),
		CreatedBy: cloneCreatedBy(input.CreatedBy),
	}
}

// runListSummaryForScheduleAction builds the memo attached to a Temporal
// ScheduleWorkflowAction. Fire time is unknown until TemporalRun starts, so
// ScheduledFor is omitted and filled on the first visibility upsert.
func runListSummaryForScheduleAction(activationID, createdBy string) runListSummaryMemo {
	return runListSummaryMemo{
		Trigger: &gestalt.WorkflowRunTrigger{Schedule: &gestalt.WorkflowScheduleTrigger{
			ActivationID: strings.TrimSpace(activationID),
		}},
		CreatedBy: cloneCreatedBy(createdBy),
	}
}

func runListSummaryFromRun(run *gestalt.WorkflowRun) runListSummaryMemo {
	if run == nil {
		return runListSummaryMemo{}
	}
	summary := runListSummaryMemo{
		Trigger:   listSummaryTrigger(run.Trigger),
		CreatedBy: cloneCreatedBy(run.CreatedBy),
	}
	if run.StartedAt != nil {
		started := run.StartedAt.UTC()
		summary.StartedAt = &started
	}
	return summary
}

// listSummaryTrigger projects a run trigger into the fields ListRuns needs for
// duration/trigger columns. Event Data/Extensions are dropped so visibility
// memos stay small and do not duplicate webhook payloads.
func listSummaryTrigger(trigger *gestalt.WorkflowRunTrigger) *gestalt.WorkflowRunTrigger {
	if trigger == nil {
		return nil
	}
	if trigger.Manual {
		return &gestalt.WorkflowRunTrigger{Manual: true}
	}
	if trigger.Schedule != nil {
		out := &gestalt.WorkflowRunTrigger{Schedule: &gestalt.WorkflowScheduleTrigger{
			ActivationID: strings.TrimSpace(trigger.Schedule.ActivationID),
		}}
		if trigger.Schedule.ScheduledFor != nil {
			scheduledFor := trigger.Schedule.ScheduledFor.UTC()
			out.Schedule.ScheduledFor = &scheduledFor
		}
		if out.Schedule.ActivationID == "" && out.Schedule.ScheduledFor == nil {
			return nil
		}
		return out
	}
	if trigger.Event != nil {
		out := &gestalt.WorkflowRunTrigger{Event: &gestalt.WorkflowEventTriggerInvocation{
			ActivationID: strings.TrimSpace(trigger.Event.ActivationID),
		}}
		if event := trigger.Event.Event; event != nil {
			out.Event.Event = &gestalt.WorkflowEvent{
				ID:              strings.TrimSpace(event.ID),
				Source:          strings.TrimSpace(event.Source),
				SpecVersion:     strings.TrimSpace(event.SpecVersion),
				Type:            strings.TrimSpace(event.Type),
				Subject:         strings.TrimSpace(event.Subject),
				DataContentType: strings.TrimSpace(event.DataContentType),
			}
			if !event.Time.IsZero() {
				out.Event.Event.Time = event.Time.UTC()
			}
		}
		if out.Event.ActivationID == "" && out.Event.Event == nil {
			return nil
		}
		return out
	}
	return nil
}

func runStartMemo(ownerKey string, summary runListSummaryMemo) map[string]any {
	memo := map[string]any{}
	if key := strings.TrimSpace(ownerKey); key != "" {
		memo[memoKeyOwnerKey] = key
	}
	if encoded := encodeRunListSummaryMemo(summary); encoded != "" {
		memo[memoKeyListSummary] = encoded
	}
	if len(memo) == 0 {
		return nil
	}
	return memo
}

func encodeRunListSummaryMemo(summary runListSummaryMemo) string {
	summary.Trigger = listSummaryTrigger(summary.Trigger)
	summary.CreatedBy = cloneCreatedBy(summary.CreatedBy)
	if summary.StartedAt != nil {
		started := summary.StartedAt.UTC()
		summary.StartedAt = &started
	}
	if summary.Trigger == nil && summary.StartedAt == nil && strings.TrimSpace(summary.CreatedBy) == "" {
		return ""
	}
	raw, err := json.Marshal(summary)
	if err != nil {
		return ""
	}
	return string(raw)
}

func decodeRunListSummaryMemo(raw string) runListSummaryMemo {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return runListSummaryMemo{}
	}
	var summary runListSummaryMemo
	if err := json.Unmarshal([]byte(raw), &summary); err != nil {
		return runListSummaryMemo{}
	}
	// Re-project on read so legacy fat memos cannot resurface event payloads.
	return runListSummaryMemo{
		Trigger:   listSummaryTrigger(summary.Trigger),
		CreatedBy: cloneCreatedBy(summary.CreatedBy),
		StartedAt: func() *time.Time {
			if summary.StartedAt == nil {
				return nil
			}
			started := summary.StartedAt.UTC()
			return &started
		}(),
	}
}

func payloadListSummary(payload *commonpb.Payload) runListSummaryMemo {
	if payload == nil {
		return runListSummaryMemo{}
	}
	var raw string
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &raw); err == nil {
		if summary := decodeRunListSummaryMemo(raw); summary.Trigger != nil || summary.StartedAt != nil || summary.CreatedBy != "" {
			return summary
		}
	}
	var summary runListSummaryMemo
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &summary); err != nil {
		return runListSummaryMemo{}
	}
	return decodeRunListSummaryMemo(encodeRunListSummaryMemo(summary))
}
