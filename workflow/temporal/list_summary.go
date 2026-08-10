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
type runListSummaryMemo struct {
	Trigger   *gestalt.WorkflowRunTrigger `json:"trigger,omitempty"`
	StartedAt *time.Time                  `json:"startedAt,omitempty"`
	CreatedBy string                      `json:"createdBy,omitempty"`
}

func runListSummaryFromInput(input runWorkflowInput, now time.Time) runListSummaryMemo {
	return runListSummaryMemo{
		Trigger:   input.triggerInput(now),
		CreatedBy: cloneCreatedBy(input.CreatedBy),
	}
}

func runListSummaryFromRun(run *gestalt.WorkflowRun) runListSummaryMemo {
	if run == nil {
		return runListSummaryMemo{}
	}
	summary := runListSummaryMemo{
		Trigger:   cloneTriggerInput(run.Trigger),
		CreatedBy: cloneCreatedBy(run.CreatedBy),
	}
	if run.StartedAt != nil {
		started := run.StartedAt.UTC()
		summary.StartedAt = &started
	}
	return summary
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
	summary.CreatedBy = cloneCreatedBy(summary.CreatedBy)
	if summary.StartedAt != nil {
		started := summary.StartedAt.UTC()
		summary.StartedAt = &started
	}
	return summary
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
