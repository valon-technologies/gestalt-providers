package temporal

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func ownerIdempotencyLedgerKey(ownerKey, key string) string {
	ownerKey = strings.TrimSpace(ownerKey)
	key = strings.TrimSpace(key)
	if ownerKey == "" || key == "" {
		return ""
	}
	return "owner-idem:" + hashID(ownerKey, key)
}

func explicitSignalLedgerKey(signal *gestalt.WorkflowSignal) string {
	if signal == nil {
		return ""
	}
	id := strings.TrimSpace(signal.ID)
	if id == "" {
		return ""
	}
	return "signal-id:" + hashID(id)
}

func signalFingerprint(ownerKey, workflowKey string, signal *gestalt.WorkflowSignal) string {
	stableSignal := cloneSignalInput(signal)
	if stableSignal != nil {
		stableSignal.CreatedAt = time.Time{}
		stableSignal.Sequence = 0
	}
	return hashID("signal", ownerKey, workflowKey, valueHashID(stableSignal))
}

func startFingerprint(ownerKey, key, workflowKey, definitionID string, target *gestalt.BoundWorkflowTarget, createdBySubjectID string) string {
	return hashID("start", ownerKey, key, workflowKey, definitionID, valueHashID(target), valueHashID(createdBySubjectID))
}

func idempotentDefinitionID(ownerKey, key string) string {
	return "definition:" + hashID(strings.TrimSpace(ownerKey), strings.TrimSpace(key))
}

func eventRunWorkflowID(scopeID, triggerID string, event *gestalt.WorkflowEvent) string {
	// The event-v3 family is a persisted idempotency namespace for published event
	// IDs. Keep it stable even though the provider runtime is now V4-only.
	if event != nil && strings.TrimSpace(event.ID) != "" {
		return workflowID(scopeID, "event-v3", triggerID, event.Source, event.ID)
	}
	source := ""
	if event != nil {
		source = event.Source
	}
	return workflowID(scopeID, "event-v3", triggerID, source, uuid.NewString())
}

func signalInputForStartedRun(run *gestalt.BoundWorkflowRun, signal *gestalt.WorkflowSignal) *gestalt.WorkflowSignal {
	if signal == nil {
		return nil
	}
	out := *signal
	if out.Sequence <= 0 {
		out.Sequence = 1
	}
	if strings.TrimSpace(out.ID) == "" {
		runID := ""
		if run != nil {
			runID = run.ID
		}
		out.ID = "signal:" + hashID(runID, out.Name, fmt.Sprintf("%d", out.Sequence), out.IdempotencyKey)
	}
	return &out
}
