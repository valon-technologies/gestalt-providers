package temporal

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
)

func ownerIdempotencyLedgerKey(ownerKey, key string) string {
	ownerKey = strings.TrimSpace(ownerKey)
	key = strings.TrimSpace(key)
	if ownerKey == "" || key == "" {
		return ""
	}
	return "owner-idem:" + hashID(ownerKey, key)
}

func explicitSignalLedgerKey(signal *proto.WorkflowSignal) string {
	id := strings.TrimSpace(signal.GetId())
	if id == "" {
		return ""
	}
	return "signal-id:" + hashID(id)
}

func signalFingerprint(ownerKey, workflowKey string, signal *proto.WorkflowSignal) string {
	stableSignal := cloneSignal(signal)
	if stableSignal != nil {
		stableSignal.CreatedAt = nil
		stableSignal.Sequence = 0
	}
	return hashID("signal", ownerKey, workflowKey, protoHashID(stableSignal))
}

func startFingerprint(ownerKey, key, workflowKey, executionRef string, target *proto.BoundWorkflowTarget, createdBy *proto.WorkflowActor) string {
	return hashID("start", ownerKey, key, workflowKey, executionRef, protoHashID(target), protoHashID(createdBy))
}

func eventRunWorkflowID(scopeID, triggerID string, event *proto.WorkflowEvent) string {
	// The event-v3 family is a persisted idempotency namespace for published event
	// IDs. Keep it stable even though the provider runtime is now V4-only.
	if event.GetId() != "" {
		return workflowID(scopeID, "event-v3", triggerID, event.GetSource(), event.GetId())
	}
	return workflowID(scopeID, "event-v3", triggerID, event.GetSource(), uuid.NewString())
}

func signalForStartedRun(run *proto.BoundWorkflowRun, signal *proto.WorkflowSignal) *proto.WorkflowSignal {
	out := cloneSignal(signal)
	if out == nil {
		return nil
	}
	if out.GetSequence() <= 0 {
		out.Sequence = 1
	}
	if strings.TrimSpace(out.GetId()) == "" {
		out.Id = "signal:" + hashID(run.GetId(), out.GetName(), fmt.Sprintf("%d", out.GetSequence()), out.GetIdempotencyKey())
	}
	return out
}
