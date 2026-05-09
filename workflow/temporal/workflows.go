package temporal

import (
	"context"

	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	gproto "google.golang.org/protobuf/proto"
)

const (
	queryRunState = "gestalt.run_state"

	updateAddSignal = "gestalt.add_signal"
	updateCancelRun = "gestalt.cancel_run"
	updateClaimRun  = "gestalt.claim_run"

	workflowInvokeMetadataWorkflowKey = "workflow_key"
)

type workflowActivities struct {
	host  workflowHost
	state *workflowStateStore
}

func (a *workflowActivities) InvokeOperation(ctx context.Context, req *proto.InvokeWorkflowOperationRequest) (*proto.InvokeWorkflowOperationResponse, error) {
	return a.host.InvokeOperation(ctx, req)
}

func (a *workflowActivities) ProjectRun(ctx context.Context, run *proto.BoundWorkflowRun) error {
	if a.state == nil {
		return nil
	}
	return a.state.putRun(ctx, run)
}

func cloneRunTrigger(trigger *proto.WorkflowRunTrigger) *proto.WorkflowRunTrigger {
	if trigger == nil {
		return nil
	}
	return gproto.Clone(trigger).(*proto.WorkflowRunTrigger)
}

func cloneSignals(signals []*proto.WorkflowSignal) []*proto.WorkflowSignal {
	if len(signals) == 0 {
		return nil
	}
	out := make([]*proto.WorkflowSignal, 0, len(signals))
	for _, signal := range signals {
		out = append(out, cloneSignal(signal))
	}
	return out
}
