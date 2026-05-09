package temporal

import (
	"context"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
)

const (
	updateAddSignal = "gestalt.add_signal"
	updateCancelRun = "gestalt.cancel_run"
	updateClaimRun  = "gestalt.claim_run"

	workflowInvokeMetadataWorkflowKey = "workflow_key"
)

type workflowActivities struct {
	host  workflowHost
	state *workflowStateStore
}

func (a *workflowActivities) InvokeOperation(ctx context.Context, req gestalt.InvokeWorkflowOperationInput) (*gestalt.InvokeWorkflowOperationResponse, error) {
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
	out, err := gestalt.NewWorkflowRunTriggerFromTrigger(trigger)
	if err != nil {
		panic("clone workflow run trigger: " + err.Error())
	}
	return out
}
