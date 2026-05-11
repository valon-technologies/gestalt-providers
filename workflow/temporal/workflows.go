package temporal

import (
	"context"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
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

func (a *workflowActivities) ProjectRun(ctx context.Context, run gestalt.BoundWorkflowRunInput) error {
	if a.state == nil {
		return nil
	}
	return a.state.putRun(ctx, &run)
}
