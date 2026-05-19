package temporal

import (
	"context"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

const (
	updateAddSignal       = "gestalt.add_signal"
	updateActivateBinding = "gestalt.activate_binding"
	updateAbortBinding    = "gestalt.abort_binding"
	updateCancelRun       = "gestalt.cancel_run"
	updateClaimRun        = "gestalt.claim_run"

	workflowInvokeMetadataWorkflowKey = "workflow_key"
)

type workflowActivities struct {
	host  workflowHost
	state *workflowStateStore
}

func (a *workflowActivities) InvokeOperation(ctx context.Context, req gestalt.InvokeWorkflowOperationInput) (*gestalt.InvokeWorkflowOperationResponse, error) {
	return a.host.InvokeOperation(ctx, req)
}

func (a *workflowActivities) InvokeWorkflowPluginAction(ctx context.Context, req gestalt.InvokeWorkflowPluginActionInput) (*gestalt.WorkflowHostActionResponse, error) {
	return a.host.InvokeWorkflowPluginAction(ctx, req)
}

func (a *workflowActivities) InvokeWorkflowAgentTurn(ctx context.Context, req gestalt.InvokeWorkflowAgentTurnInput) (*gestalt.WorkflowHostActionResponse, error) {
	return a.host.InvokeWorkflowAgentTurn(ctx, req)
}

func (a *workflowActivities) ProjectRun(ctx context.Context, run gestalt.BoundWorkflowRun) error {
	if a.state == nil {
		return nil
	}
	return a.state.putRun(ctx, &run)
}
