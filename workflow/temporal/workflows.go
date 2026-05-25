package temporal

import (
	"context"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	gestaltworkflow "github.com/valon-technologies/gestalt/sdk/go/workflow"
)

const (
	updateAddSignal = "gestalt.add_signal"
	updateCancelRun = "gestalt.cancel_run"
	updateClaimRun  = "gestalt.claim_run"

	workflowInvokeMetadataWorkflowKey  = "workflow_key"
	workflowInvokeMetadataDefinitionID = "definition_id"
)

type workflowActivities struct {
	executor gestaltworkflow.StepExecutor
	state    *workflowStateStore
}

func (a *workflowActivities) ExecuteSteps(ctx context.Context, req gestaltworkflow.Request) (*gestaltworkflow.Response, error) {
	return a.executor.Execute(ctx, req)
}

func (a *workflowActivities) ProjectRun(ctx context.Context, run gestalt.BoundWorkflowRun) error {
	if a.state == nil {
		return nil
	}
	return a.state.putRun(ctx, &run)
}
