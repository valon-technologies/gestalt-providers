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

	queryGetRun = "gestalt.get_run"
)

type workflowActivities struct {
	executor gestaltworkflow.StepExecutor
	state    *workflowStateStore
}

func (a *workflowActivities) ExecuteStep(ctx context.Context, req gestaltworkflow.StepRequest) (*gestaltworkflow.StepResponse, error) {
	return a.executor.ExecuteStep(ctx, req)
}

func (a *workflowActivities) ProjectRun(ctx context.Context, run gestalt.WorkflowRun) error {
	if a.state == nil {
		return nil
	}
	return a.state.putRun(ctx, &run)
}
