package temporal

import (
	"context"

	gestaltworkflow "github.com/valon-technologies/gestalt/sdk/go/workflow"
)

const (
	updateAddSignal = "gestalt.add_signal"
	updateCancelRun = "gestalt.cancel_run"

	queryGetRun = "gestalt.get_run"
)

type workflowActivities struct {
	executor gestaltworkflow.StepExecutor
}

func (a *workflowActivities) ExecuteStep(ctx context.Context, req gestaltworkflow.StepRequest) (*gestaltworkflow.StepResponse, error) {
	return a.executor.ExecuteStep(ctx, req)
}
