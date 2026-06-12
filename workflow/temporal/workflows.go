package temporal

import (
	"context"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	gestaltworkflow "github.com/valon-technologies/gestalt/sdk/go/workflow"
)

const (
	updateAddSignal = "gestalt.add_signal"
	updateCancelRun = "gestalt.cancel_run"

	queryGetRun = "gestalt.get_run"

	workflowOperationExecuteRun = "execute_run"
)

type workflowActivities struct {
	executor gestaltworkflow.StepExecutor
	backend  *temporalBackend
}

func (a *workflowActivities) ExecuteStep(ctx context.Context, req gestaltworkflow.StepRequest) (*gestaltworkflow.StepResponse, error) {
	return a.executor.ExecuteStep(ctx, req)
}

func (a *workflowActivities) RecordRunCompleted(ctx context.Context, run *gestalt.WorkflowRun) error {
	if run == nil {
		return nil
	}
	startedAt := run.CreatedAt
	if run.StartedAt != nil {
		startedAt = *run.StartedAt
	}
	gestalt.RecordWorkflowRunCompleted(ctx, startedAt, a.backend.workflowTelemetryOptions(
		workflowOperationExecuteRun,
		workflowTriggerKindInput(run.Trigger),
		workflowTelemetryTargetKindInput(run.Target),
		workflowTelemetryRunStatus(run),
	))
	return nil
}
