package temporal

import (
	"context"
	"errors"
	"fmt"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	gestaltworkflow "github.com/valon-technologies/gestalt/sdk/go/workflow"
	sdktemporal "go.temporal.io/sdk/temporal"
)

const (
	updateAddSignal = "gestalt.add_signal"
	updateCancelRun = "gestalt.cancel_run"

	queryGetRun = "gestalt.get_run"

	workflowOperationExecuteRun = "execute_run"

	retryableStepResponseErrorType = "gestalt.workflow.step_response_failed"
)

type workflowActivities struct {
	executor gestaltworkflow.StepExecutor
	backend  *temporalBackend
}

func (a *workflowActivities) ExecuteStep(ctx context.Context, req gestaltworkflow.StepRequest) (*gestaltworkflow.StepResponse, error) {
	resp, err := a.executor.ExecuteStep(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("workflow step execution failed: %w", err)
	}
	if err := retryableStepResponseError(resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func retryableStepResponseError(resp *gestaltworkflow.StepResponse) error {
	if resp == nil {
		return fmt.Errorf("workflow step returned no response")
	}
	status := workflowStepStatusFromStringInput(resp.Step.Status)
	if resp.Status < 400 && status != gestalt.WorkflowStepStatusValueFailed {
		return nil
	}
	message := workflowStepFailureMessageInput(resp, "")
	if message == "" {
		message = fmt.Sprintf("workflow operation returned status %d", resp.Status)
	}
	if resp.Step.ID != "" {
		message = fmt.Sprintf("workflow step %q failed: %s", resp.Step.ID, message)
	}
	return sdktemporal.NewApplicationError(message, retryableStepResponseErrorType, resp)
}

func failedStepResponseFromActivityError(err error) (*gestaltworkflow.StepResponse, bool) {
	var applicationErr *sdktemporal.ApplicationError
	if !errors.As(err, &applicationErr) || applicationErr.Type() != retryableStepResponseErrorType {
		return nil, false
	}
	var resp gestaltworkflow.StepResponse
	if err := applicationErr.Details(&resp); err != nil {
		return nil, false
	}
	return &resp, true
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
