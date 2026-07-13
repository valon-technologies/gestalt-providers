package temporal

import (
	"fmt"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	gestaltworkflow "github.com/valon-technologies/gestalt/sdk/go/workflow"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type runWorkflowInput struct {
	ActivityStartToCloseTimeoutNS time.Duration                `json:"activity_start_to_close_timeout_ns"`
	ScopeID                       string                       `json:"scope_id,omitempty"`
	ProviderName                  string                       `json:"provider_name,omitempty"`
	ActivationID                  string                       `json:"activation_id,omitempty"`
	DefinitionID                  string                       `json:"definition_id,omitempty"`
	DefinitionGeneration          int64                        `json:"definition_generation,omitempty"`
	Input                         map[string]any               `json:"input,omitempty"`
	RunAs                         *gestalt.Subject             `json:"run_as,omitempty"`
	WorkflowKey                   string                       `json:"workflow_key,omitempty"`
	OwnerKey                      string                       `json:"owner_key,omitempty"`
	Target                        *gestalt.BoundWorkflowTarget `json:"target,omitempty"`
	Trigger                       *gestalt.WorkflowRunTrigger  `json:"trigger,omitempty"`
	CreatedBy                     string                       `json:"created_by,omitempty"`
	InitialSignal                 *gestalt.WorkflowSignal      `json:"initial_signal,omitempty"`
	RequireSignal                 bool                         `json:"require_signal,omitempty"`
}

// TemporalRun is the Temporal workflow type for Gestalt workflow runs.
func TemporalRun(ctx workflow.Context, input runWorkflowInput) (*gestalt.WorkflowRun, error) {
	info := workflow.GetInfo(ctx)
	now := workflow.Now(ctx).UTC()
	publicID := encodeTemporalRunHandle(temporalRunHandle{
		Kind:             runHandleKind,
		RunWorkflowID:    info.WorkflowExecution.ID,
		RunTemporalRunID: info.WorkflowExecution.RunID,
		WorkflowKey:      input.WorkflowKey,
		OwnerKey:         input.OwnerKey,
	})
	state := &gestalt.WorkflowRun{
		ID:                   publicID,
		Status:               gestalt.WorkflowRunStatusValuePending,
		Target:               input.Target,
		Trigger:              input.triggerInput(now),
		CreatedAt:            now,
		CreatedBy:            input.CreatedBy,
		RunAs:                cloneSubjectInput(input.RunAs),
		WorkflowKey:          strings.TrimSpace(input.WorkflowKey),
		DefinitionID:         strings.TrimSpace(input.DefinitionID),
		DefinitionGeneration: input.DefinitionGeneration,
		ProviderName:         strings.TrimSpace(input.ProviderName),
		Input:                cloneMapInput(input.Input),
	}
	pendingSignals := make([]gestalt.WorkflowSignal, 0)
	nextSignalSequence := int64(1)
	signalCount := 0
	runMutex := workflow.NewMutex(ctx)

	upsertVisibility := func(ctx workflow.Context) {
		_ = workflow.UpsertTypedSearchAttributes(ctx, workflowRunSearchAttributeUpdates(input.ScopeID, state)...)
	}
	rebuildRun := func(mutate func(*gestalt.WorkflowRun)) error {
		next := *state
		mutate(&next)
		state = &next
		return nil
	}
	if err := workflow.SetQueryHandler(ctx, queryGetRun, func() (*gestalt.WorkflowRun, error) {
		return cloneRunInput(state), nil
	}); err != nil {
		return nil, err
	}
	appendSignalInput := func(signalInput gestalt.WorkflowSignal) (*gestalt.WorkflowSignal, error) {
		if signalInput.CreatedAt.IsZero() {
			signalInput.CreatedAt = workflow.Now(ctx).UTC()
		}
		if signalInput.Sequence <= 0 {
			signalInput.Sequence = nextSignalSequence
		}
		if signalInput.Sequence >= nextSignalSequence {
			nextSignalSequence = signalInput.Sequence + 1
		}
		if strings.TrimSpace(signalInput.ID) == "" {
			signalInput.ID = "signal:" + hashID(state.ID, signalInput.Name, fmt.Sprintf("%d", signalInput.Sequence), signalInput.IdempotencyKey)
		}
		pendingSignals = append(pendingSignals, signalInput)
		signalCount++
		return &signalInput, nil
	}
	if initial := input.InitialSignal; initial != nil {
		if _, err := appendSignalInput(*initial); err != nil {
			return nil, err
		}
	}
	if err := workflow.SetUpdateHandler(ctx, updateAddSignal, func(ctx workflow.Context, signal gestalt.WorkflowSignal) (*gestalt.SignalWorkflowRunResponse, error) {
		if err := runMutex.Lock(ctx); err != nil {
			return nil, err
		}
		defer runMutex.Unlock()
		if workflowRunTerminal(state.Status) {
			return nil, fmt.Errorf("failed_precondition: workflow run %q is %s", state.ID, workflowRunStatusName(state.Status))
		}
		appended, err := appendSignalInput(signal)
		if err != nil {
			return nil, err
		}
		upsertVisibility(ctx)
		return &gestalt.SignalWorkflowRunResponse{
			Run:         cloneRunInput(state),
			Signal:      cloneSignalInput(appended),
			StartedRun:  signalCount == 1 && state.StartedAt == nil,
			WorkflowKey: strings.TrimSpace(state.WorkflowKey),
		}, nil
	}); err != nil {
		return nil, err
	}
	if err := workflow.SetUpdateHandler(ctx, updateCancelRun, func(ctx workflow.Context, reason string) (*gestalt.WorkflowRun, error) {
		if err := runMutex.Lock(ctx); err != nil {
			return nil, err
		}
		defer runMutex.Unlock()
		if state.Status != gestalt.WorkflowRunStatusValuePending {
			return nil, fmt.Errorf("failed_precondition: workflow run %q is %s; only pending runs can be canceled", state.ID, workflowRunStatusName(state.Status))
		}
		completedAt := workflow.Now(ctx).UTC()
		statusMessage := strings.TrimSpace(reason)
		if statusMessage == "" {
			statusMessage = "canceled"
		}
		if err := rebuildRun(func(input *gestalt.WorkflowRun) {
			input.Status = gestalt.WorkflowRunStatusValueCanceled
			input.CompletedAt = &completedAt
			input.StatusMessage = statusMessage
		}); err != nil {
			return nil, err
		}
		upsertVisibility(ctx)
		return cloneRunInput(state), nil
	}); err != nil {
		return nil, err
	}

	upsertVisibility(ctx)
	if input.RequireSignal {
		_ = workflow.Await(ctx, func() bool {
			return len(pendingSignals) > 0 || workflowRunTerminal(state.Status)
		})
	}
	for !workflowRunTerminal(state.Status) {
		if len(pendingSignals) == 0 && input.RequireSignal {
			_ = workflow.Await(ctx, func() bool {
				return len(pendingSignals) > 0 || workflowRunTerminal(state.Status)
			})
			if workflowRunTerminal(state.Status) {
				break
			}
		}
		if err := runMutex.Lock(ctx); err != nil {
			return nil, err
		}
		startedAt := workflow.Now(ctx).UTC()
		if err := rebuildRun(func(input *gestalt.WorkflowRun) {
			input.Status = gestalt.WorkflowRunStatusValueRunning
			input.StartedAt = &startedAt
			input.CompletedAt = nil
			input.StatusMessage = ""
		}); err != nil {
			runMutex.Unlock()
			return nil, err
		}
		upsertVisibility(ctx)
		batch := pendingSignals
		pendingSignals = nil
		runMutex.Unlock()

		activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: input.ActivityStartToCloseTimeoutNS,
			RetryPolicy:         &sdktemporal.RetryPolicy{MaximumAttempts: 1},
		})
		failed := false
		for stepIndex := 0; stepIndex < workflowTargetStepCountInput(state.Target); stepIndex++ {
			stepStartedAt := workflow.Now(ctx).UTC()
			invokeReq := gestaltworkflow.Request{
				ProviderName:         strings.TrimSpace(input.ProviderName),
				RunID:                state.ID,
				DefinitionID:         state.DefinitionID,
				DefinitionGeneration: state.DefinitionGeneration,
				WorkflowKey:          state.WorkflowKey,
				Target:               state.Target,
				Trigger:              state.Trigger,
				Input:                cloneMapInput(state.Input),
				CreatedBy:            state.CreatedBy,
				RunAs:                cloneSubjectInput(state.RunAs),
				Signals:              batch,
			}
			stepReq := gestaltworkflow.StepRequest{
				Request:        invokeReq,
				StepIndex:      stepIndex,
				Outputs:        workflowStepOutputsFromExecutionsInput(state.Steps),
				StepInputs:     workflowStepInputsFromExecutionsInput(state.Steps),
				SkippedStepIDs: workflowSkippedStepIDsFromExecutionsInput(state.Steps),
			}
			var stepResp gestaltworkflow.StepResponse
			invokeErr := workflow.ExecuteActivity(activityCtx, (*workflowActivities).ExecuteStep, stepReq).Get(activityCtx, &stepResp)

			if err := runMutex.Lock(ctx); err != nil {
				return nil, err
			}
			completedAt := workflow.Now(ctx).UTC()
			nextRun := *state
			if invokeErr != nil {
				nextRun.Status = gestalt.WorkflowRunStatusValueFailed
				nextRun.CompletedAt = &completedAt
				nextRun.StatusMessage = invokeErr.Error()
				state = &nextRun
				failed = true
				upsertVisibility(ctx)
				runMutex.Unlock()
				break
			}
			execution := workflowStepExecutionFromStepResponseInput(stepResp, stepStartedAt, completedAt)
			nextRun.Steps = append(append([]gestalt.WorkflowStepExecution(nil), nextRun.Steps...), execution)
			nextRun.CurrentStepID = execution.StepID
			if stepResp.Output != nil || execution.Status == gestalt.WorkflowStepStatusValueSucceeded {
				nextRun.Output = cloneAnyInput(stepResp.Output)
			}
			if stepResp.Status >= 400 || execution.Status == gestalt.WorkflowStepStatusValueFailed {
				nextRun.Status = gestalt.WorkflowRunStatusValueFailed
				nextRun.CompletedAt = &completedAt
				nextRun.StatusMessage = workflowStepFailureMessageInput(&stepResp, execution.StatusMessage)
				if nextRun.StatusMessage == "" {
					nextRun.StatusMessage = fmt.Sprintf("workflow operation returned status %d", stepResp.Status)
				}
				state = &nextRun
				failed = true
				upsertVisibility(ctx)
				runMutex.Unlock()
				break
			}
			nextRun.Status = gestalt.WorkflowRunStatusValueRunning
			nextRun.CompletedAt = nil
			nextRun.StatusMessage = ""
			state = &nextRun
			upsertVisibility(ctx)
			runMutex.Unlock()
		}
		if failed {
			break
		}
		if err := runMutex.Lock(ctx); err != nil {
			return nil, err
		}
		completedAt := workflow.Now(ctx).UTC()
		nextRun := *state
		if len(pendingSignals) > 0 {
			nextRun.Status = gestalt.WorkflowRunStatusValuePending
			nextRun.CompletedAt = nil
			nextRun.StatusMessage = ""
			nextRun.CurrentStepID = ""
			nextRun.Steps = nil
			nextRun.Output = nil
			state = &nextRun
			upsertVisibility(ctx)
			runMutex.Unlock()
			continue
		}
		nextRun.Status = gestalt.WorkflowRunStatusValueSucceeded
		nextRun.CompletedAt = &completedAt
		nextRun.StatusMessage = ""
		state = &nextRun
		upsertVisibility(ctx)
		runMutex.Unlock()
		break
	}
	upsertVisibility(ctx)
	_ = workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) })
	if workflowRunTerminal(state.Status) {
		// Emit from an activity, not workflow code: workflow code replays and
		// would double-count. Best effort; telemetry must never fail the run.
		recordCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: runCompletionRecordTimeout,
			RetryPolicy:         &sdktemporal.RetryPolicy{MaximumAttempts: runCompletionRecordAttempts},
		})
		_ = workflow.ExecuteActivity(recordCtx, (*workflowActivities).RecordRunCompleted, cloneRunInput(state)).Get(recordCtx, nil)
	}
	return cloneRunInput(state), nil
}

const (
	runCompletionRecordTimeout  = 30 * time.Second
	runCompletionRecordAttempts = 3
)

func (input runWorkflowInput) triggerInput(now time.Time) *gestalt.WorkflowRunTrigger {
	if input.ActivationID != "" {
		return scheduleTriggerInput(input.ActivationID, now)
	}
	if input.Trigger != nil {
		return input.Trigger
	}
	return nil
}
