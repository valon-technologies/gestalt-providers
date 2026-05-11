package temporal

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type runWorkflowV4Input struct {
	ActivityStartToCloseTimeoutNS time.Duration                     `json:"activity_start_to_close_timeout_ns"`
	ScheduleID                    string                            `json:"schedule_id,omitempty"`
	ExecutionRef                  string                            `json:"execution_ref,omitempty"`
	WorkflowKey                   string                            `json:"workflow_key,omitempty"`
	OwnerKey                      string                            `json:"owner_key,omitempty"`
	Target                        *gestalt.BoundWorkflowTargetInput `json:"target,omitempty"`
	Trigger                       *gestalt.WorkflowRunTriggerInput  `json:"trigger,omitempty"`
	CreatedBy                     *gestalt.WorkflowActorInput       `json:"created_by,omitempty"`
	InitialSignal                 *gestalt.WorkflowSignalInput      `json:"initial_signal,omitempty"`
	RequireSignal                 bool                              `json:"require_signal,omitempty"`
	RequireClaim                  bool                              `json:"require_claim,omitempty"`
}

const (
	changeV4AddSignalProjectionAfterUpdate = "v4-add-signal-projection-after-update"
	changeV4ClaimProjectionAfterUpdate     = "v4-claim-projection-after-update"
)

func gestaltRunWorkflowV4(ctx workflow.Context, input runWorkflowV4Input) (*gestalt.BoundWorkflowRunInput, error) {
	info := workflow.GetInfo(ctx)
	now := workflow.Now(ctx).UTC()
	handleKind := runHandleKindV4
	if workflow.GetVersion(ctx, "temporal-run-v4-handle-kind", workflow.DefaultVersion, 1) == workflow.DefaultVersion {
		// Alpha.19 V4 histories used the old handle kind. Preserve replay for
		// those histories; provider APIs still reject old handles.
		handleKind = "temporal-run-v3"
	}
	publicID := encodeTemporalRunHandle(temporalRunHandle{
		Kind:             handleKind,
		RunWorkflowID:    info.WorkflowExecution.ID,
		RunTemporalRunID: info.WorkflowExecution.RunID,
		WorkflowKey:      input.WorkflowKey,
		OwnerKey:         input.OwnerKey,
	})
	state := &gestalt.BoundWorkflowRunInput{
		ID:           publicID,
		Status:       gestalt.WorkflowRunStatusValuePending,
		Target:       input.targetInput(),
		Trigger:      input.triggerInput(now),
		CreatedAt:    now,
		CreatedBy:    input.createdByInput(),
		ExecutionRef: strings.TrimSpace(input.ExecutionRef),
		WorkflowKey:  strings.TrimSpace(input.WorkflowKey),
	}
	if _, err := gestalt.NewBoundWorkflowRun(*state); err != nil {
		return nil, err
	}
	pendingSignals := make([]gestalt.WorkflowSignalInput, 0)
	nextSignalSequence := int64(1)
	signalCount := 0
	runMutex := workflow.NewMutex(ctx)
	claimed := !input.RequireClaim

	project := func(ctx workflow.Context) {
		activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: input.ActivityStartToCloseTimeoutNS,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		_ = workflow.ExecuteActivity(activityCtx, (*workflowActivities).ProjectRun, *state).Get(activityCtx, nil)
	}
	rebuildRun := func(mutate func(*gestalt.BoundWorkflowRunInput)) error {
		next := *state
		mutate(&next)
		if _, err := gestalt.NewBoundWorkflowRun(next); err != nil {
			return err
		}
		state = &next
		return nil
	}
	appendSignalInput := func(signalInput gestalt.WorkflowSignalInput) (*gestalt.WorkflowSignalInput, error) {
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
		if _, err := gestalt.NewWorkflowSignal(signalInput); err != nil {
			return nil, err
		}
		pendingSignals = append(pendingSignals, signalInput)
		signalCount++
		return &signalInput, nil
	}
	if initial := input.initialSignalInput(); initial != nil {
		if _, err := appendSignalInput(*initial); err != nil {
			return nil, err
		}
	}
	if err := workflow.SetUpdateHandler(ctx, updateAddSignal, func(ctx workflow.Context, signal gestalt.WorkflowSignalInput) (*gestalt.SignalWorkflowRunResponse, error) {
		if err := runMutex.Lock(ctx); err != nil {
			return nil, err
		}
		defer runMutex.Unlock()
		if workflowRunTerminal(state.Status) {
			return nil, fmt.Errorf("failed_precondition: workflow run %q is %s", state.ID, state.Status.String())
		}
		appended, err := appendSignalInput(signal)
		if err != nil {
			return nil, err
		}
		if workflow.GetVersion(ctx, changeV4AddSignalProjectionAfterUpdate, workflow.DefaultVersion, 1) == workflow.DefaultVersion {
			project(ctx)
		}
		return &gestalt.SignalWorkflowRunResponse{
			Run:         cloneRunInput(state),
			Signal:      cloneSignalInput(appended),
			StartedRun:  signalCount == 1 && state.StartedAt == nil,
			WorkflowKey: strings.TrimSpace(state.WorkflowKey),
		}, nil
	}); err != nil {
		return nil, err
	}
	if err := workflow.SetUpdateHandler(ctx, updateClaimRun, func(ctx workflow.Context) (*gestalt.BoundWorkflowRunInput, error) {
		if err := runMutex.Lock(ctx); err != nil {
			return nil, err
		}
		defer runMutex.Unlock()
		claimed = true
		if workflow.GetVersion(ctx, changeV4ClaimProjectionAfterUpdate, workflow.DefaultVersion, 1) == workflow.DefaultVersion {
			project(ctx)
		}
		return cloneRunInput(state), nil
	}); err != nil {
		return nil, err
	}
	if err := workflow.SetUpdateHandler(ctx, updateCancelRun, func(ctx workflow.Context, reason string) (*gestalt.BoundWorkflowRunInput, error) {
		if err := runMutex.Lock(ctx); err != nil {
			return nil, err
		}
		defer runMutex.Unlock()
		if state.Status != gestalt.WorkflowRunStatusValuePending {
			return nil, fmt.Errorf("failed_precondition: workflow run %q is %s; only pending runs can be canceled", state.ID, state.Status.String())
		}
		completedAt := workflow.Now(ctx).UTC()
		statusMessage := strings.TrimSpace(reason)
		if statusMessage == "" {
			statusMessage = "canceled"
		}
		if err := rebuildRun(func(input *gestalt.BoundWorkflowRunInput) {
			input.Status = gestalt.WorkflowRunStatusValueCanceled
			input.CompletedAt = &completedAt
			input.StatusMessage = statusMessage
		}); err != nil {
			return nil, err
		}
		project(ctx)
		return cloneRunInput(state), nil
	}); err != nil {
		return nil, err
	}

	if input.RequireClaim {
		_ = workflow.Await(ctx, func() bool {
			return claimed || workflowRunTerminal(state.Status)
		})
	}
	project(ctx)
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
		if err := rebuildRun(func(input *gestalt.BoundWorkflowRunInput) {
			input.Status = gestalt.WorkflowRunStatusValueRunning
			input.StartedAt = &startedAt
			input.CompletedAt = nil
			input.StatusMessage = ""
		}); err != nil {
			runMutex.Unlock()
			return nil, err
		}
		project(ctx)
		batch := pendingSignals
		pendingSignals = nil
		runMutex.Unlock()

		activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: input.ActivityStartToCloseTimeoutNS,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		invokeReq := gestalt.InvokeWorkflowOperationInput{
			Target:       state.Target,
			RunID:        state.ID,
			Trigger:      state.Trigger,
			Metadata:     workflowInvokeMetadataInput(state.WorkflowKey),
			CreatedBy:    state.CreatedBy,
			ExecutionRef: strings.TrimSpace(state.ExecutionRef),
			Signals:      batch,
		}
		var resp gestalt.InvokeWorkflowOperationResponse
		invokeErr := workflow.ExecuteActivity(activityCtx, (*workflowActivities).InvokeOperation, invokeReq).Get(activityCtx, &resp)

		if err := runMutex.Lock(ctx); err != nil {
			return nil, err
		}
		completedAt := workflow.Now(ctx).UTC()
		runInput := *state
		runInput.CompletedAt = &completedAt
		if invokeErr != nil {
			runInput.Status = gestalt.WorkflowRunStatusValueFailed
			runInput.StatusMessage = invokeErr.Error()
		} else if resp.GetStatus() >= http.StatusBadRequest {
			runInput.Status = gestalt.WorkflowRunStatusValueFailed
			runInput.StatusMessage = fmt.Sprintf("workflow operation returned status %d", resp.GetStatus())
			runInput.ResultBody = resp.GetBody()
		} else {
			runInput.ResultBody = resp.GetBody()
			if len(pendingSignals) > 0 {
				runInput.Status = gestalt.WorkflowRunStatusValuePending
				runInput.CompletedAt = nil
				if _, err := gestalt.NewBoundWorkflowRun(runInput); err != nil {
					runMutex.Unlock()
					return nil, err
				}
				state = &runInput
				project(ctx)
				runMutex.Unlock()
				continue
			}
			runInput.Status = gestalt.WorkflowRunStatusValueSucceeded
			runInput.StatusMessage = ""
		}
		if _, err := gestalt.NewBoundWorkflowRun(runInput); err != nil {
			runMutex.Unlock()
			return nil, err
		}
		state = &runInput
		runMutex.Unlock()
		break
	}
	project(ctx)
	_ = workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) })
	return cloneRunInput(state), nil
}

func (input runWorkflowV4Input) targetInput() *gestalt.BoundWorkflowTargetInput {
	return input.Target
}

func (input runWorkflowV4Input) triggerInput(now time.Time) *gestalt.WorkflowRunTriggerInput {
	if input.ScheduleID != "" {
		return scheduleTriggerInput(input.ScheduleID, now)
	}
	if input.Trigger != nil {
		return input.Trigger
	}
	return nil
}

func (input runWorkflowV4Input) createdByInput() *gestalt.WorkflowActorInput {
	return input.CreatedBy
}

func (input runWorkflowV4Input) initialSignalInput() *gestalt.WorkflowSignalInput {
	return input.InitialSignal
}
