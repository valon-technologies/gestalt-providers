package temporal

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
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
	// Legacy payload fields are retained for replaying histories started before
	// workflow inputs moved to native JSON values.
	TargetPayload        []byte `json:"target_payload,omitempty"`
	TriggerPayload       []byte `json:"trigger_payload,omitempty"`
	CreatedByPayload     []byte `json:"created_by_payload,omitempty"`
	InitialSignalPayload []byte `json:"initial_signal_payload,omitempty"`
	RequireSignal        bool   `json:"require_signal,omitempty"`
	RequireClaim         bool   `json:"require_claim,omitempty"`
}

const (
	changeV4AddSignalProjectionAfterUpdate = "v4-add-signal-projection-after-update"
	changeV4ClaimProjectionAfterUpdate     = "v4-claim-projection-after-update"
)

func gestaltRunWorkflowV4(ctx workflow.Context, input runWorkflowV4Input) (*proto.BoundWorkflowRun, error) {
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
	state, err := gestalt.NewBoundWorkflowRun(gestalt.BoundWorkflowRunInput{
		ID:           publicID,
		Status:       proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:       input.targetInput(),
		Trigger:      input.triggerInput(now),
		CreatedAt:    now,
		CreatedBy:    input.createdByInput(),
		ExecutionRef: strings.TrimSpace(input.ExecutionRef),
		WorkflowKey:  strings.TrimSpace(input.WorkflowKey),
	})
	if err != nil {
		return nil, err
	}
	pendingSignals := make([]*proto.WorkflowSignal, 0)
	nextSignalSequence := int64(1)
	signalCount := 0
	runMutex := workflow.NewMutex(ctx)
	claimed := !input.RequireClaim

	project := func(ctx workflow.Context) {
		activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: input.ActivityStartToCloseTimeoutNS,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		runInput, err := gestalt.BoundWorkflowRunInputFromRun(state)
		if err != nil {
			return
		}
		_ = workflow.ExecuteActivity(activityCtx, (*workflowActivities).ProjectRun, runInput).Get(activityCtx, nil)
	}
	rebuildRun := func(mutate func(*gestalt.BoundWorkflowRunInput)) error {
		input, err := gestalt.BoundWorkflowRunInputFromRun(state)
		if err != nil {
			return err
		}
		mutate(&input)
		state, err = gestalt.NewBoundWorkflowRun(input)
		if err != nil {
			return err
		}
		return nil
	}
	appendSignalInput := func(signalInput gestalt.WorkflowSignalInput) (*proto.WorkflowSignal, error) {
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
			signalInput.ID = "signal:" + hashID(state.GetId(), signalInput.Name, fmt.Sprintf("%d", signalInput.Sequence), signalInput.IdempotencyKey)
		}
		signal, err := gestalt.NewWorkflowSignal(signalInput)
		if err != nil {
			return nil, err
		}
		pendingSignals = append(pendingSignals, signal)
		signalCount++
		return signal, nil
	}
	appendSignal := func(signal *proto.WorkflowSignal) (*proto.WorkflowSignal, error) {
		return appendSignalInput(gestalt.WorkflowSignalInputFromSignal(signal))
	}
	if initial := input.initialSignalInput(); initial != nil {
		if _, err := appendSignalInput(*initial); err != nil {
			return nil, err
		}
	}
	if err := workflow.SetUpdateHandler(ctx, updateAddSignal, func(ctx workflow.Context, signal *proto.WorkflowSignal) (*proto.SignalWorkflowRunResponse, error) {
		if err := runMutex.Lock(ctx); err != nil {
			return nil, err
		}
		defer runMutex.Unlock()
		if workflowRunTerminal(state.GetStatus()) {
			return nil, fmt.Errorf("failed_precondition: workflow run %q is %s", state.GetId(), state.GetStatus().String())
		}
		signal, err := appendSignal(signal)
		if err != nil {
			return nil, err
		}
		if workflow.GetVersion(ctx, changeV4AddSignalProjectionAfterUpdate, workflow.DefaultVersion, 1) == workflow.DefaultVersion {
			project(ctx)
		}
		return &proto.SignalWorkflowRunResponse{
			Run:         cloneRun(state),
			Signal:      cloneSignal(signal),
			StartedRun:  signalCount == 1 && state.GetStartedAt() == nil,
			WorkflowKey: strings.TrimSpace(state.GetWorkflowKey()),
		}, nil
	}); err != nil {
		return nil, err
	}
	if err := workflow.SetUpdateHandler(ctx, updateClaimRun, func(ctx workflow.Context, _ *proto.BoundWorkflowRun) (*proto.BoundWorkflowRun, error) {
		if err := runMutex.Lock(ctx); err != nil {
			return nil, err
		}
		defer runMutex.Unlock()
		claimed = true
		if workflow.GetVersion(ctx, changeV4ClaimProjectionAfterUpdate, workflow.DefaultVersion, 1) == workflow.DefaultVersion {
			project(ctx)
		}
		return cloneRun(state), nil
	}); err != nil {
		return nil, err
	}
	if err := workflow.SetUpdateHandler(ctx, updateCancelRun, func(ctx workflow.Context, reason string) (*proto.BoundWorkflowRun, error) {
		if err := runMutex.Lock(ctx); err != nil {
			return nil, err
		}
		defer runMutex.Unlock()
		if state.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING {
			return nil, fmt.Errorf("failed_precondition: workflow run %q is %s; only pending runs can be canceled", state.GetId(), state.GetStatus().String())
		}
		completedAt := workflow.Now(ctx).UTC()
		statusMessage := strings.TrimSpace(reason)
		if statusMessage == "" {
			statusMessage = "canceled"
		}
		if err := rebuildRun(func(input *gestalt.BoundWorkflowRunInput) {
			input.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_CANCELED
			input.CompletedAt = &completedAt
			input.StatusMessage = statusMessage
		}); err != nil {
			return nil, err
		}
		project(ctx)
		return cloneRun(state), nil
	}); err != nil {
		return nil, err
	}

	if input.RequireClaim {
		_ = workflow.Await(ctx, func() bool {
			return claimed || workflowRunTerminal(state.GetStatus())
		})
	}
	project(ctx)
	if input.RequireSignal {
		_ = workflow.Await(ctx, func() bool {
			return len(pendingSignals) > 0 || workflowRunTerminal(state.GetStatus())
		})
	}
	for !workflowRunTerminal(state.GetStatus()) {
		if len(pendingSignals) == 0 && input.RequireSignal {
			_ = workflow.Await(ctx, func() bool {
				return len(pendingSignals) > 0 || workflowRunTerminal(state.GetStatus())
			})
			if workflowRunTerminal(state.GetStatus()) {
				break
			}
		}
		if err := runMutex.Lock(ctx); err != nil {
			return nil, err
		}
		startedAt := workflow.Now(ctx).UTC()
		if err := rebuildRun(func(input *gestalt.BoundWorkflowRunInput) {
			input.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING
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
			Target:       workflowTargetInput(state.GetTarget()),
			RunID:        state.GetId(),
			Trigger:      workflowTriggerInput(state.GetTrigger()),
			Metadata:     workflowInvokeMetadataInput(state.GetWorkflowKey()),
			CreatedBy:    actorInputPtr(state.GetCreatedBy()),
			ExecutionRef: strings.TrimSpace(state.GetExecutionRef()),
			Signals:      workflowSignalInputs(batch),
		}
		var resp gestalt.InvokeWorkflowOperationResponse
		invokeErr := workflow.ExecuteActivity(activityCtx, (*workflowActivities).InvokeOperation, invokeReq).Get(activityCtx, &resp)

		if err := runMutex.Lock(ctx); err != nil {
			return nil, err
		}
		completedAt := workflow.Now(ctx).UTC()
		runInput, err := gestalt.BoundWorkflowRunInputFromRun(state)
		if err != nil {
			runMutex.Unlock()
			return nil, err
		}
		runInput.CompletedAt = &completedAt
		if invokeErr != nil {
			runInput.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED
			runInput.StatusMessage = invokeErr.Error()
		} else if resp.GetStatus() >= http.StatusBadRequest {
			runInput.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED
			runInput.StatusMessage = fmt.Sprintf("workflow operation returned status %d", resp.GetStatus())
			runInput.ResultBody = resp.GetBody()
		} else {
			runInput.ResultBody = resp.GetBody()
			if len(pendingSignals) > 0 {
				runInput.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING
				runInput.CompletedAt = nil
				state, err = gestalt.NewBoundWorkflowRun(runInput)
				if err != nil {
					runMutex.Unlock()
					return nil, err
				}
				project(ctx)
				runMutex.Unlock()
				continue
			}
			runInput.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED
			runInput.StatusMessage = ""
		}
		state, err = gestalt.NewBoundWorkflowRun(runInput)
		if err != nil {
			runMutex.Unlock()
			return nil, err
		}
		runMutex.Unlock()
		break
	}
	project(ctx)
	_ = workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) })
	return cloneRun(state), nil
}

func (input runWorkflowV4Input) targetInput() *gestalt.BoundWorkflowTargetInput {
	if input.Target != nil {
		return input.Target
	}
	return workflowTargetInput(targetFromPayload(input.TargetPayload))
}

func (input runWorkflowV4Input) triggerInput(now time.Time) *gestalt.WorkflowRunTriggerInput {
	if input.ScheduleID != "" {
		return scheduleTriggerInput(input.ScheduleID, now)
	}
	if input.Trigger != nil {
		return input.Trigger
	}
	return workflowTriggerInput(triggerFromPayload(input.TriggerPayload))
}

func (input runWorkflowV4Input) createdByInput() *gestalt.WorkflowActorInput {
	if input.CreatedBy != nil {
		return input.CreatedBy
	}
	return actorInputPtr(actorFromPayload(input.CreatedByPayload))
}

func (input runWorkflowV4Input) initialSignalInput() *gestalt.WorkflowSignalInput {
	if input.InitialSignal != nil {
		return input.InitialSignal
	}
	if signal := signalFromPayload(input.InitialSignalPayload); signal != nil {
		native := gestalt.WorkflowSignalInputFromSignal(signal)
		return &native
	}
	return nil
}
