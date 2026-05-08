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
	ProviderName                  string        `json:"provider_name"`
	ScopeID                       string        `json:"scope_id"`
	TaskQueue                     string        `json:"task_queue"`
	WorkflowRunTimeoutNS          time.Duration `json:"workflow_run_timeout_ns"`
	WorkflowTaskTimeoutNS         time.Duration `json:"workflow_task_timeout_ns"`
	ActivityStartToCloseTimeoutNS time.Duration `json:"activity_start_to_close_timeout_ns"`
	ScheduleID                    string        `json:"schedule_id,omitempty"`
	ExecutionRef                  string        `json:"execution_ref,omitempty"`
	WorkflowKey                   string        `json:"workflow_key,omitempty"`
	OwnerKey                      string        `json:"owner_key,omitempty"`
	TargetPayload                 []byte        `json:"target_payload,omitempty"`
	TriggerPayload                []byte        `json:"trigger_payload,omitempty"`
	CreatedByPayload              []byte        `json:"created_by_payload,omitempty"`
	InitialSignalPayload          []byte        `json:"initial_signal_payload,omitempty"`
	RequireSignal                 bool          `json:"require_signal,omitempty"`
	RequireClaim                  bool          `json:"require_claim,omitempty"`
}

func gestaltRunWorkflowV4(ctx workflow.Context, input runWorkflowV4Input) (*proto.BoundWorkflowRun, error) {
	info := workflow.GetInfo(ctx)
	now := workflow.Now(ctx).UTC()
	if input.ScheduleID != "" {
		input.TriggerPayload = protoPayload(scheduleTrigger(input.ScheduleID, now))
	}
	publicID := encodeV3RunHandle(v3RunHandle{
		Kind:             runHandleKindV3,
		RunWorkflowID:    info.WorkflowExecution.ID,
		RunTemporalRunID: info.WorkflowExecution.RunID,
		WorkflowKey:      input.WorkflowKey,
		OwnerKey:         input.OwnerKey,
	})
	state := gestalt.NewBoundWorkflowRun(gestalt.BoundWorkflowRunInput{
		ID:           publicID,
		Status:       proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:       targetFromPayload(input.TargetPayload),
		Trigger:      triggerFromPayload(input.TriggerPayload),
		CreatedAt:    now,
		CreatedBy:    actorFromPayload(input.CreatedByPayload),
		ExecutionRef: strings.TrimSpace(input.ExecutionRef),
		WorkflowKey:  strings.TrimSpace(input.WorkflowKey),
	})
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
		_ = workflow.ExecuteActivity(activityCtx, (*workflowActivities).ProjectRun, cloneRun(state)).Get(activityCtx, nil)
	}
	appendSignal := func(signal *proto.WorkflowSignal) *proto.WorkflowSignal {
		signal = cloneSignal(signal)
		if signal.GetCreatedAt() == nil {
			signal.CreatedAt = gestalt.TimestampFromTime(workflow.Now(ctx).UTC())
		}
		if signal.GetSequence() <= 0 {
			signal.Sequence = nextSignalSequence
		}
		if signal.GetSequence() >= nextSignalSequence {
			nextSignalSequence = signal.GetSequence() + 1
		}
		if strings.TrimSpace(signal.GetId()) == "" {
			signal.Id = "signal:" + hashID(state.GetId(), signal.GetName(), fmt.Sprintf("%d", signal.GetSequence()), signal.GetIdempotencyKey())
		}
		pendingSignals = append(pendingSignals, signal)
		signalCount++
		return signal
	}
	if initial := signalFromPayload(input.InitialSignalPayload); initial != nil {
		appendSignal(initial)
	}

	if err := workflow.SetQueryHandler(ctx, queryRunState, func() (*proto.BoundWorkflowRun, error) {
		return cloneRun(state), nil
	}); err != nil {
		return nil, err
	}
	if err := workflow.SetUpdateHandler(ctx, updateAddSignal, func(ctx workflow.Context, signal *proto.WorkflowSignal) (*proto.SignalWorkflowRunResponse, error) {
		if err := runMutex.Lock(ctx); err != nil {
			return nil, err
		}
		defer runMutex.Unlock()
		if !claimed {
			return nil, fmt.Errorf("failed_precondition: workflow run %q is not claimed", state.GetId())
		}
		if workflowRunTerminal(state.GetStatus()) {
			return nil, fmt.Errorf("failed_precondition: workflow run %q is %s", state.GetId(), state.GetStatus().String())
		}
		signal = appendSignal(signal)
		project(ctx)
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
		project(ctx)
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
		state.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_CANCELED
		state.CompletedAt = gestalt.TimestampFromTime(completedAt)
		state.StatusMessage = strings.TrimSpace(reason)
		if state.GetStatusMessage() == "" {
			state.StatusMessage = "canceled"
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
		state.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING
		state.StartedAt = gestalt.TimestampFromTime(startedAt)
		state.CompletedAt = nil
		state.StatusMessage = ""
		project(ctx)
		batch := pendingSignals
		pendingSignals = nil
		runMutex.Unlock()

		activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: input.ActivityStartToCloseTimeoutNS,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		invokeReq := &proto.InvokeWorkflowOperationRequest{
			Target:       cloneTarget(state.GetTarget()),
			RunId:        state.GetId(),
			Trigger:      cloneRunTrigger(state.GetTrigger()),
			Metadata:     workflowInvokeMetadata(state.GetWorkflowKey()),
			CreatedBy:    cloneActor(state.GetCreatedBy()),
			ExecutionRef: strings.TrimSpace(state.GetExecutionRef()),
			Signals:      cloneSignals(batch),
		}
		var resp proto.InvokeWorkflowOperationResponse
		invokeErr := workflow.ExecuteActivity(activityCtx, (*workflowActivities).InvokeOperation, invokeReq).Get(activityCtx, &resp)

		if err := runMutex.Lock(ctx); err != nil {
			return nil, err
		}
		completedAt := workflow.Now(ctx).UTC()
		state.CompletedAt = gestalt.TimestampFromTime(completedAt)
		if invokeErr != nil {
			state.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED
			state.StatusMessage = invokeErr.Error()
		} else if resp.GetStatus() >= http.StatusBadRequest {
			state.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED
			state.StatusMessage = fmt.Sprintf("workflow operation returned status %d", resp.GetStatus())
			state.ResultBody = resp.GetBody()
		} else {
			state.ResultBody = resp.GetBody()
			if len(pendingSignals) > 0 {
				state.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING
				state.CompletedAt = nil
				project(ctx)
				runMutex.Unlock()
				continue
			}
			state.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED
			state.StatusMessage = ""
		}
		runMutex.Unlock()
		break
	}
	project(ctx)
	_ = workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) })
	return cloneRun(state), nil
}
