package temporal

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (b *temporalBackend) startKeyedRunV4(ctx context.Context, target scopedTarget, req *gestalt.StartWorkflowProviderRunRequest, key, fingerprint string) (*gestalt.BoundWorkflowRunInput, error) {
	now := time.Now().UTC()
	key = strings.TrimSpace(key)
	workflowKey := strings.TrimSpace(req.WorkflowKey)
	if workflowKey == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow_key is required")
	}
	temporalWorkflowID := workflowID(b.cfg.ScopeID, "run-v4-key", target.OwnerKey, hashID(workflowKey), uuid.NewString())
	conflictPolicy := enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL
	if key != "" {
		temporalWorkflowID = workflowID(b.cfg.ScopeID, "manual-keyed-v4", target.OwnerKey, key, hashID(workflowKey))
		conflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
	}
	if key != "" {
		entry, existing, err := b.state.reserveRunIdempotency(ctx, target.OwnerKey, key, fingerprint, b.cfg.IdempotencyRetention, now)
		if err != nil {
			var conflict *runIdempotencyConflictError
			if errors.As(err, &conflict) {
				return nil, status.Error(codes.FailedPrecondition, err.Error())
			}
			if status.Code(err) == codes.Aborted {
				return nil, status.Errorf(codes.Aborted, "reserve workflow run idempotency: %v", err)
			}
			return nil, status.Errorf(codes.Internal, "reserve workflow run idempotency: %v", err)
		}
		if existing && entry != nil && entry.Status == "completed" {
			if run := runInputFromPayload(entry.RunPayload); run != nil {
				return run, nil
			}
		}
	}
	if existing, found, err := b.state.getWorkflowKeyRun(ctx, workflowKey); err != nil {
		return nil, mapWorkflowKeyLoadError(err)
	} else if found && !workflowRunTerminal(existing.Status) {
		if key != "" && runHasTemporalWorkflowID(existing, temporalWorkflowID) {
			if err := b.releaseClaimedRunV4(ctx, existing); err != nil {
				return nil, err
			}
			if err := b.state.completeRunIdempotency(ctx, target.OwnerKey, key, fingerprint, existing, b.cfg.IdempotencyRetention, time.Now().UTC()); err != nil {
				return nil, status.Errorf(codes.Internal, "complete workflow run idempotency: %v", err)
			}
			return existing, nil
		}
		return nil, status.Errorf(codes.FailedPrecondition, "workflow key %q already has an active run", workflowKey)
	}

	input := b.runV4Input(target.OwnerKey, req.ExecutionRef, workflowKey, target.Target, newManualTrigger(), workflowActorProto(req.CreatedBy), false)
	input.RequireClaim = true
	run, err := b.executeRunV4(ctx, temporalWorkflowID, input, conflictPolicy, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
	if err != nil {
		return nil, err
	}
	owner, claimed, err := b.state.claimWorkflowKeyRun(ctx, workflowKey, run, time.Now().UTC())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "claim workflow key: %v", err)
	}
	if !claimed {
		b.terminateUnclaimedRunV4(ctx, run)
		return nil, status.Errorf(codes.FailedPrecondition, "workflow key %q already has an active run %q", workflowKey, owner.ID)
	}
	if err := b.releaseClaimedRunV4(ctx, owner); err != nil {
		return nil, err
	}
	if key != "" {
		if err := b.state.completeRunIdempotency(ctx, target.OwnerKey, key, fingerprint, owner, b.cfg.IdempotencyRetention, time.Now().UTC()); err != nil {
			var conflict *runIdempotencyConflictError
			if errors.As(err, &conflict) {
				return nil, status.Error(codes.FailedPrecondition, err.Error())
			}
			return nil, status.Errorf(codes.Internal, "complete workflow run idempotency: %v", err)
		}
	}
	return owner, nil
}

func (b *temporalBackend) startUnkeyedRunV4(ctx context.Context, target scopedTarget, req *gestalt.StartWorkflowProviderRunRequest, key, fingerprint string) (*gestalt.BoundWorkflowRunInput, error) {
	now := time.Now().UTC()
	entry, existing, err := b.state.reserveRunIdempotency(ctx, target.OwnerKey, key, fingerprint, b.cfg.IdempotencyRetention, now)
	if err != nil {
		var conflict *runIdempotencyConflictError
		if errors.As(err, &conflict) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		if status.Code(err) == codes.Aborted {
			return nil, status.Errorf(codes.Aborted, "reserve workflow run idempotency: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "reserve workflow run idempotency: %v", err)
	}
	if existing && entry != nil && entry.Status == "completed" {
		if run := runInputFromPayload(entry.RunPayload); run != nil {
			return run, nil
		}
	}
	temporalWorkflowID := workflowID(b.cfg.ScopeID, "manual-v4", target.OwnerKey, key)
	run, err := b.executeRunV4(ctx, temporalWorkflowID, b.runV4Input(target.OwnerKey, req.ExecutionRef, "", target.Target, newManualTrigger(), workflowActorProto(req.CreatedBy), false), enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
	if err != nil {
		return nil, err
	}
	if err := b.state.completeRunIdempotency(ctx, target.OwnerKey, key, fingerprint, run, b.cfg.IdempotencyRetention, time.Now().UTC()); err != nil {
		var conflict *runIdempotencyConflictError
		if errors.As(err, &conflict) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "complete workflow run idempotency: %v", err)
	}
	return run, nil
}

func (b *temporalBackend) signalOrStartRunV4(ctx context.Context, target scopedTarget, req *gestalt.SignalOrStartWorkflowProviderRunRequest, workflowKey string, signal *proto.WorkflowSignal, updateID string) (*proto.SignalWorkflowRunResponse, error) {
	workflowKey = strings.TrimSpace(workflowKey)
	if workflowKey == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow_key is required")
	}
	if existing, found, err := b.state.getWorkflowKeyRun(ctx, workflowKey); err != nil {
		return nil, mapWorkflowKeyLoadError(err)
	} else if found && !workflowRunTerminal(existing.Status) {
		resp, err := b.signalExistingWorkflowKeyRunV4(ctx, workflowKey, existing, signal, updateID)
		if err == nil {
			resp.StartedRun = false
			return resp, nil
		}
		if status.Code(err) == codes.FailedPrecondition {
			if releaseErr := b.releaseClaimedRunV4(ctx, existing); releaseErr == nil {
				if resp, retryErr := b.signalExistingWorkflowKeyRunV4(ctx, workflowKey, existing, signal, updateID); retryErr == nil {
					resp.StartedRun = false
					return resp, nil
				}
			}
		}
		if status.Code(err) != codes.NotFound && status.Code(err) != codes.FailedPrecondition {
			return nil, err
		}
		if _, clearErr := b.state.clearWorkflowKeyRun(ctx, workflowKey, existing.ID); clearErr != nil {
			return nil, status.Errorf(codes.Internal, "clear stale workflow key: %v", clearErr)
		}
	} else if found {
		if _, err := b.state.clearWorkflowKeyRun(ctx, workflowKey, existing.ID); err != nil {
			return nil, status.Errorf(codes.Internal, "clear terminal workflow key: %v", err)
		}
	}
	return b.startSignalWorkflowRunV4(ctx, target, req, workflowKey, signal, updateID)
}

func (b *temporalBackend) signalExistingWorkflowKeyRunV4(ctx context.Context, workflowKey string, run *gestalt.BoundWorkflowRunInput, signal *proto.WorkflowSignal, updateID string) (*proto.SignalWorkflowRunResponse, error) {
	handle, err := decodeTemporalRunHandle(run.ID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if strings.TrimSpace(handle.RunTemporalRunID) == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is missing run_temporal_run_id")
	}
	update, err := b.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   handle.RunWorkflowID,
		RunID:        handle.RunTemporalRunID,
		UpdateID:     updateID,
		UpdateName:   updateAddSignal,
		Args:         []any{signal},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return nil, mapTemporalWorkflowCallError("signal temporal workflow", err)
	}
	var out proto.SignalWorkflowRunResponse
	if err := update.Get(ctx, &out); err != nil {
		return nil, mapTemporalWorkflowCallError("temporal workflow update", err)
	}
	if out.GetRun() == nil {
		runProto, err := gestalt.NewBoundWorkflowRun(*run)
		if err != nil {
			return nil, err
		}
		out.Run = runProto
	}
	if out.GetSignal() == nil {
		out.Signal = cloneSignal(signal)
	}
	if strings.TrimSpace(out.GetWorkflowKey()) == "" {
		out.WorkflowKey = strings.TrimSpace(workflowKey)
	}
	return cloneSignalResponse(&out), nil
}

func (b *temporalBackend) releaseClaimedRunV4(ctx context.Context, run *gestalt.BoundWorkflowRunInput) error {
	handle, err := decodeTemporalRunHandle(run.ID)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	runProto, err := gestalt.NewBoundWorkflowRun(*run)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	update, err := b.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   handle.RunWorkflowID,
		RunID:        handle.RunTemporalRunID,
		UpdateID:     "claim:" + hashID(run.ID),
		UpdateName:   updateClaimRun,
		Args:         []any{runProto},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return mapTemporalWorkflowCallError("claim temporal workflow", err)
	}
	if err := update.Get(ctx, nil); err != nil {
		return mapTemporalWorkflowCallError("temporal workflow update", err)
	}
	return nil
}

func (b *temporalBackend) terminateUnclaimedRunV4(ctx context.Context, run *gestalt.BoundWorkflowRunInput) {
	handle, err := decodeTemporalRunHandle(run.ID)
	if err != nil {
		return
	}
	_ = b.client.TerminateWorkflow(ctx, handle.RunWorkflowID, handle.RunTemporalRunID, "workflow key claim lost")
}

func (b *temporalBackend) startSignalWorkflowRunV4(ctx context.Context, target scopedTarget, req *gestalt.SignalOrStartWorkflowProviderRunRequest, workflowKey string, signal *proto.WorkflowSignal, updateID string) (*proto.SignalWorkflowRunResponse, error) {
	temporalWorkflowID := workflowID(b.cfg.ScopeID, "signal-keyed-v4", target.OwnerKey, hashID(workflowKey), uuid.NewString())
	conflictPolicy := enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL
	if signalKey := strings.TrimSpace(signal.GetIdempotencyKey()); signalKey != "" {
		temporalWorkflowID = workflowID(b.cfg.ScopeID, "signal-keyed-v4", target.OwnerKey, signalKey, hashID(workflowKey))
		conflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
	} else if signalIDKey := explicitSignalLedgerKey(signal); signalIDKey != "" {
		temporalWorkflowID = workflowID(b.cfg.ScopeID, "signal-keyed-v4", target.OwnerKey, signalIDKey, hashID(workflowKey))
		conflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
	}
	input := b.runV4Input(target.OwnerKey, req.ExecutionRef, workflowKey, target.Target, newManualTrigger(), workflowActorProto(req.CreatedBy), true)
	input.RequireClaim = true
	startOperation := b.client.NewWithStartWorkflowOperation(
		b.startWorkflowOptions(temporalWorkflowID, conflictPolicy, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE),
		gestaltRunWorkflowV4,
		input,
	)
	update, err := b.client.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: startOperation,
		UpdateOptions: client.UpdateWorkflowOptions{
			WorkflowID:   temporalWorkflowID,
			UpdateID:     updateID,
			UpdateName:   updateAddSignal,
			Args:         []any{signal},
			WaitForStage: client.WorkflowUpdateStageCompleted,
		},
	})
	if err != nil {
		return nil, mapTemporalWorkflowCallError("signal-or-start temporal workflow", err)
	}
	var out proto.SignalWorkflowRunResponse
	if err := update.Get(ctx, &out); err != nil {
		return nil, mapTemporalWorkflowCallError("temporal workflow update", err)
	}
	startedRun, err := startOperation.Get(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "resolve signal-or-start temporal workflow: %v", err)
	}
	run, err := runInputFromProto(out.GetRun(), nil)
	if err != nil {
		return nil, err
	}
	if run == nil {
		run = b.pendingRunFromWorkflowRun(startedRun, input)
	}
	owner, claimed, err := b.state.claimWorkflowKeyRun(ctx, workflowKey, run, time.Now().UTC())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "claim workflow key: %v", err)
	}
	if !claimed {
		b.terminateUnclaimedRunV4(ctx, run)
		_ = b.releaseClaimedRunV4(ctx, owner)
		resp, err := b.signalExistingWorkflowKeyRunV4(ctx, workflowKey, owner, signal, updateID)
		if err != nil {
			return nil, err
		}
		resp.StartedRun = false
		return resp, nil
	}
	if err := b.releaseClaimedRunV4(ctx, owner); err != nil {
		return nil, err
	}
	ownerProto, err := gestalt.NewBoundWorkflowRun(*owner)
	if err != nil {
		return nil, err
	}
	out.Run = ownerProto
	if out.GetSignal() == nil {
		out.Signal = signalForStartedRun(ownerProto, signal)
	}
	out.StartedRun = true
	out.WorkflowKey = strings.TrimSpace(workflowKey)
	return cloneSignalResponse(&out), nil
}

func runHasTemporalWorkflowID(run *gestalt.BoundWorkflowRunInput, workflowID string) bool {
	handle, err := decodeTemporalRunHandle(run.ID)
	if err != nil {
		return false
	}
	return handle.RunWorkflowID == strings.TrimSpace(workflowID)
}

func mapTemporalWorkflowCallError(prefix string, err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	switch {
	case isNotFoundLike(err):
		return status.Error(codes.NotFound, msg)
	case strings.Contains(msg, "failed_precondition"):
		return status.Error(codes.FailedPrecondition, msg)
	case strings.Contains(msg, "invalid_argument"):
		return status.Error(codes.InvalidArgument, msg)
	default:
		return status.Errorf(codes.Internal, "%s: %v", prefix, err)
	}
}

func mapWorkflowKeyLoadError(err error) error {
	if err == nil {
		return nil
	}
	if code := status.Code(err); code != codes.Unknown {
		return err
	}
	return status.Errorf(codes.Internal, "load workflow key: %v", err)
}

func (b *temporalBackend) runV4Input(ownerKey, executionRef, workflowKey string, target *proto.BoundWorkflowTarget, trigger *proto.WorkflowRunTrigger, createdBy *proto.WorkflowActor, requireSignal bool) runWorkflowV4Input {
	return runWorkflowV4Input{
		ActivityStartToCloseTimeoutNS: b.cfg.ActivityStartToCloseTimeout,
		ExecutionRef:                  strings.TrimSpace(executionRef),
		WorkflowKey:                   strings.TrimSpace(workflowKey),
		OwnerKey:                      strings.TrimSpace(ownerKey),
		TargetPayload:                 protoPayload(target),
		TriggerPayload:                protoPayload(trigger),
		CreatedByPayload:              protoPayload(createdBy),
		RequireSignal:                 requireSignal,
	}
}

func (b *temporalBackend) executeRunV4(ctx context.Context, workflowID string, input runWorkflowV4Input, conflict enumspb.WorkflowIdConflictPolicy, reuse enumspb.WorkflowIdReusePolicy) (*gestalt.BoundWorkflowRunInput, error) {
	run, err := b.client.ExecuteWorkflow(ctx, b.startWorkflowOptions(workflowID, conflict, reuse), gestaltRunWorkflowV4, input)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "start temporal v4 workflow: %v", err)
	}
	out := b.pendingRunFromWorkflowRun(run, input)
	if !input.RequireClaim {
		_ = b.state.putRun(ctx, out)
	}
	return out, nil
}

func (b *temporalBackend) pendingRunFromWorkflowRun(run client.WorkflowRun, input runWorkflowV4Input) *gestalt.BoundWorkflowRunInput {
	now := time.Now().UTC()
	if input.ScheduleID != "" {
		input.TriggerPayload = protoPayload(scheduleTrigger(input.ScheduleID, now))
	}
	publicID := encodeTemporalRunHandle(temporalRunHandle{
		RunWorkflowID:    run.GetID(),
		RunTemporalRunID: run.GetRunID(),
		WorkflowKey:      input.WorkflowKey,
		OwnerKey:         input.OwnerKey,
	})
	out := &gestalt.BoundWorkflowRunInput{
		ID:           publicID,
		Status:       gestalt.WorkflowRunStatusValuePending,
		Target:       workflowTargetInput(targetFromPayload(input.TargetPayload)),
		Trigger:      workflowTriggerInput(triggerFromPayload(input.TriggerPayload)),
		CreatedAt:    now,
		CreatedBy:    actorInputPtr(actorFromPayload(input.CreatedByPayload)),
		ExecutionRef: strings.TrimSpace(input.ExecutionRef),
		WorkflowKey:  strings.TrimSpace(input.WorkflowKey),
	}
	if _, err := gestalt.NewBoundWorkflowRun(*out); err != nil {
		panic("build pending workflow run: " + err.Error())
	}
	return out
}

func (b *temporalBackend) startWorkflowOptions(workflowID string, conflict enumspb.WorkflowIdConflictPolicy, reuse enumspb.WorkflowIdReusePolicy) client.StartWorkflowOptions {
	opts := b.runStartOptions(workflowID, conflict, reuse)
	opts.WorkflowIDReusePolicy = reuse
	return opts
}
