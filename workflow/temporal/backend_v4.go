package temporal

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (b *temporalBackend) startKeyedRunV4(ctx context.Context, target scopedTarget, req *proto.StartWorkflowProviderRunRequest, key, fingerprint string) (*proto.BoundWorkflowRun, error) {
	if b.state == nil {
		return nil, status.Error(codes.FailedPrecondition, "workflow state store is required for keyed temporal runs")
	}
	now := time.Now().UTC()
	key = strings.TrimSpace(key)
	workflowKey := strings.TrimSpace(req.GetWorkflowKey())
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
			if run := runFromPayload(entry.RunPayload); run != nil {
				return run, nil
			}
		}
	}
	if existing, found, err := b.state.getWorkflowKeyRun(ctx, workflowKey); err != nil {
		return nil, mapWorkflowKeyLoadError(err)
	} else if found && !workflowRunTerminal(existing.GetStatus()) {
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

	input := b.runV4Input(target.OwnerKey, req.GetExecutionRef(), workflowKey, target.Target, newManualTrigger(), req.GetCreatedBy(), false)
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
		return nil, status.Errorf(codes.FailedPrecondition, "workflow key %q already has an active run %q", workflowKey, owner.GetId())
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

func (b *temporalBackend) startUnkeyedRunV4(ctx context.Context, target scopedTarget, req *proto.StartWorkflowProviderRunRequest, key, fingerprint string) (*proto.BoundWorkflowRun, error) {
	now := time.Now().UTC()
	if b.state != nil {
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
			if run := runFromPayload(entry.RunPayload); run != nil {
				return run, nil
			}
		}
	}
	temporalWorkflowID := workflowID(b.cfg.ScopeID, "manual-v4", target.OwnerKey, key)
	run, err := b.executeRunV4(ctx, temporalWorkflowID, b.runV4Input(target.OwnerKey, req.GetExecutionRef(), "", target.Target, newManualTrigger(), req.GetCreatedBy(), false), enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
	if err != nil {
		return nil, err
	}
	if b.state != nil {
		if err := b.state.completeRunIdempotency(ctx, target.OwnerKey, key, fingerprint, run, b.cfg.IdempotencyRetention, time.Now().UTC()); err != nil {
			var conflict *runIdempotencyConflictError
			if errors.As(err, &conflict) {
				return nil, status.Error(codes.FailedPrecondition, err.Error())
			}
			return nil, status.Errorf(codes.Internal, "complete workflow run idempotency: %v", err)
		}
	}
	return run, nil
}

func (b *temporalBackend) signalOrStartRunV4(ctx context.Context, target scopedTarget, req *proto.SignalOrStartWorkflowProviderRunRequest, workflowKey string, signal *proto.WorkflowSignal, updateID string) (*proto.SignalWorkflowRunResponse, error) {
	if b.state == nil {
		return nil, status.Error(codes.FailedPrecondition, "workflow state store is required for keyed temporal runs")
	}
	workflowKey = strings.TrimSpace(workflowKey)
	if workflowKey == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow_key is required")
	}
	if existing, found, err := b.state.getWorkflowKeyRun(ctx, workflowKey); err != nil {
		return nil, mapWorkflowKeyLoadError(err)
	} else if found && !workflowRunTerminal(existing.GetStatus()) {
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
		if _, clearErr := b.state.clearWorkflowKeyRun(ctx, workflowKey, existing.GetId()); clearErr != nil {
			return nil, status.Errorf(codes.Internal, "clear stale workflow key: %v", clearErr)
		}
	} else if found {
		if _, err := b.state.clearWorkflowKeyRun(ctx, workflowKey, existing.GetId()); err != nil {
			return nil, status.Errorf(codes.Internal, "clear terminal workflow key: %v", err)
		}
	}
	return b.startSignalWorkflowRunV4(ctx, target, req, workflowKey, signal, updateID)
}

func (b *temporalBackend) signalExistingWorkflowKeyRunV4(ctx context.Context, workflowKey string, run *proto.BoundWorkflowRun, signal *proto.WorkflowSignal, updateID string) (*proto.SignalWorkflowRunResponse, error) {
	handle, err := decodeTemporalRunHandle(run.GetId())
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
		return nil, mapWorkflowUpdateError(err)
	}
	if out.GetRun() == nil {
		out.Run = cloneRun(run)
	}
	if out.GetSignal() == nil {
		out.Signal = cloneSignal(signal)
	}
	if strings.TrimSpace(out.GetWorkflowKey()) == "" {
		out.WorkflowKey = strings.TrimSpace(workflowKey)
	}
	return cloneSignalResponse(&out), nil
}

func (b *temporalBackend) releaseClaimedRunV4(ctx context.Context, run *proto.BoundWorkflowRun) error {
	handle, err := decodeTemporalRunHandle(run.GetId())
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	update, err := b.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   handle.RunWorkflowID,
		RunID:        handle.RunTemporalRunID,
		UpdateID:     "claim:" + hashID(run.GetId()),
		UpdateName:   updateClaimRun,
		Args:         []any{cloneRun(run)},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return mapTemporalWorkflowCallError("claim temporal workflow", err)
	}
	if err := update.Get(ctx, nil); err != nil {
		return mapWorkflowUpdateError(err)
	}
	return nil
}

func (b *temporalBackend) terminateUnclaimedRunV4(ctx context.Context, run *proto.BoundWorkflowRun) {
	handle, err := decodeTemporalRunHandle(run.GetId())
	if err != nil {
		return
	}
	_ = b.client.TerminateWorkflow(ctx, handle.RunWorkflowID, handle.RunTemporalRunID, "workflow key claim lost")
}

func (b *temporalBackend) startSignalWorkflowRunV4(ctx context.Context, target scopedTarget, req *proto.SignalOrStartWorkflowProviderRunRequest, workflowKey string, signal *proto.WorkflowSignal, updateID string) (*proto.SignalWorkflowRunResponse, error) {
	temporalWorkflowID := workflowID(b.cfg.ScopeID, "signal-keyed-v4", target.OwnerKey, hashID(workflowKey), uuid.NewString())
	conflictPolicy := enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL
	if signalKey := strings.TrimSpace(signal.GetIdempotencyKey()); signalKey != "" {
		temporalWorkflowID = workflowID(b.cfg.ScopeID, "signal-keyed-v4", target.OwnerKey, signalKey, hashID(workflowKey))
		conflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
	} else if signalIDKey := explicitSignalLedgerKey(signal); signalIDKey != "" {
		temporalWorkflowID = workflowID(b.cfg.ScopeID, "signal-keyed-v4", target.OwnerKey, signalIDKey, hashID(workflowKey))
		conflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
	}
	input := b.runV4Input(target.OwnerKey, req.GetExecutionRef(), workflowKey, target.Target, newManualTrigger(), req.GetCreatedBy(), true)
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
		return nil, mapWorkflowUpdateError(err)
	}
	startedRun, err := startOperation.Get(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "resolve signal-or-start temporal workflow: %v", err)
	}
	run := out.GetRun()
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
	out.Run = cloneRun(owner)
	if out.GetSignal() == nil {
		out.Signal = signalForStartedRun(owner, signal)
	}
	out.StartedRun = true
	out.WorkflowKey = strings.TrimSpace(workflowKey)
	return cloneSignalResponse(&out), nil
}

func runHasTemporalWorkflowID(run *proto.BoundWorkflowRun, workflowID string) bool {
	handle, err := decodeTemporalRunHandle(run.GetId())
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
		ProviderName:                  b.providerName,
		ScopeID:                       b.cfg.ScopeID,
		TaskQueue:                     b.cfg.TaskQueue,
		WorkflowRunTimeoutNS:          b.cfg.WorkflowRunTimeout,
		WorkflowTaskTimeoutNS:         b.cfg.WorkflowTaskTimeout,
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

func (b *temporalBackend) executeRunV4(ctx context.Context, workflowID string, input runWorkflowV4Input, conflict enumspb.WorkflowIdConflictPolicy, reuse enumspb.WorkflowIdReusePolicy) (*proto.BoundWorkflowRun, error) {
	run, err := b.client.ExecuteWorkflow(ctx, b.startWorkflowOptions(workflowID, conflict, reuse), gestaltRunWorkflowV4, input)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "start temporal v4 workflow: %v", err)
	}
	out := b.pendingRunFromWorkflowRun(run, input)
	if b.state != nil && !input.RequireClaim {
		_ = b.state.putRun(ctx, out)
	}
	return out, nil
}

func (b *temporalBackend) pendingRunFromWorkflowRun(run client.WorkflowRun, input runWorkflowV4Input) *proto.BoundWorkflowRun {
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
	out := &proto.BoundWorkflowRun{
		Id:           publicID,
		Status:       proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:       targetFromPayload(input.TargetPayload),
		Trigger:      triggerFromPayload(input.TriggerPayload),
		CreatedAt:    timestamppb.New(now),
		CreatedBy:    actorFromPayload(input.CreatedByPayload),
		ExecutionRef: strings.TrimSpace(input.ExecutionRef),
		WorkflowKey:  strings.TrimSpace(input.WorkflowKey),
	}
	return out
}

func (b *temporalBackend) queryRunWorkflow(ctx context.Context, handle *temporalRunHandle) (*proto.BoundWorkflowRun, error) {
	value, err := b.client.QueryWorkflow(ctx, handle.RunWorkflowID, handle.RunTemporalRunID, queryRunState)
	if err != nil {
		if isNotFoundLike(err) {
			return nil, status.Errorf(codes.NotFound, "workflow run %q not found", encodeTemporalRunHandle(*handle))
		}
		return nil, status.Errorf(codes.Internal, "query temporal workflow: %v", err)
	}
	var run proto.BoundWorkflowRun
	if err := value.Get(&run); err != nil {
		return nil, status.Errorf(codes.Internal, "decode temporal workflow run: %v", err)
	}
	if run.GetId() == "" {
		run.Id = encodeTemporalRunHandle(*handle)
	}
	return cloneRun(&run), nil
}

func (b *temporalBackend) startWorkflowOptions(workflowID string, conflict enumspb.WorkflowIdConflictPolicy, reuse enumspb.WorkflowIdReusePolicy) client.StartWorkflowOptions {
	opts := b.runStartOptions(workflowID, conflict, reuse)
	opts.WorkflowIDReusePolicy = reuse
	return opts
}

func (b *temporalBackend) getRunProjection(ctx context.Context, runID string) (*proto.BoundWorkflowRun, bool, error) {
	if b.state == nil {
		return nil, false, nil
	}
	return b.state.getRun(ctx, runID)
}

func (b *temporalBackend) listRunProjections(ctx context.Context) ([]*proto.BoundWorkflowRun, error) {
	if b.state == nil {
		return nil, nil
	}
	return b.state.listRuns(ctx)
}
