package temporal

import (
	"context"
	"errors"
	"fmt"
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
	if existing, found, err := b.getWorkflowKeyRunForStart(ctx, workflowKey); err != nil {
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

	input := b.runV4Input(target.OwnerKey, req.GetExecutionRef(), workflowKey, target.Target, newManualTrigger(), req.GetCreatedBy(), nil, false)
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
	if run, found, err := b.startFromLegacyStartRunEntry(ctx, target, req, key, fingerprint, now); err != nil {
		return nil, err
	} else if found {
		return run, nil
	}
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
	run, err := b.executeRunV4(ctx, temporalWorkflowID, b.runV4Input(target.OwnerKey, req.GetExecutionRef(), "", target.Target, newManualTrigger(), req.GetCreatedBy(), nil, false), enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
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

func (b *temporalBackend) startFromLegacyStartRunEntry(ctx context.Context, target scopedTarget, req *proto.StartWorkflowProviderRunRequest, key, fingerprint string, now time.Time) (*proto.BoundWorkflowRun, bool, error) {
	ledgerKey := startLedgerKey(target.OwnerKey, key)
	entry, found, err := b.queryLegacyLedger(ctx, ledgerKey)
	if err != nil || !found || entry.Status != "completed" {
		if err != nil || !found {
			return nil, false, err
		}
		if legacyLedgerEntryExpired(entry, now) {
			return nil, false, nil
		}
		if strings.TrimSpace(entry.Fingerprint) != strings.TrimSpace(fingerprint) {
			return nil, true, status.Errorf(codes.FailedPrecondition, "idempotency key %q is already reserved for a different request", ledgerKey)
		}
		run, err := b.startLegacyReservedRun(ctx, target, req, key, fingerprint, ledgerKey)
		return run, true, err
	}
	if legacyLedgerEntryExpired(entry, now) {
		return nil, false, nil
	}
	if strings.TrimSpace(entry.Fingerprint) != strings.TrimSpace(fingerprint) {
		return nil, true, status.Errorf(codes.FailedPrecondition, "idempotency key %q is already reserved for a different request", ledgerKey)
	}
	run := runFromPayload(entry.RunPayload)
	if run == nil {
		run, err := b.startLegacyReservedRun(ctx, target, req, key, fingerprint, ledgerKey)
		return run, true, err
	}
	return run, true, nil
}

func (b *temporalBackend) startLegacyReservedRun(ctx context.Context, target scopedTarget, req *proto.StartWorkflowProviderRunRequest, key, fingerprint, ledgerKey string) (*proto.BoundWorkflowRun, error) {
	temporalWorkflowID := workflowID(b.cfg.ScopeID, "manual-v3", target.OwnerKey, key)
	run, err := b.executeRunV4(ctx, temporalWorkflowID, b.runV4Input(target.OwnerKey, req.GetExecutionRef(), "", target.Target, newManualTrigger(), req.GetCreatedBy(), nil, false), enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
	if err != nil {
		return nil, err
	}
	if err := b.completeLedger(ctx, ledgerKey, fingerprint, &proto.SignalWorkflowRunResponse{Run: run}, run); err != nil {
		return nil, err
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
	if existing, found, err := b.getWorkflowKeyRunForStart(ctx, workflowKey); err != nil {
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

func (b *temporalBackend) getWorkflowKeyRunForStart(ctx context.Context, workflowKey string) (*proto.BoundWorkflowRun, bool, error) {
	run, found, err := b.state.getWorkflowKeyRun(ctx, workflowKey)
	if err != nil || found {
		return run, found, err
	}
	run, found, err = b.queryLegacyLaneRun(ctx, workflowKey)
	if err != nil || !found || workflowRunTerminal(run.GetStatus()) {
		return run, found, err
	}
	owner, _, claimErr := b.state.claimWorkflowKeyRun(ctx, workflowKey, run, time.Now().UTC())
	if claimErr != nil {
		return nil, false, claimErr
	}
	return owner, true, nil
}

func (b *temporalBackend) queryLegacyLaneRun(ctx context.Context, workflowKey string) (*proto.BoundWorkflowRun, bool, error) {
	workflowKey = strings.TrimSpace(workflowKey)
	if workflowKey == "" {
		return nil, false, nil
	}
	value, err := b.client.QueryWorkflow(ctx, b.laneWorkflowID(workflowKey), "", queryLaneRun)
	if err != nil {
		if isNotFoundLike(err) || isQueryHandlerUnavailable(err) {
			if isQueryHandlerUnavailable(err) {
				return nil, false, status.Errorf(codes.FailedPrecondition, "legacy workflow key lane %q cannot report its active run yet", workflowKey)
			}
			return nil, false, nil
		}
		return nil, false, err
	}
	var run proto.BoundWorkflowRun
	if err := value.Get(&run); err != nil {
		return nil, false, err
	}
	if strings.TrimSpace(run.GetId()) == "" {
		return nil, false, nil
	}
	return cloneRun(&run), true, nil
}

func (b *temporalBackend) signalExistingWorkflowKeyRunV4(ctx context.Context, workflowKey string, run *proto.BoundWorkflowRun, signal *proto.WorkflowSignal, updateID string) (*proto.SignalWorkflowRunResponse, error) {
	handle, err := decodeV3RunHandle(run.GetId())
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
	if b.state != nil && out.GetRun() != nil {
		_ = b.state.putRun(ctx, out.GetRun())
	}
	return cloneSignalResponse(&out), nil
}

func (b *temporalBackend) releaseClaimedRunV4(ctx context.Context, run *proto.BoundWorkflowRun) error {
	handle, err := decodeV3RunHandle(run.GetId())
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
	var out proto.BoundWorkflowRun
	if err := update.Get(ctx, &out); err != nil {
		return mapWorkflowUpdateError(err)
	}
	if b.state != nil && out.GetId() != "" {
		_ = b.state.putRun(ctx, &out)
	}
	return nil
}

func (b *temporalBackend) terminateUnclaimedRunV4(ctx context.Context, run *proto.BoundWorkflowRun) {
	handle, err := decodeV3RunHandle(run.GetId())
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
	}
	input := b.runV4Input(target.OwnerKey, req.GetExecutionRef(), workflowKey, target.Target, newManualTrigger(), req.GetCreatedBy(), nil, true)
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
	resp, err := b.signalExistingWorkflowKeyRunV4(ctx, workflowKey, owner, signal, updateID)
	if err != nil {
		return nil, err
	}
	resp.StartedRun = true
	if resp.GetSignal().GetSequence() <= 0 || strings.TrimSpace(resp.GetSignal().GetId()) == "" {
		resp.Signal = signalForStartedRun(owner, signal)
	}
	return resp, nil
}

func signalForStartedRun(run *proto.BoundWorkflowRun, signal *proto.WorkflowSignal) *proto.WorkflowSignal {
	out := cloneSignal(signal)
	if out == nil {
		return nil
	}
	if out.GetSequence() <= 0 {
		out.Sequence = 1
	}
	if strings.TrimSpace(out.GetId()) == "" {
		out.Id = "signal:" + hashID(run.GetId(), out.GetName(), fmt.Sprintf("%d", out.GetSequence()), out.GetIdempotencyKey())
	}
	return out
}

func runHasTemporalWorkflowID(run *proto.BoundWorkflowRun, workflowID string) bool {
	handle, err := decodeV3RunHandle(run.GetId())
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

func legacyLedgerEntryExpired(entry *ownerLedgerEntry, now time.Time) bool {
	if entry == nil || entry.ExpiresAt.IsZero() {
		return false
	}
	return !entry.ExpiresAt.After(now.UTC())
}

func (b *temporalBackend) queryLegacyLedger(ctx context.Context, key string) (*ownerLedgerEntry, bool, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, false, nil
	}
	value, err := b.client.QueryWorkflow(ctx, b.ownerLedgerWorkflowID(key), "", queryLedgerGet, key)
	if err != nil {
		if isNotFoundLike(err) {
			return nil, false, nil
		}
		return nil, false, status.Errorf(codes.Internal, "query temporal idempotency ledger: %v", err)
	}
	var entry ownerLedgerEntry
	if err := value.Get(&entry); err != nil {
		return nil, false, status.Errorf(codes.Internal, "decode temporal idempotency ledger: %v", err)
	}
	if strings.TrimSpace(entry.Key) == "" {
		return nil, false, nil
	}
	return &entry, true, nil
}

func (b *temporalBackend) runV4Input(ownerKey, executionRef, workflowKey string, target *proto.BoundWorkflowTarget, trigger *proto.WorkflowRunTrigger, createdBy *proto.WorkflowActor, initialSignal *proto.WorkflowSignal, requireSignal bool) runWorkflowV4Input {
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
		InitialSignalPayload:          protoPayload(initialSignal),
		RequireSignal:                 requireSignal,
	}
}

func (b *temporalBackend) executeRunV4(ctx context.Context, workflowID string, input runWorkflowV4Input, conflict enumspb.WorkflowIdConflictPolicy, reuse enumspb.WorkflowIdReusePolicy) (*proto.BoundWorkflowRun, error) {
	run, err := b.client.ExecuteWorkflow(ctx, b.startV3WorkflowOptions(workflowID, conflict, reuse), gestaltRunWorkflowV4, input)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "start temporal v4 workflow: %v", err)
	}
	now := time.Now().UTC()
	if input.ScheduleID != "" {
		input.TriggerPayload = protoPayload(scheduleTrigger(input.ScheduleID, now))
	}
	publicID := encodeV3RunHandle(v3RunHandle{
		Kind:             runHandleKindV3,
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
	if b.state != nil && !input.RequireClaim {
		_ = b.state.putRun(ctx, out)
	}
	return out, nil
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

func mergeRuns(primary, legacy []*proto.BoundWorkflowRun) []*proto.BoundWorkflowRun {
	if len(primary) == 0 {
		return cloneRuns(legacy)
	}
	out := make([]*proto.BoundWorkflowRun, 0, len(primary)+len(legacy))
	seen := make(map[string]struct{}, len(primary)+len(legacy))
	for _, run := range primary {
		run = cloneRun(run)
		if run == nil || strings.TrimSpace(run.GetId()) == "" {
			continue
		}
		seen[run.GetId()] = struct{}{}
		out = append(out, run)
	}
	for _, run := range legacy {
		run = cloneRun(run)
		if run == nil || strings.TrimSpace(run.GetId()) == "" {
			continue
		}
		if _, ok := seen[run.GetId()]; ok {
			continue
		}
		out = append(out, run)
	}
	sortRuns(out)
	return out
}

func cloneRuns(runs []*proto.BoundWorkflowRun) []*proto.BoundWorkflowRun {
	out := make([]*proto.BoundWorkflowRun, 0, len(runs))
	for _, run := range runs {
		if cloned := cloneRun(run); cloned != nil {
			out = append(out, cloned)
		}
	}
	sortRuns(out)
	return out
}
