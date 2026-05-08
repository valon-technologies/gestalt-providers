package temporal

import (
	"context"
	"errors"
	"strings"
	"time"

	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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
	if b.state != nil {
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
