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
)

func (b *temporalBackend) laneWorkflowID(workflowKey string) string {
	return workflowID(b.cfg.ScopeID, "key-lane-v3", workflowKey)
}

func (b *temporalBackend) ownerLedgerWorkflowID(key string) string {
	shard := shardFor(key, b.cfg.IndexShardCount)
	return workflowID(b.cfg.ScopeID, "owner-ledger-v3", fmt.Sprintf("%04d", shard))
}

func (b *temporalBackend) startV3WorkflowOptions(workflowID string, conflict enumspb.WorkflowIdConflictPolicy, reuse enumspb.WorkflowIdReusePolicy) client.StartWorkflowOptions {
	opts := b.runStartOptions(workflowID, conflict, reuse)
	opts.WorkflowIDReusePolicy = reuse
	return opts
}

func (b *temporalBackend) queryOrGetV3Run(ctx context.Context, handle *v3RunHandle) (*proto.BoundWorkflowRun, error) {
	value, err := b.client.QueryWorkflow(ctx, handle.RunWorkflowID, handle.RunTemporalRunID, queryRunState)
	if err == nil {
		var run proto.BoundWorkflowRun
		if err := value.Get(&run); err == nil && run.GetId() != "" {
			return &run, nil
		}
	}
	var run proto.BoundWorkflowRun
	if err := b.client.GetWorkflow(ctx, handle.RunWorkflowID, handle.RunTemporalRunID).Get(ctx, &run); err == nil && run.GetId() != "" {
		return &run, nil
	}
	return nil, status.Errorf(codes.NotFound, "workflow run %q not found; workflow history may have expired and projection is missing", encodeV3RunHandle(*handle))
}

func (b *temporalBackend) listRunsFromTemporalIndex(ctx context.Context) ([]*proto.BoundWorkflowRun, error) {
	var runs []*proto.BoundWorkflowRun
	err := b.updateAllIndexes(ctx, updateListRuns, func() any {
		return &proto.ListWorkflowProviderRunsResponse{}
	}, func(out any) error {
		resp, ok := out.(*proto.ListWorkflowProviderRunsResponse)
		if !ok || resp == nil {
			return nil
		}
		for _, run := range resp.GetRuns() {
			runs = append(runs, cloneRun(run))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return runs, nil
}

func (b *temporalBackend) putRunTemporalIndex(ctx context.Context, run *proto.BoundWorkflowRun) error {
	run = cloneRun(run)
	if run == nil || strings.TrimSpace(run.GetId()) == "" {
		return nil
	}
	shard := shardFor(run.GetId(), b.cfg.IndexShardCount)
	var out proto.BoundWorkflowRun
	if err := b.updateIndexShard(ctx, shard, updatePutRun, []any{cloneRun(run)}, &out); err != nil {
		return err
	}
	return nil
}

func (b *temporalBackend) ensureTemporalRunIndexes(ctx context.Context) error {
	return b.updateAllIndexes(ctx, updateListRuns, func() any {
		return &proto.ListWorkflowProviderRunsResponse{}
	}, func(any) error {
		return nil
	})
}

func (b *temporalBackend) deleteDeprecatedTemporalIndexState(ctx context.Context) error {
	for shard := 0; shard < b.cfg.IndexShardCount; shard++ {
		workflowID := indexWorkflowID(b.cfg.ScopeID, shard)
		if _, err := b.client.DescribeWorkflowExecution(ctx, workflowID, ""); err != nil {
			if isNotFoundLike(err) {
				continue
			}
			return status.Errorf(codes.Internal, "describe temporal index shard %d: %v", shard, err)
		}
		var removed int
		if err := b.updateIndexShard(ctx, shard, updatePruneRuns, []any{b.cfg.IndexShardCount}, &removed); err != nil {
			if terminateErr := b.client.TerminateWorkflow(ctx, workflowID, "", "delete deprecated temporal index state"); terminateErr != nil && !isNotFoundLike(terminateErr) {
				return status.Errorf(codes.Internal, "terminate deprecated temporal index shard %d after cleanup failure %v: %v", shard, err, terminateErr)
			}
			continue
		}
		if err := b.compactIndexShard(ctx, shard); err != nil {
			if terminateErr := b.client.TerminateWorkflow(ctx, workflowID, "", "delete deprecated temporal index state"); terminateErr != nil && !isNotFoundLike(terminateErr) {
				return status.Errorf(codes.Internal, "terminate deprecated temporal index shard %d after compaction failure %v: %v", shard, err, terminateErr)
			}
		}
	}
	return nil
}

func (b *temporalBackend) completeLedger(ctx context.Context, key, fingerprint string, resp *proto.SignalWorkflowRunResponse, run *proto.BoundWorkflowRun) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil
	}
	workflowID := b.ownerLedgerWorkflowID(key)
	update, err := b.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   workflowID,
		UpdateID:     "complete:" + hashID(key, fingerprint),
		UpdateName:   updateLedgerComplete,
		Args:         []any{ownerLedgerCompleteRequest{Key: key, Fingerprint: fingerprint, ResponsePayload: protoPayload(resp), RunPayload: protoPayload(run), RetentionNS: b.cfg.IdempotencyRetention}},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return status.Errorf(codes.Internal, "complete temporal idempotency ledger: %v", err)
	}
	if err := update.Get(ctx, nil); err != nil {
		return mapWorkflowUpdateError(err)
	}
	return nil
}

func (b *temporalBackend) reserveSignalIdempotency(ctx context.Context, req signalIdempotencyReserveRequest) (*proto.SignalWorkflowRunResponse, error) {
	req.Key = strings.TrimSpace(req.Key)
	req.Fingerprint = strings.TrimSpace(req.Fingerprint)
	if req.Key == "" {
		return nil, nil
	}
	if b.state == nil {
		return nil, status.Error(codes.FailedPrecondition, "workflow state store is required for signal idempotency")
	}

	now := time.Now().UTC()
	entry, existing, err := b.state.reserveSignalIdempotency(ctx, req, b.cfg.IdempotencyRetention, now)
	if err != nil {
		var conflict *runIdempotencyConflictError
		if errors.As(err, &conflict) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		if status.Code(err) == codes.Aborted {
			return nil, status.Errorf(codes.Aborted, "reserve workflow signal idempotency: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "reserve workflow signal idempotency: %v", err)
	}
	if existing && entry != nil && entry.Status == "completed" {
		if resp := signalResponseFromPayload(entry.ResponsePayload); resp != nil {
			return resp, nil
		}
	}
	legacy, found, err := b.queryLegacyLedger(ctx, req.Key)
	if err != nil {
		return nil, err
	}
	if found && !legacyLedgerEntryExpired(legacy, now) {
		if strings.TrimSpace(legacy.Fingerprint) != req.Fingerprint {
			return nil, status.Errorf(codes.FailedPrecondition, "idempotency key %q is already reserved for a different request", req.Key)
		}
		if legacy.Status == "completed" {
			if resp := signalResponseFromPayload(legacy.ResponsePayload); resp != nil {
				if err := b.state.completeSignalIdempotency(ctx, req.Key, req.Fingerprint, resp, b.cfg.IdempotencyRetention, now); err != nil {
					return nil, status.Errorf(codes.Internal, "complete workflow signal idempotency: %v", err)
				}
				return resp, nil
			}
		}
	}
	return nil, nil
}

func (b *temporalBackend) completeSignalIdempotency(ctx context.Context, key, fingerprint string, resp *proto.SignalWorkflowRunResponse) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil
	}
	if b.state == nil {
		return status.Error(codes.FailedPrecondition, "workflow state store is required for signal idempotency")
	}
	if err := b.state.completeSignalIdempotency(ctx, key, fingerprint, resp, b.cfg.IdempotencyRetention, time.Now().UTC()); err != nil {
		var conflict *runIdempotencyConflictError
		if errors.As(err, &conflict) {
			return status.Error(codes.FailedPrecondition, err.Error())
		}
		return status.Errorf(codes.Internal, "complete workflow signal idempotency: %v", err)
	}
	return nil
}

func ownerIdempotencyLedgerKey(ownerKey, key string) string {
	ownerKey = strings.TrimSpace(ownerKey)
	key = strings.TrimSpace(key)
	if ownerKey == "" || key == "" {
		return ""
	}
	return "owner-idem:" + hashID(ownerKey, key)
}

func explicitSignalLedgerKey(signal *proto.WorkflowSignal) string {
	id := strings.TrimSpace(signal.GetId())
	if id == "" {
		return ""
	}
	return "signal-id:" + hashID(id)
}

func startLedgerKey(ownerKey, key string) string {
	ownerKey = strings.TrimSpace(ownerKey)
	key = strings.TrimSpace(key)
	if ownerKey == "" || key == "" {
		return ""
	}
	return "start:" + hashID(ownerKey, key)
}

func signalFingerprint(ownerKey, workflowKey string, signal *proto.WorkflowSignal) string {
	stableSignal := cloneSignal(signal)
	if stableSignal != nil {
		stableSignal.CreatedAt = nil
		stableSignal.Sequence = 0
	}
	return hashID("signal", ownerKey, workflowKey, protoHashID(stableSignal))
}

func startFingerprint(ownerKey, key, workflowKey, executionRef string, target *proto.BoundWorkflowTarget, createdBy *proto.WorkflowActor) string {
	return hashID("start", ownerKey, key, workflowKey, executionRef, protoHashID(target), protoHashID(createdBy))
}

func eventRunWorkflowID(scopeID, triggerID string, event *proto.WorkflowEvent) string {
	if event.GetId() != "" {
		return workflowID(scopeID, "event-v3", triggerID, event.GetSource(), event.GetId())
	}
	return workflowID(scopeID, "event-v3", triggerID, event.GetSource(), uuid.NewString())
}
