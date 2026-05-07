package temporal

import (
	"context"
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

func (b *temporalBackend) laneWorkflowID(workflowKey string) string {
	return workflowID(b.cfg.ScopeID, "key-lane-v3", workflowKey)
}

func (b *temporalBackend) ownerLedgerWorkflowID(key string) string {
	shard := shardFor(key, b.cfg.IndexShardCount)
	return workflowID(b.cfg.ScopeID, "owner-ledger-v3", fmt.Sprintf("%04d", shard))
}

func (b *temporalBackend) laneInput(workflowKey string) laneWorkflowSnapshot {
	return laneWorkflowSnapshot{Input: laneWorkflowInput{
		ProviderName:                  b.providerName,
		ScopeID:                       b.cfg.ScopeID,
		IndexShardCount:               b.cfg.IndexShardCount,
		TaskQueue:                     b.cfg.TaskQueue,
		WorkflowRunTimeoutNS:          b.cfg.WorkflowRunTimeout,
		WorkflowTaskTimeoutNS:         b.cfg.WorkflowTaskTimeout,
		ActivityStartToCloseTimeoutNS: b.cfg.ActivityStartToCloseTimeout,
		IdempotencyRetentionNS:        b.cfg.IdempotencyRetention,
		WorkflowKey:                   strings.TrimSpace(workflowKey),
	}}
}

func (b *temporalBackend) runV3Input(ownerKey, executionRef, workflowKey string, target *proto.BoundWorkflowTarget, trigger *proto.WorkflowRunTrigger, createdBy *proto.WorkflowActor, requireSignal bool) runWorkflowV3Input {
	return runWorkflowV3Input{
		ProviderName:                  b.providerName,
		ScopeID:                       b.cfg.ScopeID,
		IndexShardCount:               b.cfg.IndexShardCount,
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

func (b *temporalBackend) startV3WorkflowOptions(workflowID string, conflict enumspb.WorkflowIdConflictPolicy, reuse enumspb.WorkflowIdReusePolicy) client.StartWorkflowOptions {
	opts := b.runStartOptions(workflowID, conflict, reuse)
	opts.WorkflowIDReusePolicy = reuse
	return opts
}

func (b *temporalBackend) signalOrStartLane(ctx context.Context, workflowKey, updateID string, req laneSignalRequest) (*proto.SignalWorkflowRunResponse, error) {
	workflowKey = strings.TrimSpace(workflowKey)
	laneID := b.laneWorkflowID(workflowKey)
	startOp := b.client.NewWithStartWorkflowOperation(
		b.startOptions(laneID),
		gestaltWorkflowKeyLaneV1,
		b.laneInput(workflowKey),
	)
	update, err := b.client.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: startOp,
		UpdateOptions: client.UpdateWorkflowOptions{
			UpdateID:     strings.TrimSpace(updateID),
			UpdateName:   updateLaneSignalOrStart,
			Args:         []any{req},
			WaitForStage: client.WorkflowUpdateStageCompleted,
		},
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "signal-or-start temporal lane: %v", err)
	}
	var resp proto.SignalWorkflowRunResponse
	if err := update.Get(ctx, &resp); err != nil {
		return nil, mapWorkflowUpdateError(err)
	}
	return &resp, nil
}

func (b *temporalBackend) startKeyedRunInLane(ctx context.Context, workflowKey, updateID string, req laneStartRequest) (*proto.BoundWorkflowRun, error) {
	laneID := b.laneWorkflowID(workflowKey)
	startOp := b.client.NewWithStartWorkflowOperation(
		b.startOptions(laneID),
		gestaltWorkflowKeyLaneV1,
		b.laneInput(workflowKey),
	)
	update, err := b.client.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: startOp,
		UpdateOptions: client.UpdateWorkflowOptions{
			UpdateID:     strings.TrimSpace(updateID),
			UpdateName:   updateLaneStartRun,
			Args:         []any{req},
			WaitForStage: client.WorkflowUpdateStageCompleted,
		},
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "start temporal lane run: %v", err)
	}
	var run proto.BoundWorkflowRun
	if err := update.Get(ctx, &run); err != nil {
		return nil, mapWorkflowUpdateError(err)
	}
	return &run, nil
}

func (b *temporalBackend) updateLaneSignalRun(ctx context.Context, handle *v3RunHandle, signal *proto.WorkflowSignal, updateID string) (*proto.SignalWorkflowRunResponse, error) {
	update, err := b.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID: handle.LaneWorkflowID,
		UpdateID:   strings.TrimSpace(updateID),
		UpdateName: updateLaneSignalRun,
		Args: []any{laneSignalRunRequest{
			RunID:          encodeV3RunHandle(*handle),
			OwnerKey:       handle.OwnerKey,
			SignalPayload:  protoPayload(signal),
			RequestID:      strings.TrimSpace(updateID),
			IdempotencyKey: strings.TrimSpace(signal.GetIdempotencyKey()),
		}},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "signal temporal lane run: %v", err)
	}
	var resp proto.SignalWorkflowRunResponse
	if err := update.Get(ctx, &resp); err != nil {
		return nil, mapWorkflowUpdateError(err)
	}
	return &resp, nil
}

func (b *temporalBackend) cancelLaneRun(ctx context.Context, handle *v3RunHandle, reason string) (*proto.BoundWorkflowRun, error) {
	update, err := b.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   handle.LaneWorkflowID,
		UpdateID:     "cancel:" + hashID(handle.LaneWorkflowID, handle.RunWorkflowID, handle.RunTemporalRunID, reason),
		UpdateName:   updateLaneCancelRun,
		Args:         []any{laneCancelRequest{RunID: encodeV3RunHandle(*handle), Reason: strings.TrimSpace(reason)}},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cancel temporal lane run: %v", err)
	}
	var run proto.BoundWorkflowRun
	if err := update.Get(ctx, &run); err != nil {
		return nil, mapWorkflowUpdateError(err)
	}
	return &run, nil
}

func (b *temporalBackend) executeRunV3(ctx context.Context, workflowID string, input runWorkflowV3Input, conflict enumspb.WorkflowIdConflictPolicy, reuse enumspb.WorkflowIdReusePolicy) (*proto.BoundWorkflowRun, error) {
	run, err := b.client.ExecuteWorkflow(ctx, b.startV3WorkflowOptions(workflowID, conflict, reuse), gestaltRunWorkflowV3, input)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "start temporal v3 workflow: %v", err)
	}
	now := time.Now().UTC()
	publicID := encodeV3RunHandle(v3RunHandle{
		Kind:             runHandleKindV3,
		RunWorkflowID:    run.GetID(),
		RunTemporalRunID: run.GetRunID(),
		LogicalRunKey:    input.LogicalRunKey,
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
	_ = b.putRunTemporalIndex(ctx, out)
	return out, nil
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

func (b *temporalBackend) reserveLedger(ctx context.Context, key string, req ownerLedgerReserveRequest) (*ownerLedgerEntry, bool, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, false, nil
	}
	req.Key = key
	req.RetentionNS = b.cfg.IdempotencyRetention
	workflowID := b.ownerLedgerWorkflowID(key)
	startOp := b.client.NewWithStartWorkflowOperation(
		b.startOptions(workflowID),
		gestaltOwnerLedgerWorkflow,
		ownerLedgerInput{ScopeID: b.cfg.ScopeID, Shard: shardFor(key, b.cfg.IndexShardCount), IdempotencyRetentionNS: b.cfg.IdempotencyRetention},
	)
	update, err := b.client.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: startOp,
		UpdateOptions: client.UpdateWorkflowOptions{
			UpdateID:     "reserve:" + hashID(key, req.Fingerprint),
			UpdateName:   updateLedgerReserve,
			Args:         []any{req},
			WaitForStage: client.WorkflowUpdateStageCompleted,
		},
	})
	if err != nil {
		return nil, false, status.Errorf(codes.Internal, "reserve temporal idempotency ledger: %v", err)
	}
	var resp ownerLedgerReserveResponse
	if err := update.Get(ctx, &resp); err != nil {
		return nil, false, mapWorkflowUpdateError(err)
	}
	return resp.Entry, resp.Existing, nil
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
