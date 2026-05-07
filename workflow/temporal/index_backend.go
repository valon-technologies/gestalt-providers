package temporal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxIndexFanoutConcurrency = 16

func (b *temporalBackend) ensureScope(ctx context.Context) error {
	startOp := b.client.NewWithStartWorkflowOperation(b.startOptions(metadataWorkflowID(b.cfg.ScopeID)), scopeMetadataWorkflow, scopeMetadata{ScopeID: b.cfg.ScopeID, IndexShardCount: b.cfg.IndexShardCount})
	update, err := b.client.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: startOp,
		UpdateOptions: client.UpdateWorkflowOptions{
			UpdateID:     "ensure:" + uuid.NewString(),
			UpdateName:   updateEnsureScope,
			Args:         []any{scopeMetadata{ScopeID: b.cfg.ScopeID, IndexShardCount: b.cfg.IndexShardCount}},
			WaitForStage: client.WorkflowUpdateStageCompleted,
		},
	})
	if err != nil {
		return fmt.Errorf("ensure temporal scope metadata: %w", err)
	}
	if err := update.Get(ctx, nil); err != nil {
		return mapWorkflowUpdateError(err)
	}
	return nil
}

func (b *temporalBackend) updateIndex(ctx context.Context, key, updateName string, args []any, out any) error {
	shard := shardFor(key, b.cfg.IndexShardCount)
	return b.updateIndexShard(ctx, shard, updateName, args, out)
}

func (b *temporalBackend) updateIndexShard(ctx context.Context, shard int, updateName string, args []any, out any) error {
	if err := b.runIndexUpdate(ctx, shard, updateName, args, out); err == nil {
		return nil
	} else if !isTemporalUpdateLimitError(err) {
		return status.Errorf(codes.Internal, "update temporal index: %v", err)
	}
	if err := b.compactIndexShard(ctx, shard); err != nil {
		return err
	}
	if err := b.runIndexUpdate(ctx, shard, updateName, args, out); err != nil {
		return status.Errorf(codes.Internal, "update temporal index after compaction: %v", err)
	}
	return nil
}

func (b *temporalBackend) runIndexUpdate(ctx context.Context, shard int, updateName string, args []any, out any) error {
	startOp := b.client.NewWithStartWorkflowOperation(
		b.startOptions(indexWorkflowID(b.cfg.ScopeID, shard)),
		indexWorkflow,
		indexInput{ScopeID: b.cfg.ScopeID, Shard: shard},
	)
	update, err := b.client.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: startOp,
		UpdateOptions: client.UpdateWorkflowOptions{
			UpdateID:     updateName + ":" + uuid.NewString(),
			UpdateName:   updateName,
			Args:         args,
			WaitForStage: client.WorkflowUpdateStageCompleted,
		},
	})
	if err != nil {
		return err
	}
	if err := update.Get(ctx, out); err != nil {
		return mapWorkflowUpdateError(err)
	}
	return nil
}

func (b *temporalBackend) queryIndex(ctx context.Context, key, queryName string, args []any, out any) error {
	shard := shardFor(key, b.cfg.IndexShardCount)
	if err := b.queryIndexShard(ctx, shard, queryName, args, out); err == nil {
		return nil
	} else if !isNotFoundLike(err) && !isQueryHandlerUnavailable(err) {
		return status.Errorf(codes.Internal, "query temporal index: %v", err)
	}
	return b.updateIndexShard(ctx, shard, queryName, args, out)
}

func (b *temporalBackend) queryIndexShard(ctx context.Context, shard int, queryName string, args []any, out any) error {
	var lastErr error
	delay := 50 * time.Millisecond
	for attempt := 0; attempt < 5; attempt++ {
		value, err := b.client.QueryWorkflow(ctx, indexWorkflowID(b.cfg.ScopeID, shard), "", queryName, args...)
		if err == nil {
			return value.Get(out)
		}
		if !isQueryHandlerUnavailable(err) {
			return err
		}
		lastErr = err
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if delay < 500*time.Millisecond {
			delay *= 2
		}
	}
	return lastErr
}

func (b *temporalBackend) compactIndexShard(ctx context.Context, shard int) error {
	workflowID := indexWorkflowID(b.cfg.ScopeID, shard)
	beforeRunID := ""
	if desc, err := b.client.DescribeWorkflowExecution(ctx, workflowID, ""); err == nil && desc.GetWorkflowExecutionInfo() != nil && desc.GetWorkflowExecutionInfo().GetExecution() != nil {
		beforeRunID = desc.GetWorkflowExecutionInfo().GetExecution().GetRunId()
	} else if err != nil && !isNotFoundLike(err) {
		return status.Errorf(codes.Internal, "describe temporal index shard %d: %v", shard, err)
	}
	if err := b.client.SignalWorkflow(ctx, workflowID, "", signalIndexCompact, "workflow update limit reached"); err != nil {
		if isNotFoundLike(err) {
			return nil
		}
		return status.Errorf(codes.Internal, "compact temporal index shard %d: %v", shard, err)
	}
	if beforeRunID == "" {
		return nil
	}
	delay := 100 * time.Millisecond
	for attempt := 0; attempt < 20; attempt++ {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		desc, err := b.client.DescribeWorkflowExecution(ctx, workflowID, "")
		if err != nil {
			if isNotFoundLike(err) {
				continue
			}
			return status.Errorf(codes.Internal, "describe compacted temporal index shard %d: %v", shard, err)
		}
		info := desc.GetWorkflowExecutionInfo()
		if info != nil && info.GetExecution() != nil {
			runID := info.GetExecution().GetRunId()
			if runID != "" && runID != beforeRunID {
				return nil
			}
		}
		if delay < time.Second {
			delay *= 2
		}
	}
	return status.Errorf(codes.DeadlineExceeded, "timed out compacting temporal index shard %d", shard)
}

func (b *temporalBackend) updateAllIndexes(ctx context.Context, updateName string, outFactory func() any, consume func(any) error) error {
	shardCount := b.cfg.IndexShardCount
	if shardCount <= 0 {
		return nil
	}
	fanoutCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	concurrency := shardCount
	if concurrency > maxIndexFanoutConcurrency {
		concurrency = maxIndexFanoutConcurrency
	}
	sem := make(chan struct{}, concurrency)
	results := make([]any, shardCount)
	errs := make([]error, shardCount)
	var wg sync.WaitGroup
	for shard := 0; shard < shardCount; shard++ {
		shard := shard
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-fanoutCtx.Done():
				errs[shard] = fanoutCtx.Err()
				return
			}
			out := outFactory()
			if err := b.queryIndexShard(fanoutCtx, shard, updateName, nil, out); err != nil {
				if !isNotFoundLike(err) && !isQueryHandlerUnavailable(err) {
					errs[shard] = status.Errorf(codes.Internal, "query temporal index shard %d: %v", shard, err)
					cancel()
					return
				}
				if err := b.updateIndexShard(fanoutCtx, shard, updateName, nil, out); err != nil {
					errs[shard] = err
					cancel()
					return
				}
			}
			results[shard] = out
		}()
	}
	wg.Wait()
	var firstErr error
	var canceledErr error
	for _, err := range errs {
		if err == nil {
			continue
		}
		if errors.Is(err, context.Canceled) && ctx.Err() == nil {
			if canceledErr == nil {
				canceledErr = err
			}
			continue
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return firstErr
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if canceledErr != nil {
		return canceledErr
	}
	for _, out := range results {
		if err := consume(out); err != nil {
			return err
		}
	}
	return nil
}

func (b *temporalBackend) putScheduleIndex(ctx context.Context, schedule *proto.BoundWorkflowSchedule) error {
	if b.state == nil {
		return status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.putSchedule(ctx, schedule)
}

func (b *temporalBackend) getScheduleIndex(ctx context.Context, id string) (*proto.BoundWorkflowSchedule, bool, error) {
	if b.state == nil {
		return nil, false, status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.getSchedule(ctx, id)
}

func (b *temporalBackend) listSchedulesIndex(ctx context.Context) ([]*proto.BoundWorkflowSchedule, error) {
	if b.state == nil {
		return nil, status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return nil, nil
}

func (b *temporalBackend) deleteScheduleIndex(ctx context.Context, id string) error {
	if b.state == nil {
		return status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.deleteSchedule(ctx, id)
}

func (b *temporalBackend) putTriggerInIndex(ctx context.Context, key string, trigger *proto.BoundWorkflowEventTrigger) error {
	if b.state == nil {
		return status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.putTrigger(ctx, trigger)
}

func (b *temporalBackend) putTriggerIndex(ctx context.Context, trigger *proto.BoundWorkflowEventTrigger) error {
	if b.state == nil {
		return status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.putTrigger(ctx, trigger)
}

func (b *temporalBackend) getTriggerIndex(ctx context.Context, id string) (*proto.BoundWorkflowEventTrigger, bool, error) {
	if b.state == nil {
		return nil, false, status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.getTrigger(ctx, id)
}

func (b *temporalBackend) listTriggersIndex(ctx context.Context) ([]*proto.BoundWorkflowEventTrigger, error) {
	if b.state == nil {
		return nil, status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.listTriggers(ctx)
}

func (b *temporalBackend) matchTriggersIndex(ctx context.Context, ownerKey string, event *proto.WorkflowEvent) ([]*proto.BoundWorkflowEventTrigger, error) {
	if b.state == nil {
		return nil, status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.matchTriggers(ctx, ownerKey, event)
}

func (b *temporalBackend) deleteTriggerFromIndex(ctx context.Context, key, id string) error {
	var out bool
	return b.updateIndex(ctx, key, updateDeleteTrigger, []any{id}, &out)
}

func (b *temporalBackend) deleteTriggerIndex(ctx context.Context, id string) (bool, error) {
	if b.state == nil {
		return false, status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.deleteTrigger(ctx, id)
}

func (b *temporalBackend) putExecutionRefInIndex(ctx context.Context, key string, ref *proto.WorkflowExecutionReference) error {
	if b.state == nil {
		return status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.putExecutionRef(ctx, ref)
}

func (b *temporalBackend) putExecutionRefIndex(ctx context.Context, ref *proto.WorkflowExecutionReference) error {
	if b.state == nil {
		return status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.putExecutionRef(ctx, ref)
}

func (b *temporalBackend) getExecutionRefIndex(ctx context.Context, id string) (*proto.WorkflowExecutionReference, bool, error) {
	if b.state == nil {
		return nil, false, status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.getExecutionRef(ctx, id)
}

func (b *temporalBackend) listExecutionRefsIndex(ctx context.Context, subjectID string) ([]*proto.WorkflowExecutionReference, error) {
	if b.state == nil {
		return nil, status.Error(codes.FailedPrecondition, "temporal workflow state store is not configured")
	}
	return b.state.listExecutionRefs(ctx, subjectID)
}

func (b *temporalBackend) deleteExecutionRefFromIndex(ctx context.Context, key, id string) error {
	var out bool
	return b.updateIndex(ctx, key, updateDeleteRef, []any{id}, &out)
}

func triggerPrimaryIndexKey(id string) string {
	return "trigger:" + id
}

func triggerMatchIndexKey(key string) string {
	return "trigger-match:" + key
}

func executionRefPrimaryIndexKey(id string) string {
	return "ref:" + id
}

func executionRefSubjectIndexKey(subjectID string) string {
	return "subject:" + subjectID
}

func isTemporalUpdateLimitError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "limit on the total number of distinct updates")
}

func isNotFoundLike(err error) bool {
	if err == nil {
		return false
	}
	return isNotFound(err) || strings.Contains(strings.ToLower(err.Error()), "not found")
}

func isQueryHandlerUnavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unknown query") || strings.Contains(msg, "query handler")
}
