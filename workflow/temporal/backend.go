package temporal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type workflowHost interface {
	InvokeOperation(context.Context, *proto.InvokeWorkflowOperationRequest) (*proto.InvokeWorkflowOperationResponse, error)
	Close() error
}

type temporalBackend struct {
	providerName string
	cfg          config
	client       client.Client
	host         workflowHost

	mu      sync.Mutex
	started bool
	worker  worker.Worker
}

func newTemporalBackend(providerName string, cfg config, tc client.Client, host workflowHost) *temporalBackend {
	return &temporalBackend{
		providerName: strings.TrimSpace(providerName),
		cfg:          cfg,
		client:       tc,
		host:         host,
	}
}

func (b *temporalBackend) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.started {
		return nil
	}
	w := worker.New(b.client, b.cfg.TaskQueue, worker.Options{})
	w.RegisterWorkflow(gestaltRunWorkflow)
	w.RegisterWorkflow(indexWorkflow)
	w.RegisterWorkflow(scopeMetadataWorkflow)
	w.RegisterActivity(&workflowActivities{host: b.host})
	if err := w.Start(); err != nil {
		return fmt.Errorf("start temporal worker: %w", err)
	}
	if err := b.ensureScope(ctx); err != nil {
		w.Stop()
		return err
	}
	b.worker = w
	b.started = true
	return nil
}

func (b *temporalBackend) Close() error {
	b.mu.Lock()
	w := b.worker
	b.worker = nil
	b.started = false
	b.mu.Unlock()
	if w != nil {
		w.Stop()
	}
	var errs []error
	if b.host != nil {
		errs = append(errs, b.host.Close())
	}
	if b.client != nil {
		b.client.Close()
	}
	return errors.Join(errs...)
}

func (b *temporalBackend) HealthCheck(ctx context.Context) error {
	if b.client == nil {
		return errors.New("temporal workflow: client is not configured")
	}
	_, err := b.client.CheckHealth(ctx, &client.CheckHealthRequest{})
	if err != nil {
		return fmt.Errorf("temporal health check: %w", err)
	}
	return nil
}

func (b *temporalBackend) StartRun(ctx context.Context, req *proto.StartWorkflowProviderRunRequest) (*proto.BoundWorkflowRun, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	target, err := normalizeTarget(req.GetTarget())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	key := strings.TrimSpace(req.GetIdempotencyKey())
	workflowKey := strings.TrimSpace(req.GetWorkflowKey())
	if key != "" {
		if existing, found, err := b.getIdempotency(ctx, target.OwnerKey, key); err != nil {
			return nil, err
		} else if found && existing.GetRun() != nil {
			return cloneRun(existing.GetRun()), nil
		}
	}
	if workflowKey != "" {
		active, found, err := b.getWorkflowKey(ctx, workflowKey)
		if err != nil {
			return nil, err
		}
		if found && !workflowRunTerminal(active.GetStatus()) {
			return nil, status.Errorf(codes.FailedPrecondition, "workflow key %q already has an active run", workflowKey)
		}
	}
	temporalWorkflowID := workflowID(b.cfg.ScopeID, "run", uuid.NewString())
	if key != "" {
		temporalWorkflowID = workflowID(b.cfg.ScopeID, "manual", target.OwnerKey, key)
	}
	if workflowKey != "" {
		temporalWorkflowID = workflowID(b.cfg.ScopeID, "key", workflowKey)
	}
	reusePolicy := enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
	if workflowKey != "" {
		reusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	}
	input := runWorkflowOptions{
		ProviderName:                  b.providerName,
		ScopeID:                       b.cfg.ScopeID,
		IndexShardCount:               b.cfg.IndexShardCount,
		ExecutionRef:                  strings.TrimSpace(req.GetExecutionRef()),
		WorkflowKey:                   workflowKey,
		ActivityStartToCloseTimeoutNS: b.cfg.ActivityStartToCloseTimeout,
		RequireSignal:                 false,
	}
	run, err := b.client.ExecuteWorkflow(ctx, b.runStartOptions(temporalWorkflowID, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL, reusePolicy), gestaltRunWorkflow, input, target.Target, newManualTrigger(), cloneActor(req.GetCreatedBy()))
	if err != nil {
		if key != "" {
			if existing, found, getErr := b.getIdempotency(ctx, target.OwnerKey, key); getErr == nil && found && existing.GetRun() != nil {
				return cloneRun(existing.GetRun()), nil
			}
		}
		return nil, status.Errorf(codes.Internal, "start temporal workflow: %v", err)
	}
	now := time.Now().UTC()
	resp := &proto.BoundWorkflowRun{
		Id:           publicRunID(run.GetID(), run.GetRunID()),
		Status:       proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:       cloneTarget(target.Target),
		Trigger:      newManualTrigger(),
		CreatedAt:    timestamppb.New(now),
		CreatedBy:    cloneActor(req.GetCreatedBy()),
		ExecutionRef: strings.TrimSpace(req.GetExecutionRef()),
		WorkflowKey:  workflowKey,
	}
	if err := b.putRun(ctx, resp); err != nil {
		return nil, err
	}
	if workflowKey != "" {
		if err := b.putWorkflowKey(ctx, workflowKey, resp); err != nil {
			return nil, err
		}
	}
	if key != "" {
		if err := b.putIdempotency(ctx, target.OwnerKey, key, &proto.SignalWorkflowRunResponse{Run: resp}); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (b *temporalBackend) GetRun(ctx context.Context, req *proto.GetWorkflowProviderRunRequest) (*proto.BoundWorkflowRun, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	runID := strings.TrimSpace(req.GetRunId())
	if runID == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	run, found, err := b.getRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow run %q not found", runID)
	}
	return run, nil
}

func (b *temporalBackend) ListRuns(ctx context.Context, _ *proto.ListWorkflowProviderRunsRequest) (*proto.ListWorkflowProviderRunsResponse, error) {
	runs, err := b.listRuns(ctx)
	if err != nil {
		return nil, err
	}
	sortRuns(runs)
	return &proto.ListWorkflowProviderRunsResponse{Runs: runs}, nil
}

func (b *temporalBackend) CancelRun(ctx context.Context, req *proto.CancelWorkflowProviderRunRequest) (*proto.BoundWorkflowRun, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	handle, err := decodeRunHandle(req.GetRunId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	reason := strings.TrimSpace(req.GetReason())
	if reason == "" {
		reason = "canceled"
	}
	update, err := b.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   handle.WorkflowID,
		RunID:        handle.RunID,
		UpdateID:     "cancel:" + hashID(handle.WorkflowID, handle.RunID, reason),
		UpdateName:   updateCancelRun,
		Args:         []any{reason},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cancel temporal workflow: %v", err)
	}
	var run proto.BoundWorkflowRun
	if err := update.Get(ctx, &run); err != nil {
		return nil, mapWorkflowUpdateError(err)
	}
	if err := b.putRun(ctx, &run); err != nil {
		return nil, err
	}
	return &run, nil
}

func (b *temporalBackend) SignalRun(ctx context.Context, req *proto.SignalWorkflowProviderRunRequest) (*proto.SignalWorkflowRunResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	handle, err := decodeRunHandle(req.GetRunId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	signal, err := normalizeWorkflowSignal(req.GetSignal(), time.Now().UTC())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	run, found, err := b.getRun(ctx, req.GetRunId())
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow run %q not found", req.GetRunId())
	}
	ownerKey := targetOwnerKey(run.GetTarget())
	if signal.GetIdempotencyKey() != "" {
		if existing, found, err := b.getIdempotency(ctx, ownerKey, signal.GetIdempotencyKey()); err != nil {
			return nil, err
		} else if found && existing.GetSignal() != nil {
			return cloneSignalResponse(existing), nil
		}
	}
	updateID := signalUpdateID(signal)
	update, err := b.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   handle.WorkflowID,
		RunID:        handle.RunID,
		UpdateID:     updateID,
		UpdateName:   updateAddSignal,
		Args:         []any{signal},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "signal temporal workflow: %v", err)
	}
	var resp proto.SignalWorkflowRunResponse
	if err := update.Get(ctx, &resp); err != nil {
		return nil, mapWorkflowUpdateError(err)
	}
	if err := b.putRun(ctx, resp.GetRun()); err != nil {
		return nil, err
	}
	if signal.GetIdempotencyKey() != "" {
		if err := b.putIdempotency(ctx, ownerKey, signal.GetIdempotencyKey(), &resp); err != nil {
			return nil, err
		}
	}
	return &resp, nil
}

func (b *temporalBackend) SignalOrStartRun(ctx context.Context, req *proto.SignalOrStartWorkflowProviderRunRequest) (*proto.SignalWorkflowRunResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	workflowKey := strings.TrimSpace(req.GetWorkflowKey())
	if workflowKey == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow_key is required")
	}
	target, err := normalizeTarget(req.GetTarget())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	signal, err := normalizeWorkflowSignal(req.GetSignal(), time.Now().UTC())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ownerKey := target.OwnerKey
	if signal.GetIdempotencyKey() != "" {
		if existing, found, err := b.getIdempotency(ctx, ownerKey, signal.GetIdempotencyKey()); err != nil {
			return nil, err
		} else if found && existing.GetSignal() != nil {
			return cloneSignalResponse(existing), nil
		}
	}
	temporalWorkflowID := workflowID(b.cfg.ScopeID, "key", workflowKey)
	input := runWorkflowOptions{
		ProviderName:                  b.providerName,
		ScopeID:                       b.cfg.ScopeID,
		IndexShardCount:               b.cfg.IndexShardCount,
		ExecutionRef:                  strings.TrimSpace(req.GetExecutionRef()),
		WorkflowKey:                   workflowKey,
		ActivityStartToCloseTimeoutNS: b.cfg.ActivityStartToCloseTimeout,
		RequireSignal:                 true,
	}
	var resp proto.SignalWorkflowRunResponse
	for attempt := 0; attempt < 3; attempt++ {
		startOp := b.client.NewWithStartWorkflowOperation(
			b.runStartOptions(temporalWorkflowID, enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING, enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE),
			gestaltRunWorkflow,
			input,
			target.Target,
			newManualTrigger(),
			cloneActor(req.GetCreatedBy()),
		)
		update, err := b.client.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
			StartWorkflowOperation: startOp,
			UpdateOptions: client.UpdateWorkflowOptions{
				UpdateID:     signalUpdateID(signal),
				UpdateName:   updateAddSignal,
				Args:         []any{signal},
				WaitForStage: client.WorkflowUpdateStageCompleted,
			},
		})
		if err != nil {
			if attempt < 2 {
				continue
			}
			return nil, status.Errorf(codes.Internal, "signal-or-start temporal workflow: %v", err)
		}
		if err := update.Get(ctx, &resp); err != nil {
			if attempt < 2 && retrySignalOrStart(ctx, err) {
				continue
			}
			return nil, mapWorkflowUpdateError(err)
		}
		break
	}
	if resp.GetRun() == nil {
		return nil, status.Error(codes.Internal, "signal-or-start returned no run")
	}
	if err := b.putRun(ctx, resp.GetRun()); err != nil {
		return nil, err
	}
	if err := b.putWorkflowKey(ctx, workflowKey, resp.GetRun()); err != nil {
		return nil, err
	}
	if signal.GetIdempotencyKey() != "" {
		if err := b.putIdempotency(ctx, ownerKey, signal.GetIdempotencyKey(), &resp); err != nil {
			return nil, err
		}
	}
	return &resp, nil
}

func (b *temporalBackend) UpsertSchedule(ctx context.Context, req *proto.UpsertWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	target, err := normalizeTarget(req.GetTarget())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	scheduleID := strings.TrimSpace(req.GetScheduleId())
	if scheduleID == "" {
		scheduleID = uuid.NewString()
	}
	cron := strings.TrimSpace(req.GetCron())
	if cron == "" {
		return nil, status.Error(codes.InvalidArgument, "cron is required")
	}
	timezone := strings.TrimSpace(req.GetTimezone())
	if timezone == "" {
		timezone = defaultTimezone
	}
	now := time.Now().UTC()
	existing, found, err := b.getScheduleIndex(ctx, scheduleID)
	if err != nil {
		return nil, err
	}
	createdAt := now
	createdBy := cloneActor(req.GetRequestedBy())
	if found {
		createdAt = existing.GetCreatedAt().AsTime()
		createdBy = createdByForUpsert(existing.GetCreatedBy(), req.GetRequestedBy())
	}
	schedule := &proto.BoundWorkflowSchedule{
		Id:           scheduleID,
		Cron:         cron,
		Timezone:     timezone,
		Target:       cloneTarget(target.Target),
		Paused:       req.GetPaused(),
		CreatedAt:    timestamppb.New(createdAt),
		UpdatedAt:    timestamppb.New(now),
		CreatedBy:    createdBy,
		ExecutionRef: strings.TrimSpace(req.GetExecutionRef()),
	}
	if err := b.upsertTemporalSchedule(ctx, schedule); err != nil {
		return nil, err
	}
	if err := b.putScheduleIndex(ctx, schedule); err != nil {
		return nil, err
	}
	return schedule, nil
}

func (b *temporalBackend) GetSchedule(ctx context.Context, req *proto.GetWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	id := strings.TrimSpace(req.GetScheduleId())
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "schedule_id is required")
	}
	schedule, found, err := b.getScheduleIndex(ctx, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow schedule %q not found", id)
	}
	b.fillScheduleNextRun(ctx, schedule)
	return schedule, nil
}

func (b *temporalBackend) ListSchedules(ctx context.Context, _ *proto.ListWorkflowProviderSchedulesRequest) (*proto.ListWorkflowProviderSchedulesResponse, error) {
	schedules, err := b.listSchedulesIndex(ctx)
	if err != nil {
		return nil, err
	}
	for _, schedule := range schedules {
		b.fillScheduleNextRun(ctx, schedule)
	}
	sortSchedules(schedules)
	return &proto.ListWorkflowProviderSchedulesResponse{Schedules: schedules}, nil
}

func (b *temporalBackend) DeleteSchedule(ctx context.Context, req *proto.DeleteWorkflowProviderScheduleRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	id := strings.TrimSpace(req.GetScheduleId())
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "schedule_id is required")
	}
	if _, found, err := b.getScheduleIndex(ctx, id); err != nil {
		return nil, err
	} else if !found {
		return nil, status.Errorf(codes.NotFound, "workflow schedule %q not found", id)
	}
	err := b.client.ScheduleClient().GetHandle(ctx, b.temporalScheduleID(id)).Delete(ctx)
	if err != nil && !isNotFound(err) {
		return nil, status.Errorf(codes.Internal, "delete temporal schedule: %v", err)
	}
	if err := b.deleteScheduleIndex(ctx, id); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (b *temporalBackend) PauseSchedule(ctx context.Context, req *proto.PauseWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error) {
	if req == nil || strings.TrimSpace(req.GetScheduleId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "schedule_id is required")
	}
	id := strings.TrimSpace(req.GetScheduleId())
	schedule, found, err := b.getScheduleIndex(ctx, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow schedule %q not found", id)
	}
	if err := b.client.ScheduleClient().GetHandle(ctx, b.temporalScheduleID(id)).Pause(ctx, client.SchedulePauseOptions{Note: "paused by Gestalt"}); err != nil {
		return nil, status.Errorf(codes.Internal, "pause temporal schedule: %v", err)
	}
	schedule.Paused = true
	schedule.UpdatedAt = timestamppb.Now()
	if err := b.putScheduleIndex(ctx, schedule); err != nil {
		return nil, err
	}
	return schedule, nil
}

func (b *temporalBackend) ResumeSchedule(ctx context.Context, req *proto.ResumeWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error) {
	if req == nil || strings.TrimSpace(req.GetScheduleId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "schedule_id is required")
	}
	id := strings.TrimSpace(req.GetScheduleId())
	schedule, found, err := b.getScheduleIndex(ctx, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow schedule %q not found", id)
	}
	if err := b.client.ScheduleClient().GetHandle(ctx, b.temporalScheduleID(id)).Unpause(ctx, client.ScheduleUnpauseOptions{Note: "resumed by Gestalt"}); err != nil {
		return nil, status.Errorf(codes.Internal, "resume temporal schedule: %v", err)
	}
	schedule.Paused = false
	schedule.UpdatedAt = timestamppb.Now()
	if err := b.putScheduleIndex(ctx, schedule); err != nil {
		return nil, err
	}
	return schedule, nil
}

func (b *temporalBackend) UpsertEventTrigger(ctx context.Context, req *proto.UpsertWorkflowProviderEventTriggerRequest) (*proto.BoundWorkflowEventTrigger, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	target, err := normalizeTarget(req.GetTarget())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	triggerID := strings.TrimSpace(req.GetTriggerId())
	if triggerID == "" {
		triggerID = uuid.NewString()
	}
	match := req.GetMatch()
	if strings.TrimSpace(match.GetType()) == "" {
		return nil, status.Error(codes.InvalidArgument, "match.type is required")
	}
	now := time.Now().UTC()
	existing, found, err := b.getTriggerIndex(ctx, triggerID)
	if err != nil {
		return nil, err
	}
	createdAt := now
	createdBy := cloneActor(req.GetRequestedBy())
	if found {
		createdAt = existing.GetCreatedAt().AsTime()
		createdBy = createdByForUpsert(existing.GetCreatedBy(), req.GetRequestedBy())
	}
	trigger := &proto.BoundWorkflowEventTrigger{
		Id:           triggerID,
		Match:        &proto.WorkflowEventMatch{Type: strings.TrimSpace(match.GetType()), Source: strings.TrimSpace(match.GetSource()), Subject: strings.TrimSpace(match.GetSubject())},
		Target:       cloneTarget(target.Target),
		Paused:       req.GetPaused(),
		CreatedAt:    timestamppb.New(createdAt),
		UpdatedAt:    timestamppb.New(now),
		CreatedBy:    createdBy,
		ExecutionRef: strings.TrimSpace(req.GetExecutionRef()),
	}
	if err := b.putTriggerIndex(ctx, trigger); err != nil {
		return nil, err
	}
	return trigger, nil
}

func (b *temporalBackend) GetEventTrigger(ctx context.Context, req *proto.GetWorkflowProviderEventTriggerRequest) (*proto.BoundWorkflowEventTrigger, error) {
	if req == nil || strings.TrimSpace(req.GetTriggerId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "trigger_id is required")
	}
	trigger, found, err := b.getTriggerIndex(ctx, strings.TrimSpace(req.GetTriggerId()))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow event trigger %q not found", req.GetTriggerId())
	}
	return trigger, nil
}

func (b *temporalBackend) ListEventTriggers(ctx context.Context, _ *proto.ListWorkflowProviderEventTriggersRequest) (*proto.ListWorkflowProviderEventTriggersResponse, error) {
	triggers, err := b.listTriggersIndex(ctx)
	if err != nil {
		return nil, err
	}
	sortTriggers(triggers)
	return &proto.ListWorkflowProviderEventTriggersResponse{Triggers: triggers}, nil
}

func (b *temporalBackend) DeleteEventTrigger(ctx context.Context, req *proto.DeleteWorkflowProviderEventTriggerRequest) (*emptypb.Empty, error) {
	if req == nil || strings.TrimSpace(req.GetTriggerId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "trigger_id is required")
	}
	found, err := b.deleteTriggerIndex(ctx, strings.TrimSpace(req.GetTriggerId()))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow event trigger %q not found", req.GetTriggerId())
	}
	return &emptypb.Empty{}, nil
}

func (b *temporalBackend) PauseEventTrigger(ctx context.Context, req *proto.PauseWorkflowProviderEventTriggerRequest) (*proto.BoundWorkflowEventTrigger, error) {
	if req == nil || strings.TrimSpace(req.GetTriggerId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "trigger_id is required")
	}
	return b.setTriggerPaused(ctx, strings.TrimSpace(req.GetTriggerId()), true)
}

func (b *temporalBackend) ResumeEventTrigger(ctx context.Context, req *proto.ResumeWorkflowProviderEventTriggerRequest) (*proto.BoundWorkflowEventTrigger, error) {
	if req == nil || strings.TrimSpace(req.GetTriggerId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "trigger_id is required")
	}
	return b.setTriggerPaused(ctx, strings.TrimSpace(req.GetTriggerId()), false)
}

func (b *temporalBackend) PutExecutionReference(ctx context.Context, req *proto.PutWorkflowExecutionReferenceRequest) (*proto.WorkflowExecutionReference, error) {
	if req == nil || req.GetReference() == nil {
		return nil, status.Error(codes.InvalidArgument, "reference is required")
	}
	ref := cloneExecutionReference(req.GetReference())
	if strings.TrimSpace(ref.GetProviderName()) == "" {
		ref.ProviderName = b.providerName
	}
	ref, err := validateExecutionReference(ref)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	existing, found, err := b.getExecutionRefIndex(ctx, ref.GetId())
	if err != nil {
		return nil, err
	}
	if found && existing.GetCreatedAt() != nil && existing.GetCreatedAt().IsValid() {
		ref.CreatedAt = existing.GetCreatedAt()
	}
	if ref.GetCreatedAt() == nil || !ref.GetCreatedAt().IsValid() {
		ref.CreatedAt = timestamppb.Now()
	}
	if err := b.putExecutionRefIndex(ctx, ref); err != nil {
		return nil, err
	}
	return ref, nil
}

func (b *temporalBackend) GetExecutionReference(ctx context.Context, req *proto.GetWorkflowExecutionReferenceRequest) (*proto.WorkflowExecutionReference, error) {
	if req == nil || strings.TrimSpace(req.GetId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	ref, found, err := b.getExecutionRefIndex(ctx, strings.TrimSpace(req.GetId()))
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow execution reference %q not found", req.GetId())
	}
	return ref, nil
}

func (b *temporalBackend) ListExecutionReferences(ctx context.Context, req *proto.ListWorkflowExecutionReferencesRequest) (*proto.ListWorkflowExecutionReferencesResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	refs, err := b.listExecutionRefsIndex(ctx, strings.TrimSpace(req.GetSubjectId()))
	if err != nil {
		return nil, err
	}
	sortReferences(refs)
	return &proto.ListWorkflowExecutionReferencesResponse{References: refs}, nil
}

func (b *temporalBackend) PublishEvent(ctx context.Context, req *proto.PublishWorkflowProviderEventRequest) (*emptypb.Empty, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	pluginName := strings.TrimSpace(req.GetPluginName())
	event, err := normalizeWorkflowEvent(req.GetEvent(), time.Now)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	triggers, err := b.matchTriggersIndex(ctx, pluginName, event)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	publishedBy := cloneActor(req.GetPublishedBy())
	for _, trigger := range triggers {
		if !eventMatchesTrigger(event, trigger) {
			continue
		}
		createdBy := cloneActor(trigger.GetCreatedBy())
		executionRef := strings.TrimSpace(trigger.GetExecutionRef())
		if actorHasSubject(publishedBy) {
			createdBy = cloneActor(publishedBy)
		}
		temporalWorkflowID := workflowID(b.cfg.ScopeID, "event", trigger.GetId(), event.GetSource(), uuid.NewString())
		if event.GetId() != "" {
			temporalWorkflowID = workflowID(b.cfg.ScopeID, "event", trigger.GetId(), event.GetSource(), event.GetId())
		}
		if actorHasSubject(publishedBy) {
			ref, err := publishedEventExecutionReference(b.providerName, temporalWorkflowID, trigger, publishedBy, now)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "build event execution reference: %v", err)
			}
			if ref != nil {
				if stored, err := b.PutExecutionReference(ctx, &proto.PutWorkflowExecutionReferenceRequest{Reference: ref}); err == nil && stored != nil {
					executionRef = stored.GetId()
				}
			}
		}
		input := runWorkflowOptions{
			ProviderName:                  b.providerName,
			ScopeID:                       b.cfg.ScopeID,
			IndexShardCount:               b.cfg.IndexShardCount,
			ExecutionRef:                  executionRef,
			ActivityStartToCloseTimeoutNS: b.cfg.ActivityStartToCloseTimeout,
		}
		run, err := b.client.ExecuteWorkflow(ctx, b.runStartOptions(temporalWorkflowID, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE), gestaltRunWorkflow, input, cloneTarget(trigger.GetTarget()), eventTrigger(trigger.GetId(), event), createdBy)
		if err != nil {
			if event.GetId() != "" && isAlreadyStarted(err) {
				continue
			}
			return nil, status.Errorf(codes.Internal, "start event workflow: %v", err)
		}
		publicID := publicRunID(run.GetID(), run.GetRunID())
		if err := b.putRun(ctx, &proto.BoundWorkflowRun{
			Id:           publicID,
			Status:       proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
			Target:       cloneTarget(trigger.GetTarget()),
			Trigger:      eventTrigger(trigger.GetId(), event),
			CreatedAt:    timestamppb.New(now),
			CreatedBy:    createdBy,
			ExecutionRef: executionRef,
		}); err != nil {
			return nil, err
		}
	}
	return &emptypb.Empty{}, nil
}

func (b *temporalBackend) setTriggerPaused(ctx context.Context, id string, paused bool) (*proto.BoundWorkflowEventTrigger, error) {
	trigger, found, err := b.getTriggerIndex(ctx, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow event trigger %q not found", id)
	}
	trigger.Paused = paused
	trigger.UpdatedAt = timestamppb.Now()
	if err := b.putTriggerIndex(ctx, trigger); err != nil {
		return nil, err
	}
	return trigger, nil
}

func (b *temporalBackend) upsertTemporalSchedule(ctx context.Context, schedule *proto.BoundWorkflowSchedule) error {
	actionInput := runWorkflowOptions{
		ProviderName:                  b.providerName,
		ScopeID:                       b.cfg.ScopeID,
		IndexShardCount:               b.cfg.IndexShardCount,
		ExecutionRef:                  strings.TrimSpace(schedule.GetExecutionRef()),
		ActivityStartToCloseTimeoutNS: b.cfg.ActivityStartToCloseTimeout,
		ScheduleID:                    schedule.GetId(),
	}
	action := &client.ScheduleWorkflowAction{
		ID:                  workflowID(b.cfg.ScopeID, "schedule-run", schedule.GetId()),
		Workflow:            gestaltRunWorkflow,
		Args:                []any{actionInput, cloneTarget(schedule.GetTarget()), scheduleTrigger(schedule.GetId(), time.Now().UTC()), cloneActor(schedule.GetCreatedBy())},
		TaskQueue:           b.cfg.TaskQueue,
		WorkflowRunTimeout:  b.cfg.WorkflowRunTimeout,
		WorkflowTaskTimeout: b.cfg.WorkflowTaskTimeout,
	}
	temporalID := b.temporalScheduleID(schedule.GetId())
	spec := client.ScheduleSpec{
		CronExpressions: []string{schedule.GetCron()},
		TimeZoneName:    schedule.GetTimezone(),
	}
	handle := b.client.ScheduleClient().GetHandle(ctx, temporalID)
	_, err := handle.Describe(ctx)
	if err != nil {
		if !isNotFound(err) {
			return status.Errorf(codes.Internal, "describe temporal schedule: %v", err)
		}
		_, err = b.client.ScheduleClient().Create(ctx, client.ScheduleOptions{
			ID:            temporalID,
			Spec:          spec,
			Action:        action,
			Overlap:       enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
			CatchupWindow: b.cfg.ScheduleCatchupWindow,
			Paused:        schedule.GetPaused(),
		})
		if err != nil {
			return status.Errorf(codes.Internal, "create temporal schedule: %v", err)
		}
		return nil
	}
	err = handle.Update(ctx, client.ScheduleUpdateOptions{DoUpdate: func(input client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
		return &client.ScheduleUpdate{Schedule: &client.Schedule{
			Action: action,
			Spec:   &spec,
			Policy: &client.SchedulePolicies{
				Overlap:       enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
				CatchupWindow: b.cfg.ScheduleCatchupWindow,
			},
			State: &client.ScheduleState{Paused: schedule.GetPaused()},
		}}, nil
	}})
	if err != nil {
		return status.Errorf(codes.Internal, "update temporal schedule: %v", err)
	}
	return nil
}

func (b *temporalBackend) fillScheduleNextRun(ctx context.Context, schedule *proto.BoundWorkflowSchedule) {
	desc, err := b.client.ScheduleClient().GetHandle(ctx, b.temporalScheduleID(schedule.GetId())).Describe(ctx)
	if err != nil || len(desc.Info.NextActionTimes) == 0 {
		return
	}
	schedule.NextRunAt = timestamppb.New(desc.Info.NextActionTimes[0].UTC())
}

func (b *temporalBackend) temporalScheduleID(scheduleID string) string {
	return workflowID(b.cfg.ScopeID, "schedule", scheduleID)
}

func (b *temporalBackend) startOptions(workflowID string) client.StartWorkflowOptions {
	return client.StartWorkflowOptions{
		ID:                                       workflowID,
		TaskQueue:                                b.cfg.TaskQueue,
		WorkflowIDConflictPolicy:                 enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowIDReusePolicy:                    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowExecutionErrorWhenAlreadyStarted: false,
		WorkflowTaskTimeout:                      b.cfg.WorkflowTaskTimeout,
	}
}

func (b *temporalBackend) runStartOptions(workflowID string, conflict enumspb.WorkflowIdConflictPolicy, reuse enumspb.WorkflowIdReusePolicy) client.StartWorkflowOptions {
	opts := b.startOptions(workflowID)
	opts.WorkflowIDConflictPolicy = conflict
	opts.WorkflowIDReusePolicy = reuse
	opts.WorkflowRunTimeout = b.cfg.WorkflowRunTimeout
	return opts
}

func mapWorkflowUpdateError(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "not found"):
		return status.Error(codes.NotFound, msg)
	case strings.Contains(msg, "failed_precondition"):
		return status.Error(codes.FailedPrecondition, msg)
	case strings.Contains(msg, "invalid_argument"):
		return status.Error(codes.InvalidArgument, msg)
	default:
		return status.Errorf(codes.Internal, "temporal workflow update: %v", err)
	}
}

func isNotFound(err error) bool {
	var notFound *serviceerror.NotFound
	return errors.As(err, &notFound)
}

func isAlreadyStarted(err error) bool {
	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	return errors.As(err, &alreadyStarted) || strings.Contains(err.Error(), "already started")
}

func workflowRunTerminal(status proto.WorkflowRunStatus) bool {
	switch status {
	case proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED,
		proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED,
		proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_CANCELED:
		return true
	default:
		return false
	}
}

func signalUpdateID(signal *proto.WorkflowSignal) string {
	if signal.GetIdempotencyKey() != "" {
		return "signal-key:" + hashID(signal.GetName(), signal.GetIdempotencyKey())
	}
	if signal.GetId() != "" {
		return "signal-id:" + hashID(signal.GetId())
	}
	return "signal:" + uuid.NewString()
}

func retrySignalOrStart(ctx context.Context, err error) bool {
	msg := err.Error()
	if !strings.Contains(msg, "not found") && !strings.Contains(msg, "failed_precondition") {
		return false
	}
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func cloneSignalResponse(resp *proto.SignalWorkflowRunResponse) *proto.SignalWorkflowRunResponse {
	if resp == nil {
		return nil
	}
	return &proto.SignalWorkflowRunResponse{
		Run:         cloneRun(resp.GetRun()),
		Signal:      cloneSignal(resp.GetSignal()),
		StartedRun:  resp.GetStartedRun(),
		WorkflowKey: strings.TrimSpace(resp.GetWorkflowKey()),
	}
}

var _ workflowBackend = (*temporalBackend)(nil)
var _ workflowHost = (*gestalt.WorkflowHostClient)(nil)
