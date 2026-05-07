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
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const workflowScheduleMemoKey = "gestalt.workflow_schedule"

type workflowHost interface {
	InvokeOperation(context.Context, *proto.InvokeWorkflowOperationRequest) (*proto.InvokeWorkflowOperationResponse, error)
	Close() error
}

type temporalBackend struct {
	providerName string
	cfg          config
	client       client.Client
	host         workflowHost
	state        *workflowStateStore

	mu      sync.Mutex
	started bool
	worker  worker.Worker
}

func newTemporalBackend(providerName string, cfg config, tc client.Client, host workflowHost, state *workflowStateStore) *temporalBackend {
	return &temporalBackend{
		providerName: strings.TrimSpace(providerName),
		cfg:          cfg,
		client:       tc,
		host:         host,
		state:        state,
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
	w.RegisterWorkflow(gestaltRunWorkflowV3)
	w.RegisterWorkflow(gestaltWorkflowKeyLaneV1)
	w.RegisterWorkflow(gestaltOwnerLedgerWorkflow)
	w.RegisterWorkflow(indexWorkflow)
	w.RegisterWorkflow(scopeMetadataWorkflow)
	w.RegisterActivity(&workflowActivities{host: b.host})
	if err := w.Start(); err != nil {
		return fmt.Errorf("start temporal worker: %w", err)
	}
	if err := b.deleteDeprecatedTemporalIndexState(ctx); err != nil {
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
	if b.state != nil {
		errs = append(errs, b.state.Close())
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
	fingerprint := startFingerprint(target.OwnerKey, key, workflowKey, req.GetExecutionRef(), target.Target, req.GetCreatedBy())
	ledgerKey := startLedgerKey(target.OwnerKey, key)
	if key != "" {
		entry, _, err := b.reserveLedger(ctx, ledgerKey, ownerLedgerReserveRequest{
			Operation:   "start",
			Fingerprint: fingerprint,
			OwnerKey:    target.OwnerKey,
			WorkflowKey: workflowKey,
		})
		if err != nil {
			return nil, err
		}
		if entry != nil && entry.Status == "completed" {
			if run := runFromPayload(entry.RunPayload); run != nil {
				return run, nil
			}
		}
	}
	if workflowKey != "" {
		updateID := "start:" + uuid.NewString()
		if key != "" {
			updateID = "start:" + hashID(target.OwnerKey, key, workflowKey)
		}
		run, err := b.startKeyedRunInLane(ctx, workflowKey, updateID, laneStartRequest{
			OwnerKey:         target.OwnerKey,
			TargetPayload:    protoPayload(target.Target),
			ExecutionRef:     strings.TrimSpace(req.GetExecutionRef()),
			CreatedByPayload: protoPayload(req.GetCreatedBy()),
			RequestID:        updateID,
		})
		if err != nil {
			return nil, err
		}
		if key != "" {
			if err := b.completeLedger(ctx, ledgerKey, fingerprint, &proto.SignalWorkflowRunResponse{Run: run}, run); err != nil {
				return nil, err
			}
		}
		return run, nil
	}
	temporalWorkflowID := workflowID(b.cfg.ScopeID, "run-v3", uuid.NewString())
	if key != "" {
		temporalWorkflowID = workflowID(b.cfg.ScopeID, "manual-v3", target.OwnerKey, key)
	}
	conflictPolicy := enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL
	if key != "" {
		conflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
	}
	run, err := b.executeRunV3(ctx, temporalWorkflowID, b.runV3Input(target.OwnerKey, req.GetExecutionRef(), "", target.Target, newManualTrigger(), req.GetCreatedBy(), false), conflictPolicy, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
	if err != nil {
		return nil, err
	}
	if key != "" {
		if err := b.completeLedger(ctx, ledgerKey, fingerprint, &proto.SignalWorkflowRunResponse{Run: run}, run); err != nil {
			return nil, err
		}
	}
	return run, nil
}

func (b *temporalBackend) GetRun(ctx context.Context, req *proto.GetWorkflowProviderRunRequest) (*proto.BoundWorkflowRun, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	runID := strings.TrimSpace(req.GetRunId())
	if runID == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	handle, err := decodeV3RunHandle(runID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return b.queryOrGetV3Run(ctx, handle)
}

func (b *temporalBackend) ListRuns(ctx context.Context, _ *proto.ListWorkflowProviderRunsRequest) (*proto.ListWorkflowProviderRunsResponse, error) {
	runs, err := b.listRunsFromTemporalIndex(ctx)
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
	handle, err := decodeV3RunHandle(req.GetRunId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	reason := strings.TrimSpace(req.GetReason())
	if reason == "" {
		reason = "canceled"
	}
	if handle.LaneWorkflowID != "" {
		return b.cancelLaneRun(ctx, handle, reason)
	}
	update, err := b.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   handle.RunWorkflowID,
		RunID:        handle.RunTemporalRunID,
		UpdateID:     "cancel:" + hashID(handle.RunWorkflowID, handle.RunTemporalRunID, reason),
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
	return &run, nil
}

func (b *temporalBackend) SignalRun(ctx context.Context, req *proto.SignalWorkflowProviderRunRequest) (*proto.SignalWorkflowRunResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	handle, err := decodeV3RunHandle(req.GetRunId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	signal, err := normalizeWorkflowSignal(req.GetSignal(), time.Now().UTC())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ownerKey := handle.OwnerKey
	updateID := signalUpdateID(signal)
	ledgerKey := ownerIdempotencyLedgerKey(ownerKey, signal.GetIdempotencyKey())
	fingerprint := signalFingerprint(ownerKey, handle.WorkflowKey+"\x00"+req.GetRunId(), signal)
	if ledgerKey != "" {
		entry, _, err := b.reserveLedger(ctx, ledgerKey, ownerLedgerReserveRequest{
			Operation:      "signal",
			Fingerprint:    fingerprint,
			OwnerKey:       ownerKey,
			WorkflowKey:    handle.WorkflowKey,
			RunID:          req.GetRunId(),
			SignalID:       signal.GetId(),
			LaneWorkflowID: handle.LaneWorkflowID,
			LaneUpdateID:   updateID,
		})
		if err != nil {
			return nil, err
		}
		if entry != nil && entry.Status == "completed" {
			if resp := signalResponseFromPayload(entry.ResponsePayload); resp != nil {
				return resp, nil
			}
		}
	}
	if sigKey := explicitSignalLedgerKey(signal); sigKey != "" {
		if entry, _, err := b.reserveLedger(ctx, sigKey, ownerLedgerReserveRequest{
			Operation:   "signal-id",
			Fingerprint: fingerprint,
			OwnerKey:    ownerKey,
			WorkflowKey: handle.WorkflowKey,
			RunID:       req.GetRunId(),
			SignalID:    signal.GetId(),
		}); err != nil {
			return nil, err
		} else if entry != nil && entry.Status == "completed" {
			if resp := signalResponseFromPayload(entry.ResponsePayload); resp != nil {
				return resp, nil
			}
		}
	}
	var resp *proto.SignalWorkflowRunResponse
	if handle.LaneWorkflowID != "" {
		resp, err = b.updateLaneSignalRun(ctx, handle, signal, updateID)
	} else {
		update, updateErr := b.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
			WorkflowID:   handle.RunWorkflowID,
			RunID:        handle.RunTemporalRunID,
			UpdateID:     updateID,
			UpdateName:   updateAddSignal,
			Args:         []any{signal},
			WaitForStage: client.WorkflowUpdateStageCompleted,
		})
		if updateErr != nil {
			return nil, status.Errorf(codes.Internal, "signal temporal workflow: %v", updateErr)
		}
		var out proto.SignalWorkflowRunResponse
		if updateErr := update.Get(ctx, &out); updateErr != nil {
			return nil, mapWorkflowUpdateError(updateErr)
		}
		resp = &out
	}
	if err != nil {
		return nil, err
	}
	if ledgerKey != "" {
		if err := b.completeLedger(ctx, ledgerKey, fingerprint, resp, resp.GetRun()); err != nil {
			return nil, err
		}
	}
	if sigKey := explicitSignalLedgerKey(signal); sigKey != "" {
		if err := b.completeLedger(ctx, sigKey, fingerprint, resp, resp.GetRun()); err != nil {
			return nil, err
		}
	}
	return resp, nil
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
	updateID := signalUpdateID(signal)
	fingerprint := signalFingerprint(ownerKey, workflowKey, signal)
	ledgerKey := ownerIdempotencyLedgerKey(ownerKey, signal.GetIdempotencyKey())
	if ledgerKey != "" {
		entry, _, err := b.reserveLedger(ctx, ledgerKey, ownerLedgerReserveRequest{
			Operation:      "signal_or_start",
			Fingerprint:    fingerprint,
			OwnerKey:       ownerKey,
			WorkflowKey:    workflowKey,
			SignalID:       signal.GetId(),
			LaneWorkflowID: b.laneWorkflowID(workflowKey),
			LaneUpdateID:   updateID,
		})
		if err != nil {
			return nil, err
		}
		if entry != nil && entry.Status == "completed" {
			if resp := signalResponseFromPayload(entry.ResponsePayload); resp != nil {
				return resp, nil
			}
		}
	}
	if sigKey := explicitSignalLedgerKey(signal); sigKey != "" {
		if entry, _, err := b.reserveLedger(ctx, sigKey, ownerLedgerReserveRequest{
			Operation:      "signal-id",
			Fingerprint:    fingerprint,
			OwnerKey:       ownerKey,
			WorkflowKey:    workflowKey,
			SignalID:       signal.GetId(),
			LaneWorkflowID: b.laneWorkflowID(workflowKey),
			LaneUpdateID:   updateID,
		}); err != nil {
			return nil, err
		} else if entry != nil && entry.Status == "completed" {
			if resp := signalResponseFromPayload(entry.ResponsePayload); resp != nil {
				return resp, nil
			}
		}
	}
	resp, err := b.signalOrStartLane(ctx, workflowKey, updateID, laneSignalRequest{
		OwnerKey:         ownerKey,
		TargetPayload:    protoPayload(target.Target),
		ExecutionRef:     strings.TrimSpace(req.GetExecutionRef()),
		CreatedByPayload: protoPayload(req.GetCreatedBy()),
		SignalPayload:    protoPayload(signal),
		RequestID:        updateID,
		IdempotencyKey:   strings.TrimSpace(signal.GetIdempotencyKey()),
	})
	if err != nil {
		return nil, err
	}
	if resp.GetRun() == nil {
		return nil, status.Error(codes.Internal, "signal-or-start returned no run")
	}
	if ledgerKey != "" {
		if err := b.completeLedger(ctx, ledgerKey, fingerprint, resp, resp.GetRun()); err != nil {
			return nil, err
		}
	}
	if sigKey := explicitSignalLedgerKey(signal); sigKey != "" {
		if err := b.completeLedger(ctx, sigKey, fingerprint, resp, resp.GetRun()); err != nil {
			return nil, err
		}
	}
	return resp, nil
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
		if temporalSchedule, temporalFound, temporalErr := b.describeTemporalSchedule(ctx, id); temporalErr == nil && temporalFound {
			return temporalSchedule, nil
		}
		return nil, err
	}
	if !found {
		if temporalSchedule, temporalFound, temporalErr := b.describeTemporalSchedule(ctx, id); temporalErr != nil {
			return nil, temporalErr
		} else if temporalFound {
			return temporalSchedule, nil
		}
		return nil, status.Errorf(codes.NotFound, "workflow schedule %q not found", id)
	}
	b.fillScheduleNextRun(ctx, schedule)
	return schedule, nil
}

func (b *temporalBackend) ListSchedules(ctx context.Context, _ *proto.ListWorkflowProviderSchedulesRequest) (*proto.ListWorkflowProviderSchedulesResponse, error) {
	schedules, err := b.listTemporalSchedules(ctx)
	if err != nil {
		return nil, err
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
	if _, found, err := b.getScheduleForMutation(ctx, id); err != nil {
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
	schedule, found, err := b.getScheduleForMutation(ctx, id)
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
	schedule, found, err := b.getScheduleForMutation(ctx, id)
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

func (b *temporalBackend) getScheduleForMutation(ctx context.Context, id string) (*proto.BoundWorkflowSchedule, bool, error) {
	schedule, found, err := b.getScheduleIndex(ctx, id)
	if err != nil {
		return nil, false, err
	}
	if found {
		return schedule, true, nil
	}
	return b.describeTemporalSchedule(ctx, id)
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
		temporalWorkflowID := eventRunWorkflowID(b.cfg.ScopeID, trigger.GetId(), event)
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
		input := b.runV3Input(targetOwnerKey(trigger.GetTarget()), executionRef, "", cloneTarget(trigger.GetTarget()), eventTrigger(trigger.GetId(), event), createdBy, false)
		_, err := b.executeRunV3(ctx, temporalWorkflowID, input, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
		if err != nil {
			if event.GetId() != "" && isAlreadyStarted(err) {
				continue
			}
			return nil, status.Errorf(codes.Internal, "start event workflow: %v", err)
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
	if err := b.ensureTemporalRunIndexes(ctx); err != nil {
		return err
	}
	scheduleMemo := map[string]interface{}{
		workflowScheduleMemoKey: cloneSchedule(schedule),
	}
	actionInput := b.runV3Input(targetOwnerKey(schedule.GetTarget()), schedule.GetExecutionRef(), "", cloneTarget(schedule.GetTarget()), scheduleTrigger(schedule.GetId(), time.Now().UTC()), cloneActor(schedule.GetCreatedBy()), false)
	actionInput.ScheduleID = schedule.GetId()
	action := &client.ScheduleWorkflowAction{
		Workflow:            gestaltRunWorkflowV3,
		Args:                []any{actionInput},
		TaskQueue:           b.cfg.TaskQueue,
		WorkflowRunTimeout:  b.cfg.WorkflowRunTimeout,
		WorkflowTaskTimeout: b.cfg.WorkflowTaskTimeout,
		Memo:                scheduleMemo,
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
			Memo:          scheduleMemo,
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

func (b *temporalBackend) listTemporalSchedules(ctx context.Context) ([]*proto.BoundWorkflowSchedule, error) {
	iter, err := b.client.ScheduleClient().List(ctx, client.ScheduleListOptions{PageSize: 1000})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list temporal schedules: %v", err)
	}
	prefix := b.temporalScheduleIDPrefix()
	var schedules []*proto.BoundWorkflowSchedule
	for iter.HasNext() {
		entry, err := iter.Next()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "list temporal schedules: %v", err)
		}
		if entry == nil || !strings.HasPrefix(strings.TrimSpace(entry.ID), prefix) {
			continue
		}
		schedule, found, err := b.describeTemporalScheduleByTemporalID(ctx, entry.ID, "")
		if err != nil {
			return nil, err
		}
		if found {
			schedules = append(schedules, schedule)
		}
	}
	return schedules, nil
}

func (b *temporalBackend) describeTemporalSchedule(ctx context.Context, scheduleID string) (*proto.BoundWorkflowSchedule, bool, error) {
	scheduleID = strings.TrimSpace(scheduleID)
	if scheduleID == "" {
		return nil, false, nil
	}
	return b.describeTemporalScheduleByTemporalID(ctx, b.temporalScheduleID(scheduleID), scheduleID)
}

func (b *temporalBackend) describeTemporalScheduleByTemporalID(ctx context.Context, temporalID, fallbackScheduleID string) (*proto.BoundWorkflowSchedule, bool, error) {
	temporalID = strings.TrimSpace(temporalID)
	if temporalID == "" {
		return nil, false, nil
	}
	desc, err := b.client.ScheduleClient().GetHandle(ctx, temporalID).Describe(ctx)
	if err != nil {
		if isNotFound(err) {
			return nil, false, nil
		}
		return nil, false, status.Errorf(codes.Internal, "describe temporal schedule: %v", err)
	}
	schedule, found, err := scheduleFromTemporalDescription(fallbackScheduleID, desc)
	if err != nil {
		return nil, false, status.Errorf(codes.Internal, "decode temporal schedule %q: %v", temporalID, err)
	}
	return schedule, found, nil
}

func scheduleFromTemporalDescription(fallbackScheduleID string, desc *client.ScheduleDescription) (*proto.BoundWorkflowSchedule, bool, error) {
	if desc == nil {
		return nil, false, nil
	}
	action, ok := desc.Schedule.Action.(*client.ScheduleWorkflowAction)
	if !ok || action == nil {
		return nil, false, nil
	}
	if schedule, found, err := scheduleFromTemporalActionMemo(action); found || err != nil {
		if schedule != nil && schedule.GetId() == "" {
			schedule.Id = strings.TrimSpace(fallbackScheduleID)
		}
		if schedule != nil {
			applyTemporalScheduleDescription(schedule, desc)
		}
		return schedule, found, err
	}
	schedule, found, err := scheduleFromTemporalActionArgs(fallbackScheduleID, action)
	if schedule != nil {
		applyTemporalScheduleDescription(schedule, desc)
	}
	return schedule, found, err
}

func scheduleFromTemporalActionMemo(action *client.ScheduleWorkflowAction) (*proto.BoundWorkflowSchedule, bool, error) {
	if action == nil || action.Memo == nil {
		return nil, false, nil
	}
	payload, ok := temporalPayload(action.Memo[workflowScheduleMemoKey])
	if !ok {
		return nil, false, nil
	}
	var schedule proto.BoundWorkflowSchedule
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &schedule); err != nil {
		return nil, false, err
	}
	return cloneSchedule(&schedule), schedule.GetId() != "", nil
}

func scheduleFromTemporalActionArgs(fallbackScheduleID string, action *client.ScheduleWorkflowAction) (*proto.BoundWorkflowSchedule, bool, error) {
	if action == nil {
		return nil, false, nil
	}
	if len(action.Args) != 1 {
		return nil, false, nil
	}
	payload, ok := temporalActionArg(action, 0)
	if !ok {
		return nil, false, nil
	}
	var input runWorkflowV3Input
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &input); err != nil {
		return nil, false, err
	}
	scheduleID := strings.TrimSpace(input.ScheduleID)
	if scheduleID == "" {
		scheduleID = strings.TrimSpace(fallbackScheduleID)
	}
	if scheduleID == "" {
		return nil, false, nil
	}
	return &proto.BoundWorkflowSchedule{
		Id:           scheduleID,
		Target:       targetFromPayload(input.TargetPayload),
		CreatedBy:    actorFromPayload(input.CreatedByPayload),
		ExecutionRef: strings.TrimSpace(input.ExecutionRef),
	}, true, nil
}

func applyTemporalScheduleDescription(schedule *proto.BoundWorkflowSchedule, desc *client.ScheduleDescription) {
	if schedule == nil || desc == nil {
		return
	}
	if spec := desc.Schedule.Spec; spec != nil {
		if schedule.GetCron() == "" && len(spec.CronExpressions) > 0 {
			schedule.Cron = spec.CronExpressions[0]
		}
		if schedule.GetTimezone() == "" {
			schedule.Timezone = strings.TrimSpace(spec.TimeZoneName)
		}
	}
	if state := desc.Schedule.State; state != nil {
		schedule.Paused = state.Paused
	}
	if !desc.Info.CreatedAt.IsZero() && schedule.GetCreatedAt() == nil {
		schedule.CreatedAt = timestamppb.New(desc.Info.CreatedAt.UTC())
	}
	if !desc.Info.LastUpdateAt.IsZero() {
		schedule.UpdatedAt = timestamppb.New(desc.Info.LastUpdateAt.UTC())
	}
	if len(desc.Info.NextActionTimes) > 0 {
		schedule.NextRunAt = timestamppb.New(desc.Info.NextActionTimes[0].UTC())
	}
}

func temporalActionArg(action *client.ScheduleWorkflowAction, index int) (*commonpb.Payload, bool) {
	if action == nil || index < 0 || index >= len(action.Args) {
		return nil, false
	}
	return temporalPayload(action.Args[index])
}

func temporalPayload(value interface{}) (*commonpb.Payload, bool) {
	payload, ok := value.(*commonpb.Payload)
	return payload, ok && payload != nil
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

func (b *temporalBackend) temporalScheduleIDPrefix() string {
	return "gestalt/" + scopeHash(b.cfg.ScopeID) + "/schedule/"
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
