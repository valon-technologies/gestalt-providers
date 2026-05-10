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
	sdkworkflow "go.temporal.io/sdk/workflow"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type workflowHost interface {
	InvokeOperation(context.Context, gestalt.InvokeWorkflowOperationInput) (*gestalt.InvokeWorkflowOperationResponse, error)
	Close() error
}

type temporalWorker interface {
	RegisterWorkflow(interface{})
	RegisterActivity(interface{})
	Start() error
	Stop()
}

type temporalWorkerFactory func(client.Client, string, worker.Options) temporalWorker

type temporalBackend struct {
	providerName string
	cfg          config
	client       client.Client
	host         workflowHost
	state        *workflowStateStore

	newWorker temporalWorkerFactory

	mu      sync.Mutex
	started bool
	worker  temporalWorker
}

func newTemporalBackend(providerName string, cfg config, tc client.Client, host workflowHost, state *workflowStateStore) *temporalBackend {
	return &temporalBackend{
		providerName: strings.TrimSpace(providerName),
		cfg:          cfg,
		client:       tc,
		host:         host,
		state:        state,
		newWorker:    defaultTemporalWorkerFactory,
	}
}

func defaultTemporalWorkerFactory(tc client.Client, taskQueue string, options worker.Options) temporalWorker {
	return worker.New(tc, taskQueue, options)
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
	w := b.newWorker(b.client, b.cfg.TaskQueue, b.workerOptions())
	w.RegisterWorkflow(gestaltRunWorkflowV4)
	w.RegisterActivity(&workflowActivities{host: b.host, state: b.state})
	if err := w.Start(); err != nil {
		return fmt.Errorf("start temporal worker: %w", err)
	}
	b.worker = w
	b.started = true
	return nil
}

func (b *temporalBackend) workerOptions() worker.Options {
	return worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			UseVersioning: true,
			Version: worker.WorkerDeploymentVersion{
				DeploymentName: b.cfg.Versioning.DeploymentName,
				BuildID:        b.cfg.Versioning.BuildID,
			},
			DefaultVersioningBehavior: sdkworkflow.VersioningBehaviorAutoUpgrade,
		},
	}
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

func (b *temporalBackend) StartRun(ctx context.Context, req *gestalt.StartWorkflowProviderRunRequest) (*gestalt.BoundWorkflowRunInput, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	targetProto, err := workflowTargetProto(req.Target)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	target, err := normalizeTarget(targetProto)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	key := strings.TrimSpace(req.IdempotencyKey)
	workflowKey := strings.TrimSpace(req.WorkflowKey)
	createdBy := workflowActorProto(req.CreatedBy)
	fingerprint := startFingerprint(target.OwnerKey, key, workflowKey, req.ExecutionRef, target.Target, createdBy)
	if key != "" && workflowKey == "" {
		return runInputFromProto(b.startUnkeyedRunV4(ctx, target, req, key, fingerprint))
	}
	if workflowKey != "" {
		return runInputFromProto(b.startKeyedRunV4(ctx, target, req, key, fingerprint))
	}
	temporalWorkflowID := workflowID(b.cfg.ScopeID, "run-v4", uuid.NewString())
	conflictPolicy := enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL
	run, err := b.executeRunV4(ctx, temporalWorkflowID, b.runV4Input(target.OwnerKey, req.ExecutionRef, "", target.Target, newManualTrigger(), createdBy, false), conflictPolicy, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
	if err != nil {
		return nil, err
	}
	return runInputFromProto(run, nil)
}

func (b *temporalBackend) GetRun(ctx context.Context, req *gestalt.GetWorkflowProviderRunRequest) (*gestalt.BoundWorkflowRunInput, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	runID := strings.TrimSpace(req.RunID)
	if runID == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	if _, err := decodeTemporalRunHandle(runID); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if run, found, err := b.state.getRun(ctx, runID); err != nil {
		return nil, status.Errorf(codes.Internal, "load workflow run projection: %v", err)
	} else if found {
		return runInputFromProto(run, nil)
	}
	return nil, status.Errorf(codes.NotFound, "workflow run %q not found", runID)
}

func (b *temporalBackend) ListRuns(ctx context.Context, _ *gestalt.ListWorkflowProviderRunsRequest) (*gestalt.ListWorkflowProviderRunsResponse, error) {
	runs, err := b.state.listRuns(ctx)
	if err != nil {
		return nil, err
	}
	inputs, err := runInputsFromProto(runs)
	if err != nil {
		return nil, err
	}
	return &gestalt.ListWorkflowProviderRunsResponse{Runs: inputs}, nil
}

func (b *temporalBackend) CancelRun(ctx context.Context, req *gestalt.CancelWorkflowProviderRunRequest) (*gestalt.BoundWorkflowRunInput, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	handle, err := decodeTemporalRunHandle(req.RunID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = "canceled"
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
		return nil, mapTemporalWorkflowCallError("temporal workflow update", err)
	}
	return runInputFromProto(&run, nil)
}

func (b *temporalBackend) SignalRun(ctx context.Context, req *gestalt.SignalWorkflowProviderRunRequest) (*gestalt.SignalWorkflowRunResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	handle, err := decodeTemporalRunHandle(req.RunID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	signalProto, err := workflowSignalProto(req.Signal)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	signal, err := normalizeWorkflowSignal(signalProto, time.Now().UTC())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ownerKey := handle.OwnerKey
	updateID := signalUpdateID(signal)
	ledgerKey := ownerIdempotencyLedgerKey(ownerKey, signal.GetIdempotencyKey())
	sigKey := explicitSignalLedgerKey(signal)
	fingerprint := signalFingerprint(ownerKey, handle.WorkflowKey+"\x00"+req.RunID, signal)
	var ownerResp *proto.SignalWorkflowRunResponse
	if ledgerKey != "" {
		resp, err := b.reserveSignalIdempotency(ctx, signalIdempotencyReserveRequest{
			Key:                  ledgerKey,
			Operation:            "signal",
			Fingerprint:          fingerprint,
			OwnerKey:             ownerKey,
			WorkflowKey:          handle.WorkflowKey,
			RunID:                req.RunID,
			SignalID:             signal.GetId(),
			AllowPayloadVariance: true,
		})
		if err != nil {
			return nil, err
		}
		if resp != nil {
			ownerResp = resp
		}
	}
	var explicitResp *proto.SignalWorkflowRunResponse
	if sigKey != "" {
		resp, err := b.reserveSignalIdempotency(ctx, signalIdempotencyReserveRequest{
			Key:         sigKey,
			Operation:   "signal-id",
			Fingerprint: fingerprint,
			OwnerKey:    ownerKey,
			WorkflowKey: handle.WorkflowKey,
			RunID:       req.RunID,
			SignalID:    signal.GetId(),
		})
		if err != nil {
			return nil, err
		}
		if resp != nil {
			explicitResp = resp
		}
	}
	if explicitResp != nil {
		if ledgerKey != "" && ownerResp == nil {
			if err := b.completeSignalIdempotency(ctx, ledgerKey, fingerprint, explicitResp, true); err != nil {
				return nil, err
			}
		}
		return signalRunResponseInputFromProto(explicitResp, nil)
	}
	if ownerResp != nil {
		if sigKey != "" {
			if err := b.completeSignalIdempotency(ctx, sigKey, fingerprint, ownerResp, false); err != nil {
				return nil, err
			}
		}
		return signalRunResponseInputFromProto(ownerResp, nil)
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
		return nil, status.Errorf(codes.Internal, "signal temporal workflow: %v", err)
	}
	var out proto.SignalWorkflowRunResponse
	if err := update.Get(ctx, &out); err != nil {
		return nil, mapTemporalWorkflowCallError("temporal workflow update", err)
	}
	resp := &out
	if ledgerKey != "" {
		if err := b.completeSignalIdempotency(ctx, ledgerKey, fingerprint, resp, true); err != nil {
			return nil, err
		}
	}
	if sigKey != "" {
		if err := b.completeSignalIdempotency(ctx, sigKey, fingerprint, resp, false); err != nil {
			return nil, err
		}
	}
	return signalRunResponseInputFromProto(resp, nil)
}

func (b *temporalBackend) SignalOrStartRun(ctx context.Context, req *gestalt.SignalOrStartWorkflowProviderRunRequest) (*gestalt.SignalWorkflowRunResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	workflowKey := strings.TrimSpace(req.WorkflowKey)
	if workflowKey == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow_key is required")
	}
	targetProto, err := workflowTargetProto(req.Target)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	target, err := normalizeTarget(targetProto)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	signalProto, err := workflowSignalProto(req.Signal)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	signal, err := normalizeWorkflowSignal(signalProto, time.Now().UTC())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ownerKey := target.OwnerKey
	updateID := signalUpdateID(signal)
	fingerprint := signalFingerprint(ownerKey, workflowKey, signal)
	ledgerKey := ownerIdempotencyLedgerKey(ownerKey, signal.GetIdempotencyKey())
	sigKey := explicitSignalLedgerKey(signal)
	var ownerResp *proto.SignalWorkflowRunResponse
	if ledgerKey != "" {
		resp, err := b.reserveSignalIdempotency(ctx, signalIdempotencyReserveRequest{
			Key:                  ledgerKey,
			Operation:            "signal_or_start",
			Fingerprint:          fingerprint,
			OwnerKey:             ownerKey,
			WorkflowKey:          workflowKey,
			SignalID:             signal.GetId(),
			AllowPayloadVariance: true,
		})
		if err != nil {
			return nil, err
		}
		if resp != nil {
			ownerResp = resp
		}
	}
	var explicitResp *proto.SignalWorkflowRunResponse
	if sigKey != "" {
		resp, err := b.reserveSignalIdempotency(ctx, signalIdempotencyReserveRequest{
			Key:         sigKey,
			Operation:   "signal-id",
			Fingerprint: fingerprint,
			OwnerKey:    ownerKey,
			WorkflowKey: workflowKey,
			SignalID:    signal.GetId(),
		})
		if err != nil {
			return nil, err
		}
		if resp != nil {
			explicitResp = resp
		}
	}
	if explicitResp != nil {
		if ledgerKey != "" && ownerResp == nil {
			if err := b.completeSignalIdempotency(ctx, ledgerKey, fingerprint, explicitResp, true); err != nil {
				return nil, err
			}
		}
		return signalRunResponseInputFromProto(explicitResp, nil)
	}
	if ownerResp != nil {
		if sigKey != "" {
			if err := b.completeSignalIdempotency(ctx, sigKey, fingerprint, ownerResp, false); err != nil {
				return nil, err
			}
		}
		return signalRunResponseInputFromProto(ownerResp, nil)
	}
	resp, err := b.signalOrStartRunV4(ctx, target, req, workflowKey, signal, updateID)
	if err != nil {
		return nil, err
	}
	if resp.GetRun() == nil {
		return nil, status.Error(codes.Internal, "signal-or-start returned no run")
	}
	if ledgerKey != "" {
		if err := b.completeSignalIdempotency(ctx, ledgerKey, fingerprint, resp, true); err != nil {
			return nil, err
		}
	}
	if sigKey != "" {
		if err := b.completeSignalIdempotency(ctx, sigKey, fingerprint, resp, false); err != nil {
			return nil, err
		}
	}
	return signalRunResponseInputFromProto(resp, nil)
}

func (b *temporalBackend) UpsertSchedule(ctx context.Context, req *gestalt.UpsertWorkflowProviderScheduleRequest) (*gestalt.BoundWorkflowScheduleInput, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	targetProto, err := workflowTargetProto(req.Target)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	target, err := normalizeTarget(targetProto)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	scheduleID := strings.TrimSpace(req.ScheduleID)
	if scheduleID == "" {
		scheduleID = uuid.NewString()
	}
	cron := strings.TrimSpace(req.Cron)
	if cron == "" {
		return nil, status.Error(codes.InvalidArgument, "cron is required")
	}
	timezone := strings.TrimSpace(req.Timezone)
	if timezone == "" {
		timezone = defaultTimezone
	}
	now := time.Now().UTC()
	existing, found, err := b.state.getSchedule(ctx, scheduleID)
	if err != nil {
		return nil, err
	}
	createdAt := now
	requestedBy := workflowActorProto(req.RequestedBy)
	createdBy := cloneActor(requestedBy)
	if found {
		createdAt = existing.GetCreatedAt().AsTime()
		createdBy = createdByForUpsert(existing.GetCreatedBy(), requestedBy)
	}
	schedule, err := gestalt.NewBoundWorkflowSchedule(gestalt.BoundWorkflowScheduleInput{
		ID:           scheduleID,
		Cron:         cron,
		Timezone:     timezone,
		Target:       workflowTargetInput(target.Target),
		Paused:       req.Paused,
		CreatedAt:    createdAt,
		UpdatedAt:    now,
		CreatedBy:    actorInputPtr(createdBy),
		ExecutionRef: strings.TrimSpace(req.ExecutionRef),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build workflow schedule: %v", err)
	}
	if err := b.upsertTemporalSchedule(ctx, schedule); err != nil {
		return nil, err
	}
	if err := b.state.putSchedule(ctx, schedule); err != nil {
		return nil, err
	}
	return scheduleInputFromProto(schedule, nil)
}

func (b *temporalBackend) GetSchedule(ctx context.Context, req *gestalt.GetWorkflowProviderScheduleRequest) (*gestalt.BoundWorkflowScheduleInput, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	id := strings.TrimSpace(req.ScheduleID)
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "schedule_id is required")
	}
	schedule, found, err := b.state.getSchedule(ctx, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow schedule %q not found", id)
	}
	b.fillScheduleNextRun(ctx, schedule)
	return scheduleInputFromProto(schedule, nil)
}

func (b *temporalBackend) ListSchedules(ctx context.Context, _ *gestalt.ListWorkflowProviderSchedulesRequest) (*gestalt.ListWorkflowProviderSchedulesResponse, error) {
	schedules, err := b.state.listSchedules(ctx)
	if err != nil {
		return nil, err
	}
	for _, schedule := range schedules {
		b.fillScheduleNextRun(ctx, schedule)
	}
	sortSchedules(schedules)
	inputs, err := scheduleInputsFromProto(schedules)
	if err != nil {
		return nil, err
	}
	return &gestalt.ListWorkflowProviderSchedulesResponse{Schedules: inputs}, nil
}

func (b *temporalBackend) DeleteSchedule(ctx context.Context, req *gestalt.DeleteWorkflowProviderScheduleRequest) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}
	id := strings.TrimSpace(req.ScheduleID)
	if id == "" {
		return status.Error(codes.InvalidArgument, "schedule_id is required")
	}
	if _, found, err := b.state.getSchedule(ctx, id); err != nil {
		return err
	} else if !found {
		return status.Errorf(codes.NotFound, "workflow schedule %q not found", id)
	}
	err := b.client.ScheduleClient().GetHandle(ctx, b.temporalScheduleID(id)).Delete(ctx)
	if err != nil && !isNotFound(err) {
		return status.Errorf(codes.Internal, "delete temporal schedule: %v", err)
	}
	if err := b.state.deleteSchedule(ctx, id); err != nil {
		return err
	}
	return nil
}

func (b *temporalBackend) PauseSchedule(ctx context.Context, req *gestalt.PauseWorkflowProviderScheduleRequest) (*gestalt.BoundWorkflowScheduleInput, error) {
	if req == nil || strings.TrimSpace(req.ScheduleID) == "" {
		return nil, status.Error(codes.InvalidArgument, "schedule_id is required")
	}
	id := strings.TrimSpace(req.ScheduleID)
	schedule, found, err := b.state.getSchedule(ctx, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow schedule %q not found", id)
	}
	if err := b.client.ScheduleClient().GetHandle(ctx, b.temporalScheduleID(id)).Pause(ctx, client.SchedulePauseOptions{Note: "paused by Gestalt"}); err != nil {
		return nil, status.Errorf(codes.Internal, "pause temporal schedule: %v", err)
	}
	scheduleInput, err := gestalt.BoundWorkflowScheduleInputFromSchedule(schedule)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build workflow schedule: %v", err)
	}
	scheduleInput.Paused = true
	scheduleInput.UpdatedAt = time.Now().UTC()
	schedule, err = gestalt.NewBoundWorkflowSchedule(scheduleInput)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build workflow schedule: %v", err)
	}
	if err := b.state.putSchedule(ctx, schedule); err != nil {
		return nil, err
	}
	return scheduleInputFromProto(schedule, nil)
}

func (b *temporalBackend) ResumeSchedule(ctx context.Context, req *gestalt.ResumeWorkflowProviderScheduleRequest) (*gestalt.BoundWorkflowScheduleInput, error) {
	if req == nil || strings.TrimSpace(req.ScheduleID) == "" {
		return nil, status.Error(codes.InvalidArgument, "schedule_id is required")
	}
	id := strings.TrimSpace(req.ScheduleID)
	schedule, found, err := b.state.getSchedule(ctx, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow schedule %q not found", id)
	}
	if err := b.client.ScheduleClient().GetHandle(ctx, b.temporalScheduleID(id)).Unpause(ctx, client.ScheduleUnpauseOptions{Note: "resumed by Gestalt"}); err != nil {
		return nil, status.Errorf(codes.Internal, "resume temporal schedule: %v", err)
	}
	scheduleInput, err := gestalt.BoundWorkflowScheduleInputFromSchedule(schedule)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build workflow schedule: %v", err)
	}
	scheduleInput.Paused = false
	scheduleInput.UpdatedAt = time.Now().UTC()
	schedule, err = gestalt.NewBoundWorkflowSchedule(scheduleInput)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build workflow schedule: %v", err)
	}
	if err := b.state.putSchedule(ctx, schedule); err != nil {
		return nil, err
	}
	return scheduleInputFromProto(schedule, nil)
}

func (b *temporalBackend) UpsertEventTrigger(ctx context.Context, req *gestalt.UpsertWorkflowProviderEventTriggerRequest) (*gestalt.BoundWorkflowEventTriggerInput, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	targetProto, err := workflowTargetProto(req.Target)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	target, err := normalizeTarget(targetProto)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	triggerID := strings.TrimSpace(req.TriggerID)
	if triggerID == "" {
		triggerID = uuid.NewString()
	}
	match := req.Match
	matchType := ""
	matchSource := ""
	matchSubject := ""
	if match != nil {
		matchType = strings.TrimSpace(match.Type)
		matchSource = strings.TrimSpace(match.Source)
		matchSubject = strings.TrimSpace(match.Subject)
	}
	if matchType == "" {
		return nil, status.Error(codes.InvalidArgument, "match.type is required")
	}
	now := time.Now().UTC()
	existing, found, err := b.state.getTrigger(ctx, triggerID)
	if err != nil {
		return nil, err
	}
	createdAt := now
	requestedBy := workflowActorProto(req.RequestedBy)
	createdBy := cloneActor(requestedBy)
	if found {
		createdAt = existing.GetCreatedAt().AsTime()
		createdBy = createdByForUpsert(existing.GetCreatedBy(), requestedBy)
	}
	trigger, err := gestalt.NewBoundWorkflowEventTrigger(gestalt.BoundWorkflowEventTriggerInput{
		ID: triggerID,
		Match: &gestalt.WorkflowEventMatchInput{
			Type:    matchType,
			Source:  matchSource,
			Subject: matchSubject,
		},
		Target:       workflowTargetInput(target.Target),
		Paused:       req.Paused,
		CreatedAt:    createdAt,
		UpdatedAt:    now,
		CreatedBy:    actorInputPtr(createdBy),
		ExecutionRef: strings.TrimSpace(req.ExecutionRef),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build workflow event trigger: %v", err)
	}
	if err := b.state.putTrigger(ctx, trigger); err != nil {
		return nil, err
	}
	return eventTriggerInputFromProto(trigger, nil)
}

func (b *temporalBackend) GetEventTrigger(ctx context.Context, req *gestalt.GetWorkflowProviderEventTriggerRequest) (*gestalt.BoundWorkflowEventTriggerInput, error) {
	if req == nil || strings.TrimSpace(req.TriggerID) == "" {
		return nil, status.Error(codes.InvalidArgument, "trigger_id is required")
	}
	triggerID := strings.TrimSpace(req.TriggerID)
	trigger, found, err := b.state.getTrigger(ctx, triggerID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow event trigger %q not found", triggerID)
	}
	return eventTriggerInputFromProto(trigger, nil)
}

func (b *temporalBackend) ListEventTriggers(ctx context.Context, _ *gestalt.ListWorkflowProviderEventTriggersRequest) (*gestalt.ListWorkflowProviderEventTriggersResponse, error) {
	triggers, err := b.state.listTriggers(ctx)
	if err != nil {
		return nil, err
	}
	sortTriggers(triggers)
	inputs, err := eventTriggerInputsFromProto(triggers)
	if err != nil {
		return nil, err
	}
	return &gestalt.ListWorkflowProviderEventTriggersResponse{Triggers: inputs}, nil
}

func (b *temporalBackend) DeleteEventTrigger(ctx context.Context, req *gestalt.DeleteWorkflowProviderEventTriggerRequest) error {
	if req == nil || strings.TrimSpace(req.TriggerID) == "" {
		return status.Error(codes.InvalidArgument, "trigger_id is required")
	}
	triggerID := strings.TrimSpace(req.TriggerID)
	found, err := b.state.deleteTrigger(ctx, triggerID)
	if err != nil {
		return err
	}
	if !found {
		return status.Errorf(codes.NotFound, "workflow event trigger %q not found", triggerID)
	}
	return nil
}

func (b *temporalBackend) PauseEventTrigger(ctx context.Context, req *gestalt.PauseWorkflowProviderEventTriggerRequest) (*gestalt.BoundWorkflowEventTriggerInput, error) {
	if req == nil || strings.TrimSpace(req.TriggerID) == "" {
		return nil, status.Error(codes.InvalidArgument, "trigger_id is required")
	}
	return b.setTriggerPaused(ctx, strings.TrimSpace(req.TriggerID), true)
}

func (b *temporalBackend) ResumeEventTrigger(ctx context.Context, req *gestalt.ResumeWorkflowProviderEventTriggerRequest) (*gestalt.BoundWorkflowEventTriggerInput, error) {
	if req == nil || strings.TrimSpace(req.TriggerID) == "" {
		return nil, status.Error(codes.InvalidArgument, "trigger_id is required")
	}
	return b.setTriggerPaused(ctx, strings.TrimSpace(req.TriggerID), false)
}

func (b *temporalBackend) PutExecutionReference(ctx context.Context, req *gestalt.PutWorkflowExecutionReferenceRequest) (*gestalt.WorkflowExecutionReferenceInput, error) {
	if req == nil || req.Reference == nil {
		return nil, status.Error(codes.InvalidArgument, "reference is required")
	}
	ref, err := workflowExecutionReferenceProto(req.Reference)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ref = cloneExecutionReference(ref)
	if strings.TrimSpace(ref.GetProviderName()) == "" {
		ref.ProviderName = b.providerName
	}
	ref, err = validateExecutionReference(ref)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	existing, found, err := b.state.getExecutionRef(ctx, ref.GetId())
	if err != nil {
		return nil, err
	}
	refInput, err := gestalt.WorkflowExecutionReferenceInputFromReference(ref)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build workflow execution reference: %v", err)
	}
	if found && existing.GetCreatedAt() != nil && existing.GetCreatedAt().IsValid() {
		existingInput, err := gestalt.WorkflowExecutionReferenceInputFromReference(existing)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "build workflow execution reference: %v", err)
		}
		refInput.CreatedAt = existingInput.CreatedAt
	}
	if refInput.CreatedAt.IsZero() {
		refInput.CreatedAt = time.Now().UTC()
	}
	ref, err = gestalt.NewWorkflowExecutionReference(refInput)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build workflow execution reference: %v", err)
	}
	if err := b.state.putExecutionRef(ctx, ref); err != nil {
		return nil, err
	}
	return executionReferenceInputFromProto(ref, nil)
}

func (b *temporalBackend) GetExecutionReference(ctx context.Context, req *gestalt.GetWorkflowExecutionReferenceRequest) (*gestalt.WorkflowExecutionReferenceInput, error) {
	if req == nil || strings.TrimSpace(req.ID) == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	id := strings.TrimSpace(req.ID)
	ref, found, err := b.state.getExecutionRef(ctx, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow execution reference %q not found", id)
	}
	return executionReferenceInputFromProto(ref, nil)
}

func (b *temporalBackend) ListExecutionReferences(ctx context.Context, req *gestalt.ListWorkflowExecutionReferencesRequest) (*gestalt.ListWorkflowExecutionReferencesResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	refs, err := b.state.listExecutionRefs(ctx, strings.TrimSpace(req.SubjectID))
	if err != nil {
		return nil, err
	}
	sortReferences(refs)
	inputs, err := executionReferenceInputsFromProto(refs)
	if err != nil {
		return nil, err
	}
	return &gestalt.ListWorkflowExecutionReferencesResponse{References: inputs}, nil
}

func (b *temporalBackend) PublishEvent(ctx context.Context, req *gestalt.PublishWorkflowProviderEventRequest) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}
	pluginName := strings.TrimSpace(req.PluginName)
	eventProto, err := workflowEventProto(req.Event)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	event, err := normalizeWorkflowEvent(eventProto, time.Now)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	triggers, err := b.state.matchTriggers(ctx, pluginName, event)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	publishedBy := cloneActor(workflowActorProto(req.PublishedBy))
	matchedTriggers := make([]*proto.BoundWorkflowEventTrigger, 0, len(triggers))
	matchedTriggerCounts := map[string]int64{}
	for _, trigger := range triggers {
		if eventMatchesTrigger(event, trigger) {
			matchedTriggers = append(matchedTriggers, trigger)
			matchedTriggerCounts[workflowTelemetryTargetKind(trigger.GetTarget())]++
		}
	}
	for targetKind, count := range matchedTriggerCounts {
		gestalt.RecordWorkflowEventMatchedTriggers(ctx, count, b.workflowTelemetryOptions(
			gestalt.WorkflowOperationPublishEvent,
			gestalt.WorkflowTriggerKindEvent,
			targetKind,
			gestalt.WorkflowRunStatusUnknown,
		))
	}
	for _, trigger := range matchedTriggers {
		createdBy := cloneActor(trigger.GetCreatedBy())
		executionRef := strings.TrimSpace(trigger.GetExecutionRef())
		if actorHasSubject(publishedBy) {
			createdBy = cloneActor(publishedBy)
		}
		temporalWorkflowID := eventRunWorkflowID(b.cfg.ScopeID, trigger.GetId(), event)
		if actorHasSubject(publishedBy) {
			ref, err := publishedEventExecutionReference(b.providerName, temporalWorkflowID, trigger, publishedBy, now)
			if err != nil {
				return status.Errorf(codes.Internal, "build event execution reference: %v", err)
			}
			if ref != nil {
				input, err := gestalt.WorkflowExecutionReferenceInputFromReference(ref)
				if err != nil {
					return status.Errorf(codes.Internal, "build event execution reference: %v", err)
				}
				if stored, err := b.PutExecutionReference(ctx, &gestalt.PutWorkflowExecutionReferenceRequest{Reference: &input}); err == nil && stored != nil {
					executionRef = stored.ID
				}
			}
		}
		input := b.runV4Input(targetOwnerKey(trigger.GetTarget()), executionRef, "", cloneTarget(trigger.GetTarget()), eventTrigger(trigger.GetId(), event), createdBy, false)
		run, err := b.executeRunV4(ctx, temporalWorkflowID, input, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
		if err != nil {
			if event.GetId() != "" && isAlreadyStarted(err) {
				continue
			}
			return status.Errorf(codes.Internal, "start event workflow: %v", err)
		}
		gestalt.RecordWorkflowRunStarted(ctx, b.workflowTelemetryOptions(
			gestalt.WorkflowOperationPublishEvent,
			gestalt.WorkflowTriggerKindEvent,
			workflowTelemetryTargetKind(trigger.GetTarget()),
			workflowTelemetryRunStatus(run),
		))
	}
	return nil
}

func (b *temporalBackend) workflowTelemetryOptions(operationName, triggerKind, targetKind, runStatus string) gestalt.WorkflowOperationOptions {
	providerName := ""
	if b != nil {
		providerName = b.providerName
	}
	return gestalt.WorkflowOperationOptions{
		ProviderName:  providerName,
		OperationName: operationName,
		TriggerKind:   triggerKind,
		TargetKind:    targetKind,
		RunStatus:     runStatus,
	}
}

func workflowTelemetryTargetKind(target *proto.BoundWorkflowTarget) string {
	switch {
	case target.GetPlugin() != nil:
		return gestalt.WorkflowTargetKindPlugin
	case target.GetAgent() != nil:
		return gestalt.WorkflowTargetKindAgent
	default:
		return gestalt.WorkflowTargetKindUnknown
	}
}

func workflowTelemetryRunStatus(run *proto.BoundWorkflowRun) string {
	if run == nil {
		return gestalt.WorkflowRunStatusUnknown
	}
	switch run.GetStatus() {
	case proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING:
		return gestalt.WorkflowRunStatusPending
	case proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING:
		return gestalt.WorkflowRunStatusRunning
	case proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED:
		return gestalt.WorkflowRunStatusSucceeded
	case proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED:
		return gestalt.WorkflowRunStatusFailed
	case proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_CANCELED:
		return gestalt.WorkflowRunStatusCanceled
	default:
		return gestalt.WorkflowRunStatusUnknown
	}
}

func (b *temporalBackend) setTriggerPaused(ctx context.Context, id string, paused bool) (*gestalt.BoundWorkflowEventTriggerInput, error) {
	trigger, found, err := b.state.getTrigger(ctx, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow event trigger %q not found", id)
	}
	triggerInput, err := gestalt.BoundWorkflowEventTriggerInputFromTrigger(trigger)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build workflow event trigger: %v", err)
	}
	triggerInput.Paused = paused
	triggerInput.UpdatedAt = time.Now().UTC()
	trigger, err = gestalt.NewBoundWorkflowEventTrigger(triggerInput)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "build workflow event trigger: %v", err)
	}
	if err := b.state.putTrigger(ctx, trigger); err != nil {
		return nil, err
	}
	return eventTriggerInputFromProto(trigger, nil)
}

func (b *temporalBackend) upsertTemporalSchedule(ctx context.Context, schedule *proto.BoundWorkflowSchedule) error {
	actionInput := b.runV4Input(targetOwnerKey(schedule.GetTarget()), schedule.GetExecutionRef(), "", cloneTarget(schedule.GetTarget()), scheduleTrigger(schedule.GetId(), time.Now().UTC()), cloneActor(schedule.GetCreatedBy()), false)
	actionInput.ScheduleID = schedule.GetId()
	action := &client.ScheduleWorkflowAction{
		Workflow:            gestaltRunWorkflowV4,
		Args:                []any{actionInput},
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
	scheduleInput, err := gestalt.BoundWorkflowScheduleInputFromSchedule(schedule)
	if err != nil {
		return
	}
	nextRunAt := desc.Info.NextActionTimes[0].UTC()
	scheduleInput.NextRunAt = &nextRunAt
	next, err := gestalt.NewBoundWorkflowSchedule(scheduleInput)
	if err != nil {
		return
	}
	*schedule = *next
}

func (b *temporalBackend) temporalScheduleID(scheduleID string) string {
	return workflowID(b.cfg.ScopeID, "schedule", scheduleID)
}

func (b *temporalBackend) runStartOptions(workflowID string, conflict enumspb.WorkflowIdConflictPolicy, reuse enumspb.WorkflowIdReusePolicy) client.StartWorkflowOptions {
	return client.StartWorkflowOptions{
		ID:                                       workflowID,
		TaskQueue:                                b.cfg.TaskQueue,
		WorkflowIDConflictPolicy:                 conflict,
		WorkflowIDReusePolicy:                    reuse,
		WorkflowExecutionErrorWhenAlreadyStarted: false,
		WorkflowTaskTimeout:                      b.cfg.WorkflowTaskTimeout,
		WorkflowRunTimeout:                       b.cfg.WorkflowRunTimeout,
	}
}

func isNotFound(err error) bool {
	var notFound *serviceerror.NotFound
	return errors.As(err, &notFound)
}

func isNotFoundLike(err error) bool {
	if err == nil {
		return false
	}
	return isNotFound(err) || strings.Contains(strings.ToLower(err.Error()), "not found")
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
		return "signal-key:" + hashID(signal.GetIdempotencyKey())
	}
	if signal.GetId() != "" {
		return "signal-id:" + hashID(signal.GetId())
	}
	return "signal:" + uuid.NewString()
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
