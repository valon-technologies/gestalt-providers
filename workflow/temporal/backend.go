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
	gestaltworkflow "github.com/valon-technologies/gestalt/sdk/go/workflow"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
	stepExecutor gestaltworkflow.StepExecutor
	state        *workflowStateStore

	newWorker temporalWorkerFactory

	mu      sync.Mutex
	started bool
	worker  temporalWorker
}

func newTemporalBackend(providerName string, cfg config, tc client.Client, executor gestaltworkflow.StepExecutor, state *workflowStateStore) *temporalBackend {
	if executor == nil {
		executor = gestaltworkflow.New(gestaltworkflow.Config{})
	}
	return &temporalBackend{
		providerName: strings.TrimSpace(providerName),
		cfg:          cfg,
		client:       tc,
		stepExecutor: executor,
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
	w.RegisterActivity(&workflowActivities{executor: b.stepExecutor, state: b.state})
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
	if b.stepExecutor != nil {
		errs = append(errs, b.stepExecutor.Close())
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

func (b *temporalBackend) StartRun(ctx context.Context, req *gestalt.StartWorkflowProviderRunRequest) (*gestalt.BoundWorkflowRun, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	target, err := normalizeTarget(req.Target)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	key := strings.TrimSpace(req.IdempotencyKey)
	workflowKey := strings.TrimSpace(req.WorkflowKey)
	fingerprint := startFingerprint(target.OwnerKey, key, workflowKey, req.ExecutionRef, target.Target, req.CreatedBy)
	if key != "" && workflowKey == "" {
		return b.startUnkeyedRunV4(ctx, target, req, key, fingerprint)
	}
	if workflowKey != "" {
		return b.startKeyedRunV4(ctx, target, req, key, fingerprint)
	}
	temporalWorkflowID := workflowID(b.cfg.ScopeID, "run-v4", uuid.NewString())
	conflictPolicy := enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL
	input := b.runV4Input(target.OwnerKey, req.ExecutionRef, "", target.Target, manualTriggerInput(), req.CreatedBy, false)
	input.InvocationToken = strings.TrimSpace(gestalt.InvocationTokenFromContext(ctx))
	run, err := b.executeRunV4(ctx, temporalWorkflowID, input, conflictPolicy, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
	if err != nil {
		return nil, err
	}
	return run, nil
}

func (b *temporalBackend) GetRun(ctx context.Context, req *gestalt.GetWorkflowProviderRunRequest) (*gestalt.BoundWorkflowRun, error) {
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
		return run, nil
	}
	return nil, status.Errorf(codes.NotFound, "workflow run %q not found", runID)
}

func (b *temporalBackend) ListRuns(ctx context.Context, req *gestalt.ListWorkflowProviderRunsRequest) (*gestalt.ListWorkflowProviderRunsResponse, error) {
	runs, nextPageToken, err := b.state.listRuns(ctx, req)
	if err != nil {
		return nil, err
	}
	inputs := make([]gestalt.BoundWorkflowRun, 0, len(runs))
	for _, run := range runs {
		if run != nil {
			inputs = append(inputs, *run)
		}
	}
	return &gestalt.ListWorkflowProviderRunsResponse{Runs: inputs, NextPageToken: nextPageToken}, nil
}

func (b *temporalBackend) CancelRun(ctx context.Context, req *gestalt.CancelWorkflowProviderRunRequest) (*gestalt.BoundWorkflowRun, error) {
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
	var run gestalt.BoundWorkflowRun
	if err := update.Get(ctx, &run); err != nil {
		return nil, mapTemporalWorkflowCallError("temporal workflow update", err)
	}
	return &run, nil
}

func (b *temporalBackend) SignalRun(ctx context.Context, req *gestalt.SignalWorkflowProviderRunRequest) (*gestalt.SignalWorkflowRunResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	handle, err := decodeTemporalRunHandle(req.RunID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	signal, err := normalizeWorkflowSignalInput(req.Signal, time.Now().UTC())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ownerKey := handle.OwnerKey
	updateID := signalUpdateID(signal)
	ledgerKey := ownerIdempotencyLedgerKey(ownerKey, signal.IdempotencyKey)
	sigKey := explicitSignalLedgerKey(signal)
	fingerprint := signalFingerprint(ownerKey, handle.WorkflowKey+"\x00"+req.RunID, signal)
	var ownerResp *gestalt.SignalWorkflowRunResponse
	if ledgerKey != "" {
		resp, err := b.reserveSignalIdempotency(ctx, signalIdempotencyReserveRequest{
			Key:                  ledgerKey,
			Operation:            "signal",
			Fingerprint:          fingerprint,
			OwnerKey:             ownerKey,
			WorkflowKey:          handle.WorkflowKey,
			RunID:                req.RunID,
			SignalID:             signal.ID,
			AllowPayloadVariance: true,
		})
		if err != nil {
			return nil, err
		}
		if resp != nil {
			ownerResp = resp
		}
	}
	var explicitResp *gestalt.SignalWorkflowRunResponse
	if sigKey != "" {
		resp, err := b.reserveSignalIdempotency(ctx, signalIdempotencyReserveRequest{
			Key:         sigKey,
			Operation:   "signal-id",
			Fingerprint: fingerprint,
			OwnerKey:    ownerKey,
			WorkflowKey: handle.WorkflowKey,
			RunID:       req.RunID,
			SignalID:    signal.ID,
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
		return explicitResp, nil
	}
	if ownerResp != nil {
		if sigKey != "" {
			if err := b.completeSignalIdempotency(ctx, sigKey, fingerprint, ownerResp, false); err != nil {
				return nil, err
			}
		}
		return ownerResp, nil
	}
	update, err := b.client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   handle.RunWorkflowID,
		RunID:        handle.RunTemporalRunID,
		UpdateID:     updateID,
		UpdateName:   updateAddSignal,
		Args:         []any{*signal},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "signal temporal workflow: %v", err)
	}
	var out gestalt.SignalWorkflowRunResponse
	if err := update.Get(ctx, &out); err != nil {
		return nil, mapTemporalWorkflowCallError("temporal workflow update", err)
	}
	resp := cloneSignalResponseInput(&out)
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
	return resp, nil
}

func (b *temporalBackend) SignalOrStartRun(ctx context.Context, req *gestalt.SignalOrStartWorkflowProviderRunRequest) (*gestalt.SignalWorkflowRunResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	workflowKey := strings.TrimSpace(req.WorkflowKey)
	if workflowKey == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow_key is required")
	}
	target, err := normalizeTarget(req.Target)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	signal, err := normalizeWorkflowSignalInput(req.Signal, time.Now().UTC())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ownerKey := target.OwnerKey
	updateID := signalUpdateID(signal)
	fingerprint := signalFingerprint(ownerKey, workflowKey, signal)
	ledgerKey := ownerIdempotencyLedgerKey(ownerKey, signal.IdempotencyKey)
	sigKey := explicitSignalLedgerKey(signal)
	var ownerResp *gestalt.SignalWorkflowRunResponse
	if ledgerKey != "" {
		resp, err := b.reserveSignalIdempotency(ctx, signalIdempotencyReserveRequest{
			Key:                  ledgerKey,
			Operation:            "signal_or_start",
			Fingerprint:          fingerprint,
			OwnerKey:             ownerKey,
			WorkflowKey:          workflowKey,
			SignalID:             signal.ID,
			AllowPayloadVariance: true,
		})
		if err != nil {
			return nil, err
		}
		if resp != nil {
			ownerResp = resp
		}
	}
	var explicitResp *gestalt.SignalWorkflowRunResponse
	if sigKey != "" {
		resp, err := b.reserveSignalIdempotency(ctx, signalIdempotencyReserveRequest{
			Key:         sigKey,
			Operation:   "signal-id",
			Fingerprint: fingerprint,
			OwnerKey:    ownerKey,
			WorkflowKey: workflowKey,
			SignalID:    signal.ID,
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
		return explicitResp, nil
	}
	if ownerResp != nil {
		if sigKey != "" {
			if err := b.completeSignalIdempotency(ctx, sigKey, fingerprint, ownerResp, false); err != nil {
				return nil, err
			}
		}
		return ownerResp, nil
	}
	resp, err := b.signalOrStartRunV4(ctx, target, req, workflowKey, signal, updateID)
	if err != nil {
		return nil, err
	}
	if resp == nil || resp.Run == nil {
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
	return resp, nil
}

func (b *temporalBackend) UpsertSchedule(ctx context.Context, req *gestalt.UpsertWorkflowProviderScheduleRequest) (*gestalt.BoundWorkflowSchedule, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	target, err := normalizeTarget(req.Target)
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
	createdBy := cloneActorInput(req.RequestedBy)
	if found {
		createdAt = existing.CreatedAt
		createdBy = createdByForUpsertInput(existing.CreatedBy, req.RequestedBy)
	}
	schedule := &gestalt.BoundWorkflowSchedule{
		ID:           scheduleID,
		Cron:         cron,
		Timezone:     timezone,
		Target:       target.Target,
		Paused:       req.Paused,
		CreatedAt:    createdAt,
		UpdatedAt:    now,
		CreatedBy:    createdBy,
		ExecutionRef: strings.TrimSpace(req.ExecutionRef),
	}
	if err := b.upsertTemporalSchedule(ctx, schedule, gestalt.InvocationTokenFromContext(ctx)); err != nil {
		return nil, err
	}
	if err := b.state.putSchedule(ctx, schedule); err != nil {
		return nil, err
	}
	return schedule, nil
}

func (b *temporalBackend) GetSchedule(ctx context.Context, req *gestalt.GetWorkflowProviderScheduleRequest) (*gestalt.BoundWorkflowSchedule, error) {
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
	return schedule, nil
}

func (b *temporalBackend) ListSchedules(ctx context.Context, _ *gestalt.ListWorkflowProviderSchedulesRequest) (*gestalt.ListWorkflowProviderSchedulesResponse, error) {
	schedules, err := b.state.listSchedules(ctx)
	if err != nil {
		return nil, err
	}
	for _, schedule := range schedules {
		b.fillScheduleNextRun(ctx, schedule)
	}
	sortScheduleInputs(schedules)
	inputs := make([]gestalt.BoundWorkflowSchedule, 0, len(schedules))
	for _, schedule := range schedules {
		if schedule != nil {
			inputs = append(inputs, *schedule)
		}
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

func (b *temporalBackend) PauseSchedule(ctx context.Context, req *gestalt.PauseWorkflowProviderScheduleRequest) (*gestalt.BoundWorkflowSchedule, error) {
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
	schedule.Paused = true
	schedule.UpdatedAt = time.Now().UTC()
	if err := b.state.putSchedule(ctx, schedule); err != nil {
		return nil, err
	}
	return schedule, nil
}

func (b *temporalBackend) ResumeSchedule(ctx context.Context, req *gestalt.ResumeWorkflowProviderScheduleRequest) (*gestalt.BoundWorkflowSchedule, error) {
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
	schedule.Paused = false
	schedule.UpdatedAt = time.Now().UTC()
	if err := b.state.putSchedule(ctx, schedule); err != nil {
		return nil, err
	}
	return schedule, nil
}

func (b *temporalBackend) UpsertEventTrigger(ctx context.Context, req *gestalt.UpsertWorkflowProviderEventTriggerRequest) (*gestalt.BoundWorkflowEventTrigger, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	target, err := normalizeTarget(req.Target)
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
	createdBy := cloneActorInput(req.RequestedBy)
	if found {
		createdAt = existing.CreatedAt
		createdBy = createdByForUpsertInput(existing.CreatedBy, req.RequestedBy)
	}
	trigger := &gestalt.BoundWorkflowEventTrigger{
		ID: triggerID,
		Match: &gestalt.WorkflowEventMatch{
			Type:    matchType,
			Source:  matchSource,
			Subject: matchSubject,
		},
		Target:       target.Target,
		Paused:       req.Paused,
		CreatedAt:    createdAt,
		UpdatedAt:    now,
		CreatedBy:    createdBy,
		ExecutionRef: strings.TrimSpace(req.ExecutionRef),
	}
	if err := b.state.putTrigger(ctx, trigger); err != nil {
		return nil, err
	}
	return trigger, nil
}

func (b *temporalBackend) GetEventTrigger(ctx context.Context, req *gestalt.GetWorkflowProviderEventTriggerRequest) (*gestalt.BoundWorkflowEventTrigger, error) {
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
	return trigger, nil
}

func (b *temporalBackend) ListEventTriggers(ctx context.Context, _ *gestalt.ListWorkflowProviderEventTriggersRequest) (*gestalt.ListWorkflowProviderEventTriggersResponse, error) {
	triggers, err := b.state.listTriggers(ctx)
	if err != nil {
		return nil, err
	}
	sortTriggerInputs(triggers)
	inputs := make([]gestalt.BoundWorkflowEventTrigger, 0, len(triggers))
	for _, trigger := range triggers {
		if trigger != nil {
			inputs = append(inputs, *trigger)
		}
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

func (b *temporalBackend) PauseEventTrigger(ctx context.Context, req *gestalt.PauseWorkflowProviderEventTriggerRequest) (*gestalt.BoundWorkflowEventTrigger, error) {
	if req == nil || strings.TrimSpace(req.TriggerID) == "" {
		return nil, status.Error(codes.InvalidArgument, "trigger_id is required")
	}
	return b.setTriggerPaused(ctx, strings.TrimSpace(req.TriggerID), true)
}

func (b *temporalBackend) ResumeEventTrigger(ctx context.Context, req *gestalt.ResumeWorkflowProviderEventTriggerRequest) (*gestalt.BoundWorkflowEventTrigger, error) {
	if req == nil || strings.TrimSpace(req.TriggerID) == "" {
		return nil, status.Error(codes.InvalidArgument, "trigger_id is required")
	}
	return b.setTriggerPaused(ctx, strings.TrimSpace(req.TriggerID), false)
}

func (b *temporalBackend) PutExecutionReference(ctx context.Context, req *gestalt.PutWorkflowExecutionReferenceRequest) (*gestalt.WorkflowExecutionReference, error) {
	if req == nil || req.Reference == nil {
		return nil, status.Error(codes.InvalidArgument, "reference is required")
	}
	ref := *req.Reference
	if strings.TrimSpace(ref.ProviderName) == "" {
		ref.ProviderName = b.providerName
	}
	refInput, err := validateExecutionReferenceInput(&ref)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	existing, found, err := b.state.getExecutionRef(ctx, refInput.ID)
	if err != nil {
		return nil, err
	}
	if found && !existing.CreatedAt.IsZero() {
		refInput.CreatedAt = existing.CreatedAt
	}
	if refInput.CreatedAt.IsZero() {
		refInput.CreatedAt = time.Now().UTC()
	}
	if err := b.state.putExecutionRef(ctx, refInput); err != nil {
		return nil, err
	}
	return refInput, nil
}

func (b *temporalBackend) GetExecutionReference(ctx context.Context, req *gestalt.GetWorkflowExecutionReferenceRequest) (*gestalt.WorkflowExecutionReference, error) {
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
	return ref, nil
}

func (b *temporalBackend) ListExecutionReferences(ctx context.Context, req *gestalt.ListWorkflowExecutionReferencesRequest) (*gestalt.ListWorkflowExecutionReferencesResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	refs, err := b.state.listExecutionRefs(ctx, strings.TrimSpace(req.SubjectID))
	if err != nil {
		return nil, err
	}
	sortReferenceInputs(refs)
	inputs := make([]gestalt.WorkflowExecutionReference, 0, len(refs))
	for _, ref := range refs {
		if ref != nil {
			inputs = append(inputs, *ref)
		}
	}
	return &gestalt.ListWorkflowExecutionReferencesResponse{References: inputs}, nil
}

func (b *temporalBackend) PublishEvent(ctx context.Context, req *gestalt.PublishWorkflowProviderEventRequest) (*gestalt.WorkflowEvent, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	appName := strings.TrimSpace(req.AppName)
	eventInput, err := normalizeWorkflowEvent(req.Event, time.Now)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	triggers, err := b.state.matchTriggers(ctx, appName, eventInput)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	publishedBy := cloneActorInput(req.PublishedBy)
	matchedTriggers := make([]*gestalt.BoundWorkflowEventTrigger, 0, len(triggers))
	matchedTriggerCounts := map[string]int64{}
	for _, trigger := range triggers {
		if eventMatchesTriggerInput(eventInput, trigger) {
			matchedTriggers = append(matchedTriggers, trigger)
			matchedTriggerCounts[workflowTelemetryTargetKindInput(trigger.Target)]++
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
		createdBy := trigger.CreatedBy
		executionRef := strings.TrimSpace(trigger.ExecutionRef)
		if actorHasSubject(publishedBy) {
			createdBy = cloneActorInput(publishedBy)
		}
		temporalWorkflowID := eventRunWorkflowID(b.cfg.ScopeID, trigger.ID, eventInput)
		if actorHasSubject(publishedBy) {
			ref, err := publishedEventExecutionReference(b.providerName, temporalWorkflowID, trigger, publishedBy, now)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "build event execution reference: %v", err)
			}
			if ref != nil {
				if stored, err := b.PutExecutionReference(ctx, &gestalt.PutWorkflowExecutionReferenceRequest{Reference: ref}); err == nil && stored != nil {
					executionRef = stored.ID
				}
			}
		}
		eventTriggerInput := &gestalt.WorkflowRunTrigger{Event: &gestalt.WorkflowEventTriggerInvocation{
			TriggerID: trigger.ID,
			Event:     eventInput,
		}}
		input := b.runV4Input(targetOwnerKeyInput(trigger.Target), executionRef, "", trigger.Target, eventTriggerInput, createdBy, false)
		input.InvocationToken = strings.TrimSpace(gestalt.InvocationTokenFromContext(ctx))
		run, err := b.executeRunV4(ctx, temporalWorkflowID, input, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
		if err != nil {
			if strings.TrimSpace(eventInput.ID) != "" && isAlreadyStarted(err) {
				continue
			}
			return nil, status.Errorf(codes.Internal, "start event workflow: %v", err)
		}
		gestalt.RecordWorkflowRunStarted(ctx, b.workflowTelemetryOptions(
			gestalt.WorkflowOperationPublishEvent,
			gestalt.WorkflowTriggerKindEvent,
			workflowTelemetryTargetKindInput(trigger.Target),
			workflowTelemetryRunStatus(run),
		))
	}
	return eventInput, nil
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

func workflowTelemetryTargetKindInput(target *gestalt.BoundWorkflowTarget) string {
	if target == nil {
		return gestalt.WorkflowTargetKindUnknown
	}
	if len(target.Steps) > 0 {
		return gestalt.WorkflowTargetKindSteps
	}
	return gestalt.WorkflowTargetKindUnknown
}

func workflowTelemetryRunStatus(run *gestalt.BoundWorkflowRun) string {
	if run == nil {
		return gestalt.WorkflowRunStatusUnknown
	}
	switch run.Status {
	case gestalt.WorkflowRunStatusValuePending:
		return gestalt.WorkflowRunStatusPending
	case gestalt.WorkflowRunStatusValueRunning:
		return gestalt.WorkflowRunStatusRunning
	case gestalt.WorkflowRunStatusValueSucceeded:
		return gestalt.WorkflowRunStatusSucceeded
	case gestalt.WorkflowRunStatusValueFailed:
		return gestalt.WorkflowRunStatusFailed
	case gestalt.WorkflowRunStatusValueCanceled:
		return gestalt.WorkflowRunStatusCanceled
	default:
		return gestalt.WorkflowRunStatusUnknown
	}
}

func (b *temporalBackend) setTriggerPaused(ctx context.Context, id string, paused bool) (*gestalt.BoundWorkflowEventTrigger, error) {
	trigger, found, err := b.state.getTrigger(ctx, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow event trigger %q not found", id)
	}
	trigger.Paused = paused
	trigger.UpdatedAt = time.Now().UTC()
	if err := b.state.putTrigger(ctx, trigger); err != nil {
		return nil, err
	}
	return trigger, nil
}

func (b *temporalBackend) upsertTemporalSchedule(ctx context.Context, schedule *gestalt.BoundWorkflowSchedule, invocationToken string) error {
	actionInput := b.runV4Input(targetOwnerKeyInput(schedule.Target), schedule.ExecutionRef, "", schedule.Target, scheduleTriggerInput(schedule.ID, time.Now().UTC()), schedule.CreatedBy, false)
	actionInput.ScheduleID = schedule.ID
	actionInput.InvocationToken = strings.TrimSpace(invocationToken)
	action := &client.ScheduleWorkflowAction{
		Workflow:            gestaltRunWorkflowV4,
		Args:                []any{actionInput},
		TaskQueue:           b.cfg.TaskQueue,
		WorkflowRunTimeout:  b.cfg.WorkflowRunTimeout,
		WorkflowTaskTimeout: defaultWorkflowTaskTimeout,
	}
	temporalID := b.temporalScheduleID(schedule.ID)
	spec := client.ScheduleSpec{
		CronExpressions: []string{schedule.Cron},
		TimeZoneName:    schedule.Timezone,
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
			Paused:        schedule.Paused,
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
			State: &client.ScheduleState{Paused: schedule.Paused},
		}}, nil
	}})
	if err != nil {
		return status.Errorf(codes.Internal, "update temporal schedule: %v", err)
	}
	return nil
}

func (b *temporalBackend) fillScheduleNextRun(ctx context.Context, schedule *gestalt.BoundWorkflowSchedule) {
	if schedule == nil {
		return
	}
	desc, err := b.client.ScheduleClient().GetHandle(ctx, b.temporalScheduleID(schedule.ID)).Describe(ctx)
	if err != nil || len(desc.Info.NextActionTimes) == 0 {
		return
	}
	nextRunAt := desc.Info.NextActionTimes[0].UTC()
	schedule.NextRunAt = &nextRunAt
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
		WorkflowTaskTimeout:                      defaultWorkflowTaskTimeout,
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

func workflowRunTerminal(status gestalt.WorkflowRunStatus) bool {
	switch status {
	case gestalt.WorkflowRunStatusValueSucceeded,
		gestalt.WorkflowRunStatusValueFailed,
		gestalt.WorkflowRunStatusValueCanceled:
		return true
	default:
		return false
	}
}

func workflowRunStatusName(status gestalt.WorkflowRunStatus) string {
	switch status {
	case gestalt.WorkflowRunStatusValuePending:
		return "pending"
	case gestalt.WorkflowRunStatusValueRunning:
		return "running"
	case gestalt.WorkflowRunStatusValueSucceeded:
		return "succeeded"
	case gestalt.WorkflowRunStatusValueFailed:
		return "failed"
	case gestalt.WorkflowRunStatusValueCanceled:
		return "canceled"
	default:
		return "unspecified"
	}
}

func signalUpdateID(signal *gestalt.WorkflowSignal) string {
	if signal == nil {
		return "signal:" + uuid.NewString()
	}
	if signal.IdempotencyKey != "" {
		return "signal-key:" + hashID(signal.IdempotencyKey)
	}
	if signal.ID != "" {
		return "signal-id:" + hashID(signal.ID)
	}
	return "signal:" + uuid.NewString()
}

func cloneSignalResponseInput(resp *gestalt.SignalWorkflowRunResponse) *gestalt.SignalWorkflowRunResponse {
	if resp == nil {
		return nil
	}
	return &gestalt.SignalWorkflowRunResponse{
		Run:         cloneRunInput(resp.Run),
		Signal:      cloneSignalInput(resp.Signal),
		StartedRun:  resp.StartedRun,
		WorkflowKey: strings.TrimSpace(resp.WorkflowKey),
	}
}
