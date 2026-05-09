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
	"google.golang.org/protobuf/types/known/emptypb"
)

const workflowScheduleMemoKey = "gestalt.workflow_schedule"

type workflowHost interface {
	InvokeOperation(context.Context, *proto.InvokeWorkflowOperationRequest) (*proto.InvokeWorkflowOperationResponse, error)
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
	if err := b.promoteWorkerDeployment(ctx); err != nil {
		w.Stop()
		return err
	}
	b.worker = w
	b.started = true
	return nil
}

func (b *temporalBackend) workerOptions() worker.Options {
	if !b.cfg.Versioning.Enabled {
		return worker.Options{}
	}
	return worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			UseVersioning: true,
			Version: worker.WorkerDeploymentVersion{
				DeploymentName: b.cfg.Versioning.DeploymentName,
				BuildID:        b.cfg.Versioning.ResolvedBuildID,
			},
			DefaultVersioningBehavior: sdkworkflow.VersioningBehaviorAutoUpgrade,
		},
	}
}

func (b *temporalBackend) promoteWorkerDeployment(ctx context.Context) error {
	cfg := b.cfg.Versioning
	if !cfg.Enabled || cfg.Promotion.Mode == promotionModeNone {
		return nil
	}
	if b.client == nil {
		return errors.New("temporal workflow: client is not configured")
	}
	promoteCtx, cancel := context.WithTimeout(ctx, cfg.Promotion.Timeout)
	defer cancel()
	handle := b.client.WorkerDeploymentClient().GetHandle(cfg.DeploymentName)
	for {
		err := b.promoteWorkerDeploymentOnce(promoteCtx, handle, cfg)
		if err == nil {
			return nil
		}
		if !retryWorkerDeploymentPromotion(promoteCtx, err) {
			return fmt.Errorf("promote temporal worker deployment: %w", err)
		}
		select {
		case <-promoteCtx.Done():
			return fmt.Errorf("promote temporal worker deployment: %w", promoteCtx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (b *temporalBackend) promoteWorkerDeploymentOnce(ctx context.Context, handle client.WorkerDeploymentHandle, cfg versioningConfig) error {
	desc, err := handle.Describe(ctx, client.WorkerDeploymentDescribeOptions{})
	if err != nil {
		return err
	}
	switch cfg.Promotion.Mode {
	case promotionModeCurrent:
		current := desc.Info.RoutingConfig.CurrentVersion
		if sameWorkerDeploymentVersion(current, cfg.DeploymentName, cfg.ResolvedBuildID) {
			return nil
		}
		if current != nil && !cfg.Promotion.AllowReplaceCurrent {
			return fmt.Errorf("current worker deployment version is %s/%s; set versioning.promotion.allowReplaceCurrent to replace it", current.DeploymentName, current.BuildID)
		}
		_, err := handle.SetCurrentVersion(ctx, client.WorkerDeploymentSetCurrentVersionOptions{
			BuildID:                 cfg.ResolvedBuildID,
			ConflictToken:           desc.ConflictToken,
			Identity:                b.cfg.Identity,
			IgnoreMissingTaskQueues: false,
			AllowNoPollers:          false,
		})
		return err
	case promotionModeRamping:
		if cfg.Promotion.RampPercentage == nil {
			return errors.New("versioning.promotion.rampPercentage is required when mode is ramping")
		}
		current := desc.Info.RoutingConfig.CurrentVersion
		if sameWorkerDeploymentVersion(current, cfg.DeploymentName, cfg.ResolvedBuildID) {
			return nil
		}
		ramping := desc.Info.RoutingConfig.RampingVersion
		if sameWorkerDeploymentVersion(ramping, cfg.DeploymentName, cfg.ResolvedBuildID) &&
			desc.Info.RoutingConfig.RampingVersionPercentage == *cfg.Promotion.RampPercentage {
			return nil
		}
		_, err := handle.SetRampingVersion(ctx, client.WorkerDeploymentSetRampingVersionOptions{
			BuildID:                 cfg.ResolvedBuildID,
			Percentage:              *cfg.Promotion.RampPercentage,
			ConflictToken:           desc.ConflictToken,
			Identity:                b.cfg.Identity,
			IgnoreMissingTaskQueues: false,
			AllowNoPollers:          false,
		})
		return err
	default:
		return fmt.Errorf("unsupported worker deployment promotion mode %q", cfg.Promotion.Mode)
	}
}

func sameWorkerDeploymentVersion(version *worker.WorkerDeploymentVersion, deploymentName, buildID string) bool {
	return version != nil && version.DeploymentName == deploymentName && version.BuildID == buildID
}

func retryWorkerDeploymentPromotion(ctx context.Context, err error) bool {
	if ctx.Err() != nil || err == nil {
		return false
	}
	var failedPrecondition *serviceerror.FailedPrecondition
	if errors.As(err, &failedPrecondition) {
		return true
	}
	var unavailable *serviceerror.Unavailable
	if errors.As(err, &unavailable) {
		return true
	}
	var resourceExhausted *serviceerror.ResourceExhausted
	if errors.As(err, &resourceExhausted) {
		return true
	}
	var notFound *serviceerror.NotFound
	return errors.As(err, &notFound)
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
	if key != "" && workflowKey == "" {
		return b.startUnkeyedRunV4(ctx, target, req, key, fingerprint)
	}
	if workflowKey != "" {
		return b.startKeyedRunV4(ctx, target, req, key, fingerprint)
	}
	temporalWorkflowID := workflowID(b.cfg.ScopeID, "run-v4", uuid.NewString())
	conflictPolicy := enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL
	run, err := b.executeRunV4(ctx, temporalWorkflowID, b.runV4Input(target.OwnerKey, req.GetExecutionRef(), "", target.Target, newManualTrigger(), req.GetCreatedBy(), false), conflictPolicy, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
	if err != nil {
		return nil, err
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
	handle, err := decodeTemporalRunHandle(runID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if run, found, err := b.getRunProjection(ctx, runID); err != nil {
		return nil, status.Errorf(codes.Internal, "load workflow run projection: %v", err)
	} else if found {
		return run, nil
	}
	run, err := b.queryRunWorkflow(ctx, handle)
	if err != nil {
		return nil, err
	}
	if b.state != nil {
		_ = b.state.putRun(ctx, run)
	}
	return run, nil
}

func (b *temporalBackend) ListRuns(ctx context.Context, _ *proto.ListWorkflowProviderRunsRequest) (*proto.ListWorkflowProviderRunsResponse, error) {
	runs, err := b.listRunProjections(ctx)
	if err != nil {
		return nil, err
	}
	return &proto.ListWorkflowProviderRunsResponse{Runs: runs}, nil
}

func (b *temporalBackend) CancelRun(ctx context.Context, req *proto.CancelWorkflowProviderRunRequest) (*proto.BoundWorkflowRun, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	handle, err := decodeTemporalRunHandle(req.GetRunId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	reason := strings.TrimSpace(req.GetReason())
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
		return nil, mapWorkflowUpdateError(err)
	}
	return &run, nil
}

func (b *temporalBackend) SignalRun(ctx context.Context, req *proto.SignalWorkflowProviderRunRequest) (*proto.SignalWorkflowRunResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	handle, err := decodeTemporalRunHandle(req.GetRunId())
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
	sigKey := explicitSignalLedgerKey(signal)
	fingerprint := signalFingerprint(ownerKey, handle.WorkflowKey+"\x00"+req.GetRunId(), signal)
	var ownerResp *proto.SignalWorkflowRunResponse
	if ledgerKey != "" {
		resp, err := b.reserveSignalIdempotency(ctx, signalIdempotencyReserveRequest{
			Key:                  ledgerKey,
			Operation:            "signal",
			Fingerprint:          fingerprint,
			OwnerKey:             ownerKey,
			WorkflowKey:          handle.WorkflowKey,
			RunID:                req.GetRunId(),
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
			RunID:       req.GetRunId(),
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
		Args:         []any{signal},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "signal temporal workflow: %v", err)
	}
	var out proto.SignalWorkflowRunResponse
	if err := update.Get(ctx, &out); err != nil {
		return nil, mapWorkflowUpdateError(err)
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
	schedule := gestalt.NewBoundWorkflowSchedule(gestalt.BoundWorkflowScheduleInput{
		ID:           scheduleID,
		Cron:         cron,
		Timezone:     timezone,
		Target:       cloneTarget(target.Target),
		Paused:       req.GetPaused(),
		CreatedAt:    createdAt,
		UpdatedAt:    now,
		CreatedBy:    createdBy,
		ExecutionRef: strings.TrimSpace(req.GetExecutionRef()),
	})
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
	gestalt.SetTime(&schedule.UpdatedAt, time.Now().UTC())
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
	gestalt.SetTime(&schedule.UpdatedAt, time.Now().UTC())
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
	trigger := gestalt.NewBoundWorkflowEventTrigger(gestalt.BoundWorkflowEventTriggerInput{
		ID:           triggerID,
		Match:        &proto.WorkflowEventMatch{Type: strings.TrimSpace(match.GetType()), Source: strings.TrimSpace(match.GetSource()), Subject: strings.TrimSpace(match.GetSubject())},
		Target:       cloneTarget(target.Target),
		Paused:       req.GetPaused(),
		CreatedAt:    createdAt,
		UpdatedAt:    now,
		CreatedBy:    createdBy,
		ExecutionRef: strings.TrimSpace(req.GetExecutionRef()),
	})
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
		gestalt.SetTime(&ref.CreatedAt, time.Now().UTC())
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
				return nil, status.Errorf(codes.Internal, "build event execution reference: %v", err)
			}
			if ref != nil {
				if stored, err := b.PutExecutionReference(ctx, &proto.PutWorkflowExecutionReferenceRequest{Reference: ref}); err == nil && stored != nil {
					executionRef = stored.GetId()
				}
			}
		}
		input := b.runV4Input(targetOwnerKey(trigger.GetTarget()), executionRef, "", cloneTarget(trigger.GetTarget()), eventTrigger(trigger.GetId(), event), createdBy, false)
		run, err := b.executeRunV4(ctx, temporalWorkflowID, input, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
		if err != nil {
			if event.GetId() != "" && isAlreadyStarted(err) {
				continue
			}
			return nil, status.Errorf(codes.Internal, "start event workflow: %v", err)
		}
		gestalt.RecordWorkflowRunStarted(ctx, b.workflowTelemetryOptions(
			gestalt.WorkflowOperationPublishEvent,
			gestalt.WorkflowTriggerKindEvent,
			workflowTelemetryTargetKind(trigger.GetTarget()),
			workflowTelemetryRunStatus(run),
		))
	}
	return &emptypb.Empty{}, nil
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

func (b *temporalBackend) setTriggerPaused(ctx context.Context, id string, paused bool) (*proto.BoundWorkflowEventTrigger, error) {
	trigger, found, err := b.getTriggerIndex(ctx, id)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, status.Errorf(codes.NotFound, "workflow event trigger %q not found", id)
	}
	trigger.Paused = paused
	gestalt.SetTime(&trigger.UpdatedAt, time.Now().UTC())
	if err := b.putTriggerIndex(ctx, trigger); err != nil {
		return nil, err
	}
	return trigger, nil
}

func (b *temporalBackend) upsertTemporalSchedule(ctx context.Context, schedule *proto.BoundWorkflowSchedule) error {
	scheduleMemo := map[string]interface{}{
		workflowScheduleMemoKey: cloneSchedule(schedule),
	}
	actionInput := b.runV4Input(targetOwnerKey(schedule.GetTarget()), schedule.GetExecutionRef(), "", cloneTarget(schedule.GetTarget()), scheduleTrigger(schedule.GetId(), time.Now().UTC()), cloneActor(schedule.GetCreatedBy()), false)
	actionInput.ScheduleID = schedule.GetId()
	action := &client.ScheduleWorkflowAction{
		Workflow:            gestaltRunWorkflowV4,
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

func (b *temporalBackend) fillScheduleNextRun(ctx context.Context, schedule *proto.BoundWorkflowSchedule) {
	desc, err := b.client.ScheduleClient().GetHandle(ctx, b.temporalScheduleID(schedule.GetId())).Describe(ctx)
	if err != nil || len(desc.Info.NextActionTimes) == 0 {
		return
	}
	gestalt.SetTime(&schedule.NextRunAt, desc.Info.NextActionTimes[0].UTC())
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
		return "signal-key:" + hashID(signal.GetIdempotencyKey())
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
