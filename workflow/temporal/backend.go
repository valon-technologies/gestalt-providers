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
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservicepb "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	memoKeyOwnerKey = "gestaltOwnerKey"
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

type workflowRunStartSnapshot struct {
	DefinitionID         string
	DefinitionGeneration int64
	OwnerKey             string
	Target               *gestalt.BoundWorkflowTarget
	Input                map[string]any
	RunAs                *gestalt.Subject
	CreatedBySubjectID   string
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
	w.RegisterWorkflow(TemporalRun)
	w.RegisterActivity(&workflowActivities{executor: b.stepExecutor})
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

func (b *temporalBackend) StartRun(ctx context.Context, req *gestalt.StartWorkflowProviderRunRequest) (*gestalt.WorkflowRun, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	start, err := b.manualRunStartSnapshot(ctx, req.DefinitionID, req.ExpectedDefinitionGeneration, req.Input, req.RunAs, req.CreatedBySubjectID)
	if err != nil {
		return nil, err
	}
	key := strings.TrimSpace(req.IdempotencyKey)
	workflowKey := strings.TrimSpace(req.WorkflowKey)
	fingerprint := startFingerprint(start.OwnerKey, key, workflowKey, start.DefinitionID, start.DefinitionGeneration, start.Input, start.CreatedBySubjectID)
	if key != "" && workflowKey == "" {
		return b.startUnkeyedRun(ctx, start, key, fingerprint)
	}
	if workflowKey != "" {
		return b.startKeyedRun(ctx, start, workflowKey, key, fingerprint)
	}
	temporalWorkflowID := workflowID(b.cfg.ScopeID, "temporal-run", uuid.NewString())
	conflictPolicy := enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL
	input := b.runInput(start.OwnerKey, start.DefinitionID, start.DefinitionGeneration, "", start.Target, start.Input, manualTriggerInput(), start.CreatedBySubjectID, false)
	input.RunAs = cloneSubjectInput(start.RunAs)
	run, err := b.executeRun(ctx, temporalWorkflowID, input, conflictPolicy, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
	if err != nil {
		return nil, err
	}
	return run, nil
}

func (b *temporalBackend) GetRun(ctx context.Context, req *gestalt.GetWorkflowProviderRunRequest) (*gestalt.WorkflowRun, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	runID := strings.TrimSpace(req.RunID)
	if runID == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	handle, err := decodeTemporalRunHandle(runID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return b.getRunFromTemporal(ctx, handle)
}

func (b *temporalBackend) getRunFromTemporal(ctx context.Context, handle *temporalRunHandle) (*gestalt.WorkflowRun, error) {
	desc, err := b.client.DescribeWorkflowExecution(ctx, handle.RunWorkflowID, handle.RunTemporalRunID)
	if err != nil {
		return nil, mapTemporalWorkflowCallError("describe temporal workflow", err)
	}
	status := enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED
	if desc != nil && desc.WorkflowExecutionInfo != nil {
		status = desc.WorkflowExecutionInfo.GetStatus()
	}
	switch status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED,
		enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED:
		return b.queryRunFromTemporal(ctx, handle)
	default:
		return b.resultRunFromTemporal(ctx, handle)
	}
}

func (b *temporalBackend) queryRunFromTemporal(ctx context.Context, handle *temporalRunHandle) (*gestalt.WorkflowRun, error) {
	value, err := b.client.QueryWorkflow(ctx, handle.RunWorkflowID, handle.RunTemporalRunID, queryGetRun)
	if err != nil {
		return nil, mapTemporalWorkflowCallError("query temporal workflow run", err)
	}
	if value == nil || !value.HasValue() {
		return nil, status.Errorf(codes.Internal, "query temporal workflow run: empty run state")
	}
	var run gestalt.WorkflowRun
	if err := value.Get(&run); err != nil {
		return nil, status.Errorf(codes.Internal, "decode temporal workflow run query: %v", err)
	}
	return cloneRunInput(&run), nil
}

func (b *temporalBackend) resultRunFromTemporal(ctx context.Context, handle *temporalRunHandle) (*gestalt.WorkflowRun, error) {
	var run gestalt.WorkflowRun
	if err := b.client.GetWorkflow(ctx, handle.RunWorkflowID, handle.RunTemporalRunID).Get(ctx, &run); err != nil {
		return nil, mapTemporalWorkflowCallError("get temporal workflow result", err)
	}
	return cloneRunInput(&run), nil
}

func (b *temporalBackend) ListRuns(ctx context.Context, req *gestalt.ListWorkflowProviderRunsRequest) (*gestalt.ListWorkflowProviderRunsResponse, error) {
	pageSize := effectiveRunListPageSize(req)
	pageToken := ""
	if req != nil {
		pageToken = req.PageToken
	}
	nextPageToken, err := decodeTemporalListPageToken(pageToken)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "page_token is invalid")
	}
	resp, err := b.client.ListWorkflow(ctx, &workflowservicepb.ListWorkflowExecutionsRequest{
		PageSize:      int32(pageSize),
		NextPageToken: nextPageToken,
		Query:         b.runVisibilityQuery(req),
	})
	if err != nil {
		return nil, mapTemporalWorkflowCallError("list temporal workflows", err)
	}
	inputs := make([]gestalt.WorkflowRun, 0, len(resp.GetExecutions()))
	for _, info := range resp.GetExecutions() {
		run := listedRunFromExecutionInfo(info)
		if run != nil {
			inputs = append(inputs, *run)
		}
	}
	return &gestalt.ListWorkflowProviderRunsResponse{Runs: inputs, NextPageToken: encodeTemporalListPageToken(resp.GetNextPageToken())}, nil
}

// listedRunFromExecutionInfo builds a run summary from visibility data alone,
// so listing never depends on per-run worker round-trips. Runs that cannot be
// summarized (no owner-key memo) are skipped rather than failing the page.
func listedRunFromExecutionInfo(info *workflowpb.WorkflowExecutionInfo) *gestalt.WorkflowRun {
	if info == nil || info.GetExecution() == nil || strings.TrimSpace(info.GetExecution().GetWorkflowId()) == "" {
		return nil
	}
	ownerKey := payloadString(info.GetMemo().GetFields()[memoKeyOwnerKey])
	if ownerKey == "" {
		return nil
	}
	attrs := info.GetSearchAttributes().GetIndexedFields()
	run := &gestalt.WorkflowRun{
		ID: encodeTemporalRunHandle(temporalRunHandle{
			RunWorkflowID:    strings.TrimSpace(info.GetExecution().GetWorkflowId()),
			RunTemporalRunID: strings.TrimSpace(info.GetExecution().GetRunId()),
			OwnerKey:         ownerKey,
		}),
		Status:       workflowRunStatusValues[payloadString(attrs[searchAttrRunStatus.GetName()])],
		ProviderName: payloadString(attrs[searchAttrProviderName.GetName()]),
		DefinitionID: payloadString(attrs[searchAttrDefinitionID.GetName()]),
	}
	if start := info.GetStartTime(); start != nil {
		run.CreatedAt = start.AsTime()
	}
	if closed := info.GetCloseTime(); closed != nil {
		completed := closed.AsTime()
		run.CompletedAt = &completed
	}
	return run
}

var workflowRunStatusValues = map[string]gestalt.WorkflowRunStatus{
	"pending":   gestalt.WorkflowRunStatusValuePending,
	"running":   gestalt.WorkflowRunStatusValueRunning,
	"succeeded": gestalt.WorkflowRunStatusValueSucceeded,
	"failed":    gestalt.WorkflowRunStatusValueFailed,
	"canceled":  gestalt.WorkflowRunStatusValueCanceled,
}

func payloadString(payload *commonpb.Payload) string {
	if payload == nil {
		return ""
	}
	var value string
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &value); err != nil {
		return ""
	}
	return strings.TrimSpace(value)
}

func (b *temporalBackend) runVisibilityQuery(req *gestalt.ListWorkflowProviderRunsRequest) string {
	parts := []string{
		"WorkflowType = " + temporalVisibilityQuote(temporalRunWorkflowType),
		"GestaltScopeId = " + temporalVisibilityQuote(b.cfg.ScopeID),
		"GestaltProviderName = " + temporalVisibilityQuote(b.providerName),
	}
	if req != nil && req.Status != gestalt.WorkflowRunStatusValueUnspecified {
		if statusName := workflowRunStatusName(req.Status); statusName != "" && statusName != "unspecified" {
			parts = append(parts, "GestaltRunStatus = "+temporalVisibilityQuote(statusName))
		}
	}
	if req != nil {
		if targetApp := strings.TrimSpace(req.TargetApp); targetApp != "" {
			parts = append(parts, "GestaltTargetApps = "+temporalVisibilityQuote(targetApp))
		}
	}
	return strings.Join(parts, " AND ")
}

func (b *temporalBackend) GetRunEvents(ctx context.Context, req *gestalt.GetWorkflowProviderRunEventsRequest) (*gestalt.GetWorkflowProviderRunEventsResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	runID := strings.TrimSpace(req.RunID)
	if runID == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	run, err := b.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: runID})
	if err != nil {
		return nil, err
	}
	return &gestalt.GetWorkflowProviderRunEventsResponse{Events: workflowRunEventsFromRun(run)}, nil
}

func (b *temporalBackend) GetRunOutput(ctx context.Context, req *gestalt.GetWorkflowProviderRunOutputRequest) (*gestalt.GetWorkflowProviderRunOutputResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	runID := strings.TrimSpace(req.RunID)
	if runID == "" {
		return nil, status.Error(codes.InvalidArgument, "run_id is required")
	}
	run, err := b.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: runID})
	if err != nil {
		return nil, err
	}
	return &gestalt.GetWorkflowProviderRunOutputResponse{Output: run.Output}, nil
}

func (b *temporalBackend) CancelRun(ctx context.Context, req *gestalt.CancelWorkflowProviderRunRequest) (*gestalt.WorkflowRun, error) {
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
	var run gestalt.WorkflowRun
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
	signal, err := normalizeWorkflowSignalInput(req.Signal, time.Now().UTC())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	start, err := b.manualRunStartSnapshot(ctx, req.DefinitionID, req.ExpectedDefinitionGeneration, req.Input, req.RunAs, req.CreatedBySubjectID)
	if err != nil {
		return nil, err
	}
	ownerKey := start.OwnerKey
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
	resp, err := b.signalOrStartRun(ctx, start, workflowKey, signal, updateID)
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

func (b *temporalBackend) DeliverEvent(ctx context.Context, req *gestalt.DeliverWorkflowProviderEventRequest) (*gestalt.WorkflowEvent, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	appName := strings.TrimSpace(req.AppName)
	if appName == "" {
		return nil, status.Error(codes.InvalidArgument, "app_name is required")
	}
	eventRequest := cloneWorkflowEventInput(req.Event)
	if eventRequest != nil {
		eventRequest.Source = appName
	}
	eventInput, err := normalizeWorkflowEvent(eventRequest, time.Now)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	matches, err := b.state.matchEventActivations(ctx, eventInput)
	if err != nil {
		return nil, err
	}
	deliveredBy := cloneCreatedBySubjectID(req.DeliveredBySubjectID)
	gestalt.RecordWorkflowEventDelivered(ctx, nil, b.workflowTelemetryOptions(
		gestalt.WorkflowOperationDeliverEvent,
		gestalt.WorkflowTriggerKindEvent,
		gestalt.WorkflowTargetKindUnknown,
		gestalt.WorkflowRunStatusUnknown,
	))
	matchedActivationCounts := map[string]int64{}
	for _, match := range matches {
		matchedActivationCounts[workflowTelemetryTargetKindInput(match.Definition.Target)]++
	}
	for targetKind, count := range matchedActivationCounts {
		gestalt.RecordWorkflowEventMatchedActivations(ctx, count, b.workflowTelemetryOptions(
			gestalt.WorkflowOperationDeliverEvent,
			gestalt.WorkflowTriggerKindEvent,
			targetKind,
			gestalt.WorkflowRunStatusUnknown,
		))
	}
	for _, match := range matches {
		definition := match.Definition
		activation := match.Activation
		createdBy := definition.CreatedBySubjectID
		if createdBySubjectIDSet(deliveredBy) {
			createdBy = cloneCreatedBySubjectID(deliveredBy)
		}
		activationInput, err := workflowEventActivationInputMap(activation.Input, eventInput)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "activation %q input: %v", activation.ID, err)
		}
		temporalWorkflowID := eventRunWorkflowID(b.cfg.ScopeID, definition.ID+"\x00"+activation.ID, eventInput)
		eventTriggerInput := &gestalt.WorkflowRunTrigger{Event: &gestalt.WorkflowEventTriggerInvocation{
			ActivationID: activation.ID,
			Event:        eventInput,
		}}
		input := b.runInput(targetOwnerKeyInput(definition.Target), definition.ID, definition.Generation, "", definition.Target, activationInput, eventTriggerInput, createdBy, false)
		input.RunAs = cloneSubjectInput(definition.RunAs)
		if err := validateWorkflowRunAsInput(input.RunAs); err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "workflow definition %q run_as: %v", definition.ID, err)
		}
		run, err := b.executeRun(ctx, temporalWorkflowID, input, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
		if err != nil {
			if strings.TrimSpace(eventInput.ID) != "" && isAlreadyStarted(err) {
				continue
			}
			return nil, status.Errorf(codes.Internal, "start event workflow: %v", err)
		}
		gestalt.RecordWorkflowActivationFired(ctx, b.workflowTelemetryOptions(
			gestalt.WorkflowOperationDeliverEvent,
			gestalt.WorkflowTriggerKindEvent,
			workflowTelemetryTargetKindInput(definition.Target),
			gestalt.WorkflowRunStatusUnknown,
		))
		gestalt.RecordWorkflowRunStarted(ctx, b.workflowTelemetryOptions(
			gestalt.WorkflowOperationDeliverEvent,
			gestalt.WorkflowTriggerKindEvent,
			workflowTelemetryTargetKindInput(definition.Target),
			workflowTelemetryRunStatus(run),
		))
	}
	return eventInput, nil
}

func (b *temporalBackend) manualRunStartSnapshot(ctx context.Context, definitionID string, expectedGeneration int64, input map[string]any, runAs *gestalt.Subject, createdBySubjectID string) (workflowRunStartSnapshot, error) {
	definitionID = strings.TrimSpace(definitionID)
	if definitionID == "" {
		return workflowRunStartSnapshot{}, status.Error(codes.InvalidArgument, "definition_id is required")
	}
	definition, found, err := b.state.getDefinition(ctx, definitionID)
	if err != nil {
		return workflowRunStartSnapshot{}, err
	}
	if !found {
		return workflowRunStartSnapshot{}, status.Errorf(codes.NotFound, "workflow definition %q not found", definitionID)
	}
	if definition.Paused {
		return workflowRunStartSnapshot{}, status.Errorf(codes.FailedPrecondition, "workflow definition %q is paused", definitionID)
	}
	if expectedGeneration > 0 && definition.Generation != expectedGeneration {
		return workflowRunStartSnapshot{}, status.Errorf(codes.FailedPrecondition, "workflow definition %q generation is %d, expected %d", definitionID, definition.Generation, expectedGeneration)
	}
	ownerKey := targetOwnerKeyInput(definition.Target)
	if ownerKey == "" {
		return workflowRunStartSnapshot{}, status.Error(codes.InvalidArgument, "definition target owner is required")
	}
	effectiveRunAs := cloneSubjectInput(definition.RunAs)
	if runAs != nil {
		effectiveRunAs = cloneSubjectInput(runAs)
	}
	if err := validateWorkflowRunAsInput(effectiveRunAs); err != nil {
		return workflowRunStartSnapshot{}, status.Error(codes.InvalidArgument, err.Error())
	}
	return workflowRunStartSnapshot{
		DefinitionID:         definition.ID,
		DefinitionGeneration: definition.Generation,
		OwnerKey:             ownerKey,
		Target:               cloneBoundWorkflowTargetInput(definition.Target),
		Input:                cloneMapInput(input),
		RunAs:                effectiveRunAs,
		CreatedBySubjectID:   cloneCreatedBySubjectID(createdBySubjectID),
	}, nil
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

func workflowTelemetryRunStatus(run *gestalt.WorkflowRun) string {
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

func (b *temporalBackend) syncDefinitionSchedules(ctx context.Context, previous, next *gestalt.WorkflowDefinition) error {
	previousSchedules := definitionScheduleActivations(previous)
	nextSchedules := definitionScheduleActivations(next)
	for activationID := range previousSchedules {
		if _, keep := nextSchedules[activationID]; keep {
			continue
		}
		if err := b.deleteDefinitionSchedule(ctx, previous.ID, activationID); err != nil {
			return err
		}
	}
	if next == nil {
		return nil
	}
	for activationID, activation := range nextSchedules {
		if err := b.upsertDefinitionSchedule(ctx, next, activation); err != nil {
			return status.Errorf(codes.Internal, "sync schedule activation %q: %v", activationID, err)
		}
	}
	return nil
}

func definitionScheduleActivations(definition *gestalt.WorkflowDefinition) map[string]gestalt.WorkflowActivation {
	out := map[string]gestalt.WorkflowActivation{}
	if definition == nil {
		return out
	}
	for _, activation := range definition.Activations {
		activationID := strings.TrimSpace(activation.ID)
		if activationID == "" || activation.Schedule == nil {
			continue
		}
		activation.ID = activationID
		out[activationID] = activation
	}
	return out
}

func (b *temporalBackend) upsertDefinitionSchedule(ctx context.Context, definition *gestalt.WorkflowDefinition, activation gestalt.WorkflowActivation) error {
	if definition == nil || activation.Schedule == nil {
		return nil
	}
	activationInput, err := workflowActivationInputMap(activation.Input)
	if err != nil {
		return err
	}
	actionInput := b.runInput(targetOwnerKeyInput(definition.Target), definition.ID, definition.Generation, "", definition.Target, activationInput, scheduleTriggerInput(activation.ID, time.Now().UTC()), definition.CreatedBySubjectID, false)
	actionInput.ActivationID = activation.ID
	actionInput.RunAs = cloneSubjectInput(definition.RunAs)
	if err := validateWorkflowRunAsInput(actionInput.RunAs); err != nil {
		return status.Errorf(codes.InvalidArgument, "workflow definition %q run_as: %v", definition.ID, err)
	}
	action := &client.ScheduleWorkflowAction{
		Workflow:              TemporalRun,
		Args:                  []any{actionInput},
		TaskQueue:             b.cfg.TaskQueue,
		WorkflowRunTimeout:    b.cfg.WorkflowRunTimeout,
		WorkflowTaskTimeout:   defaultWorkflowTaskTimeout,
		TypedSearchAttributes: workflowRunSearchAttributesFromInput(actionInput, gestalt.WorkflowRunStatusValuePending),
	}
	if ownerKey := strings.TrimSpace(actionInput.OwnerKey); ownerKey != "" {
		action.Memo = map[string]any{memoKeyOwnerKey: ownerKey}
	}
	temporalID := b.temporalScheduleID(definition.ID, activation.ID)
	spec := client.ScheduleSpec{
		CronExpressions: []string{activation.Schedule.Cron},
		TimeZoneName:    activation.Schedule.Timezone,
	}
	paused := definition.Paused || activation.Paused
	handle := b.client.ScheduleClient().GetHandle(ctx, temporalID)
	_, err = handle.Describe(ctx)
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
			Paused:        paused,
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
			State: &client.ScheduleState{Paused: paused},
		}}, nil
	}})
	if err != nil {
		return status.Errorf(codes.Internal, "update temporal schedule: %v", err)
	}
	return nil
}

func (b *temporalBackend) deleteDefinitionSchedule(ctx context.Context, definitionID, activationID string) error {
	err := b.client.ScheduleClient().GetHandle(ctx, b.temporalScheduleID(definitionID, activationID)).Delete(ctx)
	if err != nil && !isNotFound(err) {
		return status.Errorf(codes.Internal, "delete temporal schedule: %v", err)
	}
	return nil
}

func (b *temporalBackend) temporalScheduleID(definitionID, activationID string) string {
	return workflowID(b.cfg.ScopeID, "definition-schedule", definitionID, activationID)
}

func (b *temporalBackend) runStartOptions(workflowID string, conflict enumspb.WorkflowIdConflictPolicy, reuse enumspb.WorkflowIdReusePolicy) client.StartWorkflowOptions {
	return client.StartWorkflowOptions{
		ID:                                       workflowID,
		TaskQueue:                                b.cfg.TaskQueue,
		WorkflowIDConflictPolicy:                 conflict,
		WorkflowIDReusePolicy:                    reuse,
		WorkflowExecutionErrorWhenAlreadyStarted: conflict == enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
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
