package temporal

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	relationaldb "github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestGestaltRunWorkflowV4ProjectsRunStateToIndexedDB(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	host := &capturingHost{resp: &proto.InvokeWorkflowOperationResponse{Status: http.StatusOK, Body: "ok"}}
	env.RegisterWorkflow(gestaltRunWorkflowV4)
	env.RegisterActivity(&workflowActivities{host: host, state: state})

	env.ExecuteWorkflow(gestaltRunWorkflowV4, runWorkflowV4Input{
		ProviderName:                  "temporal",
		ScopeID:                       "scope",
		ExecutionRef:                  "ref-1",
		ActivityStartToCloseTimeoutNS: time.Minute,
		TargetPayload:                 protoPayload(pluginTarget("slack", "postMessage")),
		TriggerPayload:                protoPayload(newManualTrigger()),
		CreatedByPayload:              protoPayload(&proto.WorkflowActor{SubjectId: "user-1"}),
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var run proto.BoundWorkflowRun
	if err := env.GetWorkflowResult(&run); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	projected, found, err := state.getRun(ctx, run.GetId())
	if err != nil || !found {
		t.Fatalf("projected run found=%v err=%v", found, err)
	}
	if projected.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED || projected.GetResultBody() != "ok" {
		t.Fatalf("projected run = %#v, want succeeded with body", projected)
	}
	listed, err := state.listRuns(ctx)
	if err != nil {
		t.Fatalf("listRuns: %v", err)
	}
	if len(listed) != 1 || listed[0].GetId() != run.GetId() {
		t.Fatalf("listed runs = %#v, want %q", listed, run.GetId())
	}
}

func TestGestaltRunWorkflowV4WaitsForClaimBeforeInvokingHost(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	host := &capturingHost{resp: &proto.InvokeWorkflowOperationResponse{Status: http.StatusOK, Body: "ok"}}
	env.RegisterWorkflow(gestaltRunWorkflowV4)
	env.RegisterActivity(&workflowActivities{host: host})

	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(updateAddSignal, "signal-1", updateCallback(t, func(value interface{}) {
			resp := value.(*proto.SignalWorkflowRunResponse)
			if resp.GetSignal().GetIdempotencyKey() != "signal-1" {
				t.Fatalf("queued signal response = %#v, want signal-1", resp.GetSignal())
			}
		}), &proto.WorkflowSignal{Name: "slack.event", IdempotencyKey: "signal-1", CreatedAt: timestamppb.Now()})
		if len(host.calls) != 0 {
			t.Fatalf("host calls before claim = %d, want 0", len(host.calls))
		}
		env.UpdateWorkflow(updateClaimRun, "claim-run", updateCallback(t, nil), &proto.BoundWorkflowRun{})
	}, time.Millisecond)

	env.ExecuteWorkflow(gestaltRunWorkflowV4, runWorkflowV4Input{
		ProviderName:                  "temporal",
		ScopeID:                       "scope",
		ExecutionRef:                  "ref-1",
		ActivityStartToCloseTimeoutNS: time.Minute,
		WorkflowKey:                   "thread-1",
		OwnerKey:                      "slack",
		TargetPayload:                 protoPayload(pluginTarget("slack", "postMessage")),
		TriggerPayload:                protoPayload(newManualTrigger()),
		CreatedByPayload:              protoPayload(&proto.WorkflowActor{SubjectId: "user-1"}),
		RequireSignal:                 true,
		RequireClaim:                  true,
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(host.calls) != 1 {
		t.Fatalf("host calls = %d, want 1 after claim", len(host.calls))
	}
}

func TestGestaltRunWorkflowV4AcceptsInitialSignalPayloadForReplayCompatibility(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	host := &capturingHost{resp: &proto.InvokeWorkflowOperationResponse{Status: http.StatusOK, Body: "ok"}}
	env.RegisterWorkflow(gestaltRunWorkflowV4)
	env.RegisterActivity(&workflowActivities{host: host})

	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(updateClaimRun, "claim-run", updateCallback(t, nil), &proto.BoundWorkflowRun{})
	}, time.Millisecond)

	env.ExecuteWorkflow(gestaltRunWorkflowV4, runWorkflowV4Input{
		ProviderName:                  "temporal",
		ScopeID:                       "scope",
		ExecutionRef:                  "ref-1",
		ActivityStartToCloseTimeoutNS: time.Minute,
		WorkflowKey:                   "thread-1",
		OwnerKey:                      "slack",
		TargetPayload:                 protoPayload(pluginTarget("slack", "postMessage")),
		TriggerPayload:                protoPayload(newManualTrigger()),
		CreatedByPayload:              protoPayload(&proto.WorkflowActor{SubjectId: "user-1"}),
		InitialSignalPayload:          protoPayload(&proto.WorkflowSignal{Name: "slack.event", IdempotencyKey: "signal-1", CreatedAt: timestamppb.Now()}),
		RequireSignal:                 true,
		RequireClaim:                  true,
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(host.calls) != 1 {
		t.Fatalf("host calls = %d, want 1 after claim", len(host.calls))
	}
	signals := host.calls[0].GetSignals()
	if len(signals) != 1 || signals[0].GetIdempotencyKey() != "signal-1" {
		t.Fatalf("signals = %#v, want initial signal", signals)
	}
}

func TestGestaltRunWorkflowV4ClaimUpdateDoesNotWaitForProjection(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	host := &capturingHost{resp: &proto.InvokeWorkflowOperationResponse{Status: http.StatusOK, Body: "ok"}}
	activities := &workflowActivities{host: host}
	env.RegisterWorkflow(gestaltRunWorkflowV4)
	env.RegisterActivity(activities)

	var mu sync.Mutex
	var projectedStatuses []proto.WorkflowRunStatus
	env.OnActivity(activities.ProjectRun, mock.Anything, mock.Anything).Return(func(_ context.Context, run *proto.BoundWorkflowRun) error {
		mu.Lock()
		defer mu.Unlock()
		projectedStatuses = append(projectedStatuses, run.GetStatus())
		return nil
	}).Maybe()

	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(updateClaimRun, "claim-run", updateCallback(t, nil), &proto.BoundWorkflowRun{})
	}, time.Millisecond)

	env.ExecuteWorkflow(gestaltRunWorkflowV4, runWorkflowV4Input{
		ProviderName:                  "temporal",
		ScopeID:                       "scope",
		ExecutionRef:                  "ref-1",
		ActivityStartToCloseTimeoutNS: time.Minute,
		WorkflowKey:                   "thread-1",
		OwnerKey:                      "slack",
		TargetPayload:                 protoPayload(pluginTarget("slack", "postMessage")),
		TriggerPayload:                protoPayload(newManualTrigger()),
		CreatedByPayload:              protoPayload(&proto.WorkflowActor{SubjectId: "user-1"}),
		InitialSignalPayload:          protoPayload(&proto.WorkflowSignal{Name: "slack.event", CreatedAt: timestamppb.Now()}),
		RequireSignal:                 true,
		RequireClaim:                  true,
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(projectedStatuses) != 3 ||
		projectedStatuses[0] != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING ||
		projectedStatuses[1] != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING ||
		projectedStatuses[2] != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED {
		t.Fatalf("projected statuses = %v, want pending/running/succeeded", projectedStatuses)
	}
}

func TestGestaltRunWorkflowV4AddSignalUpdateDoesNotWaitForProjection(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	host := &capturingHost{resp: &proto.InvokeWorkflowOperationResponse{Status: http.StatusOK, Body: "ok"}}
	activities := &workflowActivities{host: host}
	env.RegisterWorkflow(gestaltRunWorkflowV4)
	env.RegisterActivity(activities)

	var mu sync.Mutex
	var projectedStatuses []proto.WorkflowRunStatus
	env.OnActivity(activities.ProjectRun, mock.Anything, mock.Anything).Return(func(_ context.Context, run *proto.BoundWorkflowRun) error {
		mu.Lock()
		defer mu.Unlock()
		projectedStatuses = append(projectedStatuses, run.GetStatus())
		return nil
	}).Maybe()

	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(updateAddSignal, "signal-run", updateCallback(t, nil), &proto.WorkflowSignal{Name: "slack.event", CreatedAt: timestamppb.Now()})
	}, time.Millisecond)

	env.ExecuteWorkflow(gestaltRunWorkflowV4, runWorkflowV4Input{
		ProviderName:                  "temporal",
		ScopeID:                       "scope",
		ExecutionRef:                  "ref-1",
		ActivityStartToCloseTimeoutNS: time.Minute,
		WorkflowKey:                   "thread-1",
		OwnerKey:                      "slack",
		TargetPayload:                 protoPayload(pluginTarget("slack", "postMessage")),
		TriggerPayload:                protoPayload(newManualTrigger()),
		CreatedByPayload:              protoPayload(&proto.WorkflowActor{SubjectId: "user-1"}),
		RequireSignal:                 true,
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(projectedStatuses) != 3 ||
		projectedStatuses[0] != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING ||
		projectedStatuses[1] != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING ||
		projectedStatuses[2] != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED {
		t.Fatalf("projected statuses = %v, want pending/running/succeeded", projectedStatuses)
	}
}

func TestGestaltRunWorkflowV4ContinuesWhenProjectionFails(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })
	if err := state.db.DeleteObjectStore(ctx, storeTemporalRunProjections); err != nil {
		t.Fatalf("DeleteObjectStore(%s): %v", storeTemporalRunProjections, err)
	}

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	host := &capturingHost{resp: &proto.InvokeWorkflowOperationResponse{Status: http.StatusOK, Body: "ok"}}
	env.RegisterWorkflow(gestaltRunWorkflowV4)
	env.RegisterActivity(&workflowActivities{host: host, state: state})

	env.ExecuteWorkflow(gestaltRunWorkflowV4, runWorkflowV4Input{
		ProviderName:                  "temporal",
		ScopeID:                       "scope",
		ExecutionRef:                  "ref-1",
		ActivityStartToCloseTimeoutNS: time.Minute,
		TargetPayload:                 protoPayload(pluginTarget("slack", "postMessage")),
		TriggerPayload:                protoPayload(newManualTrigger()),
		CreatedByPayload:              protoPayload(&proto.WorkflowActor{SubjectId: "user-1"}),
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var run proto.BoundWorkflowRun
	if err := env.GetWorkflowResult(&run); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	if run.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED || run.GetResultBody() != "ok" {
		t.Fatalf("run = %#v, want succeeded with body", &run)
	}
	if len(host.calls) != 1 {
		t.Fatalf("host calls = %d, want 1", len(host.calls))
	}
}

func TestScheduleFromTemporalDescriptionUsesActionMemo(t *testing.T) {
	createdAt := time.Unix(100, 0).UTC()
	updatedAt := time.Unix(200, 0).UTC()
	nextAt := time.Unix(300, 0).UTC()
	schedule := &proto.BoundWorkflowSchedule{
		Id:           "schedule-1",
		Target:       pluginTarget("slack", "postMessage"),
		CreatedBy:    &proto.WorkflowActor{SubjectId: "system:config", SubjectKind: "system", AuthSource: "config"},
		ExecutionRef: "ref-1",
	}
	payload, err := converter.GetDefaultDataConverter().ToPayload(schedule)
	if err != nil {
		t.Fatalf("encode schedule memo: %v", err)
	}

	got, found, err := scheduleFromTemporalDescription("", &client.ScheduleDescription{
		Schedule: client.Schedule{
			Action: &client.ScheduleWorkflowAction{
				Memo: map[string]interface{}{workflowScheduleMemoKey: payload},
			},
			Spec:  &client.ScheduleSpec{CronExpressions: []string{"0 * * * *"}, TimeZoneName: "America/New_York"},
			State: &client.ScheduleState{Paused: true},
		},
		Info: client.ScheduleInfo{CreatedAt: createdAt, LastUpdateAt: updatedAt, NextActionTimes: []time.Time{nextAt}},
	})
	if err != nil {
		t.Fatalf("scheduleFromTemporalDescription: %v", err)
	}
	if !found {
		t.Fatalf("schedule not found")
	}
	if got.GetId() != "schedule-1" || got.GetCron() != "0 * * * *" || got.GetTimezone() != "America/New_York" || !got.GetPaused() {
		t.Fatalf("decoded schedule = %#v", got)
	}
	if got.GetExecutionRef() != "ref-1" || got.GetTarget().GetPlugin().GetPluginName() != "slack" {
		t.Fatalf("decoded schedule metadata = %#v", got)
	}
	if !got.GetCreatedAt().AsTime().Equal(createdAt) || !got.GetUpdatedAt().AsTime().Equal(updatedAt) || !got.GetNextRunAt().AsTime().Equal(nextAt) {
		t.Fatalf("decoded schedule times = created %v updated %v next %v", got.GetCreatedAt(), got.GetUpdatedAt(), got.GetNextRunAt())
	}
}

func TestTemporalBackendStartKeepsWorkerUnversionedWhenConfigOmitted(t *testing.T) {
	order := []string{}
	fw := &fakeTemporalWorker{order: &order}
	tc := &recordingTemporalClient{deploymentClient: &fakeWorkerDeploymentClient{handle: &fakeWorkerDeploymentHandle{}}}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
	}, tc, nil, nil)
	backend.newWorker = func(_ client.Client, taskQueue string, options worker.Options) temporalWorker {
		if taskQueue != "gestalt-workflow" {
			t.Fatalf("task queue = %q, want gestalt-workflow", taskQueue)
		}
		if options.DeploymentOptions.UseVersioning {
			t.Fatalf("unversioned config set deployment options: %#v", options.DeploymentOptions)
		}
		return fw
	}
	if err := backend.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := strings.Join(order, ","); got != "start" {
		t.Fatalf("startup order = %s, want start", got)
	}
	if fw.registeredWorkflows != 1 || fw.registeredActivities != 1 {
		t.Fatalf("registered workflows=%d activities=%d, want only v4 workflow and activities", fw.registeredWorkflows, fw.registeredActivities)
	}
	if len(tc.deploymentClient.handle.describeCalls) != 0 || len(tc.deploymentClient.handle.setCurrentCalls) != 0 {
		t.Fatalf("unversioned config touched worker deployments: %#v", tc.deploymentClient.handle)
	}
	if !backend.started {
		t.Fatalf("backend not marked started")
	}
}

func TestTemporalBackendStartRegistersOnlyRunWorkflow(t *testing.T) {
	order := []string{}
	fw := &fakeTemporalWorker{order: &order}
	tc := &recordingTemporalClient{deploymentClient: &fakeWorkerDeploymentClient{handle: &fakeWorkerDeploymentHandle{}}}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
	}, tc, nil, nil)
	backend.newWorker = func(client.Client, string, worker.Options) temporalWorker { return fw }

	if err := backend.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := strings.Join(order, ","); got != "start" {
		t.Fatalf("startup order = %s, want start", got)
	}
	if fw.registeredWorkflows != 1 || fw.registeredActivities != 1 {
		t.Fatalf("registered workflows=%d activities=%d, want only v4 workflow and activities", fw.registeredWorkflows, fw.registeredActivities)
	}
	if len(tc.updates) != 0 || len(tc.queries) != 0 {
		t.Fatalf("startup touched workflow APIs queries=%#v updates=%#v", tc.queries, tc.updates)
	}
}

func TestTemporalBackendStartPromotesCurrentAfterWorkerStart(t *testing.T) {
	t.Setenv("TEMPORAL_BUILD_ID", "revision-1")
	raw := baseTemporalConfigRaw()
	raw["versioning"] = map[string]any{
		"enabled":                   true,
		"deploymentName":            "valon-tools-prod",
		"buildIDEnv":                "TEMPORAL_BUILD_ID",
		"defaultVersioningBehavior": "autoUpgrade",
		"promotion": map[string]any{
			"mode": "current",
		},
	}
	cfg, err := decodeConfig(raw)
	if err != nil {
		t.Fatalf("decodeConfig: %v", err)
	}

	order := []string{}
	fw := &fakeTemporalWorker{order: &order}
	handle := &fakeWorkerDeploymentHandle{
		describeResponse: client.WorkerDeploymentDescribeResponse{
			ConflictToken: []byte("token-1"),
		},
		order: &order,
	}
	tc := &recordingTemporalClient{deploymentClient: &fakeWorkerDeploymentClient{handle: handle}}
	backend := newTemporalBackend("temporal", cfg, tc, nil, nil)
	backend.newWorker = func(_ client.Client, _ string, options worker.Options) temporalWorker {
		deployment := options.DeploymentOptions
		if !deployment.UseVersioning {
			t.Fatalf("DeploymentOptions.UseVersioning = false, want true")
		}
		if deployment.Version.DeploymentName != "valon-tools-prod" || deployment.Version.BuildID != "revision-1" {
			t.Fatalf("deployment version = %#v", deployment.Version)
		}
		if deployment.DefaultVersioningBehavior != sdkworkflow.VersioningBehaviorAutoUpgrade {
			t.Fatalf("default versioning behavior = %v, want auto-upgrade", deployment.DefaultVersioningBehavior)
		}
		return fw
	}
	if err := backend.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := strings.Join(order, ","); got != "start,describe,set-current" {
		t.Fatalf("startup order = %s, want start,describe,set-current", got)
	}
	if len(handle.setCurrentCalls) != 1 {
		t.Fatalf("SetCurrent calls = %d, want 1", len(handle.setCurrentCalls))
	}
	call := handle.setCurrentCalls[0]
	if call.BuildID != "revision-1" || call.Identity != "gestalt-test" || string(call.ConflictToken) != "token-1" {
		t.Fatalf("SetCurrent call = %#v", call)
	}
	if call.AllowNoPollers || call.IgnoreMissingTaskQueues {
		t.Fatalf("SetCurrent bypassed worker deployment guardrails: %#v", call)
	}
}

func TestTemporalBackendStartRampingPromotion(t *testing.T) {
	ramp := float32(10)
	cfg := baseTemporalConfig()
	cfg.Versioning = versioningConfig{
		Enabled:                   true,
		DeploymentName:            "valon-tools-prod",
		BuildID:                   "revision-2",
		ResolvedBuildID:           "revision-2",
		DefaultVersioningBehavior: versioningBehaviorAutoUpgrade,
		Promotion: promotionConfig{
			Mode:           promotionModeRamping,
			Timeout:        time.Second,
			RampPercentage: &ramp,
		},
	}
	handle := &fakeWorkerDeploymentHandle{describeResponse: client.WorkerDeploymentDescribeResponse{}}
	backend := newTestTemporalBackendForStart(cfg, handle, &fakeTemporalWorker{})

	if err := backend.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if len(handle.setRampingCalls) != 1 {
		t.Fatalf("SetRamping calls = %d, want 1", len(handle.setRampingCalls))
	}
	call := handle.setRampingCalls[0]
	if call.BuildID != "revision-2" || call.Percentage != 10 {
		t.Fatalf("SetRamping call = %#v", call)
	}
	if call.AllowNoPollers || call.IgnoreMissingTaskQueues {
		t.Fatalf("SetRamping bypassed worker deployment guardrails: %#v", call)
	}
}

func TestTemporalBackendStartPromotionFailureStopsWorker(t *testing.T) {
	cfg := baseTemporalConfig()
	cfg.Versioning = versioningConfig{
		Enabled:                   true,
		DeploymentName:            "valon-tools-prod",
		BuildID:                   "revision-1",
		ResolvedBuildID:           "revision-1",
		DefaultVersioningBehavior: versioningBehaviorAutoUpgrade,
		Promotion: promotionConfig{
			Mode:    promotionModeCurrent,
			Timeout: time.Second,
		},
	}
	fw := &fakeTemporalWorker{}
	handle := &fakeWorkerDeploymentHandle{
		describeResponse: client.WorkerDeploymentDescribeResponse{},
		setCurrentErrs:   []error{errors.New("permission denied")},
	}
	backend := newTestTemporalBackendForStart(cfg, handle, fw)

	err := backend.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("Start error = %v, want promotion failure", err)
	}
	if fw.stopCount != 1 {
		t.Fatalf("worker Stop calls = %d, want 1", fw.stopCount)
	}
	if backend.started {
		t.Fatalf("backend marked started after promotion failure")
	}
}

func TestTemporalBackendStartRetriesPromotionConflict(t *testing.T) {
	cfg := baseTemporalConfig()
	cfg.Versioning = versioningConfig{
		Enabled:                   true,
		DeploymentName:            "valon-tools-prod",
		BuildID:                   "revision-1",
		ResolvedBuildID:           "revision-1",
		DefaultVersioningBehavior: versioningBehaviorAutoUpgrade,
		Promotion: promotionConfig{
			Mode:    promotionModeCurrent,
			Timeout: time.Second,
		},
	}
	handle := &fakeWorkerDeploymentHandle{
		describeResponse: client.WorkerDeploymentDescribeResponse{},
		setCurrentErrs:   []error{serviceerror.NewFailedPrecondition("conflict token mismatch"), nil},
	}
	backend := newTestTemporalBackendForStart(cfg, handle, &fakeTemporalWorker{})

	if err := backend.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if len(handle.describeCalls) != 2 || len(handle.setCurrentCalls) != 2 {
		t.Fatalf("promotion calls describe=%d setCurrent=%d, want 2/2", len(handle.describeCalls), len(handle.setCurrentCalls))
	}
}

func TestTemporalBackendStartRetriesPromotionDescribeNotFound(t *testing.T) {
	cfg := baseTemporalConfig()
	cfg.Versioning = versioningConfig{
		Enabled:                   true,
		DeploymentName:            "valon-tools-prod",
		BuildID:                   "revision-1",
		ResolvedBuildID:           "revision-1",
		DefaultVersioningBehavior: versioningBehaviorAutoUpgrade,
		Promotion: promotionConfig{
			Mode:    promotionModeCurrent,
			Timeout: time.Second,
		},
	}
	handle := &fakeWorkerDeploymentHandle{
		describeResponse: client.WorkerDeploymentDescribeResponse{},
		describeErrs:     []error{serviceerror.NewNotFound("worker deployment not found"), nil},
	}
	backend := newTestTemporalBackendForStart(cfg, handle, &fakeTemporalWorker{})

	if err := backend.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if len(handle.describeCalls) != 2 || len(handle.setCurrentCalls) != 1 {
		t.Fatalf("promotion calls describe=%d setCurrent=%d, want 2/1", len(handle.describeCalls), len(handle.setCurrentCalls))
	}
}

func TestTemporalBackendStartTreatsRampingTargetAlreadyCurrentAsSuccess(t *testing.T) {
	ramp := float32(10)
	cfg := baseTemporalConfig()
	cfg.Versioning = versioningConfig{
		Enabled:                   true,
		DeploymentName:            "valon-tools-prod",
		BuildID:                   "revision-2",
		ResolvedBuildID:           "revision-2",
		DefaultVersioningBehavior: versioningBehaviorAutoUpgrade,
		Promotion: promotionConfig{
			Mode:           promotionModeRamping,
			Timeout:        time.Second,
			RampPercentage: &ramp,
		},
	}
	handle := &fakeWorkerDeploymentHandle{
		describeResponse: client.WorkerDeploymentDescribeResponse{
			Info: client.WorkerDeploymentInfo{RoutingConfig: client.WorkerDeploymentRoutingConfig{
				CurrentVersion: &worker.WorkerDeploymentVersion{DeploymentName: "valon-tools-prod", BuildID: "revision-2"},
			}},
		},
	}
	backend := newTestTemporalBackendForStart(cfg, handle, &fakeTemporalWorker{})

	if err := backend.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if len(handle.setRampingCalls) != 0 {
		t.Fatalf("SetRamping calls = %d, want 0", len(handle.setRampingCalls))
	}
}

func TestTemporalBackendStartDoesNotReplaceDifferentCurrentByDefault(t *testing.T) {
	cfg := baseTemporalConfig()
	cfg.Versioning = versioningConfig{
		Enabled:                   true,
		DeploymentName:            "valon-tools-prod",
		BuildID:                   "revision-2",
		ResolvedBuildID:           "revision-2",
		DefaultVersioningBehavior: versioningBehaviorAutoUpgrade,
		Promotion: promotionConfig{
			Mode:    promotionModeCurrent,
			Timeout: time.Second,
		},
	}
	fw := &fakeTemporalWorker{}
	handle := &fakeWorkerDeploymentHandle{
		describeResponse: client.WorkerDeploymentDescribeResponse{
			Info: client.WorkerDeploymentInfo{RoutingConfig: client.WorkerDeploymentRoutingConfig{
				CurrentVersion: &worker.WorkerDeploymentVersion{DeploymentName: "valon-tools-prod", BuildID: "revision-1"},
			}},
		},
	}
	backend := newTestTemporalBackendForStart(cfg, handle, fw)

	err := backend.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "allowReplaceCurrent") {
		t.Fatalf("Start error = %v, want replace-current guard", err)
	}
	if len(handle.setCurrentCalls) != 0 {
		t.Fatalf("SetCurrent calls = %d, want 0", len(handle.setCurrentCalls))
	}
	if fw.stopCount != 1 {
		t.Fatalf("worker Stop calls = %d, want 1", fw.stopCount)
	}
}

func TestTemporalVersioningConfigValidation(t *testing.T) {
	t.Setenv("BUILD_ID", "revision-1")
	validVersioning := map[string]any{
		"enabled":                   true,
		"deploymentName":            "valon-tools-prod",
		"buildIDEnv":                "BUILD_ID",
		"defaultVersioningBehavior": "autoUpgrade",
	}
	tests := []struct {
		name       string
		versioning map[string]any
		want       string
	}{
		{
			name:       "both build id sources",
			versioning: withMap(validVersioning, "buildID", "revision-direct"),
			want:       "exactly one of versioning.buildID or versioning.buildIDEnv",
		},
		{
			name:       "missing default behavior",
			versioning: withoutMapKey(validVersioning, "defaultVersioningBehavior"),
			want:       "versioning.defaultVersioningBehavior",
		},
		{
			name:       "pinned unsupported",
			versioning: withMap(validVersioning, "defaultVersioningBehavior", "pinned"),
			want:       "versioning.defaultVersioningBehavior",
		},
		{
			name:       "deployment separator",
			versioning: withMap(validVersioning, "deploymentName", "valon.tools"),
			want:       "versioning.deploymentName cannot contain",
		},
		{
			name:       "missing build env",
			versioning: withMap(validVersioning, "buildIDEnv", "MISSING_BUILD_ID"),
			want:       "versioning.buildIDEnv",
		},
		{
			name:       "ramping missing percentage",
			versioning: withMap(validVersioning, "promotion", map[string]any{"mode": "ramping"}),
			want:       "versioning.promotion.rampPercentage is required",
		},
		{
			name:       "ramping invalid percentage",
			versioning: withMap(validVersioning, "promotion", map[string]any{"mode": "ramping", "rampPercentage": float32(0)}),
			want:       "versioning.promotion.rampPercentage must be greater than 0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := baseTemporalConfigRaw()
			raw["versioning"] = tt.versioning
			_, err := decodeConfig(raw)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("decodeConfig error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestSecondaryIndexWritesUseLookupShards(t *testing.T) {
	startTestIndexedDBBackend(t)
	state, err := openWorkflowStateStore(context.Background(), "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })
	tc := &recordingTemporalClient{}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
	}, tc, nil, state)
	trigger := &proto.BoundWorkflowEventTrigger{
		Id:        "trigger-1",
		Match:     &proto.WorkflowEventMatch{Type: "message.created"},
		Target:    pluginTarget("slack", "postMessage"),
		CreatedAt: timestamppb.Now(),
		UpdatedAt: timestamppb.Now(),
	}

	if err := backend.putTriggerIndex(context.Background(), trigger); err != nil {
		t.Fatalf("putTriggerIndex: %v", err)
	}
	if len(tc.updates) != 0 {
		t.Fatalf("putTriggerIndex touched workflow updates=%#v", tc.updates)
	}
	matched, err := backend.matchTriggersIndex(context.Background(), "slack", &proto.WorkflowEvent{Type: "message.created"})
	if err != nil {
		t.Fatalf("matchTriggersIndex: %v", err)
	}
	if len(matched) != 1 || matched[0].GetId() != trigger.GetId() {
		t.Fatalf("matched triggers = %#v, want %q", matched, trigger.GetId())
	}
	if len(tc.queries) != 0 {
		t.Fatalf("matchTriggersIndex touched workflow queries=%#v", tc.queries)
	}

	ref := &proto.WorkflowExecutionReference{
		Id:           "ref-1",
		ProviderName: "temporal",
		Target:       pluginTarget("slack", "postMessage"),
		SubjectId:    "user-1",
		CreatedAt:    timestamppb.Now(),
		RunAs: &proto.WorkflowRunAsSubject{
			SubjectId:   "service_account:slack-sync",
			SubjectKind: "service_account",
			DisplayName: "Slack sync",
			AuthSource:  "config",
		},
	}
	if err := backend.putExecutionRefIndex(context.Background(), ref); err != nil {
		t.Fatalf("putExecutionRefIndex: %v", err)
	}
	if len(tc.updates) != 0 {
		t.Fatalf("putExecutionRefIndex touched workflow updates=%#v", tc.updates)
	}
	refs, err := backend.listExecutionRefsIndex(context.Background(), "user-1")
	if err != nil {
		t.Fatalf("listExecutionRefsIndex: %v", err)
	}
	if len(refs) != 1 || refs[0].GetId() != ref.GetId() {
		t.Fatalf("refs = %#v, want %q", refs, ref.GetId())
	}
	if refs[0].GetRunAs().GetSubjectId() != "service_account:slack-sync" {
		t.Fatalf("ref run_as = %#v, want slack sync service account", refs[0].GetRunAs())
	}
	if len(tc.queries) != 0 {
		t.Fatalf("listExecutionRefsIndex touched workflow queries=%#v", tc.queries)
	}

	tc.scheduleClient = newFakeScheduleClient(map[string]*client.ScheduleDescription{
		backend.temporalScheduleID("schedule-1"): {
			Schedule: client.Schedule{
				Action: &client.ScheduleWorkflowAction{},
				Spec:   &client.ScheduleSpec{CronExpressions: []string{"0 * * * *"}, TimeZoneName: "America/New_York"},
				State:  &client.ScheduleState{},
			},
		},
	})
	if _, err := backend.UpsertSchedule(context.Background(), &proto.UpsertWorkflowProviderScheduleRequest{
		ScheduleId:  "schedule-1",
		Cron:        "0 * * * *",
		Timezone:    "America/New_York",
		Target:      pluginTarget("slack", "postMessage"),
		RequestedBy: &proto.WorkflowActor{SubjectId: "system:config", SubjectKind: "system", AuthSource: "config"},
	}); err != nil {
		t.Fatalf("UpsertSchedule: %v", err)
	}
	if len(tc.updates) != 0 {
		t.Fatalf("UpsertSchedule touched workflow updates=%#v", tc.updates)
	}
	if len(tc.queries) != 0 {
		t.Fatalf("UpsertSchedule touched workflow queries=%#v", tc.queries)
	}
}

func TestListSchedulesUsesIndexedDBMetadata(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	nextRunAt := time.Unix(200, 0).UTC()
	tc := &recordingTemporalClient{}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
	}, tc, nil, state)
	scheduleClient := newFakeScheduleClient(map[string]*client.ScheduleDescription{
		backend.temporalScheduleID("schedule-1"): {
			Schedule: client.Schedule{
				Action: &client.ScheduleWorkflowAction{},
				Spec:   &client.ScheduleSpec{CronExpressions: []string{"0 * * * *"}, TimeZoneName: "America/New_York"},
				State:  &client.ScheduleState{},
			},
			Info: client.ScheduleInfo{NextActionTimes: []time.Time{nextRunAt}},
		},
	})
	tc.scheduleClient = scheduleClient
	if err := state.putSchedule(ctx, &proto.BoundWorkflowSchedule{
		Id:        "schedule-1",
		Cron:      "0 * * * *",
		Timezone:  "America/New_York",
		Target:    pluginTarget("slack", "postMessage"),
		CreatedAt: timestamppb.New(time.Unix(100, 0).UTC()),
		UpdatedAt: timestamppb.New(time.Unix(100, 0).UTC()),
	}); err != nil {
		t.Fatalf("putSchedule: %v", err)
	}

	resp, err := backend.ListSchedules(ctx, &proto.ListWorkflowProviderSchedulesRequest{})
	if err != nil {
		t.Fatalf("ListSchedules: %v", err)
	}
	if len(resp.GetSchedules()) != 1 || resp.GetSchedules()[0].GetId() != "schedule-1" {
		t.Fatalf("schedules = %#v, want schedule-1", resp.GetSchedules())
	}
	if got := resp.GetSchedules()[0].GetNextRunAt().AsTime(); !got.Equal(nextRunAt) {
		t.Fatalf("next_run_at = %v, want %v", got, nextRunAt)
	}
	if scheduleClient.listCount != 0 {
		t.Fatalf("Temporal schedule list calls = %d, want 0", scheduleClient.listCount)
	}
	if len(tc.queries) != 0 || len(tc.updates) != 0 {
		t.Fatalf("ListSchedules touched workflow APIs queries=%#v updates=%#v", tc.queries, tc.updates)
	}
}

func TestStartRunUsesV4WorkflowAndStoresRunProjection(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	tc := &recordingTemporalClient{}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
	}, tc, nil, state)

	run, err := backend.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		Target:    pluginTarget("slack", "postMessage"),
		CreatedBy: &proto.WorkflowActor{SubjectId: "user-1"},
	})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions = %d, want 1", len(tc.executions))
	}
	if _, ok := tc.executions[0].Args[0].(runWorkflowV4Input); !ok {
		t.Fatalf("execution input = %T, want runWorkflowV4Input", tc.executions[0].Args[0])
	}
	projected, found, err := state.getRun(ctx, run.GetId())
	if err != nil || !found {
		t.Fatalf("projected run found=%v err=%v", found, err)
	}
	if projected.GetId() != run.GetId() || projected.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING {
		t.Fatalf("projected run = %#v, want pending %q", projected, run.GetId())
	}
}

func TestStartRunWithWorkflowKeyUsesV4AndStoresOwnership(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	tc := &recordingTemporalClient{}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
	}, tc, nil, state)

	run, err := backend.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		WorkflowKey: "thread-1",
		Target:      pluginTarget("slack", "postMessage"),
		CreatedBy:   &proto.WorkflowActor{SubjectId: "user-1"},
	})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions = %d, want 1", len(tc.executions))
	}
	input, ok := tc.executions[0].Args[0].(runWorkflowV4Input)
	if !ok {
		t.Fatalf("execution input = %T, want runWorkflowV4Input", tc.executions[0].Args[0])
	}
	if input.WorkflowKey != "thread-1" || input.OwnerKey != "slack" {
		t.Fatalf("v4 input workflow_key=%q owner_key=%q, want thread-1/slack", input.WorkflowKey, input.OwnerKey)
	}
	if !tc.hasUpdate(tc.executions[0].WorkflowID, updateClaimRun) {
		t.Fatalf("updates = %#v, want claim update on v4 run", tc.updates)
	}
	owned, found, err := state.getWorkflowKeyRun(ctx, "thread-1")
	if err != nil || !found {
		t.Fatalf("getWorkflowKeyRun found=%v err=%v", found, err)
	}
	if owned.GetId() != run.GetId() {
		t.Fatalf("owned run = %q, want %q", owned.GetId(), run.GetId())
	}
	handle, err := decodeTemporalRunHandle(run.GetId())
	if err != nil {
		t.Fatalf("decode run handle: %v", err)
	}
	if handle.Kind != runHandleKindV4 || handle.RunWorkflowID == "" || handle.WorkflowKey != "thread-1" {
		t.Fatalf("handle = %#v, want v4 keyed run handle", handle)
	}
}

func TestStartRunWithWorkflowKeyRejectsActiveOwnerBeforeExecuting(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	tc := &recordingTemporalClient{}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
	}, tc, nil, state)
	if _, err := backend.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		WorkflowKey: "thread-1",
		Target:      pluginTarget("slack", "postMessage"),
		CreatedBy:   &proto.WorkflowActor{SubjectId: "user-1"},
	}); err != nil {
		t.Fatalf("StartRun(first): %v", err)
	}
	_, err = backend.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		WorkflowKey: "thread-1",
		Target:      pluginTarget("slack", "sendMessage"),
		CreatedBy:   &proto.WorkflowActor{SubjectId: "user-2"},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StartRun(second) error = %v, want FailedPrecondition", err)
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions = %d, want only the first run", len(tc.executions))
	}
}

func TestStartRunWithWorkflowKeyUsesIndexedDBIdempotency(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	tc := &recordingTemporalClient{}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
		IdempotencyRetention:        time.Hour,
	}, tc, nil, state)
	req := &proto.StartWorkflowProviderRunRequest{
		IdempotencyKey: "start-1",
		WorkflowKey:    "thread-1",
		Target:         pluginTarget("slack", "postMessage"),
		CreatedBy:      &proto.WorkflowActor{SubjectId: "user-1"},
	}
	first, err := backend.StartRun(ctx, req)
	if err != nil {
		t.Fatalf("StartRun(first): %v", err)
	}
	duplicate, err := backend.StartRun(ctx, req)
	if err != nil {
		t.Fatalf("StartRun(duplicate): %v", err)
	}
	if duplicate.GetId() != first.GetId() {
		t.Fatalf("duplicate run id = %q, want %q", duplicate.GetId(), first.GetId())
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions = %d, want 1", len(tc.executions))
	}
	if !tc.hasUpdate(tc.executions[0].WorkflowID, updateClaimRun) {
		t.Fatalf("updates = %#v, want claim update on v4 run", tc.updates)
	}
	_, err = backend.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		IdempotencyKey: "start-1",
		WorkflowKey:    "thread-1",
		Target:         pluginTarget("slack", "sendMessage"),
		CreatedBy:      &proto.WorkflowActor{SubjectId: "user-1"},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StartRun(conflict) error = %v, want FailedPrecondition", err)
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions after conflict = %d, want 1", len(tc.executions))
	}
}

func TestStartRunWithWorkflowKeyCompletesReservedIndexedDBIdempotency(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	workflowKey := "thread-1"
	key := "start-1"
	target := pluginTarget("slack", "postMessage")
	createdBy := &proto.WorkflowActor{SubjectId: "user-1"}
	fingerprint := startFingerprint("slack", key, workflowKey, "", target, createdBy)
	if _, _, err := state.reserveRunIdempotency(ctx, "slack", key, fingerprint, time.Hour, time.Unix(100, 0).UTC()); err != nil {
		t.Fatalf("reserveRunIdempotency: %v", err)
	}
	temporalWorkflowID := workflowID("scope", "manual-keyed-v4", "slack", key, hashID(workflowKey))
	run := &proto.BoundWorkflowRun{
		Id: encodeTemporalRunHandle(temporalRunHandle{
			RunWorkflowID:    temporalWorkflowID,
			RunTemporalRunID: "run-1",
			WorkflowKey:      workflowKey,
			OwnerKey:         "slack",
		}),
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:      target,
		WorkflowKey: workflowKey,
		CreatedAt:   timestamppb.New(time.Unix(100, 0).UTC()),
	}
	if _, claimed, err := state.claimWorkflowKeyRun(ctx, workflowKey, run, time.Unix(100, 0).UTC()); err != nil || !claimed {
		t.Fatalf("claimWorkflowKeyRun claimed=%v err=%v", claimed, err)
	}

	tc := &recordingTemporalClient{}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
		IdempotencyRetention:        time.Hour,
	}, tc, nil, state)
	recovered, err := backend.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		IdempotencyKey: key,
		WorkflowKey:    workflowKey,
		Target:         target,
		CreatedBy:      createdBy,
	})
	if err != nil {
		t.Fatalf("StartRun(recovery): %v", err)
	}
	if recovered.GetId() != run.GetId() {
		t.Fatalf("recovered run = %q, want %q", recovered.GetId(), run.GetId())
	}
	if len(tc.executions) != 0 {
		t.Fatalf("executions = %d, want none during recovery", len(tc.executions))
	}
	duplicate, err := backend.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		IdempotencyKey: key,
		WorkflowKey:    workflowKey,
		Target:         target,
		CreatedBy:      createdBy,
	})
	if err != nil {
		t.Fatalf("StartRun(duplicate): %v", err)
	}
	if duplicate.GetId() != run.GetId() {
		t.Fatalf("duplicate run = %q, want %q", duplicate.GetId(), run.GetId())
	}
}

func TestStartRunContinuesWhenInitialRunProjectionWriteFails(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })
	if err := state.db.DeleteObjectStore(ctx, storeTemporalRunProjections); err != nil {
		t.Fatalf("DeleteObjectStore(%s): %v", storeTemporalRunProjections, err)
	}

	tc := &recordingTemporalClient{}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
	}, tc, nil, state)

	run, err := backend.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		Target:    pluginTarget("slack", "postMessage"),
		CreatedBy: &proto.WorkflowActor{SubjectId: "user-1"},
	})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if run.GetId() == "" || len(tc.executions) != 1 {
		t.Fatalf("run=%#v executions=%d, want started run", run, len(tc.executions))
	}
}

func TestStartRunUsesIndexedDBIdempotencyForUnkeyedRuns(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	tc := &recordingTemporalClient{}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
		IdempotencyRetention:        time.Hour,
	}, tc, nil, state)
	req := &proto.StartWorkflowProviderRunRequest{
		IdempotencyKey: "start-1",
		Target:         pluginTarget("slack", "postMessage"),
		CreatedBy:      &proto.WorkflowActor{SubjectId: "user-1"},
	}
	first, err := backend.StartRun(ctx, req)
	if err != nil {
		t.Fatalf("StartRun(first): %v", err)
	}
	duplicate, err := backend.StartRun(ctx, req)
	if err != nil {
		t.Fatalf("StartRun(duplicate): %v", err)
	}
	if duplicate.GetId() != first.GetId() {
		t.Fatalf("duplicate run id = %q, want %q", duplicate.GetId(), first.GetId())
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions = %d, want 1", len(tc.executions))
	}
	if len(tc.updates) != 0 {
		t.Fatalf("updates = %#v, want none", tc.updates)
	}
}

func TestStartRunRejectsConflictingIndexedDBIdempotency(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	tc := &recordingTemporalClient{}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
		IdempotencyRetention:        time.Hour,
	}, tc, nil, state)
	_, err = backend.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		IdempotencyKey: "start-1",
		Target:         pluginTarget("slack", "postMessage"),
		CreatedBy:      &proto.WorkflowActor{SubjectId: "user-1"},
	})
	if err != nil {
		t.Fatalf("StartRun(first): %v", err)
	}
	_, err = backend.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		IdempotencyKey: "start-1",
		Target:         pluginTarget("slack", "sendMessage"),
		CreatedBy:      &proto.WorkflowActor{SubjectId: "user-1"},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StartRun(conflict) error = %v, want FailedPrecondition", err)
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions = %d, want 1", len(tc.executions))
	}
}

func TestStartRunReturnsErrorWhenIdempotencyCompletionFails(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	tc := &recordingTemporalClient{}
	tc.afterExecute = func() {
		if err := state.db.DeleteObjectStore(ctx, storeTemporalRunIdempotency); err != nil {
			t.Errorf("DeleteObjectStore(%s): %v", storeTemporalRunIdempotency, err)
		}
	}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
		IdempotencyRetention:        time.Hour,
	}, tc, nil, state)

	_, err = backend.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		IdempotencyKey: "start-1",
		Target:         pluginTarget("slack", "postMessage"),
		CreatedBy:      &proto.WorkflowActor{SubjectId: "user-1"},
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("StartRun error = %v, want Internal", err)
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions = %d, want 1", len(tc.executions))
	}
}

func TestCompleteRunIdempotencyReadsThroughCompletedRecord(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	ownerKey := "slack"
	key := "start-1"
	fingerprint := "same-request"
	run := &proto.BoundWorkflowRun{Id: "run-1", Status: proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING}
	if _, _, err := state.reserveRunIdempotency(ctx, ownerKey, key, fingerprint, time.Hour, time.Unix(100, 0).UTC()); err != nil {
		t.Fatalf("reserveRunIdempotency: %v", err)
	}
	completedAt := time.Unix(200, 0).UTC()
	if err := state.completeRunIdempotency(ctx, ownerKey, key, fingerprint, run, time.Hour, completedAt); err != nil {
		t.Fatalf("completeRunIdempotency(first): %v", err)
	}
	if err := state.completeRunIdempotency(ctx, ownerKey, key, fingerprint, run, time.Hour, time.Unix(300, 0).UTC()); err != nil {
		t.Fatalf("completeRunIdempotency(duplicate): %v", err)
	}
	record, err := state.runIdempotency.Get(ctx, state.runIdempotencyID(ownerKey, key))
	if err != nil {
		t.Fatalf("load idempotency record: %v", err)
	}
	updatedAt := recordTime(record, "updated_at")
	if updatedAt == nil || !updatedAt.Equal(completedAt) {
		t.Fatalf("updated_at = %v, want original completion %v", updatedAt, completedAt)
	}
}

func TestSignalOrStartRunStartsV4WorkflowAndStoresOwnership(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	tc := &recordingTemporalClient{}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
	}, tc, nil, state)
	resp, err := backend.SignalOrStartRun(ctx, &proto.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey: "thread-1",
		Target:      pluginTarget("slack", "postMessage"),
		CreatedBy:   &proto.WorkflowActor{SubjectId: "user-1"},
		Signal:      &proto.WorkflowSignal{Name: "slack.event"},
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun: %v", err)
	}
	if !resp.GetStartedRun() || resp.GetRun().GetWorkflowKey() != "thread-1" {
		t.Fatalf("response = %#v, want started thread-1 run", resp)
	}
	if resp.GetSignal().GetSequence() != 1 || resp.GetSignal().GetId() == "" {
		t.Fatalf("response signal = %#v, want assigned sequence/id", resp.GetSignal())
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions = %d, want 1", len(tc.executions))
	}
	input, ok := tc.executions[0].Args[0].(runWorkflowV4Input)
	if !ok {
		t.Fatalf("execution input = %T, want runWorkflowV4Input", tc.executions[0].Args[0])
	}
	if input.WorkflowKey != "thread-1" || !input.RequireSignal || !input.RequireClaim {
		t.Fatalf("v4 input = %#v, want claimed keyed run waiting for signal", input)
	}
	if !tc.hasUpdate(tc.executions[0].WorkflowID, updateAddSignal) || !tc.hasUpdate(tc.executions[0].WorkflowID, updateClaimRun) {
		t.Fatalf("updates = %#v, want UpdateWithStart add-signal and claim on v4 run", tc.updates)
	}
	addSignalUpdateFound := false
	for _, update := range tc.updates {
		if update.WorkflowID == tc.executions[0].WorkflowID && update.Name == updateAddSignal {
			addSignalUpdateFound = true
			if update.WaitForStage != client.WorkflowUpdateStageCompleted {
				t.Fatalf("signal update wait stage = %v, want completed", update.WaitForStage)
			}
		}
		if update.WorkflowID == tc.executions[0].WorkflowID && update.Name == updateClaimRun {
			if update.WaitForStage != client.WorkflowUpdateStageCompleted {
				t.Fatalf("claim update wait stage = %v, want completed", update.WaitForStage)
			}
		}
	}
	if !addSignalUpdateFound {
		t.Fatalf("updates = %#v, want add-signal update", tc.updates)
	}
	if len(tc.queries) != 0 {
		t.Fatalf("queries = %#v, want no temporal lookup", tc.queries)
	}
	owned, found, err := state.getWorkflowKeyRun(ctx, "thread-1")
	if err != nil || !found || owned.GetId() != resp.GetRun().GetId() {
		t.Fatalf("owned found=%v run=%#v err=%v, want response run", found, owned, err)
	}
}

func TestSignalOrStartRunUsesIndexedDBSignalIdempotency(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	tc := &recordingTemporalClient{}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
		IdempotencyRetention:        time.Hour,
	}, tc, nil, state)
	req := &proto.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey: "thread-1",
		Target:      pluginTarget("slack", "postMessage"),
		CreatedBy:   &proto.WorkflowActor{SubjectId: "user-1"},
		Signal:      &proto.WorkflowSignal{Name: "slack.event", IdempotencyKey: "signal-1"},
	}
	first, err := backend.SignalOrStartRun(ctx, req)
	if err != nil {
		t.Fatalf("SignalOrStartRun(first): %v", err)
	}
	if len(tc.updates) == 0 || tc.updates[0].Name != updateAddSignal || tc.updates[0].UpdateID != "signal-key:"+hashID("signal-1") {
		t.Fatalf("first update = %#v, want add-signal keyed only by idempotency key", tc.updates)
	}
	updateCount := len(tc.updates)
	executionCount := len(tc.executions)

	duplicateReq := cloneSignalOrStartRequest(req)
	duplicateReq.Signal.Name = "slack.changed"
	duplicate, err := backend.SignalOrStartRun(ctx, duplicateReq)
	if err != nil {
		t.Fatalf("SignalOrStartRun(duplicate different payload): %v", err)
	}
	if duplicate.GetRun().GetId() != first.GetRun().GetId() || duplicate.GetSignal().GetId() != first.GetSignal().GetId() {
		t.Fatalf("duplicate response = %#v, want first response %#v", duplicate, first)
	}
	if len(tc.executions) != executionCount {
		t.Fatalf("executions = %d, want %d", len(tc.executions), executionCount)
	}
	if len(tc.updates) != updateCount {
		t.Fatalf("updates = %#v, want no duplicate temporal signal", tc.updates[updateCount:])
	}
	key := ownerIdempotencyLedgerKey("slack", "signal-1")
	record, err := state.signalIdempotency.Get(ctx, state.signalIdempotencyID(key))
	if err != nil {
		t.Fatalf("load signal idempotency record: %v", err)
	}
	if recordString(record, "status") != "completed" || len(recordBytes(record, "response_payload")) == 0 {
		t.Fatalf("signal idempotency record = %#v, want completed response", record)
	}
}

func TestSignalOrStartRunUsesExplicitSignalIDForStartWorkflowID(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	tc := &recordingTemporalClient{}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
		IdempotencyRetention:        time.Hour,
	}, tc, nil, state)
	signal := &proto.WorkflowSignal{Id: "signal-id-1", Name: "slack.event"}
	if _, err := backend.SignalOrStartRun(ctx, &proto.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey: "thread-1",
		Target:      pluginTarget("slack", "postMessage"),
		CreatedBy:   &proto.WorkflowActor{SubjectId: "user-1"},
		Signal:      signal,
	}); err != nil {
		t.Fatalf("SignalOrStartRun: %v", err)
	}
	expectedWorkflowID := workflowID("scope", "signal-keyed-v4", "slack", explicitSignalLedgerKey(signal), hashID("thread-1"))
	if len(tc.executions) != 1 || tc.executions[0].WorkflowID != expectedWorkflowID {
		t.Fatalf("executions = %#v, want workflow id %q", tc.executions, expectedWorkflowID)
	}
}

func TestSignalOrStartRunRejectsExplicitSignalIDPayloadMismatchWithOwnerKey(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	tc := &recordingTemporalClient{}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
		IdempotencyRetention:        time.Hour,
	}, tc, nil, state)
	req := &proto.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey: "thread-1",
		Target:      pluginTarget("slack", "postMessage"),
		CreatedBy:   &proto.WorkflowActor{SubjectId: "user-1"},
		Signal:      &proto.WorkflowSignal{Id: "signal-id-1", Name: "slack.event", IdempotencyKey: "owner-key-1"},
	}
	if _, err := backend.SignalOrStartRun(ctx, req); err != nil {
		t.Fatalf("SignalOrStartRun(first): %v", err)
	}
	updateCount := len(tc.updates)
	duplicateReq := cloneSignalOrStartRequest(req)
	duplicateReq.Signal.Name = "slack.changed"
	_, err = backend.SignalOrStartRun(ctx, duplicateReq)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("SignalOrStartRun(conflict) error = %v, want FailedPrecondition", err)
	}
	if len(tc.updates) != updateCount {
		t.Fatalf("updates = %#v, want no second temporal signal", tc.updates[updateCount:])
	}
}

func TestSignalOrStartRunSignalsExistingV4Workflow(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	tc := &recordingTemporalClient{}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
	}, tc, nil, state)
	run, err := backend.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		WorkflowKey: "thread-1",
		Target:      pluginTarget("slack", "postMessage"),
		CreatedBy:   &proto.WorkflowActor{SubjectId: "user-1"},
	})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	handle, err := decodeTemporalRunHandle(run.GetId())
	if err != nil {
		t.Fatalf("decode run handle: %v", err)
	}
	updateStart := len(tc.updates)

	resp, err := backend.SignalOrStartRun(ctx, &proto.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey: "thread-1",
		Target:      pluginTarget("slack", "sendMessage"),
		CreatedBy:   &proto.WorkflowActor{SubjectId: "user-2"},
		Signal:      &proto.WorkflowSignal{Name: "slack.event"},
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun: %v", err)
	}
	if resp.GetStartedRun() || resp.GetRun().GetId() != run.GetId() {
		t.Fatalf("response = %#v, want existing run without started flag", resp)
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions = %d, want no new run", len(tc.executions))
	}
	newUpdates := tc.updates[updateStart:]
	if len(newUpdates) != 1 {
		t.Fatalf("new updates = %#v, want one direct signal update", newUpdates)
	}
	if newUpdates[0].WorkflowID != handle.RunWorkflowID || newUpdates[0].Name != updateAddSignal {
		t.Fatalf("update = %#v, want updateAddSignal on run workflow %q", newUpdates[0], handle.RunWorkflowID)
	}
	if newUpdates[0].WaitForStage != client.WorkflowUpdateStageCompleted {
		t.Fatalf("signal update wait stage = %v, want completed", newUpdates[0].WaitForStage)
	}
}

func TestSignalOrStartRunReplacesTerminalWorkflowKeyOwner(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	tc := &recordingTemporalClient{}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
	}, tc, nil, state)
	first, err := backend.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		WorkflowKey: "thread-1",
		Target:      pluginTarget("slack", "postMessage"),
		CreatedBy:   &proto.WorkflowActor{SubjectId: "user-1"},
	})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	terminal := cloneRun(first)
	terminal.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED
	if err := state.putRun(ctx, terminal); err != nil {
		t.Fatalf("put terminal run: %v", err)
	}

	resp, err := backend.SignalOrStartRun(ctx, &proto.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey: "thread-1",
		Target:      pluginTarget("slack", "sendMessage"),
		CreatedBy:   &proto.WorkflowActor{SubjectId: "user-2"},
		Signal:      &proto.WorkflowSignal{Name: "slack.event"},
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun: %v", err)
	}
	if !resp.GetStartedRun() || resp.GetRun().GetId() == first.GetId() {
		t.Fatalf("response = %#v, want replacement run", resp)
	}
	if len(tc.executions) != 2 {
		t.Fatalf("executions = %d, want replacement execution", len(tc.executions))
	}
	owned, found, err := state.getWorkflowKeyRun(ctx, "thread-1")
	if err != nil || !found || owned.GetId() != resp.GetRun().GetId() {
		t.Fatalf("owned found=%v run=%#v err=%v, want replacement", found, owned, err)
	}
}

func TestSignalOrStartRunReplacesMissingWorkflowKeyOwner(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	stale := workflowKeyClaimRun("stale", "thread-1", proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING)
	if _, claimed, err := state.claimWorkflowKeyRun(ctx, "thread-1", stale, time.Unix(100, 0).UTC()); err != nil || !claimed {
		t.Fatalf("claim stale run claimed=%v err=%v", claimed, err)
	}
	tc := &recordingTemporalClient{updateErrs: []error{serviceerror.NewNotFound("missing workflow")}}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
	}, tc, nil, state)
	resp, err := backend.SignalOrStartRun(ctx, &proto.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey: "thread-1",
		Target:      pluginTarget("slack", "sendMessage"),
		CreatedBy:   &proto.WorkflowActor{SubjectId: "user-2"},
		Signal:      &proto.WorkflowSignal{Name: "slack.event"},
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun: %v", err)
	}
	if !resp.GetStartedRun() || resp.GetRun().GetId() == stale.GetId() {
		t.Fatalf("response = %#v, want replacement run", resp)
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions = %d, want replacement execution", len(tc.executions))
	}
	owned, found, err := state.getWorkflowKeyRun(ctx, "thread-1")
	if err != nil || !found || owned.GetId() != resp.GetRun().GetId() {
		t.Fatalf("owned found=%v run=%#v err=%v, want replacement", found, owned, err)
	}
}

func TestSignalRunUsesIndexedDBSignalIdempotency(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	run := workflowKeyClaimRun("signal-idem", "thread-1", proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING)
	tc := &recordingTemporalClient{}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
		IdempotencyRetention:        time.Hour,
	}, tc, nil, state)
	req := &proto.SignalWorkflowProviderRunRequest{
		RunId:  run.GetId(),
		Signal: &proto.WorkflowSignal{Name: "slack.event", IdempotencyKey: "signal-1"},
	}
	first, err := backend.SignalRun(ctx, req)
	if err != nil {
		t.Fatalf("SignalRun(first): %v", err)
	}
	if len(tc.updates) != 1 || tc.updates[0].Name != updateAddSignal || tc.updates[0].UpdateID != "signal-key:"+hashID("signal-1") {
		t.Fatalf("first update = %#v, want add-signal keyed only by idempotency key", tc.updates)
	}
	updateCount := len(tc.updates)
	duplicateReq := cloneSignalRunRequest(req)
	duplicateReq.Signal.Name = "slack.changed"
	duplicate, err := backend.SignalRun(ctx, duplicateReq)
	if err != nil {
		t.Fatalf("SignalRun(duplicate different payload): %v", err)
	}
	if duplicate.GetSignal().GetId() != first.GetSignal().GetId() {
		t.Fatalf("duplicate signal = %#v, want first signal %#v", duplicate.GetSignal(), first.GetSignal())
	}
	if len(tc.updates) != updateCount {
		t.Fatalf("updates = %#v, want no duplicate temporal signal", tc.updates[updateCount:])
	}
	key := ownerIdempotencyLedgerKey("slack", "signal-1")
	record, err := state.signalIdempotency.Get(ctx, state.signalIdempotencyID(key))
	if err != nil {
		t.Fatalf("load signal idempotency record: %v", err)
	}
	if recordString(record, "status") != "completed" || recordString(record, "run_id") != run.GetId() {
		t.Fatalf("signal idempotency record = %#v, want completed run %q", record, run.GetId())
	}
}

func TestSignalRunRejectsExplicitSignalIDPayloadMismatchWithOwnerKey(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	run := workflowKeyClaimRun("strict-signal-id", "thread-1", proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING)
	tc := &recordingTemporalClient{}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
		IdempotencyRetention:        time.Hour,
	}, tc, nil, state)
	if _, err := backend.SignalRun(ctx, &proto.SignalWorkflowProviderRunRequest{
		RunId:  run.GetId(),
		Signal: &proto.WorkflowSignal{Id: "signal-id-1", Name: "slack.event", IdempotencyKey: "owner-key-1"},
	}); err != nil {
		t.Fatalf("SignalRun(first): %v", err)
	}
	updateCount := len(tc.updates)
	_, err = backend.SignalRun(ctx, &proto.SignalWorkflowProviderRunRequest{
		RunId:  run.GetId(),
		Signal: &proto.WorkflowSignal{Id: "signal-id-1", Name: "slack.changed", IdempotencyKey: "owner-key-1"},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("SignalRun(conflict) error = %v, want FailedPrecondition", err)
	}
	if len(tc.updates) != updateCount {
		t.Fatalf("updates = %#v, want no second temporal signal", tc.updates[updateCount:])
	}
}

func TestSignalRunRejectsSignalIdempotencyWithoutIndexedDB(t *testing.T) {
	ctx := context.Background()
	run := workflowKeyClaimRun("missing-state", "thread-1", proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING)
	tc := &recordingTemporalClient{}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
		IdempotencyRetention:        time.Hour,
	}, tc, nil, nil)

	_, err := backend.SignalRun(ctx, &proto.SignalWorkflowProviderRunRequest{
		RunId:  run.GetId(),
		Signal: &proto.WorkflowSignal{Name: "slack.event", IdempotencyKey: "signal-1"},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("SignalRun error = %v, want FailedPrecondition", err)
	}
	if len(tc.updates) != 0 {
		t.Fatalf("updates = %#v, want no temporal signal after idempotency failure", tc.updates)
	}
}

func TestWorkflowStateStoreClaimsWorkflowKeyRun(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	workflowKey := "slack:T:C:1778164397.804829"
	now := time.Unix(200, 0).UTC()
	run := workflowKeyClaimRun("first", workflowKey, proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING)
	owner, claimed, err := state.claimWorkflowKeyRun(ctx, workflowKey, run, now)
	if err != nil {
		t.Fatalf("claimWorkflowKeyRun: %v", err)
	}
	if !claimed || owner.GetId() != run.GetId() {
		t.Fatalf("claim owner=%q claimed=%v, want caller", owner.GetId(), claimed)
	}
	got, found, err := state.getWorkflowKeyRun(ctx, workflowKey)
	if err != nil || !found || got.GetId() != run.GetId() {
		t.Fatalf("getWorkflowKeyRun found=%v run=%#v err=%v, want first run", found, got, err)
	}
	record, err := state.workflowKeys.Get(ctx, state.workflowKeyID(workflowKey))
	if err != nil {
		t.Fatalf("load workflow key record: %v", err)
	}
	handle, err := decodeTemporalRunHandle(run.GetId())
	if err != nil {
		t.Fatalf("decode run handle: %v", err)
	}
	if recordString(record, "id") != state.workflowKeyID(workflowKey) ||
		recordString(record, "scope_id") != "scope" ||
		recordString(record, "workflow_key") != workflowKey ||
		recordString(record, "owner_key") != "slack" ||
		recordString(record, "run_id") != run.GetId() ||
		recordString(record, "temporal_workflow_id") != handle.RunWorkflowID ||
		recordString(record, "temporal_run_id") != handle.RunTemporalRunID ||
		recordInt64(record, "status") != int64(proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING) {
		t.Fatalf("workflow key record = %#v, want routing metadata for first run", record)
	}
	if createdAt, updatedAt := recordTime(record, "created_at"), recordTime(record, "updated_at"); createdAt == nil || updatedAt == nil || !createdAt.Equal(now) || !updatedAt.Equal(now) {
		t.Fatalf("record timestamps created=%v updated=%v, want %v", createdAt, updatedAt, now)
	}

	running := cloneRun(run)
	running.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING
	owner, claimed, err = state.claimWorkflowKeyRun(ctx, workflowKey, running, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("claimWorkflowKeyRun(same run): %v", err)
	}
	if !claimed || owner.GetId() != run.GetId() || owner.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING {
		t.Fatalf("same-run claim owner=%#v claimed=%v, want running caller", owner, claimed)
	}
	other := workflowKeyClaimRun("other", workflowKey, proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING)
	owner, claimed, err = state.claimWorkflowKeyRun(ctx, workflowKey, other, now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("claimWorkflowKeyRun(conflict): %v", err)
	}
	if claimed || owner.GetId() != run.GetId() {
		t.Fatalf("conflict owner=%q claimed=%v, want existing run", owner.GetId(), claimed)
	}

	cleared, err := state.clearWorkflowKeyRun(ctx, workflowKey, other.GetId())
	if err != nil {
		t.Fatalf("clearWorkflowKeyRun(wrong run): %v", err)
	}
	if cleared {
		t.Fatalf("clearWorkflowKeyRun(wrong run) = true, want false")
	}
	cleared, err = state.clearWorkflowKeyRun(ctx, workflowKey, run.GetId())
	if err != nil {
		t.Fatalf("clearWorkflowKeyRun: %v", err)
	}
	if !cleared {
		t.Fatalf("clearWorkflowKeyRun = false, want true")
	}
	if _, found, err := state.getWorkflowKeyRun(ctx, workflowKey); err != nil || found {
		t.Fatalf("getWorkflowKeyRun after clear found=%v err=%v, want not found", found, err)
	}
}

func TestWorkflowStateStoreWorkflowKeyClaimReplacesTerminalOrMissingProjection(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	workflowKey := "thread-terminal"
	terminal := workflowKeyClaimRun("terminal", workflowKey, proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED)
	if _, claimed, err := state.claimWorkflowKeyRun(ctx, workflowKey, terminal, time.Unix(100, 0).UTC()); err != nil || !claimed {
		t.Fatalf("claim terminal claimed=%v err=%v", claimed, err)
	}
	got, found, err := state.getWorkflowKeyRun(ctx, workflowKey)
	if err != nil || !found || got.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED {
		t.Fatalf("get terminal found=%v run=%#v err=%v, want terminal owner", found, got, err)
	}
	stale := cloneRun(terminal)
	stale.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING
	owner, claimed, err := state.claimWorkflowKeyRun(ctx, workflowKey, stale, time.Unix(150, 0).UTC())
	if err != nil {
		t.Fatalf("claim stale terminal owner: %v", err)
	}
	if !claimed || owner.GetId() != terminal.GetId() || owner.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED {
		t.Fatalf("stale terminal owner=%#v claimed=%v, want terminal projection", owner, claimed)
	}
	replacement := workflowKeyClaimRun("replacement", workflowKey, proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING)
	owner, claimed, err = state.claimWorkflowKeyRun(ctx, workflowKey, replacement, time.Unix(200, 0).UTC())
	if err != nil {
		t.Fatalf("claim replacement: %v", err)
	}
	if !claimed || owner.GetId() != replacement.GetId() {
		t.Fatalf("replacement owner=%q claimed=%v, want replacement", owner.GetId(), claimed)
	}

	missingKey := "thread-missing"
	missingRun := workflowKeyClaimRun("missing", missingKey, proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING)
	missingHandle, err := decodeTemporalRunHandle(missingRun.GetId())
	if err != nil {
		t.Fatalf("decode missing run handle: %v", err)
	}
	if err := state.workflowKeys.Put(ctx, state.workflowKeyRecord(workflowKeyRecord{
		ID:                 state.workflowKeyID(missingKey),
		WorkflowKey:        missingKey,
		OwnerKey:           "slack",
		RunID:              missingRun.GetId(),
		TemporalWorkflowID: missingHandle.RunWorkflowID,
		TemporalRunID:      missingHandle.RunTemporalRunID,
		Status:             proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		CreatedAt:          time.Unix(300, 0).UTC(),
		UpdatedAt:          time.Unix(300, 0).UTC(),
	})); err != nil {
		t.Fatalf("seed missing workflow key: %v", err)
	}
	missingReplacement := workflowKeyClaimRun("missing-replacement", missingKey, proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING)
	owner, claimed, err = state.claimWorkflowKeyRun(ctx, missingKey, missingReplacement, time.Unix(400, 0).UTC())
	if err != nil {
		t.Fatalf("claim missing replacement: %v", err)
	}
	if !claimed || owner.GetId() != missingReplacement.GetId() {
		t.Fatalf("missing replacement owner=%q claimed=%v, want replacement", owner.GetId(), claimed)
	}
}

func TestWorkflowStateStoreWorkflowKeyClaimValidationAndScopeIsolation(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	scopeA, err := openWorkflowStateStore(ctx, "", "scope-a")
	if err != nil {
		t.Fatalf("open scope-a: %v", err)
	}
	t.Cleanup(func() { _ = scopeA.Close() })
	scopeB, err := openWorkflowStateStore(ctx, "", "scope-b")
	if err != nil {
		t.Fatalf("open scope-b: %v", err)
	}
	t.Cleanup(func() { _ = scopeB.Close() })

	valid := workflowKeyClaimRun("valid", "thread", proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING)
	for name, tc := range map[string]struct {
		workflowKey string
		run         *proto.BoundWorkflowRun
	}{
		"empty workflow key": {workflowKey: "", run: valid},
		"nil run":            {workflowKey: "thread", run: nil},
		"empty run id":       {workflowKey: "thread", run: &proto.BoundWorkflowRun{}},
		"malformed run id":   {workflowKey: "thread", run: &proto.BoundWorkflowRun{Id: "not-a-handle", WorkflowKey: "thread", Target: pluginTarget("slack", "postMessage")}},
		"missing temporal run id": {
			workflowKey: "thread",
			run: &proto.BoundWorkflowRun{
				Id:          encodeTemporalRunHandle(temporalRunHandle{RunWorkflowID: "workflow-without-run-id", WorkflowKey: "thread", OwnerKey: "slack"}),
				WorkflowKey: "thread",
				Target:      pluginTarget("slack", "postMessage"),
			},
		},
	} {
		if _, _, err := scopeA.claimWorkflowKeyRun(ctx, tc.workflowKey, tc.run, time.Unix(100, 0).UTC()); err == nil {
			t.Fatalf("%s claim succeeded, want error", name)
		}
	}

	runA := workflowKeyClaimRun("scope-a", "shared-thread", proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING)
	runB := workflowKeyClaimRun("scope-b", "shared-thread", proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING)
	if _, claimed, err := scopeA.claimWorkflowKeyRun(ctx, "shared-thread", runA, time.Unix(200, 0).UTC()); err != nil || !claimed {
		t.Fatalf("scopeA claim claimed=%v err=%v", claimed, err)
	}
	if _, claimed, err := scopeB.claimWorkflowKeyRun(ctx, "shared-thread", runB, time.Unix(200, 0).UTC()); err != nil || !claimed {
		t.Fatalf("scopeB claim claimed=%v err=%v", claimed, err)
	}
	gotA, found, err := scopeA.getWorkflowKeyRun(ctx, "shared-thread")
	if err != nil || !found || gotA.GetId() != runA.GetId() {
		t.Fatalf("scopeA get found=%v run=%#v err=%v, want runA", found, gotA, err)
	}
	gotB, found, err := scopeB.getWorkflowKeyRun(ctx, "shared-thread")
	if err != nil || !found || gotB.GetId() != runB.GetId() {
		t.Fatalf("scopeB get found=%v run=%#v err=%v, want runB", found, gotB, err)
	}
}

func TestWorkflowStateStoreWorkflowKeyConcurrentClaim(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	workflowKey := "thread-race"
	runs := []*proto.BoundWorkflowRun{
		workflowKeyClaimRun("race-a", workflowKey, proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING),
		workflowKeyClaimRun("race-b", workflowKey, proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING),
	}
	type claimResult struct {
		owner   *proto.BoundWorkflowRun
		claimed bool
		err     error
	}
	start := make(chan struct{})
	results := make(chan claimResult, len(runs))
	for _, run := range runs {
		run := run
		go func() {
			<-start
			owner, claimed, err := state.claimWorkflowKeyRun(ctx, workflowKey, run, time.Unix(500, 0).UTC())
			results <- claimResult{owner: owner, claimed: claimed, err: err}
		}()
	}
	close(start)

	claimedCount := 0
	var winner string
	for range runs {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent claim error: %v", result.err)
		}
		if result.owner == nil || result.owner.GetId() == "" {
			t.Fatalf("concurrent claim owner = %#v, want owner", result.owner)
		}
		if result.claimed {
			claimedCount++
			winner = result.owner.GetId()
		}
	}
	if claimedCount != 1 {
		t.Fatalf("claimed count = %d, want 1", claimedCount)
	}
	got, found, err := state.getWorkflowKeyRun(ctx, workflowKey)
	if err != nil || !found || got.GetId() != winner {
		t.Fatalf("stored owner found=%v run=%#v err=%v, want winner %q", found, got, err, winner)
	}
}

func TestWorkflowStateStoreIgnoresUnsupportedRunHandleRecords(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	legacyID := encodeTemporalRunHandle(temporalRunHandle{
		Kind:             "temporal-run-v3",
		RunWorkflowID:    "legacy-workflow",
		RunTemporalRunID: "legacy-run",
		WorkflowKey:      "legacy-thread",
		OwnerKey:         "slack",
	})
	currentID := encodeTemporalRunHandle(temporalRunHandle{
		RunWorkflowID:    "current-workflow",
		RunTemporalRunID: "current-run",
		WorkflowKey:      "current-thread",
		OwnerKey:         "slack",
	})
	legacyRun := &proto.BoundWorkflowRun{Id: legacyID, Status: proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING, Target: pluginTarget("slack", "postMessage"), WorkflowKey: "legacy-thread"}
	currentRun := &proto.BoundWorkflowRun{Id: currentID, Status: proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING, Target: pluginTarget("slack", "postMessage"), WorkflowKey: "current-thread"}
	if err := state.runProjections.Put(ctx, state.runRecord(legacyRun)); err != nil {
		t.Fatalf("put legacy run projection: %v", err)
	}
	if err := state.putRun(ctx, currentRun); err != nil {
		t.Fatalf("put current run: %v", err)
	}
	if err := state.workflowKeys.Put(ctx, state.workflowKeyRecord(workflowKeyRecord{
		ID:                 state.workflowKeyID("legacy-thread"),
		WorkflowKey:        "legacy-thread",
		OwnerKey:           "slack",
		RunID:              legacyID,
		TemporalWorkflowID: "legacy-workflow",
		TemporalRunID:      "legacy-run",
		Status:             proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		CreatedAt:          time.Unix(100, 0).UTC(),
		UpdatedAt:          time.Unix(100, 0).UTC(),
	})); err != nil {
		t.Fatalf("put legacy workflow key: %v", err)
	}
	if err := state.workflowKeys.Put(ctx, state.workflowKeyRecord(workflowKeyRecord{
		ID:                 state.workflowKeyID("current-thread"),
		WorkflowKey:        "current-thread",
		OwnerKey:           "slack",
		RunID:              currentID,
		TemporalWorkflowID: "current-workflow",
		TemporalRunID:      "current-run",
		Status:             proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		CreatedAt:          time.Unix(100, 0).UTC(),
		UpdatedAt:          time.Unix(100, 0).UTC(),
	})); err != nil {
		t.Fatalf("put current workflow key: %v", err)
	}

	if _, found, err := state.getRun(ctx, legacyID); err != nil || found {
		t.Fatalf("legacy run found=%v err=%v, want ignored", found, err)
	}
	if _, found, err := state.getRun(ctx, currentID); err != nil || !found {
		t.Fatalf("current run found=%v err=%v, want preserved", found, err)
	}
	if _, found, err := state.getWorkflowKeyRun(ctx, "legacy-thread"); err != nil || found {
		t.Fatalf("legacy workflow key found=%v err=%v, want ignored", found, err)
	}
	if _, found, err := state.getWorkflowKeyRun(ctx, "current-thread"); err != nil || !found {
		t.Fatalf("current workflow key found=%v err=%v, want preserved", found, err)
	}
	replacementID := encodeTemporalRunHandle(temporalRunHandle{
		RunWorkflowID:    "replacement-workflow",
		RunTemporalRunID: "replacement-run",
		WorkflowKey:      "legacy-thread",
		OwnerKey:         "slack",
	})
	replacementRun := &proto.BoundWorkflowRun{Id: replacementID, Status: proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING, Target: pluginTarget("slack", "postMessage"), WorkflowKey: "legacy-thread"}
	owner, claimed, err := state.claimWorkflowKeyRun(ctx, "legacy-thread", replacementRun, time.Unix(200, 0).UTC())
	if err != nil {
		t.Fatalf("claim replacement over unsupported owner: %v", err)
	}
	if !claimed || owner.GetId() != replacementID {
		t.Fatalf("replacement claim owner=%q claimed=%v, want replacement", owner.GetId(), claimed)
	}
	tc := &recordingTemporalClient{}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
	}, tc, nil, state)
	if _, err := backend.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{RunId: legacyID}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GetRun legacy projection error = %v, want InvalidArgument", err)
	}
	listed, err := backend.ListRuns(ctx, &proto.ListWorkflowProviderRunsRequest{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	for _, run := range listed.GetRuns() {
		if run.GetId() == legacyID {
			t.Fatalf("ListRuns included legacy projection %#v", run)
		}
	}
}

func TestListRunsIncludesIndexedDBRunProjections(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })
	run := &proto.BoundWorkflowRun{
		Id: encodeTemporalRunHandle(temporalRunHandle{
			RunWorkflowID:    "run-projected-workflow",
			RunTemporalRunID: "run-projected-temporal-run",
			OwnerKey:         "slack",
		}),
		Status:    proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED,
		Target:    pluginTarget("slack", "postMessage"),
		Trigger:   newManualTrigger(),
		CreatedAt: timestamppb.New(time.Unix(100, 0).UTC()),
	}
	if err := state.putRun(ctx, run); err != nil {
		t.Fatalf("putRun: %v", err)
	}

	tc := &recordingTemporalClient{}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
	}, tc, nil, state)
	resp, err := backend.ListRuns(ctx, &proto.ListWorkflowProviderRunsRequest{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(resp.GetRuns()) != 1 || resp.GetRuns()[0].GetId() != run.GetId() {
		t.Fatalf("runs = %#v, want projected run", resp.GetRuns())
	}
	if len(tc.queries) != 0 || len(tc.updates) != 0 {
		t.Fatalf("temporal calls queries=%#v updates=%#v, want indexeddb-only list", tc.queries, tc.updates)
	}
}

func TestTriggerMatchKeysAreReplacedAtomically(t *testing.T) {
	startTestIndexedDBBackend(t)
	state, err := openWorkflowStateStore(context.Background(), "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
	}, &recordingTemporalClient{}, nil, state)

	trigger := &proto.BoundWorkflowEventTrigger{
		Id:        "trigger-1",
		Match:     &proto.WorkflowEventMatch{Type: "message.created"},
		Target:    pluginTarget("slack", "postMessage"),
		CreatedAt: timestamppb.Now(),
		UpdatedAt: timestamppb.Now(),
	}
	if err := backend.putTriggerIndex(context.Background(), trigger); err != nil {
		t.Fatalf("putTriggerIndex(first): %v", err)
	}
	trigger.Match = &proto.WorkflowEventMatch{Type: "reaction.added"}
	if err := backend.putTriggerIndex(context.Background(), trigger); err != nil {
		t.Fatalf("putTriggerIndex(second): %v", err)
	}
	oldMatches, err := backend.matchTriggersIndex(context.Background(), "slack", &proto.WorkflowEvent{Type: "message.created"})
	if err != nil {
		t.Fatalf("match old: %v", err)
	}
	if len(oldMatches) != 0 {
		t.Fatalf("old match returned %#v, want none", oldMatches)
	}
	newMatches, err := backend.matchTriggersIndex(context.Background(), "slack", &proto.WorkflowEvent{Type: "reaction.added"})
	if err != nil {
		t.Fatalf("match new: %v", err)
	}
	if len(newMatches) != 1 || newMatches[0].GetId() != trigger.GetId() {
		t.Fatalf("new match returned %#v, want %q", newMatches, trigger.GetId())
	}
}

func TestPublishEventRecordsMatchedTriggersAndStartedRuns(t *testing.T) {
	startTestIndexedDBBackend(t)
	state, err := openWorkflowStateStore(context.Background(), "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		_ = provider.Shutdown(context.Background())
	})

	tc := &recordingTemporalClient{}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
	}, tc, nil, state)
	for _, trigger := range []*proto.BoundWorkflowEventTrigger{
		{
			Id:        "trigger-plugin-1",
			Match:     &proto.WorkflowEventMatch{Type: "message.created"},
			Target:    pluginTarget("slack", "postMessage"),
			CreatedAt: timestamppb.Now(),
			UpdatedAt: timestamppb.Now(),
		},
		{
			Id:        "trigger-plugin-2",
			Match:     &proto.WorkflowEventMatch{Type: "message.created"},
			Target:    pluginTarget("slack", "sendMessage"),
			CreatedAt: timestamppb.Now(),
			UpdatedAt: timestamppb.Now(),
		},
		{
			Id:        "trigger-paused",
			Match:     &proto.WorkflowEventMatch{Type: "message.created"},
			Target:    pluginTarget("slack", "archiveMessage"),
			Paused:    true,
			CreatedAt: timestamppb.Now(),
			UpdatedAt: timestamppb.Now(),
		},
	} {
		if err := backend.putTriggerIndex(context.Background(), trigger); err != nil {
			t.Fatalf("putTriggerIndex(%s): %v", trigger.GetId(), err)
		}
	}

	_, err = backend.PublishEvent(context.Background(), &proto.PublishWorkflowProviderEventRequest{
		PluginName: "slack",
		Event:      &proto.WorkflowEvent{Id: "event-1", Source: "slack", Type: "message.created"},
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	if len(tc.executions) != 2 {
		t.Fatalf("executions = %d, want 2", len(tc.executions))
	}

	rm := collectTemporalWorkflowMetrics(t, reader)
	metrictestAttrs := temporalWorkflowMetricAttrs(
		gestalt.WorkflowOperationPublishEvent,
		gestalt.WorkflowTriggerKindEvent,
		gestalt.WorkflowTargetKindPlugin,
		gestalt.WorkflowRunStatusUnknown,
	)
	requireTemporalInt64Sum(t, rm, "gestaltd.workflows.events.matched_triggers.count", 2, metrictestAttrs)
	requireTemporalInt64Sum(t, rm, "gestaltd.workflows.runs.started.count", 2, temporalWorkflowMetricAttrs(
		gestalt.WorkflowOperationPublishEvent,
		gestalt.WorkflowTriggerKindEvent,
		gestalt.WorkflowTargetKindPlugin,
		gestalt.WorkflowRunStatusPending,
	))
}

func TestWorkflowStateStoreScopesMetadataByScopeID(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	scopeA, err := openWorkflowStateStore(ctx, "", "scope-a")
	if err != nil {
		t.Fatalf("open scope-a: %v", err)
	}
	t.Cleanup(func() { _ = scopeA.Close() })
	scopeB, err := openWorkflowStateStore(ctx, "", "scope-b")
	if err != nil {
		t.Fatalf("open scope-b: %v", err)
	}
	t.Cleanup(func() { _ = scopeB.Close() })

	refA := &proto.WorkflowExecutionReference{
		Id:           "ref-1",
		ProviderName: "temporal",
		Target:       pluginTarget("slack", "postMessage"),
		SubjectId:    "user-1",
		CreatedAt:    timestamppb.Now(),
		RunAs: &proto.WorkflowRunAsSubject{
			SubjectId:   "service_account:slack-sync",
			SubjectKind: "service_account",
			DisplayName: "Slack sync",
			AuthSource:  "config",
		},
	}
	refB := cloneExecutionReference(refA)
	refB.SubjectId = "user-2"
	if err := scopeA.putExecutionRef(ctx, refA); err != nil {
		t.Fatalf("scopeA put ref: %v", err)
	}
	if err := scopeB.putExecutionRef(ctx, refB); err != nil {
		t.Fatalf("scopeB put ref: %v", err)
	}
	gotA, found, err := scopeA.getExecutionRef(ctx, "ref-1")
	if err != nil || !found {
		t.Fatalf("scopeA get ref found=%v err=%v", found, err)
	}
	gotB, found, err := scopeB.getExecutionRef(ctx, "ref-1")
	if err != nil || !found {
		t.Fatalf("scopeB get ref found=%v err=%v", found, err)
	}
	if gotA.GetSubjectId() != "user-1" || gotB.GetSubjectId() != "user-2" {
		t.Fatalf("scoped refs leaked: scopeA=%q scopeB=%q", gotA.GetSubjectId(), gotB.GetSubjectId())
	}
	if gotA.GetRunAs().GetSubjectId() != "service_account:slack-sync" || gotB.GetRunAs().GetSubjectId() != "service_account:slack-sync" {
		t.Fatalf("scoped ref run_as lost: scopeA=%#v scopeB=%#v", gotA.GetRunAs(), gotB.GetRunAs())
	}

	trigger := &proto.BoundWorkflowEventTrigger{Id: "trigger-1", Match: &proto.WorkflowEventMatch{Type: "message.created"}, Target: pluginTarget("slack", "postMessage"), CreatedAt: timestamppb.Now(), UpdatedAt: timestamppb.Now()}
	if err := scopeA.putTrigger(ctx, trigger); err != nil {
		t.Fatalf("scopeA put trigger: %v", err)
	}
	matchesB, err := scopeB.matchTriggers(ctx, "slack", &proto.WorkflowEvent{Type: "message.created"})
	if err != nil {
		t.Fatalf("scopeB match: %v", err)
	}
	if len(matchesB) != 0 {
		t.Fatalf("scopeB matched scopeA triggers: %#v", matchesB)
	}
}

func TestProviderSurfaceRequiresConfiguredBackend(t *testing.T) {
	provider := New()
	_, err := provider.ListRuns(context.Background(), &proto.ListWorkflowProviderRunsRequest{})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ListRuns error = %v, want FailedPrecondition", err)
	}
}

func TestProviderSurfaceDelegatesWorkflowRPCs(t *testing.T) {
	backend := &fakeBackend{
		listRuns: &proto.ListWorkflowProviderRunsResponse{Runs: []*proto.BoundWorkflowRun{{Id: "run-1"}}},
	}
	provider := &Provider{name: "temporal", backend: backend}
	resp, err := provider.ListRuns(context.Background(), &proto.ListWorkflowProviderRunsRequest{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(resp.GetRuns()) != 1 || resp.GetRuns()[0].GetId() != "run-1" {
		t.Fatalf("ListRuns response = %#v", resp)
	}
	if !backend.calledListRuns {
		t.Fatalf("backend ListRuns was not called")
	}
	if backend.startCount != 0 {
		t.Fatalf("metadata ListRuns started backend %d times, want 0", backend.startCount)
	}
}

func TestProviderSurfaceStartsBackendForExecutionRPCs(t *testing.T) {
	backend := &fakeBackend{
		startErr: errors.New("worker unavailable"),
	}
	provider := &Provider{name: "temporal", backend: backend}
	_, err := provider.StartRun(context.Background(), &proto.StartWorkflowProviderRunRequest{})
	if status.Code(err) != codes.Internal {
		t.Fatalf("StartRun error = %v, want Internal", err)
	}
	if backend.startCount != 1 {
		t.Fatalf("backend Start calls = %d, want 1", backend.startCount)
	}
}

type capturingHost struct {
	resp  *proto.InvokeWorkflowOperationResponse
	err   error
	calls []*proto.InvokeWorkflowOperationRequest
}

func (h *capturingHost) InvokeOperation(_ context.Context, req *proto.InvokeWorkflowOperationRequest) (*proto.InvokeWorkflowOperationResponse, error) {
	h.calls = append(h.calls, req)
	return h.resp, h.err
}

func (h *capturingHost) Close() error { return nil }

func workflowKeyClaimRun(suffix, workflowKey string, status proto.WorkflowRunStatus) *proto.BoundWorkflowRun {
	return &proto.BoundWorkflowRun{
		Id: encodeTemporalRunHandle(temporalRunHandle{
			RunWorkflowID:    "temporal-workflow-" + strings.TrimSpace(suffix),
			RunTemporalRunID: "temporal-run-" + strings.TrimSpace(suffix),
			WorkflowKey:      strings.TrimSpace(workflowKey),
			OwnerKey:         "slack",
		}),
		Status:      status,
		Target:      pluginTarget("slack", "postMessage"),
		WorkflowKey: strings.TrimSpace(workflowKey),
		CreatedAt:   timestamppb.New(time.Unix(100, 0).UTC()),
	}
}

func cloneSignalOrStartRequest(req *proto.SignalOrStartWorkflowProviderRunRequest) *proto.SignalOrStartWorkflowProviderRunRequest {
	if req == nil {
		return nil
	}
	return &proto.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  req.GetWorkflowKey(),
		Target:       cloneTarget(req.GetTarget()),
		Signal:       cloneSignal(req.GetSignal()),
		CreatedBy:    cloneActor(req.GetCreatedBy()),
		ExecutionRef: strings.TrimSpace(req.GetExecutionRef()),
	}
}

func cloneSignalRunRequest(req *proto.SignalWorkflowProviderRunRequest) *proto.SignalWorkflowProviderRunRequest {
	if req == nil {
		return nil
	}
	return &proto.SignalWorkflowProviderRunRequest{
		RunId:  strings.TrimSpace(req.GetRunId()),
		Signal: cloneSignal(req.GetSignal()),
	}
}

type blockingHost struct {
	mu           sync.Mutex
	resp         *proto.InvokeWorkflowOperationResponse
	err          error
	releaseFirst <-chan struct{}
	calls        []*proto.InvokeWorkflowOperationRequest
}

func (h *blockingHost) InvokeOperation(ctx context.Context, req *proto.InvokeWorkflowOperationRequest) (*proto.InvokeWorkflowOperationResponse, error) {
	h.mu.Lock()
	h.calls = append(h.calls, req)
	callIndex := len(h.calls)
	h.mu.Unlock()
	if callIndex == 1 && h.releaseFirst != nil {
		select {
		case <-h.releaseFirst:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return h.resp, h.err
}

func (h *blockingHost) Close() error { return nil }

func pluginTarget(plugin, operation string) *proto.BoundWorkflowTarget {
	input, _ := structpb.NewStruct(map[string]any{"text": "hello"})
	return &proto.BoundWorkflowTarget{Kind: &proto.BoundWorkflowTarget_Plugin{Plugin: &proto.BoundWorkflowPluginTarget{
		PluginName: strings.TrimSpace(plugin),
		Operation:  strings.TrimSpace(operation),
		Input:      input,
	}}}
}

func TestNormalizeTargetPreservesPluginCredentialMode(t *testing.T) {
	target := pluginTarget(" github ", " reviewPullRequest ")
	target.GetPlugin().CredentialMode = " none "

	scoped, err := normalizeTarget(target)
	if err != nil {
		t.Fatalf("normalizeTarget: %v", err)
	}
	plugin := scoped.Target.GetPlugin()
	if plugin.GetPluginName() != "github" || plugin.GetOperation() != "reviewPullRequest" {
		t.Fatalf("plugin target = %#v", plugin)
	}
	if got := plugin.GetCredentialMode(); got != "none" {
		t.Fatalf("credential mode = %q, want none", got)
	}
}

func TestNormalizeTargetRejectsInvalidPluginCredentialMode(t *testing.T) {
	target := pluginTarget("github", "reviewPullRequest")
	target.GetPlugin().CredentialMode = "platform"

	_, err := normalizeTarget(target)
	if err == nil || !strings.Contains(err.Error(), `target.plugin.credential_mode "platform" is not supported`) {
		t.Fatalf("normalizeTarget error = %v, want unsupported credential mode", err)
	}
}

func TestNormalizeTargetRejectsOutputDeliveryTargetCredentialMode(t *testing.T) {
	target := &proto.BoundWorkflowTarget{
		Kind: &proto.BoundWorkflowTarget_Agent{Agent: &proto.BoundWorkflowAgentTarget{
			ProviderName: "managed",
			Prompt:       "review",
			OutputDelivery: &proto.WorkflowOutputDelivery{
				Target: &proto.BoundWorkflowPluginTarget{
					PluginName:     "github",
					Operation:      "reviewPullRequest",
					CredentialMode: "none",
				},
				CredentialMode: "none",
			},
		}},
	}

	_, err := normalizeTarget(target)
	if err == nil || !strings.Contains(err.Error(), `target.agent.output_delivery.target.credential_mode "none" is not supported`) {
		t.Fatalf("normalizeTarget error = %v, want unsupported output delivery target mode", err)
	}
}

func TestNormalizeTargetPreservesSessionReadyDelivery(t *testing.T) {
	target := temporalAgentTargetWithSessionReadyDelivery()

	scoped, err := normalizeTarget(target)
	if err != nil {
		t.Fatalf("normalizeTarget: %v", err)
	}
	delivery := scoped.Target.GetAgent().GetSessionReadyDelivery()
	if delivery.GetTarget().GetPluginName() != "slack" || delivery.GetTarget().GetOperation() != "events.replySessionStarted" {
		t.Fatalf("session ready delivery target = %#v", delivery.GetTarget())
	}
	if delivery.GetCredentialMode() != "none" {
		t.Fatalf("session ready delivery credential mode = %q, want none", delivery.GetCredentialMode())
	}
	if got := delivery.GetInputBindings()[0].GetValue().GetAgentSession(); got != "id" {
		t.Fatalf("agent session source = %q, want id", got)
	}
}

func TestNormalizeTargetRejectsSessionReadyDeliveryAgentOutput(t *testing.T) {
	target := temporalAgentTargetWithSessionReadyDelivery()
	target.GetAgent().GetSessionReadyDelivery().InputBindings[0].Value = &proto.WorkflowOutputValueSource{
		Kind: &proto.WorkflowOutputValueSource_AgentOutput{AgentOutput: "text"},
	}

	_, err := normalizeTarget(target)
	if err == nil || !strings.Contains(err.Error(), "target.agent.session_ready_delivery.input_bindings.value.agent_output is not available before the agent turn starts") {
		t.Fatalf("normalizeTarget error = %v, want unsupported session ready delivery agent output", err)
	}
}

func TestExecutionReferencePermissionsIncludeSessionReadyDelivery(t *testing.T) {
	permissions := executionReferencePermissionsForTarget(temporalAgentTargetWithSessionReadyDelivery())
	got := map[string]map[string]bool{}
	for _, permission := range permissions {
		ops := got[permission.GetPlugin()]
		if ops == nil {
			ops = map[string]bool{}
			got[permission.GetPlugin()] = ops
		}
		for _, operation := range permission.GetOperations() {
			ops[operation] = true
		}
	}
	if !got["slack"]["events.replySessionStarted"] {
		t.Fatalf("permissions = %#v, missing slack/events.replySessionStarted", permissions)
	}
}

func temporalAgentTargetWithSessionReadyDelivery() *proto.BoundWorkflowTarget {
	return &proto.BoundWorkflowTarget{
		Kind: &proto.BoundWorkflowTarget_Agent{Agent: &proto.BoundWorkflowAgentTarget{
			ProviderName: "managed",
			Prompt:       "review",
			ToolRefs: []*proto.AgentToolRef{
				{Plugin: "slack", Operation: "chat.postMessage"},
			},
			SessionReadyDelivery: &proto.WorkflowOutputDelivery{
				Target: &proto.BoundWorkflowPluginTarget{
					PluginName: "slack",
					Operation:  "events.replySessionStarted",
				},
				CredentialMode: "none",
				InputBindings: []*proto.WorkflowOutputBinding{
					{
						InputField: "session_id",
						Value: &proto.WorkflowOutputValueSource{
							Kind: &proto.WorkflowOutputValueSource_AgentSession{AgentSession: "id"},
						},
					},
				},
			},
		}},
	}
}

func updateCallback(t *testing.T, onComplete func(interface{})) *testsuite.TestUpdateCallback {
	t.Helper()
	return &testsuite.TestUpdateCallback{
		OnReject: func(err error) { t.Fatalf("update rejected: %v", err) },
		OnAccept: func() {},
		OnComplete: func(value interface{}, err error) {
			if err != nil {
				t.Fatalf("update completed with error: %v", err)
			}
			if onComplete != nil {
				onComplete(value)
			}
		},
	}
}

func baseTemporalConfigRaw() map[string]any {
	return map[string]any{
		"hostPort":                    "localhost:7233",
		"namespace":                   "default",
		"apiKey":                      "test-api-key",
		"taskQueue":                   "gestalt-workflow",
		"scopeID":                     "scope",
		"identity":                    "gestalt-test",
		"workflowRunTimeout":          time.Minute,
		"workflowTaskTimeout":         time.Second,
		"activityStartToCloseTimeout": time.Minute,
		"scheduleCatchupWindow":       time.Minute,
	}
}

func baseTemporalConfig() config {
	return config{
		HostPort:                    "localhost:7233",
		Namespace:                   "default",
		APIKey:                      "test-api-key",
		TaskQueue:                   "gestalt-workflow",
		ScopeID:                     "scope",
		Identity:                    "gestalt-test",
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
		IdempotencyRetention:        time.Hour,
	}
}

func withMap(in map[string]any, key string, value any) map[string]any {
	out := make(map[string]any, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	out[key] = value
	return out
}

func withoutMapKey(in map[string]any, key string) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		if k != key {
			out[k] = v
		}
	}
	return out
}

func newTestTemporalBackendForStart(cfg config, handle *fakeWorkerDeploymentHandle, fw *fakeTemporalWorker) *temporalBackend {
	if handle == nil {
		handle = &fakeWorkerDeploymentHandle{}
	}
	if fw == nil {
		fw = &fakeTemporalWorker{}
	}
	backend := newTemporalBackend(
		"temporal",
		cfg,
		&recordingTemporalClient{deploymentClient: &fakeWorkerDeploymentClient{handle: handle}},
		nil,
		nil,
	)
	backend.newWorker = func(client.Client, string, worker.Options) temporalWorker { return fw }
	return backend
}

type recordedUpdate struct {
	WorkflowID   string
	UpdateID     string
	Name         string
	Args         []any
	WaitForStage client.WorkflowUpdateStage
}

type recordedQuery struct {
	WorkflowID string
	Name       string
	Args       []any
}

type recordedExecution struct {
	WorkflowID string
	Args       []any
}

type recordingTemporalClient struct {
	client.Client
	mu                 sync.Mutex
	pendingWorkflowIDs []string
	executions         []recordedExecution
	updates            []recordedUpdate
	updateErrs         []error
	queries            []recordedQuery
	scheduleClient     client.ScheduleClient
	deploymentClient   *fakeWorkerDeploymentClient
	afterExecute       func()
}

func (c *recordingTemporalClient) ExecuteWorkflow(_ context.Context, options client.StartWorkflowOptions, _ interface{}, args ...interface{}) (client.WorkflowRun, error) {
	c.mu.Lock()
	c.executions = append(c.executions, recordedExecution{
		WorkflowID: options.ID,
		Args:       args,
	})
	runNumber := len(c.executions)
	afterExecute := c.afterExecute
	c.mu.Unlock()
	if afterExecute != nil {
		afterExecute()
	}
	return recordingWorkflowRun{
		id:    options.ID,
		runID: fmt.Sprintf("run-%d", runNumber),
	}, nil
}

func (c *recordingTemporalClient) NewWithStartWorkflowOperation(options client.StartWorkflowOptions, _ interface{}, args ...interface{}) client.WithStartWorkflowOperation {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.executions = append(c.executions, recordedExecution{
		WorkflowID: options.ID,
		Args:       args,
	})
	runNumber := len(c.executions)
	return recordingWithStartOperation{
		workflowID: options.ID,
		runID:      fmt.Sprintf("run-%d", runNumber),
	}
}

func (c *recordingTemporalClient) UpdateWorkflow(_ context.Context, options client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.updateErrs) > 0 {
		err := c.updateErrs[0]
		c.updateErrs = c.updateErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	update := recordedUpdate{
		WorkflowID:   options.WorkflowID,
		UpdateID:     options.UpdateID,
		Name:         options.UpdateName,
		Args:         options.Args,
		WaitForStage: options.WaitForStage,
	}
	c.updates = append(c.updates, update)
	return recordingUpdateHandle{update: update}, nil
}

func (c *recordingTemporalClient) UpdateWithStartWorkflow(_ context.Context, options client.UpdateWithStartWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	workflowID := ""
	if op, ok := options.StartWorkflowOperation.(recordingWithStartOperation); ok {
		workflowID = op.workflowID
	} else if op, ok := options.StartWorkflowOperation.(*recordingWithStartOperation); ok {
		workflowID = op.workflowID
	}
	update := recordedUpdate{
		WorkflowID:   workflowID,
		UpdateID:     options.UpdateOptions.UpdateID,
		Name:         options.UpdateOptions.UpdateName,
		Args:         options.UpdateOptions.Args,
		WaitForStage: options.UpdateOptions.WaitForStage,
	}
	c.updates = append(c.updates, update)
	return recordingUpdateHandle{update: update}, nil
}

func (c *recordingTemporalClient) QueryWorkflow(ctx context.Context, workflowID string, _ string, queryType string, args ...interface{}) (converter.EncodedValue, error) {
	query := recordedQuery{
		WorkflowID: workflowID,
		Name:       queryType,
		Args:       args,
	}
	c.mu.Lock()
	c.queries = append(c.queries, query)
	c.mu.Unlock()
	return recordingEncodedValue{}, nil
}

func (c *recordingTemporalClient) TerminateWorkflow(_ context.Context, workflowID string, runID string, reason string, details ...interface{}) error {
	return nil
}

func (c *recordingTemporalClient) ScheduleClient() client.ScheduleClient {
	if c.scheduleClient != nil {
		return c.scheduleClient
	}
	return c.Client.ScheduleClient()
}

func (c *recordingTemporalClient) WorkerDeploymentClient() client.WorkerDeploymentClient {
	if c.deploymentClient != nil {
		return c.deploymentClient
	}
	return c.Client.WorkerDeploymentClient()
}

func (c *recordingTemporalClient) hasUpdate(workflowID, name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, update := range c.updates {
		if update.WorkflowID == workflowID && update.Name == name {
			return true
		}
	}
	return false
}

type fakeTemporalWorker struct {
	order                *[]string
	startErr             error
	startCount           int
	stopCount            int
	registeredWorkflows  int
	registeredActivities int
}

func (w *fakeTemporalWorker) RegisterWorkflow(interface{}) {
	w.registeredWorkflows++
}

func (w *fakeTemporalWorker) RegisterActivity(interface{}) {
	w.registeredActivities++
}

func (w *fakeTemporalWorker) Start() error {
	w.startCount++
	if w.order != nil {
		*w.order = append(*w.order, "start")
	}
	return w.startErr
}

func (w *fakeTemporalWorker) Stop() {
	w.stopCount++
	if w.order != nil {
		*w.order = append(*w.order, "stop")
	}
}

type fakeWorkerDeploymentClient struct {
	handle *fakeWorkerDeploymentHandle
}

func (c *fakeWorkerDeploymentClient) List(context.Context, client.WorkerDeploymentListOptions) (client.WorkerDeploymentListIterator, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (c *fakeWorkerDeploymentClient) GetHandle(string) client.WorkerDeploymentHandle {
	if c.handle == nil {
		c.handle = &fakeWorkerDeploymentHandle{}
	}
	return c.handle
}

func (c *fakeWorkerDeploymentClient) Delete(context.Context, client.WorkerDeploymentDeleteOptions) (client.WorkerDeploymentDeleteResponse, error) {
	return client.WorkerDeploymentDeleteResponse{}, status.Error(codes.Unimplemented, "not implemented")
}

type fakeWorkerDeploymentHandle struct {
	order            *[]string
	describeResponse client.WorkerDeploymentDescribeResponse
	describeErrs     []error
	setCurrentErrs   []error
	setRampingErrs   []error
	describeCalls    []client.WorkerDeploymentDescribeOptions
	setCurrentCalls  []client.WorkerDeploymentSetCurrentVersionOptions
	setRampingCalls  []client.WorkerDeploymentSetRampingVersionOptions
}

func (h *fakeWorkerDeploymentHandle) Describe(_ context.Context, options client.WorkerDeploymentDescribeOptions) (client.WorkerDeploymentDescribeResponse, error) {
	h.describeCalls = append(h.describeCalls, options)
	if h.order != nil {
		*h.order = append(*h.order, "describe")
	}
	if len(h.describeErrs) > 0 {
		err := h.describeErrs[0]
		h.describeErrs = h.describeErrs[1:]
		if err != nil {
			return client.WorkerDeploymentDescribeResponse{}, err
		}
	}
	return h.describeResponse, nil
}

func (h *fakeWorkerDeploymentHandle) SetCurrentVersion(_ context.Context, options client.WorkerDeploymentSetCurrentVersionOptions) (client.WorkerDeploymentSetCurrentVersionResponse, error) {
	h.setCurrentCalls = append(h.setCurrentCalls, options)
	if h.order != nil {
		*h.order = append(*h.order, "set-current")
	}
	if len(h.setCurrentErrs) > 0 {
		err := h.setCurrentErrs[0]
		h.setCurrentErrs = h.setCurrentErrs[1:]
		if err != nil {
			return client.WorkerDeploymentSetCurrentVersionResponse{}, err
		}
	}
	h.describeResponse.Info.RoutingConfig.CurrentVersion = &worker.WorkerDeploymentVersion{
		DeploymentName: "valon-tools-prod",
		BuildID:        options.BuildID,
	}
	return client.WorkerDeploymentSetCurrentVersionResponse{}, nil
}

func (h *fakeWorkerDeploymentHandle) SetRampingVersion(_ context.Context, options client.WorkerDeploymentSetRampingVersionOptions) (client.WorkerDeploymentSetRampingVersionResponse, error) {
	h.setRampingCalls = append(h.setRampingCalls, options)
	if h.order != nil {
		*h.order = append(*h.order, "set-ramping")
	}
	if len(h.setRampingErrs) > 0 {
		err := h.setRampingErrs[0]
		h.setRampingErrs = h.setRampingErrs[1:]
		if err != nil {
			return client.WorkerDeploymentSetRampingVersionResponse{}, err
		}
	}
	h.describeResponse.Info.RoutingConfig.RampingVersion = &worker.WorkerDeploymentVersion{
		DeploymentName: "valon-tools-prod",
		BuildID:        options.BuildID,
	}
	h.describeResponse.Info.RoutingConfig.RampingVersionPercentage = options.Percentage
	return client.WorkerDeploymentSetRampingVersionResponse{}, nil
}

func (h *fakeWorkerDeploymentHandle) SetManagerIdentity(context.Context, client.WorkerDeploymentSetManagerIdentityOptions) (client.WorkerDeploymentSetManagerIdentityResponse, error) {
	return client.WorkerDeploymentSetManagerIdentityResponse{}, status.Error(codes.Unimplemented, "not implemented")
}

func (h *fakeWorkerDeploymentHandle) DescribeVersion(context.Context, client.WorkerDeploymentDescribeVersionOptions) (client.WorkerDeploymentVersionDescription, error) {
	return client.WorkerDeploymentVersionDescription{}, status.Error(codes.Unimplemented, "not implemented")
}

func (h *fakeWorkerDeploymentHandle) DeleteVersion(context.Context, client.WorkerDeploymentDeleteVersionOptions) (client.WorkerDeploymentDeleteVersionResponse, error) {
	return client.WorkerDeploymentDeleteVersionResponse{}, status.Error(codes.Unimplemented, "not implemented")
}

func (h *fakeWorkerDeploymentHandle) UpdateVersionMetadata(context.Context, client.WorkerDeploymentUpdateVersionMetadataOptions) (client.WorkerDeploymentUpdateVersionMetadataResponse, error) {
	return client.WorkerDeploymentUpdateVersionMetadataResponse{}, status.Error(codes.Unimplemented, "not implemented")
}

type recordingWorkflowRun struct {
	id    string
	runID string
}

func (r recordingWorkflowRun) GetID() string { return r.id }

func (r recordingWorkflowRun) GetRunID() string { return r.runID }

func (r recordingWorkflowRun) Get(context.Context, interface{}) error { return nil }

func (r recordingWorkflowRun) GetWithOptions(context.Context, interface{}, client.WorkflowRunGetOptions) error {
	return nil
}

type recordingWithStartOperation struct {
	workflowID string
	runID      string
}

func (o recordingWithStartOperation) Get(context.Context) (client.WorkflowRun, error) {
	return recordingWorkflowRun{id: o.workflowID, runID: o.runID}, nil
}

type recordingUpdateHandle struct {
	update recordedUpdate
}

func (h recordingUpdateHandle) WorkflowID() string { return h.update.WorkflowID }
func (h recordingUpdateHandle) RunID() string      { return "" }
func (h recordingUpdateHandle) UpdateID() string   { return h.update.UpdateID }

func (h recordingUpdateHandle) Get(_ context.Context, valuePtr interface{}) error {
	if valuePtr == nil {
		return nil
	}
	switch out := valuePtr.(type) {
	case *proto.BoundWorkflowRun:
		if len(h.update.Args) > 0 {
			if run, ok := h.update.Args[len(h.update.Args)-1].(*proto.BoundWorkflowRun); ok {
				*out = *cloneRun(run)
			}
		}
	case *proto.BoundWorkflowEventTrigger:
		if len(h.update.Args) > 0 {
			if trigger, ok := h.update.Args[len(h.update.Args)-1].(*proto.BoundWorkflowEventTrigger); ok {
				*out = *cloneTrigger(trigger)
			}
		}
	case *proto.WorkflowExecutionReference:
		if len(h.update.Args) > 0 {
			if ref, ok := h.update.Args[len(h.update.Args)-1].(*proto.WorkflowExecutionReference); ok {
				*out = *cloneExecutionReference(ref)
			}
		}
	case *proto.SignalWorkflowRunResponse:
		if len(h.update.Args) > 0 {
			if resp, ok := h.update.Args[len(h.update.Args)-1].(*proto.SignalWorkflowRunResponse); ok {
				*out = *cloneSignalResponse(resp)
				return nil
			}
			if signal, ok := h.update.Args[len(h.update.Args)-1].(*proto.WorkflowSignal); ok && h.update.Name == updateAddSignal {
				out.Signal = cloneSignal(signal)
				if out.Signal.Sequence == 0 {
					out.Signal.Sequence = 1
				}
				if out.Signal.Id == "" {
					out.Signal.Id = "signal:" + hashID(h.update.WorkflowID, out.Signal.GetName(), fmt.Sprintf("%d", out.Signal.GetSequence()), out.Signal.GetIdempotencyKey())
				}
				out.StartedRun = true
			}
		}
	}
	return nil
}

type recordingEncodedValue struct {
	value any
}

func (v recordingEncodedValue) HasValue() bool { return true }

func (v recordingEncodedValue) Get(valuePtr interface{}) error {
	switch out := valuePtr.(type) {
	case *proto.BoundWorkflowRun:
		if run, ok := v.value.(*proto.BoundWorkflowRun); ok {
			*out = *cloneRun(run)
		}
	}
	return nil
}

type fakeScheduleClient struct {
	handles   map[string]*fakeScheduleHandle
	order     []string
	listCount int
}

func newFakeScheduleClient(descriptions map[string]*client.ScheduleDescription) *fakeScheduleClient {
	c := &fakeScheduleClient{handles: map[string]*fakeScheduleHandle{}, order: make([]string, 0, len(descriptions))}
	for id, desc := range descriptions {
		c.handles[id] = &fakeScheduleHandle{id: id, desc: cloneScheduleDescription(desc)}
		c.order = append(c.order, id)
	}
	return c
}

func (c *fakeScheduleClient) Create(_ context.Context, options client.ScheduleOptions) (client.ScheduleHandle, error) {
	handle := &fakeScheduleHandle{id: options.ID, desc: &client.ScheduleDescription{
		Schedule: client.Schedule{
			Action: options.Action,
			Spec:   &options.Spec,
			Policy: &client.SchedulePolicies{Overlap: options.Overlap, CatchupWindow: options.CatchupWindow},
			State:  &client.ScheduleState{Paused: options.Paused},
		},
	}}
	c.handles[options.ID] = handle
	c.order = append(c.order, options.ID)
	return handle, nil
}

func (c *fakeScheduleClient) List(context.Context, client.ScheduleListOptions) (client.ScheduleListIterator, error) {
	c.listCount++
	entries := make([]*client.ScheduleListEntry, 0, len(c.order))
	for _, id := range c.order {
		entries = append(entries, &client.ScheduleListEntry{ID: id})
	}
	return &fakeScheduleListIterator{entries: entries}, nil
}

func (c *fakeScheduleClient) GetHandle(_ context.Context, scheduleID string) client.ScheduleHandle {
	handle, ok := c.handles[scheduleID]
	if !ok {
		handle = &fakeScheduleHandle{id: scheduleID}
		c.handles[scheduleID] = handle
	}
	return handle
}

type fakeScheduleHandle struct {
	id          string
	desc        *client.ScheduleDescription
	updateCount int
}

func (h *fakeScheduleHandle) GetID() string { return h.id }
func (h *fakeScheduleHandle) Delete(context.Context) error {
	h.desc = nil
	return nil
}
func (h *fakeScheduleHandle) Backfill(context.Context, client.ScheduleBackfillOptions) error {
	return nil
}
func (h *fakeScheduleHandle) Update(_ context.Context, options client.ScheduleUpdateOptions) error {
	update, err := options.DoUpdate(client.ScheduleUpdateInput{Description: *cloneScheduleDescription(h.desc)})
	if err != nil {
		return err
	}
	if update != nil && update.Schedule != nil {
		h.desc = &client.ScheduleDescription{Schedule: *update.Schedule}
	}
	h.updateCount++
	return nil
}
func (h *fakeScheduleHandle) Describe(context.Context) (*client.ScheduleDescription, error) {
	if h.desc == nil {
		return nil, errors.New("not found")
	}
	return cloneScheduleDescription(h.desc), nil
}
func (h *fakeScheduleHandle) Trigger(context.Context, client.ScheduleTriggerOptions) error {
	return nil
}
func (h *fakeScheduleHandle) Pause(context.Context, client.SchedulePauseOptions) error {
	if h.desc != nil {
		h.desc.Schedule.State = &client.ScheduleState{Paused: true}
	}
	return nil
}
func (h *fakeScheduleHandle) Unpause(context.Context, client.ScheduleUnpauseOptions) error {
	if h.desc != nil {
		h.desc.Schedule.State = &client.ScheduleState{Paused: false}
	}
	return nil
}

type fakeScheduleListIterator struct {
	entries []*client.ScheduleListEntry
	index   int
}

func (i *fakeScheduleListIterator) HasNext() bool {
	return i.index < len(i.entries)
}

func (i *fakeScheduleListIterator) Next() (*client.ScheduleListEntry, error) {
	entry := i.entries[i.index]
	i.index++
	return entry, nil
}

func cloneScheduleDescription(desc *client.ScheduleDescription) *client.ScheduleDescription {
	if desc == nil {
		return nil
	}
	clone := *desc
	return &clone
}

type fakeBackend struct {
	calledListRuns bool
	startCount     int
	startErr       error
	listRuns       *proto.ListWorkflowProviderRunsResponse
}

func (f *fakeBackend) Start(context.Context) error {
	f.startCount++
	return f.startErr
}
func (f *fakeBackend) Close() error { return nil }
func (f *fakeBackend) HealthCheck(context.Context) error {
	return nil
}
func (f *fakeBackend) StartRun(context.Context, *proto.StartWorkflowProviderRunRequest) (*proto.BoundWorkflowRun, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (f *fakeBackend) GetRun(context.Context, *proto.GetWorkflowProviderRunRequest) (*proto.BoundWorkflowRun, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (f *fakeBackend) ListRuns(context.Context, *proto.ListWorkflowProviderRunsRequest) (*proto.ListWorkflowProviderRunsResponse, error) {
	f.calledListRuns = true
	return f.listRuns, nil
}
func (f *fakeBackend) CancelRun(context.Context, *proto.CancelWorkflowProviderRunRequest) (*proto.BoundWorkflowRun, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (f *fakeBackend) SignalRun(context.Context, *proto.SignalWorkflowProviderRunRequest) (*proto.SignalWorkflowRunResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (f *fakeBackend) SignalOrStartRun(context.Context, *proto.SignalOrStartWorkflowProviderRunRequest) (*proto.SignalWorkflowRunResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (f *fakeBackend) UpsertSchedule(context.Context, *proto.UpsertWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (f *fakeBackend) GetSchedule(context.Context, *proto.GetWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (f *fakeBackend) ListSchedules(context.Context, *proto.ListWorkflowProviderSchedulesRequest) (*proto.ListWorkflowProviderSchedulesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (f *fakeBackend) DeleteSchedule(context.Context, *proto.DeleteWorkflowProviderScheduleRequest) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (f *fakeBackend) PauseSchedule(context.Context, *proto.PauseWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (f *fakeBackend) ResumeSchedule(context.Context, *proto.ResumeWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (f *fakeBackend) UpsertEventTrigger(context.Context, *proto.UpsertWorkflowProviderEventTriggerRequest) (*proto.BoundWorkflowEventTrigger, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (f *fakeBackend) GetEventTrigger(context.Context, *proto.GetWorkflowProviderEventTriggerRequest) (*proto.BoundWorkflowEventTrigger, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (f *fakeBackend) ListEventTriggers(context.Context, *proto.ListWorkflowProviderEventTriggersRequest) (*proto.ListWorkflowProviderEventTriggersResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (f *fakeBackend) DeleteEventTrigger(context.Context, *proto.DeleteWorkflowProviderEventTriggerRequest) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (f *fakeBackend) PauseEventTrigger(context.Context, *proto.PauseWorkflowProviderEventTriggerRequest) (*proto.BoundWorkflowEventTrigger, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (f *fakeBackend) ResumeEventTrigger(context.Context, *proto.ResumeWorkflowProviderEventTriggerRequest) (*proto.BoundWorkflowEventTrigger, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (f *fakeBackend) PutExecutionReference(context.Context, *proto.PutWorkflowExecutionReferenceRequest) (*proto.WorkflowExecutionReference, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (f *fakeBackend) GetExecutionReference(context.Context, *proto.GetWorkflowExecutionReferenceRequest) (*proto.WorkflowExecutionReference, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (f *fakeBackend) ListExecutionReferences(context.Context, *proto.ListWorkflowExecutionReferencesRequest) (*proto.ListWorkflowExecutionReferencesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
func (f *fakeBackend) PublishEvent(context.Context, *proto.PublishWorkflowProviderEventRequest) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func collectTemporalWorkflowMetrics(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	return rm
}

func temporalWorkflowMetricAttrs(operation, triggerKind, targetKind, runStatus string) map[string]string {
	return map[string]string{
		"gestaltd.workflow.provider.name":    "temporal",
		"gestaltd.workflow.operation.name":   operation,
		"gestaltd.workflow.trigger.kind":     triggerKind,
		"gestaltd.workflow.target.kind":      targetKind,
		"gestaltd.workflow.run.status":       runStatus,
		"gestaltd.workflow.telemetry.source": "provider",
	}
}

func requireTemporalInt64Sum(t *testing.T, rm metricdata.ResourceMetrics, name string, want int64, attrs map[string]string) {
	t.Helper()

	for _, scope := range rm.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != name {
				continue
			}
			sum, ok := metric.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %q is %T, want Sum[int64]", name, metric.Data)
			}
			for _, point := range sum.DataPoints {
				if temporalMetricAttrsMatch(point.Attributes, attrs) {
					if point.Value != want {
						t.Fatalf("metric %q attrs %v = %d, want %d", name, attrs, point.Value, want)
					}
					return
				}
			}
		}
	}

	t.Fatalf("metric %q with attrs %v not found", name, attrs)
}

func temporalMetricAttrsMatch(set attribute.Set, want map[string]string) bool {
	for key, expected := range want {
		value, ok := set.Value(attribute.Key(key))
		if !ok || value.AsString() != expected {
			return false
		}
	}
	return true
}

func startTestIndexedDBBackend(t *testing.T) {
	t.Helper()

	socketDir, err := os.MkdirTemp("/tmp", "temporal-indexeddb-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	socketPath := filepath.Join(socketDir, "indexeddb.sock")
	store := relationaldb.New()
	if err := store.Configure(context.Background(), "temporal_workflow_state", map[string]any{
		"dsn": "file:" + filepath.Join(t.TempDir(), "workflow.sqlite") + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)",
	}); err != nil {
		t.Fatalf("relationaldb.Configure: %v", err)
	}
	seedTemporalWorkflowStores(t, store)

	t.Setenv(proto.EnvProviderSocket, socketPath)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- gestalt.ServeIndexedDBProvider(ctx, store)
	}()

	deadline := time.Now().Add(time.Second)
	for {
		conn, err := net.DialTimeout("unix", socketPath, 20*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("indexeddb socket did not start: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Cleanup(func() {
		cancel()
		err := <-errCh
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("ServeIndexedDBProvider: %v", err)
		}
		_ = os.Remove(socketPath)
	})
	t.Setenv(gestalt.EnvIndexedDBSocket, socketPath)
}

func seedTemporalWorkflowStores(t *testing.T, store *relationaldb.Provider) {
	t.Helper()
	for _, def := range []struct {
		name   string
		schema gestalt.ObjectStoreSchema
	}{
		{name: storeTemporalSchedules, schema: temporalScheduleSchema()},
		{name: storeTemporalEventTriggers, schema: temporalEventTriggerSchema()},
		{name: storeTemporalEventTriggerKeys, schema: temporalEventTriggerKeySchema()},
		{name: storeTemporalExecutionRefs, schema: temporalExecutionRefSchema()},
	} {
		if err := store.CreateObjectStore(context.Background(), def.name, def.schema); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
			t.Fatalf("CreateObjectStore(%s): %v", def.name, err)
		}
	}
}

func temporalScheduleSchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true},
			{Name: "scope_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "owner_key", Type: gestalt.TypeString},
			{Name: "created_at", Type: gestalt.TypeTime},
			{Name: "updated_at", Type: gestalt.TypeTime},
			{Name: "payload", Type: gestalt.TypeBytes, NotNull: true},
		},
	}
}

func temporalEventTriggerSchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true},
			{Name: "scope_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "owner_key", Type: gestalt.TypeString},
			{Name: "paused", Type: gestalt.TypeBool},
			{Name: "created_at", Type: gestalt.TypeTime},
			{Name: "updated_at", Type: gestalt.TypeTime},
			{Name: "payload", Type: gestalt.TypeBytes, NotNull: true},
		},
	}
}

func temporalEventTriggerKeySchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Indexes: []gestalt.IndexSchema{
			{Name: indexByMatchKey, KeyPath: []string{"match_key"}},
			{Name: indexByTriggerID, KeyPath: []string{"trigger_id"}},
		},
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true},
			{Name: "scope_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "match_key", Type: gestalt.TypeString, NotNull: true},
			{Name: "trigger_id", Type: gestalt.TypeString, NotNull: true},
		},
	}
}

func temporalExecutionRefSchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Indexes: []gestalt.IndexSchema{{Name: indexBySubject, KeyPath: []string{"subject_id"}}},
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true},
			{Name: "scope_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "provider_name", Type: gestalt.TypeString, NotNull: true},
			{Name: "subject_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "created_at", Type: gestalt.TypeTime},
			{Name: "revoked_at", Type: gestalt.TypeTime},
			{Name: "payload", Type: gestalt.TypeBytes, NotNull: true},
		},
	}
}
