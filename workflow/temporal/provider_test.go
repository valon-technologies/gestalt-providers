package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	relationaldb "github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb"
	workflowfake "github.com/valon-technologies/gestalt-providers/workflow/indexeddb/fake"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	gestaltworkflow "github.com/valon-technologies/gestalt/sdk/go/workflow"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newTestWorkflowEnvironment(suite *testsuite.WorkflowTestSuite) *testsuite.TestWorkflowEnvironment {
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{DeadlockDetectionTimeout: 5 * time.Second})
	return env
}

func TestGestaltRunWorkflowV4ProjectsRunStateToIndexedDB(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)

	var suite testsuite.WorkflowTestSuite
	env := newTestWorkflowEnvironment(&suite)
	host := &capturingHost{resp: &gestaltworkflow.Response{Status: http.StatusOK, Body: "ok"}}
	env.RegisterWorkflow(gestaltRunWorkflowV4)
	env.RegisterActivity(&workflowActivities{executor: host, state: state})

	env.ExecuteWorkflow(gestaltRunWorkflowV4, runWorkflowV4Input{
		DefinitionID:                  "ref-1",
		ActivityStartToCloseTimeoutNS: time.Minute,
		Target:                        nativeAppTargetInput("slack", "postMessage"),
		Trigger:                       &gestalt.WorkflowRunTrigger{Manual: true},
		CreatedBySubjectID: actor("user-1"),
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var run gestalt.BoundWorkflowRun
	if err := env.GetWorkflowResult(&run); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	projected, found, err := state.getRun(ctx, run.ID)
	if err != nil || !found {
		t.Fatalf("projected run found=%v err=%v", found, err)
	}
	if projected.Status != gestalt.WorkflowRunStatusValueSucceeded || projected.ResultBody != "ok" {
		t.Fatalf("projected run = %#v, want succeeded with body", projected)
	}
	if projected.DefinitionID != "ref-1" {
		t.Fatalf("projected definition_id = %q, want ref-1", projected.DefinitionID)
	}
	if len(host.calls) != 1 || host.calls[0].Metadata[workflowInvokeMetadataDefinitionID] != "ref-1" {
		t.Fatalf("host call metadata = %#v, want definition_id ref-1", host.calls)
	}
	listed, _, err := state.listRuns(ctx, nil)
	if err != nil {
		t.Fatalf("listRuns: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != run.ID {
		t.Fatalf("listed runs = %#v, want %q", listed, run.ID)
	}
}

func TestBackendDefinitionCRUD(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)
	backend := newRecordingTemporalBackend(&recordingTemporalClient{}, state)
	createTarget := nativeAppTargetInputWithObject("slack", "postMessage", map[string]any{"mode": "full"})

	created, err := backend.CreateDefinition(ctx, &gestalt.CreateWorkflowProviderDefinitionRequest{
		IdempotencyKey: "definition-sync",
		Target:         createTarget,
	})
	if err != nil {
		t.Fatalf("CreateDefinition: %v", err)
	}
	createTarget.Steps[0].App.Input.Object["mode"] = gestalt.WorkflowValue{Literal: "mutated", LiteralSet: true}
	if created.ID == "" || created.ProviderName != "temporal" {
		t.Fatalf("created definition = %#v, want id and provider", created)
	}
	if got := workflowValueObjectField(created.Target, "mode"); got != "full" {
		t.Fatalf("created definition input mode = %v, want isolated full", got)
	}

	again, err := backend.CreateDefinition(ctx, &gestalt.CreateWorkflowProviderDefinitionRequest{
		IdempotencyKey: "definition-sync",
		Target:         nativeAppTargetInput("slack", "postMessage"),
	})
	if err != nil {
		t.Fatalf("CreateDefinition(idempotent): %v", err)
	}
	if again.ID != created.ID {
		t.Fatalf("idempotent definition ids = %q and %q, want equal", created.ID, again.ID)
	}
	conflicting, err := backend.CreateDefinition(ctx, &gestalt.CreateWorkflowProviderDefinitionRequest{
		IdempotencyKey: "definition-sync",
		Target:         nativeAppTargetInput("slack", "conflictingMessage"),
	})
	if err != nil {
		t.Fatalf("CreateDefinition(conflicting idempotent target): %v", err)
	}
	if conflicting.ID != created.ID || firstWorkflowAppStep(conflicting.Target).Operation != "postMessage" {
		t.Fatalf("conflicting idempotent definition = %#v, want original postMessage definition", conflicting)
	}
	created.CreatedBySubjectID = actor("creator-1")
	if err := state.putDefinition(ctx, created); err != nil {
		t.Fatalf("store definition creator: %v", err)
	}

	updated, err := backend.UpdateDefinition(ctx, &gestalt.UpdateWorkflowProviderDefinitionRequest{
		DefinitionID: created.ID,
		Target:       nativeAppTargetInput("slack", "updateMessage"),
	})
	if err != nil {
		t.Fatalf("UpdateDefinition: %v", err)
	}
	if updated.ID != created.ID || updated.CreatedAt != created.CreatedAt {
		t.Fatalf("updated definition = %#v, want same id and created_at", updated)
	}
	if firstWorkflowAppStep(updated.Target).Operation != "updateMessage" {
		t.Fatalf("updated operation = %q, want updateMessage", firstWorkflowAppStep(updated.Target).Operation)
	}
	if updated.CreatedBySubjectID == "" || updated.CreatedBySubjectID != "creator-1" {
		t.Fatalf("updated created_by = %#v, want creator-1", updated.CreatedBySubjectID)
	}

	got, err := backend.GetDefinition(ctx, &gestalt.GetWorkflowProviderDefinitionRequest{DefinitionID: created.ID})
	if err != nil {
		t.Fatalf("GetDefinition: %v", err)
	}
	if firstWorkflowAppStep(got.Target).Operation != "updateMessage" {
		t.Fatalf("stored operation = %q, want updateMessage", firstWorkflowAppStep(got.Target).Operation)
	}
	if got.CreatedBySubjectID == "" || got.CreatedBySubjectID != "creator-1" {
		t.Fatalf("stored created_by = %#v, want creator-1", got.CreatedBySubjectID)
	}

	if err := backend.DeleteDefinition(ctx, &gestalt.DeleteWorkflowProviderDefinitionRequest{DefinitionID: created.ID}); err != nil {
		t.Fatalf("DeleteDefinition: %v", err)
	}
	if _, err := backend.GetDefinition(ctx, &gestalt.GetWorkflowProviderDefinitionRequest{DefinitionID: created.ID}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetDefinition after delete error = %v, want NotFound", err)
	}
}

func TestGestaltRunWorkflowV4WaitsForClaimBeforeInvokingHost(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := newTestWorkflowEnvironment(&suite)
	host := &capturingHost{resp: &gestaltworkflow.Response{Status: http.StatusOK, Body: "ok"}}
	env.RegisterWorkflow(gestaltRunWorkflowV4)
	env.RegisterActivity(&workflowActivities{executor: host})

	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(updateAddSignal, "signal-1", updateCallback(t, func(value interface{}) {
			resp := value.(*gestalt.SignalWorkflowRunResponse)
			if resp.Signal == nil || resp.Signal.IdempotencyKey != "signal-1" {
				t.Fatalf("queued signal response = %#v, want signal-1", resp.Signal)
			}
		}), gestalt.WorkflowSignal{Name: "slack.event", IdempotencyKey: "signal-1", CreatedAt: time.Now().UTC()})
		if len(host.calls) != 0 {
			t.Fatalf("step calls before claim = %d, want 0", len(host.calls))
		}
		env.UpdateWorkflow(updateClaimRun, "claim-run", updateCallback(t, nil))
	}, time.Millisecond)

	env.ExecuteWorkflow(gestaltRunWorkflowV4, runWorkflowV4Input{
		DefinitionID:                  "ref-1",
		ActivityStartToCloseTimeoutNS: time.Minute,
		WorkflowKey:                   "thread-1",
		OwnerKey:                      "slack",
		Target:                        nativeAppTargetInput("slack", "postMessage"),
		Trigger:                       &gestalt.WorkflowRunTrigger{Manual: true},
		CreatedBySubjectID: actor("user-1"),
		RequireSignal:                 true,
		RequireClaim:                  true,
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if len(host.calls) != 1 {
		t.Fatalf("step calls = %d, want 1 after claim", len(host.calls))
	}
}

func TestGestaltRunWorkflowV4ClaimUpdateDoesNotWaitForProjection(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := newTestWorkflowEnvironment(&suite)
	host := &capturingHost{resp: &gestaltworkflow.Response{Status: http.StatusOK, Body: "ok"}}
	activities := &workflowActivities{executor: host}
	env.RegisterWorkflow(gestaltRunWorkflowV4)
	env.RegisterActivity(activities)

	var mu sync.Mutex
	var projectedStatuses []gestalt.WorkflowRunStatus
	env.OnActivity(activities.ProjectRun, mock.Anything, mock.Anything).Return(func(_ context.Context, run gestalt.BoundWorkflowRun) error {
		mu.Lock()
		defer mu.Unlock()
		projectedStatuses = append(projectedStatuses, run.Status)
		return nil
	}).Maybe()

	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(updateClaimRun, "claim-run", updateCallback(t, nil))
	}, time.Millisecond)

	env.ExecuteWorkflow(gestaltRunWorkflowV4, runWorkflowV4Input{
		DefinitionID:                  "ref-1",
		ActivityStartToCloseTimeoutNS: time.Minute,
		WorkflowKey:                   "thread-1",
		OwnerKey:                      "slack",
		Target:                        nativeAppTargetInput("slack", "postMessage"),
		Trigger:                       &gestalt.WorkflowRunTrigger{Manual: true},
		CreatedBySubjectID: actor("user-1"),
		InitialSignal:                 &gestalt.WorkflowSignal{Name: "slack.event", CreatedAt: time.Now().UTC()},
		RequireSignal:                 true,
		RequireClaim:                  true,
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(projectedStatuses) != 3 ||
		projectedStatuses[0] != gestalt.WorkflowRunStatusValuePending ||
		projectedStatuses[1] != gestalt.WorkflowRunStatusValueRunning ||
		projectedStatuses[2] != gestalt.WorkflowRunStatusValueSucceeded {
		t.Fatalf("projected statuses = %v, want pending/running/succeeded", projectedStatuses)
	}
}

func TestGestaltRunWorkflowV4AddSignalUpdateDoesNotWaitForProjection(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := newTestWorkflowEnvironment(&suite)
	host := &capturingHost{resp: &gestaltworkflow.Response{Status: http.StatusOK, Body: "ok"}}
	activities := &workflowActivities{executor: host}
	env.RegisterWorkflow(gestaltRunWorkflowV4)
	env.RegisterActivity(activities)

	var mu sync.Mutex
	var projectedStatuses []gestalt.WorkflowRunStatus
	env.OnActivity(activities.ProjectRun, mock.Anything, mock.Anything).Return(func(_ context.Context, run gestalt.BoundWorkflowRun) error {
		mu.Lock()
		defer mu.Unlock()
		projectedStatuses = append(projectedStatuses, run.Status)
		return nil
	}).Maybe()

	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(updateAddSignal, "signal-run", updateCallback(t, nil), gestalt.WorkflowSignal{Name: "slack.event", CreatedAt: time.Now().UTC()})
	}, time.Millisecond)

	env.ExecuteWorkflow(gestaltRunWorkflowV4, runWorkflowV4Input{
		DefinitionID:                  "ref-1",
		ActivityStartToCloseTimeoutNS: time.Minute,
		WorkflowKey:                   "thread-1",
		OwnerKey:                      "slack",
		Target:                        nativeAppTargetInput("slack", "postMessage"),
		Trigger:                       &gestalt.WorkflowRunTrigger{Manual: true},
		CreatedBySubjectID: actor("user-1"),
		RequireSignal:                 true,
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(projectedStatuses) != 3 ||
		projectedStatuses[0] != gestalt.WorkflowRunStatusValuePending ||
		projectedStatuses[1] != gestalt.WorkflowRunStatusValueRunning ||
		projectedStatuses[2] != gestalt.WorkflowRunStatusValueSucceeded {
		t.Fatalf("projected statuses = %v, want pending/running/succeeded", projectedStatuses)
	}
}

func TestGestaltRunWorkflowV4ContinuesWhenProjectionFails(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)
	if err := state.db.DeleteObjectStore(ctx, storeTemporalRunProjections); err != nil {
		t.Fatalf("DeleteObjectStore(%s): %v", storeTemporalRunProjections, err)
	}

	var suite testsuite.WorkflowTestSuite
	env := newTestWorkflowEnvironment(&suite)
	host := &capturingHost{resp: &gestaltworkflow.Response{Status: http.StatusOK, Body: "ok"}}
	env.RegisterWorkflow(gestaltRunWorkflowV4)
	env.RegisterActivity(&workflowActivities{executor: host, state: state})

	env.ExecuteWorkflow(gestaltRunWorkflowV4, runWorkflowV4Input{
		DefinitionID:                  "ref-1",
		ActivityStartToCloseTimeoutNS: time.Minute,
		Target:                        nativeAppTargetInput("slack", "postMessage"),
		Trigger:                       &gestalt.WorkflowRunTrigger{Manual: true},
		CreatedBySubjectID: actor("user-1"),
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var run gestalt.BoundWorkflowRun
	if err := env.GetWorkflowResult(&run); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	if run.Status != gestalt.WorkflowRunStatusValueSucceeded || run.ResultBody != "ok" {
		t.Fatalf("run = %#v, want succeeded with body", &run)
	}
	if len(host.calls) != 1 {
		t.Fatalf("step calls = %d, want 1", len(host.calls))
	}
}

func TestTemporalBackendStartRegistersOnlyRunWorkflow(t *testing.T) {
	order := []string{}
	fw := &fakeTemporalWorker{order: &order}
	tc := &recordingTemporalClient{}
	backend := newTemporalBackend("temporal", baseTemporalConfig(), tc, nil, nil)
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
	if len(tc.updates) != 0 {
		t.Fatalf("startup touched workflow updates=%#v", tc.updates)
	}
}

func TestTemporalBackendStartUsesWorkerVersioningOptions(t *testing.T) {
	raw := baseTemporalConfigRaw()
	raw["versioning"] = map[string]any{
		"deploymentName": "valon-tools-prod",
		"buildID":        "revision-1",
	}
	cfg, err := decodeConfig(raw)
	if err != nil {
		t.Fatalf("decodeConfig: %v", err)
	}

	order := []string{}
	fw := &fakeTemporalWorker{order: &order}
	backend := newTemporalBackend("temporal", cfg, &recordingTemporalClient{}, nil, nil)
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
	if got := strings.Join(order, ","); got != "start" {
		t.Fatalf("startup order = %s, want start", got)
	}
}

func TestTemporalVersioningConfigValidation(t *testing.T) {
	validVersioning := map[string]any{
		"deploymentName": "valon-tools-prod",
		"buildID":        "revision-1",
	}
	tests := []struct {
		name       string
		versioning map[string]any
		want       string
	}{
		{
			name:       "missing versioning",
			versioning: nil,
			want:       "versioning.deploymentName is required",
		},
		{
			name:       "missing deployment name",
			versioning: withMap(validVersioning, "deploymentName", ""),
			want:       "versioning.deploymentName is required",
		},
		{
			name:       "missing build id",
			versioning: withMap(validVersioning, "buildID", ""),
			want:       "versioning.buildID is required",
		},
		{
			name:       "deployment separator",
			versioning: withMap(validVersioning, "deploymentName", "valon.tools"),
			want:       "versioning.deploymentName cannot contain",
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
	_, state := newTestWorkflowStateStore(t)
	tc := &recordingTemporalClient{}
	backend := newRecordingTemporalBackend(tc, state)
	trigger := &gestalt.BoundWorkflowEventTrigger{
		ID:        "trigger-1",
		Match:     &gestalt.WorkflowEventMatch{Type: "message.created"},
		Target:    nativeAppTargetInput("slack", "postMessage"),
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	if err := backend.state.putTrigger(context.Background(), trigger); err != nil {
		t.Fatalf("state.putTrigger: %v", err)
	}
	if len(tc.updates) != 0 {
		t.Fatalf("state.putTrigger touched workflow updates=%#v", tc.updates)
	}
	matched, err := backend.state.matchTriggers(context.Background(), &gestalt.WorkflowEvent{Type: "message.created", Source: "slack"})
	if err != nil {
		t.Fatalf("state.matchTriggers: %v", err)
	}
	if len(matched) != 1 || matched[0].Trigger.ID != trigger.ID {
		t.Fatalf("matched triggers = %#v, want %q", matched, trigger.ID)
	}
	scheduleClient := newFakeScheduleClient(map[string]*client.ScheduleDescription{
		backend.temporalScheduleID("schedule-1"): {
			Schedule: client.Schedule{
				Action: &client.ScheduleWorkflowAction{},
				Spec:   &client.ScheduleSpec{CronExpressions: []string{"0 * * * *"}, TimeZoneName: "America/New_York"},
				State:  &client.ScheduleState{},
			},
		},
	})
	tc.scheduleClient = scheduleClient
	if _, err := backend.UpsertSchedule(context.Background(), &gestalt.UpsertWorkflowProviderScheduleRequest{
		ScheduleID:   "schedule-1",
		Cron:         "0 * * * *",
		Timezone:     "America/New_York",
		Target:       nativeAppTargetInput("slack", "postMessage"),
		DefinitionID: "schedule-definition",
		RequestedBySubjectID: "system:config",
	}); err != nil {
		t.Fatalf("UpsertSchedule: %v", err)
	}
	if len(tc.updates) != 0 {
		t.Fatalf("UpsertSchedule touched workflow updates=%#v", tc.updates)
	}
	storedSchedule, found, err := backend.state.getSchedule(context.Background(), "schedule-1")
	if err != nil || !found {
		t.Fatalf("state.getSchedule found=%v err=%v", found, err)
	}
	if storedSchedule.DefinitionID != "schedule-definition" {
		t.Fatalf("stored schedule definition_id = %q, want schedule-definition", storedSchedule.DefinitionID)
	}
	action := scheduleClient.handles[backend.temporalScheduleID("schedule-1")].desc.Schedule.Action.(*client.ScheduleWorkflowAction)
	input, ok := action.Args[0].(runWorkflowV4Input)
	if !ok || input.DefinitionID != "schedule-definition" {
		t.Fatalf("schedule action input = %#v, want definition_id schedule-definition", action.Args[0])
	}
}

func TestListSchedulesUsesIndexedDBMetadata(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)

	nextRunAt := time.Unix(200, 0).UTC()
	tc := &recordingTemporalClient{}
	backend := newRecordingTemporalBackend(tc, state)
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
	if err := state.putSchedule(ctx, &gestalt.BoundWorkflowSchedule{
		ID:        "schedule-1",
		Cron:      "0 * * * *",
		Timezone:  "America/New_York",
		Target:    nativeAppTargetInput("slack", "postMessage"),
		CreatedAt: time.Unix(100, 0).UTC(),
		UpdatedAt: time.Unix(100, 0).UTC(),
	}); err != nil {
		t.Fatalf("putSchedule: %v", err)
	}

	resp, err := backend.ListSchedules(ctx, &gestalt.ListWorkflowProviderSchedulesRequest{})
	if err != nil {
		t.Fatalf("ListSchedules: %v", err)
	}
	if len(resp.GetSchedules()) != 1 || resp.GetSchedules()[0].ID != "schedule-1" {
		t.Fatalf("schedules = %#v, want schedule-1", resp.GetSchedules())
	}
	if got := resp.GetSchedules()[0].NextRunAt; got == nil || !got.Equal(nextRunAt) {
		t.Fatalf("next_run_at = %v, want %v", got, nextRunAt)
	}
	if scheduleClient.listCount != 0 {
		t.Fatalf("Temporal schedule list calls = %d, want 0", scheduleClient.listCount)
	}
	if len(tc.updates) != 0 {
		t.Fatalf("ListSchedules touched workflow updates=%#v", tc.updates)
	}
}

func TestStartRunUsesV4WorkflowAndStoresRunProjection(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)

	tc := &recordingTemporalClient{}
	backend := newRecordingTemporalBackend(tc, state)

	run, err := backend.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		Target:       nativeAppTargetInput("slack", "postMessage"),
		CreatedBySubjectID: actor("user-1"),
		DefinitionID: "definition-1",
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
	if input.DefinitionID != "definition-1" {
		t.Fatalf("execution definition_id = %q, want definition-1", input.DefinitionID)
	}
	projected, found, err := state.getRun(ctx, run.ID)
	if err != nil || !found {
		t.Fatalf("projected run found=%v err=%v", found, err)
	}
	if projected.ID != run.ID || projected.Status != gestalt.WorkflowRunStatusValuePending {
		t.Fatalf("projected run = %#v, want pending %q", projected, run.ID)
	}
	if projected.DefinitionID != "definition-1" {
		t.Fatalf("projected definition_id = %q, want definition-1", projected.DefinitionID)
	}
}

func TestStartRunWithWorkflowKeyUsesV4AndStoresOwnership(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)

	tc := &recordingTemporalClient{}
	backend := newRecordingTemporalBackend(tc, state)

	run, err := backend.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		WorkflowKey:  "thread-1",
		Target:       nativeAppTargetInput("slack", "postMessage"),
		CreatedBySubjectID: actor("user-1"),
		DefinitionID: "thread-definition",
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
	if input.WorkflowKey != "thread-1" || input.OwnerKey != "slack" || input.DefinitionID != "thread-definition" {
		t.Fatalf("v4 input workflow_key=%q owner_key=%q definition_id=%q, want thread-1/slack/thread-definition", input.WorkflowKey, input.OwnerKey, input.DefinitionID)
	}
	if !tc.hasUpdate(tc.executions[0].WorkflowID, updateClaimRun) {
		t.Fatalf("updates = %#v, want claim update on v4 run", tc.updates)
	}
	owned, found, err := state.getWorkflowKeyRun(ctx, "thread-1")
	if err != nil || !found {
		t.Fatalf("getWorkflowKeyRun found=%v err=%v", found, err)
	}
	if owned.ID != run.ID {
		t.Fatalf("owned run = %q, want %q", owned.ID, run.ID)
	}
	handle, err := decodeTemporalRunHandle(run.ID)
	if err != nil {
		t.Fatalf("decode run handle: %v", err)
	}
	if handle.Kind != runHandleKindV4 || handle.RunWorkflowID == "" || handle.WorkflowKey != "thread-1" {
		t.Fatalf("handle = %#v, want v4 keyed run handle", handle)
	}
}

func TestStartRunWithWorkflowKeyRejectsActiveOwnerBeforeExecuting(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)

	tc := &recordingTemporalClient{}
	backend := newRecordingTemporalBackend(tc, state)
	if _, err := backend.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		WorkflowKey: "thread-1",
		Target:      nativeAppTargetInput("slack", "postMessage"),
		CreatedBySubjectID: actor("user-1"),
	}); err != nil {
		t.Fatalf("StartRun(first): %v", err)
	}
	_, err := backend.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		WorkflowKey: "thread-1",
		Target:      nativeAppTargetInput("slack", "sendMessage"),
		CreatedBySubjectID: actor("user-2"),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StartRun(second) error = %v, want FailedPrecondition", err)
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions = %d, want only the first run", len(tc.executions))
	}
}

func TestStartRunWithWorkflowKeyUsesIndexedDBIdempotency(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)

	tc := &recordingTemporalClient{}
	backend := newRecordingTemporalBackend(tc, state)
	req := &gestalt.StartWorkflowProviderRunRequest{
		IdempotencyKey: "start-1",
		WorkflowKey:    "thread-1",
		Target:         nativeAppTargetInput("slack", "postMessage"),
		CreatedBySubjectID: actor("user-1"),
		DefinitionID:   "definition-a",
	}
	first, err := backend.StartRun(ctx, req)
	if err != nil {
		t.Fatalf("StartRun(first): %v", err)
	}
	duplicate, err := backend.StartRun(ctx, req)
	if err != nil {
		t.Fatalf("StartRun(duplicate): %v", err)
	}
	if duplicate.ID != first.ID {
		t.Fatalf("duplicate run id = %q, want %q", duplicate.ID, first.ID)
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions = %d, want 1", len(tc.executions))
	}
	if !tc.hasUpdate(tc.executions[0].WorkflowID, updateClaimRun) {
		t.Fatalf("updates = %#v, want claim update on v4 run", tc.updates)
	}
	_, err = backend.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		IdempotencyKey: "start-1",
		WorkflowKey:    "thread-1",
		Target:         nativeAppTargetInput("slack", "postMessage"),
		CreatedBySubjectID: actor("user-1"),
		DefinitionID:   "definition-b",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StartRun(definition conflict) error = %v, want FailedPrecondition", err)
	}
	_, err = backend.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		IdempotencyKey: "start-1",
		WorkflowKey:    "thread-1",
		Target:         nativeAppTargetInput("slack", "sendMessage"),
		CreatedBySubjectID: actor("user-1"),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StartRun(conflict) error = %v, want FailedPrecondition", err)
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions after conflict = %d, want 1", len(tc.executions))
	}
}

func TestStartRunWithWorkflowKeyCompletesReservedIndexedDBIdempotency(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)

	workflowKey := "thread-1"
	key := "start-1"
	target := nativeAppTargetInput("slack", "postMessage")
	createdBy := actor("user-1")
	fingerprint := startFingerprint("slack", key, workflowKey, "", target, createdBy)
	if _, _, err := state.reserveRunIdempotency(ctx, "slack", key, fingerprint, time.Hour, time.Unix(100, 0).UTC()); err != nil {
		t.Fatalf("reserveRunIdempotency: %v", err)
	}
	temporalWorkflowID := workflowID("scope", "manual-keyed-v4", "slack", key, hashID(workflowKey))
	run := &gestalt.BoundWorkflowRun{
		ID: encodeTemporalRunHandle(temporalRunHandle{
			RunWorkflowID:    temporalWorkflowID,
			RunTemporalRunID: "run-1",
			WorkflowKey:      workflowKey,
			OwnerKey:         "slack",
		}),
		Status:      gestalt.WorkflowRunStatusValuePending,
		Target:      target,
		WorkflowKey: workflowKey,
		CreatedAt:   time.Unix(100, 0).UTC(),
	}
	if _, claimed, err := state.claimWorkflowKeyRun(ctx, workflowKey, run, time.Unix(100, 0).UTC()); err != nil || !claimed {
		t.Fatalf("claimWorkflowKeyRun claimed=%v err=%v", claimed, err)
	}

	tc := &recordingTemporalClient{}
	backend := newRecordingTemporalBackend(tc, state)
	recovered, err := backend.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		IdempotencyKey: key,
		WorkflowKey:    workflowKey,
		Target:         target,
		CreatedBySubjectID: createdBy,
	})
	if err != nil {
		t.Fatalf("StartRun(recovery): %v", err)
	}
	if recovered.ID != run.ID {
		t.Fatalf("recovered run = %q, want %q", recovered.ID, run.ID)
	}
	if len(tc.executions) != 0 {
		t.Fatalf("executions = %d, want none during recovery", len(tc.executions))
	}
	duplicate, err := backend.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		IdempotencyKey: key,
		WorkflowKey:    workflowKey,
		Target:         target,
		CreatedBySubjectID: createdBy,
	})
	if err != nil {
		t.Fatalf("StartRun(duplicate): %v", err)
	}
	if duplicate.ID != run.ID {
		t.Fatalf("duplicate run = %q, want %q", duplicate.ID, run.ID)
	}
}

func TestStartRunContinuesWhenInitialRunProjectionWriteFails(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)
	if err := state.db.DeleteObjectStore(ctx, storeTemporalRunProjections); err != nil {
		t.Fatalf("DeleteObjectStore(%s): %v", storeTemporalRunProjections, err)
	}

	tc := &recordingTemporalClient{}
	backend := newRecordingTemporalBackend(tc, state)

	run, err := backend.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		Target:    nativeAppTargetInput("slack", "postMessage"),
		CreatedBySubjectID: actor("user-1"),
	})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if run.ID == "" || len(tc.executions) != 1 {
		t.Fatalf("run=%#v executions=%d, want started run", run, len(tc.executions))
	}
}

func TestStartRunUsesIndexedDBIdempotencyForUnkeyedRuns(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)

	tc := &recordingTemporalClient{}
	backend := newRecordingTemporalBackend(tc, state)
	req := &gestalt.StartWorkflowProviderRunRequest{
		IdempotencyKey: "start-1",
		Target:         nativeAppTargetInput("slack", "postMessage"),
		CreatedBySubjectID: actor("user-1"),
		DefinitionID:   "definition-a",
	}
	first, err := backend.StartRun(ctx, req)
	if err != nil {
		t.Fatalf("StartRun(first): %v", err)
	}
	duplicate, err := backend.StartRun(ctx, req)
	if err != nil {
		t.Fatalf("StartRun(duplicate): %v", err)
	}
	if duplicate.ID != first.ID {
		t.Fatalf("duplicate run id = %q, want %q", duplicate.ID, first.ID)
	}
	_, err = backend.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		IdempotencyKey: "start-1",
		Target:         nativeAppTargetInput("slack", "postMessage"),
		CreatedBySubjectID: actor("user-1"),
		DefinitionID:   "definition-b",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StartRun(definition conflict) error = %v, want FailedPrecondition", err)
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions = %d, want 1", len(tc.executions))
	}
	if len(tc.updates) != 0 {
		t.Fatalf("updates = %#v, want none", tc.updates)
	}
}

func TestStartRunRejectsConflictingIndexedDBIdempotency(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)

	tc := &recordingTemporalClient{}
	backend := newRecordingTemporalBackend(tc, state)
	_, err := backend.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		IdempotencyKey: "start-1",
		Target:         nativeAppTargetInput("slack", "postMessage"),
		CreatedBySubjectID: actor("user-1"),
	})
	if err != nil {
		t.Fatalf("StartRun(first): %v", err)
	}
	_, err = backend.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		IdempotencyKey: "start-1",
		Target:         nativeAppTargetInput("slack", "sendMessage"),
		CreatedBySubjectID: actor("user-1"),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StartRun(conflict) error = %v, want FailedPrecondition", err)
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions = %d, want 1", len(tc.executions))
	}
}

func TestStartRunReturnsErrorWhenIdempotencyCompletionFails(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)

	tc := &recordingTemporalClient{}
	tc.afterExecute = func() {
		if err := state.db.DeleteObjectStore(ctx, storeTemporalRunIdempotency); err != nil {
			t.Errorf("DeleteObjectStore(%s): %v", storeTemporalRunIdempotency, err)
		}
	}
	backend := newRecordingTemporalBackend(tc, state)

	_, err := backend.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		IdempotencyKey: "start-1",
		Target:         nativeAppTargetInput("slack", "postMessage"),
		CreatedBySubjectID: actor("user-1"),
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("StartRun error = %v, want Internal", err)
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions = %d, want 1", len(tc.executions))
	}
}

func TestCompleteRunIdempotencyReadsThroughCompletedRecord(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)

	ownerKey := "slack"
	key := "start-1"
	fingerprint := "same-request"
	run := &gestalt.BoundWorkflowRun{ID: "run-1", Status: gestalt.WorkflowRunStatusValuePending}
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
	ctx, state := newTestWorkflowStateStore(t)

	tc := &recordingTemporalClient{}
	backend := newRecordingTemporalBackend(tc, state)
	resp, err := backend.SignalOrStartRun(ctx, &gestalt.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  "thread-1",
		Target:       nativeAppTargetInput("slack", "postMessage"),
		CreatedBySubjectID: actor("user-1"),
		Signal:       &gestalt.WorkflowSignal{Name: "slack.event"},
		DefinitionID: "signal-definition",
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun: %v", err)
	}
	if !resp.StartedRun || resp.Run == nil || resp.Run.WorkflowKey != "thread-1" {
		t.Fatalf("response = %#v, want started thread-1 run", resp)
	}
	if resp.Run.DefinitionID != "signal-definition" {
		t.Fatalf("response definition_id = %q, want signal-definition", resp.Run.DefinitionID)
	}
	if resp.Signal == nil || resp.Signal.Sequence != 1 || resp.Signal.ID == "" {
		t.Fatalf("response signal = %#v, want assigned sequence/id", resp.Signal)
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions = %d, want 1", len(tc.executions))
	}
	input, ok := tc.executions[0].Args[0].(runWorkflowV4Input)
	if !ok {
		t.Fatalf("execution input = %T, want runWorkflowV4Input", tc.executions[0].Args[0])
	}
	if input.WorkflowKey != "thread-1" || input.DefinitionID != "signal-definition" || !input.RequireSignal || !input.RequireClaim {
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
	owned, found, err := state.getWorkflowKeyRun(ctx, "thread-1")
	if err != nil || !found || resp.Run == nil || owned.ID != resp.Run.ID {
		t.Fatalf("owned found=%v run=%#v err=%v, want response run", found, owned, err)
	}
}

func TestSignalOrStartRunUsesIndexedDBSignalIdempotency(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)

	tc := &recordingTemporalClient{}
	backend := newRecordingTemporalBackend(tc, state)
	req := &gestalt.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey: "thread-1",
		Target:      nativeAppTargetInput("slack", "postMessage"),
		CreatedBySubjectID: actor("user-1"),
		Signal:      &gestalt.WorkflowSignal{Name: "slack.event", IdempotencyKey: "signal-1"},
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
	if duplicate.Run == nil || first.Run == nil || duplicate.Signal == nil || first.Signal == nil || duplicate.Run.ID != first.Run.ID || duplicate.Signal.ID != first.Signal.ID {
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
	ctx, state := newTestWorkflowStateStore(t)

	tc := &recordingTemporalClient{}
	backend := newRecordingTemporalBackend(tc, state)
	signal := &gestalt.WorkflowSignal{ID: "signal-id-1", Name: "slack.event"}
	if _, err := backend.SignalOrStartRun(ctx, &gestalt.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey: "thread-1",
		Target:      nativeAppTargetInput("slack", "postMessage"),
		CreatedBySubjectID: actor("user-1"),
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
	ctx, state := newTestWorkflowStateStore(t)

	tc := &recordingTemporalClient{}
	backend := newRecordingTemporalBackend(tc, state)
	req := &gestalt.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey: "thread-1",
		Target:      nativeAppTargetInput("slack", "postMessage"),
		CreatedBySubjectID: actor("user-1"),
		Signal:      &gestalt.WorkflowSignal{ID: "signal-id-1", Name: "slack.event", IdempotencyKey: "owner-key-1"},
	}
	if _, err := backend.SignalOrStartRun(ctx, req); err != nil {
		t.Fatalf("SignalOrStartRun(first): %v", err)
	}
	updateCount := len(tc.updates)
	duplicateReq := cloneSignalOrStartRequest(req)
	duplicateReq.Signal.Name = "slack.changed"
	_, err := backend.SignalOrStartRun(ctx, duplicateReq)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("SignalOrStartRun(conflict) error = %v, want FailedPrecondition", err)
	}
	if len(tc.updates) != updateCount {
		t.Fatalf("updates = %#v, want no second temporal signal", tc.updates[updateCount:])
	}
}

func TestSignalOrStartRunSignalsExistingV4Workflow(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)

	tc := &recordingTemporalClient{}
	backend := newRecordingTemporalBackend(tc, state)
	run, err := backend.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		WorkflowKey: "thread-1",
		Target:      nativeAppTargetInput("slack", "postMessage"),
		CreatedBySubjectID: actor("user-1"),
	})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	handle, err := decodeTemporalRunHandle(run.ID)
	if err != nil {
		t.Fatalf("decode run handle: %v", err)
	}
	updateStart := len(tc.updates)

	resp, err := backend.SignalOrStartRun(ctx, &gestalt.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey: "thread-1",
		Target:      nativeAppTargetInput("slack", "sendMessage"),
		CreatedBySubjectID: actor("user-2"),
		Signal:      &gestalt.WorkflowSignal{Name: "slack.event"},
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun: %v", err)
	}
	if resp.StartedRun || resp.Run == nil || resp.Run.ID != run.ID {
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
	ctx, state := newTestWorkflowStateStore(t)

	tc := &recordingTemporalClient{}
	backend := newRecordingTemporalBackend(tc, state)
	first, err := backend.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		WorkflowKey: "thread-1",
		Target:      nativeAppTargetInput("slack", "postMessage"),
		CreatedBySubjectID: actor("user-1"),
	})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	storedFirst, found, err := state.getRun(ctx, first.ID)
	if err != nil || !found {
		t.Fatalf("get first run found=%v err=%v", found, err)
	}
	terminal := *storedFirst
	terminal.Status = gestalt.WorkflowRunStatusValueSucceeded
	if err := state.putRun(ctx, &terminal); err != nil {
		t.Fatalf("put terminal run: %v", err)
	}

	resp, err := backend.SignalOrStartRun(ctx, &gestalt.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey: "thread-1",
		Target:      nativeAppTargetInput("slack", "sendMessage"),
		CreatedBySubjectID: actor("user-2"),
		Signal:      &gestalt.WorkflowSignal{Name: "slack.event"},
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun: %v", err)
	}
	if !resp.StartedRun || resp.Run == nil || resp.Run.ID == first.ID {
		t.Fatalf("response = %#v, want replacement run", resp)
	}
	if len(tc.executions) != 2 {
		t.Fatalf("executions = %d, want replacement execution", len(tc.executions))
	}
	owned, found, err := state.getWorkflowKeyRun(ctx, "thread-1")
	if err != nil || !found || resp.Run == nil || owned.ID != resp.Run.ID {
		t.Fatalf("owned found=%v run=%#v err=%v, want replacement", found, owned, err)
	}
}

func TestSignalOrStartRunReplacesMissingWorkflowKeyOwner(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)

	stale := workflowKeyClaimRun("stale", "thread-1", gestalt.WorkflowRunStatusValuePending)
	if _, claimed, err := state.claimWorkflowKeyRun(ctx, "thread-1", stale, time.Unix(100, 0).UTC()); err != nil || !claimed {
		t.Fatalf("claim stale run claimed=%v err=%v", claimed, err)
	}
	tc := &recordingTemporalClient{updateErrs: []error{serviceerror.NewNotFound("missing workflow")}}
	backend := newRecordingTemporalBackend(tc, state)
	resp, err := backend.SignalOrStartRun(ctx, &gestalt.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey: "thread-1",
		Target:      nativeAppTargetInput("slack", "sendMessage"),
		CreatedBySubjectID: actor("user-2"),
		Signal:      &gestalt.WorkflowSignal{Name: "slack.event"},
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun: %v", err)
	}
	if !resp.StartedRun || resp.Run == nil || resp.Run.ID == stale.ID {
		t.Fatalf("response = %#v, want replacement run", resp)
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions = %d, want replacement execution", len(tc.executions))
	}
	owned, found, err := state.getWorkflowKeyRun(ctx, "thread-1")
	if err != nil || !found || resp.Run == nil || owned.ID != resp.Run.ID {
		t.Fatalf("owned found=%v run=%#v err=%v, want replacement", found, owned, err)
	}
}

func TestSignalRunUsesIndexedDBSignalIdempotency(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)

	run := workflowKeyClaimRun("signal-idem", "thread-1", gestalt.WorkflowRunStatusValuePending)
	tc := &recordingTemporalClient{}
	backend := newRecordingTemporalBackend(tc, state)
	req := &gestalt.SignalWorkflowProviderRunRequest{
		RunID:  run.ID,
		Signal: &gestalt.WorkflowSignal{Name: "slack.event", IdempotencyKey: "signal-1"},
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
	if duplicate.Signal == nil || first.Signal == nil || duplicate.Signal.ID != first.Signal.ID {
		t.Fatalf("duplicate signal = %#v, want first signal %#v", duplicate.Signal, first.Signal)
	}
	if len(tc.updates) != updateCount {
		t.Fatalf("updates = %#v, want no duplicate temporal signal", tc.updates[updateCount:])
	}
	key := ownerIdempotencyLedgerKey("slack", "signal-1")
	record, err := state.signalIdempotency.Get(ctx, state.signalIdempotencyID(key))
	if err != nil {
		t.Fatalf("load signal idempotency record: %v", err)
	}
	if recordString(record, "status") != "completed" || recordString(record, "run_id") != run.ID {
		t.Fatalf("signal idempotency record = %#v, want completed run %q", record, run.ID)
	}
}

func TestSignalRunRejectsExplicitSignalIDPayloadMismatchWithOwnerKey(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)

	run := workflowKeyClaimRun("strict-signal-id", "thread-1", gestalt.WorkflowRunStatusValuePending)
	tc := &recordingTemporalClient{}
	backend := newRecordingTemporalBackend(tc, state)
	if _, err := backend.SignalRun(ctx, &gestalt.SignalWorkflowProviderRunRequest{
		RunID:  run.ID,
		Signal: &gestalt.WorkflowSignal{ID: "signal-id-1", Name: "slack.event", IdempotencyKey: "owner-key-1"},
	}); err != nil {
		t.Fatalf("SignalRun(first): %v", err)
	}
	updateCount := len(tc.updates)
	_, err := backend.SignalRun(ctx, &gestalt.SignalWorkflowProviderRunRequest{
		RunID:  run.ID,
		Signal: &gestalt.WorkflowSignal{ID: "signal-id-1", Name: "slack.changed", IdempotencyKey: "owner-key-1"},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("SignalRun(conflict) error = %v, want FailedPrecondition", err)
	}
	if len(tc.updates) != updateCount {
		t.Fatalf("updates = %#v, want no second temporal signal", tc.updates[updateCount:])
	}
}

func TestWorkflowStateStoreClaimsWorkflowKeyRun(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)

	workflowKey := "slack:T:C:1778164397.804829"
	now := time.Unix(200, 0).UTC()
	run := workflowKeyClaimRun("first", workflowKey, gestalt.WorkflowRunStatusValuePending)
	owner, claimed, err := state.claimWorkflowKeyRun(ctx, workflowKey, run, now)
	if err != nil {
		t.Fatalf("claimWorkflowKeyRun: %v", err)
	}
	if !claimed || owner.ID != run.ID {
		t.Fatalf("claim owner=%q claimed=%v, want caller", owner.ID, claimed)
	}
	got, found, err := state.getWorkflowKeyRun(ctx, workflowKey)
	if err != nil || !found || got.ID != run.ID {
		t.Fatalf("getWorkflowKeyRun found=%v run=%#v err=%v, want first run", found, got, err)
	}
	record, err := state.workflowKeys.Get(ctx, state.workflowKeyID(workflowKey))
	if err != nil {
		t.Fatalf("load workflow key record: %v", err)
	}
	handle, err := decodeTemporalRunHandle(run.ID)
	if err != nil {
		t.Fatalf("decode run handle: %v", err)
	}
	if recordString(record, "id") != state.workflowKeyID(workflowKey) ||
		recordString(record, "scope_id") != "scope" ||
		recordString(record, "workflow_key") != workflowKey ||
		recordString(record, "owner_key") != "slack" ||
		recordString(record, "run_id") != run.ID ||
		recordString(record, "temporal_workflow_id") != handle.RunWorkflowID ||
		recordString(record, "temporal_run_id") != handle.RunTemporalRunID ||
		recordInt64(record, "status") != int64(gestalt.WorkflowRunStatusValuePending) {
		t.Fatalf("workflow key record = %#v, want routing metadata for first run", record)
	}
	if createdAt, updatedAt := recordTime(record, "created_at"), recordTime(record, "updated_at"); createdAt == nil || updatedAt == nil || !createdAt.Equal(now) || !updatedAt.Equal(now) {
		t.Fatalf("record timestamps created=%v updated=%v, want %v", createdAt, updatedAt, now)
	}

	running := *run
	running.Status = gestalt.WorkflowRunStatusValueRunning
	owner, claimed, err = state.claimWorkflowKeyRun(ctx, workflowKey, &running, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("claimWorkflowKeyRun(same run): %v", err)
	}
	if !claimed || owner.ID != run.ID || owner.Status != gestalt.WorkflowRunStatusValueRunning {
		t.Fatalf("same-run claim owner=%#v claimed=%v, want running caller", owner, claimed)
	}
	other := workflowKeyClaimRun("other", workflowKey, gestalt.WorkflowRunStatusValuePending)
	owner, claimed, err = state.claimWorkflowKeyRun(ctx, workflowKey, other, now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("claimWorkflowKeyRun(conflict): %v", err)
	}
	if claimed || owner.ID != run.ID {
		t.Fatalf("conflict owner=%q claimed=%v, want existing run", owner.ID, claimed)
	}

	cleared, err := state.clearWorkflowKeyRun(ctx, workflowKey, other.ID)
	if err != nil {
		t.Fatalf("clearWorkflowKeyRun(wrong run): %v", err)
	}
	if cleared {
		t.Fatalf("clearWorkflowKeyRun(wrong run) = true, want false")
	}
	cleared, err = state.clearWorkflowKeyRun(ctx, workflowKey, run.ID)
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
	ctx, state := newTestWorkflowStateStore(t)

	workflowKey := "thread-terminal"
	terminal := workflowKeyClaimRun("terminal", workflowKey, gestalt.WorkflowRunStatusValueSucceeded)
	if _, claimed, err := state.claimWorkflowKeyRun(ctx, workflowKey, terminal, time.Unix(100, 0).UTC()); err != nil || !claimed {
		t.Fatalf("claim terminal claimed=%v err=%v", claimed, err)
	}
	got, found, err := state.getWorkflowKeyRun(ctx, workflowKey)
	if err != nil || !found || got.Status != gestalt.WorkflowRunStatusValueSucceeded {
		t.Fatalf("get terminal found=%v run=%#v err=%v, want terminal owner", found, got, err)
	}
	stale := *terminal
	stale.Status = gestalt.WorkflowRunStatusValuePending
	owner, claimed, err := state.claimWorkflowKeyRun(ctx, workflowKey, &stale, time.Unix(150, 0).UTC())
	if err != nil {
		t.Fatalf("claim stale terminal owner: %v", err)
	}
	if !claimed || owner.ID != terminal.ID || owner.Status != gestalt.WorkflowRunStatusValueSucceeded {
		t.Fatalf("stale terminal owner=%#v claimed=%v, want terminal projection", owner, claimed)
	}
	replacement := workflowKeyClaimRun("replacement", workflowKey, gestalt.WorkflowRunStatusValuePending)
	owner, claimed, err = state.claimWorkflowKeyRun(ctx, workflowKey, replacement, time.Unix(200, 0).UTC())
	if err != nil {
		t.Fatalf("claim replacement: %v", err)
	}
	if !claimed || owner.ID != replacement.ID {
		t.Fatalf("replacement owner=%q claimed=%v, want replacement", owner.ID, claimed)
	}

	missingKey := "thread-missing"
	missingRun := workflowKeyClaimRun("missing", missingKey, gestalt.WorkflowRunStatusValuePending)
	missingHandle, err := decodeTemporalRunHandle(missingRun.ID)
	if err != nil {
		t.Fatalf("decode missing run handle: %v", err)
	}
	if err := state.workflowKeys.Put(ctx, state.workflowKeyRecord(workflowKeyRecord{
		ID:                 state.workflowKeyID(missingKey),
		WorkflowKey:        missingKey,
		OwnerKey:           "slack",
		RunID:              missingRun.ID,
		TemporalWorkflowID: missingHandle.RunWorkflowID,
		TemporalRunID:      missingHandle.RunTemporalRunID,
		Status:             gestalt.WorkflowRunStatusValuePending,
		CreatedAt:          time.Unix(300, 0).UTC(),
		UpdatedAt:          time.Unix(300, 0).UTC(),
	})); err != nil {
		t.Fatalf("seed missing workflow key: %v", err)
	}
	missingReplacement := workflowKeyClaimRun("missing-replacement", missingKey, gestalt.WorkflowRunStatusValuePending)
	owner, claimed, err = state.claimWorkflowKeyRun(ctx, missingKey, missingReplacement, time.Unix(400, 0).UTC())
	if err != nil {
		t.Fatalf("claim missing replacement: %v", err)
	}
	if !claimed || owner.ID != missingReplacement.ID {
		t.Fatalf("missing replacement owner=%q claimed=%v, want replacement", owner.ID, claimed)
	}
}

func TestWorkflowStateStoreWorkflowKeyClaimValidationAndScopeIsolation(t *testing.T) {
	db := startTestIndexedDBBackend(t)
	ctx := context.Background()
	scopeA, err := openWorkflowStateStore(ctx, "scope-a", db)
	if err != nil {
		t.Fatalf("open scope-a: %v", err)
	}
	t.Cleanup(func() { _ = scopeA.Close() })
	scopeB, err := openWorkflowStateStore(ctx, "scope-b", db)
	if err != nil {
		t.Fatalf("open scope-b: %v", err)
	}
	t.Cleanup(func() { _ = scopeB.Close() })

	valid := workflowKeyClaimRun("valid", "thread", gestalt.WorkflowRunStatusValuePending)
	for name, tc := range map[string]struct {
		workflowKey string
		run         *gestalt.BoundWorkflowRun
	}{
		"empty workflow key": {workflowKey: "", run: valid},
		"nil run":            {workflowKey: "thread", run: nil},
		"empty run id":       {workflowKey: "thread", run: &gestalt.BoundWorkflowRun{}},
		"malformed run id":   {workflowKey: "thread", run: &gestalt.BoundWorkflowRun{ID: "not-a-handle", WorkflowKey: "thread", Target: nativeAppTargetInput("slack", "postMessage")}},
		"missing temporal run id": {
			workflowKey: "thread",
			run: &gestalt.BoundWorkflowRun{
				ID:          encodeTemporalRunHandle(temporalRunHandle{RunWorkflowID: "workflow-without-run-id", WorkflowKey: "thread", OwnerKey: "slack"}),
				WorkflowKey: "thread",
				Target:      nativeAppTargetInput("slack", "postMessage"),
			},
		},
	} {
		if _, _, err := scopeA.claimWorkflowKeyRun(ctx, tc.workflowKey, tc.run, time.Unix(100, 0).UTC()); err == nil {
			t.Fatalf("%s claim succeeded, want error", name)
		}
	}

	runA := workflowKeyClaimRun("scope-a", "shared-thread", gestalt.WorkflowRunStatusValuePending)
	runB := workflowKeyClaimRun("scope-b", "shared-thread", gestalt.WorkflowRunStatusValuePending)
	if _, claimed, err := scopeA.claimWorkflowKeyRun(ctx, "shared-thread", runA, time.Unix(200, 0).UTC()); err != nil || !claimed {
		t.Fatalf("scopeA claim claimed=%v err=%v", claimed, err)
	}
	if _, claimed, err := scopeB.claimWorkflowKeyRun(ctx, "shared-thread", runB, time.Unix(200, 0).UTC()); err != nil || !claimed {
		t.Fatalf("scopeB claim claimed=%v err=%v", claimed, err)
	}
	gotA, found, err := scopeA.getWorkflowKeyRun(ctx, "shared-thread")
	if err != nil || !found || gotA.ID != runA.ID {
		t.Fatalf("scopeA get found=%v run=%#v err=%v, want runA", found, gotA, err)
	}
	gotB, found, err := scopeB.getWorkflowKeyRun(ctx, "shared-thread")
	if err != nil || !found || gotB.ID != runB.ID {
		t.Fatalf("scopeB get found=%v run=%#v err=%v, want runB", found, gotB, err)
	}
}

func TestWorkflowStateStoreWorkflowKeyConcurrentClaim(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)

	workflowKey := "thread-race"
	runs := []*gestalt.BoundWorkflowRun{
		workflowKeyClaimRun("race-a", workflowKey, gestalt.WorkflowRunStatusValuePending),
		workflowKeyClaimRun("race-b", workflowKey, gestalt.WorkflowRunStatusValuePending),
	}
	type claimResult struct {
		owner   *gestalt.BoundWorkflowRun
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
		if result.owner == nil || result.owner.ID == "" {
			t.Fatalf("concurrent claim owner = %#v, want owner", result.owner)
		}
		if result.claimed {
			claimedCount++
			winner = result.owner.ID
		}
	}
	if claimedCount != 1 {
		t.Fatalf("claimed count = %d, want 1", claimedCount)
	}
	got, found, err := state.getWorkflowKeyRun(ctx, workflowKey)
	if err != nil || !found || got.ID != winner {
		t.Fatalf("stored owner found=%v run=%#v err=%v, want winner %q", found, got, err, winner)
	}
}

func TestWorkflowStateStoreIgnoresUnsupportedRunHandleRecords(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)

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
	legacyRun := &gestalt.BoundWorkflowRun{ID: legacyID, Status: gestalt.WorkflowRunStatusValuePending, Target: nativeAppTargetInput("slack", "postMessage"), WorkflowKey: "legacy-thread"}
	currentRun := &gestalt.BoundWorkflowRun{ID: currentID, Status: gestalt.WorkflowRunStatusValuePending, Target: nativeAppTargetInput("slack", "postMessage"), WorkflowKey: "current-thread"}
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
		Status:             gestalt.WorkflowRunStatusValuePending,
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
		Status:             gestalt.WorkflowRunStatusValuePending,
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
	replacementRun := &gestalt.BoundWorkflowRun{ID: replacementID, Status: gestalt.WorkflowRunStatusValuePending, Target: nativeAppTargetInput("slack", "postMessage"), WorkflowKey: "legacy-thread"}
	owner, claimed, err := state.claimWorkflowKeyRun(ctx, "legacy-thread", replacementRun, time.Unix(200, 0).UTC())
	if err != nil {
		t.Fatalf("claim replacement over unsupported owner: %v", err)
	}
	if !claimed || owner.ID != replacementID {
		t.Fatalf("replacement claim owner=%q claimed=%v, want replacement", owner.ID, claimed)
	}
	tc := &recordingTemporalClient{}
	backend := newRecordingTemporalBackend(tc, state)
	if _, err := backend.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: legacyID}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GetRun legacy projection error = %v, want InvalidArgument", err)
	}
	listed, err := backend.ListRuns(ctx, &gestalt.ListWorkflowProviderRunsRequest{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	for _, run := range listed.GetRuns() {
		if run.ID == legacyID {
			t.Fatalf("ListRuns included legacy projection %#v", run)
		}
	}
}

func TestGetRunUsesIndexedDBRunProjectionOnly(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)
	backend := newRecordingTemporalBackend(&recordingTemporalClient{}, state)
	missingID := encodeTemporalRunHandle(temporalRunHandle{
		RunWorkflowID:    "missing-workflow",
		RunTemporalRunID: "missing-run",
		OwnerKey:         "slack",
	})

	if _, err := backend.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: missingID}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetRun error = %v, want NotFound", err)
	}
}

func TestListRunsIncludesIndexedDBRunProjections(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)
	run := &gestalt.BoundWorkflowRun{
		ID: encodeTemporalRunHandle(temporalRunHandle{
			RunWorkflowID:    "run-projected-workflow",
			RunTemporalRunID: "run-projected-temporal-run",
			OwnerKey:         "slack",
		}),
		Status:    gestalt.WorkflowRunStatusValueSucceeded,
		Target:    nativeAppTargetInput("slack", "postMessage"),
		Trigger:   &gestalt.WorkflowRunTrigger{Manual: true},
		CreatedAt: time.Unix(100, 0).UTC(),
	}
	if err := state.putRun(ctx, run); err != nil {
		t.Fatalf("putRun: %v", err)
	}

	tc := &recordingTemporalClient{}
	backend := newRecordingTemporalBackend(tc, state)
	resp, err := backend.ListRuns(ctx, &gestalt.ListWorkflowProviderRunsRequest{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(resp.GetRuns()) != 1 || resp.GetRuns()[0].ID != run.ID {
		t.Fatalf("runs = %#v, want projected run", resp.GetRuns())
	}
	if len(tc.updates) != 0 {
		t.Fatalf("temporal updates=%#v, want indexeddb-only list", tc.updates)
	}
}

func TestListRunsPaginatesAndFiltersIndexedDBRunProjections(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)
	runs := []*gestalt.BoundWorkflowRun{
		{
			ID: encodeTemporalRunHandle(temporalRunHandle{
				RunWorkflowID:    "run-alpha-workflow",
				RunTemporalRunID: "run-alpha-temporal-run",
				OwnerKey:         "slack",
			}),
			Status:    gestalt.WorkflowRunStatusValueSucceeded,
			Target:    nativeAppTargetInput("slack", "postMessage"),
			Trigger:   &gestalt.WorkflowRunTrigger{Manual: true},
			CreatedAt: time.Unix(100, 0).UTC(),
		},
		{
			ID: encodeTemporalRunHandle(temporalRunHandle{
				RunWorkflowID:    "run-beta-workflow",
				RunTemporalRunID: "run-beta-temporal-run",
				OwnerKey:         "github",
			}),
			Status:    gestalt.WorkflowRunStatusValueSucceeded,
			Target:    nativeAppTargetInput("github", "createIssue"),
			Trigger:   &gestalt.WorkflowRunTrigger{Manual: true},
			CreatedAt: time.Unix(200, 0).UTC(),
		},
		{
			ID: encodeTemporalRunHandle(temporalRunHandle{
				RunWorkflowID:    "run-charlie-workflow",
				RunTemporalRunID: "run-charlie-temporal-run",
				OwnerKey:         "slack",
			}),
			Status:    gestalt.WorkflowRunStatusValueSucceeded,
			Target:    nativeAppTargetInput("slack", "postMessage"),
			Trigger:   &gestalt.WorkflowRunTrigger{Manual: true},
			CreatedAt: time.Unix(300, 0).UTC(),
		},
		{
			ID: encodeTemporalRunHandle(temporalRunHandle{
				RunWorkflowID:    "run-delta-workflow",
				RunTemporalRunID: "run-delta-temporal-run",
				OwnerKey:         "slack",
			}),
			Status:    gestalt.WorkflowRunStatusValueSucceeded,
			Target:    nativeAppTargetInput("slack", "postMessage"),
			Trigger:   &gestalt.WorkflowRunTrigger{Manual: true},
			CreatedAt: time.Unix(400, 0).UTC(),
		},
	}
	for _, run := range runs {
		if err := state.putRun(ctx, run); err != nil {
			t.Fatalf("putRun %q: %v", run.ID, err)
		}
	}

	backend := newRecordingTemporalBackend(&recordingTemporalClient{}, state)
	first, err := backend.ListRuns(ctx, &gestalt.ListWorkflowProviderRunsRequest{
		PageSize: 2,
		Status:   gestalt.WorkflowRunStatusValueSucceeded,
	})
	if err != nil {
		t.Fatalf("first ListRuns: %v", err)
	}
	if len(first.GetRuns()) != 2 || first.GetNextPageToken() == "" {
		t.Fatalf("first page runs=%#v next=%q, want two runs and next token", first.GetRuns(), first.GetNextPageToken())
	}
	if first.GetRuns()[0].ID != runs[3].ID || first.GetRuns()[1].ID != runs[2].ID {
		t.Fatalf("first page order = [%q %q], want newest succeeded runs [%q %q]", first.GetRuns()[0].ID, first.GetRuns()[1].ID, runs[3].ID, runs[2].ID)
	}
	assertSucceededRun := func(t *testing.T, run gestalt.BoundWorkflowRun) {
		t.Helper()
		if run.Status != gestalt.WorkflowRunStatusValueSucceeded {
			t.Fatalf("run = %#v, want succeeded run", run)
		}
	}
	assertSucceededRun(t, first.GetRuns()[0])
	assertSucceededRun(t, first.GetRuns()[1])

	second, err := backend.ListRuns(ctx, &gestalt.ListWorkflowProviderRunsRequest{
		PageSize:  2,
		PageToken: first.GetNextPageToken(),
		Status:    gestalt.WorkflowRunStatusValueSucceeded,
	})
	if err != nil {
		t.Fatalf("second ListRuns: %v", err)
	}
	if len(second.GetRuns()) != 2 || second.GetNextPageToken() != "" {
		t.Fatalf("second page runs=%#v next=%q, want final two runs", second.GetRuns(), second.GetNextPageToken())
	}
	if second.GetRuns()[0].ID != runs[1].ID || second.GetRuns()[1].ID != runs[0].ID {
		t.Fatalf("second page order = [%q %q], want remaining succeeded runs [%q %q]", second.GetRuns()[0].ID, second.GetRuns()[1].ID, runs[1].ID, runs[0].ID)
	}
	assertSucceededRun(t, second.GetRuns()[0])
	assertSucceededRun(t, second.GetRuns()[1])
	for _, run := range first.GetRuns() {
		for _, secondRun := range second.GetRuns() {
			if run.ID == secondRun.ID {
				t.Fatalf("second page repeated run %q", secondRun.ID)
			}
		}
	}
}

func TestListRunsRejectsCrossScopePageToken(t *testing.T) {
	_, state := newTestWorkflowStateStore(t)
	token := encodeRunListPageToken(&gestalt.BoundWorkflowRun{
		ID:        "run-id",
		CreatedAt: time.Unix(100, 0).UTC(),
	}, "other-scope", nil)
	backend := newRecordingTemporalBackend(&recordingTemporalClient{}, state)
	_, err := backend.ListRuns(context.Background(), &gestalt.ListWorkflowProviderRunsRequest{PageToken: token})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ListRuns cross-scope token error = %v, want InvalidArgument", err)
	}
}

func TestListRunsRejectsPageTokenWithChangedFilters(t *testing.T) {
	_, state := newTestWorkflowStateStore(t)
	token := encodeRunListPageToken(&gestalt.BoundWorkflowRun{
		ID:        "run-id",
		CreatedAt: time.Unix(100, 0).UTC(),
	}, state.scopeID, &gestalt.ListWorkflowProviderRunsRequest{
		Status: gestalt.WorkflowRunStatusValueSucceeded,
	})
	backend := newRecordingTemporalBackend(&recordingTemporalClient{}, state)
	_, err := backend.ListRuns(context.Background(), &gestalt.ListWorkflowProviderRunsRequest{
		PageToken: token,
		Status:    gestalt.WorkflowRunStatusValueRunning,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ListRuns changed-filter token error = %v, want InvalidArgument", err)
	}
}

func TestWorkflowStateStoreWritesNativeRunPayloads(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)
	nativeID := encodeTemporalRunHandle(temporalRunHandle{
		RunWorkflowID:    "native-workflow",
		RunTemporalRunID: "native-run",
		OwnerKey:         "slack",
	})
	nativeRun := &gestalt.BoundWorkflowRun{
		ID:        nativeID,
		Status:    gestalt.WorkflowRunStatusValuePending,
		Target:    nativeAppTargetInput("slack", "postMessage"),
		Trigger:   &gestalt.WorkflowRunTrigger{Manual: true},
		CreatedAt: time.Unix(200, 0).UTC(),
	}
	if err := state.putRun(ctx, nativeRun); err != nil {
		t.Fatalf("put native run: %v", err)
	}
	record, err := state.runProjections.Get(ctx, state.scopedID(nativeID))
	if err != nil {
		t.Fatalf("load native projection: %v", err)
	}
	if payload := recordBytes(record, "payload"); !json.Valid(payload) {
		t.Fatalf("native projection payload is not JSON: %q", payload)
	}
}

func TestTriggerMatchKeysAreReplacedAtomically(t *testing.T) {
	_, state := newTestWorkflowStateStore(t)
	backend := newRecordingTemporalBackend(&recordingTemporalClient{}, state)

	trigger := &gestalt.BoundWorkflowEventTrigger{
		ID:        "trigger-1",
		Match:     &gestalt.WorkflowEventMatch{Type: "message.created"},
		Target:    nativeAppTargetInput("slack", "postMessage"),
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	if err := backend.state.putTrigger(context.Background(), trigger); err != nil {
		t.Fatalf("state.putTrigger(first): %v", err)
	}
	trigger.Match = &gestalt.WorkflowEventMatch{Type: "reaction.added"}
	if err := backend.state.putTrigger(context.Background(), trigger); err != nil {
		t.Fatalf("state.putTrigger(second): %v", err)
	}
	oldMatches, err := backend.state.matchTriggers(context.Background(), &gestalt.WorkflowEvent{Type: "message.created", Source: "slack"})
	if err != nil {
		t.Fatalf("match old: %v", err)
	}
	if len(oldMatches) != 0 {
		t.Fatalf("old match returned %#v, want none", oldMatches)
	}
	newMatches, err := backend.state.matchTriggers(context.Background(), &gestalt.WorkflowEvent{Type: "reaction.added", Source: "slack"})
	if err != nil {
		t.Fatalf("match new: %v", err)
	}
	if len(newMatches) != 1 || newMatches[0].Trigger.ID != trigger.ID {
		t.Fatalf("new match returned %#v, want %q", newMatches, trigger.ID)
	}
}

func TestPublishEventRecordsMatchedTriggersAndStartedRuns(t *testing.T) {
	_, state := newTestWorkflowStateStore(t)

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		_ = provider.Shutdown(context.Background())
	})

	tc := &recordingTemporalClient{}
	backend := newRecordingTemporalBackend(tc, state)
	for _, trigger := range []*gestalt.BoundWorkflowEventTrigger{
		{
			ID:           "trigger-app-1",
			Match:        &gestalt.WorkflowEventMatch{Type: "message.created"},
			Target:       nativeAppTargetInput("allMessages", "postMessage"),
			DefinitionID: "definition-app-1",
			RunAs:        &gestalt.Subject{ID: "service_account:messages-workflow"},
			CreatedAt:    time.Now().UTC(),
			UpdatedAt:    time.Now().UTC(),
		},
		{
			ID:           "trigger-app-2",
			Match:        &gestalt.WorkflowEventMatch{Type: "message.created", Source: "publisherA"},
			Target:       nativeAppTargetInput("sourceConsumer", "processMessage"),
			DefinitionID: "definition-app-2",
			RunAs:        &gestalt.Subject{ID: "service_account:source-workflow"},
			CreatedAt:    time.Now().UTC(),
			UpdatedAt:    time.Now().UTC(),
		},
		{
			ID:        "trigger-paused",
			Match:     &gestalt.WorkflowEventMatch{Type: "message.created"},
			Target:    nativeAppTargetInput("allMessages", "archiveMessage"),
			Paused:    true,
			CreatedAt: time.Now().UTC(),
			UpdatedAt: time.Now().UTC(),
		},
	} {
		if err := backend.state.putTrigger(context.Background(), trigger); err != nil {
			t.Fatalf("state.putTrigger(%s): %v", trigger.ID, err)
		}
	}
	if _, err := backend.setTriggerPaused(context.Background(), "trigger-app-1", true); err != nil {
		t.Fatalf("setTriggerPaused(true): %v", err)
	}
	if _, err := backend.setTriggerPaused(context.Background(), "trigger-app-1", false); err != nil {
		t.Fatalf("setTriggerPaused(false): %v", err)
	}
	if _, err := backend.UpsertEventTrigger(context.Background(), &gestalt.UpsertWorkflowProviderEventTriggerRequest{
		TriggerID:    "trigger-app-2",
		Match:        &gestalt.WorkflowEventMatch{Type: "message.created", Source: "publisherA"},
		Target:       nativeAppTargetInput("sourceConsumer", "processMessage"),
		DefinitionID: "definition-app-2",
		RequestedBySubjectID: actor("config-sync"),
		RunAs:        &gestalt.Subject{ID: "service_account:source-workflow"},
	}); err != nil {
		t.Fatalf("UpsertEventTrigger(existing): %v", err)
	}

	requestEvent := &gestalt.WorkflowEvent{
		ID:     "event-1",
		Source: "publisherB",
		Type:   "message.created",
		Data:   map[string]any{"channel": "C123"},
	}
	published, err := backend.PublishEvent(context.Background(), &gestalt.PublishWorkflowProviderEventRequest{
		AppName:     "publisherA",
		Event:       requestEvent,
		PublishedBySubjectID: actor("publisher-1"),
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	requestEvent.Data.(map[string]any)["channel"] = "mutated"
	if published.ID != "event-1" || published.Source != "publisherA" || published.Type != "message.created" || published.SpecVersion != defaultSpecVersion {
		t.Fatalf("published event = %#v, want normalized input event", published)
	}
	if got := published.Data.(map[string]any)["channel"]; got != "C123" {
		t.Fatalf("published event data channel = %v, want isolated C123", got)
	}
	if len(tc.executions) != 2 {
		t.Fatalf("executions = %d, want 2", len(tc.executions))
	}
	gotDefinitions := map[string]bool{}
	for _, execution := range tc.executions {
		input, ok := execution.Args[0].(runWorkflowV4Input)
		if !ok {
			t.Fatalf("execution input = %T, want runWorkflowV4Input", execution.Args[0])
		}
		gotDefinitions[input.DefinitionID] = true
		if input.DefinitionID == "definition-app-1" && (input.RunAs == nil || input.RunAs.ID != "service_account:messages-workflow") {
			t.Fatalf("definition-app-1 runAs = %#v, want messages workflow subject", input.RunAs)
		}
		if input.DefinitionID == "definition-app-2" && (input.RunAs == nil || input.RunAs.ID != "service_account:source-workflow") {
			t.Fatalf("definition-app-2 runAs = %#v, want source workflow subject", input.RunAs)
		}
		if input.DefinitionID == "definition-app-2" && input.OwnerKey != "sourceConsumer" {
			t.Fatalf("source-specific owner key = %q, want sourceConsumer", input.OwnerKey)
		}
		if input.CreatedBySubjectID == "" || input.CreatedBySubjectID != "publisher-1" {
			t.Fatalf("execution created_by = %#v, want publisher-1", input.CreatedBySubjectID)
		}
	}
	if !gotDefinitions["definition-app-1"] || !gotDefinitions["definition-app-2"] {
		t.Fatalf("execution definition ids = %#v, want both app definitions", gotDefinitions)
	}

	rm := collectTemporalWorkflowMetrics(t, reader)
	metrictestAttrs := temporalWorkflowMetricAttrs(
		gestalt.WorkflowOperationPublishEvent,
		gestalt.WorkflowTriggerKindEvent,
		gestalt.WorkflowTargetKindSteps,
		gestalt.WorkflowRunStatusUnknown,
	)
	requireTemporalInt64Sum(t, rm, "gestaltd.workflows.events.matched_triggers.count", 2, metrictestAttrs)
	requireTemporalInt64Sum(t, rm, "gestaltd.workflows.runs.started.count", 2, temporalWorkflowMetricAttrs(
		gestalt.WorkflowOperationPublishEvent,
		gestalt.WorkflowTriggerKindEvent,
		gestalt.WorkflowTargetKindSteps,
		gestalt.WorkflowRunStatusPending,
	))
}

func TestWorkflowStateStoreScopesMetadataByScopeID(t *testing.T) {
	db := startTestIndexedDBBackend(t)
	ctx := context.Background()
	scopeA, err := openWorkflowStateStore(ctx, "scope-a", db)
	if err != nil {
		t.Fatalf("open scope-a: %v", err)
	}
	t.Cleanup(func() { _ = scopeA.Close() })
	scopeB, err := openWorkflowStateStore(ctx, "scope-b", db)
	if err != nil {
		t.Fatalf("open scope-b: %v", err)
	}
	t.Cleanup(func() { _ = scopeB.Close() })

	trigger := &gestalt.BoundWorkflowEventTrigger{ID: "trigger-1", Match: &gestalt.WorkflowEventMatch{Type: "message.created"}, Target: nativeAppTargetInput("slack", "postMessage"), CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if err := scopeA.putTrigger(ctx, trigger); err != nil {
		t.Fatalf("scopeA put trigger: %v", err)
	}
	matchesB, err := scopeB.matchTriggers(ctx, &gestalt.WorkflowEvent{Type: "message.created", Source: "slack"})
	if err != nil {
		t.Fatalf("scopeB match: %v", err)
	}
	if len(matchesB) != 0 {
		t.Fatalf("scopeB matched scopeA triggers: %#v", matchesB)
	}
}

func TestProviderSurfaceRequiresConfiguredBackend(t *testing.T) {
	provider := New()
	_, err := provider.ListRuns(context.Background(), &gestalt.ListWorkflowProviderRunsRequest{})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ListRuns error = %v, want FailedPrecondition", err)
	}
}

func TestProviderSurfaceStartsBackendForExecutionRPCs(t *testing.T) {
	fw := &fakeTemporalWorker{startErr: errors.New("worker unavailable")}
	backend := newTemporalBackend("temporal", baseTemporalConfig(), &recordingTemporalClient{}, nil, nil)
	backend.newWorker = func(client.Client, string, worker.Options) temporalWorker { return fw }
	provider := &Provider{name: "temporal", backend: backend}

	_, err := provider.StartRun(context.Background(), &gestalt.StartWorkflowProviderRunRequest{})
	if status.Code(err) != codes.Internal {
		t.Fatalf("StartRun error = %v, want Internal", err)
	}
	if fw.startCount != 1 {
		t.Fatalf("worker Start calls = %d, want 1", fw.startCount)
	}
}

type capturingHost struct {
	resp  *gestaltworkflow.Response
	err   error
	calls []gestaltworkflow.Request
}

func (h *capturingHost) Execute(_ context.Context, req gestaltworkflow.Request) (*gestaltworkflow.Response, error) {
	h.calls = append(h.calls, req)
	return h.resp, h.err
}

func (h *capturingHost) Close() error { return nil }

func workflowKeyClaimRun(suffix, workflowKey string, status gestalt.WorkflowRunStatus) *gestalt.BoundWorkflowRun {
	return &gestalt.BoundWorkflowRun{
		ID: encodeTemporalRunHandle(temporalRunHandle{
			RunWorkflowID:    "temporal-workflow-" + strings.TrimSpace(suffix),
			RunTemporalRunID: "temporal-run-" + strings.TrimSpace(suffix),
			WorkflowKey:      strings.TrimSpace(workflowKey),
			OwnerKey:         "slack",
		}),
		Status:      status,
		Target:      nativeAppTargetInput("slack", "postMessage"),
		WorkflowKey: strings.TrimSpace(workflowKey),
		CreatedAt:   time.Unix(100, 0).UTC(),
	}
}

func actor(subjectID string) string {
	return strings.TrimSpace(subjectID)
}

func cloneSignalOrStartRequest(req *gestalt.SignalOrStartWorkflowProviderRunRequest) *gestalt.SignalOrStartWorkflowProviderRunRequest {
	if req == nil {
		return nil
	}
	out := *req
	if req.Signal != nil {
		signal := *req.Signal
		out.Signal = &signal
	}
	if subjectID := strings.TrimSpace(req.CreatedBySubjectID); subjectID != "" {
		out.CreatedBySubjectID = subjectID
	}
	if req.Target != nil {
		target := *req.Target
		out.Target = &target
	}
	return &out
}

func cloneSignalRunRequest(req *gestalt.SignalWorkflowProviderRunRequest) *gestalt.SignalWorkflowProviderRunRequest {
	if req == nil {
		return nil
	}
	out := *req
	if req.Signal != nil {
		signal := *req.Signal
		out.Signal = &signal
	}
	return &out
}

func TestNormalizeTargetPreservesAppCredentialMode(t *testing.T) {
	target := appTarget(" github ", " reviewPullRequest ")
	target.Steps[0].App.CredentialMode = " none "

	scoped, err := normalizeTarget(target)
	if err != nil {
		t.Fatalf("normalizeTarget: %v", err)
	}
	app := firstWorkflowAppStep(scoped.Target)
	if app.Name != "github" || app.Operation != "reviewPullRequest" {
		t.Fatalf("app target = %#v", app)
	}
	if got := app.CredentialMode; got != "none" {
		t.Fatalf("credential mode = %q, want none", got)
	}
}

func TestNormalizeTargetRejectsInvalidAppCredentialMode(t *testing.T) {
	const mode = "unsupported-mode"
	target := appTarget("github", "reviewPullRequest")
	target.Steps[0].App.CredentialMode = mode

	_, err := normalizeTarget(target)
	want := fmt.Sprintf(`target.steps[0].app.credential_mode %q is not supported`, mode)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("normalizeTarget error = %v, want unsupported credential mode %q", err, mode)
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
		"workflowRunTimeout":          time.Minute,
		"activityStartToCloseTimeout": time.Minute,
		"scheduleCatchupWindow":       time.Minute,
		"versioning": map[string]any{
			"deploymentName": "valon-tools-test",
			"buildID":        "revision-1",
		},
	}
}

func baseTemporalConfig() config {
	return config{
		HostPort:                    "localhost:7233",
		Namespace:                   "default",
		APIKey:                      "test-api-key",
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		WorkflowRunTimeout:          time.Minute,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
		Versioning: versioningConfig{
			DeploymentName: "valon-tools-test",
			BuildID:        "revision-1",
		},
	}
}

func newTestWorkflowStateStore(t *testing.T) (context.Context, *workflowStateStore) {
	t.Helper()
	db := startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "scope", db)
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })
	return ctx, state
}

func newRecordingTemporalBackend(tc *recordingTemporalClient, state *workflowStateStore) *temporalBackend {
	return newTemporalBackend("temporal", baseTemporalConfig(), tc, nil, state)
}

func withMap(in map[string]any, key string, value any) map[string]any {
	out := make(map[string]any, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	out[key] = value
	return out
}

type recordedUpdate struct {
	WorkflowID   string
	UpdateID     string
	Name         string
	Args         []any
	WaitForStage client.WorkflowUpdateStage
}

type recordedExecution struct {
	WorkflowID string
	Args       []any
}

type recordingTemporalClient struct {
	client.Client
	mu             sync.Mutex
	executions     []recordedExecution
	updates        []recordedUpdate
	updateErrs     []error
	scheduleClient client.ScheduleClient
	afterExecute   func()
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

func (c *recordingTemporalClient) TerminateWorkflow(_ context.Context, workflowID string, runID string, reason string, details ...interface{}) error {
	return nil
}

func (c *recordingTemporalClient) ScheduleClient() client.ScheduleClient {
	if c.scheduleClient != nil {
		return c.scheduleClient
	}
	return c.Client.ScheduleClient()
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
	case *gestalt.BoundWorkflowRun:
		if len(h.update.Args) > 0 {
			switch run := h.update.Args[len(h.update.Args)-1].(type) {
			case *gestalt.BoundWorkflowRun:
				*out = *cloneRunInput(run)
			case gestalt.BoundWorkflowRun:
				*out = run
			}
		}
	case *gestalt.SignalWorkflowRunResponse:
		if len(h.update.Args) == 0 {
			return nil
		}
		switch resp := h.update.Args[len(h.update.Args)-1].(type) {
		case *gestalt.SignalWorkflowRunResponse:
			*out = *cloneSignalResponseInput(resp)
		case gestalt.WorkflowSignal:
			out.Signal = signalInputForStartedRun(&gestalt.BoundWorkflowRun{ID: h.update.WorkflowID}, &resp)
			out.StartedRun = true
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

func startTestIndexedDBBackend(t *testing.T) indexeddb.Database {
	t.Helper()

	store := relationaldb.New()
	if err := store.Configure(context.Background(), "temporal_workflow_state", map[string]any{
		"dsn": "file:" + filepath.Join(t.TempDir(), "workflow.sqlite") + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)",
	}); err != nil {
		t.Fatalf("relationaldb.Configure: %v", err)
	}

	t.Cleanup(func() { _ = store.Close() })
	return workflowfake.NewProviderDB(store)
}
