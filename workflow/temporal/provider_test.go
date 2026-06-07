package temporal

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	relationaldb "github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb"
	workflowfake "github.com/valon-technologies/gestalt-providers/workflow/indexeddb/fake"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	gestaltworkflow "github.com/valon-technologies/gestalt/sdk/go/workflow"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservicepb "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
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
	host := &capturingHost{resp: &gestaltworkflow.Response{
		Status: http.StatusOK,
		Body:   `{"version":1,"status":"succeeded","steps":[{"id":"postMessage","status":"succeeded"}],"outputs":{"postMessage":"ok"},"finalStepId":"postMessage","finalOutput":"ok"}`,
	}}
	env.RegisterWorkflow(gestaltRunWorkflowV4)
	env.RegisterActivity(&workflowActivities{executor: host, state: state})

	env.ExecuteWorkflow(gestaltRunWorkflowV4, runWorkflowV4Input{
		ActivityStartToCloseTimeoutNS: time.Minute,
		ProviderName:                  "temporal",
		DefinitionID:                  "definition-1",
		DefinitionGeneration:          7,
		Input:                         map[string]any{"ticket": "T-1"},
		RunAs:                         &gestalt.Subject{ID: "service:workflow-test"},
		Target:                        nativeAppTargetInput("slack", "postMessage"),
		Trigger:                       manualTriggerInput(),
		CreatedBySubjectID:            actor("user-1"),
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var run gestalt.WorkflowRun
	if err := env.GetWorkflowResult(&run); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	if run.Status != gestalt.WorkflowRunStatusValueSucceeded || run.Output != "ok" {
		t.Fatalf("run = %#v, want succeeded with output", &run)
	}
	if run.CurrentStepID != "postMessage" || len(run.Steps) != 1 || run.Steps[0].Status != gestalt.WorkflowStepStatusValueSucceeded {
		t.Fatalf("run steps = %#v", run.Steps)
	}
	if run.DefinitionGeneration != 7 || run.Input["ticket"] != "T-1" || run.ProviderName != "temporal" {
		t.Fatalf("run projection fields = %#v", &run)
	}
	projected, found, err := state.getRun(ctx, run.ID)
	if err != nil || !found {
		t.Fatalf("projected run found=%v err=%v", found, err)
	}
	if projected.Output != "ok" || projected.DefinitionGeneration != 7 || projected.Input["ticket"] != "T-1" {
		t.Fatalf("projected = %#v, want output/input/generation", projected)
	}
	if projected.CurrentStepID != "postMessage" || len(projected.Steps) != 1 || projected.Steps[0].Output != "ok" {
		t.Fatalf("projected steps = %#v", projected.Steps)
	}
	if len(host.calls) != 1 ||
		host.calls[0].Request.Input["ticket"] != "T-1" ||
		host.calls[0].Request.DefinitionID != "definition-1" ||
		host.calls[0].Request.DefinitionGeneration != 7 {
		t.Fatalf("host calls = %#v", host.calls)
	}
}

func TestWorkflowStateStorePutRunRefreshesProjection(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)
	createdAt := time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC)
	startedAt := createdAt.Add(time.Second)
	completedAt := startedAt.Add(time.Second)
	runID := encodeTemporalRunHandle(temporalRunHandle{
		RunWorkflowID:    "workflow-1",
		RunTemporalRunID: "run-1",
		OwnerKey:         "app:slack",
	})

	run := &gestalt.WorkflowRun{
		ID:                   runID,
		Status:               gestalt.WorkflowRunStatusValuePending,
		Target:               nativeAppTargetInput("slack", "postMessage"),
		CreatedAt:            createdAt,
		ProviderName:         "temporal",
		DefinitionID:         "definition-1",
		DefinitionGeneration: 1,
		Input:                map[string]any{"version": "pending"},
	}
	if err := state.putRun(ctx, run); err != nil {
		t.Fatalf("putRun(pending): %v", err)
	}

	running := *run
	running.Status = gestalt.WorkflowRunStatusValueRunning
	running.StartedAt = &startedAt
	running.CurrentStepID = "postMessage"
	running.Input = map[string]any{"version": "running"}
	running.Steps = []gestalt.WorkflowStepExecution{{
		StepID:    "postMessage",
		Status:    gestalt.WorkflowStepStatusValueRunning,
		StartedAt: &startedAt,
	}}
	if err := state.putRun(ctx, &running); err != nil {
		t.Fatalf("putRun(running refresh): %v", err)
	}

	projected, found, err := state.getRun(ctx, runID)
	if err != nil || !found {
		t.Fatalf("getRun after refresh found=%v err=%v", found, err)
	}
	if projected.Status != gestalt.WorkflowRunStatusValueRunning ||
		projected.CurrentStepID != "postMessage" ||
		projected.Input["version"] != "running" ||
		len(projected.Steps) != 1 ||
		projected.Steps[0].Status != gestalt.WorkflowStepStatusValueRunning {
		t.Fatalf("projected refresh = %#v", projected)
	}

	staleRunning := running
	staleRunning.Steps = append([]gestalt.WorkflowStepExecution(nil), running.Steps...)

	succeeded := running
	succeeded.Steps = append([]gestalt.WorkflowStepExecution(nil), running.Steps...)
	succeeded.Status = gestalt.WorkflowRunStatusValueSucceeded
	succeeded.CompletedAt = &completedAt
	succeeded.Output = "ok"
	succeeded.Steps[0].Status = gestalt.WorkflowStepStatusValueSucceeded
	succeeded.Steps[0].CompletedAt = &completedAt
	if err := state.putRun(ctx, &succeeded); err != nil {
		t.Fatalf("putRun(succeeded): %v", err)
	}
	if err := state.putRun(ctx, &staleRunning); err != nil {
		t.Fatalf("putRun(stale running): %v", err)
	}

	projected, found, err = state.getRun(ctx, runID)
	if err != nil || !found {
		t.Fatalf("getRun after stale refresh found=%v err=%v", found, err)
	}
	if projected.Status != gestalt.WorkflowRunStatusValueSucceeded ||
		projected.Output != "ok" ||
		projected.CompletedAt == nil ||
		len(projected.Steps) != 1 ||
		projected.Steps[0].Status != gestalt.WorkflowStepStatusValueSucceeded {
		t.Fatalf("projected terminal = %#v", projected)
	}
	runs, nextPageToken, err := state.listRuns(ctx, &gestalt.ListWorkflowProviderRunsRequest{PageSize: 10})
	if err != nil {
		t.Fatalf("listRuns: %v", err)
	}
	if nextPageToken != "" || len(runs) != 1 || runs[0].ID != runID {
		t.Fatalf("listed runs len=%d next=%q runs=%#v", len(runs), nextPageToken, runs)
	}
}

func TestWorkflowStateStorePutRunRetriesProjectionConflict(t *testing.T) {
	ctx := context.Background()
	db := &projectionConflictDB{Database: startTestIndexedDBBackend(t)}
	db.failNextProjectionPut.Store(true)
	state, err := openWorkflowStateStore(ctx, "scope", db)
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	runID := encodeTemporalRunHandle(temporalRunHandle{
		RunWorkflowID:    "workflow-1",
		RunTemporalRunID: "run-1",
		OwnerKey:         "app:slack",
	})
	if err := state.putRun(ctx, &gestalt.WorkflowRun{
		ID:          runID,
		Status:      gestalt.WorkflowRunStatusValueRunning,
		Target:      nativeAppTargetInput("slack", "postMessage"),
		CreatedAt:   time.Date(2026, 6, 7, 12, 0, 0, 0, time.UTC),
		WorkflowKey: "workflow-key-1",
	}); err != nil {
		t.Fatalf("putRun after injected conflict: %v", err)
	}
	if db.failNextProjectionPut.Load() {
		t.Fatalf("projection conflict was not exercised")
	}
	projected, found, err := state.getRun(ctx, runID)
	if err != nil || !found {
		t.Fatalf("projected run found=%v err=%v", found, err)
	}
	if projected.Status != gestalt.WorkflowRunStatusValueRunning || projected.WorkflowKey != "workflow-key-1" {
		t.Fatalf("projected run = %#v", projected)
	}
}

func TestGestaltRunWorkflowV4RunsOneDurableStepAtATime(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)

	var suite testsuite.WorkflowTestSuite
	env := newTestWorkflowEnvironment(&suite)
	host := &capturingHost{resp: &gestaltworkflow.Response{
		Status: http.StatusOK,
		Body:   `{"version":1,"status":"succeeded","steps":[{"id":"collect","status":"succeeded"},{"id":"notify","status":"succeeded"}],"outputs":{"collect":{"ok":true},"notify":{"sent":true}},"finalStepId":"notify","finalOutput":{"sent":true}}`,
	}}
	env.RegisterWorkflow(gestaltRunWorkflowV4)
	env.RegisterActivity(&workflowActivities{executor: host, state: state})

	target := &gestalt.BoundWorkflowTarget{Steps: []gestalt.WorkflowStep{
		{
			ID: "collect",
			App: &gestalt.WorkflowStepAppCall{
				Name:      "github",
				Operation: "pullRequests.get",
			},
		},
		{
			ID: "notify",
			App: &gestalt.WorkflowStepAppCall{
				Name:      "slack",
				Operation: "chat.postMessage",
			},
		},
	}}
	env.ExecuteWorkflow(gestaltRunWorkflowV4, runWorkflowV4Input{
		ActivityStartToCloseTimeoutNS: time.Minute,
		ProviderName:                  "temporal",
		DefinitionID:                  "definition-1",
		DefinitionGeneration:          7,
		RunAs:                         &gestalt.Subject{ID: "service:workflow-test"},
		Target:                        target,
		Trigger:                       manualTriggerInput(),
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var run gestalt.WorkflowRun
	if err := env.GetWorkflowResult(&run); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	if run.Status != gestalt.WorkflowRunStatusValueSucceeded || run.CurrentStepID != "notify" || len(run.Steps) != 2 {
		t.Fatalf("run = %#v", &run)
	}
	if len(host.calls) != 2 || host.calls[0].StepIndex != 0 || host.calls[1].StepIndex != 1 {
		t.Fatalf("host calls = %#v", host.calls)
	}
	collectOutput := host.calls[1].Outputs["collect"].(map[string]any)
	if collectOutput["ok"] != true {
		t.Fatalf("second call outputs = %#v", host.calls[1].Outputs)
	}
	projected, found, err := state.getRun(ctx, run.ID)
	if err != nil || !found {
		t.Fatalf("projected run found=%v err=%v", found, err)
	}
	if projected.CurrentStepID != "notify" || len(projected.Steps) != 2 {
		t.Fatalf("projected steps = %#v", projected.Steps)
	}
}

func TestBackendApplyDefinitionListAndActivationPause(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)
	schedules := newFakeScheduleClient(nil)
	backend := newRecordingTemporalBackend(&recordingTemporalClient{scheduleClient: schedules}, state)

	definition, err := backend.ApplyDefinition(ctx, &gestalt.ApplyWorkflowProviderDefinitionRequest{
		Spec: &gestalt.WorkflowDefinitionSpec{
			ID:     "definition-1",
			Target: nativeAppTargetInput("slack", "postMessage"),
			Activations: []gestalt.WorkflowActivation{
				{
					ID:       "hourly",
					Input:    gestalt.WorkflowValue{Object: map[string]gestalt.WorkflowValue{"channel": {Literal: "ops", LiteralSet: true}}},
					Schedule: &gestalt.WorkflowScheduleActivation{Cron: "0 * * * *"},
				},
				{
					ID:    "message-created",
					Event: &gestalt.WorkflowEventActivation{Match: &gestalt.WorkflowEventMatch{Type: "message.created"}},
				},
			},
			RunAs: &gestalt.Subject{ID: "service-account"},
		},
		RequestedBySubjectID: actor("creator-1"),
	})
	if err != nil {
		t.Fatalf("ApplyDefinition: %v", err)
	}
	if definition.Generation != 1 || definition.ProviderName != "temporal" || definition.CreatedBySubjectID != "creator-1" {
		t.Fatalf("definition = %#v", definition)
	}
	scheduleID := backend.temporalScheduleID("definition-1", "hourly")
	handle := schedules.handles[scheduleID]
	if handle == nil || handle.desc == nil {
		t.Fatalf("schedule %q was not created", scheduleID)
	}
	actionInput := handle.desc.Schedule.Action.(*client.ScheduleWorkflowAction).Args[0].(runWorkflowV4Input)
	if actionInput.DefinitionID != "definition-1" || actionInput.DefinitionGeneration != 1 || actionInput.ActivationID != "hourly" {
		t.Fatalf("schedule action input = %#v", actionInput)
	}
	if actionInput.Input["channel"] != "ops" || actionInput.RunAs.ID != "service-account" {
		t.Fatalf("schedule action run input/run_as = %#v", actionInput)
	}

	listed, err := backend.ListDefinitions(ctx, &gestalt.ListWorkflowProviderDefinitionsRequest{})
	if err != nil {
		t.Fatalf("ListDefinitions: %v", err)
	}
	if len(listed.GetDefinitions()) != 1 || listed.GetDefinitions()[0].ID != "definition-1" {
		t.Fatalf("definitions = %#v", listed.GetDefinitions())
	}

	paused, err := backend.SetActivationPaused(ctx, &gestalt.SetWorkflowProviderActivationPausedRequest{
		DefinitionID: "definition-1",
		ActivationID: "hourly",
		Paused:       true,
	})
	if err != nil {
		t.Fatalf("SetActivationPaused: %v", err)
	}
	if paused.Generation != 2 || !paused.Activations[0].Paused {
		t.Fatalf("paused definition = %#v", paused)
	}
	if handle.desc.Schedule.State == nil || !handle.desc.Schedule.State.Paused {
		t.Fatalf("schedule state = %#v, want paused", handle.desc.Schedule.State)
	}
}

func TestBackendStartRunUsesDefinitionSnapshotInputProjection(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)
	tc := &recordingTemporalClient{}
	backend := newRecordingTemporalBackend(tc, state)
	definition, err := backend.ApplyDefinition(ctx, &gestalt.ApplyWorkflowProviderDefinitionRequest{
		Spec: &gestalt.WorkflowDefinitionSpec{
			ID:     "definition-1",
			Target: nativeAppTargetInput("slack", "postMessage"),
			RunAs:  &gestalt.Subject{ID: "service:slack-post"},
		},
	})
	if err != nil {
		t.Fatalf("ApplyDefinition: %v", err)
	}

	run, err := backend.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		DefinitionID:                 definition.ID,
		ExpectedDefinitionGeneration: definition.Generation,
		Input:                        map[string]any{"ticket": "T-1"},
		CreatedBySubjectID:           actor("user-1"),
	})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if run.DefinitionID != definition.ID || run.DefinitionGeneration != definition.Generation || run.Input["ticket"] != "T-1" {
		t.Fatalf("run = %#v", run)
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions = %#v, want one", tc.executions)
	}
	startInput := tc.executions[0].Args[0].(runWorkflowV4Input)
	if firstWorkflowAppStep(startInput.Target).Operation != "postMessage" || startInput.Input["ticket"] != "T-1" {
		t.Fatalf("start input = %#v", startInput)
	}
	if startInput.ProviderName != "temporal" || startInput.RunAs == nil || startInput.RunAs.ID != "service:slack-post" {
		t.Fatalf("start input authority = %#v", startInput)
	}

	projected, found, err := state.getRun(ctx, run.ID)
	if err != nil || !found {
		t.Fatalf("getRun projection found=%v err=%v", found, err)
	}
	if projected.Input["ticket"] != "T-1" || projected.DefinitionGeneration != definition.Generation {
		t.Fatalf("projected run = %#v", projected)
	}
	if _, err := backend.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		DefinitionID:                 definition.ID,
		ExpectedDefinitionGeneration: definition.Generation + 1,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StartRun generation mismatch error = %v, want FailedPrecondition", err)
	}
}

func TestBackendDeliverEventStartsMatchingActivation(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)
	tc := &recordingTemporalClient{}
	backend := newRecordingTemporalBackend(tc, state)
	if _, err := backend.ApplyDefinition(ctx, &gestalt.ApplyWorkflowProviderDefinitionRequest{
		Spec: &gestalt.WorkflowDefinitionSpec{
			ID:     "definition-1",
			Target: nativeAppTargetInput("slack", "postMessage"),
			RunAs:  &gestalt.Subject{ID: "service:slack-events"},
			Activations: []gestalt.WorkflowActivation{{
				ID:    "message-created",
				Input: gestalt.WorkflowValue{Object: map[string]gestalt.WorkflowValue{"channel": {Signal: "data.channel"}}},
				Event: &gestalt.WorkflowEventActivation{Match: &gestalt.WorkflowEventMatch{Type: "message.created", Source: "slack"}},
			}},
		},
	}); err != nil {
		t.Fatalf("ApplyDefinition: %v", err)
	}
	if _, err := backend.ApplyDefinition(ctx, &gestalt.ApplyWorkflowProviderDefinitionRequest{
		Spec: &gestalt.WorkflowDefinitionSpec{
			ID:     "definition-2",
			Target: nativeAppTargetInput("github", "createIssue"),
			RunAs:  &gestalt.Subject{ID: "service:github-events"},
			Activations: []gestalt.WorkflowActivation{{
				ID:    "issue-created",
				Event: &gestalt.WorkflowEventActivation{Match: &gestalt.WorkflowEventMatch{Type: "issue.created", Source: "github"}},
			}},
		},
	}); err != nil {
		t.Fatalf("ApplyDefinition(non-match): %v", err)
	}

	delivered, err := backend.DeliverEvent(ctx, &gestalt.DeliverWorkflowProviderEventRequest{
		AppName: "slack",
		Event:   &gestalt.WorkflowEvent{ID: "event-1", Type: "message.created", Data: map[string]any{"channel": "alerts"}},
	})
	if err != nil {
		t.Fatalf("DeliverEvent: %v", err)
	}
	if delivered.Source != "slack" || delivered.Type != "message.created" {
		t.Fatalf("delivered event = %#v", delivered)
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions = %#v, want one", tc.executions)
	}
	startInput := tc.executions[0].Args[0].(runWorkflowV4Input)
	if startInput.DefinitionID != "definition-1" || startInput.Trigger.Event.ActivationID != "message-created" || startInput.Input["channel"] != "alerts" {
		t.Fatalf("event start input = %#v", startInput)
	}
	if startInput.ProviderName != "temporal" || startInput.RunAs == nil || startInput.RunAs.ID != "service:slack-events" {
		t.Fatalf("event start input authority = %#v", startInput)
	}
}

func TestBackendGetRunQueriesRunningWorkflow(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)
	runID := encodeTemporalRunHandle(temporalRunHandle{
		RunWorkflowID:    "workflow-1",
		RunTemporalRunID: "run-1",
		OwnerKey:         "app:slack",
	})
	tc := &recordingTemporalClient{
		describeResp: describeWorkflowStatus(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING),
		queryRun: &gestalt.WorkflowRun{
			ID:     runID,
			Status: gestalt.WorkflowRunStatusValueRunning,
			Output: "live-state",
		},
	}
	backend := newRecordingTemporalBackend(tc, state)

	run, err := backend.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: runID})
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.Output != "live-state" || run.Status != gestalt.WorkflowRunStatusValueRunning {
		t.Fatalf("run = %#v", run)
	}
	if len(tc.queryCalls) != 1 || tc.queryCalls[0].WorkflowID != "workflow-1" || tc.queryCalls[0].RunID != "run-1" || tc.queryCalls[0].QueryType != queryGetRun {
		t.Fatalf("query calls = %#v", tc.queryCalls)
	}
	if len(tc.getWorkflowCalls) != 0 {
		t.Fatalf("get workflow calls = %#v, want none", tc.getWorkflowCalls)
	}
}

func TestBackendGetRunReadsCompletedWorkflowResult(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)
	runID := encodeTemporalRunHandle(temporalRunHandle{
		RunWorkflowID:    "workflow-1",
		RunTemporalRunID: "run-1",
		OwnerKey:         "app:slack",
	})
	tc := &recordingTemporalClient{
		describeResp: describeWorkflowStatus(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED),
		workflowResult: &gestalt.WorkflowRun{
			ID:     runID,
			Status: gestalt.WorkflowRunStatusValueSucceeded,
			Output: "completed-state",
		},
	}
	backend := newRecordingTemporalBackend(tc, state)

	run, err := backend.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: runID})
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.Output != "completed-state" || run.Status != gestalt.WorkflowRunStatusValueSucceeded {
		t.Fatalf("run = %#v", run)
	}
	if len(tc.getWorkflowCalls) != 1 || tc.getWorkflowCalls[0].WorkflowID != "workflow-1" || tc.getWorkflowCalls[0].RunID != "run-1" {
		t.Fatalf("get workflow calls = %#v", tc.getWorkflowCalls)
	}
	if len(tc.queryCalls) != 0 {
		t.Fatalf("query calls = %#v, want none", tc.queryCalls)
	}
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

func actor(subjectID string) string {
	return strings.TrimSpace(subjectID)
}

type capturingHost struct {
	resp  *gestaltworkflow.Response
	err   error
	calls []gestaltworkflow.StepRequest
}

func (h *capturingHost) Execute(_ context.Context, req gestaltworkflow.Request) (*gestaltworkflow.Response, error) {
	return h.resp, h.err
}

func (h *capturingHost) ExecuteStep(_ context.Context, req gestaltworkflow.StepRequest) (*gestaltworkflow.StepResponse, error) {
	h.calls = append(h.calls, req)
	if h.err != nil {
		return nil, h.err
	}
	if h.resp == nil {
		return stepResponseFromTemporalStubBody(req, http.StatusOK, ""), nil
	}
	return stepResponseFromTemporalStubBody(req, h.resp.Status, h.resp.Body), nil
}

func stepResponseFromTemporalStubBody(req gestaltworkflow.StepRequest, statusCode int, body string) *gestaltworkflow.StepResponse {
	stepID := ""
	if req.Request.Target != nil && req.StepIndex >= 0 && req.StepIndex < len(req.Request.Target.Steps) {
		stepID = req.Request.Target.Steps[req.StepIndex].ID
	}
	statusText := "succeeded"
	if statusCode >= 400 {
		statusText = "failed"
	}
	step := gestaltworkflow.StepResult{ID: stepID, Status: statusText}
	var result gestaltworkflow.StepsResult
	if body != "" {
		_ = json.Unmarshal([]byte(body), &result)
	}
	for _, candidate := range result.Steps {
		if candidate.ID == stepID {
			step = candidate
			break
		}
	}
	output := result.Outputs[stepID]
	if output == nil && result.FinalStepID == stepID {
		output = result.FinalOutput
	}
	return &gestaltworkflow.StepResponse{
		Status:      statusCode,
		Step:        step,
		Output:      output,
		Outputs:     result.Outputs,
		FinalStepID: stepID,
		FinalOutput: output,
	}
}

func (h *capturingHost) Close() error { return nil }

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

type recordedExecution struct {
	WorkflowID string
	Args       []any
}

type recordedTemporalCall struct {
	WorkflowID string
	RunID      string
	QueryType  string
}

type recordingTemporalClient struct {
	client.Client
	mu               sync.Mutex
	executions       []recordedExecution
	describeResp     *workflowservicepb.DescribeWorkflowExecutionResponse
	describeErr      error
	queryRun         *gestalt.WorkflowRun
	queryErr         error
	queryCalls       []recordedTemporalCall
	workflowResult   *gestalt.WorkflowRun
	workflowErr      error
	getWorkflowCalls []recordedTemporalCall
	scheduleClient   client.ScheduleClient
}

func (c *recordingTemporalClient) ExecuteWorkflow(_ context.Context, options client.StartWorkflowOptions, _ interface{}, args ...interface{}) (client.WorkflowRun, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.executions = append(c.executions, recordedExecution{WorkflowID: options.ID, Args: args})
	return recordingWorkflowRun{id: options.ID, runID: fmt.Sprintf("run-%d", len(c.executions))}, nil
}

func (c *recordingTemporalClient) DescribeWorkflowExecution(_ context.Context, workflowID, runID string) (*workflowservicepb.DescribeWorkflowExecutionResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.describeErr != nil {
		return nil, c.describeErr
	}
	if c.describeResp != nil {
		return c.describeResp, nil
	}
	return describeWorkflowStatus(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING), nil
}

func (c *recordingTemporalClient) QueryWorkflow(_ context.Context, workflowID string, runID string, queryType string, _ ...interface{}) (converter.EncodedValue, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queryCalls = append(c.queryCalls, recordedTemporalCall{WorkflowID: workflowID, RunID: runID, QueryType: queryType})
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	return encodedWorkflowRun{run: c.queryRun}, nil
}

type encodedWorkflowRun struct {
	run *gestalt.WorkflowRun
}

func (v encodedWorkflowRun) HasValue() bool {
	return v.run != nil
}

func (v encodedWorkflowRun) Get(valuePtr interface{}) error {
	switch out := valuePtr.(type) {
	case *gestalt.WorkflowRun:
		*out = *cloneRunInput(v.run)
		return nil
	default:
		return fmt.Errorf("unsupported query result target %T", valuePtr)
	}
}

func describeWorkflowStatus(status enumspb.WorkflowExecutionStatus) *workflowservicepb.DescribeWorkflowExecutionResponse {
	return &workflowservicepb.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: status},
	}
}

func (c *recordingTemporalClient) GetWorkflow(_ context.Context, workflowID string, runID string) client.WorkflowRun {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.getWorkflowCalls = append(c.getWorkflowCalls, recordedTemporalCall{WorkflowID: workflowID, RunID: runID})
	return recordingWorkflowRun{id: workflowID, runID: runID, result: c.workflowResult, err: c.workflowErr}
}

func (c *recordingTemporalClient) ScheduleClient() client.ScheduleClient {
	if c.scheduleClient != nil {
		return c.scheduleClient
	}
	return c.Client.ScheduleClient()
}

func (c *recordingTemporalClient) Close() {}

type recordingWorkflowRun struct {
	id     string
	runID  string
	result *gestalt.WorkflowRun
	err    error
}

func (r recordingWorkflowRun) GetID() string { return r.id }

func (r recordingWorkflowRun) GetRunID() string { return r.runID }

func (r recordingWorkflowRun) Get(_ context.Context, valuePtr interface{}) error {
	if r.err != nil {
		return r.err
	}
	if r.result == nil {
		return nil
	}
	switch out := valuePtr.(type) {
	case *gestalt.WorkflowRun:
		*out = *cloneRunInput(r.result)
		return nil
	default:
		return fmt.Errorf("unsupported workflow result target %T", valuePtr)
	}
}

func (r recordingWorkflowRun) GetWithOptions(ctx context.Context, valuePtr interface{}, _ client.WorkflowRunGetOptions) error {
	return r.Get(ctx, valuePtr)
}

type fakeScheduleClient struct {
	client.ScheduleClient
	handles map[string]*fakeScheduleHandle
}

func newFakeScheduleClient(descriptions map[string]*client.ScheduleDescription) *fakeScheduleClient {
	c := &fakeScheduleClient{handles: map[string]*fakeScheduleHandle{}}
	for id, desc := range descriptions {
		c.handles[id] = &fakeScheduleHandle{id: id, desc: cloneScheduleDescription(desc)}
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
	return handle, nil
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
	client.ScheduleHandle
	id   string
	desc *client.ScheduleDescription
}

func (h *fakeScheduleHandle) GetID() string { return h.id }

func (h *fakeScheduleHandle) Delete(context.Context) error {
	h.desc = nil
	return nil
}

func (h *fakeScheduleHandle) Update(_ context.Context, options client.ScheduleUpdateOptions) error {
	current := cloneScheduleDescription(h.desc)
	if current == nil {
		return serviceerror.NewNotFound("schedule not found")
	}
	update, err := options.DoUpdate(client.ScheduleUpdateInput{Description: *current})
	if err != nil {
		return err
	}
	if update != nil && update.Schedule != nil {
		h.desc = &client.ScheduleDescription{Schedule: *update.Schedule}
	}
	return nil
}

func (h *fakeScheduleHandle) Describe(context.Context) (*client.ScheduleDescription, error) {
	if h.desc == nil {
		return nil, serviceerror.NewNotFound("schedule not found")
	}
	return cloneScheduleDescription(h.desc), nil
}

func cloneScheduleDescription(desc *client.ScheduleDescription) *client.ScheduleDescription {
	if desc == nil {
		return nil
	}
	clone := *desc
	return &clone
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

type projectionConflictDB struct {
	indexeddb.Database
	failNextProjectionPut atomic.Bool
}

func (db *projectionConflictDB) Transaction(ctx context.Context, stores []string, mode indexeddb.TransactionMode, opts indexeddb.TransactionOptions) (indexeddb.Transaction, error) {
	tx, err := db.Database.Transaction(ctx, stores, mode, opts)
	if err != nil {
		return nil, err
	}
	return &projectionConflictTx{Transaction: tx, db: db}, nil
}

type projectionConflictTx struct {
	indexeddb.Transaction
	db *projectionConflictDB
}

func (tx *projectionConflictTx) ObjectStore(name string) indexeddb.TransactionObjectStore {
	store := tx.Transaction.ObjectStore(name)
	if name != storeTemporalRunProjections {
		return store
	}
	return &projectionConflictStore{TransactionObjectStore: store, db: tx.db}
}

type projectionConflictStore struct {
	indexeddb.TransactionObjectStore
	db *projectionConflictDB
}

func (s *projectionConflictStore) Put(ctx context.Context, record indexeddb.Record) error {
	if s.db.failNextProjectionPut.Swap(false) {
		return status.Error(codes.AlreadyExists, "already exists")
	}
	return s.TransactionObjectStore.Put(ctx, record)
}
