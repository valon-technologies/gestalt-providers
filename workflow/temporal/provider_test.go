package temporal

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	relationaldb "github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb"
	workflowfake "github.com/valon-technologies/gestalt-providers/workflow/indexeddb/fake"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	gestaltworkflow "github.com/valon-technologies/gestalt/sdk/go/workflow"
	commonpb "go.temporal.io/api/common/v1"
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

func TestTemporalRunReturnsRunState(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := newTestWorkflowEnvironment(&suite)
	host := &capturingHost{resp: &gestaltworkflow.Response{
		Status: http.StatusOK,
		Body:   `{"version":1,"status":"succeeded","steps":[{"id":"postMessage","status":"succeeded"}],"outputs":{"postMessage":"ok"},"finalStepId":"postMessage","finalOutput":"ok"}`,
	}}
	env.RegisterWorkflow(TemporalRun)
	env.RegisterActivity(&workflowActivities{executor: host})

	env.ExecuteWorkflow(TemporalRun, runWorkflowInput{
		ActivityStartToCloseTimeoutNS: time.Minute,
		ScopeID:                       "scope",
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
		t.Fatalf("run fields = %#v", &run)
	}
	if len(host.calls) != 1 ||
		host.calls[0].Request.Input["ticket"] != "T-1" ||
		host.calls[0].Request.DefinitionID != "definition-1" ||
		host.calls[0].Request.DefinitionGeneration != 7 {
		t.Fatalf("host calls = %#v", host.calls)
	}
}

func TestTemporalRunRunsOneDurableStepAtATime(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := newTestWorkflowEnvironment(&suite)
	host := &capturingHost{resp: &gestaltworkflow.Response{
		Status: http.StatusOK,
		Body:   `{"version":1,"status":"succeeded","steps":[{"id":"collect","status":"succeeded"},{"id":"notify","status":"succeeded"}],"outputs":{"collect":{"ok":true},"notify":{"sent":true}},"finalStepId":"notify","finalOutput":{"sent":true}}`,
	}}
	env.RegisterWorkflow(TemporalRun)
	env.RegisterActivity(&workflowActivities{executor: host})

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
	env.ExecuteWorkflow(TemporalRun, runWorkflowInput{
		ActivityStartToCloseTimeoutNS: time.Minute,
		ScopeID:                       "scope",
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
	actionInput := handle.desc.Schedule.Action.(*client.ScheduleWorkflowAction).Args[0].(runWorkflowInput)
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

func TestBackendStartRunUsesDefinitionSnapshotInputAndVisibility(t *testing.T) {
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
	startInput := tc.executions[0].Args[0].(runWorkflowInput)
	if firstWorkflowAppStep(startInput.Target).Operation != "postMessage" || startInput.Input["ticket"] != "T-1" {
		t.Fatalf("start input = %#v", startInput)
	}
	if startInput.ProviderName != "temporal" || startInput.RunAs == nil || startInput.RunAs.ID != "service:slack-post" {
		t.Fatalf("start input authority = %#v", startInput)
	}
	if startInput.ScopeID != "scope" {
		t.Fatalf("start input scope_id = %q, want scope", startInput.ScopeID)
	}
	attrs := tc.executions[0].Options.TypedSearchAttributes
	if got, ok := attrs.GetKeyword(searchAttrScopeID); !ok || got != "scope" {
		t.Fatalf("scope search attribute = %q ok=%v", got, ok)
	}
	if got, ok := attrs.GetKeyword(searchAttrRunStatus); !ok || got != "pending" {
		t.Fatalf("status search attribute = %q ok=%v", got, ok)
	}
	if got, ok := attrs.GetKeyword(searchAttrProviderName); !ok || got != "temporal" {
		t.Fatalf("provider search attribute = %q ok=%v", got, ok)
	}
	if got, ok := attrs.GetKeyword(searchAttrDefinitionID); !ok || got != definition.ID {
		t.Fatalf("definition search attribute = %q ok=%v", got, ok)
	}
	if got, ok := attrs.GetKeywordList(searchAttrTargetApps); !ok || len(got) != 1 || got[0] != "slack" {
		t.Fatalf("target app search attribute = %#v ok=%v", got, ok)
	}
	if _, err := backend.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		DefinitionID:                 definition.ID,
		ExpectedDefinitionGeneration: definition.Generation + 1,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StartRun generation mismatch error = %v, want FailedPrecondition", err)
	}
}

func TestBackendStartRunWorkflowKeyRejectsActiveRun(t *testing.T) {
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

	if _, err := backend.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		DefinitionID:                 definition.ID,
		ExpectedDefinitionGeneration: definition.Generation,
		WorkflowKey:                  "thread:C123:170000",
	}); err != nil {
		t.Fatalf("StartRun(first): %v", err)
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions = %#v, want one", tc.executions)
	}
	expectedWorkflowID := workflowKeyRunWorkflowID("scope", "thread:C123:170000")
	if tc.executions[0].WorkflowID != expectedWorkflowID {
		t.Fatalf("workflow ID = %q, want %q", tc.executions[0].WorkflowID, expectedWorkflowID)
	}
	if !tc.executions[0].Options.WorkflowExecutionErrorWhenAlreadyStarted {
		t.Fatal("WorkflowExecutionErrorWhenAlreadyStarted = false, want true")
	}

	_, err = backend.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		DefinitionID:                 definition.ID,
		ExpectedDefinitionGeneration: definition.Generation,
		WorkflowKey:                  "thread:C123:170000",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StartRun(duplicate) error = %v, want FailedPrecondition", err)
	}
}

func TestBackendSignalOrStartUsesWorkflowKeyTemporalID(t *testing.T) {
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

	resp, err := backend.SignalOrStartRun(ctx, &gestalt.SignalOrStartWorkflowProviderRunRequest{
		DefinitionID:                 definition.ID,
		ExpectedDefinitionGeneration: definition.Generation,
		WorkflowKey:                  " thread:C123:170000 ",
		Signal:                       &gestalt.WorkflowSignal{ID: "signal-1", Name: "message", Payload: map[string]any{"text": "hello"}},
		CreatedBySubjectID:           actor("user-1"),
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun: %v", err)
	}
	expectedWorkflowID := workflowKeyRunWorkflowID("scope", "thread:C123:170000")
	if resp.Run == nil || resp.Run.ID == "" || resp.WorkflowKey != "thread:C123:170000" {
		t.Fatalf("response = %#v", resp)
	}
	handle, err := decodeTemporalRunHandle(resp.Run.ID)
	if err != nil {
		t.Fatalf("decode run handle: %v", err)
	}
	if handle.RunWorkflowID != expectedWorkflowID || handle.WorkflowKey != "thread:C123:170000" {
		t.Fatalf("run handle = %#v", handle)
	}
	if len(tc.updateWithStartCalls) != 1 {
		t.Fatalf("update-with-start calls = %#v, want one", tc.updateWithStartCalls)
	}
	call := tc.updateWithStartCalls[0]
	if call.StartOptions.ID != expectedWorkflowID || call.UpdateOptions.WorkflowID != expectedWorkflowID {
		t.Fatalf("workflow IDs = start %q update %q, want %q", call.StartOptions.ID, call.UpdateOptions.WorkflowID, expectedWorkflowID)
	}
	if call.StartOptions.WorkflowIDConflictPolicy != enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING {
		t.Fatalf("conflict policy = %s, want USE_EXISTING", call.StartOptions.WorkflowIDConflictPolicy)
	}
	startInput := call.Args[0].(runWorkflowInput)
	if startInput.WorkflowKey != "thread:C123:170000" || !startInput.RequireSignal || startInput.ScopeID != "scope" {
		t.Fatalf("signal-or-start input = %#v", startInput)
	}
	if got, ok := call.StartOptions.TypedSearchAttributes.GetKeyword(searchAttrScopeID); !ok || got != "scope" {
		t.Fatalf("scope search attribute = %q ok=%v", got, ok)
	}
	if got, ok := call.StartOptions.TypedSearchAttributes.GetKeywordList(searchAttrTargetApps); !ok || len(got) != 1 || got[0] != "slack" {
		t.Fatalf("target app search attribute = %#v ok=%v", got, ok)
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
	startInput := tc.executions[0].Args[0].(runWorkflowInput)
	if startInput.DefinitionID != "definition-1" || startInput.Trigger.Event.ActivationID != "message-created" || startInput.Input["channel"] != "alerts" {
		t.Fatalf("event start input = %#v", startInput)
	}
	if startInput.ProviderName != "temporal" || startInput.RunAs == nil || startInput.RunAs.ID != "service:slack-events" {
		t.Fatalf("event start input authority = %#v", startInput)
	}

	if _, err := backend.DeliverEvent(ctx, &gestalt.DeliverWorkflowProviderEventRequest{
		AppName: "slack",
		Event:   &gestalt.WorkflowEvent{ID: "event-1", Type: "message.created", Data: map[string]any{"channel": "alerts"}},
	}); err != nil {
		t.Fatalf("DeliverEvent(duplicate): %v", err)
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions after duplicate event = %#v, want one", tc.executions)
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

func TestBackendListRunsUsesTemporalVisibilityAndHydratesRuns(t *testing.T) {
	ctx, state := newTestWorkflowStateStore(t)
	runID := encodeTemporalRunHandle(temporalRunHandle{
		RunWorkflowID:    "workflow-1",
		RunTemporalRunID: "run-1",
		OwnerKey:         "slack",
	})
	nextToken := []byte("next-page")
	tc := &recordingTemporalClient{
		listResp: &workflowservicepb.ListWorkflowExecutionsResponse{
			Executions: []*workflowpb.WorkflowExecutionInfo{{
				Execution: &commonpb.WorkflowExecution{WorkflowId: "workflow-1", RunId: "run-1"},
				Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			}},
			NextPageToken: nextToken,
		},
		queryRun: &gestalt.WorkflowRun{
			ID:                   runID,
			Status:               gestalt.WorkflowRunStatusValueRunning,
			Target:               nativeAppTargetInput("slack", "postMessage"),
			CreatedBySubjectID:   actor("user-1"),
			ProviderName:         "temporal",
			DefinitionID:         "definition-1",
			DefinitionGeneration: 7,
		},
	}
	backend := newRecordingTemporalBackend(tc, state)

	resp, err := backend.ListRuns(ctx, &gestalt.ListWorkflowProviderRunsRequest{
		PageSize:  25,
		Status:    gestalt.WorkflowRunStatusValueRunning,
		TargetApp: "slack",
	})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(resp.GetRuns()) != 1 || resp.GetRuns()[0].ID != runID || resp.GetRuns()[0].DefinitionID != "definition-1" {
		t.Fatalf("runs = %#v", resp.GetRuns())
	}
	if resp.NextPageToken != encodeTemporalListPageToken(nextToken) {
		t.Fatalf("next page token = %q", resp.NextPageToken)
	}
	if len(tc.listWorkflowRequests) != 1 {
		t.Fatalf("list workflow requests = %#v", tc.listWorkflowRequests)
	}
	listReq := tc.listWorkflowRequests[0]
	if listReq.GetPageSize() != 25 {
		t.Fatalf("page size = %d, want 25", listReq.GetPageSize())
	}
	for _, want := range []string{
		"WorkflowType = 'TemporalRun'",
		"GestaltScopeId = 'scope'",
		"GestaltProviderName = 'temporal'",
		"GestaltRunStatus = 'running'",
		"GestaltTargetApps = 'slack'",
	} {
		if !strings.Contains(listReq.GetQuery(), want) {
			t.Fatalf("query = %q, missing %q", listReq.GetQuery(), want)
		}
	}
	if len(tc.queryCalls) != 1 || tc.queryCalls[0].WorkflowID != "workflow-1" || tc.queryCalls[0].RunID != "run-1" {
		t.Fatalf("query calls = %#v", tc.queryCalls)
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
	Options    client.StartWorkflowOptions
	Workflow   interface{}
	Args       []any
}

type recordedTemporalCall struct {
	WorkflowID string
	RunID      string
	QueryType  string
}

type recordingTemporalClient struct {
	client.Client
	mu                   sync.Mutex
	executions           []recordedExecution
	updateWithStartCalls []recordedUpdateWithStart
	listWorkflowRequests []*workflowservicepb.ListWorkflowExecutionsRequest
	listResp             *workflowservicepb.ListWorkflowExecutionsResponse
	listErr              error
	describeResp         *workflowservicepb.DescribeWorkflowExecutionResponse
	describeErr          error
	queryRun             *gestalt.WorkflowRun
	queryErr             error
	queryCalls           []recordedTemporalCall
	workflowResult       *gestalt.WorkflowRun
	workflowErr          error
	getWorkflowCalls     []recordedTemporalCall
	scheduleClient       client.ScheduleClient
}

type recordedUpdateWithStart struct {
	StartOptions  client.StartWorkflowOptions
	Workflow      interface{}
	Args          []any
	UpdateOptions client.UpdateWorkflowOptions
}

func (c *recordingTemporalClient) ExecuteWorkflow(_ context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, execution := range c.executions {
		if execution.WorkflowID == options.ID && options.WorkflowIDConflictPolicy == enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL {
			if options.WorkflowExecutionErrorWhenAlreadyStarted {
				return nil, serviceerror.NewWorkflowExecutionAlreadyStarted("workflow execution already started", "", "existing-run")
			}
			return recordingWorkflowRun{id: execution.WorkflowID, runID: "existing-run"}, nil
		}
	}
	c.executions = append(c.executions, recordedExecution{WorkflowID: options.ID, Options: options, Workflow: workflow, Args: args})
	return recordingWorkflowRun{id: options.ID, runID: fmt.Sprintf("run-%d", len(c.executions))}, nil
}

func (c *recordingTemporalClient) NewWithStartWorkflowOperation(options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) client.WithStartWorkflowOperation {
	return &recordingWithStartWorkflowOperation{
		execution: recordedExecution{WorkflowID: options.ID, Options: options, Workflow: workflow, Args: args},
		run:       recordingWorkflowRun{id: options.ID, runID: "run-update-with-start"},
	}
}

func (c *recordingTemporalClient) UpdateWithStartWorkflow(_ context.Context, options client.UpdateWithStartWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	op, ok := options.StartWorkflowOperation.(*recordingWithStartWorkflowOperation)
	if !ok {
		return nil, fmt.Errorf("unsupported start operation %T", options.StartWorkflowOperation)
	}
	c.updateWithStartCalls = append(c.updateWithStartCalls, recordedUpdateWithStart{
		StartOptions:  op.execution.Options,
		Workflow:      op.execution.Workflow,
		Args:          op.execution.Args,
		UpdateOptions: options.UpdateOptions,
	})
	return recordingWorkflowUpdateHandle{
		workflowID: options.UpdateOptions.WorkflowID,
		runID:      op.run.GetRunID(),
		updateID:   options.UpdateOptions.UpdateID,
	}, nil
}

func (c *recordingTemporalClient) ListWorkflow(_ context.Context, req *workflowservicepb.ListWorkflowExecutionsRequest) (*workflowservicepb.ListWorkflowExecutionsResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.listWorkflowRequests = append(c.listWorkflowRequests, req)
	if c.listErr != nil {
		return nil, c.listErr
	}
	if c.listResp != nil {
		return c.listResp, nil
	}
	return &workflowservicepb.ListWorkflowExecutionsResponse{}, nil
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

type recordingWithStartWorkflowOperation struct {
	execution recordedExecution
	run       recordingWorkflowRun
	err       error
}

func (o *recordingWithStartWorkflowOperation) Get(context.Context) (client.WorkflowRun, error) {
	if o.err != nil {
		return nil, o.err
	}
	return o.run, nil
}

type recordingWorkflowUpdateHandle struct {
	workflowID string
	runID      string
	updateID   string
	resp       *gestalt.SignalWorkflowRunResponse
	err        error
}

func (h recordingWorkflowUpdateHandle) WorkflowID() string { return h.workflowID }

func (h recordingWorkflowUpdateHandle) RunID() string { return h.runID }

func (h recordingWorkflowUpdateHandle) UpdateID() string { return h.updateID }

func (h recordingWorkflowUpdateHandle) Get(_ context.Context, valuePtr interface{}) error {
	if h.err != nil {
		return h.err
	}
	if h.resp == nil || valuePtr == nil {
		return nil
	}
	switch out := valuePtr.(type) {
	case *gestalt.SignalWorkflowRunResponse:
		*out = *cloneSignalResponseInput(h.resp)
		return nil
	default:
		return fmt.Errorf("unsupported workflow update target %T", valuePtr)
	}
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
