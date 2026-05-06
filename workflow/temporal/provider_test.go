package temporal

import (
	"context"
	"errors"
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
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	sdkworkflow "go.temporal.io/sdk/workflow"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestGestaltRunWorkflowInvokesHostActivityAndReturnsSucceededRun(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	host := &capturingHost{resp: &proto.InvokeWorkflowOperationResponse{Status: http.StatusOK, Body: "ok"}}
	env.RegisterWorkflow(gestaltRunWorkflow)
	env.RegisterActivity(&workflowActivities{host: host})
	env.OnSignalExternalWorkflow(mock.Anything, indexWorkflowID("scope", 0), "", signalIndexPutRun, mock.Anything).Return(nil)

	env.ExecuteWorkflow(gestaltRunWorkflow, runWorkflowOptions{
		ProviderName:                  "temporal",
		ScopeID:                       "scope",
		IndexShardCount:               1,
		ExecutionRef:                  "ref-1",
		ActivityStartToCloseTimeoutNS: time.Minute,
	}, pluginTarget("slack", "postMessage"), newManualTrigger(), &proto.WorkflowActor{SubjectId: "user-1"})

	if !env.IsWorkflowCompleted() {
		t.Fatalf("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var run proto.BoundWorkflowRun
	if err := env.GetWorkflowResult(&run); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	if run.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED {
		t.Fatalf("status = %s, want succeeded", run.GetStatus())
	}
	if run.GetResultBody() != "ok" {
		t.Fatalf("result body = %q", run.GetResultBody())
	}
	if len(host.calls) != 1 {
		t.Fatalf("host calls = %d, want 1", len(host.calls))
	}
	if got := host.calls[0].GetRunId(); got != run.GetId() {
		t.Fatalf("host run id = %q, want %q", got, run.GetId())
	}
}

func TestGestaltRunWorkflowV2PersistsFinalRunStateInIndexedDB(t *testing.T) {
	startTestIndexedDBBackend(t)
	state, err := openWorkflowStateStore(context.Background(), "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	host := &capturingHost{resp: &proto.InvokeWorkflowOperationResponse{Status: http.StatusOK, Body: "ok"}}
	env.RegisterWorkflow(gestaltRunWorkflowV2)
	env.RegisterActivity(&workflowActivities{host: host, state: state})

	env.ExecuteWorkflow(gestaltRunWorkflowV2, runWorkflowOptions{
		ProviderName:                  "temporal",
		ScopeID:                       "scope",
		IndexShardCount:               1,
		ExecutionRef:                  "ref-1",
		ActivityStartToCloseTimeoutNS: time.Minute,
	}, pluginTarget("slack", "postMessage"), newManualTrigger(), &proto.WorkflowActor{SubjectId: "user-1"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var run proto.BoundWorkflowRun
	if err := env.GetWorkflowResult(&run); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	stored, found, err := state.getRun(context.Background(), run.GetId())
	if err != nil {
		t.Fatalf("state getRun: %v", err)
	}
	if !found {
		t.Fatalf("stored run %q not found", run.GetId())
	}
	if stored.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED || stored.GetResultBody() != "ok" {
		t.Fatalf("stored run = %#v, want succeeded with body", stored)
	}
}

func TestGestaltRunWorkflowSignalOrStartBatchesSignals(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	host := &capturingHost{resp: &proto.InvokeWorkflowOperationResponse{Status: http.StatusOK, Body: "delivered"}}
	env.RegisterWorkflow(gestaltRunWorkflow)
	env.RegisterActivity(&workflowActivities{host: host})
	env.OnSignalExternalWorkflow(mock.Anything, indexWorkflowID("scope", 0), "", signalIndexPutRun, mock.Anything).Return(nil)
	env.RegisterDelayedCallback(func() {
		uc := &testsuite.TestUpdateCallback{
			OnReject: func(err error) { t.Fatalf("update rejected: %v", err) },
			OnAccept: func() {},
			OnComplete: func(value interface{}, err error) {
				if err != nil {
					t.Fatalf("update completed with error: %v", err)
				}
				resp, ok := value.(*proto.SignalWorkflowRunResponse)
				if !ok || resp.GetSignal().GetSequence() != 1 || !resp.GetStartedRun() {
					t.Fatalf("unexpected signal response: %#v", value)
				}
			},
		}
		env.UpdateWorkflow(updateAddSignal, "sig-1", uc, &proto.WorkflowSignal{Name: "message", IdempotencyKey: "sig-key"})
	}, time.Millisecond)

	env.ExecuteWorkflow(gestaltRunWorkflow, runWorkflowOptions{
		ProviderName:                  "temporal",
		ScopeID:                       "scope",
		IndexShardCount:               1,
		WorkflowKey:                   "thread-1",
		RequireSignal:                 true,
		ActivityStartToCloseTimeoutNS: time.Minute,
	}, pluginTarget("slack", "postMessage"), newManualTrigger(), nil)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var run proto.BoundWorkflowRun
	if err := env.GetWorkflowResult(&run); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	if run.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED {
		t.Fatalf("status = %s, want succeeded", run.GetStatus())
	}
	if len(host.calls) != 1 {
		t.Fatalf("host calls = %d, want 1", len(host.calls))
	}
	if signals := host.calls[0].GetSignals(); len(signals) != 1 || signals[0].GetSequence() != 1 {
		t.Fatalf("signals = %#v", signals)
	}
}

func TestGestaltRunWorkflowCancelIsPendingOnly(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	host := &capturingHost{resp: &proto.InvokeWorkflowOperationResponse{Status: http.StatusOK}}
	env.RegisterWorkflow(gestaltRunWorkflow)
	env.RegisterActivity(&workflowActivities{host: host})
	env.OnSignalExternalWorkflow(mock.Anything, indexWorkflowID("scope", 0), "", signalIndexPutRun, mock.Anything).Return(nil)
	env.RegisterDelayedCallback(func() {
		uc := &testsuite.TestUpdateCallback{
			OnReject: func(err error) { t.Fatalf("cancel rejected: %v", err) },
			OnAccept: func() {},
			OnComplete: func(value interface{}, err error) {
				if err != nil {
					t.Fatalf("cancel completed with error: %v", err)
				}
				run, ok := value.(*proto.BoundWorkflowRun)
				if !ok || run.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_CANCELED {
					t.Fatalf("unexpected cancel response: %#v", value)
				}
			},
		}
		env.UpdateWorkflow(updateCancelRun, "cancel-1", uc, "stop")
	}, time.Millisecond)

	env.ExecuteWorkflow(gestaltRunWorkflow, runWorkflowOptions{
		ProviderName:                  "temporal",
		ScopeID:                       "scope",
		IndexShardCount:               1,
		RequireSignal:                 true,
		ActivityStartToCloseTimeoutNS: time.Minute,
	}, pluginTarget("slack", "postMessage"), newManualTrigger(), nil)

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var run proto.BoundWorkflowRun
	if err := env.GetWorkflowResult(&run); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	if run.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_CANCELED {
		t.Fatalf("status = %s, want canceled", run.GetStatus())
	}
	if len(host.calls) != 0 {
		t.Fatalf("host calls = %d, want 0", len(host.calls))
	}
}

func TestIndexWorkflowStoresProviderIndexes(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(indexWorkflow)

	createdAt := timestamppb.New(time.Unix(100, 0).UTC())
	run := &proto.BoundWorkflowRun{
		Id:          "run-1",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:      pluginTarget("slack", "postMessage"),
		Trigger:     newManualTrigger(),
		CreatedAt:   createdAt,
		WorkflowKey: "thread-1",
	}
	trigger := &proto.BoundWorkflowEventTrigger{
		Id:        "trigger-1",
		Match:     &proto.WorkflowEventMatch{Type: "message.created"},
		Target:    pluginTarget("slack", "postMessage"),
		CreatedAt: createdAt,
		UpdatedAt: createdAt,
	}
	signalResp := &proto.SignalWorkflowRunResponse{
		Run:         run,
		Signal:      &proto.WorkflowSignal{Name: "message", Sequence: 7},
		StartedRun:  true,
		WorkflowKey: "thread-1",
	}
	ref := &proto.WorkflowExecutionReference{
		Id:           "ref-1",
		ProviderName: "temporal",
		Target:       pluginTarget("slack", "postMessage"),
		SubjectId:    "user-1",
		CreatedAt:    createdAt,
	}

	var checkedRun, checkedTrigger, checkedIdempotency, checkedRef bool
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(updatePutRun, "put-run", updateCallback(t, func(value interface{}) {
			got := value.(*proto.BoundWorkflowRun)
			if got.GetId() != run.GetId() {
				t.Fatalf("put run id = %q, want %q", got.GetId(), run.GetId())
			}
		}), run)
		env.UpdateWorkflow(updateListRuns, "list-runs", updateCallback(t, func(value interface{}) {
			got := value.(*proto.ListWorkflowProviderRunsResponse)
			if len(got.GetRuns()) != 1 || got.GetRuns()[0].GetId() != run.GetId() {
				t.Fatalf("runs = %#v", got.GetRuns())
			}
			checkedRun = true
		}))
		env.UpdateWorkflow(updatePutWorkflowKey, "put-workflow-key", updateCallback(t, nil), "thread-1", run)
		env.UpdateWorkflow(updateGetWorkflowKey, "get-workflow-key", updateCallback(t, func(value interface{}) {
			got := value.(*proto.BoundWorkflowRun)
			if got.GetId() != run.GetId() {
				t.Fatalf("workflow key run = %q, want %q", got.GetId(), run.GetId())
			}
		}), "thread-1")
		env.UpdateWorkflow(updatePutTrigger, "put-trigger", updateCallback(t, nil), trigger)
		env.UpdateWorkflow(updateMatchTriggers, "match-trigger", updateCallback(t, func(value interface{}) {
			got := value.(*proto.ListWorkflowProviderEventTriggersResponse)
			if len(got.GetTriggers()) != 1 || got.GetTriggers()[0].GetId() != trigger.GetId() {
				t.Fatalf("matched triggers = %#v", got.GetTriggers())
			}
			checkedTrigger = true
		}), eventMatchKey("slack", "message.created", "", ""))
		env.UpdateWorkflow(updatePutIdempotency, "put-idempotency", updateCallback(t, nil), "slack", "event-1", signalResp)
		env.UpdateWorkflow(updateGetIdempotency, "get-idempotency", updateCallback(t, func(value interface{}) {
			got := value.(*proto.SignalWorkflowRunResponse)
			if got.GetSignal().GetSequence() != 7 || got.GetWorkflowKey() != "thread-1" {
				t.Fatalf("idempotency response = %#v", got)
			}
			checkedIdempotency = true
		}), "slack", "event-1")
		env.UpdateWorkflow(updatePutRef, "put-ref", updateCallback(t, nil), ref)
		env.UpdateWorkflow(updateListRefsBySubject, "list-refs-by-subject", updateCallback(t, func(value interface{}) {
			got := value.(*proto.ListWorkflowExecutionReferencesResponse)
			if len(got.GetReferences()) != 1 || got.GetReferences()[0].GetId() != ref.GetId() {
				t.Fatalf("refs = %#v", got.GetReferences())
			}
			checkedRef = true
		}), "user-1")
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.CancelWorkflow()
	}, 10*time.Millisecond)

	env.ExecuteWorkflow(indexWorkflow, indexInput{ScopeID: "scope", Shard: 0})

	if !env.IsWorkflowCompleted() {
		t.Fatalf("index workflow did not complete after cancellation")
	}
	if !checkedRun || !checkedTrigger || !checkedIdempotency || !checkedRef {
		t.Fatalf("index checks: run=%v trigger=%v idempotency=%v ref=%v", checkedRun, checkedTrigger, checkedIdempotency, checkedRef)
	}
}

func TestIndexWorkflowPreventsTerminalRunRegression(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(indexWorkflow)

	createdAt := timestamppb.New(time.Unix(100, 0).UTC())
	pending := &proto.BoundWorkflowRun{
		Id:          "run-1",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:      pluginTarget("slack", "postMessage"),
		Trigger:     newManualTrigger(),
		CreatedAt:   createdAt,
		WorkflowKey: "thread-1",
	}
	succeeded := cloneRun(pending)
	succeeded.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED
	succeeded.CompletedAt = timestamppb.New(time.Unix(120, 0).UTC())
	stale := cloneRun(pending)
	stale.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING
	stale.StartedAt = timestamppb.New(time.Unix(110, 0).UTC())

	var checkedRun, checkedWorkflowKey bool
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(updatePutWorkflowKey, "put-active-key", updateCallback(t, nil), "thread-1", pending)
		env.UpdateWorkflow(updatePutRun, "put-terminal", updateCallback(t, nil), succeeded)
		env.UpdateWorkflow(updatePutRun, "put-stale", updateCallback(t, func(value interface{}) {
			got := value.(*proto.BoundWorkflowRun)
			if got.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED {
				t.Fatalf("stale put returned status %s, want succeeded", got.GetStatus())
			}
		}), stale)
		env.UpdateWorkflow(updateGetRun, "get-run", updateCallback(t, func(value interface{}) {
			got := value.(*proto.BoundWorkflowRun)
			if got.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED {
				t.Fatalf("indexed run status = %s, want succeeded", got.GetStatus())
			}
			checkedRun = true
		}), pending.GetId())
		env.UpdateWorkflow(updateGetWorkflowKey, "get-workflow-key", updateCallback(t, func(value interface{}) {
			got := value.(*proto.BoundWorkflowRun)
			if got.GetId() != "" {
				t.Fatalf("workflow key still points at run %#v", got)
			}
			checkedWorkflowKey = true
		}), "thread-1")
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.CancelWorkflow()
	}, 2*time.Millisecond)

	env.ExecuteWorkflow(indexWorkflow, indexInput{ScopeID: "scope", Shard: 0})

	if !checkedRun || !checkedWorkflowKey {
		t.Fatalf("index checks: run=%v workflowKey=%v", checkedRun, checkedWorkflowKey)
	}
}

func TestIndexWorkflowQueriesProviderIndexes(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(indexWorkflow)

	schedule := &proto.BoundWorkflowSchedule{
		Id:        "schedule-1",
		Target:    pluginTarget("slack", "postMessage"),
		CreatedAt: timestamppb.New(time.Unix(100, 0).UTC()),
		UpdatedAt: timestamppb.New(time.Unix(100, 0).UTC()),
	}
	var checked bool
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(updatePutSchedule, "put-schedule", updateCallback(t, nil), schedule)
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		value, err := env.QueryWorkflow(updateGetSchedule, "schedule-1")
		if err != nil {
			t.Fatalf("query schedule: %v", err)
		}
		var got proto.BoundWorkflowSchedule
		if err := value.Get(&got); err != nil {
			t.Fatalf("query value: %v", err)
		}
		if got.GetId() != schedule.GetId() {
			t.Fatalf("queried schedule id = %q, want %q", got.GetId(), schedule.GetId())
		}
		checked = true
		env.CancelWorkflow()
	}, 2*time.Millisecond)

	env.ExecuteWorkflow(indexWorkflow, indexInput{ScopeID: "scope", Shard: 0})

	if !checked {
		t.Fatalf("schedule query was not checked")
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

func TestScheduleFromTemporalDescriptionDecodesLegacyActionArgs(t *testing.T) {
	nextAt := time.Unix(400, 0).UTC()
	payloads, err := converter.GetDefaultDataConverter().ToPayloads(
		runWorkflowOptions{ScheduleID: "schedule-legacy", ExecutionRef: "ref-legacy"},
		pluginTarget("github", "PullRequests.List"),
		scheduleTrigger("schedule-legacy", nextAt),
		&proto.WorkflowActor{SubjectId: "system:config", SubjectKind: "system", AuthSource: "config"},
	)
	if err != nil {
		t.Fatalf("encode action args: %v", err)
	}
	args := make([]interface{}, 0, len(payloads.GetPayloads()))
	for _, payload := range payloads.GetPayloads() {
		args = append(args, payload)
	}

	got, found, err := scheduleFromTemporalDescription("", &client.ScheduleDescription{
		Schedule: client.Schedule{
			Action: &client.ScheduleWorkflowAction{Args: args},
			Spec:   &client.ScheduleSpec{CronExpressions: []string{"15 2 * * *"}, TimeZoneName: "UTC"},
			State:  &client.ScheduleState{},
		},
		Info: client.ScheduleInfo{NextActionTimes: []time.Time{nextAt}},
	})
	if err != nil {
		t.Fatalf("scheduleFromTemporalDescription: %v", err)
	}
	if !found {
		t.Fatalf("schedule not found")
	}
	if got.GetId() != "schedule-legacy" || got.GetExecutionRef() != "ref-legacy" {
		t.Fatalf("decoded schedule metadata = %#v", got)
	}
	if got.GetTarget().GetPlugin().GetPluginName() != "github" || got.GetTarget().GetPlugin().GetOperation() != "PullRequests.List" {
		t.Fatalf("decoded schedule target = %#v", got.GetTarget())
	}
	if got.GetCron() != "15 2 * * *" || got.GetTimezone() != "UTC" || !got.GetNextRunAt().AsTime().Equal(nextAt) {
		t.Fatalf("decoded schedule timing = %#v", got)
	}
}

func TestIndexWorkflowCompactsViaSignal(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(indexWorkflow)

	schedule := &proto.BoundWorkflowSchedule{
		Id:        "schedule-1",
		Target:    pluginTarget("slack", "postMessage"),
		CreatedAt: timestamppb.New(time.Unix(100, 0).UTC()),
		UpdatedAt: timestamppb.New(time.Unix(100, 0).UTC()),
	}
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(updatePutSchedule, "put-schedule", updateCallback(t, nil), schedule)
		env.SignalWorkflow(signalIndexCompact, "test")
	}, time.Millisecond)

	env.ExecuteWorkflow(indexWorkflow, indexInput{ScopeID: "scope", Shard: 0})

	if !env.IsWorkflowCompleted() {
		t.Fatalf("index workflow did not complete")
	}
	err := env.GetWorkflowError()
	if err == nil {
		t.Fatalf("workflow error is nil, want continue-as-new")
	}
	var continueAsNew *sdkworkflow.ContinueAsNewError
	if !errors.As(err, &continueAsNew) {
		t.Fatalf("workflow error = %v, want continue-as-new", err)
	}
}

func TestIndexInputStateDataConverterRoundTrip(t *testing.T) {
	state := newIndexState()
	state.Schedules["schedule-1"] = &proto.BoundWorkflowSchedule{
		Id:        "schedule-1",
		Target:    pluginTarget("slack", "postMessage"),
		CreatedAt: timestamppb.New(time.Unix(100, 0).UTC()),
		UpdatedAt: timestamppb.New(time.Unix(100, 0).UTC()),
	}
	state.Runs["run-1"] = &proto.BoundWorkflowRun{
		Id:        "run-1",
		Status:    proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:    pluginTarget("slack", "postMessage"),
		Trigger:   newManualTrigger(),
		CreatedAt: timestamppb.New(time.Unix(100, 0).UTC()),
	}

	snapshot, err := indexSnapshotFromState(state)
	if err != nil {
		t.Fatalf("snapshot index state: %v", err)
	}
	payloads, err := converter.GetDefaultDataConverter().ToPayloads(indexInput{ScopeID: "scope", Shard: 0, Snapshot: snapshot})
	if err != nil {
		t.Fatalf("encode index input: %v", err)
	}
	var got indexInput
	if err := converter.GetDefaultDataConverter().FromPayloads(payloads, &got); err != nil {
		t.Fatalf("decode index input: %v", err)
	}
	gotState, err := indexStateFromInput(got)
	if err != nil {
		t.Fatalf("decode index state: %v", err)
	}
	if got.ScopeID != "scope" || got.Shard != 0 {
		t.Fatalf("decoded input = %#v", got)
	}
	if gotState.Schedules["schedule-1"].GetId() != "schedule-1" {
		t.Fatalf("decoded schedules = %#v", gotState.Schedules)
	}
	if gotState.Runs["run-1"].GetId() != "run-1" {
		t.Fatalf("decoded runs = %#v", gotState.Runs)
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
		IndexShardCount:             8,
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
		t.Fatalf("putTriggerIndex touched temporal index updates=%#v", tc.updates)
	}
	matched, err := backend.matchTriggersIndex(context.Background(), "slack", &proto.WorkflowEvent{Type: "message.created"})
	if err != nil {
		t.Fatalf("matchTriggersIndex: %v", err)
	}
	if len(matched) != 1 || matched[0].GetId() != trigger.GetId() {
		t.Fatalf("matched triggers = %#v, want %q", matched, trigger.GetId())
	}
	if len(tc.queries) != 0 {
		t.Fatalf("matchTriggersIndex touched temporal index queries=%#v", tc.queries)
	}

	ref := &proto.WorkflowExecutionReference{
		Id:           "ref-1",
		ProviderName: "temporal",
		Target:       pluginTarget("slack", "postMessage"),
		SubjectId:    "user-1",
		CreatedAt:    timestamppb.Now(),
	}
	if err := backend.putExecutionRefIndex(context.Background(), ref); err != nil {
		t.Fatalf("putExecutionRefIndex: %v", err)
	}
	if len(tc.updates) != 0 {
		t.Fatalf("putExecutionRefIndex touched temporal index updates=%#v", tc.updates)
	}
	refs, err := backend.listExecutionRefsIndex(context.Background(), "user-1")
	if err != nil {
		t.Fatalf("listExecutionRefsIndex: %v", err)
	}
	if len(refs) != 1 || refs[0].GetId() != ref.GetId() {
		t.Fatalf("refs = %#v, want %q", refs, ref.GetId())
	}
	if len(tc.queries) != 0 {
		t.Fatalf("listExecutionRefsIndex touched temporal index queries=%#v", tc.queries)
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
		IndexShardCount:             4,
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

	refA := &proto.WorkflowExecutionReference{Id: "ref-1", ProviderName: "temporal", Target: pluginTarget("slack", "postMessage"), SubjectId: "user-1", CreatedAt: timestamppb.Now()}
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

func pluginTarget(plugin, operation string) *proto.BoundWorkflowTarget {
	input, _ := structpb.NewStruct(map[string]any{"text": "hello"})
	return &proto.BoundWorkflowTarget{Kind: &proto.BoundWorkflowTarget_Plugin{Plugin: &proto.BoundWorkflowPluginTarget{
		PluginName: strings.TrimSpace(plugin),
		Operation:  strings.TrimSpace(operation),
		Input:      input,
	}}}
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

type recordedUpdate struct {
	WorkflowID string
	Name       string
	Args       []any
}

type recordedQuery struct {
	WorkflowID string
	Name       string
	Args       []any
}

type recordingTemporalClient struct {
	client.Client
	mu                 sync.Mutex
	pendingWorkflowIDs []string
	updates            []recordedUpdate
	queries            []recordedQuery
	queryStarted       chan<- recordedQuery
	releaseQueries     <-chan struct{}
}

func (c *recordingTemporalClient) NewWithStartWorkflowOperation(options client.StartWorkflowOptions, _ interface{}, _ ...interface{}) client.WithStartWorkflowOperation {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pendingWorkflowIDs = append(c.pendingWorkflowIDs, options.ID)
	return nil
}

func (c *recordingTemporalClient) UpdateWithStartWorkflow(_ context.Context, options client.UpdateWithStartWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	workflowID := ""
	if len(c.pendingWorkflowIDs) > 0 {
		workflowID = c.pendingWorkflowIDs[0]
		c.pendingWorkflowIDs = c.pendingWorkflowIDs[1:]
	}
	update := recordedUpdate{
		WorkflowID: workflowID,
		Name:       options.UpdateOptions.UpdateName,
		Args:       options.UpdateOptions.Args,
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
	if c.queryStarted != nil {
		c.queryStarted <- query
	}
	if c.releaseQueries != nil {
		select {
		case <-c.releaseQueries:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return recordingEncodedValue{query: query}, nil
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

func (c *recordingTemporalClient) hasQuery(workflowID, name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, query := range c.queries {
		if query.WorkflowID == workflowID && query.Name == name {
			return true
		}
	}
	return false
}

func (c *recordingTemporalClient) queryCount(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, query := range c.queries {
		if query.Name == name {
			count++
		}
	}
	return count
}

type recordingUpdateHandle struct {
	update recordedUpdate
}

func (h recordingUpdateHandle) WorkflowID() string { return h.update.WorkflowID }
func (h recordingUpdateHandle) RunID() string      { return "" }
func (h recordingUpdateHandle) UpdateID() string   { return "" }

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
			}
		}
	}
	return nil
}

type recordingEncodedValue struct {
	query recordedQuery
}

func (v recordingEncodedValue) HasValue() bool { return true }

func (v recordingEncodedValue) Get(valuePtr interface{}) error {
	return nil
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
