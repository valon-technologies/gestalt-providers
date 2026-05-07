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

func TestGestaltRunWorkflowV3InvokesHostWhenProjectionUnavailable(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	host := &capturingHost{resp: &proto.InvokeWorkflowOperationResponse{Status: http.StatusOK, Body: "ok"}}
	env.RegisterWorkflow(gestaltRunWorkflowV3)
	env.RegisterActivity(&workflowActivities{host: host})
	env.OnSignalExternalWorkflow(mock.Anything, indexWorkflowID("scope", 0), "", signalIndexPutRun, mock.Anything).Return(nil)

	env.ExecuteWorkflow(gestaltRunWorkflowV3, runWorkflowV3Input{
		ProviderName:                  "temporal",
		ScopeID:                       "scope",
		IndexShardCount:               1,
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
	decoded, err := decodeV3RunHandle(run.GetId())
	if err != nil || decoded.RunWorkflowID == "" {
		t.Fatalf("decode v3 handle = %#v err=%v", decoded, err)
	}
}

func TestGestaltWorkflowKeyLaneV1DedupesDuplicateSignalOrStart(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	host := &capturingHost{resp: &proto.InvokeWorkflowOperationResponse{Status: http.StatusOK, Body: "ok"}}
	env.RegisterWorkflow(gestaltWorkflowKeyLaneV1)
	env.RegisterWorkflow(gestaltRunWorkflowV3)
	env.RegisterActivity(&workflowActivities{host: host})
	env.OnSignalExternalWorkflow(mock.Anything, indexWorkflowID("scope", 0), "", signalIndexPutRun, mock.Anything).Return(nil)

	var first *proto.SignalWorkflowRunResponse
	var duplicate *proto.SignalWorkflowRunResponse
	request := laneSignalRequest{
		OwnerKey:         "slack",
		TargetPayload:    protoPayload(pluginTarget("slack", "postMessage")),
		ExecutionRef:     "ref-1",
		CreatedByPayload: protoPayload(&proto.WorkflowActor{SubjectId: "user-1"}),
		SignalPayload:    protoPayload(&proto.WorkflowSignal{Name: "slack.event", IdempotencyKey: "evt-1", CreatedAt: timestamppb.Now()}),
		RequestID:        "signal-key:" + hashID("slack.event", "evt-1"),
		IdempotencyKey:   "evt-1",
	}
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(updateLaneSignalOrStart, "evt-1", updateCallback(t, func(value interface{}) {
			first = cloneSignalResponse(value.(*proto.SignalWorkflowRunResponse))
		}), request)
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(updateLaneSignalOrStart, "evt-1-duplicate", updateCallback(t, func(value interface{}) {
			duplicate = cloneSignalResponse(value.(*proto.SignalWorkflowRunResponse))
		}), request)
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.CancelWorkflow()
	}, 20*time.Millisecond)

	env.ExecuteWorkflow(gestaltWorkflowKeyLaneV1, laneWorkflowSnapshot{Input: laneWorkflowInput{
		ProviderName:                  "temporal",
		ScopeID:                       "scope",
		IndexShardCount:               1,
		TaskQueue:                     "test",
		WorkflowRunTimeoutNS:          time.Minute,
		WorkflowTaskTimeoutNS:         time.Second,
		ActivityStartToCloseTimeoutNS: time.Minute,
		IdempotencyRetentionNS:        time.Hour,
		WorkflowKey:                   "slack:T:C",
	}})

	if first == nil || duplicate == nil {
		t.Fatalf("responses first=%#v duplicate=%#v", first, duplicate)
	}
	if first.GetRun().GetId() == "" || duplicate.GetRun().GetId() != first.GetRun().GetId() {
		t.Fatalf("duplicate run = %q, want %q", duplicate.GetRun().GetId(), first.GetRun().GetId())
	}
	if first.GetSignal().GetId() == "" || duplicate.GetSignal().GetId() != first.GetSignal().GetId() {
		t.Fatalf("duplicate signal = %#v, want %#v", duplicate.GetSignal(), first.GetSignal())
	}
	if !first.GetStartedRun() || !duplicate.GetStartedRun() {
		t.Fatalf("started flags first=%v duplicate=%v, want replayed first response", first.GetStartedRun(), duplicate.GetStartedRun())
	}
	if len(host.calls) != 1 {
		t.Fatalf("host calls = %d, want 1", len(host.calls))
	}
}

func TestGestaltWorkflowKeyLaneV1AcksSignalWhileActivityRunning(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	release := make(chan struct{})
	host := &blockingHost{resp: &proto.InvokeWorkflowOperationResponse{Status: http.StatusOK, Body: "ok"}, releaseFirst: release}
	env.RegisterWorkflow(gestaltWorkflowKeyLaneV1)
	env.RegisterWorkflow(gestaltRunWorkflowV3)
	env.RegisterActivity(&workflowActivities{host: host})
	env.OnSignalExternalWorkflow(mock.Anything, indexWorkflowID("scope", 0), "", signalIndexPutRun, mock.Anything).Return(nil)

	var first *proto.SignalWorkflowRunResponse
	var second *proto.SignalWorkflowRunResponse
	firstReq := laneSignalRequest{
		OwnerKey:         "slack",
		TargetPayload:    protoPayload(pluginTarget("slack", "postMessage")),
		ExecutionRef:     "ref-1",
		CreatedByPayload: protoPayload(&proto.WorkflowActor{SubjectId: "user-1"}),
		SignalPayload:    protoPayload(&proto.WorkflowSignal{Name: "slack.event", IdempotencyKey: "evt-1", CreatedAt: timestamppb.Now()}),
		RequestID:        "evt-1",
		IdempotencyKey:   "evt-1",
	}
	secondReq := firstReq
	secondReq.SignalPayload = protoPayload(&proto.WorkflowSignal{Name: "slack.event", IdempotencyKey: "evt-2", CreatedAt: timestamppb.Now()})
	secondReq.RequestID = "evt-2"
	secondReq.IdempotencyKey = "evt-2"
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(updateLaneSignalOrStart, "evt-1", updateCallback(t, func(value interface{}) {
			first = cloneSignalResponse(value.(*proto.SignalWorkflowRunResponse))
		}), firstReq)
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(updateLaneSignalOrStart, "evt-2", updateCallback(t, func(value interface{}) {
			second = cloneSignalResponse(value.(*proto.SignalWorkflowRunResponse))
		}), secondReq)
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		if second == nil {
			t.Fatalf("second signal was not acknowledged while first activity was running")
		}
		close(release)
	}, 10*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.CancelWorkflow()
	}, 50*time.Millisecond)

	env.ExecuteWorkflow(gestaltWorkflowKeyLaneV1, laneWorkflowSnapshot{Input: laneWorkflowInput{
		ProviderName:                  "temporal",
		ScopeID:                       "scope",
		IndexShardCount:               1,
		TaskQueue:                     "test",
		WorkflowRunTimeoutNS:          time.Minute,
		WorkflowTaskTimeoutNS:         time.Second,
		ActivityStartToCloseTimeoutNS: time.Minute,
		IdempotencyRetentionNS:        time.Hour,
		WorkflowKey:                   "slack:T:C",
	}})

	if first == nil || second == nil {
		t.Fatalf("responses first=%#v second=%#v", first, second)
	}
	if second.GetRun().GetId() != first.GetRun().GetId() {
		t.Fatalf("second run = %q, want first run %q", second.GetRun().GetId(), first.GetRun().GetId())
	}
	if first.GetSignal().GetSequence() != 1 || second.GetSignal().GetSequence() != 2 {
		t.Fatalf("sequences first=%d second=%d, want 1 and 2", first.GetSignal().GetSequence(), second.GetSignal().GetSequence())
	}
	host.mu.Lock()
	calls := append([]*proto.InvokeWorkflowOperationRequest(nil), host.calls...)
	host.mu.Unlock()
	if len(calls) != 2 {
		t.Fatalf("host calls = %d, want 2 batches", len(calls))
	}
	if calls[0].GetRunId() != calls[1].GetRunId() || calls[0].GetRunId() != first.GetRun().GetId() {
		t.Fatalf("batch run ids = %q/%q, want %q", calls[0].GetRunId(), calls[1].GetRunId(), first.GetRun().GetId())
	}
	if len(calls[0].GetSignals()) != 1 || calls[0].GetSignals()[0].GetIdempotencyKey() != "evt-1" || len(calls[1].GetSignals()) != 1 || calls[1].GetSignals()[0].GetIdempotencyKey() != "evt-2" {
		t.Fatalf("batches signals = %#v / %#v", calls[0].GetSignals(), calls[1].GetSignals())
	}
}

func TestGestaltOwnerLedgerWorkflowRetainsCompletedResponse(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(gestaltOwnerLedgerWorkflow)

	var replay *ownerLedgerEntry
	resp := &proto.SignalWorkflowRunResponse{
		Run:         &proto.BoundWorkflowRun{Id: "run-1", Status: proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING},
		Signal:      &proto.WorkflowSignal{Id: "sig-1", Name: "message"},
		StartedRun:  true,
		WorkflowKey: "thread-1",
	}
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(updateLedgerReserve, "reserve-1", updateCallback(t, nil), ownerLedgerReserveRequest{
			Key:         "owner-idem:1",
			Operation:   "signal_or_start",
			Fingerprint: "fp-1",
			OwnerKey:    "slack",
			WorkflowKey: "thread-1",
		})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(updateLedgerComplete, "complete-1", updateCallback(t, nil), ownerLedgerCompleteRequest{
			Key:             "owner-idem:1",
			Fingerprint:     "fp-1",
			ResponsePayload: protoPayload(resp),
			RunPayload:      protoPayload(resp.GetRun()),
		})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(updateLedgerReserve, "reserve-2", updateCallback(t, func(value interface{}) {
			out := value.(*ownerLedgerReserveResponse)
			replay = out.Entry
		}), ownerLedgerReserveRequest{
			Key:         "owner-idem:1",
			Operation:   "signal_or_start",
			Fingerprint: "fp-1",
			OwnerKey:    "slack",
			WorkflowKey: "thread-1",
		})
	}, 3*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.CancelWorkflow()
	}, 10*time.Millisecond)

	env.ExecuteWorkflow(gestaltOwnerLedgerWorkflow, ownerLedgerInput{ScopeID: "scope", Shard: 0, IdempotencyRetentionNS: time.Hour})

	if replay == nil || replay.Status != "completed" {
		t.Fatalf("replay entry = %#v, want completed", replay)
	}
	gotResp := signalResponseFromPayload(replay.ResponsePayload)
	gotRun := runFromPayload(replay.RunPayload)
	if gotResp.GetSignal().GetId() != "sig-1" || gotRun.GetId() != "run-1" {
		t.Fatalf("replay payloads resp=%#v run=%#v", gotResp, gotRun)
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
	ref := &proto.WorkflowExecutionReference{
		Id:           "ref-1",
		ProviderName: "temporal",
		Target:       pluginTarget("slack", "postMessage"),
		SubjectId:    "user-1",
		CreatedAt:    createdAt,
	}

	var checkedRun, checkedTrigger, checkedRef bool
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
		env.UpdateWorkflow(updatePutTrigger, "put-trigger", updateCallback(t, nil), trigger)
		env.UpdateWorkflow(updateMatchTriggers, "match-trigger", updateCallback(t, func(value interface{}) {
			got := value.(*proto.ListWorkflowProviderEventTriggersResponse)
			if len(got.GetTriggers()) != 1 || got.GetTriggers()[0].GetId() != trigger.GetId() {
				t.Fatalf("matched triggers = %#v", got.GetTriggers())
			}
			checkedTrigger = true
		}), eventMatchKey("slack", "message.created", "", ""))
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
	if !checkedRun || !checkedTrigger || !checkedRef {
		t.Fatalf("index checks: run=%v trigger=%v ref=%v", checkedRun, checkedTrigger, checkedRef)
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

	var checkedRun bool
	env.RegisterDelayedCallback(func() {
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
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.CancelWorkflow()
	}, 2*time.Millisecond)

	env.ExecuteWorkflow(indexWorkflow, indexInput{ScopeID: "scope", Shard: 0})

	if !checkedRun {
		t.Fatalf("index checks: run=%v", checkedRun)
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
		t.Fatalf("UpsertSchedule touched temporal index updates=%#v", tc.updates)
	}
	if len(tc.queries) != 0 {
		t.Fatalf("UpsertSchedule touched temporal index queries=%#v", tc.queries)
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
	scheduleClient     client.ScheduleClient
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

type fakeScheduleClient struct {
	handles map[string]*fakeScheduleHandle
	order   []string
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
