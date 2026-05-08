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

func TestGestaltRunWorkflowV3AcksLaneSignalWithoutSelectorBlocking(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	host := &capturingHost{resp: &proto.InvokeWorkflowOperationResponse{Status: http.StatusOK, Body: "ok"}}
	env.RegisterWorkflow(gestaltRunWorkflowV3)
	env.RegisterActivity(&workflowActivities{host: host})
	env.OnSignalExternalWorkflow(mock.Anything, indexWorkflowID("scope", 0), "", signalIndexPutRun, mock.Anything).Return(nil)
	env.OnSignalExternalWorkflow(mock.Anything, "lane-workflow", "lane-run", signalLaneAck, mock.Anything).Return(nil)
	env.OnSignalExternalWorkflow(mock.Anything, "lane-workflow", "lane-run", signalLaneRunDone, mock.Anything).Return(nil)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalRunAddV3, runSignalMessage{
			AckID:         "ack-1",
			SignalPayload: protoPayload(&proto.WorkflowSignal{Name: "slack.event", IdempotencyKey: "evt-1", CreatedAt: timestamppb.Now()}),
		})
	}, time.Millisecond)

	env.ExecuteWorkflow(gestaltRunWorkflowV3, runWorkflowV3Input{
		ProviderName:                  "temporal",
		ScopeID:                       "scope",
		IndexShardCount:               1,
		ExecutionRef:                  "ref-1",
		WorkflowKey:                   "slack:T:C:1778164397.804829",
		LaneWorkflowID:                "lane-workflow",
		LaneTemporalRunID:             "lane-run",
		LogicalRunKey:                 "logical-1",
		OwnerKey:                      "slack",
		RequireSignal:                 true,
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
	if run.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED {
		t.Fatalf("run status = %s, want succeeded", run.GetStatus())
	}
	if len(host.calls) != 1 {
		t.Fatalf("host calls = %d, want 1", len(host.calls))
	}
	signals := host.calls[0].GetSignals()
	if len(signals) != 1 || signals[0].GetIdempotencyKey() != "evt-1" {
		t.Fatalf("host signals = %#v, want evt-1", signals)
	}
	if host.calls[0].GetMetadata() == nil || host.calls[0].GetMetadata().AsMap()["workflow_key"] != "slack:T:C:1778164397.804829" {
		t.Fatalf("host metadata = %#v, want workflow_key", host.calls[0].GetMetadata())
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
		RunAs: &proto.WorkflowRunAsSubject{
			SubjectId:   "service_account:slack-sync",
			SubjectKind: "service_account",
			DisplayName: "Slack sync",
			AuthSource:  "config",
		},
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
			if got.GetReferences()[0].GetRunAs().GetSubjectId() != "service_account:slack-sync" {
				t.Fatalf("ref run_as = %#v, want slack sync service account", got.GetReferences()[0].GetRunAs())
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

func TestScheduleFromTemporalDescriptionUsesLegacyV3ActionArgs(t *testing.T) {
	createdAt := time.Unix(100, 0).UTC()
	updatedAt := time.Unix(200, 0).UTC()
	nextAt := time.Unix(300, 0).UTC()
	input := runWorkflowV3Input{
		ScheduleID:       "schedule-1",
		ExecutionRef:     "ref-1",
		TargetPayload:    protoPayload(pluginTarget("slack", "postMessage")),
		CreatedByPayload: protoPayload(&proto.WorkflowActor{SubjectId: "system:config", SubjectKind: "system", AuthSource: "config"}),
	}
	payload, err := converter.GetDefaultDataConverter().ToPayload(input)
	if err != nil {
		t.Fatalf("encode legacy schedule args: %v", err)
	}

	got, found, err := scheduleFromTemporalDescription("schedule-1", &client.ScheduleDescription{
		Schedule: client.Schedule{
			Action: &client.ScheduleWorkflowAction{
				Args: []interface{}{payload},
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
	if got.GetId() != "schedule-1" || got.GetExecutionRef() != "ref-1" || got.GetTarget().GetPlugin().GetPluginName() != "slack" {
		t.Fatalf("decoded legacy schedule = %#v", got)
	}
	if got.GetCreatedBy().GetSubjectId() != "system:config" || !got.GetNextRunAt().AsTime().Equal(nextAt) {
		t.Fatalf("decoded legacy schedule metadata = %#v", got)
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

func TestTemporalBackendStartKeepsLegacyWorkerUnversionedWhenConfigOmitted(t *testing.T) {
	order := []string{}
	fw := &fakeTemporalWorker{order: &order}
	tc := &recordingTemporalClient{deploymentClient: &fakeWorkerDeploymentClient{handle: &fakeWorkerDeploymentHandle{}}}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		IndexShardCount:             1,
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
			t.Fatalf("legacy config set deployment options: %#v", options.DeploymentOptions)
		}
		return fw
	}
	backend.startupCleanup = func(context.Context) error {
		order = append(order, "cleanup")
		return nil
	}

	if err := backend.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := strings.Join(order, ","); got != "start,cleanup" {
		t.Fatalf("startup order = %s, want start,cleanup", got)
	}
	if len(tc.deploymentClient.handle.describeCalls) != 0 || len(tc.deploymentClient.handle.setCurrentCalls) != 0 {
		t.Fatalf("legacy config touched worker deployments: %#v", tc.deploymentClient.handle)
	}
	if !backend.started {
		t.Fatalf("backend not marked started")
	}
}

func TestTemporalBackendStartPromotesCurrentBeforeStartupCleanup(t *testing.T) {
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
	backend.startupCleanup = func(context.Context) error {
		order = append(order, "cleanup")
		return nil
	}

	if err := backend.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := strings.Join(order, ","); got != "start,describe,set-current,cleanup" {
		t.Fatalf("startup order = %s, want start,describe,set-current,cleanup", got)
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
	backend := newTestTemporalBackendForStart(cfg, handle, &fakeTemporalWorker{}, nil)

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
	backend := newTestTemporalBackendForStart(cfg, handle, fw, func(context.Context) error {
		t.Fatalf("startup cleanup should not run after promotion failure")
		return nil
	})

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
	backend := newTestTemporalBackendForStart(cfg, handle, &fakeTemporalWorker{}, nil)

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
	backend := newTestTemporalBackendForStart(cfg, handle, &fakeTemporalWorker{}, nil)

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
	backend := newTestTemporalBackendForStart(cfg, handle, &fakeTemporalWorker{}, nil)

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
	backend := newTestTemporalBackendForStart(cfg, handle, fw, nil)

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
		t.Fatalf("putExecutionRefIndex touched temporal index updates=%#v", tc.updates)
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
		IndexShardCount:             4,
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
		t.Fatalf("ListSchedules touched temporal index queries=%#v updates=%#v", tc.queries, tc.updates)
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
		IndexShardCount:             4,
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
		IndexShardCount:             4,
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
		IndexShardCount:             4,
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
		t.Fatalf("legacy ledger updates = %#v, want none", tc.updates)
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
		IndexShardCount:             4,
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
		IndexShardCount:             4,
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

func TestStartRunReturnsCompletedLegacyIdempotentRun(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	target := pluginTarget("slack", "postMessage")
	createdBy := &proto.WorkflowActor{SubjectId: "user-1"}
	key := "start-1"
	ledgerKey := startLedgerKey("slack", key)
	legacyRun := &proto.BoundWorkflowRun{Id: "legacy-run", Status: proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED, Target: target}
	tc := &recordingTemporalClient{ledgerEntries: map[string]*ownerLedgerEntry{
		ledgerKey: {
			Key:         ledgerKey,
			Status:      "completed",
			Fingerprint: startFingerprint("slack", key, "", "", target, createdBy),
			RunPayload:  protoPayload(legacyRun),
		},
	}}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		IndexShardCount:             4,
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
		IdempotencyRetention:        time.Hour,
	}, tc, nil, state)

	run, err := backend.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		IdempotencyKey: key,
		Target:         target,
		CreatedBy:      createdBy,
	})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if run.GetId() != legacyRun.GetId() {
		t.Fatalf("run id = %q, want legacy %q", run.GetId(), legacyRun.GetId())
	}
	if len(tc.executions) != 0 {
		t.Fatalf("executions = %d, want none", len(tc.executions))
	}
}

func TestStartRunRejectsConflictingCompletedLegacyIdempotency(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	key := "start-1"
	ledgerKey := startLedgerKey("slack", key)
	tc := &recordingTemporalClient{ledgerEntries: map[string]*ownerLedgerEntry{
		ledgerKey: {
			Key:         ledgerKey,
			Status:      "completed",
			Fingerprint: "different-request",
			RunPayload:  protoPayload(&proto.BoundWorkflowRun{Id: "legacy-run"}),
		},
	}}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		IndexShardCount:             4,
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
		IdempotencyRetention:        time.Hour,
	}, tc, nil, state)

	_, err = backend.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		IdempotencyKey: key,
		Target:         pluginTarget("slack", "postMessage"),
		CreatedBy:      &proto.WorkflowActor{SubjectId: "user-1"},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StartRun error = %v, want FailedPrecondition", err)
	}
	if len(tc.executions) != 0 {
		t.Fatalf("executions = %d, want none", len(tc.executions))
	}
}

func TestStartRunIgnoresExpiredLegacyIdempotency(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	key := "start-1"
	ledgerKey := startLedgerKey("slack", key)
	tc := &recordingTemporalClient{ledgerEntries: map[string]*ownerLedgerEntry{
		ledgerKey: {
			Key:         ledgerKey,
			Status:      "completed",
			Fingerprint: "different-expired-request",
			ExpiresAt:   time.Now().Add(-time.Minute),
			RunPayload:  protoPayload(&proto.BoundWorkflowRun{Id: "legacy-run"}),
		},
	}}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		IndexShardCount:             4,
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
		IdempotencyRetention:        time.Hour,
	}, tc, nil, state)

	if _, err := backend.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		IdempotencyKey: key,
		Target:         pluginTarget("slack", "postMessage"),
		CreatedBy:      &proto.WorkflowActor{SubjectId: "user-1"},
	}); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions = %d, want 1", len(tc.executions))
	}
	expectedWorkflowID := workflowID("scope", "manual-v4", "slack", key)
	if tc.executions[0].WorkflowID != expectedWorkflowID {
		t.Fatalf("workflow id = %q, want %q", tc.executions[0].WorkflowID, expectedWorkflowID)
	}
}

func TestStartRunUsesLegacyWorkflowIDForReservedLegacyIdempotency(t *testing.T) {
	startTestIndexedDBBackend(t)
	ctx := context.Background()
	state, err := openWorkflowStateStore(ctx, "", "scope")
	if err != nil {
		t.Fatalf("openWorkflowStateStore: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	target := pluginTarget("slack", "postMessage")
	createdBy := &proto.WorkflowActor{SubjectId: "user-1"}
	key := "start-1"
	ledgerKey := startLedgerKey("slack", key)
	tc := &recordingTemporalClient{ledgerEntries: map[string]*ownerLedgerEntry{
		ledgerKey: {
			Key:         ledgerKey,
			Status:      "reserved",
			Fingerprint: startFingerprint("slack", key, "", "", target, createdBy),
		},
	}}
	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		IndexShardCount:             4,
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
		IdempotencyRetention:        time.Hour,
	}, tc, nil, state)

	if _, err := backend.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		IdempotencyKey: key,
		Target:         target,
		CreatedBy:      createdBy,
	}); err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if len(tc.executions) != 1 {
		t.Fatalf("executions = %d, want 1", len(tc.executions))
	}
	expectedWorkflowID := workflowID("scope", "manual-v3", "slack", key)
	if tc.executions[0].WorkflowID != expectedWorkflowID {
		t.Fatalf("workflow id = %q, want %q", tc.executions[0].WorkflowID, expectedWorkflowID)
	}
	if !tc.hasUpdate(backend.ownerLedgerWorkflowID(ledgerKey), updateLedgerComplete) {
		t.Fatalf("legacy ledger completion updates = %#v, want %s", tc.updates, updateLedgerComplete)
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
		Id:        "run-projected",
		Status:    proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED,
		Target:    pluginTarget("slack", "postMessage"),
		Trigger:   newManualTrigger(),
		CreatedAt: timestamppb.New(time.Unix(100, 0).UTC()),
	}
	if err := state.putRun(ctx, run); err != nil {
		t.Fatalf("putRun: %v", err)
	}

	backend := newTemporalBackend("temporal", config{
		ScopeID:                     "scope",
		TaskQueue:                   "gestalt-workflow",
		IndexShardCount:             2,
		WorkflowRunTimeout:          time.Minute,
		WorkflowTaskTimeout:         time.Second,
		ActivityStartToCloseTimeout: time.Minute,
		ScheduleCatchupWindow:       time.Minute,
	}, &recordingTemporalClient{}, nil, state)
	resp, err := backend.ListRuns(ctx, &proto.ListWorkflowProviderRunsRequest{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(resp.GetRuns()) != 1 || resp.GetRuns()[0].GetId() != run.GetId() {
		t.Fatalf("runs = %#v, want projected run", resp.GetRuns())
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
		IndexShardCount:             4,
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
		"indexShardCount":             1,
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
		IndexShardCount:             1,
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

func newTestTemporalBackendForStart(cfg config, handle *fakeWorkerDeploymentHandle, fw *fakeTemporalWorker, cleanup func(context.Context) error) *temporalBackend {
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
	if cleanup == nil {
		cleanup = func(context.Context) error { return nil }
	}
	backend.startupCleanup = cleanup
	return backend
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
	queries            []recordedQuery
	queryStarted       chan<- recordedQuery
	releaseQueries     <-chan struct{}
	scheduleClient     client.ScheduleClient
	deploymentClient   *fakeWorkerDeploymentClient
	ledgerEntries      map[string]*ownerLedgerEntry
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

func (c *recordingTemporalClient) NewWithStartWorkflowOperation(options client.StartWorkflowOptions, _ interface{}, _ ...interface{}) client.WithStartWorkflowOperation {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pendingWorkflowIDs = append(c.pendingWorkflowIDs, options.ID)
	return nil
}

func (c *recordingTemporalClient) UpdateWorkflow(_ context.Context, options client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	update := recordedUpdate{
		WorkflowID: options.WorkflowID,
		Name:       options.UpdateName,
		Args:       options.Args,
	}
	c.updates = append(c.updates, update)
	return recordingUpdateHandle{update: update}, nil
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
	if queryType == queryLedgerGet && len(args) > 0 {
		key, _ := args[0].(string)
		c.mu.Lock()
		entry := cloneLedgerEntry(c.ledgerEntries[strings.TrimSpace(key)])
		c.mu.Unlock()
		if entry != nil {
			return recordingEncodedValue{query: query, value: entry}, nil
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
	value any
}

func (v recordingEncodedValue) HasValue() bool { return true }

func (v recordingEncodedValue) Get(valuePtr interface{}) error {
	switch out := valuePtr.(type) {
	case *ownerLedgerEntry:
		if entry, ok := v.value.(*ownerLedgerEntry); ok {
			*out = *cloneLedgerEntry(entry)
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
