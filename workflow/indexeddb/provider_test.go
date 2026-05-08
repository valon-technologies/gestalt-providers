package indexeddb

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	relationaldb "github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestProviderStartRunUsesIdempotencyAndExecutesHostCallbacks(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "10ms"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	startProviderWorker(t, provider)
	t.Cleanup(func() { _ = provider.Close() })

	first, err := provider.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		IdempotencyKey: "manual-sync",
		Target:         protoBoundTarget(t, "roadmap", "sync", map[string]any{"mode": "full"}),
		CreatedBy:      &proto.WorkflowActor{SubjectId: "user:123", SubjectKind: "user", DisplayName: "Ada"},
	})
	if err != nil {
		t.Fatalf("StartRun(first): %v", err)
	}
	second, err := provider.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		IdempotencyKey: "manual-sync",
		Target:         protoBoundTarget(t, "roadmap", "sync", map[string]any{"mode": "full"}),
	})
	if err != nil {
		t.Fatalf("StartRun(second): %v", err)
	}
	if first.GetId() != second.GetId() {
		t.Fatalf("idempotent run ids = %q and %q, want equal", first.GetId(), second.GetId())
	}

	call, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall: %v", err)
	}
	plugin := call.GetTarget().GetPlugin()
	if plugin.GetPluginName() != "roadmap" || plugin.GetOperation() != "sync" {
		t.Fatalf("target = %#v", call.GetTarget())
	}
	if got := plugin.GetInput().AsMap()["mode"]; got != "full" {
		t.Fatalf("target.input.mode = %v, want full", got)
	}
	if call.GetCreatedBy().GetSubjectId() != "user:123" {
		t.Fatalf("created_by.subject_id = %q, want user:123", call.GetCreatedBy().GetSubjectId())
	}

	waitForCondition(t, time.Second, func() bool {
		run, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{
			RunId: first.GetId(),
		})
		return err == nil && run.GetStatus() == proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED
	})

	runs, err := provider.ListRuns(ctx, &proto.ListWorkflowProviderRunsRequest{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs.GetRuns()) != 1 {
		t.Fatalf("runs len = %d, want 1", len(runs.GetRuns()))
	}
	if len(host.calls()) != 1 {
		t.Fatalf("host calls = %d, want 1", len(host.calls()))
	}
}

func TestProviderStartControlsPollLoopLifecycle(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "10ms"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	pending, err := provider.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		IdempotencyKey: "pending-before-start",
		Target:         protoBoundTarget(t, "roadmap", "sync", map[string]any{"mode": "pending"}),
	})
	if err != nil {
		t.Fatalf("StartRun(before Start): %v", err)
	}
	if _, err := host.waitForCall(100 * time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("host call before Start error = %v, want deadline exceeded", err)
	}

	startCtx, cancelStart := context.WithCancel(ctx)
	if err := provider.Start(startCtx); err != nil {
		t.Fatalf("Start(first): %v", err)
	}
	provider.mu.Lock()
	firstDone := provider.pollDone
	provider.mu.Unlock()
	if err := provider.Start(ctx); err != nil {
		t.Fatalf("Start(second): %v", err)
	}
	provider.mu.Lock()
	secondDone := provider.pollDone
	provider.mu.Unlock()
	if firstDone == nil || firstDone != secondDone {
		t.Fatalf("Start was not idempotent: first done=%p second done=%p", firstDone, secondDone)
	}
	cancelStart()

	firstCall, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall(first): %v", err)
	}
	if firstCall.GetRunId() != pending.GetId() {
		t.Fatalf("first call run_id = %q, want %q", firstCall.GetRunId(), pending.GetId())
	}

	second, err := provider.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		IdempotencyKey: "after-start-context-cancel",
		Target:         protoBoundTarget(t, "roadmap", "sync", map[string]any{"mode": "after-cancel"}),
	})
	if err != nil {
		t.Fatalf("StartRun(after Start context cancel): %v", err)
	}
	secondCall, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall(second): %v", err)
	}
	if secondCall.GetRunId() != second.GetId() {
		t.Fatalf("second call run_id = %q, want %q", secondCall.GetRunId(), second.GetId())
	}
	if len(host.calls()) != 2 {
		t.Fatalf("host calls = %d, want 2", len(host.calls()))
	}

	if err := provider.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if provider.pollCancel != nil || provider.pollDone != nil || provider.wake != nil {
		t.Fatalf("poll worker state after Close = cancel:%v done:%p wake:%p, want cleared", provider.pollCancel != nil, provider.pollDone, provider.wake)
	}
}

func TestProviderConfigureDoesNotStartPollLoopBeforeStart(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "10ms"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	provider.mu.Lock()
	done := provider.pollDone
	provider.mu.Unlock()
	if done != nil {
		t.Fatalf("poll worker started during Configure: done=%p", done)
	}

	run, err := provider.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		IdempotencyKey: "start-after-configure",
		Target:         protoBoundTarget(t, "roadmap", "sync", map[string]any{"mode": "compat"}),
	})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	if _, err := host.waitForCall(100 * time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("host call before Start error = %v, want deadline exceeded", err)
	}

	startProviderWorker(t, provider)

	call, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall: %v", err)
	}
	if call.GetRunId() != run.GetId() {
		t.Fatalf("run_id = %q, want %q", call.GetRunId(), run.GetId())
	}
}

func TestProviderStartRunRepairsMissingIdempotencyRecord(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "10ms"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	startProviderWorker(t, provider)
	t.Cleanup(func() { _ = provider.Close() })

	now := time.Now().UTC()
	runID := idempotentManualRunID("roadmap", "manual-sync")
	run := workflowRunRecord{
		ID:          runID,
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:      protoBoundTarget(t, "roadmap", "sync", map[string]any{"mode": "full"}),
		TriggerKind: triggerKindManual,
		CreatedAt:   now,
	}
	if err := provider.runStore.Put(ctx, run.toRecord()); err != nil {
		t.Fatalf("Put(run): %v", err)
	}

	first, err := provider.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		IdempotencyKey: "manual-sync",
		Target:         protoBoundTarget(t, "roadmap", "sync", map[string]any{"mode": "full"}),
	})
	if err != nil {
		t.Fatalf("StartRun(first): %v", err)
	}
	second, err := provider.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		IdempotencyKey: "manual-sync",
		Target:         protoBoundTarget(t, "roadmap", "sync", map[string]any{"mode": "full"}),
	})
	if err != nil {
		t.Fatalf("StartRun(second): %v", err)
	}
	if first.GetId() != runID || second.GetId() != runID {
		t.Fatalf("returned run ids = %q and %q, want %q", first.GetId(), second.GetId(), runID)
	}

	record, found, err := loadIdempotencyRecord(ctx, provider.idempotencyStore, "roadmap", "manual-sync")
	if err != nil {
		t.Fatalf("loadIdempotencyRecord: %v", err)
	}
	if !found || record.RunID != runID {
		t.Fatalf("idempotency record = %#v, found=%v, want run %q", record, found, runID)
	}
	rawIDRecord, err := provider.idempotencyStore.Get(ctx, idempotencyID("roadmap", "manual-sync"))
	if err != nil {
		t.Fatalf("raw idempotency get: %v", err)
	}
	assertRecordDoesNotContainFields(t, rawIDRecord, "plugin_name")

	call, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall: %v", err)
	}
	if call.GetRunId() != runID {
		t.Fatalf("run_id = %q, want %q", call.GetRunId(), runID)
	}
	if len(host.calls()) != 1 {
		t.Fatalf("host calls = %d, want 1", len(host.calls()))
	}
}

func TestProviderSignalOrStartRunReinvokesSameRunForQueuedSignals(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	host.releaseCh = make(chan struct{})
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "10ms"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	startProviderWorker(t, provider)
	t.Cleanup(func() { _ = provider.Close() })

	target := protoAgentTarget("managed", "gpt-5.5", "Respond in the Slack thread")
	changedTarget := protoAgentTarget("managed", "gpt-5.5-latest", "Updated prompt")
	workflowKey := "slack:T123:C123:1700000000.000001"
	first, err := provider.SignalOrStartRun(ctx, &proto.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  workflowKey,
		Target:       target,
		ExecutionRef: "agent-ref",
		Signal:       protoWorkflowSignal(t, "", "evt-1", "first"),
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun(first): %v", err)
	}
	if !first.GetStartedRun() {
		t.Fatalf("first started_run = false, want true")
	}
	if first.GetSignal().GetId() == "" {
		t.Fatalf("first signal id is empty")
	}
	if got := first.GetSignal().GetSequence(); got != 1 {
		t.Fatalf("first signal sequence = %d, want 1", got)
	}

	firstCall, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall(first): %v", err)
	}
	if firstCall.GetRunId() != first.GetRun().GetId() {
		t.Fatalf("first call run_id = %q, want %q", firstCall.GetRunId(), first.GetRun().GetId())
	}
	if firstCall.GetTarget().GetAgent().GetProviderName() != "managed" {
		t.Fatalf("first call target = %#v", firstCall.GetTarget())
	}
	if firstCall.GetExecutionRef() != "agent-ref" {
		t.Fatalf("first call execution_ref = %q, want agent-ref", firstCall.GetExecutionRef())
	}
	if len(firstCall.GetSignals()) != 1 || firstCall.GetSignals()[0].GetIdempotencyKey() != "evt-1" {
		t.Fatalf("first call signals = %#v", firstCall.GetSignals())
	}
	if firstCall.GetMetadata() == nil || firstCall.GetMetadata().AsMap()["workflow_key"] != workflowKey {
		t.Fatalf("first call metadata = %#v, want workflow_key %q", firstCall.GetMetadata(), workflowKey)
	}

	second, err := provider.SignalOrStartRun(ctx, &proto.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  workflowKey,
		Target:       changedTarget,
		ExecutionRef: "ignored-new-ref",
		Signal:       protoWorkflowSignal(t, "", "evt-2", "second"),
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun(second): %v", err)
	}
	if second.GetStartedRun() {
		t.Fatalf("second started_run = true, want false")
	}
	if second.GetRun().GetId() != first.GetRun().GetId() {
		t.Fatalf("second run_id = %q, want %q", second.GetRun().GetId(), first.GetRun().GetId())
	}
	if got := second.GetSignal().GetSequence(); got != 2 {
		t.Fatalf("second signal sequence = %d, want 2", got)
	}

	close(host.releaseCh)
	secondCall, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall(second): %v", err)
	}
	if secondCall.GetRunId() != first.GetRun().GetId() {
		t.Fatalf("second call run_id = %q, want %q", secondCall.GetRunId(), first.GetRun().GetId())
	}
	if secondCall.GetTarget().GetAgent().GetModel() != "gpt-5.5" {
		t.Fatalf("second call target model = %q, want original model", secondCall.GetTarget().GetAgent().GetModel())
	}
	if len(secondCall.GetSignals()) != 1 || secondCall.GetSignals()[0].GetIdempotencyKey() != "evt-2" {
		t.Fatalf("second call signals = %#v", secondCall.GetSignals())
	}
	if secondCall.GetExecutionRef() != "agent-ref" {
		t.Fatalf("second call execution_ref = %q, want original agent-ref", secondCall.GetExecutionRef())
	}
	if secondCall.GetMetadata() == nil || secondCall.GetMetadata().AsMap()["workflow_key"] != workflowKey {
		t.Fatalf("second call metadata = %#v, want workflow_key %q", secondCall.GetMetadata(), workflowKey)
	}

	waitForCondition(t, time.Second, func() bool {
		run, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{RunId: first.GetRun().GetId()})
		return err == nil && run.GetStatus() == proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED
	})

	duplicate, err := provider.SignalOrStartRun(ctx, &proto.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  "slack:T123:C123:1700000000.000001",
		Target:       target,
		ExecutionRef: "ignored-duplicate-ref",
		Signal:       protoWorkflowSignal(t, "", "evt-2", "second duplicate"),
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun(duplicate): %v", err)
	}
	if duplicate.GetRun().GetId() != first.GetRun().GetId() || duplicate.GetStartedRun() {
		t.Fatalf("duplicate response = %#v, want same run without start", duplicate)
	}

	third, err := provider.SignalOrStartRun(ctx, &proto.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  "slack:T123:C123:1700000000.000001",
		Target:       target,
		ExecutionRef: "agent-ref-new-run",
		Signal:       protoWorkflowSignal(t, "sig-3", "evt-3", "third"),
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun(third): %v", err)
	}
	if !third.GetStartedRun() || third.GetRun().GetId() == first.GetRun().GetId() {
		t.Fatalf("third response = %#v, want new run", third)
	}
}

func TestProviderSignalOrStartRunFailsQueuedSignalsWhenRunFails(t *testing.T) {
	for _, tc := range []struct {
		name          string
		status        int32
		body          string
		errs          []error
		statusMessage string
	}{
		{
			name:          "transport error",
			status:        202,
			body:          `{"ok":true}`,
			errs:          []error{context.DeadlineExceeded},
			statusMessage: context.DeadlineExceeded.Error(),
		},
		{
			name:          "http error",
			status:        500,
			body:          `{"error":"boom"}`,
			statusMessage: "workflow operation returned status 500",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			host := newWorkflowHostStub(tc.status, tc.body)
			host.errs = tc.errs
			host.releaseCh = make(chan struct{})
			startTestIndexedDBBackend(t)
			startTestWorkflowHost(t, host)

			provider := New()
			if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "10ms"}); err != nil {
				t.Fatalf("Configure: %v", err)
			}
			startProviderWorker(t, provider)
			t.Cleanup(func() { _ = provider.Close() })

			workflowKey := "slack:T123:C123:1700000000.000001:" + strings.ReplaceAll(tc.name, " ", "-")
			target := protoAgentTarget("managed", "gpt-5.5", "Respond in the Slack thread")
			first, err := provider.SignalOrStartRun(ctx, &proto.SignalOrStartWorkflowProviderRunRequest{
				WorkflowKey:  workflowKey,
				Target:       target,
				ExecutionRef: "agent-ref",
				Signal:       protoWorkflowSignal(t, "", "evt-1", "first"),
			})
			if err != nil {
				t.Fatalf("SignalOrStartRun(first): %v", err)
			}
			if _, err := host.waitForCall(time.Second); err != nil {
				t.Fatalf("waitForCall(first): %v", err)
			}

			if _, err := provider.SignalOrStartRun(ctx, &proto.SignalOrStartWorkflowProviderRunRequest{
				WorkflowKey:  workflowKey,
				Target:       target,
				ExecutionRef: "ignored-new-ref",
				Signal:       protoWorkflowSignal(t, "", "evt-2", "second"),
			}); err != nil {
				t.Fatalf("SignalOrStartRun(second): %v", err)
			}

			close(host.releaseCh)
			waitForCondition(t, time.Second, func() bool {
				run, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{RunId: first.GetRun().GetId()})
				return err == nil && run.GetStatus() == proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED
			})

			signals, err := listSignalRecords(ctx, provider.signalStore, first.GetRun().GetId(), "")
			if err != nil {
				t.Fatalf("listSignalRecords: %v", err)
			}
			if len(signals) != 2 {
				t.Fatalf("signals len = %d, want 2", len(signals))
			}
			for _, signal := range signals {
				if signal.State != signalStateFailed {
					t.Fatalf("signal %q state = %q, want failed", signal.ID, signal.State)
				}
				if !strings.Contains(signal.StatusMessage, tc.statusMessage) {
					t.Fatalf("signal %q status_message = %q", signal.ID, signal.StatusMessage)
				}
			}
			if len(host.calls()) != 1 {
				t.Fatalf("host calls = %d, want 1", len(host.calls()))
			}

			third, err := provider.SignalOrStartRun(ctx, &proto.SignalOrStartWorkflowProviderRunRequest{
				WorkflowKey:  workflowKey,
				Target:       target,
				ExecutionRef: "agent-ref-new-run",
				Signal:       protoWorkflowSignal(t, "sig-3", "evt-3", "third"),
			})
			if err != nil {
				t.Fatalf("SignalOrStartRun(third): %v", err)
			}
			if !third.GetStartedRun() || third.GetRun().GetId() == first.GetRun().GetId() {
				t.Fatalf("third response = %#v, want new run", third)
			}
		})
	}
}

func TestProviderSignalOrStartRunDoesNotScanSignalsForOtherRuns(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackendWithWrapper(t, func(inner proto.IndexedDBServer) proto.IndexedDBServer {
		return &indexedDBServerSpy{IndexedDBServer: inner, failUnscopedSignalGetAll: true}
	})
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	now := time.Now().UTC()
	largePayload := strings.Repeat("x", 256*1024)
	for i := 0; i < 24; i++ {
		signal := &proto.WorkflowSignal{
			Id:             fmt.Sprintf("other-signal-%02d", i),
			Name:           "github.app.webhook",
			Payload:        mustStruct(t, map[string]any{"body": largePayload}),
			CreatedAt:      timestamppb.New(now),
			IdempotencyKey: fmt.Sprintf("other-event-%02d", i),
			Sequence:       int64(i + 1),
		}
		record := workflowSignalRecord{
			ID:             signal.GetId(),
			RunID:          "other-run",
			WorkflowKey:    "github:other",
			State:          signalStateDelivered,
			Signal:         signal,
			IdempotencyKey: signal.GetIdempotencyKey(),
			Sequence:       signal.GetSequence(),
			CreatedAt:      now,
		}
		if err := provider.signalStore.Add(ctx, record.toRecord()); err != nil {
			t.Fatalf("seed large signal %d: %v", i, err)
		}
	}

	resp, err := provider.SignalOrStartRun(ctx, &proto.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  "github:127579767:valon-technologies/toolshed:1",
		Target:       protoAgentTarget("managed", "gpt-5.5", "Respond in the GitHub thread"),
		ExecutionRef: "agent-ref",
		Signal:       protoWorkflowSignal(t, "", "new-event", "new"),
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun: %v", err)
	}
	if got := resp.GetSignal().GetSequence(); got != 1 {
		t.Fatalf("signal sequence = %d, want 1", got)
	}
}

func TestProviderListRunsDoesNotLoadEachRunByKey(t *testing.T) {
	ctx := context.Background()
	var spy *indexedDBServerSpy
	startTestIndexedDBBackendWithWrapper(t, func(inner proto.IndexedDBServer) proto.IndexedDBServer {
		spy = &indexedDBServerSpy{IndexedDBServer: inner}
		return spy
	})
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	for i := 0; i < 3; i++ {
		_, err := provider.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
			IdempotencyKey: fmt.Sprintf("list-runs-%d", i),
			Target:         protoBoundTarget(t, "roadmap", "sync", map[string]any{"index": i}),
			CreatedBy:      &proto.WorkflowActor{SubjectId: "user:123", SubjectKind: "user"},
		})
		if err != nil {
			t.Fatalf("StartRun(%d): %v", i, err)
		}
	}

	spy.resetOperationCounts()
	runs, err := provider.ListRuns(ctx, &proto.ListWorkflowProviderRunsRequest{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs.GetRuns()) != 3 {
		t.Fatalf("runs len = %d, want 3", len(runs.GetRuns()))
	}
	if got := spy.getCount(storeRuns); got != 0 {
		t.Fatalf("workflow_runs Get count = %d, want 0", got)
	}
}

func TestProviderSignalOrStartRunConcurrentSignalsShareWorkflowKeyRun(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	secondProvider := New()
	if err := secondProvider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure(second): %v", err)
	}
	t.Cleanup(func() { _ = secondProvider.Close() })
	providers := []*Provider{provider, secondProvider}

	const signalCount = 12
	type result struct {
		resp *proto.SignalWorkflowRunResponse
		err  error
	}
	start := make(chan struct{})
	results := make(chan result, signalCount)
	for i := 0; i < signalCount; i++ {
		i := i
		go func() {
			<-start
			p := providers[i%len(providers)]
			resp, err := p.SignalOrStartRun(ctx, &proto.SignalOrStartWorkflowProviderRunRequest{
				WorkflowKey:  "github:127579767:valon-technologies/gestalt:issue_comment:42",
				Target:       protoAgentTarget("managed", "gpt-5.5", "Respond in the GitHub thread"),
				ExecutionRef: "agent-ref",
				Signal:       protoWorkflowSignal(t, "", fmt.Sprintf("github-delivery-%02d", i), fmt.Sprintf("event %02d", i)),
			})
			results <- result{resp: resp, err: err}
		}()
	}
	close(start)

	runID := ""
	started := 0
	seenSequences := make(map[int64]bool, signalCount)
	for i := 0; i < signalCount; i++ {
		result := <-results
		if result.err != nil {
			t.Fatalf("SignalOrStartRun(%d): %v", i, result.err)
		}
		resp := result.resp
		if resp.GetRun().GetId() == "" {
			t.Fatalf("response %d run_id is empty", i)
		}
		if runID == "" {
			runID = resp.GetRun().GetId()
		}
		if resp.GetRun().GetId() != runID {
			t.Fatalf("response %d run_id = %q, want %q", i, resp.GetRun().GetId(), runID)
		}
		if resp.GetStartedRun() {
			started++
		}
		sequence := resp.GetSignal().GetSequence()
		if sequence < 1 || sequence > signalCount {
			t.Fatalf("response %d sequence = %d, want 1..%d", i, sequence, signalCount)
		}
		if seenSequences[sequence] {
			t.Fatalf("duplicate sequence %d", sequence)
		}
		seenSequences[sequence] = true
	}
	if started != 1 {
		t.Fatalf("started_run count = %d, want 1", started)
	}

	signals, err := listSignalRecords(ctx, provider.signalStore, runID, signalStatePending)
	if err != nil {
		t.Fatalf("listSignalRecords: %v", err)
	}
	if len(signals) != signalCount {
		t.Fatalf("pending signals len = %d, want %d", len(signals), signalCount)
	}
	for sequence := int64(1); sequence <= signalCount; sequence++ {
		if !seenSequences[sequence] {
			t.Fatalf("missing sequence %d", sequence)
		}
	}
}

func TestProviderSignalOrStartRunRejectsExplicitSignalIDFromOtherWorkflowKey(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	target := protoAgentTarget("managed", "gpt-5.5", "Respond in the GitHub thread")
	first, err := provider.SignalOrStartRun(ctx, &proto.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  "github:127579767:valon-technologies/gestalt:issue_comment:42",
		Target:       target,
		ExecutionRef: "agent-ref",
		Signal:       protoWorkflowSignal(t, "shared-signal-id", "github-delivery-1", "first"),
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun(first): %v", err)
	}
	if first.GetRun().GetId() == "" {
		t.Fatalf("first run id is empty")
	}

	_, err = provider.SignalOrStartRun(ctx, &proto.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  "github:127579767:valon-technologies/gestalt:issue_comment:43",
		Target:       target,
		ExecutionRef: "agent-ref",
		Signal:       protoWorkflowSignal(t, "shared-signal-id", "github-delivery-2", "second"),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("SignalOrStartRun(second) error = %v, want FailedPrecondition", err)
	}
}

func TestProviderTerminalKeyedRunWithPendingSignalIsRunnable(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	now := time.Now().UTC()
	run := workflowRunRecord{
		ID:          "terminal-keyed-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED,
		Target:      protoAgentTarget("managed", "gpt-5.5", "Respond in the GitHub thread"),
		TriggerKind: triggerKindManual,
		CreatedAt:   now.Add(-time.Minute),
		CompletedAt: timePtr(now),
		WorkflowKey: "github:127579767:valon-technologies/gestalt:issue_comment:42",
	}
	if err := provider.runStore.Add(ctx, run.toRecord()); err != nil {
		t.Fatalf("seed terminal run: %v", err)
	}
	signal := protoWorkflowSignal(t, "late-signal", "late-delivery", "late")
	record := workflowSignalRecord{
		ID:             signal.GetId(),
		RunID:          run.ID,
		WorkflowKey:    run.WorkflowKey,
		State:          signalStatePending,
		Signal:         signal,
		IdempotencyKey: signal.GetIdempotencyKey(),
		Sequence:       1,
		CreatedAt:      now,
	}
	if err := provider.signalStore.Add(ctx, record.toRecord()); err != nil {
		t.Fatalf("seed pending signal: %v", err)
	}

	processed, err := provider.processNextPendingRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("processNextPendingRun: %v", err)
	}
	if !processed {
		t.Fatal("processNextPendingRun processed = false, want true")
	}
	call, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall: %v", err)
	}
	if call.GetRunId() != run.ID || len(call.GetSignals()) != 1 || call.GetSignals()[0].GetId() != signal.GetId() {
		t.Fatalf("host call = %#v, want late signal on terminal run", call)
	}
}

func TestProviderProcessNextPendingRunClaimsRunAcrossProviders(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	host.releaseCh = make(chan struct{})
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	first := New()
	if err := first.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure(first): %v", err)
	}
	t.Cleanup(func() { _ = first.Close() })
	second := New()
	if err := second.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure(second): %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })

	now := time.Now().UTC()
	run := workflowRunRecord{
		ID:          "shared-pending-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:      protoBoundTarget(t, "roadmap", "sync", nil),
		TriggerKind: triggerKindManual,
		CreatedAt:   now,
	}
	if err := first.runStore.Add(ctx, run.toRecord()); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	type result struct {
		processed bool
		err       error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for _, provider := range []*Provider{first, second} {
		provider := provider
		go func() {
			<-start
			processed, err := provider.processNextPendingRun(ctx, run.ID)
			results <- result{processed: processed, err: err}
		}()
	}
	close(start)

	call, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall(first): %v", err)
	}
	if call.GetRunId() != run.ID {
		t.Fatalf("run_id = %q, want %q", call.GetRunId(), run.ID)
	}
	if _, err := host.waitForCall(100 * time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second host call error = %v, want deadline exceeded", err)
	}
	close(host.releaseCh)

	processedCount := 0
	for i := 0; i < 2; i++ {
		result := <-results
		if result.err != nil {
			t.Fatalf("process result %d error: %v", i, result.err)
		}
		if result.processed {
			processedCount++
		}
	}
	if processedCount != 1 {
		t.Fatalf("processed count = %d, want 1", processedCount)
	}
	if _, found, err := loadRunClaimRecord(ctx, first.runClaimStore, run.ID); err != nil {
		t.Fatalf("load run claim: %v", err)
	} else if found {
		t.Fatalf("run claim for %q still exists after completion", run.ID)
	}
}

func TestProviderProcessNextPendingRunStartsUnkeyedRunWithClaim(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	host.releaseCh = make(chan struct{})
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	now := time.Now().UTC()
	run := workflowRunRecord{
		ID:          "unkeyed-pending-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:      protoBoundTarget(t, "brain", "sources.slack.events.ingest", map[string]any{"sourceId": "slack-valon-public"}),
		TriggerKind: triggerKindEvent,
		CreatedAt:   now,
	}
	if err := provider.runStore.Add(ctx, run.toRecord()); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	type result struct {
		processed bool
		err       error
	}
	done := make(chan result, 1)
	go func() {
		processed, err := provider.processNextPendingRun(ctx, run.ID)
		done <- result{processed: processed, err: err}
	}()

	call, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall: %v", err)
	}
	if call.GetRunId() != run.ID {
		t.Fatalf("run_id = %q, want %q", call.GetRunId(), run.ID)
	}
	started, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{RunId: run.ID})
	if err != nil {
		t.Fatalf("GetRun(started): %v", err)
	}
	if started.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING || started.GetStartedAt() == nil {
		t.Fatalf("started run status = %s started_at=%v, want running with started_at", started.GetStatus(), started.GetStartedAt())
	}
	if _, found, err := loadRunClaimRecord(ctx, provider.runClaimStore, run.ID); err != nil {
		t.Fatalf("load run claim: %v", err)
	} else if !found {
		t.Fatalf("run claim for %q was not present while host invocation was running", run.ID)
	}

	close(host.releaseCh)
	res := <-done
	if res.err != nil {
		t.Fatalf("processNextPendingRun: %v", res.err)
	}
	if !res.processed {
		t.Fatal("processNextPendingRun processed = false, want true")
	}
	completed, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{RunId: run.ID})
	if err != nil {
		t.Fatalf("GetRun(completed): %v", err)
	}
	if completed.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED {
		t.Fatalf("completed run status = %s, want succeeded", completed.GetStatus())
	}
	if _, found, err := loadRunClaimRecord(ctx, provider.runClaimStore, run.ID); err != nil {
		t.Fatalf("load completed run claim: %v", err)
	} else if found {
		t.Fatalf("run claim for %q still exists after completion", run.ID)
	}
}

func TestProviderProcessNextPendingRunSkipsFreshClaimedRun(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	now := time.Now().UTC()
	first := workflowRunRecord{
		ID:          "fresh-claimed-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:      protoBoundTarget(t, "roadmap", "sync", map[string]any{"run": "first"}),
		TriggerKind: triggerKindManual,
		CreatedAt:   now,
	}
	second := workflowRunRecord{
		ID:          "next-runnable-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:      protoBoundTarget(t, "roadmap", "sync", map[string]any{"run": "second"}),
		TriggerKind: triggerKindManual,
		CreatedAt:   now.Add(time.Second),
	}
	if err := provider.runStore.Add(ctx, first.toRecord()); err != nil {
		t.Fatalf("seed first run: %v", err)
	}
	if err := provider.runStore.Add(ctx, second.toRecord()); err != nil {
		t.Fatalf("seed second run: %v", err)
	}
	putRunClaim(t, ctx, provider.runClaimStore, workflowRunClaimRecord{
		ID:        first.ID,
		RunID:     first.ID,
		OwnerID:   "other-provider",
		ClaimedAt: now,
		ExpiresAt: now.Add(time.Hour),
	})

	processed, err := provider.processNextPendingRun(ctx, "")
	if err != nil {
		t.Fatalf("processNextPendingRun: %v", err)
	}
	if !processed {
		t.Fatal("processNextPendingRun processed = false, want true")
	}
	call, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall: %v", err)
	}
	if call.GetRunId() != second.ID {
		t.Fatalf("run_id = %q, want %q", call.GetRunId(), second.ID)
	}
	reloadedFirst, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{RunId: first.ID})
	if err != nil {
		t.Fatalf("GetRun(first): %v", err)
	}
	if reloadedFirst.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING {
		t.Fatalf("first status = %s, want pending", reloadedFirst.GetStatus())
	}
}

func TestProviderSignalOrStartRunReplacesStaleWorkflowKey(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	workflowKey := "github:127579767:valon-technologies/gestalt:issue_comment:42"
	if err := addWorkflowKeyRecord(ctx, provider.workflowKeyStore, workflowKey, "missing-run", time.Now().UTC()); err != nil {
		t.Fatalf("seed workflow key: %v", err)
	}

	resp, err := provider.SignalOrStartRun(ctx, &proto.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  workflowKey,
		Target:       protoAgentTarget("managed", "gpt-5.5", "Respond in the GitHub thread"),
		ExecutionRef: "agent-ref",
		Signal:       protoWorkflowSignal(t, "", "github-delivery-1", "first"),
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun: %v", err)
	}
	if !resp.GetStartedRun() || resp.GetRun().GetId() == "" || resp.GetRun().GetId() == "missing-run" {
		t.Fatalf("response = %#v, want replacement run", resp)
	}
}

func TestProviderSignalOrStartRunRecoversStaleRunningWorkflowKey(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	base := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
	now := base
	provider := New()
	provider.now = func() time.Time { return now }
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	if err := provider.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	workflowKey := "slack:T123:C123:1700000000.000001"
	staleStartedAt := base.Add(-time.Minute)
	now = base.Add(time.Minute)
	if err := provider.runStore.Put(ctx, workflowRunRecord{
		ID:           "stale-running-run",
		Status:       proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING,
		WorkflowKey:  workflowKey,
		Target:       protoAgentTarget("managed", "gpt-5.5", "Respond in the Slack thread"),
		TriggerKind:  triggerKindManual,
		CreatedAt:    staleStartedAt.Add(-time.Second),
		StartedAt:    &staleStartedAt,
		ExecutionRef: "stale-ref",
	}.toRecord()); err != nil {
		t.Fatalf("Put(stale-running-run): %v", err)
	}
	putExpiredRunClaim(t, ctx, provider.runClaimStore, "stale-running-run", staleStartedAt)
	if err := addWorkflowKeyRecord(ctx, provider.workflowKeyStore, workflowKey, "stale-running-run", staleStartedAt); err != nil {
		t.Fatalf("addWorkflowKeyRecord: %v", err)
	}
	oldSignal := workflowSignalRecord{
		ID:             "old-signal",
		RunID:          "stale-running-run",
		WorkflowKey:    workflowKey,
		State:          signalStatePending,
		Signal:         protoWorkflowSignal(t, "old-signal", "old-delivery", "old"),
		IdempotencyKey: "old-delivery",
		Sequence:       1,
		CreatedAt:      staleStartedAt,
	}
	if err := provider.signalStore.Put(ctx, oldSignal.toRecord()); err != nil {
		t.Fatalf("Put(old-signal): %v", err)
	}
	if err := storeSignalIdempotencyRecord(ctx, provider.idempotencyStore, "agent:managed", "old-delivery", "stale-running-run", "old-signal", workflowKey, true, staleStartedAt); err != nil {
		t.Fatalf("store stale signal idempotency: %v", err)
	}

	resp, err := provider.SignalOrStartRun(ctx, &proto.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  workflowKey,
		Target:       protoAgentTarget("managed", "gpt-5.5", "Respond in the Slack thread"),
		ExecutionRef: "fresh-ref",
		Signal:       protoWorkflowSignal(t, "old-signal", "old-delivery", "fresh retry"),
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun: %v", err)
	}
	if !resp.GetStartedRun() || resp.GetRun().GetId() == "" || resp.GetRun().GetId() == "stale-running-run" {
		t.Fatalf("response = %#v, want fresh replacement run", resp)
	}

	stale, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{RunId: "stale-running-run"})
	if err != nil {
		t.Fatalf("GetRun(stale): %v", err)
	}
	if stale.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED ||
		stale.GetStatusMessage() != staleRunStatusMessage {
		t.Fatalf("stale run status = %s %q, want failed stale recovery", stale.GetStatus(), stale.GetStatusMessage())
	}
	signals, err := listSignalRecords(ctx, provider.signalStore, "stale-running-run", "")
	if err != nil {
		t.Fatalf("listSignalRecords(stale): %v", err)
	}
	if len(signals) != 1 || signals[0].State != signalStateFailed {
		t.Fatalf("stale signals = %#v, want one failed signal", signals)
	}
	idempotency, found, err := loadIdempotencyRecord(ctx, provider.idempotencyStore, "agent:managed", "old-delivery")
	if err != nil {
		t.Fatalf("load idempotency: %v", err)
	}
	if !found || idempotency.RunID != resp.GetRun().GetId() || idempotency.SignalID == "old-signal" {
		t.Fatalf("idempotency = %#v, found %v; want rebound to fresh run/signal", idempotency, found)
	}
}

func TestProviderFinalizingOldTerminalRunDoesNotDeleteNewerWorkflowKey(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	now := time.Now().UTC()
	workflowKey := "github:127579767:valon-technologies/gestalt:issue_comment:42"
	oldRun := workflowRunRecord{
		ID:          "old-terminal-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED,
		Target:      protoAgentTarget("managed", "gpt-5.5", "Respond in the GitHub thread"),
		TriggerKind: triggerKindManual,
		CreatedAt:   now.Add(-time.Minute),
		CompletedAt: timePtr(now),
		WorkflowKey: workflowKey,
	}
	newRun := workflowRunRecord{
		ID:          "new-active-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:      protoAgentTarget("managed", "gpt-5.5", "Respond in the GitHub thread"),
		TriggerKind: triggerKindManual,
		CreatedAt:   now,
		WorkflowKey: workflowKey,
	}
	for _, run := range []workflowRunRecord{oldRun, newRun} {
		if err := provider.runStore.Add(ctx, run.toRecord()); err != nil {
			t.Fatalf("seed run %q: %v", run.ID, err)
		}
	}
	if err := addWorkflowKeyRecord(ctx, provider.workflowKeyStore, workflowKey, newRun.ID, now); err != nil {
		t.Fatalf("seed workflow key: %v", err)
	}
	signal := protoWorkflowSignal(t, "late-signal", "late-delivery", "late")
	record := workflowSignalRecord{
		ID:             signal.GetId(),
		RunID:          oldRun.ID,
		WorkflowKey:    workflowKey,
		State:          signalStatePending,
		Signal:         signal,
		IdempotencyKey: signal.GetIdempotencyKey(),
		Sequence:       1,
		CreatedAt:      now,
	}
	if err := provider.signalStore.Add(ctx, record.toRecord()); err != nil {
		t.Fatalf("seed pending signal: %v", err)
	}

	processed, err := provider.processNextPendingRun(ctx, oldRun.ID)
	if err != nil {
		t.Fatalf("processNextPendingRun: %v", err)
	}
	if !processed {
		t.Fatal("processNextPendingRun processed = false, want true")
	}
	if _, err := host.waitForCall(time.Second); err != nil {
		t.Fatalf("waitForCall: %v", err)
	}
	key, found, err := loadWorkflowKeyRecord(ctx, provider.workflowKeyStore, workflowKey)
	if err != nil {
		t.Fatalf("loadWorkflowKeyRecord: %v", err)
	}
	if !found || key.RunID != newRun.ID {
		t.Fatalf("workflow key = %#v, found=%v, want run %q", key, found, newRun.ID)
	}
}

func TestProviderSignalRunConcurrentSignalsUseUniqueSequences(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	secondProvider := New()
	if err := secondProvider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure(second): %v", err)
	}
	t.Cleanup(func() { _ = secondProvider.Close() })
	providers := []*Provider{provider, secondProvider}

	now := time.Now().UTC()
	run := workflowRunRecord{
		ID:                 "signal-run-concurrent",
		Status:             proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:             protoAgentTarget("managed", "gpt-5.5", "Respond in the GitHub thread"),
		TriggerKind:        triggerKindManual,
		NextSignalSequence: 1,
		CreatedAt:          now,
	}
	if err := provider.runStore.Add(ctx, run.toRecord()); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	const signalCount = 10
	type result struct {
		resp *proto.SignalWorkflowRunResponse
		err  error
	}
	start := make(chan struct{})
	results := make(chan result, signalCount)
	for i := 0; i < signalCount; i++ {
		i := i
		go func() {
			<-start
			p := providers[i%len(providers)]
			resp, err := p.SignalRun(ctx, &proto.SignalWorkflowProviderRunRequest{
				RunId:  run.ID,
				Signal: protoWorkflowSignal(t, "", fmt.Sprintf("signal-run-delivery-%02d", i), fmt.Sprintf("event %02d", i)),
			})
			results <- result{resp: resp, err: err}
		}()
	}
	close(start)

	seenSequences := make(map[int64]bool, signalCount)
	for i := 0; i < signalCount; i++ {
		result := <-results
		if result.err != nil {
			t.Fatalf("SignalRun(%d): %v", i, result.err)
		}
		if result.resp.GetRun().GetId() != run.ID {
			t.Fatalf("response %d run_id = %q, want %q", i, result.resp.GetRun().GetId(), run.ID)
		}
		sequence := result.resp.GetSignal().GetSequence()
		if sequence < 1 || sequence > signalCount {
			t.Fatalf("response %d sequence = %d, want 1..%d", i, sequence, signalCount)
		}
		if seenSequences[sequence] {
			t.Fatalf("duplicate sequence %d", sequence)
		}
		seenSequences[sequence] = true
	}
}

func TestProviderSignalWakePrefersRunAndBatchesSignals(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	host.releaseCh = make(chan struct{})
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	now := time.Now().UTC()
	oldRunIDs := map[string]bool{}
	for runIndex := 0; runIndex < defaultWorkerCount; runIndex++ {
		oldRun := workflowRunRecord{
			ID:          fmt.Sprintf("old-hot-run-%d", runIndex),
			Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
			Target:      protoAgentTarget("managed", "gpt-5.5", "Respond in the GitHub thread"),
			TriggerKind: triggerKindManual,
			CreatedAt:   now.Add(-time.Hour).Add(time.Duration(runIndex) * time.Second),
			WorkflowKey: fmt.Sprintf("github:127579767:valon-technologies/gestalt:%d", runIndex),
		}
		oldRunIDs[oldRun.ID] = true
		if err := provider.runStore.Add(ctx, oldRun.toRecord()); err != nil {
			t.Fatalf("seed old run %d: %v", runIndex, err)
		}
		for sequence := 1; sequence <= defaultMaxSignalsPerBatch+1; sequence++ {
			signal := protoWorkflowSignal(
				t,
				fmt.Sprintf("old-%d-signal-%02d", runIndex, sequence),
				fmt.Sprintf("old-%d-event-%02d", runIndex, sequence),
				fmt.Sprintf("old %d/%d", runIndex, sequence),
			)
			signal.Sequence = int64(sequence)
			record := workflowSignalRecord{
				ID:             signal.GetId(),
				RunID:          oldRun.ID,
				WorkflowKey:    oldRun.WorkflowKey,
				State:          signalStatePending,
				Signal:         signal,
				IdempotencyKey: signal.GetIdempotencyKey(),
				Sequence:       signal.GetSequence(),
				CreatedAt:      oldRun.CreatedAt.Add(time.Duration(sequence) * time.Second),
			}
			if err := provider.signalStore.Add(ctx, record.toRecord()); err != nil {
				t.Fatalf("seed old signal %d/%d: %v", runIndex, sequence, err)
			}
		}
	}

	startProviderWorker(t, provider)
	call, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall(initial): %v", err)
	}
	if !oldRunIDs[call.GetRunId()] {
		t.Fatalf("initial call run_id = %q, want old hot run", call.GetRunId())
	}
	if got := len(call.GetSignals()); got != defaultMaxSignalsPerBatch {
		t.Fatalf("initial call signal count = %d, want max batch %d", got, defaultMaxSignalsPerBatch)
	}

	interactiveTarget := protoAgentTarget("managed", "gpt-5.5", "Respond in the Slack thread")
	interactiveTarget.GetAgent().Metadata = mustStruct(t, map[string]any{
		gestaltInputKey: map[string]any{
			workflowMetadataKey: map[string]any{
				dispatchPriorityMetadataKey: 5,
			},
		},
	})
	preferred, err := provider.SignalOrStartRun(ctx, &proto.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  "slack:T123:C123:1700000000.000001",
		Target:       interactiveTarget,
		ExecutionRef: "agent-ref",
		Signal:       protoWorkflowSignal(t, "", "slack-event-1", "new"),
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun(preferred): %v", err)
	}
	call, err = host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall(preferred): %v", err)
	}
	if call.GetRunId() != preferred.GetRun().GetId() {
		t.Fatalf("preferred call run_id = %q, want %q", call.GetRunId(), preferred.GetRun().GetId())
	}
	if len(call.GetSignals()) != 1 || call.GetSignals()[0].GetIdempotencyKey() != "slack-event-1" {
		t.Fatalf("preferred call signals = %#v, want preferred signal", call.GetSignals())
	}

	close(host.releaseCh)
}

func TestProviderConfigureFailsWhenSignalSequenceIndexMissing(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackendWithWrapper(t, func(inner proto.IndexedDBServer) proto.IndexedDBServer {
		return &indexedDBServerSpy{IndexedDBServer: inner, missingSignalIndex: "by_run_sequence"}
	})
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"})
	if err == nil {
		t.Fatal("Configure succeeded, want missing signal index error")
	}
	if !strings.Contains(err.Error(), "by_run_sequence") {
		t.Fatalf("Configure error = %v, want by_run_sequence validation failure", err)
	}
}

func TestProviderCancelRunOnlyWhilePending(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	stopProviderWorker(t, provider)

	now := time.Now().UTC()
	pending := workflowRunRecord{
		ID:          "pending-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:      protoBoundTarget(t, "roadmap", "sync", map[string]any{"kind": "pending"}),
		TriggerKind: triggerKindManual,
		CreatedAt:   now,
	}
	if err := provider.runStore.Put(ctx, pending.toRecord()); err != nil {
		t.Fatalf("Put(pending): %v", err)
	}

	canceled, err := provider.CancelRun(ctx, &proto.CancelWorkflowProviderRunRequest{
		RunId:  "pending-run",
		Reason: "skip this run",
	})
	if err != nil {
		t.Fatalf("CancelRun(pending): %v", err)
	}
	if canceled.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_CANCELED {
		t.Fatalf("canceled status = %v, want %v", canceled.GetStatus(), proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_CANCELED)
	}

	running := workflowRunRecord{
		ID:          "running-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING,
		Target:      protoBoundTarget(t, "roadmap", "sync", nil),
		TriggerKind: triggerKindManual,
		CreatedAt:   now,
		StartedAt:   timePtr(now),
	}
	if err := provider.runStore.Put(ctx, running.toRecord()); err != nil {
		t.Fatalf("Put(running): %v", err)
	}
	if _, err := provider.CancelRun(ctx, &proto.CancelWorkflowProviderRunRequest{
		RunId: "running-run",
	}); err == nil {
		t.Fatal("CancelRun(running) succeeded, want error")
	}
}

func TestProviderExecutionReferencesRoundTripAndListBySubject(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	firstCreatedAt := time.Date(2026, time.April, 16, 12, 0, 0, 0, time.UTC)
	secondCreatedAt := firstCreatedAt.Add(time.Minute)
	first, err := provider.PutExecutionReference(ctx, &proto.PutWorkflowExecutionReferenceRequest{
		Reference: &proto.WorkflowExecutionReference{
			Id:                  "ref-1",
			Target:              protoBoundTarget(t, "roadmap", "sync", nil),
			SubjectId:           "user:123",
			CredentialSubjectId: "svc:workflow",
			Permissions: []*proto.WorkflowAccessPermission{
				{Plugin: "roadmap", Operations: []string{"sync", "preview"}},
			},
			CreatedAt: timestamppb.New(firstCreatedAt),
		},
	})
	if err != nil {
		t.Fatalf("PutExecutionReference(ref-1): %v", err)
	}
	if first.GetProviderName() != "indexeddb" {
		t.Fatalf("provider_name = %q, want indexeddb", first.GetProviderName())
	}
	if got := first.GetCreatedAt().AsTime(); !got.Equal(firstCreatedAt) {
		t.Fatalf("created_at = %v, want %v", got, firstCreatedAt)
	}

	revokedAt := secondCreatedAt.Add(time.Minute)
	updatedTarget := protoBoundTarget(t, "roadmap", "sync", nil)
	updatedTarget.GetPlugin().CredentialMode = "none"
	updated, err := provider.PutExecutionReference(ctx, &proto.PutWorkflowExecutionReferenceRequest{
		Reference: &proto.WorkflowExecutionReference{
			Id:                  "ref-1",
			Target:              updatedTarget,
			SubjectId:           "user:123",
			CredentialSubjectId: "svc:workflow",
			RunAs: &proto.WorkflowRunAsSubject{
				SubjectId:   "service_account:roadmap-sync",
				SubjectKind: "service_account",
				DisplayName: "Roadmap sync",
				AuthSource:  "config",
			},
			Permissions: []*proto.WorkflowAccessPermission{
				{Plugin: "roadmap", Operations: []string{"sync"}},
			},
			CreatedAt: timestamppb.New(secondCreatedAt),
			RevokedAt: timestamppb.New(revokedAt),
		},
	})
	if err != nil {
		t.Fatalf("PutExecutionReference(update ref-1): %v", err)
	}
	if got := updated.GetCreatedAt().AsTime(); !got.Equal(firstCreatedAt) {
		t.Fatalf("updated created_at = %v, want preserved %v", got, firstCreatedAt)
	}
	if got := updated.GetRevokedAt().AsTime(); !got.Equal(revokedAt) {
		t.Fatalf("updated revoked_at = %v, want %v", got, revokedAt)
	}

	if _, err := provider.PutExecutionReference(ctx, &proto.PutWorkflowExecutionReferenceRequest{
		Reference: &proto.WorkflowExecutionReference{
			Id:        "ref-2",
			Target:    protoBoundTarget(t, "roadmap", "sync", nil),
			SubjectId: "user:123",
			CreatedAt: timestamppb.New(secondCreatedAt),
		},
	}); err != nil {
		t.Fatalf("PutExecutionReference(ref-2): %v", err)
	}
	if _, err := provider.PutExecutionReference(ctx, &proto.PutWorkflowExecutionReferenceRequest{
		Reference: &proto.WorkflowExecutionReference{
			Id:        "ref-3",
			Target:    protoBoundTarget(t, "billing", "collect", nil),
			SubjectId: "user:999",
			CreatedAt: timestamppb.New(secondCreatedAt.Add(time.Minute)),
		},
	}); err != nil {
		t.Fatalf("PutExecutionReference(ref-3): %v", err)
	}

	got, err := provider.GetExecutionReference(ctx, &proto.GetWorkflowExecutionReferenceRequest{Id: "ref-1"})
	if err != nil {
		t.Fatalf("GetExecutionReference(ref-1): %v", err)
	}
	if got.GetProviderName() != "indexeddb" {
		t.Fatalf("get provider_name = %q, want indexeddb", got.GetProviderName())
	}
	if got.GetCredentialSubjectId() != "svc:workflow" {
		t.Fatalf("credential_subject_id = %q, want svc:workflow", got.GetCredentialSubjectId())
	}
	if got.GetRunAs().GetSubjectId() != "service_account:roadmap-sync" || got.GetRunAs().GetSubjectKind() != "service_account" {
		t.Fatalf("run_as = %#v, want roadmap sync service account", got.GetRunAs())
	}
	if got.GetRunAs().GetDisplayName() != "Roadmap sync" || got.GetRunAs().GetAuthSource() != "config" {
		t.Fatalf("run_as metadata = (%q, %q), want display/auth", got.GetRunAs().GetDisplayName(), got.GetRunAs().GetAuthSource())
	}
	if got.GetTarget().GetPlugin().GetCredentialMode() != "none" {
		t.Fatalf("target credential mode = %q, want none", got.GetTarget().GetPlugin().GetCredentialMode())
	}
	if len(got.GetPermissions()) != 1 || got.GetPermissions()[0].GetPlugin() != "roadmap" {
		t.Fatalf("permissions = %#v, want roadmap entry", got.GetPermissions())
	}
	if ops := got.GetPermissions()[0].GetOperations(); len(ops) != 1 || ops[0] != "sync" {
		t.Fatalf("permission operations = %#v, want [sync]", ops)
	}

	listed, err := provider.ListExecutionReferences(ctx, &proto.ListWorkflowExecutionReferencesRequest{
		SubjectId: "user:123",
	})
	if err != nil {
		t.Fatalf("ListExecutionReferences(subject): %v", err)
	}
	if len(listed.GetReferences()) != 2 {
		t.Fatalf("subject references len = %d, want 2", len(listed.GetReferences()))
	}
	if listed.GetReferences()[0].GetId() != "ref-1" || listed.GetReferences()[1].GetId() != "ref-2" {
		t.Fatalf("subject references ids = [%s %s], want [ref-1 ref-2]", listed.GetReferences()[0].GetId(), listed.GetReferences()[1].GetId())
	}

	all, err := provider.ListExecutionReferences(ctx, &proto.ListWorkflowExecutionReferencesRequest{})
	if err != nil {
		t.Fatalf("ListExecutionReferences(all): %v", err)
	}
	if len(all.GetReferences()) != 3 {
		t.Fatalf("all references len = %d, want 3", len(all.GetReferences()))
	}
}

func TestNormalizeTargetPreservesPluginCredentialMode(t *testing.T) {
	target := protoBoundTarget(t, " github ", " reviewPullRequest ", nil)
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
	target := protoBoundTarget(t, "github", "reviewPullRequest", nil)
	target.GetPlugin().CredentialMode = "platform"

	_, err := normalizeTarget(target)
	if err == nil || !strings.Contains(err.Error(), `target.plugin.credential_mode "platform" is not supported`) {
		t.Fatalf("normalizeTarget error = %v, want unsupported credential mode", err)
	}
}

func TestNormalizeTargetRejectsOutputDeliveryTargetCredentialMode(t *testing.T) {
	target := protoAgentTargetWithOutputDelivery("managed", "gpt-5.4", "send a Slack reminder")
	target.GetAgent().GetOutputDelivery().GetTarget().CredentialMode = "none"

	_, err := normalizeTarget(target)
	if err == nil || !strings.Contains(err.Error(), `target.agent.output_delivery.target.credential_mode "none" is not supported`) {
		t.Fatalf("normalizeTarget error = %v, want unsupported output delivery target mode", err)
	}
}

func TestNormalizeTargetRejectsSessionReadyDeliveryInvalidSources(t *testing.T) {
	target := protoAgentTargetWithOutputDelivery("managed", "gpt-5.4", "send a Slack reminder")
	target.GetAgent().SessionReadyDelivery = protoSessionReadyDelivery()
	target.GetAgent().GetSessionReadyDelivery().GetTarget().CredentialMode = "none"

	_, err := normalizeTarget(target)
	if err == nil || !strings.Contains(err.Error(), `target.agent.session_ready_delivery.target.credential_mode "none" is not supported`) {
		t.Fatalf("normalizeTarget error = %v, want unsupported session ready delivery target mode", err)
	}

	target = protoAgentTargetWithOutputDelivery("managed", "gpt-5.4", "send a Slack reminder")
	target.GetAgent().SessionReadyDelivery = protoSessionReadyDelivery()
	target.GetAgent().GetSessionReadyDelivery().InputBindings[0].Value = &proto.WorkflowOutputValueSource{
		Kind: &proto.WorkflowOutputValueSource_AgentOutput{AgentOutput: "text"},
	}

	_, err = normalizeTarget(target)
	if err == nil || !strings.Contains(err.Error(), "target.agent.session_ready_delivery.input_bindings.value.agent_output is not available before the agent turn starts") {
		t.Fatalf("normalizeTarget error = %v, want unsupported session ready delivery agent output", err)
	}
}

func TestProviderExecutionReferenceRoundTripsAgentTarget(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	createdAt := time.Date(2026, time.April, 16, 12, 0, 0, 0, time.UTC)
	ref, err := provider.PutExecutionReference(ctx, &proto.PutWorkflowExecutionReferenceRequest{
		Reference: &proto.WorkflowExecutionReference{
			Id:        "agent-ref",
			Target:    protoAgentTargetWithDeliveries("managed", "gpt-5.4", "send a Slack reminder"),
			SubjectId: "user:123",
			Permissions: []*proto.WorkflowAccessPermission{
				{Plugin: "slack", Operations: []string{"chat.postMessage"}},
			},
			CreatedAt: timestamppb.New(createdAt),
		},
	})
	if err != nil {
		t.Fatalf("PutExecutionReference(agent-ref): %v", err)
	}
	if ref.GetProviderName() != "indexeddb" {
		t.Fatalf("provider_name = %q, want indexeddb", ref.GetProviderName())
	}
	if ref.GetTarget().GetAgent().GetProviderName() != "managed" {
		t.Fatalf("agent target = %#v", ref.GetTarget())
	}
	if ref.GetTarget().GetPlugin() != nil {
		t.Fatalf("agent target included plugin fields: %#v", ref.GetTarget())
	}
	delivery := ref.GetTarget().GetAgent().GetOutputDelivery()
	if delivery.GetTarget().GetPluginName() != "slack" || delivery.GetTarget().GetOperation() != "events.reply" {
		t.Fatalf("output delivery target = %#v", delivery.GetTarget())
	}
	if delivery.GetCredentialMode() != "none" {
		t.Fatalf("output delivery credential mode = %q, want none", delivery.GetCredentialMode())
	}
	if len(delivery.GetInputBindings()) != 2 ||
		delivery.GetInputBindings()[0].GetInputField() != "text" ||
		delivery.GetInputBindings()[0].GetValue().GetAgentOutput() != "text" ||
		delivery.GetInputBindings()[1].GetInputField() != "reply_ref" ||
		delivery.GetInputBindings()[1].GetValue().GetSignalPayload() != "reply_ref" {
		t.Fatalf("output delivery bindings = %#v", delivery.GetInputBindings())
	}
	sessionReadyDelivery := ref.GetTarget().GetAgent().GetSessionReadyDelivery()
	if sessionReadyDelivery.GetTarget().GetPluginName() != "slack" || sessionReadyDelivery.GetTarget().GetOperation() != "events.replySessionStarted" {
		t.Fatalf("session ready delivery target = %#v", sessionReadyDelivery.GetTarget())
	}
	if sessionReadyDelivery.GetCredentialMode() != "none" {
		t.Fatalf("session ready delivery credential mode = %q, want none", sessionReadyDelivery.GetCredentialMode())
	}
	if len(sessionReadyDelivery.GetInputBindings()) != 2 ||
		sessionReadyDelivery.GetInputBindings()[0].GetInputField() != "session_id" ||
		sessionReadyDelivery.GetInputBindings()[0].GetValue().GetAgentSession() != "id" ||
		sessionReadyDelivery.GetInputBindings()[1].GetInputField() != "reply_ref" ||
		sessionReadyDelivery.GetInputBindings()[1].GetValue().GetSignalPayload() != "reply_ref" {
		t.Fatalf("session ready delivery bindings = %#v", sessionReadyDelivery.GetInputBindings())
	}

	got, err := provider.GetExecutionReference(ctx, &proto.GetWorkflowExecutionReferenceRequest{Id: "agent-ref"})
	if err != nil {
		t.Fatalf("GetExecutionReference(agent-ref): %v", err)
	}
	if !gproto.Equal(got.GetTarget(), ref.GetTarget()) {
		t.Fatalf("round-tripped target = %#v, want %#v", got.GetTarget(), ref.GetTarget())
	}
}

func TestProviderStoresNestedTargetJSONWithoutScalarCopies(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	stopProviderWorker(t, provider)

	target := protoBoundTarget(t, "roadmap", "sync", map[string]any{"mode": "full"})
	target.GetPlugin().Connection = "primary"
	target.GetPlugin().Instance = "prod"

	schedule, err := provider.UpsertSchedule(ctx, &proto.UpsertWorkflowProviderScheduleRequest{
		ScheduleId: "stored-schedule",
		Cron:       "* * * * *",
		Timezone:   "UTC",
		Target:     target,
	})
	if err != nil {
		t.Fatalf("UpsertSchedule: %v", err)
	}
	scheduleRecord, err := provider.scheduleStore.Get(ctx, schedule.GetId())
	if err != nil {
		t.Fatalf("raw schedule get: %v", err)
	}
	assertRecordHasTargetJSON(t, scheduleRecord)
	assertRecordDoesNotContainFields(t, scheduleRecord, "plugin_name", "operation", "connection", "instance", "input")

	trigger, err := provider.UpsertEventTrigger(ctx, &proto.UpsertWorkflowProviderEventTriggerRequest{
		TriggerId: "stored-trigger",
		Match:     &proto.WorkflowEventMatch{Type: "roadmap.updated"},
		Target:    target,
	})
	if err != nil {
		t.Fatalf("UpsertEventTrigger: %v", err)
	}
	triggerRecord, err := provider.eventTriggerStore.Get(ctx, trigger.GetId())
	if err != nil {
		t.Fatalf("raw event trigger get: %v", err)
	}
	assertRecordHasTargetJSON(t, triggerRecord)
	assertRecordDoesNotContainFields(t, triggerRecord, "plugin_name", "operation", "connection", "instance", "input")

	run, err := provider.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{Target: target})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	runRecord, err := provider.runStore.Get(ctx, run.GetId())
	if err != nil {
		t.Fatalf("raw run get: %v", err)
	}
	assertRecordHasTargetJSON(t, runRecord)
	assertRecordDoesNotContainFields(t, runRecord, "plugin_name", "operation", "connection", "instance", "input")

	ref, err := provider.PutExecutionReference(ctx, &proto.PutWorkflowExecutionReferenceRequest{
		Reference: &proto.WorkflowExecutionReference{
			Id:        "stored-ref",
			Target:    target,
			SubjectId: "user:123",
		},
	})
	if err != nil {
		t.Fatalf("PutExecutionReference: %v", err)
	}
	refRecord, err := provider.executionRefStore.Get(ctx, ref.GetId())
	if err != nil {
		t.Fatalf("raw execution ref get: %v", err)
	}
	assertRecordHasTargetJSON(t, refRecord)
	assertRecordDoesNotContainFields(t, refRecord, "target_plugin", "target_operation", "target_connection", "target_instance", "target_fingerprint")
}

func TestProviderPublishEventAndCollapsesMissedCronTicks(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	start := time.Date(2026, time.April, 16, 12, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := New()
	provider.now = clock.Now
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "100ms"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	startProviderWorker(t, provider)
	t.Cleanup(func() { _ = provider.Close() })

	trigger, err := provider.UpsertEventTrigger(ctx, &proto.UpsertWorkflowProviderEventTriggerRequest{
		TriggerId:    "refresh-trigger",
		Match:        &proto.WorkflowEventMatch{Type: "task.updated", Source: "roadmap"},
		Target:       protoBoundTarget(t, "roadmap", "sync", map[string]any{"kind": "event"}),
		ExecutionRef: "event-ref",
	})
	if err != nil {
		t.Fatalf("UpsertEventTrigger: %v", err)
	}
	if trigger.GetExecutionRef() != "event-ref" {
		t.Fatalf("trigger execution_ref = %q, want event-ref", trigger.GetExecutionRef())
	}
	if _, err := provider.PublishEvent(ctx, &proto.PublishWorkflowProviderEventRequest{
		Event: &proto.WorkflowEvent{
			Id:          "evt-1",
			Source:      "roadmap",
			Type:        "task.updated",
			SpecVersion: "1.0",
			Data:        mustStruct(t, map[string]any{"taskId": "task-1"}),
		},
	}); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	eventCall, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall(event): %v", err)
	}
	if eventCall.GetExecutionRef() != "event-ref" {
		t.Fatalf("event execution_ref = %q, want event-ref", eventCall.GetExecutionRef())
	}
	if eventCall.GetTrigger().GetEvent() == nil || eventCall.GetTrigger().GetEvent().GetTriggerId() != "refresh-trigger" {
		t.Fatalf("event trigger = %#v", eventCall.GetTrigger())
	}

	schedule, err := provider.UpsertSchedule(ctx, &proto.UpsertWorkflowProviderScheduleRequest{
		ScheduleId:   "nightly-sync",
		Cron:         "*/5 * * * *",
		Timezone:     "UTC",
		Target:       protoBoundTarget(t, "roadmap", "sync", map[string]any{"kind": "schedule"}),
		ExecutionRef: "schedule-ref",
	})
	if err != nil {
		t.Fatalf("UpsertSchedule: %v", err)
	}
	if schedule.GetExecutionRef() != "schedule-ref" {
		t.Fatalf("schedule execution_ref = %q, want schedule-ref", schedule.GetExecutionRef())
	}
	if got := schedule.GetNextRunAt().AsTime(); !got.Equal(time.Date(2026, time.April, 16, 12, 5, 0, 0, time.UTC)) {
		t.Fatalf("initial next_run_at = %v", got)
	}

	clock.Set(time.Date(2026, time.April, 16, 12, 17, 0, 0, time.UTC))
	provider.mu.Lock()
	provider.signalWorkerLocked("")
	provider.mu.Unlock()

	scheduleCall, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall(schedule): %v", err)
	}
	if scheduleCall.GetExecutionRef() != "schedule-ref" {
		t.Fatalf("schedule call execution_ref = %q, want schedule-ref", scheduleCall.GetExecutionRef())
	}
	scheduledFor := scheduleCall.GetTrigger().GetSchedule().GetScheduledFor().AsTime()
	wantScheduledFor := time.Date(2026, time.April, 16, 12, 15, 0, 0, time.UTC)
	if !scheduledFor.Equal(wantScheduledFor) {
		t.Fatalf("scheduled_for = %v, want %v", scheduledFor, wantScheduledFor)
	}

	updated, err := provider.GetSchedule(ctx, &proto.GetWorkflowProviderScheduleRequest{
		ScheduleId: "nightly-sync",
	})
	if err != nil {
		t.Fatalf("GetSchedule: %v", err)
	}
	wantNext := time.Date(2026, time.April, 16, 12, 20, 0, 0, time.UTC)
	if got := updated.GetNextRunAt().AsTime(); !got.Equal(wantNext) {
		t.Fatalf("next_run_at = %v, want %v", got, wantNext)
	}
	if updated.GetExecutionRef() != "schedule-ref" {
		t.Fatalf("updated schedule execution_ref = %q, want schedule-ref", updated.GetExecutionRef())
	}

	waitForCondition(t, time.Second, func() bool {
		runs, err := provider.ListRuns(ctx, &proto.ListWorkflowProviderRunsRequest{})
		if err != nil || len(runs.GetRuns()) != 2 {
			return false
		}
		for _, run := range runs.GetRuns() {
			if run.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED {
				return false
			}
		}
		return true
	})

	eventRun, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{RunId: eventCall.GetRunId()})
	if err != nil {
		t.Fatalf("GetRun(event): %v", err)
	}
	if eventRun.GetExecutionRef() != "event-ref" {
		t.Fatalf("event run execution_ref = %q, want event-ref", eventRun.GetExecutionRef())
	}

	scheduleRun, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{RunId: scheduleCall.GetRunId()})
	if err != nil {
		t.Fatalf("GetRun(schedule): %v", err)
	}
	if scheduleRun.GetExecutionRef() != "schedule-ref" {
		t.Fatalf("schedule run execution_ref = %q, want schedule-ref", scheduleRun.GetExecutionRef())
	}
}

func TestProviderPublishEventDoesNotWaitForConcurrentScheduleList(t *testing.T) {
	ctx := context.Background()
	blocker := &blockingGetAllServer{
		store:   storeSchedules,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	startTestIndexedDBBackendWithWrapper(t, func(inner proto.IndexedDBServer) proto.IndexedDBServer {
		blocker.IndexedDBServer = inner
		return blocker
	})
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	provider.now = func() time.Time {
		return time.Date(2026, time.April, 16, 12, 0, 0, 0, time.UTC)
	}
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "100ms"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	const triggerID = "refresh-trigger"
	if _, err := provider.UpsertEventTrigger(ctx, &proto.UpsertWorkflowProviderEventTriggerRequest{
		TriggerId: triggerID,
		Match:     &proto.WorkflowEventMatch{Type: "task.updated", Source: "roadmap"},
		Target:    protoBoundTarget(t, "roadmap", "sync", map[string]any{"kind": "event"}),
	}); err != nil {
		t.Fatalf("UpsertEventTrigger: %v", err)
	}

	listDone := make(chan error, 1)
	go func() {
		_, err := provider.ListSchedules(ctx, &proto.ListWorkflowProviderSchedulesRequest{})
		listDone <- err
	}()
	select {
	case <-blocker.entered:
	case <-time.After(time.Second):
		t.Fatal("ListSchedules did not reach blocked backing-store call")
	}
	var releaseOnce sync.Once
	releaseList := func() {
		releaseOnce.Do(func() {
			close(blocker.release)
		})
	}
	defer releaseList()

	publishDone := make(chan error, 1)
	go func() {
		_, err := provider.PublishEvent(ctx, &proto.PublishWorkflowProviderEventRequest{
			PluginName: "roadmap",
			Event: &proto.WorkflowEvent{
				Id:          "evt-while-listing",
				Source:      "roadmap",
				Type:        "task.updated",
				SpecVersion: "1.0",
				Data:        mustStruct(t, map[string]any{"taskId": "task-1"}),
			},
		})
		publishDone <- err
	}()
	select {
	case err := <-publishDone:
		if err != nil {
			t.Fatalf("PublishEvent: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		releaseList()
		<-listDone
		err := <-publishDone
		if err != nil {
			t.Fatalf("PublishEvent after releasing ListSchedules: %v", err)
		}
		t.Fatal("PublishEvent waited for concurrent ListSchedules")
	}

	runID := eventRunID(triggerID, "roadmap", "evt-while-listing")
	run, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{RunId: runID})
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING {
		t.Fatalf("run status = %s, want pending", run.GetStatus())
	}
	if run.GetTrigger().GetEvent().GetTriggerId() != triggerID {
		t.Fatalf("event trigger = %#v", run.GetTrigger())
	}

	releaseList()
	if err := <-listDone; err != nil {
		t.Fatalf("ListSchedules: %v", err)
	}
}

func TestProviderPublishEventUsesPublisherExecutionReference(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := New()
	provider.now = func() time.Time {
		return time.Date(2026, time.April, 16, 12, 0, 0, 0, time.UTC)
	}
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "10ms"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	startProviderWorker(t, provider)
	t.Cleanup(func() { _ = provider.Close() })

	target := protoBoundTarget(t, "github", "events.runAgentFromWorkflowEvent", map[string]any{
		"_gestalt": map[string]any{
			"eventRunPermissions": []any{
				map[string]any{
					"plugin": "github",
					"operations": []any{
						"bot.commitFiles",
						"bot.openPullRequest",
						"bot.createPullRequest",
					},
				},
			},
		},
	})
	if _, err := provider.UpsertEventTrigger(ctx, &proto.UpsertWorkflowProviderEventTriggerRequest{
		TriggerId: "github-webhook",
		Match:     &proto.WorkflowEventMatch{Type: "github.app.webhook", Source: "github"},
		Target:    target,
		RequestedBy: &proto.WorkflowActor{
			SubjectId: "system:config",
		},
	}); err != nil {
		t.Fatalf("UpsertEventTrigger(existing actor shape): %v", err)
	}
	if _, err := provider.UpsertEventTrigger(ctx, &proto.UpsertWorkflowProviderEventTriggerRequest{
		TriggerId: "github-webhook",
		Match:     &proto.WorkflowEventMatch{Type: "github.app.webhook", Source: "github"},
		Target:    target,
		RequestedBy: &proto.WorkflowActor{
			SubjectId:   "system:config",
			SubjectKind: "system",
			DisplayName: "Gestalt config",
			AuthSource:  "config",
		},
	}); err != nil {
		t.Fatalf("UpsertEventTrigger: %v", err)
	}

	publishedBy := &proto.WorkflowActor{
		SubjectId:   "service_account:github_app_installation:127579767:repo:valon-technologies/gestalt",
		SubjectKind: "service_account",
		DisplayName: "GitHub App installation 127579767 (valon-technologies/gestalt)",
		AuthSource:  "github_app_webhook",
	}
	if _, err := provider.PublishEvent(ctx, &proto.PublishWorkflowProviderEventRequest{
		PluginName:  "github",
		PublishedBy: publishedBy,
		Event:       githubWebhookWorkflowEvent(t),
	}); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}

	call, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall: %v", err)
	}
	if call.GetExecutionRef() == "" || call.GetExecutionRef() == "event-ref" {
		t.Fatalf("execution_ref = %q, want publisher-scoped event ref", call.GetExecutionRef())
	}
	if call.GetCreatedBy().GetSubjectId() != publishedBy.GetSubjectId() {
		t.Fatalf("created_by.subject_id = %q, want %q", call.GetCreatedBy().GetSubjectId(), publishedBy.GetSubjectId())
	}
	ref, err := provider.GetExecutionReference(ctx, &proto.GetWorkflowExecutionReferenceRequest{Id: call.GetExecutionRef()})
	if err != nil {
		t.Fatalf("GetExecutionReference: %v", err)
	}
	if ref.GetSubjectId() != publishedBy.GetSubjectId() || ref.GetSubjectKind() != "service_account" || ref.GetAuthSource() != "github_app_webhook" {
		t.Fatalf("execution ref subject = (%q, %q, %q), want published GitHub service account", ref.GetSubjectId(), ref.GetSubjectKind(), ref.GetAuthSource())
	}
	if ref.GetCredentialSubjectId() != publishedBy.GetSubjectId() {
		t.Fatalf("credential_subject_id = %q, want publisher subject", ref.GetCredentialSubjectId())
	}
	gotOperations := map[string]bool{}
	for _, permission := range ref.GetPermissions() {
		if permission.GetPlugin() != "github" {
			continue
		}
		for _, operation := range permission.GetOperations() {
			gotOperations[operation] = true
		}
	}
	for _, operation := range []string{"events.runAgentFromWorkflowEvent", "bot.commitFiles", "bot.openPullRequest", "bot.createPullRequest"} {
		if !gotOperations[operation] {
			t.Fatalf("permissions = %#v, missing github/%s", ref.GetPermissions(), operation)
		}
	}

	duplicatePublisher := &proto.WorkflowActor{
		SubjectId:   "service_account:github_app_installation:127579767:repo:valon-technologies/other",
		SubjectKind: "service_account",
		DisplayName: "GitHub App installation 127579767 (valon-technologies/other)",
		AuthSource:  "github_app_webhook",
	}
	if _, err := provider.PublishEvent(ctx, &proto.PublishWorkflowProviderEventRequest{
		PluginName:  "github",
		PublishedBy: duplicatePublisher,
		Event:       githubWebhookWorkflowEvent(t),
	}); err != nil {
		t.Fatalf("PublishEvent(duplicate): %v", err)
	}
	ref, err = provider.GetExecutionReference(ctx, &proto.GetWorkflowExecutionReferenceRequest{Id: call.GetExecutionRef()})
	if err != nil {
		t.Fatalf("GetExecutionReference(after duplicate): %v", err)
	}
	if ref.GetSubjectId() != publishedBy.GetSubjectId() {
		t.Fatalf("duplicate publish replaced execution ref subject = %q, want %q", ref.GetSubjectId(), publishedBy.GetSubjectId())
	}
	runs, err := provider.ListRuns(ctx, &proto.ListWorkflowProviderRunsRequest{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs.GetRuns()) != 1 {
		t.Fatalf("runs len = %d, want duplicate event to keep one run", len(runs.GetRuns()))
	}
}

func TestProviderPublishEventAgentTargetExecutionReferenceIncludesOutputDelivery(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := New()
	provider.now = func() time.Time {
		return time.Date(2026, time.April, 16, 12, 0, 0, 0, time.UTC)
	}
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "10ms"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	if _, err := provider.UpsertEventTrigger(ctx, &proto.UpsertWorkflowProviderEventTriggerRequest{
		TriggerId: "github-agent-webhook",
		Match:     &proto.WorkflowEventMatch{Type: "github.app.webhook", Source: "github"},
		Target:    protoAgentTargetWithDeliveries("managed", "gpt-5.4", "respond to the GitHub event"),
	}); err != nil {
		t.Fatalf("UpsertEventTrigger(agent): %v", err)
	}

	publishedBy := &proto.WorkflowActor{
		SubjectId:   "service_account:github_app_installation:127579767:repo:valon-technologies/gestalt",
		SubjectKind: "service_account",
		DisplayName: "GitHub App installation 127579767 (valon-technologies/gestalt)",
		AuthSource:  "github_app_webhook",
	}
	if _, err := provider.PublishEvent(ctx, &proto.PublishWorkflowProviderEventRequest{
		PluginName:  "agent:managed",
		PublishedBy: publishedBy,
		Event:       githubWebhookWorkflowEvent(t),
	}); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}

	runs, err := provider.ListRuns(ctx, &proto.ListWorkflowProviderRunsRequest{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs.GetRuns()) != 1 {
		t.Fatalf("runs len = %d, want 1", len(runs.GetRuns()))
	}
	ref, err := provider.GetExecutionReference(ctx, &proto.GetWorkflowExecutionReferenceRequest{Id: runs.GetRuns()[0].GetExecutionRef()})
	if err != nil {
		t.Fatalf("GetExecutionReference: %v", err)
	}
	got := map[string]map[string]bool{}
	for _, permission := range ref.GetPermissions() {
		ops := got[permission.GetPlugin()]
		if ops == nil {
			ops = map[string]bool{}
			got[permission.GetPlugin()] = ops
		}
		for _, operation := range permission.GetOperations() {
			ops[operation] = true
		}
	}
	if !got["slack"]["events.reply"] {
		t.Fatalf("permissions = %#v, missing slack/events.reply output delivery permission", ref.GetPermissions())
	}
	if !got["slack"]["events.replySessionStarted"] {
		t.Fatalf("permissions = %#v, missing slack/events.replySessionStarted session ready delivery permission", ref.GetPermissions())
	}
	if !got["slack"]["chat.postMessage"] {
		t.Fatalf("permissions = %#v, missing slack/chat.postMessage tool permission", ref.GetPermissions())
	}
}

func githubWebhookWorkflowEvent(t *testing.T) *proto.WorkflowEvent {
	t.Helper()
	return &proto.WorkflowEvent{
		Id:          "github:delivery-1",
		Source:      "github",
		Type:        "github.app.webhook",
		SpecVersion: "1.0",
		Data:        mustStruct(t, map[string]any{"repository": "valon-technologies/gestalt"}),
	}
}

func TestProviderAgentSchedulePersistsTargetAndInvokesHost(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	start := time.Date(2026, time.April, 16, 12, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := New()
	provider.now = clock.Now
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "100ms"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	startProviderWorker(t, provider)
	t.Cleanup(func() { _ = provider.Close() })

	schedule, err := provider.UpsertSchedule(ctx, &proto.UpsertWorkflowProviderScheduleRequest{
		ScheduleId:   "slack-reminder",
		Cron:         "* * * * *",
		Timezone:     "UTC",
		Target:       protoAgentTarget("managed", "gpt-5.4", "send a Slack reminder"),
		ExecutionRef: "agent-ref",
	})
	if err != nil {
		t.Fatalf("UpsertSchedule(agent): %v", err)
	}
	if schedule.GetTarget().GetAgent().GetProviderName() != "managed" {
		t.Fatalf("schedule target = %#v", schedule.GetTarget())
	}
	if schedule.GetTarget().GetPlugin() != nil {
		t.Fatalf("schedule target included plugin fields: %#v", schedule.GetTarget())
	}

	listed, err := provider.ListSchedules(ctx, &proto.ListWorkflowProviderSchedulesRequest{})
	if err != nil {
		t.Fatalf("ListSchedules: %v", err)
	}
	if len(listed.GetSchedules()) != 1 || !gproto.Equal(listed.GetSchedules()[0].GetTarget(), schedule.GetTarget()) {
		t.Fatalf("listed schedules = %#v, want persisted agent target", listed.GetSchedules())
	}

	clock.Set(time.Date(2026, time.April, 16, 12, 1, 0, 0, time.UTC))
	provider.mu.Lock()
	provider.signalWorkerLocked("")
	provider.mu.Unlock()

	call, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall(agent schedule): %v", err)
	}
	if call.GetExecutionRef() != "agent-ref" {
		t.Fatalf("execution_ref = %q, want agent-ref", call.GetExecutionRef())
	}
	if call.GetTarget().GetAgent().GetPrompt() != "send a Slack reminder" {
		t.Fatalf("call target = %#v", call.GetTarget())
	}
	toolRefs := call.GetTarget().GetAgent().GetToolRefs()
	if len(toolRefs) != 2 ||
		toolRefs[0].GetPlugin() != "slack" ||
		toolRefs[0].GetOperation() != "chat.postMessage" ||
		toolRefs[1].GetPlugin() != "linear" ||
		toolRefs[1].GetOperation() != "" {
		t.Fatalf("tool refs = %#v", toolRefs)
	}
	if call.GetTarget().GetPlugin() != nil {
		t.Fatalf("call target included plugin fields: %#v", call.GetTarget())
	}
	if call.GetTrigger().GetSchedule().GetScheduleId() != "slack-reminder" {
		t.Fatalf("schedule trigger = %#v", call.GetTrigger())
	}

	run, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{RunId: call.GetRunId()})
	if err != nil {
		t.Fatalf("GetRun(agent schedule): %v", err)
	}
	if !gproto.Equal(run.GetTarget(), call.GetTarget()) {
		t.Fatalf("run target = %#v, want %#v", run.GetTarget(), call.GetTarget())
	}
}

func TestProviderLeavesAgentToolValidationToHost(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	cases := []struct {
		name   string
		target *proto.BoundWorkflowTarget
	}{
		{
			name:   "missing provider",
			target: protoAgentTargetFromMessage(&proto.BoundWorkflowAgentTarget{}),
		},
		{
			name:   "empty prompt",
			target: protoAgentTargetFromMessage(&proto.BoundWorkflowAgentTarget{ProviderName: "managed"}),
		},
		{
			name: "negative timeout",
			target: protoAgentTargetFromMessage(&proto.BoundWorkflowAgentTarget{
				ProviderName:   "managed",
				Prompt:         "send a Slack reminder",
				TimeoutSeconds: -1,
			}),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := provider.UpsertSchedule(ctx, &proto.UpsertWorkflowProviderScheduleRequest{
				ScheduleId: "invalid-" + strings.ReplaceAll(tc.name, " ", "-"),
				Cron:       "* * * * *",
				Timezone:   "UTC",
				Target:     tc.target,
			}); err == nil {
				t.Fatal("UpsertSchedule succeeded, want error")
			}
		})
	}

	target := protoAgentTargetFromMessage(&proto.BoundWorkflowAgentTarget{
		ProviderName: " managed ",
		Prompt:       "send a Slack reminder",
		ToolRefs: []*proto.AgentToolRef{
			{Operation: "chat.postMessage"},
		},
	})
	schedule, err := provider.UpsertSchedule(ctx, &proto.UpsertWorkflowProviderScheduleRequest{
		ScheduleId: "host-validated-agent",
		Cron:       "* * * * *",
		Timezone:   "UTC",
		Target:     target,
	})
	if err != nil {
		t.Fatalf("UpsertSchedule(agent payload host validation): %v", err)
	}
	agent := schedule.GetTarget().GetAgent()
	if agent == nil {
		t.Fatalf("schedule target = %#v, want agent target", schedule.GetTarget())
	}
	if agent.GetProviderName() != "managed" {
		t.Fatalf("agent provider_name = %q, want trimmed provider", agent.GetProviderName())
	}
	if got := agent.GetToolRefs()[0].GetPlugin(); got != "" {
		t.Fatalf("agent tool plugin = %q, want tool validation left to host", got)
	}
}

func TestProviderRequiresStoredTargetJSON(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	stopProviderWorker(t, provider)

	now := time.Now().UTC()
	cases := []struct {
		name string
		put  func() error
		read func() error
		want string
	}{
		{
			name: "schedule missing target json",
			put: func() error {
				return provider.scheduleStore.Put(ctx, gestalt.Record{
					"id":         "missing-target-schedule",
					"cron":       "* * * * *",
					"timezone":   "UTC",
					"created_at": now,
					"updated_at": now,
				})
			},
			read: func() error {
				_, err := provider.GetSchedule(ctx, &proto.GetWorkflowProviderScheduleRequest{ScheduleId: "missing-target-schedule"})
				return err
			},
			want: "missing target_json",
		},
		{
			name: "event trigger missing target json",
			put: func() error {
				return provider.eventTriggerStore.Put(ctx, gestalt.Record{
					"id":            "missing-target-trigger",
					"match_type":    "task.updated",
					"match_source":  "tests",
					"match_subject": "task-1",
					"created_at":    now,
					"updated_at":    now,
				})
			},
			read: func() error {
				_, err := provider.GetEventTrigger(ctx, &proto.GetWorkflowProviderEventTriggerRequest{TriggerId: "missing-target-trigger"})
				return err
			},
			want: "missing target_json",
		},
		{
			name: "run missing target json",
			put: func() error {
				return provider.runStore.Put(ctx, gestalt.Record{
					"id":           "missing-target-run",
					"status":       int64(proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING),
					"trigger_kind": triggerKindManual,
					"created_at":   now,
				})
			},
			read: func() error {
				_, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{RunId: "missing-target-run"})
				return err
			},
			want: "missing target_json",
		},
		{
			name: "execution reference missing target json",
			put: func() error {
				return provider.executionRefStore.Put(ctx, gestalt.Record{
					"id":            "missing-target-ref",
					"provider_name": "workflow",
					"subject_id":    "user-1",
					"created_at":    now,
				})
			},
			read: func() error {
				_, err := provider.GetExecutionReference(ctx, &proto.GetWorkflowExecutionReferenceRequest{Id: "missing-target-ref"})
				return err
			},
			want: "missing target_json",
		},
		{
			name: "run target json with unsupported field",
			put: func() error {
				return provider.runStore.Put(ctx, gestalt.Record{
					"id":           "flat-json-run",
					"status":       int64(proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING),
					"target_json":  `{"agent":{"providerName":"simple","prompt":"send a Slack reminder","unknownField":true}}`,
					"trigger_kind": triggerKindManual,
					"created_at":   now,
				})
			},
			read: func() error {
				_, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{RunId: "flat-json-run"})
				return err
			},
			want: "invalid target_json",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.put(); err != nil {
				t.Fatalf("put: %v", err)
			}
			if err := tc.read(); err == nil {
				t.Fatal("read succeeded, want target_json error")
			} else if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("read error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestProviderPublishEventDoesNotCoalesceDifferentSources(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "10ms"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	startProviderWorker(t, provider)
	t.Cleanup(func() { _ = provider.Close() })

	if _, err := provider.UpsertEventTrigger(ctx, &proto.UpsertWorkflowProviderEventTriggerRequest{
		TriggerId: "refresh-trigger",
		Match:     &proto.WorkflowEventMatch{Type: "task.updated"},
		Target:    protoBoundTarget(t, "roadmap", "sync", map[string]any{"kind": "event"}),
	}); err != nil {
		t.Fatalf("UpsertEventTrigger: %v", err)
	}

	events := []struct {
		source string
		id     string
	}{
		{source: "a:b", id: "c"},
		{source: "a", id: "b:c"},
	}
	for _, event := range events {
		if _, err := provider.PublishEvent(ctx, &proto.PublishWorkflowProviderEventRequest{
			PluginName: "roadmap",
			Event: &proto.WorkflowEvent{
				Id:          event.id,
				Source:      event.source,
				Type:        "task.updated",
				SpecVersion: "1.0",
				Data:        mustStruct(t, map[string]any{"taskId": event.source + "|" + event.id}),
			},
		}); err != nil {
			t.Fatalf("PublishEvent(%s,%s): %v", event.source, event.id, err)
		}
	}

	first, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall(first): %v", err)
	}
	second, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall(second): %v", err)
	}
	if first.GetRunId() == second.GetRunId() {
		t.Fatalf("run ids = %q and %q, want distinct per source", first.GetRunId(), second.GetRunId())
	}

	waitForCondition(t, time.Second, func() bool {
		runs, err := provider.ListRuns(ctx, &proto.ListWorkflowProviderRunsRequest{})
		if err != nil {
			return false
		}
		return len(runs.GetRuns()) == 2
	})
}

func TestProviderEnqueueDueSchedulesReusesDeterministicRunID(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, time.April, 16, 12, 17, 0, 0, time.UTC)
	clock := newFakeClock(start)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	provider.now = clock.Now
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	stopProviderWorker(t, provider)

	schedule, err := provider.UpsertSchedule(ctx, &proto.UpsertWorkflowProviderScheduleRequest{
		ScheduleId: "nightly-sync",
		Cron:       "*/5 * * * *",
		Timezone:   "UTC",
		Target:     protoBoundTarget(t, "roadmap", "sync", map[string]any{"kind": "schedule"}),
	})
	if err != nil {
		t.Fatalf("UpsertSchedule: %v", err)
	}

	latestDue := time.Date(2026, time.April, 16, 12, 15, 0, 0, time.UTC)
	runID := scheduleRunID(schedule.GetId(), latestDue)
	if err := provider.runStore.Put(ctx, workflowRunRecord{
		ID:                  runID,
		Status:              proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:              protoBoundTarget(t, "roadmap", "sync", map[string]any{"kind": "schedule"}),
		TriggerKind:         triggerKindSchedule,
		TriggerScheduleID:   schedule.GetId(),
		TriggerScheduledFor: timePtr(latestDue),
		CreatedAt:           start,
	}.toRecord()); err != nil {
		t.Fatalf("Put(run): %v", err)
	}

	if err := provider.enqueueDueSchedules(ctx); err != nil {
		t.Fatalf("enqueueDueSchedules: %v", err)
	}

	runs, err := provider.ListRuns(ctx, &proto.ListWorkflowProviderRunsRequest{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs.GetRuns()) != 1 {
		t.Fatalf("runs len = %d, want 1", len(runs.GetRuns()))
	}
	updated, err := provider.GetSchedule(ctx, &proto.GetWorkflowProviderScheduleRequest{
		ScheduleId: schedule.GetId(),
	})
	if err != nil {
		t.Fatalf("GetSchedule: %v", err)
	}
	wantNext := time.Date(2026, time.April, 16, 12, 20, 0, 0, time.UTC)
	if got := updated.GetNextRunAt().AsTime(); !got.Equal(wantNext) {
		t.Fatalf("next_run_at = %v, want %v", got, wantNext)
	}
}

func TestProviderRejectsCrossPluginScheduleAndTriggerIDCollisions(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	if _, err := provider.UpsertSchedule(ctx, &proto.UpsertWorkflowProviderScheduleRequest{
		ScheduleId: "shared-id",
		Cron:       "*/5 * * * *",
		Timezone:   "UTC",
		Target:     protoBoundTarget(t, "roadmap", "sync", map[string]any{"kind": "schedule"}),
	}); err != nil {
		t.Fatalf("UpsertSchedule(roadmap): %v", err)
	}
	if _, err := provider.UpsertSchedule(ctx, &proto.UpsertWorkflowProviderScheduleRequest{
		ScheduleId: "shared-id",
		Cron:       "*/5 * * * *",
		Timezone:   "UTC",
		Target:     protoBoundTarget(t, "billing", "sync", map[string]any{"kind": "schedule"}),
	}); err == nil {
		t.Fatal("UpsertSchedule(billing) succeeded, want cross-plugin collision error")
	}

	if _, err := provider.UpsertEventTrigger(ctx, &proto.UpsertWorkflowProviderEventTriggerRequest{
		TriggerId: "shared-trigger",
		Match:     &proto.WorkflowEventMatch{Type: "task.updated"},
		Target:    protoBoundTarget(t, "roadmap", "sync", map[string]any{"kind": "event"}),
	}); err != nil {
		t.Fatalf("UpsertEventTrigger(roadmap): %v", err)
	}
	if _, err := provider.UpsertEventTrigger(ctx, &proto.UpsertWorkflowProviderEventTriggerRequest{
		TriggerId: "shared-trigger",
		Match:     &proto.WorkflowEventMatch{Type: "invoice.updated"},
		Target:    protoBoundTarget(t, "billing", "sync", map[string]any{"kind": "event"}),
	}); err == nil {
		t.Fatal("UpsertEventTrigger(billing) succeeded, want cross-plugin collision error")
	}
}

func TestProviderMarksStaleRunningRunsFailedOnStartup(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	first := New()
	if err := first.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure(first): %v", err)
	}

	startedAt := time.Now().UTC().Add(-time.Minute)
	if err := first.runStore.Put(ctx, workflowRunRecord{
		ID:          "stale-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING,
		Target:      protoBoundTarget(t, "roadmap", "sync", nil),
		TriggerKind: triggerKindManual,
		CreatedAt:   startedAt.Add(-time.Second),
		StartedAt:   &startedAt,
	}.toRecord()); err != nil {
		t.Fatalf("Put(stale-run): %v", err)
	}
	putExpiredRunClaim(t, ctx, first.runClaimStore, "stale-run", startedAt)
	workflowKey := "slack:T123:C123:1700000000.000001"
	if err := first.runStore.Put(ctx, workflowRunRecord{
		ID:           "stale-agent-run",
		Status:       proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING,
		WorkflowKey:  workflowKey,
		Target:       protoAgentTarget("managed", "gpt-5.5", "Respond in the Slack thread"),
		TriggerKind:  triggerKindManual,
		CreatedAt:    startedAt.Add(-time.Second),
		StartedAt:    &startedAt,
		ExecutionRef: "agent-ref",
	}.toRecord()); err != nil {
		t.Fatalf("Put(stale-agent-run): %v", err)
	}
	putExpiredRunClaim(t, ctx, first.runClaimStore, "stale-agent-run", startedAt)
	if err := addWorkflowKeyRecord(ctx, first.workflowKeyStore, workflowKey, "stale-agent-run", startedAt); err != nil {
		t.Fatalf("addWorkflowKeyRecord(stale-agent-run): %v", err)
	}
	for _, signal := range []workflowSignalRecord{
		{
			ID:             "signal-pending",
			RunID:          "stale-agent-run",
			WorkflowKey:    workflowKey,
			State:          signalStatePending,
			Signal:         protoWorkflowSignal(t, "signal-pending", "evt-pending", "pending"),
			IdempotencyKey: "evt-pending",
			Sequence:       1,
			CreatedAt:      startedAt,
		},
		{
			ID:             "signal-claimed",
			RunID:          "stale-agent-run",
			WorkflowKey:    workflowKey,
			State:          signalStateClaimed,
			Signal:         protoWorkflowSignal(t, "signal-claimed", "evt-claimed", "claimed"),
			IdempotencyKey: "evt-claimed",
			Sequence:       2,
			BatchID:        "batch-1",
			CreatedAt:      startedAt,
			ClaimedAt:      &startedAt,
		},
	} {
		if err := first.signalStore.Put(ctx, signal.toRecord()); err != nil {
			t.Fatalf("Put(%s): %v", signal.ID, err)
		}
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close(first): %v", err)
	}

	second := New()
	if err := second.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure(second): %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })
	if err := second.Start(ctx); err != nil {
		t.Fatalf("Start(second): %v", err)
	}

	waitForCondition(t, 5*time.Second, func() bool {
		stale, err := second.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{
			RunId: "stale-run",
		})
		if err != nil {
			t.Fatalf("GetRun(stale): %v", err)
		}
		return stale.GetStatus() == proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED &&
			stale.GetStatusMessage() == "workflow provider restarted while run was in progress"
	})
	waitForCondition(t, 5*time.Second, func() bool {
		staleAgent, err := second.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{RunId: "stale-agent-run"})
		if err != nil {
			t.Fatalf("GetRun(stale-agent-run): %v", err)
		}
		return staleAgent.GetStatus() == proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED
	})
	if _, found, err := loadWorkflowKeyRecord(ctx, second.workflowKeyStore, workflowKey); err != nil {
		t.Fatalf("loadWorkflowKeyRecord: %v", err)
	} else if found {
		t.Fatalf("workflow key %q still points at stale agent run", workflowKey)
	}
	signals, err := listSignalRecords(ctx, second.signalStore, "stale-agent-run", "")
	if err != nil {
		t.Fatalf("listSignalRecords(stale-agent-run): %v", err)
	}
	if len(signals) != 2 {
		t.Fatalf("stale agent signals len = %d, want 2", len(signals))
	}
	for _, signal := range signals {
		if signal.State != signalStateFailed {
			t.Fatalf("signal %q state = %q, want failed", signal.ID, signal.State)
		}
		if signal.StatusMessage != "workflow provider restarted while run was in progress" {
			t.Fatalf("signal %q status_message = %q", signal.ID, signal.StatusMessage)
		}
	}
}

func TestRecoverStaleWorkflowRunsDeletesOrphanPendingClaims(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	now := time.Now().UTC()
	run := workflowRunRecord{
		ID:          "orphan-claimed-pending-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:      protoBoundTarget(t, "brain", "sources.sync", map[string]any{"sourceId": "slack-valon-public"}),
		TriggerKind: triggerKindSchedule,
		CreatedAt:   now.Add(-2 * time.Minute),
	}
	if err := provider.runStore.Put(ctx, run.toRecord()); err != nil {
		t.Fatalf("Put(run): %v", err)
	}
	claimedAt := now.Add(-nonRunningRunClaimGrace - time.Second)
	putRunClaim(t, ctx, provider.runClaimStore, workflowRunClaimRecord{
		ID:        run.ID,
		RunID:     run.ID,
		OwnerID:   "interrupted-provider",
		ClaimedAt: claimedAt,
		ExpiresAt: now.Add(time.Hour),
	})

	if err := recoverStaleWorkflowRuns(ctx, provider.db, provider.runStore, provider.runClaimStore, provider.workflowKeyStore, provider.signalStore, now); err != nil {
		t.Fatalf("recoverStaleWorkflowRuns: %v", err)
	}
	if _, found, err := loadRunClaimRecord(ctx, provider.runClaimStore, run.ID); err != nil {
		t.Fatalf("load run claim: %v", err)
	} else if found {
		t.Fatalf("orphan claim for pending run %q still exists", run.ID)
	}
	reloaded, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{RunId: run.ID})
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if reloaded.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING {
		t.Fatalf("run status = %s, want pending after orphan claim recovery", reloaded.GetStatus())
	}
}

func TestRecoverStaleWorkflowRunsPreservesFreshAndKeyedPendingClaims(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	now := time.Now().UTC()
	cases := []struct {
		name      string
		run       workflowRunRecord
		claimedAt time.Time
	}{
		{
			name: "fresh-unkeyed",
			run: workflowRunRecord{
				ID:          "fresh-claimed-pending-run",
				Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
				Target:      protoBoundTarget(t, "brain", "sources.sync", nil),
				TriggerKind: triggerKindSchedule,
				CreatedAt:   now,
			},
			claimedAt: now.Add(-nonRunningRunClaimGrace / 2),
		},
		{
			name: "keyed",
			run: workflowRunRecord{
				ID:          "keyed-claimed-pending-run",
				Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
				Target:      protoAgentTarget("managed", "gpt-5.5", "Respond"),
				TriggerKind: triggerKindManual,
				WorkflowKey: "slack:T123:C123:1700000000.000001",
				CreatedAt:   now.Add(-2 * time.Minute),
			},
			claimedAt: now.Add(-nonRunningRunClaimGrace - time.Second),
		},
	}
	for _, tc := range cases {
		if err := provider.runStore.Put(ctx, tc.run.toRecord()); err != nil {
			t.Fatalf("%s: Put(run): %v", tc.name, err)
		}
		putRunClaim(t, ctx, provider.runClaimStore, workflowRunClaimRecord{
			ID:        tc.run.ID,
			RunID:     tc.run.ID,
			OwnerID:   "active-provider",
			ClaimedAt: tc.claimedAt,
			ExpiresAt: now.Add(time.Hour),
		})
	}

	if err := recoverStaleWorkflowRuns(ctx, provider.db, provider.runStore, provider.runClaimStore, provider.workflowKeyStore, provider.signalStore, now); err != nil {
		t.Fatalf("recoverStaleWorkflowRuns: %v", err)
	}
	for _, tc := range cases {
		if _, found, err := loadRunClaimRecord(ctx, provider.runClaimStore, tc.run.ID); err != nil {
			t.Fatalf("%s: load run claim: %v", tc.name, err)
		} else if !found {
			t.Fatalf("%s: claim for pending run %q was deleted", tc.name, tc.run.ID)
		}
	}
}

func TestRecoverStaleWorkflowRunsFailsExpiredAgentRunWithUnexpiredClaim(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	now := time.Date(2026, time.May, 3, 16, 0, 0, 0, time.UTC)
	startedAt := now.Add(-(defaultAgentRunTimeout + agentRunStaleGrace + time.Second))
	run := workflowRunRecord{
		ID:          "expired-agent-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING,
		Target:      protoAgentTarget("managed", "claude-opus-4-7", "Investigate CI"),
		TriggerKind: triggerKindManual,
		CreatedAt:   startedAt.Add(-time.Second),
		StartedAt:   &startedAt,
	}
	if err := provider.runStore.Put(ctx, run.toRecord()); err != nil {
		t.Fatalf("Put(run): %v", err)
	}
	putRunClaim(t, ctx, provider.runClaimStore, workflowRunClaimRecord{
		ID:        run.ID,
		RunID:     run.ID,
		OwnerID:   "interrupted-provider",
		ClaimedAt: startedAt,
		ExpiresAt: now.Add(time.Hour),
	})

	if err := recoverStaleWorkflowRuns(ctx, provider.db, provider.runStore, provider.runClaimStore, provider.workflowKeyStore, provider.signalStore, now); err != nil {
		t.Fatalf("recoverStaleWorkflowRuns: %v", err)
	}
	reloaded, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{RunId: run.ID})
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if reloaded.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED || reloaded.GetStatusMessage() != staleRunStatusMessage {
		t.Fatalf("run status = %s message=%q, want stale failure", reloaded.GetStatus(), reloaded.GetStatusMessage())
	}
	if _, found, err := loadRunClaimRecord(ctx, provider.runClaimStore, run.ID); err != nil {
		t.Fatalf("loadRunClaimRecord: %v", err)
	} else if found {
		t.Fatalf("claim for %q still exists after stale recovery", run.ID)
	}
}

func TestRecoverStaleWorkflowRunsPreservesAgentRunWithinConfiguredTimeout(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	now := time.Date(2026, time.May, 3, 16, 0, 0, 0, time.UTC)
	startedAt := now.Add(-10 * time.Minute)
	target := protoAgentTarget("managed", "claude-opus-4-7", "Investigate CI")
	target.GetAgent().TimeoutSeconds = int32((30 * time.Minute).Seconds())
	run := workflowRunRecord{
		ID:          "long-timeout-agent-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING,
		Target:      target,
		TriggerKind: triggerKindManual,
		CreatedAt:   startedAt.Add(-time.Second),
		StartedAt:   &startedAt,
	}
	if err := provider.runStore.Put(ctx, run.toRecord()); err != nil {
		t.Fatalf("Put(run): %v", err)
	}
	putRunClaim(t, ctx, provider.runClaimStore, workflowRunClaimRecord{
		ID:        run.ID,
		RunID:     run.ID,
		OwnerID:   "active-provider",
		ClaimedAt: startedAt,
		ExpiresAt: now.Add(time.Hour),
	})

	if err := recoverStaleWorkflowRuns(ctx, provider.db, provider.runStore, provider.runClaimStore, provider.workflowKeyStore, provider.signalStore, now); err != nil {
		t.Fatalf("recoverStaleWorkflowRuns: %v", err)
	}
	reloaded, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{RunId: run.ID})
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if reloaded.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING {
		t.Fatalf("run status = %s, want running", reloaded.GetStatus())
	}
	if _, found, err := loadRunClaimRecord(ctx, provider.runClaimStore, run.ID); err != nil {
		t.Fatalf("loadRunClaimRecord: %v", err)
	} else if !found {
		t.Fatalf("claim for %q was deleted inside configured timeout", run.ID)
	}
}

func TestDeleteInactiveRunClaimSkipsReplacedClaim(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	now := time.Now().UTC()
	run := workflowRunRecord{
		ID:          "replaced-claim-pending-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:      protoBoundTarget(t, "brain", "sources.sync", nil),
		TriggerKind: triggerKindSchedule,
		CreatedAt:   now.Add(-2 * time.Minute),
	}
	if err := provider.runStore.Put(ctx, run.toRecord()); err != nil {
		t.Fatalf("Put(run): %v", err)
	}
	observedClaim := workflowRunClaimRecord{
		ID:        run.ID,
		RunID:     run.ID,
		OwnerID:   "interrupted-provider",
		ClaimedAt: now.Add(-nonRunningRunClaimGrace - time.Second),
		ExpiresAt: now.Add(time.Hour),
	}
	putRunClaim(t, ctx, provider.runClaimStore, observedClaim)
	replacement := workflowRunClaimRecord{
		ID:        run.ID,
		RunID:     run.ID,
		OwnerID:   "active-provider",
		ClaimedAt: now,
		ExpiresAt: now.Add(time.Hour),
	}
	putRunClaim(t, ctx, provider.runClaimStore, replacement)

	deleted, err := deleteInactiveRunClaimIfRecoverable(ctx, provider.db, run, observedClaim, now)
	if err != nil {
		t.Fatalf("deleteInactiveRunClaimIfRecoverable: %v", err)
	}
	if deleted {
		t.Fatal("deleteInactiveRunClaimIfRecoverable deleted replacement claim")
	}
	current, found, err := loadRunClaimRecord(ctx, provider.runClaimStore, run.ID)
	if err != nil {
		t.Fatalf("load run claim: %v", err)
	}
	if !found || !sameRunClaim(current, replacement) {
		t.Fatalf("current claim = %#v found=%v, want replacement %#v", current, found, replacement)
	}
}

func TestProviderTickProcessesPreferredRunBeforeStaleRecovery(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, time.May, 3, 13, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := New()
	provider.now = clock.Now
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	startedAt := start.Add(-time.Minute)
	staleRun := workflowRunRecord{
		ID:          "expired-running-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING,
		Target:      protoBoundTarget(t, "roadmap", "sync", nil),
		TriggerKind: triggerKindManual,
		CreatedAt:   startedAt.Add(-time.Second),
		StartedAt:   &startedAt,
	}
	if err := provider.runStore.Put(ctx, staleRun.toRecord()); err != nil {
		t.Fatalf("Put(stale run): %v", err)
	}
	putExpiredRunClaim(t, ctx, provider.runClaimStore, staleRun.ID, startedAt)

	preferredRun := workflowRunRecord{
		ID:          "preferred-webhook-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:      protoBoundTarget(t, "github", "failed-check-run-comment", nil),
		TriggerKind: triggerKindEvent,
		CreatedAt:   start,
	}
	if err := provider.runStore.Put(ctx, preferredRun.toRecord()); err != nil {
		t.Fatalf("Put(preferred run): %v", err)
	}

	if err := provider.tick(ctx, preferredRun.ID); err != nil {
		t.Fatalf("tick(preferred): %v", err)
	}
	call, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall: %v", err)
	}
	if call.GetRunId() != preferredRun.ID {
		t.Fatalf("run_id = %q, want preferred run %q", call.GetRunId(), preferredRun.ID)
	}
	reloadedStale, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{RunId: staleRun.ID})
	if err != nil {
		t.Fatalf("GetRun(stale): %v", err)
	}
	if reloadedStale.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING {
		t.Fatalf("stale run status = %s, want running until a fallback stale-recovery tick", reloadedStale.GetStatus())
	}
}

func TestProviderTickPrioritizesPluginEventWhenPreferredWakeLost(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, time.May, 3, 17, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := New()
	provider.now = clock.Now
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	for i := 0; i < 3; i++ {
		run := workflowRunRecord{
			ID:          fmt.Sprintf("old-agent-backlog-%d", i),
			Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
			Target:      protoAgentTarget("managed", "gpt-5.5", "Process backlog"),
			TriggerKind: triggerKindManual,
			CreatedAt:   start.Add(-time.Hour).Add(time.Duration(i) * time.Second),
		}
		if err := provider.runStore.Put(ctx, run.toRecord()); err != nil {
			t.Fatalf("Put(%s): %v", run.ID, err)
		}
	}

	const triggerID = "slack-message-ingest"
	if _, err := provider.UpsertEventTrigger(ctx, &proto.UpsertWorkflowProviderEventTriggerRequest{
		TriggerId: triggerID,
		Match:     &proto.WorkflowEventMatch{Type: "message", Source: "slack"},
		Target:    protoBoundTarget(t, "brain", "sources.slack.events.ingest", map[string]any{"source": "slack"}),
	}); err != nil {
		t.Fatalf("UpsertEventTrigger: %v", err)
	}
	clock.Set(start.Add(time.Minute))
	if _, err := provider.PublishEvent(ctx, &proto.PublishWorkflowProviderEventRequest{
		Event: &proto.WorkflowEvent{
			Id:          "evt-lost-wake",
			Source:      "slack",
			Type:        "message",
			SpecVersion: "1.0",
			Data:        mustStruct(t, map[string]any{"channel": "C123"}),
		},
	}); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}

	if err := provider.tick(ctx, ""); err != nil {
		t.Fatalf("tick: %v", err)
	}
	call, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall: %v", err)
	}
	wantRunID := eventRunID(triggerID, "slack", "evt-lost-wake")
	if call.GetRunId() != wantRunID {
		t.Fatalf("run_id = %q, want plugin event run %q", call.GetRunId(), wantRunID)
	}
}

func TestWorkflowRunDispatchPriorityUsesAgentMetadataHint(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		want  int
	}{
		{name: "number", value: 5, want: 5},
		{name: "string", value: "6", want: 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := protoAgentTarget("managed", "gpt-5.5", "Respond to interactive workflow")
			target.GetAgent().Metadata = mustStruct(t, map[string]any{
				gestaltInputKey: map[string]any{
					workflowMetadataKey: map[string]any{
						dispatchPriorityMetadataKey: tc.value,
					},
				},
			})
			run := workflowRunRecord{
				ID:          "interactive-agent-run",
				Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
				Target:      target,
				TriggerKind: triggerKindManual,
				WorkflowKey: "github:127579767:valon-technologies/toolshed:1290:policy:github-thread-work",
				CreatedAt:   time.Now().UTC(),
			}
			if got := workflowRunDispatchPriority(run); got != tc.want {
				t.Fatalf("workflowRunDispatchPriority = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestWorkflowRunDispatchPriorityIgnoresInvalidAgentMetadataHint(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
	}{
		{name: "zero", value: 0},
		{name: "negative", value: -1},
		{name: "fractional", value: 1.5},
		{name: "non-numeric string", value: "interactive"},
		{name: "bool", value: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := protoAgentTarget("managed", "gpt-5.5", "Respond to interactive workflow")
			target.GetAgent().Metadata = mustStruct(t, map[string]any{
				gestaltInputKey: map[string]any{
					workflowMetadataKey: map[string]any{
						dispatchPriorityMetadataKey: tc.value,
					},
				},
			})
			run := workflowRunRecord{
				ID:          "keyed-agent-run",
				Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
				Target:      target,
				TriggerKind: triggerKindManual,
				WorkflowKey: "github:127579767:valon-technologies/toolshed:1290:policy:github-thread-work",
				CreatedAt:   time.Now().UTC(),
			}
			if got := workflowRunDispatchPriority(run); got != 10 {
				t.Fatalf("workflowRunDispatchPriority = %d, want keyed fallback priority 10", got)
			}
		})
	}
}

func TestProviderTickPreservesFIFOWithinDispatchPriority(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, time.May, 3, 17, 30, 0, 0, time.UTC)
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	oldAgent := workflowRunRecord{
		ID:          "older-agent-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:      protoAgentTarget("managed", "gpt-5.5", "Process backlog"),
		TriggerKind: triggerKindManual,
		CreatedAt:   start.Add(-time.Hour),
	}
	firstEvent := workflowRunRecord{
		ID:          "first-plugin-event",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:      protoBoundTarget(t, "brain", "sources.slack.events.ingest", nil),
		TriggerKind: triggerKindEvent,
		CreatedAt:   start,
	}
	secondEvent := firstEvent
	secondEvent.ID = "second-plugin-event"
	secondEvent.CreatedAt = start.Add(time.Second)
	for _, run := range []workflowRunRecord{oldAgent, secondEvent, firstEvent} {
		if err := provider.runStore.Put(ctx, run.toRecord()); err != nil {
			t.Fatalf("Put(%s): %v", run.ID, err)
		}
	}

	if err := provider.tick(ctx, ""); err != nil {
		t.Fatalf("tick(first): %v", err)
	}
	firstCall, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall(first): %v", err)
	}
	if firstCall.GetRunId() != firstEvent.ID {
		t.Fatalf("first run_id = %q, want %q", firstCall.GetRunId(), firstEvent.ID)
	}

	if err := provider.tick(ctx, ""); err != nil {
		t.Fatalf("tick(second): %v", err)
	}
	secondCall, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall(second): %v", err)
	}
	if secondCall.GetRunId() != secondEvent.ID {
		t.Fatalf("second run_id = %q, want %q", secondCall.GetRunId(), secondEvent.ID)
	}
}

func TestProviderTickPreferredWakeDoesNotBypassDispatchPriority(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, time.May, 3, 18, 0, 0, 0, time.UTC)
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	preferred := workflowRunRecord{
		ID:          "preferred-agent-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:      protoAgentTarget("managed", "gpt-5.5", "Respond to explicit wake"),
		TriggerKind: triggerKindManual,
		CreatedAt:   start.Add(-time.Minute),
	}
	pluginEvent := workflowRunRecord{
		ID:          "higher-priority-plugin-event",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:      protoBoundTarget(t, "brain", "sources.slack.events.ingest", nil),
		TriggerKind: triggerKindEvent,
		CreatedAt:   start,
	}
	for _, run := range []workflowRunRecord{pluginEvent, preferred} {
		if err := provider.runStore.Put(ctx, run.toRecord()); err != nil {
			t.Fatalf("Put(%s): %v", run.ID, err)
		}
	}

	if err := provider.tick(ctx, preferred.ID); err != nil {
		t.Fatalf("tick(preferred): %v", err)
	}
	call, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall: %v", err)
	}
	if call.GetRunId() != pluginEvent.ID {
		t.Fatalf("run_id = %q, want higher-priority run %q", call.GetRunId(), pluginEvent.ID)
	}
}

func TestProviderTickPreferredWakePreservesFIFOWithinDispatchPriority(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, time.May, 3, 18, 15, 0, 0, time.UTC)
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	oldWorkflowKey := "github:127579767:valon-technologies/toolshed:1289:policy:github-thread-work"
	oldRun := workflowRunRecord{
		ID:          "older-github-agent-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:      protoAgentTarget("managed", "gpt-5.5", "Review older GitHub work"),
		TriggerKind: triggerKindManual,
		WorkflowKey: oldWorkflowKey,
		CreatedAt:   start.Add(-time.Minute),
	}
	newWorkflowKey := "github:127579767:valon-technologies/toolshed:1290:policy:github-thread-work"
	newRun := workflowRunRecord{
		ID:          "newer-github-agent-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:      protoAgentTarget("managed", "gpt-5.5", "Review GitHub"),
		TriggerKind: triggerKindManual,
		WorkflowKey: newWorkflowKey,
		CreatedAt:   start,
	}
	for _, run := range []workflowRunRecord{newRun, oldRun} {
		if err := provider.runStore.Put(ctx, run.toRecord()); err != nil {
			t.Fatalf("Put(%s): %v", run.ID, err)
		}
	}
	for _, tc := range []struct {
		runID       string
		workflowKey string
		signalID    string
		createdAt   time.Time
	}{
		{runID: oldRun.ID, workflowKey: oldWorkflowKey, signalID: "older-slack-signal", createdAt: oldRun.CreatedAt},
		{runID: newRun.ID, workflowKey: newWorkflowKey, signalID: "newer-github-signal", createdAt: newRun.CreatedAt},
	} {
		signal := protoWorkflowSignal(t, tc.signalID, tc.signalID+"-idem", tc.signalID)
		if err := provider.signalStore.Put(ctx, workflowSignalRecord{
			ID:             signal.GetId(),
			RunID:          tc.runID,
			WorkflowKey:    tc.workflowKey,
			State:          signalStatePending,
			Signal:         signal,
			IdempotencyKey: signal.GetIdempotencyKey(),
			Sequence:       1,
			CreatedAt:      tc.createdAt,
		}.toRecord()); err != nil {
			t.Fatalf("Put(signal %s): %v", tc.signalID, err)
		}
	}

	if err := provider.tick(ctx, newRun.ID); err != nil {
		t.Fatalf("tick(preferred): %v", err)
	}
	call, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall: %v", err)
	}
	if call.GetRunId() != oldRun.ID {
		t.Fatalf("run_id = %q, want older same-priority run %q", call.GetRunId(), oldRun.ID)
	}
}

func TestProviderTickPrioritizesKeyedSignalContinuation(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, time.May, 3, 18, 30, 0, 0, time.UTC)
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	oldAgent := workflowRunRecord{
		ID:          "old-unkeyed-agent-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:      protoAgentTarget("managed", "gpt-5.5", "Process backlog"),
		TriggerKind: triggerKindManual,
		CreatedAt:   start.Add(-time.Hour),
	}
	workflowKey := "slack:T123:C123:1700000000.000001"
	continuation := workflowRunRecord{
		ID:           "terminal-keyed-agent-run",
		Status:       proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED,
		Target:       protoAgentTarget("managed", "gpt-5.5", "Respond to Slack thread"),
		TriggerKind:  triggerKindManual,
		WorkflowKey:  workflowKey,
		CreatedAt:    start,
		StartedAt:    timePtr(start.Add(time.Second)),
		CompletedAt:  timePtr(start.Add(2 * time.Second)),
		ExecutionRef: "agent-ref",
	}
	for _, run := range []workflowRunRecord{oldAgent, continuation} {
		if err := provider.runStore.Put(ctx, run.toRecord()); err != nil {
			t.Fatalf("Put(%s): %v", run.ID, err)
		}
	}
	signal := protoWorkflowSignal(t, "signal-keyed-continuation", "evt-keyed-continuation", "new Slack reply")
	if err := provider.signalStore.Put(ctx, workflowSignalRecord{
		ID:             signal.GetId(),
		RunID:          continuation.ID,
		WorkflowKey:    workflowKey,
		State:          signalStatePending,
		Signal:         signal,
		IdempotencyKey: signal.GetIdempotencyKey(),
		Sequence:       1,
		CreatedAt:      start.Add(3 * time.Second),
	}.toRecord()); err != nil {
		t.Fatalf("Put(signal): %v", err)
	}

	if err := provider.tick(ctx, ""); err != nil {
		t.Fatalf("tick: %v", err)
	}
	call, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall: %v", err)
	}
	if call.GetRunId() != continuation.ID {
		t.Fatalf("run_id = %q, want keyed continuation %q", call.GetRunId(), continuation.ID)
	}
	if len(call.GetSignals()) != 1 || call.GetSignals()[0].GetId() != signal.GetId() {
		t.Fatalf("signals = %#v, want keyed continuation signal %q", call.GetSignals(), signal.GetId())
	}
}

func TestProviderRunClaimTTLConfigAppliesToClaimAndRenewal(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, time.May, 3, 19, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	host := newWorkflowHostStub(202, `{"ok":true}`)
	host.releaseCh = make(chan struct{})
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := New()
	provider.now = clock.Now
	if err := provider.Configure(ctx, "indexeddb", map[string]any{
		"pollInterval":       "1h",
		"runClaimTTL":        "30s",
		"runClaimRenewEvery": "10s",
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	run := workflowRunRecord{
		ID:          "configured-ttl-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:      protoBoundTarget(t, "brain", "sources.slack.events.ingest", nil),
		TriggerKind: triggerKindEvent,
		CreatedAt:   start,
	}
	if err := provider.runStore.Put(ctx, run.toRecord()); err != nil {
		t.Fatalf("Put(run): %v", err)
	}

	done := make(chan error, 1)
	go func() {
		processed, err := provider.processNextPendingRun(ctx, "")
		if err == nil && !processed {
			err = errors.New("processNextPendingRun processed no run")
		}
		done <- err
	}()
	if _, err := host.waitForCall(time.Second); err != nil {
		t.Fatalf("waitForCall: %v", err)
	}
	claim, found, err := loadRunClaimRecord(ctx, provider.runClaimStore, run.ID)
	if err != nil {
		t.Fatalf("loadRunClaimRecord(initial): %v", err)
	}
	if !found {
		t.Fatalf("claim for %q not found", run.ID)
	}
	if want := start.Add(30 * time.Second); !claim.ExpiresAt.Equal(want) {
		t.Fatalf("initial expires_at = %v, want %v", claim.ExpiresAt, want)
	}

	clock.Set(start.Add(10 * time.Second))
	renewed, err := provider.renewWorkflowRunClaim(ctx, run.ID, provider.claimOwnerID)
	if err != nil {
		t.Fatalf("renewWorkflowRunClaim: %v", err)
	}
	if !renewed {
		t.Fatal("renewWorkflowRunClaim returned false")
	}
	claim, found, err = loadRunClaimRecord(ctx, provider.runClaimStore, run.ID)
	if err != nil {
		t.Fatalf("loadRunClaimRecord(renewed): %v", err)
	}
	if !found {
		t.Fatalf("claim for %q not found after renewal", run.ID)
	}
	if want := start.Add(40 * time.Second); !claim.ExpiresAt.Equal(want) {
		t.Fatalf("renewed expires_at = %v, want %v", claim.ExpiresAt, want)
	}

	close(host.releaseCh)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("processNextPendingRun: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("processNextPendingRun did not finish after host release")
	}
}

func TestProviderTickRecoversRunningRunAfterClaimExpires(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, time.April, 30, 12, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	provider.now = clock.Now
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	startedAt := start.Add(-time.Minute)
	run := workflowRunRecord{
		ID:          "fresh-claimed-running-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING,
		Target:      protoBoundTarget(t, "roadmap", "sync", nil),
		TriggerKind: triggerKindManual,
		CreatedAt:   startedAt.Add(-time.Second),
		StartedAt:   &startedAt,
	}
	if err := provider.runStore.Put(ctx, run.toRecord()); err != nil {
		t.Fatalf("Put(run): %v", err)
	}
	putRunClaim(t, ctx, provider.runClaimStore, workflowRunClaimRecord{
		ID:        run.ID,
		RunID:     run.ID,
		OwnerID:   "other-provider",
		ClaimedAt: start.Add(-time.Second),
		ExpiresAt: start.Add(time.Minute),
	})

	if err := provider.tick(ctx, ""); err != nil {
		t.Fatalf("tick(fresh claim): %v", err)
	}
	reloaded, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{RunId: run.ID})
	if err != nil {
		t.Fatalf("GetRun(fresh): %v", err)
	}
	if reloaded.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING {
		t.Fatalf("fresh status = %s, want running", reloaded.GetStatus())
	}

	clock.Set(start.Add(time.Minute + time.Second))
	if err := provider.tick(ctx, ""); err != nil {
		t.Fatalf("tick(expired claim): %v", err)
	}
	recovered, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{RunId: run.ID})
	if err != nil {
		t.Fatalf("GetRun(recovered): %v", err)
	}
	if recovered.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED {
		t.Fatalf("expired status = %s, want failed", recovered.GetStatus())
	}
	if _, found, err := loadRunClaimRecord(ctx, provider.runClaimStore, run.ID); err != nil {
		t.Fatalf("loadRunClaimRecord: %v", err)
	} else if found {
		t.Fatalf("expired claim for %q still exists", run.ID)
	}
}

func TestProviderCompleteRunDoesNotOverwriteLostClaim(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	startedAt := time.Now().UTC().Add(-time.Minute)
	run := workflowRunRecord{
		ID:            "lost-claim-run",
		Status:        proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED,
		StatusMessage: staleRunStatusMessage,
		Target:        protoBoundTarget(t, "roadmap", "sync", nil),
		TriggerKind:   triggerKindManual,
		CreatedAt:     startedAt.Add(-time.Second),
		StartedAt:     &startedAt,
		CompletedAt:   timePtr(startedAt.Add(time.Second)),
	}
	if err := provider.runStore.Put(ctx, run.toRecord()); err != nil {
		t.Fatalf("Put(run): %v", err)
	}

	pending := run
	pending.Status = proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING
	pending.CompletedAt = nil
	pending.StatusMessage = ""
	if err := provider.completeRunAfterInvoke(ctx, pending, nil, provider.claimOwnerID, &proto.InvokeWorkflowOperationResponse{Status: 202, Body: `{"ok":true}`}, nil); err != nil {
		t.Fatalf("completeRunAfterInvoke: %v", err)
	}
	reloaded, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{RunId: run.ID})
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if reloaded.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED || reloaded.GetStatusMessage() != staleRunStatusMessage {
		t.Fatalf("run after lost-claim completion = status:%s message:%q, want stale failure", reloaded.GetStatus(), reloaded.GetStatusMessage())
	}
}

func TestProviderStartDoesNotBlockOnStaleRunRecoveryFailure(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	if err := provider.runStore.Put(ctx, gestalt.Record{
		"id":          "malformed-run",
		"status":      int64(proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING),
		"target_json": "{",
		"created_at":  time.Now().UTC(),
	}); err != nil {
		t.Fatalf("Put(malformed-run): %v", err)
	}
	startedAt := time.Now().UTC().Add(-time.Minute)
	if err := provider.runStore.Put(ctx, workflowRunRecord{
		ID:          "recoverable-stale-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING,
		Target:      protoBoundTarget(t, "roadmap", "sync", nil),
		TriggerKind: triggerKindManual,
		CreatedAt:   startedAt.Add(-time.Second),
		StartedAt:   &startedAt,
	}.toRecord()); err != nil {
		t.Fatalf("Put(recoverable-stale-run): %v", err)
	}
	putExpiredRunClaim(t, ctx, provider.runClaimStore, "recoverable-stale-run", startedAt)

	if err := provider.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitForCondition(t, time.Second, func() bool {
		recovered, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{RunId: "recoverable-stale-run"})
		if err != nil {
			t.Fatalf("GetRun(recoverable): %v", err)
		}
		return recovered.GetStatus() == proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED &&
			recovered.GetStatusMessage() == staleRunStatusMessage
	})
}

type workflowHostStub struct {
	proto.UnimplementedWorkflowHostServer

	mu        sync.Mutex
	callsCh   chan *proto.InvokeWorkflowOperationRequest
	callsLog  []*proto.InvokeWorkflowOperationRequest
	releaseCh chan struct{}
	errs      []error
	status    int32
	body      string
}

func newWorkflowHostStub(status int32, body string) *workflowHostStub {
	return &workflowHostStub{
		callsCh: make(chan *proto.InvokeWorkflowOperationRequest, 16),
		status:  status,
		body:    body,
	}
}

func putExpiredRunClaim(t *testing.T, ctx context.Context, store *gestalt.ObjectStoreClient, runID string, claimedAt time.Time) {
	t.Helper()
	putRunClaim(t, ctx, store, workflowRunClaimRecord{
		ID:        runID,
		RunID:     runID,
		OwnerID:   "old-provider",
		ClaimedAt: claimedAt,
		ExpiresAt: claimedAt.Add(time.Second),
	})
}

func putRunClaim(t *testing.T, ctx context.Context, store *gestalt.ObjectStoreClient, claim workflowRunClaimRecord) {
	t.Helper()
	if err := store.Put(ctx, claim.toRecord()); err != nil {
		t.Fatalf("Put(%s claim): %v", claim.ID, err)
	}
}

func (s *workflowHostStub) InvokeOperation(ctx context.Context, req *proto.InvokeWorkflowOperationRequest) (*proto.InvokeWorkflowOperationResponse, error) {
	cloned := gproto.Clone(req).(*proto.InvokeWorkflowOperationRequest)
	s.mu.Lock()
	releaseCh := s.releaseCh
	s.callsLog = append(s.callsLog, cloned)
	callIndex := len(s.callsLog) - 1
	var callErr error
	if callIndex < len(s.errs) {
		callErr = s.errs[callIndex]
	}
	s.mu.Unlock()
	s.callsCh <- cloned
	if releaseCh != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-releaseCh:
		}
	}
	if callErr != nil {
		return nil, callErr
	}
	return &proto.InvokeWorkflowOperationResponse{Status: s.status, Body: s.body}, nil
}

func (s *workflowHostStub) waitForCall(timeout time.Duration) (*proto.InvokeWorkflowOperationRequest, error) {
	select {
	case call := <-s.callsCh:
		return call, nil
	case <-time.After(timeout):
		return nil, context.DeadlineExceeded
	}
}

func (s *workflowHostStub) calls() []*proto.InvokeWorkflowOperationRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*proto.InvokeWorkflowOperationRequest(nil), s.callsLog...)
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{now: now.UTC()}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Set(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now.UTC()
}

func startTestIndexedDBBackend(t *testing.T) {
	t.Helper()
	startTestIndexedDBBackendWithWrapper(t, nil)
}

func startTestIndexedDBBackendWithWrapper(t *testing.T, wrap func(proto.IndexedDBServer) proto.IndexedDBServer) {
	t.Helper()
	socketPath := newSocketPath(t, "indexeddb.sock")
	store := relationaldb.New()
	if err := store.Configure(context.Background(), "workflow_state", map[string]any{
		"dsn": "file:" + filepath.Join(t.TempDir(), "workflow.sqlite") + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)",
	}); err != nil {
		t.Fatalf("relationaldb.Configure: %v", err)
	}
	seedWorkflowObjectStores(t, store)
	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("Listen(indexeddb): %v", err)
	}
	indexedDBServer := proto.IndexedDBServer(store.Store)
	if wrap != nil {
		indexedDBServer = wrap(indexedDBServer)
	}
	server := grpc.NewServer()
	proto.RegisterIndexedDBServer(server, indexedDBServer)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() {
		server.GracefulStop()
		_ = lis.Close()
		_ = os.Remove(socketPath)
		_ = store.Close()
	})
	t.Setenv(gestalt.EnvIndexedDBSocket, socketPath)
}

func seedWorkflowObjectStores(t *testing.T, store *relationaldb.Provider) {
	t.Helper()
	for _, def := range []struct {
		name   string
		schema gestalt.ObjectStoreSchema
	}{
		{name: storeSchedules, schema: gestalt.ObjectStoreSchema{}},
		{name: storeEventTriggers, schema: gestalt.ObjectStoreSchema{}},
		{name: storeIdempotency, schema: gestalt.ObjectStoreSchema{}},
		{name: storeWorkflowKeys, schema: gestalt.ObjectStoreSchema{}},
		{name: storeRuns, schema: gestalt.ObjectStoreSchema{}},
		{name: storeRunClaims, schema: workflowRunClaimSchema()},
		{name: storeExecutionRefs, schema: workflowExecutionReferenceSchema()},
		{name: storeSignals, schema: workflowSignalSchema()},
	} {
		if err := store.CreateObjectStore(context.Background(), def.name, def.schema); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
			t.Fatalf("CreateObjectStore(%s): %v", def.name, err)
		}
	}
}

func workflowRunClaimSchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true},
			{Name: "run_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "owner_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "claimed_at", Type: gestalt.TypeTime},
			{Name: "expires_at", Type: gestalt.TypeTime},
		},
	}
}

func workflowSignalSchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Indexes: []gestalt.IndexSchema{
			{Name: "by_run", KeyPath: []string{"run_id"}},
			{Name: "by_run_state", KeyPath: []string{"run_id", "state"}},
			{Name: "by_run_sequence", KeyPath: []string{"run_id", "sequence"}, Unique: true},
		},
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true},
			{Name: "run_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "workflow_key", Type: gestalt.TypeString},
			{Name: "state", Type: gestalt.TypeString, NotNull: true},
			{Name: "signal_json", Type: gestalt.TypeString},
			{Name: "idempotency_key", Type: gestalt.TypeString},
			{Name: "sequence", Type: gestalt.TypeInt},
			{Name: "started_run", Type: gestalt.TypeBool},
			{Name: "batch_id", Type: gestalt.TypeString},
			{Name: "created_at", Type: gestalt.TypeTime},
			{Name: "claimed_at", Type: gestalt.TypeTime},
			{Name: "delivered_at", Type: gestalt.TypeTime},
			{Name: "failed_at", Type: gestalt.TypeTime},
			{Name: "status_message", Type: gestalt.TypeString},
		},
	}
}

func workflowExecutionReferenceSchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Indexes: []gestalt.IndexSchema{
			{Name: "by_subject", KeyPath: []string{"subject_id"}},
		},
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true},
			{Name: "provider_name", Type: gestalt.TypeString, NotNull: true},
			{Name: "target_json", Type: gestalt.TypeString},
			{Name: "subject_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "subject_kind", Type: gestalt.TypeString},
			{Name: "display_name", Type: gestalt.TypeString},
			{Name: "auth_source", Type: gestalt.TypeString},
			{Name: "credential_subject_id", Type: gestalt.TypeString},
			{Name: "run_as_json", Type: gestalt.TypeString},
			{Name: "permissions_json", Type: gestalt.TypeString},
			{Name: "caller_plugin_name", Type: gestalt.TypeString},
			{Name: "created_at", Type: gestalt.TypeTime},
			{Name: "revoked_at", Type: gestalt.TypeTime},
		},
	}
}

type indexedDBServerSpy struct {
	proto.IndexedDBServer
	failUnscopedSignalGetAll bool
	missingSignalIndex       string
	mu                       sync.Mutex
	getCounts                map[string]int
}

type blockingGetAllServer struct {
	proto.IndexedDBServer
	store   string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingGetAllServer) GetAll(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.RecordsResponse, error) {
	if req.GetStore() == s.store {
		block := false
		s.once.Do(func() {
			close(s.entered)
			block = true
		})
		if block {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-s.release:
			}
		}
	}
	return s.IndexedDBServer.GetAll(ctx, req)
}

func (s *indexedDBServerSpy) Get(ctx context.Context, req *proto.ObjectStoreRequest) (*proto.RecordResponse, error) {
	s.mu.Lock()
	if s.getCounts == nil {
		s.getCounts = make(map[string]int)
	}
	s.getCounts[req.GetStore()]++
	s.mu.Unlock()
	return s.IndexedDBServer.Get(ctx, req)
}

func (s *indexedDBServerSpy) GetAll(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.RecordsResponse, error) {
	if s.failUnscopedSignalGetAll && req.GetStore() == storeSignals && req.GetRange() == nil {
		return nil, status.Error(codes.Internal, "unexpected unscoped workflow_signals GetAll")
	}
	return s.IndexedDBServer.GetAll(ctx, req)
}

func (s *indexedDBServerSpy) resetOperationCounts() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getCounts = make(map[string]int)
}

func (s *indexedDBServerSpy) getCount(store string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getCounts[store]
}

func (s *indexedDBServerSpy) IndexCount(ctx context.Context, req *proto.IndexQueryRequest) (*proto.CountResponse, error) {
	if req.GetStore() == storeSignals && req.GetIndex() == s.missingSignalIndex {
		return nil, status.Errorf(codes.NotFound, "index not found: %s", req.GetIndex())
	}
	return s.IndexedDBServer.IndexCount(ctx, req)
}

func startTestWorkflowHost(t *testing.T, host proto.WorkflowHostServer) {
	t.Helper()
	socketPath := newSocketPath(t, "workflow-host.sock")
	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("Listen(workflow host): %v", err)
	}
	server := grpc.NewServer()
	proto.RegisterWorkflowHostServer(server, host)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() {
		server.GracefulStop()
		_ = lis.Close()
		_ = os.Remove(socketPath)
	})
	t.Setenv(gestalt.EnvWorkflowHostSocket, socketPath)
}

func protoBoundTarget(t *testing.T, pluginName, operation string, input map[string]any) *proto.BoundWorkflowTarget {
	t.Helper()
	return &proto.BoundWorkflowTarget{
		Kind: &proto.BoundWorkflowTarget_Plugin{
			Plugin: &proto.BoundWorkflowPluginTarget{
				PluginName: pluginName,
				Operation:  operation,
				Input:      mustStruct(t, input),
			},
		},
	}
}

func protoAgentTarget(providerName, model, prompt string) *proto.BoundWorkflowTarget {
	return protoAgentTargetFromMessage(&proto.BoundWorkflowAgentTarget{
		ProviderName: providerName,
		Model:        model,
		Prompt:       prompt,
		ToolRefs: []*proto.AgentToolRef{
			{Plugin: "slack", Operation: "chat.postMessage"},
			{Plugin: "linear"},
		},
	})
}

func protoAgentTargetWithOutputDelivery(providerName, model, prompt string) *proto.BoundWorkflowTarget {
	target := protoAgentTarget(providerName, model, prompt)
	target.GetAgent().OutputDelivery = &proto.WorkflowOutputDelivery{
		Target: &proto.BoundWorkflowPluginTarget{
			PluginName: "slack",
			Operation:  "events.reply",
		},
		CredentialMode: "none",
		InputBindings: []*proto.WorkflowOutputBinding{
			{
				InputField: "text",
				Value: &proto.WorkflowOutputValueSource{
					Kind: &proto.WorkflowOutputValueSource_AgentOutput{AgentOutput: "text"},
				},
			},
			{
				InputField: "reply_ref",
				Value: &proto.WorkflowOutputValueSource{
					Kind: &proto.WorkflowOutputValueSource_SignalPayload{SignalPayload: "reply_ref"},
				},
			},
		},
	}
	return target
}

func protoAgentTargetWithDeliveries(providerName, model, prompt string) *proto.BoundWorkflowTarget {
	target := protoAgentTargetWithOutputDelivery(providerName, model, prompt)
	target.GetAgent().SessionReadyDelivery = protoSessionReadyDelivery()
	return target
}

func protoSessionReadyDelivery() *proto.WorkflowOutputDelivery {
	return &proto.WorkflowOutputDelivery{
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
			{
				InputField: "reply_ref",
				Value: &proto.WorkflowOutputValueSource{
					Kind: &proto.WorkflowOutputValueSource_SignalPayload{SignalPayload: "reply_ref"},
				},
			},
		},
	}
}

func protoAgentTargetFromMessage(agent *proto.BoundWorkflowAgentTarget) *proto.BoundWorkflowTarget {
	return &proto.BoundWorkflowTarget{
		Kind: &proto.BoundWorkflowTarget_Agent{
			Agent: agent,
		},
	}
}

func protoWorkflowSignal(t *testing.T, id, idempotencyKey, text string) *proto.WorkflowSignal {
	t.Helper()
	return &proto.WorkflowSignal{
		Id:             id,
		Name:           "slack.message",
		IdempotencyKey: idempotencyKey,
		Payload:        mustStruct(t, map[string]any{"text": text}),
	}
}

func mustStruct(t *testing.T, value map[string]any) *structpb.Struct {
	t.Helper()
	if len(value) == 0 {
		return nil
	}
	pb, err := structpb.NewStruct(value)
	if err != nil {
		t.Fatalf("structpb.NewStruct(%#v): %v", value, err)
	}
	return pb
}

func assertRecordHasTargetJSON(t *testing.T, record gestalt.Record) {
	t.Helper()
	if strings.TrimSpace(fmt.Sprint(record["target_json"])) == "" {
		t.Fatalf("record missing target_json: %#v", record)
	}
}

func assertRecordDoesNotContainFields(t *testing.T, record gestalt.Record, fields ...string) {
	t.Helper()
	for _, field := range fields {
		if _, ok := record[field]; ok {
			t.Fatalf("record contains removed field %q: %#v", field, record)
		}
	}
}

func newSocketPath(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join("/tmp", fmt.Sprintf("gestalt-%d-%d-%s", os.Getpid(), time.Now().UnixNano(), name))
}

func waitForCondition(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not satisfied before timeout")
}

func startProviderWorker(t *testing.T, provider *Provider) {
	t.Helper()
	if err := provider.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

func stopProviderWorker(t *testing.T, provider *Provider) {
	t.Helper()
	if provider == nil {
		return
	}
	provider.mu.Lock()
	cancel := provider.pollCancel
	done := provider.pollDone
	provider.pollCancel = nil
	provider.pollDone = nil
	provider.wake = nil
	provider.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for provider worker to stop")
		}
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	if code != 0 && errors.Is(context.DeadlineExceeded, context.DeadlineExceeded) {
	}
	os.Exit(code)
}
