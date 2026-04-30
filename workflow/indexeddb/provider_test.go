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
	"google.golang.org/protobuf/encoding/protowire"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestBoundWorkflowTargetWireShapeIsNestedOnly(t *testing.T) {
	fields := (&proto.BoundWorkflowTarget{}).ProtoReflect().Descriptor().Fields()
	got := make([]string, 0, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		got = append(got, string(fields.Get(i).Name()))
	}
	if len(got) != 2 || got[0] != "plugin" || got[1] != "agent" {
		t.Fatalf("BoundWorkflowTarget fields = %#v, want [plugin agent]", got)
	}
	for _, number := range []protoreflect.FieldNumber{1, 2, 3, 4, 5} {
		if field := fields.ByNumber(number); field != nil {
			t.Fatalf("BoundWorkflowTarget field %d = %s, want reserved retired flat slot", number, field.Name())
		}
	}
}

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
	assertRecordOmitsFields(t, rawIDRecord, "plugin_name")

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
	first, err := provider.SignalOrStartRun(ctx, &proto.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  "slack:T123:C123:1700000000.000001",
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

	second, err := provider.SignalOrStartRun(ctx, &proto.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  "slack:T123:C123:1700000000.000001",
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
		ID:          "signal-run-concurrent",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:      protoAgentTarget("managed", "gpt-5.5", "Respond in the GitHub thread"),
		TriggerKind: triggerKindManual,
		CreatedAt:   now,
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

func TestProviderSignalRunBackfillsSequenceCounterForLegacyRun(t *testing.T) {
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
		ID:          "legacy-signal-run",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Target:      protoAgentTarget("managed", "gpt-5.5", "Respond in the GitHub thread"),
		TriggerKind: triggerKindManual,
		CreatedAt:   now,
	}
	if err := provider.runStore.Add(ctx, run.toRecord()); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	legacySignal := protoWorkflowSignal(t, "legacy-signal-7", "legacy-event-7", "legacy event")
	legacySignal.Sequence = 7
	legacySignal.CreatedAt = timestamppb.New(now)
	if err := provider.signalStore.Add(ctx, workflowSignalRecord{
		ID:             legacySignal.GetId(),
		RunID:          run.ID,
		State:          signalStateDelivered,
		Signal:         legacySignal,
		IdempotencyKey: legacySignal.GetIdempotencyKey(),
		Sequence:       legacySignal.GetSequence(),
		CreatedAt:      now,
	}.toRecord()); err != nil {
		t.Fatalf("seed signal: %v", err)
	}

	explicit := protoWorkflowSignal(t, "explicit-signal-3", "legacy-event-3", "explicit event")
	explicit.Sequence = 3
	explicitResp, err := provider.SignalRun(ctx, &proto.SignalWorkflowProviderRunRequest{
		RunId:  run.ID,
		Signal: explicit,
	})
	if err != nil {
		t.Fatalf("SignalRun(explicit): %v", err)
	}
	if got := explicitResp.GetSignal().GetSequence(); got != 3 {
		t.Fatalf("explicit signal sequence = %d, want 3", got)
	}
	stored, found, err := loadRunRecord(ctx, provider.runStore, "", run.ID)
	if err != nil {
		t.Fatalf("loadRunRecord(after explicit): %v", err)
	}
	if !found {
		t.Fatalf("run %q not found after explicit", run.ID)
	}
	if got := stored.NextSignalSequence; got != 8 {
		t.Fatalf("next_signal_sequence after explicit = %d, want 8", got)
	}

	resp, err := provider.SignalRun(ctx, &proto.SignalWorkflowProviderRunRequest{
		RunId:  run.ID,
		Signal: protoWorkflowSignal(t, "", "legacy-event-8", "new event"),
	})
	if err != nil {
		t.Fatalf("SignalRun: %v", err)
	}
	if got := resp.GetSignal().GetSequence(); got != 8 {
		t.Fatalf("signal sequence = %d, want 8", got)
	}
	stored, found, err = loadRunRecord(ctx, provider.runStore, "", run.ID)
	if err != nil {
		t.Fatalf("loadRunRecord: %v", err)
	}
	if !found {
		t.Fatalf("run %q not found", run.ID)
	}
	if got := stored.NextSignalSequence; got != 9 {
		t.Fatalf("next_signal_sequence = %d, want 9", got)
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

	preferred, err := provider.SignalOrStartRun(ctx, &proto.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  "slack:T123:C123:1700000000.000001",
		Target:       protoAgentTarget("managed", "gpt-5.5", "Respond in the Slack thread"),
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
		t.Fatalf("preferred call signals = %#v, want Slack signal", call.GetSignals())
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

func TestProviderConfigureFailsExistingSignalStoreMissingIndexesWithoutMigration(t *testing.T) {
	ctx := context.Background()
	var spy *indexedDBServerSpy
	startTestIndexedDBBackendWithWrapper(t, func(inner proto.IndexedDBServer) proto.IndexedDBServer {
		spy = &indexedDBServerSpy{IndexedDBServer: inner}
		return spy
	})
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	adminConn, admin, err := dialIndexedDBAdmin()
	if err != nil {
		t.Fatalf("dialIndexedDBAdmin: %v", err)
	}
	defer adminConn.Close()
	if err := createWorkflowStore(ctx, admin, storeSignals, &proto.ObjectStoreSchema{}); err != nil {
		t.Fatalf("precreate workflow_signals: %v", err)
	}
	precreateCount := spy.createObjectStoreCount(storeSignals)

	provider := New()
	err = provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"})
	if err == nil {
		t.Fatal("Configure succeeded, want missing signal index error")
	}
	if !strings.Contains(err.Error(), "by_run") {
		t.Fatalf("Configure error = %v, want signal index validation failure", err)
	}
	if got := spy.createObjectStoreCount(storeSignals); got != precreateCount {
		t.Fatalf("workflow_signals CreateObjectStore calls = %d, want %d", got, precreateCount)
	}
}

func TestProviderConfigureFailsExistingExecutionRefsMissingSubjectIndexWithoutMigration(t *testing.T) {
	ctx := context.Background()
	var spy *indexedDBServerSpy
	startTestIndexedDBBackendWithWrapper(t, func(inner proto.IndexedDBServer) proto.IndexedDBServer {
		spy = &indexedDBServerSpy{IndexedDBServer: inner}
		return spy
	})
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	adminConn, admin, err := dialIndexedDBAdmin()
	if err != nil {
		t.Fatalf("dialIndexedDBAdmin: %v", err)
	}
	defer adminConn.Close()
	if err := createWorkflowStore(ctx, admin, storeExecutionRefs, &proto.ObjectStoreSchema{}); err != nil {
		t.Fatalf("precreate execution_refs: %v", err)
	}
	precreateCount := spy.createObjectStoreCount(storeExecutionRefs)

	provider := New()
	err = provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"})
	if err == nil {
		t.Fatal("Configure succeeded, want missing execution ref index error")
	}
	if !strings.Contains(err.Error(), "by_subject") {
		t.Fatalf("Configure error = %v, want by_subject validation failure", err)
	}
	if got := spy.createObjectStoreCount(storeExecutionRefs); got != precreateCount {
		t.Fatalf("execution_refs CreateObjectStore calls = %d, want %d", got, precreateCount)
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
	updated, err := provider.PutExecutionReference(ctx, &proto.PutWorkflowExecutionReferenceRequest{
		Reference: &proto.WorkflowExecutionReference{
			Id:                  "ref-1",
			Target:              protoBoundTarget(t, "roadmap", "sync", nil),
			SubjectId:           "user:123",
			CredentialSubjectId: "svc:workflow",
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
			Target:    protoAgentTargetWithOutputDelivery("managed", "gpt-5.4", "send a Slack reminder"),
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
	assertRecordOmitsFields(t, scheduleRecord, "plugin_name", "operation", "connection", "instance", "input")

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
	assertRecordOmitsFields(t, triggerRecord, "plugin_name", "operation", "connection", "instance", "input")

	run, err := provider.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{Target: target})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	runRecord, err := provider.runStore.Get(ctx, run.GetId())
	if err != nil {
		t.Fatalf("raw run get: %v", err)
	}
	assertRecordHasTargetJSON(t, runRecord)
	assertRecordOmitsFields(t, runRecord, "plugin_name", "operation", "connection", "instance", "input")

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
	assertRecordOmitsFields(t, refRecord, "target_plugin", "target_operation", "target_connection", "target_instance", "target_fingerprint")
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
		SubjectId:   "workload:github_app_installation:127579767:repo:valon-technologies/gestalt",
		SubjectKind: "workload",
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
	if ref.GetSubjectId() != publishedBy.GetSubjectId() || ref.GetSubjectKind() != "workload" || ref.GetAuthSource() != "github_app_webhook" {
		t.Fatalf("execution ref subject = (%q, %q, %q), want published GitHub workload", ref.GetSubjectId(), ref.GetSubjectKind(), ref.GetAuthSource())
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
		SubjectId:   "workload:github_app_installation:127579767:repo:valon-technologies/other",
		SubjectKind: "workload",
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
		Target:    protoAgentTargetWithOutputDelivery("managed", "gpt-5.4", "respond to the GitHub event"),
	}); err != nil {
		t.Fatalf("UpsertEventTrigger(agent): %v", err)
	}

	publishedBy := &proto.WorkflowActor{
		SubjectId:   "workload:github_app_installation:127579767:repo:valon-technologies/gestalt",
		SubjectKind: "workload",
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

func TestProviderIgnoresReservedTargetUnknownFields(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	flatOnly := &proto.BoundWorkflowTarget{}
	appendReservedUnknownStringField(flatOnly, 1, "roadmap")
	appendReservedUnknownStringField(flatOnly, 2, "sync")
	if _, err := provider.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{Target: flatOnly}); err == nil {
		t.Fatal("StartRun(flat unknown fields) succeeded, want error")
	} else if !strings.Contains(err.Error(), "target.plugin.plugin_name is required") {
		t.Fatalf("StartRun(flat unknown fields) error = %v, want nested plugin validation", err)
	}

	agentTarget := protoAgentTarget("managed", "gpt-5.5", "send a Slack reminder")
	appendReservedUnknownStringField(agentTarget, 1, "roadmap")
	appendReservedUnknownStringField(agentTarget, 2, "sync")
	schedule, err := provider.UpsertSchedule(ctx, &proto.UpsertWorkflowProviderScheduleRequest{
		ScheduleId: "reserved-unknown-agent",
		Cron:       "* * * * *",
		Timezone:   "UTC",
		Target:     agentTarget,
	})
	if err != nil {
		t.Fatalf("UpsertSchedule(agent with reserved unknown fields): %v", err)
	}
	if schedule.GetTarget().GetAgent().GetProviderName() != "managed" {
		t.Fatalf("schedule target = %#v", schedule.GetTarget())
	}
	if len(schedule.GetTarget().ProtoReflect().GetUnknown()) != 0 {
		t.Fatalf("schedule target retained reserved unknown fields")
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
			name: "flat-shaped target json",
			put: func() error {
				return provider.runStore.Put(ctx, gestalt.Record{
					"id":           "flat-json-run",
					"status":       int64(proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING),
					"target_json":  `{"pluginName":"roadmap","operation":"sync"}`,
					"trigger_kind": triggerKindManual,
					"created_at":   now,
				})
			},
			read: func() error {
				_, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{RunId: "flat-json-run"})
				return err
			},
			want: "target_json must contain plugin or agent target",
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
	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("Listen(indexeddb): %v", err)
	}
	indexedDBServer := proto.IndexedDBServer(store)
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

type indexedDBServerSpy struct {
	proto.IndexedDBServer
	failUnscopedSignalGetAll bool
	missingSignalIndex       string
	mu                       sync.Mutex
	createObjectStores       map[string]int
}

func (s *indexedDBServerSpy) CreateObjectStore(ctx context.Context, req *proto.CreateObjectStoreRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	if s.createObjectStores == nil {
		s.createObjectStores = make(map[string]int)
	}
	s.createObjectStores[req.GetName()]++
	s.mu.Unlock()
	return s.IndexedDBServer.CreateObjectStore(ctx, req)
}

func (s *indexedDBServerSpy) createObjectStoreCount(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.createObjectStores[name]
}

func (s *indexedDBServerSpy) GetAll(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.RecordsResponse, error) {
	if s.failUnscopedSignalGetAll && req.GetStore() == storeSignals && req.GetRange() == nil {
		return nil, status.Error(codes.Internal, "unexpected unscoped workflow_signals GetAll")
	}
	return s.IndexedDBServer.GetAll(ctx, req)
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

func appendReservedUnknownStringField(target *proto.BoundWorkflowTarget, number protoreflect.FieldNumber, value string) {
	message := target.ProtoReflect()
	raw := append([]byte(nil), message.GetUnknown()...)
	raw = protowire.AppendTag(raw, protowire.Number(number), protowire.BytesType)
	raw = protowire.AppendString(raw, value)
	message.SetUnknown(raw)
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

func assertRecordOmitsFields(t *testing.T, record gestalt.Record, fields ...string) {
	t.Helper()
	for _, field := range fields {
		if _, ok := record[field]; ok {
			t.Fatalf("record contains legacy field %q: %#v", field, record)
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
