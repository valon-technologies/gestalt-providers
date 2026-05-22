package indexeddb

import (
	"context"
	"encoding/json"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestProviderStartRunUsesIdempotencyAndExecutesHostCallbacks(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "10ms"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	startProviderWorker(t, provider)
	t.Cleanup(func() { _ = provider.Close() })

	first, err := provider.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		IdempotencyKey: "manual-sync",
		Target:         workflowTarget(t, "roadmap", "sync", map[string]any{"mode": "full"}),
		CreatedBy:      &gestalt.WorkflowActor{SubjectID: "user:123", SubjectKind: "user", DisplayName: "Ada"},
	})
	if err != nil {
		t.Fatalf("StartRun(first): %v", err)
	}
	second, err := provider.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		IdempotencyKey: "manual-sync",
		Target:         workflowTarget(t, "roadmap", "sync", map[string]any{"mode": "full"}),
	})
	if err != nil {
		t.Fatalf("StartRun(second): %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("idempotent run ids = %q and %q, want equal", first.ID, second.ID)
	}

	call, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall: %v", err)
	}
	plugin := call.Target.Plugin
	if plugin.PluginName != "roadmap" || plugin.Operation != "sync" {
		t.Fatalf("target = %#v", call.Target)
	}
	if got := anyMap(plugin.Input)["mode"]; got != "full" {
		t.Fatalf("target.input.mode = %v, want full", got)
	}
	if call.CreatedBy.SubjectID != "user:123" {
		t.Fatalf("created_by.subject_id = %q, want user:123", call.CreatedBy.SubjectID)
	}

	waitForCondition(t, time.Second, func() bool {
		run, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{
			RunID: first.ID,
		})
		return err == nil && run.Status == gestalt.WorkflowRunStatusValueSucceeded
	})

	runs, err := provider.ListRuns(ctx, &gestalt.ListWorkflowProviderRunsRequest{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs.Runs) != 1 {
		t.Fatalf("runs len = %d, want 1", len(runs.Runs))
	}
	if len(host.calls()) != 1 {
		t.Fatalf("host calls = %d, want 1", len(host.calls()))
	}
}

func TestProviderDefinitionCRUD(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(200, "ok"))

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	created, err := provider.CreateDefinition(ctx, &gestalt.CreateWorkflowProviderDefinitionRequest{
		IdempotencyKey: "definition-sync",
		Target:         workflowTarget(t, "roadmap", "sync", map[string]any{"mode": "full"}),
	})
	if err != nil {
		t.Fatalf("CreateDefinition: %v", err)
	}
	if created.ID == "" || created.ProviderName != "indexeddb" {
		t.Fatalf("created definition = %#v, want id and provider", created)
	}

	again, err := provider.CreateDefinition(ctx, &gestalt.CreateWorkflowProviderDefinitionRequest{
		IdempotencyKey: "definition-sync",
		Target:         workflowTarget(t, "roadmap", "sync", map[string]any{"mode": "full"}),
	})
	if err != nil {
		t.Fatalf("CreateDefinition(idempotent): %v", err)
	}
	if again.ID != created.ID {
		t.Fatalf("idempotent definition ids = %q and %q, want equal", created.ID, again.ID)
	}
	conflicting, err := provider.CreateDefinition(ctx, &gestalt.CreateWorkflowProviderDefinitionRequest{
		IdempotencyKey: "definition-sync",
		Target:         workflowTarget(t, "roadmap", "conflicting", nil),
	})
	if err != nil {
		t.Fatalf("CreateDefinition(conflicting idempotent target): %v", err)
	}
	if conflicting.ID != created.ID || conflicting.Target.Plugin.Operation != "sync" {
		t.Fatalf("conflicting idempotent definition = %#v, want original sync definition", conflicting)
	}
	record, found, err := loadDefinitionRecord(ctx, provider.definitionStore, created.ID)
	if err != nil || !found {
		t.Fatalf("loadDefinitionRecord found=%v err=%v", found, err)
	}
	record.CreatedBy = &gestalt.WorkflowActor{SubjectID: "creator-1", SubjectKind: "user"}
	if err := provider.definitionStore.Put(ctx, record.toRecord()); err != nil {
		t.Fatalf("store definition creator: %v", err)
	}

	updated, err := provider.UpdateDefinition(ctx, &gestalt.UpdateWorkflowProviderDefinitionRequest{
		DefinitionID: created.ID,
		Target:       workflowTarget(t, "roadmap", "refresh", map[string]any{"mode": "delta"}),
	})
	if err != nil {
		t.Fatalf("UpdateDefinition: %v", err)
	}
	if updated.ID != created.ID || updated.CreatedAt != created.CreatedAt {
		t.Fatalf("updated definition = %#v, want same id and created_at", updated)
	}
	if updated.Target.Plugin.Operation != "refresh" {
		t.Fatalf("updated operation = %q, want refresh", updated.Target.Plugin.Operation)
	}
	if updated.CreatedBy == nil || updated.CreatedBy.SubjectID != "creator-1" {
		t.Fatalf("updated created_by = %#v, want creator-1", updated.CreatedBy)
	}

	got, err := provider.GetDefinition(ctx, &gestalt.GetWorkflowProviderDefinitionRequest{DefinitionID: created.ID})
	if err != nil {
		t.Fatalf("GetDefinition: %v", err)
	}
	if got.Target.Plugin.Operation != "refresh" {
		t.Fatalf("stored operation = %q, want refresh", got.Target.Plugin.Operation)
	}
	if got.CreatedBy == nil || got.CreatedBy.SubjectID != "creator-1" {
		t.Fatalf("stored created_by = %#v, want creator-1", got.CreatedBy)
	}

	if err := provider.DeleteDefinition(ctx, &gestalt.DeleteWorkflowProviderDefinitionRequest{DefinitionID: created.ID}); err != nil {
		t.Fatalf("DeleteDefinition: %v", err)
	}
	if _, err := provider.GetDefinition(ctx, &gestalt.GetWorkflowProviderDefinitionRequest{DefinitionID: created.ID}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetDefinition after delete error = %v, want NotFound", err)
	}
}

func TestProviderStartControlsPollLoopLifecycle(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "10ms"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	pending, err := provider.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		IdempotencyKey: "pending-before-start",
		Target:         workflowTarget(t, "roadmap", "sync", map[string]any{"mode": "pending"}),
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
	if firstCall.RunID != pending.ID {
		t.Fatalf("first call run_id = %q, want %q", firstCall.RunID, pending.ID)
	}

	second, err := provider.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		IdempotencyKey: "after-start-context-cancel",
		Target:         workflowTarget(t, "roadmap", "sync", map[string]any{"mode": "after-cancel"}),
	})
	if err != nil {
		t.Fatalf("StartRun(after Start context cancel): %v", err)
	}
	secondCall, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall(second): %v", err)
	}
	if secondCall.RunID != second.ID {
		t.Fatalf("second call run_id = %q, want %q", secondCall.RunID, second.ID)
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

	provider := newProviderCore()
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

	run, err := provider.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		IdempotencyKey: "start-after-configure",
		Target:         workflowTarget(t, "roadmap", "sync", map[string]any{"mode": "compat"}),
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
	if call.RunID != run.ID {
		t.Fatalf("run_id = %q, want %q", call.RunID, run.ID)
	}
}

func TestProviderStartRunRepairsMissingIdempotencyRecord(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "10ms"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	startProviderWorker(t, provider)
	t.Cleanup(func() { _ = provider.Close() })

	now := time.Now().UTC()
	runID := idempotentManualRunID("roadmap", "manual-sync")
	run := workflowRunRecord{
		ID:          runID,
		Status:      gestalt.WorkflowRunStatusValuePending,
		Target:      workflowTarget(t, "roadmap", "sync", map[string]any{"mode": "full"}),
		TriggerKind: triggerKindManual,
		CreatedAt:   now,
	}
	if err := provider.runStore.Put(ctx, run.toRecord()); err != nil {
		t.Fatalf("Put(run): %v", err)
	}

	first, err := provider.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		IdempotencyKey: "manual-sync",
		Target:         workflowTarget(t, "roadmap", "sync", map[string]any{"mode": "full"}),
	})
	if err != nil {
		t.Fatalf("StartRun(first): %v", err)
	}
	second, err := provider.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		IdempotencyKey: "manual-sync",
		Target:         workflowTarget(t, "roadmap", "sync", map[string]any{"mode": "full"}),
	})
	if err != nil {
		t.Fatalf("StartRun(second): %v", err)
	}
	if first.ID != runID || second.ID != runID {
		t.Fatalf("returned run ids = %q and %q, want %q", first.ID, second.ID, runID)
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
	if call.RunID != runID {
		t.Fatalf("run_id = %q, want %q", call.RunID, runID)
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

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "10ms"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	startProviderWorker(t, provider)
	t.Cleanup(func() { _ = provider.Close() })

	target := workflowAgentTarget("managed", "gpt-5.5", "Respond in the Slack thread")
	changedTarget := workflowAgentTarget("managed", "gpt-5.5-latest", "Updated prompt")
	workflowKey := "slack:T123:C123:1700000000.000001"
	first, err := provider.SignalOrStartRun(ctx, &gestalt.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  workflowKey,
		Target:       target,
		ExecutionRef: "agent-ref",
		Signal:       workflowSignal(t, "", "evt-1", "first"),
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun(first): %v", err)
	}
	if !first.StartedRun {
		t.Fatalf("first started_run = false, want true")
	}
	if first.Signal.ID == "" {
		t.Fatalf("first signal id is empty")
	}
	if got := first.Signal.Sequence; got != 1 {
		t.Fatalf("first signal sequence = %d, want 1", got)
	}

	firstCall, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall(first): %v", err)
	}
	if firstCall.RunID != first.Run.ID {
		t.Fatalf("first call run_id = %q, want %q", firstCall.RunID, first.Run.ID)
	}
	if firstCall.Target.Agent.ProviderName != "managed" {
		t.Fatalf("first call target = %#v", firstCall.Target)
	}
	if firstCall.ExecutionRef != "agent-ref" {
		t.Fatalf("first call execution_ref = %q, want agent-ref", firstCall.ExecutionRef)
	}
	if len(firstCall.Signals) != 1 || firstCall.Signals[0].IdempotencyKey != "evt-1" {
		t.Fatalf("first call signals = %#v", firstCall.Signals)
	}
	if firstCall.Metadata == nil || anyMap(firstCall.Metadata)["workflow_key"] != workflowKey {
		t.Fatalf("first call metadata = %#v, want workflow_key %q", firstCall.Metadata, workflowKey)
	}

	second, err := provider.SignalOrStartRun(ctx, &gestalt.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  workflowKey,
		Target:       changedTarget,
		ExecutionRef: "ignored-new-ref",
		Signal:       workflowSignal(t, "", "evt-2", "second"),
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun(second): %v", err)
	}
	if second.StartedRun {
		t.Fatalf("second started_run = true, want false")
	}
	if second.Run.ID != first.Run.ID {
		t.Fatalf("second run_id = %q, want %q", second.Run.ID, first.Run.ID)
	}
	if got := second.Signal.Sequence; got != 2 {
		t.Fatalf("second signal sequence = %d, want 2", got)
	}

	close(host.releaseCh)
	secondCall, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall(second): %v", err)
	}
	if secondCall.RunID != first.Run.ID {
		t.Fatalf("second call run_id = %q, want %q", secondCall.RunID, first.Run.ID)
	}
	if secondCall.Target.Agent.Model != "gpt-5.5" {
		t.Fatalf("second call target model = %q, want original model", secondCall.Target.Agent.Model)
	}
	if len(secondCall.Signals) != 1 || secondCall.Signals[0].IdempotencyKey != "evt-2" {
		t.Fatalf("second call signals = %#v", secondCall.Signals)
	}
	if secondCall.ExecutionRef != "agent-ref" {
		t.Fatalf("second call execution_ref = %q, want original agent-ref", secondCall.ExecutionRef)
	}
	if secondCall.Metadata == nil || anyMap(secondCall.Metadata)["workflow_key"] != workflowKey {
		t.Fatalf("second call metadata = %#v, want workflow_key %q", secondCall.Metadata, workflowKey)
	}

	waitForCondition(t, time.Second, func() bool {
		run, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: first.Run.ID})
		return err == nil && run.Status == gestalt.WorkflowRunStatusValueSucceeded
	})

	duplicate, err := provider.SignalOrStartRun(ctx, &gestalt.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  "slack:T123:C123:1700000000.000001",
		Target:       target,
		ExecutionRef: "ignored-duplicate-ref",
		Signal:       workflowSignal(t, "", "evt-2", "second duplicate"),
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun(duplicate): %v", err)
	}
	if duplicate.Run.ID != first.Run.ID || duplicate.StartedRun {
		t.Fatalf("duplicate response = %#v, want same run without start", duplicate)
	}

	third, err := provider.SignalOrStartRun(ctx, &gestalt.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  "slack:T123:C123:1700000000.000001",
		Target:       target,
		ExecutionRef: "agent-ref-new-run",
		Signal:       workflowSignal(t, "sig-3", "evt-3", "third"),
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun(third): %v", err)
	}
	if !third.StartedRun || third.Run.ID == first.Run.ID {
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

			provider := newProviderCore()
			if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "10ms"}); err != nil {
				t.Fatalf("Configure: %v", err)
			}
			startProviderWorker(t, provider)
			t.Cleanup(func() { _ = provider.Close() })

			workflowKey := "slack:T123:C123:1700000000.000001:" + strings.ReplaceAll(tc.name, " ", "-")
			target := workflowAgentTarget("managed", "gpt-5.5", "Respond in the Slack thread")
			first, err := provider.SignalOrStartRun(ctx, &gestalt.SignalOrStartWorkflowProviderRunRequest{
				WorkflowKey:  workflowKey,
				Target:       target,
				ExecutionRef: "agent-ref",
				Signal:       workflowSignal(t, "", "evt-1", "first"),
			})
			if err != nil {
				t.Fatalf("SignalOrStartRun(first): %v", err)
			}
			if _, err := host.waitForCall(time.Second); err != nil {
				t.Fatalf("waitForCall(first): %v", err)
			}

			if _, err := provider.SignalOrStartRun(ctx, &gestalt.SignalOrStartWorkflowProviderRunRequest{
				WorkflowKey:  workflowKey,
				Target:       target,
				ExecutionRef: "ignored-new-ref",
				Signal:       workflowSignal(t, "", "evt-2", "second"),
			}); err != nil {
				t.Fatalf("SignalOrStartRun(second): %v", err)
			}

			close(host.releaseCh)
			waitForCondition(t, time.Second, func() bool {
				run, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: first.Run.ID})
				return err == nil && run.Status == gestalt.WorkflowRunStatusValueFailed
			})

			signals, err := listSignalRecords(ctx, provider.signalStore, first.Run.ID, "")
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

			third, err := provider.SignalOrStartRun(ctx, &gestalt.SignalOrStartWorkflowProviderRunRequest{
				WorkflowKey:  workflowKey,
				Target:       target,
				ExecutionRef: "agent-ref-new-run",
				Signal:       workflowSignal(t, "sig-3", "evt-3", "third"),
			})
			if err != nil {
				t.Fatalf("SignalOrStartRun(third): %v", err)
			}
			if !third.StartedRun || third.Run.ID == first.Run.ID {
				t.Fatalf("third response = %#v, want new run", third)
			}
		})
	}
}

func TestProviderSignalOrStartRunDoesNotScanSignalsForOtherRuns(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackendWithWrapper(t, func(inner gestalt.IndexedDBProvider) gestalt.IndexedDBProvider {
		return &indexedDBServerSpy{IndexedDBProvider: inner, failUnscopedSignalGetAll: true}
	})
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	now := time.Now().UTC()
	largePayload := strings.Repeat("x", 256*1024)
	for i := 0; i < 24; i++ {
		signal := &gestalt.WorkflowSignal{
			ID:             fmt.Sprintf("other-signal-%02d", i),
			Name:           "github.app.webhook",
			Payload:        mustStruct(t, map[string]any{"body": largePayload}),
			CreatedAt:      now,
			IdempotencyKey: fmt.Sprintf("other-event-%02d", i),
			Sequence:       int64(i + 1),
		}
		record := workflowSignalRecord{
			ID:             signal.ID,
			RunID:          "other-run",
			WorkflowKey:    "github:other",
			State:          signalStateDelivered,
			Signal:         signal,
			IdempotencyKey: signal.IdempotencyKey,
			Sequence:       signal.Sequence,
			CreatedAt:      now,
		}
		if err := provider.signalStore.Add(ctx, record.toRecord()); err != nil {
			t.Fatalf("seed large signal %d: %v", i, err)
		}
	}

	resp, err := provider.SignalOrStartRun(ctx, &gestalt.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  "github:127579767:valon-technologies/toolshed:1",
		Target:       workflowAgentTarget("managed", "gpt-5.5", "Respond in the GitHub thread"),
		ExecutionRef: "agent-ref",
		Signal:       workflowSignal(t, "", "new-event", "new"),
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun: %v", err)
	}
	if got := resp.Signal.Sequence; got != 1 {
		t.Fatalf("signal sequence = %d, want 1", got)
	}
}

func TestProviderListRunsDoesNotLoadEachRunByKey(t *testing.T) {
	ctx := context.Background()
	var spy *indexedDBServerSpy
	startTestIndexedDBBackendWithWrapper(t, func(inner gestalt.IndexedDBProvider) gestalt.IndexedDBProvider {
		spy = &indexedDBServerSpy{IndexedDBProvider: inner}
		return spy
	})
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	for i := 0; i < 3; i++ {
		_, err := provider.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
			IdempotencyKey: fmt.Sprintf("list-runs-%d", i),
			Target:         workflowTarget(t, "roadmap", "sync", map[string]any{"index": i}),
			CreatedBy:      &gestalt.WorkflowActor{SubjectID: "user:123", SubjectKind: "user"},
		})
		if err != nil {
			t.Fatalf("StartRun(%d): %v", i, err)
		}
	}

	spy.resetOperationCounts()
	runs, err := provider.ListRuns(ctx, &gestalt.ListWorkflowProviderRunsRequest{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs.Runs) != 3 {
		t.Fatalf("runs len = %d, want 3", len(runs.Runs))
	}
	if got := spy.getCount(storeRuns); got != 0 {
		t.Fatalf("workflow_runs Get count = %d, want 0", got)
	}
}

func TestProviderSignalOrStartRunConcurrentSignalsShareWorkflowKeyRun(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	secondProvider := newProviderCore()
	if err := secondProvider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure(second): %v", err)
	}
	t.Cleanup(func() { _ = secondProvider.Close() })
	providers := []*Provider{provider, secondProvider}

	const signalCount = 12
	type result struct {
		resp *gestalt.SignalWorkflowRunResponse
		err  error
	}
	start := make(chan struct{})
	results := make(chan result, signalCount)
	for i := 0; i < signalCount; i++ {
		i := i
		go func() {
			<-start
			p := providers[i%len(providers)]
			resp, err := p.SignalOrStartRun(ctx, &gestalt.SignalOrStartWorkflowProviderRunRequest{
				WorkflowKey:  "github:127579767:valon-technologies/gestalt:issue_comment:42",
				Target:       workflowAgentTarget("managed", "gpt-5.5", "Respond in the GitHub thread"),
				ExecutionRef: "agent-ref",
				Signal:       workflowSignal(t, "", fmt.Sprintf("github-delivery-%02d", i), fmt.Sprintf("event %02d", i)),
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
		if resp.Run.ID == "" {
			t.Fatalf("response %d run_id is empty", i)
		}
		if runID == "" {
			runID = resp.Run.ID
		}
		if resp.Run.ID != runID {
			t.Fatalf("response %d run_id = %q, want %q", i, resp.Run.ID, runID)
		}
		if resp.StartedRun {
			started++
		}
		sequence := resp.Signal.Sequence
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

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	target := workflowAgentTarget("managed", "gpt-5.5", "Respond in the GitHub thread")
	first, err := provider.SignalOrStartRun(ctx, &gestalt.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  "github:127579767:valon-technologies/gestalt:issue_comment:42",
		Target:       target,
		ExecutionRef: "agent-ref",
		Signal:       workflowSignal(t, "shared-signal-id", "github-delivery-1", "first"),
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun(first): %v", err)
	}
	if first.Run.ID == "" {
		t.Fatalf("first run id is empty")
	}

	_, err = provider.SignalOrStartRun(ctx, &gestalt.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  "github:127579767:valon-technologies/gestalt:issue_comment:43",
		Target:       target,
		ExecutionRef: "agent-ref",
		Signal:       workflowSignal(t, "shared-signal-id", "github-delivery-2", "second"),
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

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	now := time.Now().UTC()
	run := workflowRunRecord{
		ID:          "terminal-keyed-run",
		Status:      gestalt.WorkflowRunStatusValueSucceeded,
		Target:      workflowAgentTarget("managed", "gpt-5.5", "Respond in the GitHub thread"),
		TriggerKind: triggerKindManual,
		CreatedAt:   now.Add(-time.Minute),
		CompletedAt: timePtr(now),
		WorkflowKey: "github:127579767:valon-technologies/gestalt:issue_comment:42",
	}
	if err := provider.runStore.Add(ctx, run.toRecord()); err != nil {
		t.Fatalf("seed terminal run: %v", err)
	}
	signal := workflowSignal(t, "late-signal", "late-delivery", "late")
	record := workflowSignalRecord{
		ID:             signal.ID,
		RunID:          run.ID,
		WorkflowKey:    run.WorkflowKey,
		State:          signalStatePending,
		Signal:         signal,
		IdempotencyKey: signal.IdempotencyKey,
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
	if call.RunID != run.ID || len(call.Signals) != 1 || call.Signals[0].ID != signal.ID {
		t.Fatalf("host call = %#v, want late signal on terminal run", call)
	}
}

func TestProviderProcessNextPendingRunClaimsRunAcrossProviders(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	host.releaseCh = make(chan struct{})
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	first := newProviderCore()
	if err := first.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure(first): %v", err)
	}
	t.Cleanup(func() { _ = first.Close() })
	second := newProviderCore()
	if err := second.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure(second): %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })

	now := time.Now().UTC()
	run := workflowRunRecord{
		ID:          "shared-pending-run",
		Status:      gestalt.WorkflowRunStatusValuePending,
		Target:      workflowTarget(t, "roadmap", "sync", nil),
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
	if call.RunID != run.ID {
		t.Fatalf("run_id = %q, want %q", call.RunID, run.ID)
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

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	now := time.Now().UTC()
	run := workflowRunRecord{
		ID:          "unkeyed-pending-run",
		Status:      gestalt.WorkflowRunStatusValuePending,
		Target:      workflowTarget(t, "brain", "sources.slack.events.ingest", map[string]any{"sourceId": "slack-valon-public"}),
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
	if call.RunID != run.ID {
		t.Fatalf("run_id = %q, want %q", call.RunID, run.ID)
	}
	started, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: run.ID})
	if err != nil {
		t.Fatalf("GetRun(started): %v", err)
	}
	if started.Status != gestalt.WorkflowRunStatusValueRunning || started.StartedAt == nil {
		t.Fatalf("started run status = %v started_at=%v, want running with started_at", started.Status, started.StartedAt)
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
	completed, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: run.ID})
	if err != nil {
		t.Fatalf("GetRun(completed): %v", err)
	}
	if completed.Status != gestalt.WorkflowRunStatusValueSucceeded {
		t.Fatalf("completed run status = %v, want succeeded", completed.Status)
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

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	now := time.Now().UTC()
	first := workflowRunRecord{
		ID:          "fresh-claimed-run",
		Status:      gestalt.WorkflowRunStatusValuePending,
		Target:      workflowTarget(t, "roadmap", "sync", map[string]any{"run": "first"}),
		TriggerKind: triggerKindManual,
		CreatedAt:   now,
	}
	second := workflowRunRecord{
		ID:          "next-runnable-run",
		Status:      gestalt.WorkflowRunStatusValuePending,
		Target:      workflowTarget(t, "roadmap", "sync", map[string]any{"run": "second"}),
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
	if call.RunID != second.ID {
		t.Fatalf("run_id = %q, want %q", call.RunID, second.ID)
	}
	reloadedFirst, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: first.ID})
	if err != nil {
		t.Fatalf("GetRun(first): %v", err)
	}
	if reloadedFirst.Status != gestalt.WorkflowRunStatusValuePending {
		t.Fatalf("first status = %v, want pending", reloadedFirst.Status)
	}
}

func TestProviderSignalOrStartRunReplacesStaleWorkflowKey(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	workflowKey := "github:127579767:valon-technologies/gestalt:issue_comment:42"
	if err := addWorkflowKeyRecord(ctx, provider.workflowKeyStore, workflowKey, "missing-run", time.Now().UTC()); err != nil {
		t.Fatalf("seed workflow key: %v", err)
	}

	resp, err := provider.SignalOrStartRun(ctx, &gestalt.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  workflowKey,
		Target:       workflowAgentTarget("managed", "gpt-5.5", "Respond in the GitHub thread"),
		ExecutionRef: "agent-ref",
		Signal:       workflowSignal(t, "", "github-delivery-1", "first"),
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun: %v", err)
	}
	if !resp.StartedRun || resp.Run.ID == "" || resp.Run.ID == "missing-run" {
		t.Fatalf("response = %#v, want replacement run", resp)
	}
}

func TestProviderSignalOrStartRunRecoversStaleRunningWorkflowKey(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	base := time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC)
	now := base
	provider := newProviderCore()
	provider.now = func() time.Time { return now }
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	workflowKey := "slack:T123:C123:1700000000.000001"
	staleStartedAt := base.Add(-time.Minute)
	now = base.Add(time.Minute)
	if err := provider.runStore.Put(ctx, workflowRunRecord{
		ID:           "stale-running-run",
		Status:       gestalt.WorkflowRunStatusValueRunning,
		WorkflowKey:  workflowKey,
		Target:       workflowAgentTarget("managed", "gpt-5.5", "Respond in the Slack thread"),
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
		Signal:         workflowSignal(t, "old-signal", "old-delivery", "old"),
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

	resp, err := provider.SignalOrStartRun(ctx, &gestalt.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  workflowKey,
		Target:       workflowAgentTarget("managed", "gpt-5.5", "Respond in the Slack thread"),
		ExecutionRef: "fresh-ref",
		Signal:       workflowSignal(t, "old-signal", "old-delivery", "fresh retry"),
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun: %v", err)
	}
	if !resp.StartedRun || resp.Run.ID == "" || resp.Run.ID == "stale-running-run" {
		t.Fatalf("response = %#v, want fresh replacement run", resp)
	}

	stale, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: "stale-running-run"})
	if err != nil {
		t.Fatalf("GetRun(stale): %v", err)
	}
	if stale.Status != gestalt.WorkflowRunStatusValueFailed ||
		stale.StatusMessage != staleRunStatusMessage {
		t.Fatalf("stale run status = %v %q, want failed stale recovery", stale.Status, stale.StatusMessage)
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
	if !found || idempotency.RunID != resp.Run.ID || idempotency.SignalID == "old-signal" {
		t.Fatalf("idempotency = %#v, found %v; want rebound to fresh run/signal", idempotency, found)
	}
}

func TestProviderFinalizingOldTerminalRunDoesNotDeleteNewerWorkflowKey(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	now := time.Now().UTC()
	workflowKey := "github:127579767:valon-technologies/gestalt:issue_comment:42"
	oldRun := workflowRunRecord{
		ID:          "old-terminal-run",
		Status:      gestalt.WorkflowRunStatusValueSucceeded,
		Target:      workflowAgentTarget("managed", "gpt-5.5", "Respond in the GitHub thread"),
		TriggerKind: triggerKindManual,
		CreatedAt:   now.Add(-time.Minute),
		CompletedAt: timePtr(now),
		WorkflowKey: workflowKey,
	}
	newRun := workflowRunRecord{
		ID:          "new-active-run",
		Status:      gestalt.WorkflowRunStatusValuePending,
		Target:      workflowAgentTarget("managed", "gpt-5.5", "Respond in the GitHub thread"),
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
	signal := workflowSignal(t, "late-signal", "late-delivery", "late")
	record := workflowSignalRecord{
		ID:             signal.ID,
		RunID:          oldRun.ID,
		WorkflowKey:    workflowKey,
		State:          signalStatePending,
		Signal:         signal,
		IdempotencyKey: signal.IdempotencyKey,
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

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	secondProvider := newProviderCore()
	if err := secondProvider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure(second): %v", err)
	}
	t.Cleanup(func() { _ = secondProvider.Close() })
	providers := []*Provider{provider, secondProvider}

	now := time.Now().UTC()
	run := workflowRunRecord{
		ID:                 "signal-run-concurrent",
		Status:             gestalt.WorkflowRunStatusValuePending,
		Target:             workflowAgentTarget("managed", "gpt-5.5", "Respond in the GitHub thread"),
		TriggerKind:        triggerKindManual,
		NextSignalSequence: 1,
		CreatedAt:          now,
	}
	if err := provider.runStore.Add(ctx, run.toRecord()); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	const signalCount = 10
	type result struct {
		resp *gestalt.SignalWorkflowRunResponse
		err  error
	}
	start := make(chan struct{})
	results := make(chan result, signalCount)
	for i := 0; i < signalCount; i++ {
		i := i
		go func() {
			<-start
			p := providers[i%len(providers)]
			resp, err := p.SignalRun(ctx, &gestalt.SignalWorkflowProviderRunRequest{
				RunID:  run.ID,
				Signal: workflowSignal(t, "", fmt.Sprintf("signal-run-delivery-%02d", i), fmt.Sprintf("event %02d", i)),
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
		if result.resp.Run.ID != run.ID {
			t.Fatalf("response %d run_id = %q, want %q", i, result.resp.Run.ID, run.ID)
		}
		sequence := result.resp.Signal.Sequence
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

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	now := time.Now().UTC()
	oldRunIDs := map[string]bool{}
	for runIndex := 0; runIndex < defaultWorkerCount; runIndex++ {
		oldRun := workflowRunRecord{
			ID:          fmt.Sprintf("old-hot-run-%d", runIndex),
			Status:      gestalt.WorkflowRunStatusValuePending,
			Target:      workflowAgentTarget("managed", "gpt-5.5", "Respond in the GitHub thread"),
			TriggerKind: triggerKindManual,
			CreatedAt:   now.Add(-time.Hour).Add(time.Duration(runIndex) * time.Second),
			WorkflowKey: fmt.Sprintf("github:127579767:valon-technologies/gestalt:%d", runIndex),
		}
		oldRunIDs[oldRun.ID] = true
		if err := provider.runStore.Add(ctx, oldRun.toRecord()); err != nil {
			t.Fatalf("seed old run %d: %v", runIndex, err)
		}
		for sequence := 1; sequence <= defaultMaxSignalsPerBatch+1; sequence++ {
			signal := workflowSignal(
				t,
				fmt.Sprintf("old-%d-signal-%02d", runIndex, sequence),
				fmt.Sprintf("old-%d-event-%02d", runIndex, sequence),
				fmt.Sprintf("old %d/%d", runIndex, sequence),
			)
			signal.Sequence = int64(sequence)
			record := workflowSignalRecord{
				ID:             signal.ID,
				RunID:          oldRun.ID,
				WorkflowKey:    oldRun.WorkflowKey,
				State:          signalStatePending,
				Signal:         signal,
				IdempotencyKey: signal.IdempotencyKey,
				Sequence:       signal.Sequence,
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
	if !oldRunIDs[call.RunID] {
		t.Fatalf("initial call run_id = %q, want old hot run", call.RunID)
	}
	if got := len(call.Signals); got != defaultMaxSignalsPerBatch {
		t.Fatalf("initial call signal count = %d, want max batch %d", got, defaultMaxSignalsPerBatch)
	}

	interactiveTarget := workflowAgentTarget("managed", "gpt-5.5", "Respond in the Slack thread")
	interactiveTarget.Agent.Metadata = mustStruct(t, map[string]any{
		gestaltInputKey: map[string]any{
			workflowMetadataKey: map[string]any{
				dispatchPriorityMetadataKey: 5,
			},
		},
	})
	preferred, err := provider.SignalOrStartRun(ctx, &gestalt.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:  "slack:T123:C123:1700000000.000001",
		Target:       interactiveTarget,
		ExecutionRef: "agent-ref",
		Signal:       workflowSignal(t, "", "slack-event-1", "new"),
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun(preferred): %v", err)
	}
	call, err = host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall(preferred): %v", err)
	}
	if call.RunID != preferred.Run.ID {
		t.Fatalf("preferred call run_id = %q, want %q", call.RunID, preferred.Run.ID)
	}
	if len(call.Signals) != 1 || call.Signals[0].IdempotencyKey != "slack-event-1" {
		t.Fatalf("preferred call signals = %#v, want preferred signal", call.Signals)
	}

	close(host.releaseCh)
}

func TestProviderConfigureFailsWhenSignalSequenceIndexMissing(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackendWithWrapper(t, func(inner gestalt.IndexedDBProvider) gestalt.IndexedDBProvider {
		return &indexedDBServerSpy{IndexedDBProvider: inner, missingSignalIndex: "by_run_sequence"}
	})
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := newProviderCore()
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

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	stopProviderWorker(t, provider)

	now := time.Now().UTC()
	pending := workflowRunRecord{
		ID:          "pending-run",
		Status:      gestalt.WorkflowRunStatusValuePending,
		Target:      workflowTarget(t, "roadmap", "sync", map[string]any{"kind": "pending"}),
		TriggerKind: triggerKindManual,
		CreatedAt:   now,
	}
	if err := provider.runStore.Put(ctx, pending.toRecord()); err != nil {
		t.Fatalf("Put(pending): %v", err)
	}

	canceled, err := provider.CancelRun(ctx, &gestalt.CancelWorkflowProviderRunRequest{
		RunID:  "pending-run",
		Reason: "skip this run",
	})
	if err != nil {
		t.Fatalf("CancelRun(pending): %v", err)
	}
	if canceled.Status != gestalt.WorkflowRunStatusValueCanceled {
		t.Fatalf("canceled status = %v, want %v", canceled.Status, gestalt.WorkflowRunStatusValueCanceled)
	}

	running := workflowRunRecord{
		ID:          "running-run",
		Status:      gestalt.WorkflowRunStatusValueRunning,
		Target:      workflowTarget(t, "roadmap", "sync", nil),
		TriggerKind: triggerKindManual,
		CreatedAt:   now,
		StartedAt:   timePtr(now),
	}
	if err := provider.runStore.Put(ctx, running.toRecord()); err != nil {
		t.Fatalf("Put(running): %v", err)
	}
	if _, err := provider.CancelRun(ctx, &gestalt.CancelWorkflowProviderRunRequest{
		RunID: "running-run",
	}); err == nil {
		t.Fatal("CancelRun(running) succeeded, want error")
	}
}

func TestProviderExecutionReferencesRoundTripAndListBySubject(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	firstCreatedAt := time.Date(2026, time.April, 16, 12, 0, 0, 0, time.UTC)
	secondCreatedAt := firstCreatedAt.Add(time.Minute)
	first, err := provider.PutExecutionReference(ctx, &gestalt.PutWorkflowExecutionReferenceRequest{
		Reference: &gestalt.WorkflowExecutionReference{
			ID:                  "ref-1",
			Target:              workflowTarget(t, "roadmap", "sync", nil),
			SubjectID:           "user:123",
			CredentialSubjectID: "svc:workflow",
			Permissions: []gestalt.WorkflowAccessPermission{
				{Plugin: "roadmap", Operations: []string{"sync", "preview"}},
			},
			CreatedAt: firstCreatedAt,
		},
	})
	if err != nil {
		t.Fatalf("PutExecutionReference(ref-1): %v", err)
	}
	if first.ProviderName != "indexeddb" {
		t.Fatalf("provider_name = %q, want indexeddb", first.ProviderName)
	}
	if got := first.CreatedAt; !got.Equal(firstCreatedAt) {
		t.Fatalf("created_at = %v, want %v", got, firstCreatedAt)
	}

	revokedAt := secondCreatedAt.Add(time.Minute)
	updatedTarget := workflowTarget(t, "roadmap", "sync", nil)
	updatedTarget.Plugin.CredentialMode = "none"
	updated, err := provider.PutExecutionReference(ctx, &gestalt.PutWorkflowExecutionReferenceRequest{
		Reference: &gestalt.WorkflowExecutionReference{
			ID:                  "ref-1",
			Target:              updatedTarget,
			SubjectID:           "user:123",
			CredentialSubjectID: "svc:workflow",
			RunAs: &gestalt.WorkflowRunAsSubject{
				SubjectID:   "service_account:roadmap-sync",
				SubjectKind: "service_account",
				DisplayName: "Roadmap sync",
				AuthSource:  "config",
			},
			Permissions: []gestalt.WorkflowAccessPermission{
				{Plugin: "roadmap", Operations: []string{"sync"}},
			},
			CreatedAt: secondCreatedAt,
			RevokedAt: &revokedAt,
		},
	})
	if err != nil {
		t.Fatalf("PutExecutionReference(update ref-1): %v", err)
	}
	if got := updated.CreatedAt; !got.Equal(firstCreatedAt) {
		t.Fatalf("updated created_at = %v, want preserved %v", got, firstCreatedAt)
	}
	if got := updated.RevokedAt; !got.Equal(revokedAt) {
		t.Fatalf("updated revoked_at = %v, want %v", got, revokedAt)
	}

	if _, err := provider.PutExecutionReference(ctx, &gestalt.PutWorkflowExecutionReferenceRequest{
		Reference: &gestalt.WorkflowExecutionReference{
			ID:        "ref-2",
			Target:    workflowTarget(t, "roadmap", "sync", nil),
			SubjectID: "user:123",
			CreatedAt: secondCreatedAt,
		},
	}); err != nil {
		t.Fatalf("PutExecutionReference(ref-2): %v", err)
	}
	if _, err := provider.PutExecutionReference(ctx, &gestalt.PutWorkflowExecutionReferenceRequest{
		Reference: &gestalt.WorkflowExecutionReference{
			ID:        "ref-3",
			Target:    workflowTarget(t, "billing", "collect", nil),
			SubjectID: "user:999",
			CreatedAt: secondCreatedAt.Add(time.Minute),
		},
	}); err != nil {
		t.Fatalf("PutExecutionReference(ref-3): %v", err)
	}

	got, err := provider.GetExecutionReference(ctx, &gestalt.GetWorkflowExecutionReferenceRequest{ID: "ref-1"})
	if err != nil {
		t.Fatalf("GetExecutionReference(ref-1): %v", err)
	}
	if got.ProviderName != "indexeddb" {
		t.Fatalf("get provider_name = %q, want indexeddb", got.ProviderName)
	}
	if got.CredentialSubjectID != "svc:workflow" {
		t.Fatalf("credential_subject_id = %q, want svc:workflow", got.CredentialSubjectID)
	}
	if got.RunAs.SubjectID != "service_account:roadmap-sync" || got.RunAs.SubjectKind != "service_account" {
		t.Fatalf("run_as = %#v, want roadmap sync service account", got.RunAs)
	}
	if got.RunAs.DisplayName != "Roadmap sync" || got.RunAs.AuthSource != "config" {
		t.Fatalf("run_as metadata = (%q, %q), want display/auth", got.RunAs.DisplayName, got.RunAs.AuthSource)
	}
	if got.Target.Plugin.CredentialMode != "none" {
		t.Fatalf("target credential mode = %q, want none", got.Target.Plugin.CredentialMode)
	}
	if len(got.Permissions) != 1 || got.Permissions[0].Plugin != "roadmap" {
		t.Fatalf("permissions = %#v, want roadmap entry", got.Permissions)
	}
	if ops := got.Permissions[0].Operations; len(ops) != 1 || ops[0] != "sync" {
		t.Fatalf("permission operations = %#v, want [sync]", ops)
	}

	listed, err := provider.ListExecutionReferences(ctx, &gestalt.ListWorkflowExecutionReferencesRequest{
		SubjectID: "user:123",
	})
	if err != nil {
		t.Fatalf("ListExecutionReferences(subject): %v", err)
	}
	if len(listed.References) != 2 {
		t.Fatalf("subject references len = %d, want 2", len(listed.References))
	}
	if listed.References[0].ID != "ref-1" || listed.References[1].ID != "ref-2" {
		t.Fatalf("subject references ids = [%s %s], want [ref-1 ref-2]", listed.References[0].ID, listed.References[1].ID)
	}

	all, err := provider.ListExecutionReferences(ctx, &gestalt.ListWorkflowExecutionReferencesRequest{})
	if err != nil {
		t.Fatalf("ListExecutionReferences(all): %v", err)
	}
	if len(all.References) != 3 {
		t.Fatalf("all references len = %d, want 3", len(all.References))
	}
}

func TestNormalizeTargetPreservesPluginCredentialMode(t *testing.T) {
	target := workflowTarget(t, " github ", " reviewPullRequest ", nil)
	target.Plugin.CredentialMode = " none "

	scoped, err := normalizeTarget(workflowTargetInput(target))
	if err != nil {
		t.Fatalf("normalizeTarget: %v", err)
	}
	plugin := scoped.Target.Plugin
	if plugin.PluginName != "github" || plugin.Operation != "reviewPullRequest" {
		t.Fatalf("plugin target = %#v", plugin)
	}
	if got := plugin.CredentialMode; got != "none" {
		t.Fatalf("credential mode = %q, want none", got)
	}
}

func TestNormalizeTargetRejectsInvalidPluginCredentialMode(t *testing.T) {
	target := workflowTarget(t, "github", "reviewPullRequest", nil)
	target.Plugin.CredentialMode = "platform"

	_, err := normalizeTarget(workflowTargetInput(target))
	if err == nil || !strings.Contains(err.Error(), `target.plugin.credential_mode "platform" is not supported`) {
		t.Fatalf("normalizeTarget error = %v, want unsupported credential mode", err)
	}
}

func TestNormalizeTargetRejectsOutputDeliveryTargetCredentialMode(t *testing.T) {
	target := workflowAgentTargetWithOutputDelivery("managed", "gpt-5.4", "send a Slack reminder")
	target.Agent.OutputDelivery.Target.CredentialMode = "none"

	_, err := normalizeTarget(workflowTargetInput(target))
	if err == nil || !strings.Contains(err.Error(), `target.agent.output_delivery.target.credential_mode "none" is not supported`) {
		t.Fatalf("normalizeTarget error = %v, want unsupported output delivery target mode", err)
	}
}

func TestNormalizeTargetRejectsSessionReadyDeliveryInvalidSources(t *testing.T) {
	target := workflowAgentTargetWithOutputDelivery("managed", "gpt-5.4", "send a Slack reminder")
	target.Agent.SessionReadyDelivery = workflowSessionReadyDelivery()
	target.Agent.SessionReadyDelivery.Target.CredentialMode = "none"

	_, err := normalizeTarget(workflowTargetInput(target))
	if err == nil || !strings.Contains(err.Error(), `target.agent.session_ready_delivery.target.credential_mode "none" is not supported`) {
		t.Fatalf("normalizeTarget error = %v, want unsupported session ready delivery target mode", err)
	}

	target = workflowAgentTargetWithOutputDelivery("managed", "gpt-5.4", "send a Slack reminder")
	target.Agent.SessionReadyDelivery = workflowSessionReadyDelivery()
	target.Agent.SessionReadyDelivery.InputBindings[0].Value = &gestalt.WorkflowOutputValueSource{
		AgentOutput: "text",
	}

	_, err = normalizeTarget(workflowTargetInput(target))
	if err == nil || !strings.Contains(err.Error(), "target.agent.session_ready_delivery.input_bindings.value.agent_output is not available before the agent turn starts") {
		t.Fatalf("normalizeTarget error = %v, want unsupported session ready delivery agent output", err)
	}
}

func TestProviderExecutionReferenceRoundTripsAgentTarget(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	createdAt := time.Date(2026, time.April, 16, 12, 0, 0, 0, time.UTC)
	ref, err := provider.PutExecutionReference(ctx, &gestalt.PutWorkflowExecutionReferenceRequest{
		Reference: &gestalt.WorkflowExecutionReference{
			ID:        "agent-ref",
			Target:    workflowAgentTargetWithDeliveries("managed", "gpt-5.4", "send a Slack reminder"),
			SubjectID: "user:123",
			Permissions: []gestalt.WorkflowAccessPermission{
				{Plugin: "slack", Operations: []string{"chat.postMessage"}},
			},
			CreatedAt: createdAt,
		},
	})
	if err != nil {
		t.Fatalf("PutExecutionReference(agent-ref): %v", err)
	}
	if ref.ProviderName != "indexeddb" {
		t.Fatalf("provider_name = %q, want indexeddb", ref.ProviderName)
	}
	if ref.Target.Agent.ProviderName != "managed" {
		t.Fatalf("agent target = %#v", ref.Target)
	}
	if ref.Target.Plugin != nil {
		t.Fatalf("agent target included plugin fields: %#v", ref.Target)
	}
	delivery := ref.Target.Agent.OutputDelivery
	if delivery.Target.PluginName != "slack" || delivery.Target.Operation != "events.reply" {
		t.Fatalf("output delivery target = %#v", delivery.Target)
	}
	if delivery.CredentialMode != "none" {
		t.Fatalf("output delivery credential mode = %q, want none", delivery.CredentialMode)
	}
	if len(delivery.InputBindings) != 2 ||
		delivery.InputBindings[0].InputField != "text" ||
		delivery.InputBindings[0].Value.AgentOutput != "text" ||
		delivery.InputBindings[1].InputField != "reply_ref" ||
		delivery.InputBindings[1].Value.SignalPayload != "reply_ref" {
		t.Fatalf("output delivery bindings = %#v", delivery.InputBindings)
	}
	sessionReadyDelivery := ref.Target.Agent.SessionReadyDelivery
	if sessionReadyDelivery.Target.PluginName != "slack" || sessionReadyDelivery.Target.Operation != "events.replySessionStarted" {
		t.Fatalf("session ready delivery target = %#v", sessionReadyDelivery.Target)
	}
	if sessionReadyDelivery.CredentialMode != "none" {
		t.Fatalf("session ready delivery credential mode = %q, want none", sessionReadyDelivery.CredentialMode)
	}
	if len(sessionReadyDelivery.InputBindings) != 2 ||
		sessionReadyDelivery.InputBindings[0].InputField != "session_id" ||
		sessionReadyDelivery.InputBindings[0].Value.AgentSession != "id" ||
		sessionReadyDelivery.InputBindings[1].InputField != "reply_ref" ||
		sessionReadyDelivery.InputBindings[1].Value.SignalPayload != "reply_ref" {
		t.Fatalf("session ready delivery bindings = %#v", sessionReadyDelivery.InputBindings)
	}

	got, err := provider.GetExecutionReference(ctx, &gestalt.GetWorkflowExecutionReferenceRequest{ID: "agent-ref"})
	if err != nil {
		t.Fatalf("GetExecutionReference(agent-ref): %v", err)
	}
	if !workflowValuesEqual(got.Target, ref.Target) {
		t.Fatalf("round-tripped target = %#v, want %#v", got.Target, ref.Target)
	}
}

func TestProviderStoresNestedTargetJSONWithoutScalarCopies(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	stopProviderWorker(t, provider)

	target := workflowTarget(t, "roadmap", "sync", map[string]any{"mode": "full"})
	target.Plugin.Connection = "primary"
	target.Plugin.Instance = "prod"

	schedule, err := provider.UpsertSchedule(ctx, &gestalt.UpsertWorkflowProviderScheduleRequest{
		ScheduleID: "stored-schedule",
		Cron:       "* * * * *",
		Timezone:   "UTC",
		Target:     target,
	})
	if err != nil {
		t.Fatalf("UpsertSchedule: %v", err)
	}
	scheduleRecord, err := provider.scheduleStore.Get(ctx, schedule.ID)
	if err != nil {
		t.Fatalf("raw schedule get: %v", err)
	}
	assertRecordHasTargetJSON(t, scheduleRecord)
	assertRecordDoesNotContainFields(t, scheduleRecord, "plugin_name", "operation", "connection", "instance", "input")

	trigger, err := provider.UpsertEventTrigger(ctx, &gestalt.UpsertWorkflowProviderEventTriggerRequest{
		TriggerID: "stored-trigger",
		Match:     &gestalt.WorkflowEventMatch{Type: "roadmap.updated"},
		Target:    target,
	})
	if err != nil {
		t.Fatalf("UpsertEventTrigger: %v", err)
	}
	triggerRecord, err := provider.eventTriggerStore.Get(ctx, trigger.ID)
	if err != nil {
		t.Fatalf("raw event trigger get: %v", err)
	}
	assertRecordHasTargetJSON(t, triggerRecord)
	assertRecordDoesNotContainFields(t, triggerRecord, "plugin_name", "operation", "connection", "instance", "input")

	run, err := provider.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{Target: target})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	runRecord, err := provider.runStore.Get(ctx, run.ID)
	if err != nil {
		t.Fatalf("raw run get: %v", err)
	}
	assertRecordHasTargetJSON(t, runRecord)
	assertRecordDoesNotContainFields(t, runRecord, "plugin_name", "operation", "connection", "instance", "input")

	ref, err := provider.PutExecutionReference(ctx, &gestalt.PutWorkflowExecutionReferenceRequest{
		Reference: &gestalt.WorkflowExecutionReference{
			ID:        "stored-ref",
			Target:    target,
			SubjectID: "user:123",
		},
	})
	if err != nil {
		t.Fatalf("PutExecutionReference: %v", err)
	}
	refRecord, err := provider.executionRefStore.Get(ctx, ref.ID)
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

	provider := newProviderCore()
	provider.now = clock.Now
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "100ms"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	startProviderWorker(t, provider)
	t.Cleanup(func() { _ = provider.Close() })

	trigger, err := provider.UpsertEventTrigger(ctx, &gestalt.UpsertWorkflowProviderEventTriggerRequest{
		TriggerID:    "refresh-trigger",
		Match:        &gestalt.WorkflowEventMatch{Type: "task.updated", Source: "roadmap"},
		Target:       workflowTarget(t, "roadmap", "sync", map[string]any{"kind": "event"}),
		ExecutionRef: "event-ref",
	})
	if err != nil {
		t.Fatalf("UpsertEventTrigger: %v", err)
	}
	if trigger.ExecutionRef != "event-ref" {
		t.Fatalf("trigger execution_ref = %q, want event-ref", trigger.ExecutionRef)
	}
	requestEvent := &gestalt.WorkflowEvent{
		ID:          "evt-1",
		Source:      "roadmap",
		Type:        "task.updated",
		SpecVersion: "1.0",
		Data:        mustStruct(t, map[string]any{"taskId": "task-1"}),
	}
	published, err := provider.PublishEvent(ctx, &gestalt.PublishWorkflowProviderEventRequest{Event: requestEvent})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	requestEvent.Data.(map[string]any)["taskId"] = "mutated"
	if published.ID != "evt-1" || published.Source != "roadmap" || published.Type != "task.updated" {
		t.Fatalf("published event = %#v, want normalized input event", published)
	}
	if got := published.Data.(map[string]any)["taskId"]; got != "task-1" {
		t.Fatalf("published event data taskId = %v, want isolated task-1", got)
	}
	eventCall, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall(event): %v", err)
	}
	if eventCall.ExecutionRef != "event-ref" {
		t.Fatalf("event execution_ref = %q, want event-ref", eventCall.ExecutionRef)
	}
	if eventCall.Trigger.Event == nil || eventCall.Trigger.Event.TriggerID != "refresh-trigger" {
		t.Fatalf("event trigger = %#v", eventCall.Trigger)
	}

	schedule, err := provider.UpsertSchedule(ctx, &gestalt.UpsertWorkflowProviderScheduleRequest{
		ScheduleID:   "nightly-sync",
		Cron:         "*/5 * * * *",
		Timezone:     "UTC",
		Target:       workflowTarget(t, "roadmap", "sync", map[string]any{"kind": "schedule"}),
		ExecutionRef: "schedule-ref",
	})
	if err != nil {
		t.Fatalf("UpsertSchedule: %v", err)
	}
	if schedule.ExecutionRef != "schedule-ref" {
		t.Fatalf("schedule execution_ref = %q, want schedule-ref", schedule.ExecutionRef)
	}
	if got := schedule.NextRunAt; !got.Equal(time.Date(2026, time.April, 16, 12, 5, 0, 0, time.UTC)) {
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
	if scheduleCall.ExecutionRef != "schedule-ref" {
		t.Fatalf("schedule call execution_ref = %q, want schedule-ref", scheduleCall.ExecutionRef)
	}
	scheduledFor := scheduleCall.Trigger.Schedule.ScheduledFor
	wantScheduledFor := time.Date(2026, time.April, 16, 12, 15, 0, 0, time.UTC)
	if !scheduledFor.Equal(wantScheduledFor) {
		t.Fatalf("scheduled_for = %v, want %v", scheduledFor, wantScheduledFor)
	}

	updated, err := provider.GetSchedule(ctx, &gestalt.GetWorkflowProviderScheduleRequest{
		ScheduleID: "nightly-sync",
	})
	if err != nil {
		t.Fatalf("GetSchedule: %v", err)
	}
	wantNext := time.Date(2026, time.April, 16, 12, 20, 0, 0, time.UTC)
	if got := updated.NextRunAt; !got.Equal(wantNext) {
		t.Fatalf("next_run_at = %v, want %v", got, wantNext)
	}
	if updated.ExecutionRef != "schedule-ref" {
		t.Fatalf("updated schedule execution_ref = %q, want schedule-ref", updated.ExecutionRef)
	}

	waitForCondition(t, time.Second, func() bool {
		runs, err := provider.ListRuns(ctx, &gestalt.ListWorkflowProviderRunsRequest{})
		if err != nil || len(runs.Runs) != 2 {
			return false
		}
		for _, run := range runs.Runs {
			if run.Status != gestalt.WorkflowRunStatusValueSucceeded {
				return false
			}
		}
		return true
	})

	eventRun, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: eventCall.RunID})
	if err != nil {
		t.Fatalf("GetRun(event): %v", err)
	}
	if eventRun.ExecutionRef != "event-ref" {
		t.Fatalf("event run execution_ref = %q, want event-ref", eventRun.ExecutionRef)
	}

	scheduleRun, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: scheduleCall.RunID})
	if err != nil {
		t.Fatalf("GetRun(schedule): %v", err)
	}
	if scheduleRun.ExecutionRef != "schedule-ref" {
		t.Fatalf("schedule run execution_ref = %q, want schedule-ref", scheduleRun.ExecutionRef)
	}
}

func TestProviderPublishEventDoesNotWaitForConcurrentScheduleList(t *testing.T) {
	ctx := context.Background()
	blocker := &blockingGetAllServer{
		store:   storeSchedules,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	startTestIndexedDBBackendWithWrapper(t, func(inner gestalt.IndexedDBProvider) gestalt.IndexedDBProvider {
		blocker.IndexedDBProvider = inner
		return blocker
	})
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := newProviderCore()
	provider.now = func() time.Time {
		return time.Date(2026, time.April, 16, 12, 0, 0, 0, time.UTC)
	}
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "100ms"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	const triggerID = "refresh-trigger"
	if _, err := provider.UpsertEventTrigger(ctx, &gestalt.UpsertWorkflowProviderEventTriggerRequest{
		TriggerID: triggerID,
		Match:     &gestalt.WorkflowEventMatch{Type: "task.updated", Source: "roadmap"},
		Target:    workflowTarget(t, "roadmap", "sync", map[string]any{"kind": "event"}),
	}); err != nil {
		t.Fatalf("UpsertEventTrigger: %v", err)
	}

	listDone := make(chan error, 1)
	go func() {
		_, err := provider.ListSchedules(ctx, &gestalt.ListWorkflowProviderSchedulesRequest{})
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
		_, err := provider.PublishEvent(ctx, &gestalt.PublishWorkflowProviderEventRequest{
			PluginName: "roadmap",
			Event: &gestalt.WorkflowEvent{
				ID:          "evt-while-listing",
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
	run, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: runID})
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.Status != gestalt.WorkflowRunStatusValuePending {
		t.Fatalf("run status = %v, want pending", run.Status)
	}
	if run.Trigger.Event.TriggerID != triggerID {
		t.Fatalf("event trigger = %#v", run.Trigger)
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

	provider := newProviderCore()
	provider.now = func() time.Time {
		return time.Date(2026, time.April, 16, 12, 0, 0, 0, time.UTC)
	}
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "10ms"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	startProviderWorker(t, provider)
	t.Cleanup(func() { _ = provider.Close() })

	target := workflowTarget(t, "github", "events.runAgentFromWorkflowEvent", map[string]any{
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
	if _, err := provider.UpsertEventTrigger(ctx, &gestalt.UpsertWorkflowProviderEventTriggerRequest{
		TriggerID: "github-webhook",
		Match:     &gestalt.WorkflowEventMatch{Type: "github.app.webhook", Source: "github"},
		Target:    target,
		RequestedBy: &gestalt.WorkflowActor{
			SubjectID: "system:config",
		},
	}); err != nil {
		t.Fatalf("UpsertEventTrigger(existing actor shape): %v", err)
	}
	if _, err := provider.UpsertEventTrigger(ctx, &gestalt.UpsertWorkflowProviderEventTriggerRequest{
		TriggerID: "github-webhook",
		Match:     &gestalt.WorkflowEventMatch{Type: "github.app.webhook", Source: "github"},
		Target:    target,
		RequestedBy: &gestalt.WorkflowActor{
			SubjectID:   "system:config",
			SubjectKind: "system",
			DisplayName: "Gestalt config",
			AuthSource:  "config",
		},
	}); err != nil {
		t.Fatalf("UpsertEventTrigger: %v", err)
	}

	publishedBy := &gestalt.WorkflowActor{
		SubjectID:   "service_account:github_app_installation:127579767:repo:valon-technologies/gestalt",
		SubjectKind: "service_account",
		DisplayName: "GitHub App installation 127579767 (valon-technologies/gestalt)",
		AuthSource:  "github_app_webhook",
	}
	if _, err := provider.PublishEvent(ctx, &gestalt.PublishWorkflowProviderEventRequest{
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
	if call.ExecutionRef == "" || call.ExecutionRef == "event-ref" {
		t.Fatalf("execution_ref = %q, want publisher-scoped event ref", call.ExecutionRef)
	}
	if call.CreatedBy.SubjectID != publishedBy.SubjectID {
		t.Fatalf("created_by.subject_id = %q, want %q", call.CreatedBy.SubjectID, publishedBy.SubjectID)
	}
	ref, err := provider.GetExecutionReference(ctx, &gestalt.GetWorkflowExecutionReferenceRequest{ID: call.ExecutionRef})
	if err != nil {
		t.Fatalf("GetExecutionReference: %v", err)
	}
	if ref.SubjectID != publishedBy.SubjectID || ref.SubjectKind != "service_account" || ref.AuthSource != "github_app_webhook" {
		t.Fatalf("execution ref subject = (%q, %q, %q), want published GitHub service account", ref.SubjectID, ref.SubjectKind, ref.AuthSource)
	}
	if ref.CredentialSubjectID != publishedBy.SubjectID {
		t.Fatalf("credential_subject_id = %q, want publisher subject", ref.CredentialSubjectID)
	}
	gotOperations := map[string]bool{}
	for _, permission := range ref.Permissions {
		if permission.Plugin != "github" {
			continue
		}
		for _, operation := range permission.Operations {
			gotOperations[operation] = true
		}
	}
	for _, operation := range []string{"events.runAgentFromWorkflowEvent", "bot.commitFiles", "bot.openPullRequest", "bot.createPullRequest"} {
		if !gotOperations[operation] {
			t.Fatalf("permissions = %#v, missing github/%s", ref.Permissions, operation)
		}
	}

	duplicatePublisher := &gestalt.WorkflowActor{
		SubjectID:   "service_account:github_app_installation:127579767:repo:valon-technologies/other",
		SubjectKind: "service_account",
		DisplayName: "GitHub App installation 127579767 (valon-technologies/other)",
		AuthSource:  "github_app_webhook",
	}
	if _, err := provider.PublishEvent(ctx, &gestalt.PublishWorkflowProviderEventRequest{
		PluginName:  "github",
		PublishedBy: duplicatePublisher,
		Event:       githubWebhookWorkflowEvent(t),
	}); err != nil {
		t.Fatalf("PublishEvent(duplicate): %v", err)
	}
	ref, err = provider.GetExecutionReference(ctx, &gestalt.GetWorkflowExecutionReferenceRequest{ID: call.ExecutionRef})
	if err != nil {
		t.Fatalf("GetExecutionReference(after duplicate): %v", err)
	}
	if ref.SubjectID != publishedBy.SubjectID {
		t.Fatalf("duplicate publish replaced execution ref subject = %q, want %q", ref.SubjectID, publishedBy.SubjectID)
	}
	runs, err := provider.ListRuns(ctx, &gestalt.ListWorkflowProviderRunsRequest{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs.Runs) != 1 {
		t.Fatalf("runs len = %d, want duplicate event to keep one run", len(runs.Runs))
	}
}

func TestProviderPublishEventAgentTargetExecutionReferenceIncludesOutputDelivery(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := newProviderCore()
	provider.now = func() time.Time {
		return time.Date(2026, time.April, 16, 12, 0, 0, 0, time.UTC)
	}
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "10ms"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	if _, err := provider.UpsertEventTrigger(ctx, &gestalt.UpsertWorkflowProviderEventTriggerRequest{
		TriggerID: "github-agent-webhook",
		Match:     &gestalt.WorkflowEventMatch{Type: "github.app.webhook", Source: "github"},
		Target:    workflowAgentTargetWithDeliveries("managed", "gpt-5.4", "respond to the GitHub event"),
	}); err != nil {
		t.Fatalf("UpsertEventTrigger(agent): %v", err)
	}

	publishedBy := &gestalt.WorkflowActor{
		SubjectID:   "service_account:github_app_installation:127579767:repo:valon-technologies/gestalt",
		SubjectKind: "service_account",
		DisplayName: "GitHub App installation 127579767 (valon-technologies/gestalt)",
		AuthSource:  "github_app_webhook",
	}
	if _, err := provider.PublishEvent(ctx, &gestalt.PublishWorkflowProviderEventRequest{
		PluginName:  "agent:managed",
		PublishedBy: publishedBy,
		Event:       githubWebhookWorkflowEvent(t),
	}); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}

	runs, err := provider.ListRuns(ctx, &gestalt.ListWorkflowProviderRunsRequest{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs.Runs) != 1 {
		t.Fatalf("runs len = %d, want 1", len(runs.Runs))
	}
	ref, err := provider.GetExecutionReference(ctx, &gestalt.GetWorkflowExecutionReferenceRequest{ID: runs.Runs[0].ExecutionRef})
	if err != nil {
		t.Fatalf("GetExecutionReference: %v", err)
	}
	got := map[string]map[string]bool{}
	for _, permission := range ref.Permissions {
		ops := got[permission.Plugin]
		if ops == nil {
			ops = map[string]bool{}
			got[permission.Plugin] = ops
		}
		for _, operation := range permission.Operations {
			ops[operation] = true
		}
	}
	if !got["slack"]["events.reply"] {
		t.Fatalf("permissions = %#v, missing slack/events.reply output delivery permission", ref.Permissions)
	}
	if !got["slack"]["events.replySessionStarted"] {
		t.Fatalf("permissions = %#v, missing slack/events.replySessionStarted session ready delivery permission", ref.Permissions)
	}
	if !got["slack"]["chat.postMessage"] {
		t.Fatalf("permissions = %#v, missing slack/chat.postMessage tool permission", ref.Permissions)
	}
}

func githubWebhookWorkflowEvent(t *testing.T) *gestalt.WorkflowEvent {
	t.Helper()
	return &gestalt.WorkflowEvent{
		ID:          "github:delivery-1",
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

	provider := newProviderCore()
	provider.now = clock.Now
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "100ms"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	startProviderWorker(t, provider)
	t.Cleanup(func() { _ = provider.Close() })

	schedule, err := provider.UpsertSchedule(ctx, &gestalt.UpsertWorkflowProviderScheduleRequest{
		ScheduleID:   "slack-reminder",
		Cron:         "* * * * *",
		Timezone:     "UTC",
		Target:       workflowAgentTarget("managed", "gpt-5.4", "send a Slack reminder"),
		ExecutionRef: "agent-ref",
	})
	if err != nil {
		t.Fatalf("UpsertSchedule(agent): %v", err)
	}
	if schedule.Target.Agent.ProviderName != "managed" {
		t.Fatalf("schedule target = %#v", schedule.Target)
	}
	if schedule.Target.Plugin != nil {
		t.Fatalf("schedule target included plugin fields: %#v", schedule.Target)
	}

	listed, err := provider.ListSchedules(ctx, &gestalt.ListWorkflowProviderSchedulesRequest{})
	if err != nil {
		t.Fatalf("ListSchedules: %v", err)
	}
	if len(listed.Schedules) != 1 || !workflowValuesEqual(listed.Schedules[0].Target, schedule.Target) {
		t.Fatalf("listed schedules = %#v, want persisted agent target", listed.Schedules)
	}

	clock.Set(time.Date(2026, time.April, 16, 12, 1, 0, 0, time.UTC))
	provider.mu.Lock()
	provider.signalWorkerLocked("")
	provider.mu.Unlock()

	call, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall(agent schedule): %v", err)
	}
	if call.ExecutionRef != "agent-ref" {
		t.Fatalf("execution_ref = %q, want agent-ref", call.ExecutionRef)
	}
	if call.Target.Agent.Prompt != "send a Slack reminder" {
		t.Fatalf("call target = %#v", call.Target)
	}
	toolRefs := call.Target.Agent.ToolRefs
	if len(toolRefs) != 2 ||
		toolRefs[0].Plugin != "slack" ||
		toolRefs[0].Operation != "chat.postMessage" ||
		toolRefs[1].Plugin != "linear" ||
		toolRefs[1].Operation != "" {
		t.Fatalf("tool refs = %#v", toolRefs)
	}
	if call.Target.Plugin != nil {
		t.Fatalf("call target included plugin fields: %#v", call.Target)
	}
	if call.Trigger.Schedule.ScheduleID != "slack-reminder" {
		t.Fatalf("schedule trigger = %#v", call.Trigger)
	}

	run, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: call.RunID})
	if err != nil {
		t.Fatalf("GetRun(agent schedule): %v", err)
	}
	if !workflowValuesEqual(run.Target, call.Target) {
		t.Fatalf("run target = %#v, want %#v", run.Target, call.Target)
	}
}

func TestProviderLeavesAgentToolValidationToHost(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	cases := []struct {
		name   string
		target *gestalt.BoundWorkflowTarget
	}{
		{
			name:   "missing provider",
			target: workflowAgentTargetFromMessage(&gestalt.BoundWorkflowAgentTarget{}),
		},
		{
			name:   "empty prompt",
			target: workflowAgentTargetFromMessage(&gestalt.BoundWorkflowAgentTarget{ProviderName: "managed"}),
		},
		{
			name: "negative timeout",
			target: workflowAgentTargetFromMessage(&gestalt.BoundWorkflowAgentTarget{
				ProviderName:   "managed",
				Prompt:         "send a Slack reminder",
				TimeoutSeconds: -1,
			}),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := provider.UpsertSchedule(ctx, &gestalt.UpsertWorkflowProviderScheduleRequest{
				ScheduleID: "invalid-" + strings.ReplaceAll(tc.name, " ", "-"),
				Cron:       "* * * * *",
				Timezone:   "UTC",
				Target:     tc.target,
			}); err == nil {
				t.Fatal("UpsertSchedule succeeded, want error")
			}
		})
	}

	target := workflowAgentTargetFromMessage(&gestalt.BoundWorkflowAgentTarget{
		ProviderName: " managed ",
		Prompt:       "send a Slack reminder",
		ToolRefs: []gestalt.AgentToolRef{
			{Operation: "chat.postMessage"},
		},
	})
	schedule, err := provider.UpsertSchedule(ctx, &gestalt.UpsertWorkflowProviderScheduleRequest{
		ScheduleID: "host-validated-agent",
		Cron:       "* * * * *",
		Timezone:   "UTC",
		Target:     target,
	})
	if err != nil {
		t.Fatalf("UpsertSchedule(agent payload host validation): %v", err)
	}
	agent := schedule.Target.Agent
	if agent == nil {
		t.Fatalf("schedule target = %#v, want agent target", schedule.Target)
	}
	if agent.ProviderName != "managed" {
		t.Fatalf("agent provider_name = %q, want trimmed provider", agent.ProviderName)
	}
	if got := agent.ToolRefs[0].Plugin; got != "" {
		t.Fatalf("agent tool plugin = %q, want tool validation left to host", got)
	}
}

func TestProviderRequiresStoredTargetJSON(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := newProviderCore()
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
				_, err := provider.GetSchedule(ctx, &gestalt.GetWorkflowProviderScheduleRequest{ScheduleID: "missing-target-schedule"})
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
				_, err := provider.GetEventTrigger(ctx, &gestalt.GetWorkflowProviderEventTriggerRequest{TriggerID: "missing-target-trigger"})
				return err
			},
			want: "missing target_json",
		},
		{
			name: "run missing target json",
			put: func() error {
				return provider.runStore.Put(ctx, gestalt.Record{
					"id":           "missing-target-run",
					"status":       int64(gestalt.WorkflowRunStatusValuePending),
					"trigger_kind": triggerKindManual,
					"created_at":   now,
				})
			},
			read: func() error {
				_, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: "missing-target-run"})
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
				_, err := provider.GetExecutionReference(ctx, &gestalt.GetWorkflowExecutionReferenceRequest{ID: "missing-target-ref"})
				return err
			},
			want: "missing target_json",
		},
		{
			name: "run target json with unsupported field",
			put: func() error {
				return provider.runStore.Put(ctx, gestalt.Record{
					"id":           "flat-json-run",
					"status":       int64(gestalt.WorkflowRunStatusValuePending),
					"target_json":  `{"agent":{"providerName":"simple","prompt":"send a Slack reminder","unknownField":true}}`,
					"trigger_kind": triggerKindManual,
					"created_at":   now,
				})
			},
			read: func() error {
				_, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: "flat-json-run"})
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

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "10ms"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	startProviderWorker(t, provider)
	t.Cleanup(func() { _ = provider.Close() })

	if _, err := provider.UpsertEventTrigger(ctx, &gestalt.UpsertWorkflowProviderEventTriggerRequest{
		TriggerID: "refresh-trigger",
		Match:     &gestalt.WorkflowEventMatch{Type: "task.updated"},
		Target:    workflowTarget(t, "roadmap", "sync", map[string]any{"kind": "event"}),
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
		if _, err := provider.PublishEvent(ctx, &gestalt.PublishWorkflowProviderEventRequest{
			PluginName: "roadmap",
			Event: &gestalt.WorkflowEvent{
				ID:          event.id,
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
	if first.RunID == second.RunID {
		t.Fatalf("run ids = %q and %q, want distinct per source", first.RunID, second.RunID)
	}

	waitForCondition(t, time.Second, func() bool {
		runs, err := provider.ListRuns(ctx, &gestalt.ListWorkflowProviderRunsRequest{})
		if err != nil {
			return false
		}
		return len(runs.Runs) == 2
	})
}

func TestProviderEnqueueDueSchedulesReusesDeterministicRunID(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, time.April, 16, 12, 17, 0, 0, time.UTC)
	clock := newFakeClock(start)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := newProviderCore()
	provider.now = clock.Now
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	stopProviderWorker(t, provider)

	schedule, err := provider.UpsertSchedule(ctx, &gestalt.UpsertWorkflowProviderScheduleRequest{
		ScheduleID: "nightly-sync",
		Cron:       "*/5 * * * *",
		Timezone:   "UTC",
		Target:     workflowTarget(t, "roadmap", "sync", map[string]any{"kind": "schedule"}),
	})
	if err != nil {
		t.Fatalf("UpsertSchedule: %v", err)
	}

	latestDue := time.Date(2026, time.April, 16, 12, 15, 0, 0, time.UTC)
	runID := scheduleRunID(schedule.ID, latestDue)
	if err := provider.runStore.Put(ctx, workflowRunRecord{
		ID:                  runID,
		Status:              gestalt.WorkflowRunStatusValuePending,
		Target:              workflowTarget(t, "roadmap", "sync", map[string]any{"kind": "schedule"}),
		TriggerKind:         triggerKindSchedule,
		TriggerScheduleID:   schedule.ID,
		TriggerScheduledFor: timePtr(latestDue),
		CreatedAt:           start,
	}.toRecord()); err != nil {
		t.Fatalf("Put(run): %v", err)
	}

	if err := provider.enqueueDueSchedules(ctx); err != nil {
		t.Fatalf("enqueueDueSchedules: %v", err)
	}

	runs, err := provider.ListRuns(ctx, &gestalt.ListWorkflowProviderRunsRequest{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs.Runs) != 1 {
		t.Fatalf("runs len = %d, want 1", len(runs.Runs))
	}
	updated, err := provider.GetSchedule(ctx, &gestalt.GetWorkflowProviderScheduleRequest{
		ScheduleID: schedule.ID,
	})
	if err != nil {
		t.Fatalf("GetSchedule: %v", err)
	}
	wantNext := time.Date(2026, time.April, 16, 12, 20, 0, 0, time.UTC)
	if got := updated.NextRunAt; !got.Equal(wantNext) {
		t.Fatalf("next_run_at = %v, want %v", got, wantNext)
	}
}

func TestProviderRejectsCrossPluginScheduleAndTriggerIDCollisions(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	if _, err := provider.UpsertSchedule(ctx, &gestalt.UpsertWorkflowProviderScheduleRequest{
		ScheduleID: "shared-id",
		Cron:       "*/5 * * * *",
		Timezone:   "UTC",
		Target:     workflowTarget(t, "roadmap", "sync", map[string]any{"kind": "schedule"}),
	}); err != nil {
		t.Fatalf("UpsertSchedule(roadmap): %v", err)
	}
	if _, err := provider.UpsertSchedule(ctx, &gestalt.UpsertWorkflowProviderScheduleRequest{
		ScheduleID: "shared-id",
		Cron:       "*/5 * * * *",
		Timezone:   "UTC",
		Target:     workflowTarget(t, "billing", "sync", map[string]any{"kind": "schedule"}),
	}); err == nil {
		t.Fatal("UpsertSchedule(billing) succeeded, want cross-plugin collision error")
	}

	if _, err := provider.UpsertEventTrigger(ctx, &gestalt.UpsertWorkflowProviderEventTriggerRequest{
		TriggerID: "shared-trigger",
		Match:     &gestalt.WorkflowEventMatch{Type: "task.updated"},
		Target:    workflowTarget(t, "roadmap", "sync", map[string]any{"kind": "event"}),
	}); err != nil {
		t.Fatalf("UpsertEventTrigger(roadmap): %v", err)
	}
	if _, err := provider.UpsertEventTrigger(ctx, &gestalt.UpsertWorkflowProviderEventTriggerRequest{
		TriggerID: "shared-trigger",
		Match:     &gestalt.WorkflowEventMatch{Type: "invoice.updated"},
		Target:    workflowTarget(t, "billing", "sync", map[string]any{"kind": "event"}),
	}); err == nil {
		t.Fatal("UpsertEventTrigger(billing) succeeded, want cross-plugin collision error")
	}
}

func TestProviderMarksStaleRunningRunsFailedOnStartup(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	first := newProviderCore()
	if err := first.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure(first): %v", err)
	}

	startedAt := time.Now().UTC().Add(-time.Minute)
	if err := first.runStore.Put(ctx, workflowRunRecord{
		ID:          "stale-run",
		Status:      gestalt.WorkflowRunStatusValueRunning,
		Target:      workflowTarget(t, "roadmap", "sync", nil),
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
		Status:       gestalt.WorkflowRunStatusValueRunning,
		WorkflowKey:  workflowKey,
		Target:       workflowAgentTarget("managed", "gpt-5.5", "Respond in the Slack thread"),
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
			Signal:         workflowSignal(t, "signal-pending", "evt-pending", "pending"),
			IdempotencyKey: "evt-pending",
			Sequence:       1,
			CreatedAt:      startedAt,
		},
		{
			ID:             "signal-claimed",
			RunID:          "stale-agent-run",
			WorkflowKey:    workflowKey,
			State:          signalStateClaimed,
			Signal:         workflowSignal(t, "signal-claimed", "evt-claimed", "claimed"),
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

	second := newProviderCore()
	if err := second.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure(second): %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })
	if err := second.Start(ctx); err != nil {
		t.Fatalf("Start(second): %v", err)
	}

	waitForCondition(t, 5*time.Second, func() bool {
		stale, err := second.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{
			RunID: "stale-run",
		})
		if err != nil {
			t.Fatalf("GetRun(stale): %v", err)
		}
		return stale.Status == gestalt.WorkflowRunStatusValueFailed &&
			stale.StatusMessage == "workflow provider restarted while run was in progress"
	})
	waitForCondition(t, 5*time.Second, func() bool {
		staleAgent, err := second.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: "stale-agent-run"})
		if err != nil {
			t.Fatalf("GetRun(stale-agent-run): %v", err)
		}
		return staleAgent.Status == gestalt.WorkflowRunStatusValueFailed
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

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	now := time.Now().UTC()
	run := workflowRunRecord{
		ID:          "orphan-claimed-pending-run",
		Status:      gestalt.WorkflowRunStatusValuePending,
		Target:      workflowTarget(t, "brain", "sources.sync", map[string]any{"sourceId": "slack-valon-public"}),
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
	reloaded, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: run.ID})
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if reloaded.Status != gestalt.WorkflowRunStatusValuePending {
		t.Fatalf("run status = %v, want pending after orphan claim recovery", reloaded.Status)
	}
}

func TestRecoverStaleWorkflowRunsPreservesFreshAndKeyedPendingClaims(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := newProviderCore()
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
				Status:      gestalt.WorkflowRunStatusValuePending,
				Target:      workflowTarget(t, "brain", "sources.sync", nil),
				TriggerKind: triggerKindSchedule,
				CreatedAt:   now,
			},
			claimedAt: now.Add(-nonRunningRunClaimGrace / 2),
		},
		{
			name: "keyed",
			run: workflowRunRecord{
				ID:          "keyed-claimed-pending-run",
				Status:      gestalt.WorkflowRunStatusValuePending,
				Target:      workflowAgentTarget("managed", "gpt-5.5", "Respond"),
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

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	now := time.Date(2026, time.May, 3, 16, 0, 0, 0, time.UTC)
	startedAt := now.Add(-(defaultAgentRunTimeout + agentRunStaleGrace + time.Second))
	run := workflowRunRecord{
		ID:          "expired-agent-run",
		Status:      gestalt.WorkflowRunStatusValueRunning,
		Target:      workflowAgentTarget("managed", "claude-opus-4-7", "Investigate CI"),
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
	reloaded, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: run.ID})
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if reloaded.Status != gestalt.WorkflowRunStatusValueFailed || reloaded.StatusMessage != staleRunStatusMessage {
		t.Fatalf("run status = %v message=%q, want stale failure", reloaded.Status, reloaded.StatusMessage)
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

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	now := time.Date(2026, time.May, 3, 16, 0, 0, 0, time.UTC)
	startedAt := now.Add(-10 * time.Minute)
	target := workflowAgentTarget("managed", "claude-opus-4-7", "Investigate CI")
	target.Agent.TimeoutSeconds = int32((30 * time.Minute).Seconds())
	run := workflowRunRecord{
		ID:          "long-timeout-agent-run",
		Status:      gestalt.WorkflowRunStatusValueRunning,
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
	reloaded, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: run.ID})
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if reloaded.Status != gestalt.WorkflowRunStatusValueRunning {
		t.Fatalf("run status = %v, want running", reloaded.Status)
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

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	now := time.Now().UTC()
	run := workflowRunRecord{
		ID:          "replaced-claim-pending-run",
		Status:      gestalt.WorkflowRunStatusValuePending,
		Target:      workflowTarget(t, "brain", "sources.sync", nil),
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

	provider := newProviderCore()
	provider.now = clock.Now
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	startedAt := start.Add(-time.Minute)
	staleRun := workflowRunRecord{
		ID:          "expired-running-run",
		Status:      gestalt.WorkflowRunStatusValueRunning,
		Target:      workflowTarget(t, "roadmap", "sync", nil),
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
		Status:      gestalt.WorkflowRunStatusValuePending,
		Target:      workflowTarget(t, "github", "failed-check-run-comment", nil),
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
	if call.RunID != preferredRun.ID {
		t.Fatalf("run_id = %q, want preferred run %q", call.RunID, preferredRun.ID)
	}
	reloadedStale, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: staleRun.ID})
	if err != nil {
		t.Fatalf("GetRun(stale): %v", err)
	}
	if reloadedStale.Status != gestalt.WorkflowRunStatusValueRunning {
		t.Fatalf("stale run status = %v, want running until a fallback stale-recovery tick", reloadedStale.Status)
	}
}

func TestProviderTickPrioritizesPluginEventWhenPreferredWakeLost(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, time.May, 3, 17, 0, 0, 0, time.UTC)
	clock := newFakeClock(start)
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := newProviderCore()
	provider.now = clock.Now
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	for i := 0; i < 3; i++ {
		run := workflowRunRecord{
			ID:          fmt.Sprintf("old-agent-backlog-%d", i),
			Status:      gestalt.WorkflowRunStatusValuePending,
			Target:      workflowAgentTarget("managed", "gpt-5.5", "Process backlog"),
			TriggerKind: triggerKindManual,
			CreatedAt:   start.Add(-time.Hour).Add(time.Duration(i) * time.Second),
		}
		if err := provider.runStore.Put(ctx, run.toRecord()); err != nil {
			t.Fatalf("Put(%s): %v", run.ID, err)
		}
	}

	const triggerID = "slack-message-ingest"
	if _, err := provider.UpsertEventTrigger(ctx, &gestalt.UpsertWorkflowProviderEventTriggerRequest{
		TriggerID: triggerID,
		Match:     &gestalt.WorkflowEventMatch{Type: "message", Source: "slack"},
		Target:    workflowTarget(t, "brain", "sources.slack.events.ingest", map[string]any{"source": "slack"}),
	}); err != nil {
		t.Fatalf("UpsertEventTrigger: %v", err)
	}
	clock.Set(start.Add(time.Minute))
	if _, err := provider.PublishEvent(ctx, &gestalt.PublishWorkflowProviderEventRequest{
		Event: &gestalt.WorkflowEvent{
			ID:          "evt-lost-wake",
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
	if call.RunID != wantRunID {
		t.Fatalf("run_id = %q, want plugin event run %q", call.RunID, wantRunID)
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
			target := workflowAgentTarget("managed", "gpt-5.5", "Respond to interactive workflow")
			target.Agent.Metadata = mustStruct(t, map[string]any{
				gestaltInputKey: map[string]any{
					workflowMetadataKey: map[string]any{
						dispatchPriorityMetadataKey: tc.value,
					},
				},
			})
			run := workflowRunRecord{
				ID:          "interactive-agent-run",
				Status:      gestalt.WorkflowRunStatusValuePending,
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
			target := workflowAgentTarget("managed", "gpt-5.5", "Respond to interactive workflow")
			target.Agent.Metadata = mustStruct(t, map[string]any{
				gestaltInputKey: map[string]any{
					workflowMetadataKey: map[string]any{
						dispatchPriorityMetadataKey: tc.value,
					},
				},
			})
			run := workflowRunRecord{
				ID:          "keyed-agent-run",
				Status:      gestalt.WorkflowRunStatusValuePending,
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

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	oldAgent := workflowRunRecord{
		ID:          "older-agent-run",
		Status:      gestalt.WorkflowRunStatusValuePending,
		Target:      workflowAgentTarget("managed", "gpt-5.5", "Process backlog"),
		TriggerKind: triggerKindManual,
		CreatedAt:   start.Add(-time.Hour),
	}
	firstEvent := workflowRunRecord{
		ID:          "first-plugin-event",
		Status:      gestalt.WorkflowRunStatusValuePending,
		Target:      workflowTarget(t, "brain", "sources.slack.events.ingest", nil),
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
	if firstCall.RunID != firstEvent.ID {
		t.Fatalf("first run_id = %q, want %q", firstCall.RunID, firstEvent.ID)
	}

	if err := provider.tick(ctx, ""); err != nil {
		t.Fatalf("tick(second): %v", err)
	}
	secondCall, err := host.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall(second): %v", err)
	}
	if secondCall.RunID != secondEvent.ID {
		t.Fatalf("second run_id = %q, want %q", secondCall.RunID, secondEvent.ID)
	}
}

func TestProviderTickPreferredWakeDoesNotBypassDispatchPriority(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, time.May, 3, 18, 0, 0, 0, time.UTC)
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	preferred := workflowRunRecord{
		ID:          "preferred-agent-run",
		Status:      gestalt.WorkflowRunStatusValuePending,
		Target:      workflowAgentTarget("managed", "gpt-5.5", "Respond to explicit wake"),
		TriggerKind: triggerKindManual,
		CreatedAt:   start.Add(-time.Minute),
	}
	pluginEvent := workflowRunRecord{
		ID:          "higher-priority-plugin-event",
		Status:      gestalt.WorkflowRunStatusValuePending,
		Target:      workflowTarget(t, "brain", "sources.slack.events.ingest", nil),
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
	if call.RunID != pluginEvent.ID {
		t.Fatalf("run_id = %q, want higher-priority run %q", call.RunID, pluginEvent.ID)
	}
}

func TestProviderTickPreferredWakePreservesFIFOWithinDispatchPriority(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, time.May, 3, 18, 15, 0, 0, time.UTC)
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	oldWorkflowKey := "github:127579767:valon-technologies/toolshed:1289:policy:github-thread-work"
	oldRun := workflowRunRecord{
		ID:          "older-github-agent-run",
		Status:      gestalt.WorkflowRunStatusValuePending,
		Target:      workflowAgentTarget("managed", "gpt-5.5", "Review older GitHub work"),
		TriggerKind: triggerKindManual,
		WorkflowKey: oldWorkflowKey,
		CreatedAt:   start.Add(-time.Minute),
	}
	newWorkflowKey := "github:127579767:valon-technologies/toolshed:1290:policy:github-thread-work"
	newRun := workflowRunRecord{
		ID:          "newer-github-agent-run",
		Status:      gestalt.WorkflowRunStatusValuePending,
		Target:      workflowAgentTarget("managed", "gpt-5.5", "Review GitHub"),
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
		signal := workflowSignal(t, tc.signalID, tc.signalID+"-idem", tc.signalID)
		if err := provider.signalStore.Put(ctx, workflowSignalRecord{
			ID:             signal.ID,
			RunID:          tc.runID,
			WorkflowKey:    tc.workflowKey,
			State:          signalStatePending,
			Signal:         signal,
			IdempotencyKey: signal.IdempotencyKey,
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
	if call.RunID != oldRun.ID {
		t.Fatalf("run_id = %q, want older same-priority run %q", call.RunID, oldRun.ID)
	}
}

func TestProviderTickPrioritizesKeyedSignalContinuation(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, time.May, 3, 18, 30, 0, 0, time.UTC)
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	oldAgent := workflowRunRecord{
		ID:          "old-unkeyed-agent-run",
		Status:      gestalt.WorkflowRunStatusValuePending,
		Target:      workflowAgentTarget("managed", "gpt-5.5", "Process backlog"),
		TriggerKind: triggerKindManual,
		CreatedAt:   start.Add(-time.Hour),
	}
	workflowKey := "slack:T123:C123:1700000000.000001"
	continuation := workflowRunRecord{
		ID:           "terminal-keyed-agent-run",
		Status:       gestalt.WorkflowRunStatusValueSucceeded,
		Target:       workflowAgentTarget("managed", "gpt-5.5", "Respond to Slack thread"),
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
	signal := workflowSignal(t, "signal-keyed-continuation", "evt-keyed-continuation", "new Slack reply")
	if err := provider.signalStore.Put(ctx, workflowSignalRecord{
		ID:             signal.ID,
		RunID:          continuation.ID,
		WorkflowKey:    workflowKey,
		State:          signalStatePending,
		Signal:         signal,
		IdempotencyKey: signal.IdempotencyKey,
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
	if call.RunID != continuation.ID {
		t.Fatalf("run_id = %q, want keyed continuation %q", call.RunID, continuation.ID)
	}
	if len(call.Signals) != 1 || call.Signals[0].ID != signal.ID {
		t.Fatalf("signals = %#v, want keyed continuation signal %q", call.Signals, signal.ID)
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

	provider := newProviderCore()
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
		Status:      gestalt.WorkflowRunStatusValuePending,
		Target:      workflowTarget(t, "brain", "sources.slack.events.ingest", nil),
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

	provider := newProviderCore()
	provider.now = clock.Now
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	startedAt := start.Add(-time.Minute)
	run := workflowRunRecord{
		ID:          "fresh-claimed-running-run",
		Status:      gestalt.WorkflowRunStatusValueRunning,
		Target:      workflowTarget(t, "roadmap", "sync", nil),
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
	reloaded, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: run.ID})
	if err != nil {
		t.Fatalf("GetRun(fresh): %v", err)
	}
	if reloaded.Status != gestalt.WorkflowRunStatusValueRunning {
		t.Fatalf("fresh status = %v, want running", reloaded.Status)
	}

	clock.Set(start.Add(time.Minute + time.Second))
	if err := provider.tick(ctx, ""); err != nil {
		t.Fatalf("tick(expired claim): %v", err)
	}
	recovered, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: run.ID})
	if err != nil {
		t.Fatalf("GetRun(recovered): %v", err)
	}
	if recovered.Status != gestalt.WorkflowRunStatusValueFailed {
		t.Fatalf("expired status = %v, want failed", recovered.Status)
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

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	startedAt := time.Now().UTC().Add(-time.Minute)
	run := workflowRunRecord{
		ID:            "lost-claim-run",
		Status:        gestalt.WorkflowRunStatusValueFailed,
		StatusMessage: staleRunStatusMessage,
		Target:        workflowTarget(t, "roadmap", "sync", nil),
		TriggerKind:   triggerKindManual,
		CreatedAt:     startedAt.Add(-time.Second),
		StartedAt:     &startedAt,
		CompletedAt:   timePtr(startedAt.Add(time.Second)),
	}
	if err := provider.runStore.Put(ctx, run.toRecord()); err != nil {
		t.Fatalf("Put(run): %v", err)
	}

	pending := run
	pending.Status = gestalt.WorkflowRunStatusValueRunning
	pending.CompletedAt = nil
	pending.StatusMessage = ""
	if err := provider.completeRunAfterInvoke(ctx, pending, nil, provider.claimOwnerID, &gestalt.InvokeWorkflowOperationResponse{Status: 202, Body: `{"ok":true}`}, nil); err != nil {
		t.Fatalf("completeRunAfterInvoke: %v", err)
	}
	reloaded, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: run.ID})
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if reloaded.Status != gestalt.WorkflowRunStatusValueFailed || reloaded.StatusMessage != staleRunStatusMessage {
		t.Fatalf("run after lost-claim completion = status:%v message:%q, want stale failure", reloaded.Status, reloaded.StatusMessage)
	}
}

func TestProviderStartDoesNotBlockOnStaleRunRecoveryFailure(t *testing.T) {
	ctx := context.Background()
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, newWorkflowHostStub(202, `{"ok":true}`))

	provider := newProviderCore()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	if err := provider.runStore.Put(ctx, gestalt.Record{
		"id":          "malformed-run",
		"status":      int64(gestalt.WorkflowRunStatusValueRunning),
		"target_json": "{",
		"created_at":  time.Now().UTC(),
	}); err != nil {
		t.Fatalf("Put(malformed-run): %v", err)
	}
	startedAt := time.Now().UTC().Add(-time.Minute)
	if err := provider.runStore.Put(ctx, workflowRunRecord{
		ID:          "recoverable-stale-run",
		Status:      gestalt.WorkflowRunStatusValueRunning,
		Target:      workflowTarget(t, "roadmap", "sync", nil),
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
		recovered, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: "recoverable-stale-run"})
		if err != nil {
			t.Fatalf("GetRun(recoverable): %v", err)
		}
		return recovered.Status == gestalt.WorkflowRunStatusValueFailed &&
			recovered.StatusMessage == staleRunStatusMessage
	})
}

type workflowHostStub struct {
	mu        sync.Mutex
	callsCh   chan gestalt.InvokeWorkflowOperationInput
	callsLog  []gestalt.InvokeWorkflowOperationInput
	releaseCh chan struct{}
	errs      []error
	status    int32
	body      string
}

func newWorkflowHostStub(status int32, body string) *workflowHostStub {
	return &workflowHostStub{
		callsCh: make(chan gestalt.InvokeWorkflowOperationInput, 16),
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

func (s *workflowHostStub) InvokeOperation(ctx context.Context, req gestalt.InvokeWorkflowOperationInput) (*gestalt.InvokeWorkflowOperationResponse, error) {
	cloned := cloneJSONValue(req)
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
	return &gestalt.InvokeWorkflowOperationResponse{Status: s.status, Body: s.body}, nil
}

func (s *workflowHostStub) waitForCall(timeout time.Duration) (*gestalt.InvokeWorkflowOperationInput, error) {
	select {
	case call := <-s.callsCh:
		return &call, nil
	case <-time.After(timeout):
		return nil, context.DeadlineExceeded
	}
}

func (s *workflowHostStub) calls() []gestalt.InvokeWorkflowOperationInput {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]gestalt.InvokeWorkflowOperationInput(nil), s.callsLog...)
}

func (s *workflowHostStub) Close() error {
	return nil
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

func startTestIndexedDBBackendWithWrapper(t *testing.T, wrap func(gestalt.IndexedDBProvider) gestalt.IndexedDBProvider) {
	t.Helper()
	socketPath := newSocketPath(t, "indexeddb.sock")
	store := relationaldb.New()
	if err := store.Configure(context.Background(), "workflow_state", map[string]any{
		"dsn": "file:" + filepath.Join(t.TempDir(), "workflow.sqlite") + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)",
	}); err != nil {
		t.Fatalf("relationaldb.Configure: %v", err)
	}
	seedWorkflowObjectStores(t, store)
	indexedDBServer := gestalt.IndexedDBProvider(store)
	if wrap != nil {
		indexedDBServer = wrap(indexedDBServer)
	}
	serverCtx, cancel := context.WithCancel(context.Background())
	t.Setenv("GESTALT_PLUGIN_SOCKET", socketPath)
	t.Setenv(gestalt.EnvIndexedDBSocket, socketPath)
	serveErr := make(chan error, 1)
	go func() { serveErr <- gestalt.ServeIndexedDBProvider(serverCtx, indexedDBServer) }()
	waitForUnixSocket(t, socketPath)
	t.Cleanup(func() {
		cancel()
		if err := <-serveErr; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("ServeIndexedDBProvider: %v", err)
		}
		_ = os.Remove(socketPath)
		_ = store.Close()
	})
}

func waitForUnixSocket(t *testing.T, socketPath string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("unix", socketPath, 10*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for unix socket %s", socketPath)
}

func seedWorkflowObjectStores(t *testing.T, store *relationaldb.Provider) {
	t.Helper()
	for _, def := range []struct {
		name   string
		schema gestalt.ObjectStoreSchema
	}{
		{name: storeSchedules, schema: gestalt.ObjectStoreSchema{}},
		{name: storeEventTriggers, schema: gestalt.ObjectStoreSchema{}},
		{name: storeDefinitions, schema: gestalt.ObjectStoreSchema{}},
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
	gestalt.IndexedDBProvider
	failUnscopedSignalGetAll bool
	missingSignalIndex       string
	mu                       sync.Mutex
	getCounts                map[string]int
}

type blockingGetAllServer struct {
	gestalt.IndexedDBProvider
	store   string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingGetAllServer) GetAll(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]gestalt.Record, error) {
	if req.Store == s.store {
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
	return s.IndexedDBProvider.GetAll(ctx, req)
}

func (s *indexedDBServerSpy) Get(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) (gestalt.Record, error) {
	s.mu.Lock()
	if s.getCounts == nil {
		s.getCounts = make(map[string]int)
	}
	s.getCounts[req.Store]++
	s.mu.Unlock()
	return s.IndexedDBProvider.Get(ctx, req)
}

func (s *indexedDBServerSpy) GetAll(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]gestalt.Record, error) {
	if s.failUnscopedSignalGetAll && req.Store == storeSignals && req.Range == nil {
		return nil, status.Error(codes.Internal, "unexpected unscoped workflow_signals GetAll")
	}
	return s.IndexedDBProvider.GetAll(ctx, req)
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

func (s *indexedDBServerSpy) IndexCount(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (int64, error) {
	if req.Store == storeSignals && req.Index == s.missingSignalIndex {
		return 0, status.Errorf(codes.NotFound, "index not found: %s", req.Index)
	}
	return s.IndexedDBProvider.IndexCount(ctx, req)
}

func startTestWorkflowHost(t *testing.T, host workflowHostClient) {
	t.Helper()
	previous := openWorkflowHost
	openWorkflowHost = func() (workflowHostClient, error) { return host, nil }
	t.Cleanup(func() {
		openWorkflowHost = previous
	})
}

func workflowTarget(t *testing.T, pluginName, operation string, input map[string]any) *gestalt.BoundWorkflowTarget {
	t.Helper()
	return &gestalt.BoundWorkflowTarget{
		Plugin: &gestalt.BoundWorkflowPluginTarget{
			PluginName: pluginName,
			Operation:  operation,
			Input:      mustStruct(t, input),
		},
	}
}

func workflowAgentTarget(providerName, model, prompt string) *gestalt.BoundWorkflowTarget {
	return workflowAgentTargetFromMessage(&gestalt.BoundWorkflowAgentTarget{
		ProviderName: providerName,
		Model:        model,
		Prompt:       prompt,
		ToolRefs: []gestalt.AgentToolRef{
			{Plugin: "slack", Operation: "chat.postMessage"},
			{Plugin: "linear"},
		},
	})
}

func workflowAgentTargetWithOutputDelivery(providerName, model, prompt string) *gestalt.BoundWorkflowTarget {
	target := workflowAgentTarget(providerName, model, prompt)
	target.Agent.OutputDelivery = &gestalt.WorkflowOutputDelivery{
		Target: &gestalt.BoundWorkflowPluginTarget{
			PluginName: "slack",
			Operation:  "events.reply",
		},
		CredentialMode: "none",
		InputBindings: []gestalt.WorkflowOutputBinding{
			{
				InputField: "text",
				Value: &gestalt.WorkflowOutputValueSource{
					AgentOutput: "text",
				},
			},
			{
				InputField: "reply_ref",
				Value: &gestalt.WorkflowOutputValueSource{
					SignalPayload: "reply_ref",
				},
			},
		},
	}
	return target
}

func workflowAgentTargetWithDeliveries(providerName, model, prompt string) *gestalt.BoundWorkflowTarget {
	target := workflowAgentTargetWithOutputDelivery(providerName, model, prompt)
	target.Agent.SessionReadyDelivery = workflowSessionReadyDelivery()
	return target
}

func workflowSessionReadyDelivery() *gestalt.WorkflowOutputDelivery {
	return &gestalt.WorkflowOutputDelivery{
		Target: &gestalt.BoundWorkflowPluginTarget{
			PluginName: "slack",
			Operation:  "events.replySessionStarted",
		},
		CredentialMode: "none",
		InputBindings: []gestalt.WorkflowOutputBinding{
			{
				InputField: "session_id",
				Value: &gestalt.WorkflowOutputValueSource{
					AgentSession: "id",
				},
			},
			{
				InputField: "reply_ref",
				Value: &gestalt.WorkflowOutputValueSource{
					SignalPayload: "reply_ref",
				},
			},
		},
	}
}

func workflowAgentTargetFromMessage(agent *gestalt.BoundWorkflowAgentTarget) *gestalt.BoundWorkflowTarget {
	return &gestalt.BoundWorkflowTarget{
		Agent: agent,
	}
}

func workflowSignal(t *testing.T, id, idempotencyKey, text string) *gestalt.WorkflowSignal {
	t.Helper()
	return &gestalt.WorkflowSignal{
		ID:             id,
		Name:           "slack.message",
		IdempotencyKey: idempotencyKey,
		Payload:        mustStruct(t, map[string]any{"text": text}),
	}
}

func mustStruct(t *testing.T, value map[string]any) map[string]any {
	t.Helper()
	if len(value) == 0 {
		return nil
	}
	return cloneAnyMap(value)
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

func workflowValuesEqual(left, right any) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
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
