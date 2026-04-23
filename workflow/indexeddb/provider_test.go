package indexeddb

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	relationaldb "github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc"
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
	if call.GetTarget().GetPluginName() != "roadmap" || call.GetTarget().GetOperation() != "sync" {
		t.Fatalf("target = %#v", call.GetTarget())
	}
	if got := call.GetTarget().GetInput().AsMap()["mode"]; got != "full" {
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

func TestProviderStartRunRepairsMissingIdempotencyRecord(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "10ms"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	now := time.Now().UTC()
	runID := idempotentManualRunID("roadmap", "manual-sync")
	run := workflowRunRecord{
		ID:          runID,
		PluginName:  "roadmap",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Operation:   "sync",
		Input:       map[string]any{"mode": "full"},
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
		PluginName:  "roadmap",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Operation:   "sync",
		Input:       map[string]any{"kind": "pending"},
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
		PluginName:  "roadmap",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING,
		Operation:   "sync",
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
		PluginName: "roadmap",
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
	provider.signalWorkerLocked()
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

func TestProviderPublishEventDoesNotCoalesceDifferentSources(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestIndexedDBBackend(t)
	startTestWorkflowHost(t, host)

	provider := New()
	if err := provider.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "10ms"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
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
		PluginName:          "roadmap",
		Status:              proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_PENDING,
		Operation:           "sync",
		Input:               map[string]any{"kind": "schedule"},
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
		PluginName:  "roadmap",
		Status:      proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_RUNNING,
		Operation:   "sync",
		TriggerKind: triggerKindManual,
		CreatedAt:   startedAt.Add(-time.Second),
		StartedAt:   &startedAt,
	}.toRecord()); err != nil {
		t.Fatalf("Put(stale-run): %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close(first): %v", err)
	}

	second := New()
	if err := second.Configure(ctx, "indexeddb", map[string]any{"pollInterval": "1h"}); err != nil {
		t.Fatalf("Configure(second): %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })

	stale, err := second.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{
		RunId: "stale-run",
	})
	if err != nil {
		t.Fatalf("GetRun(stale): %v", err)
	}
	if stale.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED {
		t.Fatalf("stale status = %v, want %v", stale.GetStatus(), proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_FAILED)
	}
	if stale.GetStatusMessage() != "workflow provider restarted while run was in progress" {
		t.Fatalf("stale status message = %q", stale.GetStatusMessage())
	}
}

type workflowHostStub struct {
	proto.UnimplementedWorkflowHostServer

	mu        sync.Mutex
	callsCh   chan *proto.InvokeWorkflowOperationRequest
	callsLog  []*proto.InvokeWorkflowOperationRequest
	releaseCh chan struct{}
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
	s.mu.Unlock()
	s.callsCh <- cloned
	if releaseCh != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-releaseCh:
		}
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
	server := grpc.NewServer()
	proto.RegisterIndexedDBServer(server, store)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() {
		server.GracefulStop()
		_ = lis.Close()
		_ = os.Remove(socketPath)
		_ = store.Close()
	})
	t.Setenv(gestalt.EnvIndexedDBSocket, socketPath)
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
		PluginName: pluginName,
		Operation:  operation,
		Input:      mustStruct(t, input),
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
