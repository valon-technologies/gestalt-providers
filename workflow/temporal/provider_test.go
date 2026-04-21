package temporal

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	gproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestProviderStartRunUsesIdempotencyAndExecutesHostCallbacks(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	provider := newTestProvider(t, host)

	first, err := provider.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		PluginName:     "roadmap",
		IdempotencyKey: "manual-sync",
		Target:         protoBoundTarget(t, "roadmap", "sync", map[string]any{"mode": "full"}),
		CreatedBy:      &proto.WorkflowActor{SubjectId: "user:123", SubjectKind: "user", DisplayName: "Ada"},
	})
	if err != nil {
		t.Fatalf("StartRun(first): %v", err)
	}
	second, err := provider.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		PluginName:     "roadmap",
		IdempotencyKey: "manual-sync",
		Target:         protoBoundTarget(t, "roadmap", "sync", map[string]any{"mode": "full"}),
	})
	if err != nil {
		t.Fatalf("StartRun(second): %v", err)
	}
	if first.GetId() != second.GetId() {
		t.Fatalf("idempotent run ids = %q and %q, want equal", first.GetId(), second.GetId())
	}

	call, err := host.waitForCall(5 * time.Second)
	if err != nil {
		t.Fatalf("waitForCall: %v", err)
	}
	if call.GetPluginName() != "roadmap" {
		t.Fatalf("plugin_name = %q, want roadmap", call.GetPluginName())
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

	waitForCondition(t, 10*time.Second, func() bool {
		run, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{
			PluginName: "roadmap",
			RunId:      first.GetId(),
		})
		return err == nil && run.GetStatus() == proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED
	})

	runs, err := provider.ListRuns(ctx, &proto.ListWorkflowProviderRunsRequest{PluginName: "roadmap"})
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

func TestProviderCancelRunCancelsInFlightHostCallback(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	host.block()
	provider := newTestProvider(t, host)

	run, err := provider.StartRun(ctx, &proto.StartWorkflowProviderRunRequest{
		PluginName: "roadmap",
		Target:     protoBoundTarget(t, "roadmap", "sync", map[string]any{"mode": "blocked"}),
	})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	if _, err := host.waitForCall(5 * time.Second); err != nil {
		t.Fatalf("waitForCall: %v", err)
	}
	canceled, err := provider.CancelRun(ctx, &proto.CancelWorkflowProviderRunRequest{
		PluginName: "roadmap",
		RunId:      run.GetId(),
		Reason:     "skip",
	})
	if err != nil {
		t.Fatalf("CancelRun: %v", err)
	}
	if canceled.GetStatus() != proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_CANCELED {
		t.Fatalf("canceled status = %v, want %v", canceled.GetStatus(), proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_CANCELED)
	}

	waitForCondition(t, 10*time.Second, func() bool {
		current, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{
			PluginName: "roadmap",
			RunId:      run.GetId(),
		})
		return err == nil &&
			current.GetStatus() == proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_CANCELED &&
			current.GetStatusMessage() == "skip"
	})
	host.release()
}

func TestProviderStartRunIdempotencyRaceReturnsPersistedRun(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	provider := newTestProvider(t, host)

	start := make(chan struct{})
	type startResult struct {
		run *proto.BoundWorkflowRun
		err error
	}
	results := make(chan startResult, 2)
	requests := []*proto.StartWorkflowProviderRunRequest{
		{
			PluginName:     "roadmap",
			IdempotencyKey: "manual-sync-race",
			Target:         protoBoundTarget(t, "roadmap", "sync", map[string]any{"mode": "alpha"}),
			CreatedBy:      &proto.WorkflowActor{SubjectId: "user:alpha", SubjectKind: "user", DisplayName: "Alpha"},
		},
		{
			PluginName:     "roadmap",
			IdempotencyKey: "manual-sync-race",
			Target:         protoBoundTarget(t, "roadmap", "sync", map[string]any{"mode": "beta"}),
			CreatedBy:      &proto.WorkflowActor{SubjectId: "user:beta", SubjectKind: "user", DisplayName: "Beta"},
		},
	}
	for _, req := range requests {
		req := req
		go func() {
			<-start
			run, err := provider.StartRun(ctx, req)
			results <- startResult{run: run, err: err}
		}()
	}
	close(start)

	first := <-results
	second := <-results
	if first.err != nil {
		t.Fatalf("StartRun(first): %v", first.err)
	}
	if second.err != nil {
		t.Fatalf("StartRun(second): %v", second.err)
	}
	if first.run.GetId() != second.run.GetId() {
		t.Fatalf("idempotent run ids = %q and %q, want equal", first.run.GetId(), second.run.GetId())
	}

	persisted, err := provider.GetRun(ctx, &proto.GetWorkflowProviderRunRequest{
		PluginName: "roadmap",
		RunId:      first.run.GetId(),
	})
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	for _, result := range []*proto.BoundWorkflowRun{first.run, second.run} {
		if !reflect.DeepEqual(result.GetTarget().GetInput().AsMap(), persisted.GetTarget().GetInput().AsMap()) {
			t.Fatalf("StartRun returned input %#v, want persisted %#v", result.GetTarget().GetInput().AsMap(), persisted.GetTarget().GetInput().AsMap())
		}
		if result.GetCreatedBy().GetSubjectId() != persisted.GetCreatedBy().GetSubjectId() {
			t.Fatalf("StartRun returned created_by %q, want persisted %q", result.GetCreatedBy().GetSubjectId(), persisted.GetCreatedBy().GetSubjectId())
		}
	}
}

func TestProviderPublishEventDoesNotCoalesceDifferentSources(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	provider := newTestProvider(t, host)

	if _, err := provider.UpsertEventTrigger(ctx, &proto.UpsertWorkflowProviderEventTriggerRequest{
		PluginName: "roadmap",
		TriggerId:  "refresh-trigger",
		Match:      &proto.WorkflowEventMatch{Type: "task.updated"},
		Target:     protoBoundTarget(t, "roadmap", "sync", map[string]any{"kind": "event"}),
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

	first, err := host.waitForCall(5 * time.Second)
	if err != nil {
		t.Fatalf("waitForCall(first): %v", err)
	}
	second, err := host.waitForCall(5 * time.Second)
	if err != nil {
		t.Fatalf("waitForCall(second): %v", err)
	}
	if first.GetRunId() == second.GetRunId() {
		t.Fatalf("run ids = %q and %q, want distinct per source", first.GetRunId(), second.GetRunId())
	}

	waitForCondition(t, 10*time.Second, func() bool {
		runs, err := provider.ListRuns(ctx, &proto.ListWorkflowProviderRunsRequest{PluginName: "roadmap"})
		return err == nil && len(runs.GetRuns()) == 2
	})
}

func TestProviderPublishEventSeesTriggersCreatedByAnotherReplica(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	name := "temporal-shared-" + sanitizeIDSegment(t.Name()) + "-" + uuid.NewString()[:8]
	publisher := newTestProviderNamed(t, host, name)
	owner := newTestProviderNamed(t, host, name)

	if _, err := owner.UpsertEventTrigger(ctx, &proto.UpsertWorkflowProviderEventTriggerRequest{
		PluginName: "roadmap",
		TriggerId:  "refresh-trigger",
		Match:      &proto.WorkflowEventMatch{Type: "task.updated"},
		Target:     protoBoundTarget(t, "roadmap", "sync", map[string]any{"kind": "event"}),
	}); err != nil {
		t.Fatalf("UpsertEventTrigger: %v", err)
	}

	if _, err := publisher.PublishEvent(ctx, &proto.PublishWorkflowProviderEventRequest{
		PluginName: "roadmap",
		Event: &proto.WorkflowEvent{
			Id:          "task-123",
			Source:      "tasks",
			Type:        "task.updated",
			SpecVersion: "1.0",
		},
	}); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}

	if _, err := host.waitForCall(5 * time.Second); err != nil {
		t.Fatalf("waitForCall: %v", err)
	}
}

func TestProviderDeletedTriggerDoesNotResurfaceFromAnotherReplica(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	name := "temporal-shared-" + sanitizeIDSegment(t.Name()) + "-" + uuid.NewString()[:8]
	creator := newTestProviderNamed(t, host, name)
	deleter := newTestProviderNamed(t, host, name)

	if _, err := creator.UpsertEventTrigger(ctx, &proto.UpsertWorkflowProviderEventTriggerRequest{
		PluginName: "roadmap",
		TriggerId:  "refresh-trigger",
		Match:      &proto.WorkflowEventMatch{Type: "task.updated"},
		Target:     protoBoundTarget(t, "roadmap", "sync", map[string]any{"kind": "event"}),
	}); err != nil {
		t.Fatalf("UpsertEventTrigger: %v", err)
	}
	if _, err := deleter.DeleteEventTrigger(ctx, &proto.DeleteWorkflowProviderEventTriggerRequest{
		PluginName: "roadmap",
		TriggerId:  "refresh-trigger",
	}); err != nil {
		t.Fatalf("DeleteEventTrigger: %v", err)
	}

	waitForCondition(t, 10*time.Second, func() bool {
		triggers, err := creator.ListEventTriggers(ctx, &proto.ListWorkflowProviderEventTriggersRequest{PluginName: "roadmap"})
		return err == nil && len(triggers.GetTriggers()) == 0
	})

	if _, err := creator.PublishEvent(ctx, &proto.PublishWorkflowProviderEventRequest{
		PluginName: "roadmap",
		Event: &proto.WorkflowEvent{
			Id:          "task-deleted",
			Source:      "tasks",
			Type:        "task.updated",
			SpecVersion: "1.0",
		},
	}); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	if _, err := host.waitForCall(500 * time.Millisecond); err != context.DeadlineExceeded {
		t.Fatalf("waitForCall error = %v, want %v", err, context.DeadlineExceeded)
	}
}

func TestProviderPublishEventSeesTriggerRecreatedByAnotherReplica(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	name := "temporal-shared-" + sanitizeIDSegment(t.Name()) + "-" + uuid.NewString()[:8]
	replicaA := newTestProviderNamed(t, host, name)
	replicaB := newTestProviderNamed(t, host, name)

	if _, err := replicaA.UpsertEventTrigger(ctx, &proto.UpsertWorkflowProviderEventTriggerRequest{
		PluginName: "roadmap",
		TriggerId:  "refresh-trigger",
		Match:      &proto.WorkflowEventMatch{Type: "task.updated"},
		Target:     protoBoundTarget(t, "roadmap", "sync", map[string]any{"kind": "initial"}),
	}); err != nil {
		t.Fatalf("UpsertEventTrigger(initial): %v", err)
	}
	if _, err := replicaB.DeleteEventTrigger(ctx, &proto.DeleteWorkflowProviderEventTriggerRequest{
		PluginName: "roadmap",
		TriggerId:  "refresh-trigger",
	}); err != nil {
		t.Fatalf("DeleteEventTrigger: %v", err)
	}
	if _, err := replicaA.UpsertEventTrigger(ctx, &proto.UpsertWorkflowProviderEventTriggerRequest{
		PluginName: "roadmap",
		TriggerId:  "refresh-trigger",
		Match:      &proto.WorkflowEventMatch{Type: "task.updated"},
		Target:     protoBoundTarget(t, "roadmap", "sync", map[string]any{"kind": "recreated"}),
	}); err != nil {
		t.Fatalf("UpsertEventTrigger(recreated): %v", err)
	}

	if _, err := replicaB.PublishEvent(ctx, &proto.PublishWorkflowProviderEventRequest{
		PluginName: "roadmap",
		Event: &proto.WorkflowEvent{
			Id:          "task-456",
			Source:      "tasks",
			Type:        "task.updated",
			SpecVersion: "1.0",
		},
	}); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}

	call, err := host.waitForCall(5 * time.Second)
	if err != nil {
		t.Fatalf("waitForCall: %v", err)
	}
	if got := call.GetTarget().GetInput().AsMap()["kind"]; got != "recreated" {
		t.Fatalf("target.input.kind = %v, want recreated", got)
	}
}

func TestProviderPublishEventAfterLocalDeleteRecreateUsesNewTrigger(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	provider := newTestProvider(t, host)
	event := &proto.WorkflowEvent{
		Id:          "task-789",
		Source:      "tasks",
		Type:        "task.updated",
		SpecVersion: "1.0",
	}

	if _, err := provider.UpsertEventTrigger(ctx, &proto.UpsertWorkflowProviderEventTriggerRequest{
		PluginName: "roadmap",
		TriggerId:  "refresh-trigger",
		Match:      &proto.WorkflowEventMatch{Type: "task.updated"},
		Target:     protoBoundTarget(t, "roadmap", "sync", map[string]any{"kind": "initial"}),
	}); err != nil {
		t.Fatalf("UpsertEventTrigger(initial): %v", err)
	}
	if _, err := provider.PublishEvent(ctx, &proto.PublishWorkflowProviderEventRequest{
		PluginName: "roadmap",
		Event:      event,
	}); err != nil {
		t.Fatalf("PublishEvent(initial): %v", err)
	}
	firstCall, err := host.waitForCall(5 * time.Second)
	if err != nil {
		t.Fatalf("waitForCall(initial): %v", err)
	}
	if got := firstCall.GetTarget().GetInput().AsMap()["kind"]; got != "initial" {
		t.Fatalf("target.input.kind = %v, want initial", got)
	}
	if _, err := provider.DeleteEventTrigger(ctx, &proto.DeleteWorkflowProviderEventTriggerRequest{
		PluginName: "roadmap",
		TriggerId:  "refresh-trigger",
	}); err != nil {
		t.Fatalf("DeleteEventTrigger: %v", err)
	}
	if _, err := provider.UpsertEventTrigger(ctx, &proto.UpsertWorkflowProviderEventTriggerRequest{
		PluginName: "roadmap",
		TriggerId:  "refresh-trigger",
		Match:      &proto.WorkflowEventMatch{Type: "task.updated"},
		Target:     protoBoundTarget(t, "roadmap", "sync", map[string]any{"kind": "recreated"}),
	}); err != nil {
		t.Fatalf("UpsertEventTrigger(recreated): %v", err)
	}

	if _, err := provider.PublishEvent(ctx, &proto.PublishWorkflowProviderEventRequest{
		PluginName: "roadmap",
		Event:      event,
	}); err != nil {
		t.Fatalf("PublishEvent(recreated): %v", err)
	}

	call, err := host.waitForCall(5 * time.Second)
	if err != nil {
		t.Fatalf("waitForCall(recreated): %v", err)
	}
	if got := call.GetTarget().GetInput().AsMap()["kind"]; got != "recreated" {
		t.Fatalf("target.input.kind = %v, want recreated", got)
	}
}

func TestProviderRejectsCrossPluginScheduleAndTriggerIDCollisions(t *testing.T) {
	ctx := context.Background()
	provider := newTestProvider(t, newWorkflowHostStub(202, `{"ok":true}`))

	if _, err := provider.UpsertSchedule(ctx, &proto.UpsertWorkflowProviderScheduleRequest{
		PluginName: "roadmap",
		ScheduleId: "shared-id",
		Cron:       "*/5 * * * *",
		Timezone:   "UTC",
		Target:     protoBoundTarget(t, "roadmap", "sync", map[string]any{"kind": "schedule"}),
	}); err != nil {
		t.Fatalf("UpsertSchedule(roadmap): %v", err)
	}
	if _, err := provider.UpsertSchedule(ctx, &proto.UpsertWorkflowProviderScheduleRequest{
		PluginName: "billing",
		ScheduleId: "shared-id",
		Cron:       "*/5 * * * *",
		Timezone:   "UTC",
		Target:     protoBoundTarget(t, "billing", "sync", map[string]any{"kind": "schedule"}),
	}); err == nil {
		t.Fatal("UpsertSchedule(billing) succeeded, want cross-plugin collision error")
	}

	if _, err := provider.UpsertEventTrigger(ctx, &proto.UpsertWorkflowProviderEventTriggerRequest{
		PluginName: "roadmap",
		TriggerId:  "shared-trigger",
		Match:      &proto.WorkflowEventMatch{Type: "task.updated"},
		Target:     protoBoundTarget(t, "roadmap", "sync", map[string]any{"kind": "event"}),
	}); err != nil {
		t.Fatalf("UpsertEventTrigger(roadmap): %v", err)
	}
	if _, err := provider.UpsertEventTrigger(ctx, &proto.UpsertWorkflowProviderEventTriggerRequest{
		PluginName: "billing",
		TriggerId:  "shared-trigger",
		Match:      &proto.WorkflowEventMatch{Type: "invoice.updated"},
		Target:     protoBoundTarget(t, "billing", "sync", map[string]any{"kind": "event"}),
	}); err == nil {
		t.Fatal("UpsertEventTrigger(billing) succeeded, want cross-plugin collision error")
	}
}

func TestProviderNamesDoNotCollideAfterSanitization(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	startTestWorkflowHost(t, host)
	hostPort, namespace := testTemporalEndpoint(t)

	providerA := New()
	if err := providerA.Configure(ctx, "foo.bar", map[string]any{
		"hostPort":        hostPort,
		"namespace":       namespace,
		"activityTimeout": "20s",
		"runTimeout":      "30s",
	}); err != nil {
		t.Fatalf("Configure(foo.bar): %v", err)
	}
	t.Cleanup(func() { _ = providerA.Close() })

	providerB := New()
	if err := providerB.Configure(ctx, "foo-bar", map[string]any{
		"hostPort":        hostPort,
		"namespace":       namespace,
		"activityTimeout": "20s",
		"runTimeout":      "30s",
	}); err != nil {
		t.Fatalf("Configure(foo-bar): %v", err)
	}
	t.Cleanup(func() { _ = providerB.Close() })

	_, cfgA, prefixA, err := providerA.state()
	if err != nil {
		t.Fatalf("state(foo.bar): %v", err)
	}
	_, cfgB, prefixB, err := providerB.state()
	if err != nil {
		t.Fatalf("state(foo-bar): %v", err)
	}
	if prefixA == prefixB {
		t.Fatalf("execution prefixes = %q and %q, want distinct", prefixA, prefixB)
	}
	if cfgA.TaskQueue == cfgB.TaskQueue {
		t.Fatalf("task queues = %q and %q, want distinct", cfgA.TaskQueue, cfgB.TaskQueue)
	}

	if _, err := providerA.UpsertSchedule(ctx, &proto.UpsertWorkflowProviderScheduleRequest{
		PluginName: "alpha",
		ScheduleId: "shared-id",
		Cron:       "*/5 * * * *",
		Timezone:   "UTC",
		Target:     protoBoundTarget(t, "alpha", "sync", map[string]any{"kind": "schedule-a"}),
	}); err != nil {
		t.Fatalf("UpsertSchedule(foo.bar): %v", err)
	}
	if _, err := providerB.UpsertSchedule(ctx, &proto.UpsertWorkflowProviderScheduleRequest{
		PluginName: "beta",
		ScheduleId: "shared-id",
		Cron:       "*/5 * * * *",
		Timezone:   "UTC",
		Target:     protoBoundTarget(t, "beta", "sync", map[string]any{"kind": "schedule-b"}),
	}); err != nil {
		t.Fatalf("UpsertSchedule(foo-bar): %v", err)
	}
}

func TestProviderScheduleDispatchesRunOnNextMinuteBoundary(t *testing.T) {
	ctx := context.Background()
	host := newWorkflowHostStub(202, `{"ok":true}`)
	provider := newTestProvider(t, host)

	schedule, err := provider.UpsertSchedule(ctx, &proto.UpsertWorkflowProviderScheduleRequest{
		PluginName: "roadmap",
		ScheduleId: "every-minute",
		Cron:       "* * * * *",
		Timezone:   "UTC",
		Target:     protoBoundTarget(t, "roadmap", "sync", map[string]any{"kind": "schedule"}),
	})
	if err != nil {
		t.Fatalf("UpsertSchedule: %v", err)
	}
	if schedule.GetNextRunAt() == nil {
		t.Fatal("next_run_at is nil")
	}

	call, err := host.waitForCall(75 * time.Second)
	if err != nil {
		t.Fatalf("waitForCall(schedule): %v", err)
	}
	if call.GetTrigger().GetSchedule() == nil {
		t.Fatalf("trigger = %#v, want schedule trigger", call.GetTrigger())
	}
	scheduledFor := call.GetTrigger().GetSchedule().GetScheduledFor().AsTime()
	if scheduledFor.Second() != 0 {
		t.Fatalf("scheduled_for second = %d, want minute boundary", scheduledFor.Second())
	}

	waitForCondition(t, 10*time.Second, func() bool {
		runs, err := provider.ListRuns(ctx, &proto.ListWorkflowProviderRunsRequest{PluginName: "roadmap"})
		if err != nil || len(runs.GetRuns()) == 0 {
			return false
		}
		for _, run := range runs.GetRuns() {
			if run.GetTrigger().GetSchedule() != nil && run.GetStatus() == proto.WorkflowRunStatus_WORKFLOW_RUN_STATUS_SUCCEEDED {
				return true
			}
		}
		return false
	})

	updated, err := provider.GetSchedule(ctx, &proto.GetWorkflowProviderScheduleRequest{
		PluginName: "roadmap",
		ScheduleId: "every-minute",
	})
	if err != nil {
		t.Fatalf("GetSchedule: %v", err)
	}
	if !updated.GetNextRunAt().AsTime().After(scheduledFor) {
		t.Fatalf("next_run_at = %v, want after %v", updated.GetNextRunAt().AsTime(), scheduledFor)
	}
}

func TestProviderPauseScheduleTimesOutWithoutWorker(t *testing.T) {
	ctx := context.Background()
	provider := newTestProvider(t, newWorkflowHostStub(202, `{"ok":true}`))

	if _, err := provider.UpsertSchedule(ctx, &proto.UpsertWorkflowProviderScheduleRequest{
		PluginName: "roadmap",
		ScheduleId: "pause-timeout",
		Cron:       "*/5 * * * *",
		Timezone:   "UTC",
		Target:     protoBoundTarget(t, "roadmap", "sync", map[string]any{"kind": "schedule"}),
	}); err != nil {
		t.Fatalf("UpsertSchedule: %v", err)
	}

	provider.worker.Stop()
	_, err := provider.PauseSchedule(ctx, &proto.PauseWorkflowProviderScheduleRequest{
		PluginName: "roadmap",
		ScheduleId: "pause-timeout",
	})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("PauseSchedule error = %v, want code %v", err, codes.DeadlineExceeded)
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

func (s *workflowHostStub) block() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.releaseCh == nil {
		s.releaseCh = make(chan struct{})
	}
}

func (s *workflowHostStub) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.releaseCh != nil {
		close(s.releaseCh)
		s.releaseCh = nil
	}
}

func newTestProvider(t *testing.T, host proto.WorkflowHostServer) *Provider {
	t.Helper()
	return newTestProviderNamed(t, host, "")
}

func newTestProviderNamed(t *testing.T, host proto.WorkflowHostServer, providerName string) *Provider {
	t.Helper()
	startTestWorkflowHost(t, host)
	hostPort, namespace := testTemporalEndpoint(t)

	provider := New()
	name := providerName
	if name == "" {
		name = "temporal-" + sanitizeIDSegment(t.Name()) + "-" + uuid.NewString()[:8]
	}
	taskQueue := "tq-" + sanitizeIDSegment(t.Name()) + "-" + uuid.NewString()[:8]
	if err := provider.Configure(context.Background(), name, map[string]any{
		"hostPort":        hostPort,
		"namespace":       namespace,
		"taskQueue":       taskQueue,
		"activityTimeout": "20s",
		"runTimeout":      "30s",
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	return provider
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
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not satisfied before timeout")
}

var (
	testTemporalOnce sync.Once
	testTemporalAddr string
	testTemporalNS   string
	testTemporalSrv  *testsuite.DevServer
	testTemporalErr  error
)

func testTemporalEndpoint(t *testing.T) (string, string) {
	t.Helper()
	if addr := firstNonEmpty(
		os.Getenv("GESTALT_TEST_TEMPORAL_ADDRESS"),
		os.Getenv("GESTALT_TEST_TEMPORAL_HOST_PORT"),
		os.Getenv("TEMPORAL_ADDRESS"),
		os.Getenv("TEMPORAL_HOST_PORT"),
	); addr != "" {
		ns := firstNonEmpty(
			os.Getenv("GESTALT_TEST_TEMPORAL_NAMESPACE"),
			os.Getenv("TEMPORAL_NAMESPACE"),
			defaultNamespace,
		)
		return addr, ns
	}

	testTemporalOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		testTemporalNS = defaultNamespace
		testTemporalSrv, testTemporalErr = testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
			ClientOptions: &client.Options{Namespace: testTemporalNS},
			LogLevel:      "error",
		})
		if testTemporalErr == nil {
			testTemporalAddr = testTemporalSrv.FrontendHostPort()
		}
	})
	if testTemporalErr != nil {
		t.Fatalf("StartDevServer: %v", testTemporalErr)
	}
	return testTemporalAddr, testTemporalNS
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func TestMain(m *testing.M) {
	code := m.Run()
	if testTemporalSrv != nil {
		_ = testTemporalSrv.Stop()
	}
	os.Exit(code)
}
