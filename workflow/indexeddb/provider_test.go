package indexeddb

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	relationaldb "github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb"
	workflowfake "github.com/valon-technologies/gestalt-providers/workflow/indexeddb/fake"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	gestaltworkflow "github.com/valon-technologies/gestalt/sdk/go/workflow"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestProviderApplyDefinitionStartRunUsesGenerationInputAndProjection(t *testing.T) {
	ctx := gestalt.WithSubject(context.Background(), gestalt.Subject{ID: "service:config"})
	executor := newStepExecutorStub(200, `{"version":1,"status":"succeeded","steps":[{"id":"sync","status":"succeeded"}],"outputs":{"sync":{"ok":true}},"finalStepId":"sync","finalOutput":{"ok":true}}`)
	provider := newTestProvider(t, executor)
	startProviderWorker(t, provider)

	definition, err := provider.ApplyDefinition(ctx, &gestalt.ApplyWorkflowProviderDefinitionRequest{
		Spec: &gestalt.WorkflowDefinitionSpec{
			ID:     "roadmap_sync",
			Target: workflowTarget(t, "roadmap", "sync", map[string]any{"mode": "full"}),
			RunAs:  &gestalt.Subject{ID: "service:roadmap-sync"},
		},
	})
	if err != nil {
		t.Fatalf("ApplyDefinition: %v", err)
	}
	if definition.Generation != 1 {
		t.Fatalf("generation = %d, want 1", definition.Generation)
	}

	runCtx := gestalt.WithSubject(ctx, gestalt.Subject{ID: "user:123"})
	first, err := provider.StartRun(runCtx, &gestalt.StartWorkflowProviderRunRequest{
		DefinitionID:   "roadmap_sync",
		IdempotencyKey: "manual-sync",
		Input:          map[string]any{"tenant": "primary"},
	})
	if err != nil {
		t.Fatalf("StartRun(first): %v", err)
	}
	second, err := provider.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{
		DefinitionID:   "roadmap_sync",
		IdempotencyKey: "manual-sync",
	})
	if err != nil {
		t.Fatalf("StartRun(second): %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("idempotent run ids = %q and %q, want equal", first.ID, second.ID)
	}

	call, err := executor.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("waitForCall: %v", err)
	}
	if call.Request.RunID != first.ID || call.Request.Input["tenant"] != "primary" {
		t.Fatalf("executor call = %#v", call)
	}
	if call.Request.DefinitionID != "roadmap_sync" || call.Request.DefinitionGeneration != 1 {
		t.Fatalf("executor definition = %q/%d, want roadmap_sync/1", call.Request.DefinitionID, call.Request.DefinitionGeneration)
	}
	if call.Request.ProviderName != "indexeddb" || call.Request.RunAs == nil || call.Request.RunAs.ID != "service:roadmap-sync" {
		t.Fatalf("executor request authority = %#v", call.Request)
	}
	if app := testAppStep(call.Request.Target); app == nil || app.Name != "roadmap" || app.Operation != "sync" {
		t.Fatalf("executor target = %#v", call.Request.Target)
	}

	waitForCondition(t, time.Second, func() bool {
		run, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: first.ID})
		return err == nil && run.Status == gestalt.WorkflowRunStatusValueSucceeded
	})
	run, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: first.ID})
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if run.ProviderName != "indexeddb" || run.DefinitionGeneration != 1 || run.Input["tenant"] != "primary" {
		t.Fatalf("run provider/generation/input = %q/%d/%#v", run.ProviderName, run.DefinitionGeneration, run.Input)
	}
	if run.CurrentStepID != "sync" || len(run.Steps) != 1 || run.Steps[0].Status != gestalt.WorkflowStepStatusValueSucceeded {
		t.Fatalf("run steps = %#v", run.Steps)
	}
	output, err := provider.GetRunOutput(ctx, &gestalt.GetWorkflowProviderRunOutputRequest{RunID: first.ID})
	if err != nil {
		t.Fatalf("GetRunOutput: %v", err)
	}
	if got := output.Output.(map[string]any)["ok"]; got != true {
		t.Fatalf("output = %#v, want ok true", output.Output)
	}
	events, err := provider.GetRunEvents(ctx, &gestalt.GetWorkflowProviderRunEventsRequest{RunID: first.ID})
	if err != nil {
		t.Fatalf("GetRunEvents: %v", err)
	}
	if len(events.Events) < 2 {
		t.Fatalf("events = %#v, want run and step events", events.Events)
	}
}

func TestProviderRunsAndPersistsOneDurableStepAtATime(t *testing.T) {
	ctx := context.Background()
	executor := newStepExecutorStub(200, `{"version":1,"status":"succeeded","steps":[{"id":"collect","status":"succeeded"},{"id":"notify","status":"succeeded"}],"outputs":{"collect":{"ok":true},"notify":{"sent":true}},"finalStepId":"notify","finalOutput":{"sent":true}}`)
	provider := newTestProvider(t, executor)
	startProviderWorker(t, provider)

	if _, err := provider.ApplyDefinition(ctx, &gestalt.ApplyWorkflowProviderDefinitionRequest{
		Spec: &gestalt.WorkflowDefinitionSpec{
			ID: "two_step",
			Target: &gestalt.BoundWorkflowTarget{Steps: []gestalt.WorkflowStep{
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
			}},
			RunAs: &gestalt.Subject{ID: "service:two-step"},
		},
	}); err != nil {
		t.Fatalf("ApplyDefinition: %v", err)
	}
	run, err := provider.StartRun(ctx, &gestalt.StartWorkflowProviderRunRequest{DefinitionID: "two_step"})
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	first, err := executor.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("wait first call: %v", err)
	}
	if first.StepIndex != 0 {
		t.Fatalf("first step index = %d, want 0", first.StepIndex)
	}
	second, err := executor.waitForCall(time.Second)
	if err != nil {
		t.Fatalf("wait second call: %v", err)
	}
	if second.StepIndex != 1 {
		t.Fatalf("second step index = %d, want 1", second.StepIndex)
	}
	collectOutput := second.Outputs["collect"].(map[string]any)
	if collectOutput["ok"] != true {
		t.Fatalf("second call outputs = %#v", second.Outputs)
	}

	waitForCondition(t, time.Second, func() bool {
		current, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: run.ID})
		return err == nil && current.Status == gestalt.WorkflowRunStatusValueSucceeded
	})
	current, err := provider.GetRun(ctx, &gestalt.GetWorkflowProviderRunRequest{RunID: run.ID})
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if current.CurrentStepID != "notify" || len(current.Steps) != 2 {
		t.Fatalf("run steps = %#v", current.Steps)
	}
	if current.Steps[0].StepID != "collect" || current.Steps[1].StepID != "notify" {
		t.Fatalf("run steps = %#v", current.Steps)
	}
	output := current.Output.(map[string]any)
	if output["sent"] != true {
		t.Fatalf("run output = %#v", current.Output)
	}
}

func TestProviderSignalOrStartRequiresDefinitionAndCarriesInput(t *testing.T) {
	ctx := context.Background()
	provider := newTestProvider(t, newStepExecutorStub(200, `{"version":1,"status":"succeeded"}`))

	_, err := provider.SignalOrStartRun(ctx, &gestalt.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey: "slack:T1:C1:123",
		Signal:      &gestalt.WorkflowSignal{Name: "slack.message", Payload: map[string]any{"text": "hello"}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("SignalOrStartRun missing definition error = %v, want InvalidArgument", err)
	}

	if _, err := provider.ApplyDefinition(ctx, &gestalt.ApplyWorkflowProviderDefinitionRequest{
		Spec: &gestalt.WorkflowDefinitionSpec{
			ID:     "slack_agent",
			Target: workflowTarget(t, "slack", "reply", nil),
			RunAs:  &gestalt.Subject{ID: "service:slack-agent"},
		},
	}); err != nil {
		t.Fatalf("ApplyDefinition: %v", err)
	}
	resp, err := provider.SignalOrStartRun(ctx, &gestalt.SignalOrStartWorkflowProviderRunRequest{
		WorkflowKey:    "slack:T1:C1:123",
		DefinitionID:   "slack_agent",
		Input:          map[string]any{"channel": "C1"},
		IdempotencyKey: "signal-1",
		Signal:         &gestalt.WorkflowSignal{Name: "slack.message", Payload: map[string]any{"text": "hello"}},
	})
	if err != nil {
		t.Fatalf("SignalOrStartRun: %v", err)
	}
	if resp == nil || resp.Run == nil || !resp.StartedRun || resp.Run.Input["channel"] != "C1" {
		t.Fatalf("signal response = %#v", resp)
	}
}

func TestProviderDeliverEventMatchesActivationMapsInputAndPause(t *testing.T) {
	ctx := gestalt.WithSubject(context.Background(), gestalt.Subject{ID: "service:config"})
	provider := newTestProvider(t, newStepExecutorStub(200, `{"version":1,"status":"succeeded"}`))

	if _, err := provider.ApplyDefinition(ctx, &gestalt.ApplyWorkflowProviderDefinitionRequest{
		Spec: &gestalt.WorkflowDefinitionSpec{
			ID:     "roadmap_event",
			Target: workflowTarget(t, "roadmap", "sync", nil),
			RunAs:  &gestalt.Subject{ID: "service:roadmap-event"},
			Activations: []gestalt.WorkflowActivation{{
				ID: "item_updated",
				Event: &gestalt.WorkflowEventActivation{Match: &gestalt.WorkflowEventMatch{
					Type:   "roadmap.item.updated",
					Source: "roadmap",
				}},
				Input: gestalt.WorkflowValue{Object: map[string]gestalt.WorkflowValue{
					"item": {Signal: "data.item"},
				}},
			}},
		},
	}); err != nil {
		t.Fatalf("ApplyDefinition: %v", err)
	}
	event := &gestalt.WorkflowEvent{
		ID:     "evt-1",
		Source: "roadmap",
		Type:   "roadmap.item.updated",
		Data:   map[string]any{"item": map[string]any{"id": "item-1"}},
	}
	deliverCtx := gestalt.WithSubject(ctx, gestalt.Subject{ID: "service:roadmap"})
	if _, err := provider.DeliverEvent(deliverCtx, &gestalt.DeliverWorkflowProviderEventRequest{
		Event: event,
	}); err != nil {
		t.Fatalf("DeliverEvent: %v", err)
	}
	runs, err := provider.ListRuns(ctx, &gestalt.ListWorkflowProviderRunsRequest{})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if len(runs.Runs) != 1 {
		t.Fatalf("runs = %#v, want one event run", runs.Runs)
	}
	if runs.Runs[0].ProviderName != "indexeddb" || runs.Runs[0].RunAs == nil || runs.Runs[0].RunAs.ID != "service:roadmap-event" {
		t.Fatalf("event run authority = %#v", runs.Runs[0])
	}
	item := runs.Runs[0].Input["item"].(map[string]any)
	if item["id"] != "item-1" {
		t.Fatalf("run input = %#v", runs.Runs[0].Input)
	}
	if runs.Runs[0].Trigger == nil || runs.Runs[0].Trigger.Event == nil || runs.Runs[0].Trigger.Event.ActivationID != "item_updated" {
		t.Fatalf("run trigger = %#v", runs.Runs[0].Trigger)
	}

	if _, err := provider.SetActivationPaused(ctx, &gestalt.SetWorkflowProviderActivationPausedRequest{
		DefinitionID: "roadmap_event",
		ActivationID: "item_updated",
		Paused:       true,
	}); err != nil {
		t.Fatalf("SetActivationPaused: %v", err)
	}
	event.ID = "evt-2"
	if _, err := provider.DeliverEvent(ctx, &gestalt.DeliverWorkflowProviderEventRequest{Event: event}); err != nil {
		t.Fatalf("DeliverEvent(paused): %v", err)
	}
	runs, err = provider.ListRuns(ctx, &gestalt.ListWorkflowProviderRunsRequest{})
	if err != nil {
		t.Fatalf("ListRuns(after pause): %v", err)
	}
	if len(runs.Runs) != 1 {
		t.Fatalf("runs after paused activation = %#v, want unchanged", runs.Runs)
	}
}

type stepExecutorStub struct {
	mu       sync.Mutex
	callsCh  chan gestaltworkflow.StepRequest
	callsLog []gestaltworkflow.StepRequest
	status   int32
	body     string
}

func newStepExecutorStub(status int32, body string) *stepExecutorStub {
	return &stepExecutorStub{
		callsCh: make(chan gestaltworkflow.StepRequest, 16),
		status:  status,
		body:    body,
	}
}

func (s *stepExecutorStub) Execute(context.Context, gestaltworkflow.Request) (*gestaltworkflow.Response, error) {
	return &gestaltworkflow.Response{Status: int(s.status), Body: s.body}, nil
}

func (s *stepExecutorStub) ExecuteStep(_ context.Context, req gestaltworkflow.StepRequest) (*gestaltworkflow.StepResponse, error) {
	cloned := cloneJSONValue(req)
	s.mu.Lock()
	s.callsLog = append(s.callsLog, cloned)
	s.mu.Unlock()
	s.callsCh <- cloned
	return stepResponseFromStubBody(req, int(s.status), s.body), nil
}

func stepResponseFromStubBody(req gestaltworkflow.StepRequest, statusCode int, body string) *gestaltworkflow.StepResponse {
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

func (s *stepExecutorStub) waitForCall(timeout time.Duration) (*gestaltworkflow.StepRequest, error) {
	select {
	case call := <-s.callsCh:
		return &call, nil
	case <-time.After(timeout):
		return nil, context.DeadlineExceeded
	}
}

func (s *stepExecutorStub) Close() error {
	return nil
}

func newTestProvider(t *testing.T, executor gestaltworkflow.StepExecutor) *Provider {
	t.Helper()
	provider := newProviderCoreWithDB(startTestIndexedDBBackend(t))
	provider.workflowExecutor = executor
	if err := provider.Configure(context.Background(), "indexeddb", map[string]any{"pollInterval": "10ms"}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	return provider
}

func startTestIndexedDBBackend(t *testing.T) indexeddb.Database {
	t.Helper()
	store := relationaldb.New()
	if err := store.Configure(context.Background(), "workflow_state", map[string]any{
		"dsn": "file:" + filepath.Join(t.TempDir(), "workflow.sqlite") + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)",
	}); err != nil {
		t.Fatalf("relationaldb.Configure: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	seedWorkflowObjectStores(t, store)
	return workflowfake.NewProviderDB(store)
}

func seedWorkflowObjectStores(t *testing.T, store *relationaldb.Provider) {
	t.Helper()
	for _, def := range []struct {
		name   string
		schema gestalt.ObjectStoreOptions
	}{
		{name: storeSchedules, schema: gestalt.ObjectStoreOptions{}},
		{name: storeDefinitions, schema: gestalt.ObjectStoreOptions{}},
		{name: storeIdempotency, schema: gestalt.ObjectStoreOptions{}},
		{name: storeWorkflowKeys, schema: gestalt.ObjectStoreOptions{}},
		{name: storeRuns, schema: gestalt.ObjectStoreOptions{}},
		{name: storeRunClaims, schema: workflowRunClaimSchema()},
		{name: storeSignals, schema: workflowSignalSchema()},
	} {
		if err := store.CreateObjectStore(context.Background(), def.name, def.schema); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
			t.Fatalf("CreateObjectStore(%s): %v", def.name, err)
		}
	}
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
