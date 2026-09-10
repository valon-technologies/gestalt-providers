package temporal

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	gestaltworkflow "github.com/valon-technologies/gestalt/sdk/go/workflow"
	"go.temporal.io/sdk/testsuite"
)

func TestTemporalRunReusesStepIdentityAfterLostResponse(t *testing.T) {
	app := &retryContractApp{loseFirstResponse: true}
	run := executeRetryContractWorkflow(t, app, retryContractTarget(120))

	if run.Status != gestalt.WorkflowRunStatusValueSucceeded {
		t.Fatalf("run = %#v, want succeeded", run)
	}
	if got := app.callCount(); got != 2 {
		t.Fatalf("app calls = %d, want 2", got)
	}
	if got := app.effectCount(); got != 1 {
		t.Fatalf("side effects = %d, want 1", got)
	}
	keys := app.idempotencyKeys()
	if len(keys) != 2 || keys[0] == "" || keys[0] != keys[1] {
		t.Fatalf("idempotency keys = %q, want the same non-empty key", keys)
	}
}

func TestTemporalRunRetriesTransientFailureBeyondFiveAttempts(t *testing.T) {
	app := &retryContractApp{failuresRemaining: 6}
	run := executeRetryContractWorkflow(t, app, retryContractTarget(300))

	if run.Status != gestalt.WorkflowRunStatusValueSucceeded {
		t.Fatalf("run = %#v, want succeeded", run)
	}
	if got := app.callCount(); got != 7 {
		t.Fatalf("app calls = %d, want 7", got)
	}
	if got := app.effectCount(); got != 1 {
		t.Fatalf("side effects = %d, want 1", got)
	}
	keys := app.idempotencyKeys()
	if keys[0] == "" {
		t.Fatal("idempotency key is empty")
	}
	for _, key := range keys[1:] {
		if key != keys[0] {
			t.Fatalf("idempotency keys = %q, want one stable key", keys)
		}
	}
}

type retryContractApp struct {
	mu                sync.Mutex
	loseFirstResponse bool
	failuresRemaining int
	calls             []string
	effects           map[string]struct{}
}

func (a *retryContractApp) InvokeWorkflowApp(_ context.Context, invocation gestaltworkflow.AppInvocation) (*gestaltworkflow.AppResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	key := invocation.IdempotencyKey
	a.calls = append(a.calls, key)
	if a.failuresRemaining > 0 {
		a.failuresRemaining--
		return nil, errors.New("redaction unavailable")
	}
	if a.effects == nil {
		a.effects = map[string]struct{}{}
	}
	if _, committed := a.effects[key]; !committed {
		a.effects[key] = struct{}{}
		if a.loseFirstResponse {
			a.loseFirstResponse = false
			return nil, context.Canceled
		}
	}
	return &gestaltworkflow.AppResult{Status: http.StatusOK, Body: `{"redacted":true}`}, nil
}

func (a *retryContractApp) callCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.calls)
}

func (a *retryContractApp) effectCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.effects)
}

func (a *retryContractApp) idempotencyKeys() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.calls...)
}

func retryContractTarget(timeoutSeconds int32) *gestalt.BoundWorkflowTarget {
	target := nativeAppTargetInput("documents", "redact")
	target.Steps[0].TimeoutSeconds = timeoutSeconds
	return target
}

func executeRetryContractWorkflow(
	t *testing.T,
	app gestaltworkflow.AppInvoker,
	target *gestalt.BoundWorkflowTarget,
) *gestalt.WorkflowRun {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := newTestWorkflowEnvironment(&suite)
	env.RegisterWorkflow(TemporalRun)
	executor := gestaltworkflow.New(gestaltworkflow.Config{AppInvoker: app})
	env.RegisterActivity(&workflowActivities{executor: executor})

	env.ExecuteWorkflow(TemporalRun, runWorkflowInput{
		ActivityStartToCloseTimeoutNS: time.Minute,
		ScopeID:                       "scope",
		ProviderName:                  "temporal",
		DefinitionID:                  "definition-1",
		DefinitionGeneration:          1,
		RunAs:                         runAsID("service:workflow-test"),
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
	return &run
}

var _ gestaltworkflow.AppInvoker = (*retryContractApp)(nil)
