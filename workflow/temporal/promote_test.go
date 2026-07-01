package temporal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// fakeDeploymentHandle is a scriptable WorkerDeploymentHandle for promotion tests.
// currentBuildID returns the routing config's current build ID for the Nth
// Describe call (0-indexed); an empty string means no current version.
type fakeDeploymentHandle struct {
	client.WorkerDeploymentHandle

	mu             sync.Mutex
	deploymentName string
	conflictToken  []byte
	setErr         error
	currentBuildID func(call int) string

	describeCalls int
	setCalls      []client.WorkerDeploymentSetCurrentVersionOptions
	onDescribe    func()
}

func (h *fakeDeploymentHandle) Describe(_ context.Context, _ client.WorkerDeploymentDescribeOptions) (client.WorkerDeploymentDescribeResponse, error) {
	h.mu.Lock()
	call := h.describeCalls
	h.describeCalls++
	build := ""
	if h.currentBuildID != nil {
		build = h.currentBuildID(call)
	}
	resp := client.WorkerDeploymentDescribeResponse{ConflictToken: h.conflictToken}
	if build != "" {
		resp.Info.RoutingConfig.CurrentVersion = &worker.WorkerDeploymentVersion{
			DeploymentName: h.deploymentName,
			BuildID:        build,
		}
	}
	hook := h.onDescribe
	h.mu.Unlock()
	if hook != nil {
		hook()
	}
	return resp, nil
}

func (h *fakeDeploymentHandle) SetCurrentVersion(_ context.Context, opts client.WorkerDeploymentSetCurrentVersionOptions) (client.WorkerDeploymentSetCurrentVersionResponse, error) {
	h.mu.Lock()
	h.setCalls = append(h.setCalls, opts)
	err := h.setErr
	h.mu.Unlock()
	return client.WorkerDeploymentSetCurrentVersionResponse{}, err
}

func (h *fakeDeploymentHandle) setCallCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.setCalls)
}

func (h *fakeDeploymentHandle) describeCallCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.describeCalls
}

type fakeDeploymentClient struct {
	client.WorkerDeploymentClient
	handle *fakeDeploymentHandle
}

func (c *fakeDeploymentClient) GetHandle(name string) client.WorkerDeploymentHandle {
	c.handle.mu.Lock()
	c.handle.deploymentName = name
	c.handle.mu.Unlock()
	return c.handle
}

type fakePromotionClient struct {
	client.Client
	deployment *fakeDeploymentClient
}

func (c *fakePromotionClient) WorkerDeploymentClient() client.WorkerDeploymentClient {
	return c.deployment
}

func (c *fakePromotionClient) Close() {}

func newPromotionBackend(handle *fakeDeploymentHandle, setCurrentOnStart bool) *temporalBackend {
	cfg := baseTemporalConfig()
	cfg.Versioning.SetCurrentOnStart = setCurrentOnStart
	tc := &fakePromotionClient{deployment: &fakeDeploymentClient{handle: handle}}
	return newTemporalBackend("temporal", cfg, tc, nil, nil)
}

func TestPromoteCurrentVersionSkippedWhenDisabled(t *testing.T) {
	handle := &fakeDeploymentHandle{}
	b := newPromotionBackend(handle, false)
	if err := b.PromoteCurrentVersion(context.Background()); err != nil {
		t.Fatalf("PromoteCurrentVersion: %v", err)
	}
	if got := handle.describeCallCount(); got != 0 {
		t.Fatalf("expected no Describe calls when disabled, got %d", got)
	}
	if got := handle.setCallCount(); got != 0 {
		t.Fatalf("expected no SetCurrentVersion calls when disabled, got %d", got)
	}
}

func TestPromoteCurrentVersionSetsBuildID(t *testing.T) {
	handle := &fakeDeploymentHandle{
		conflictToken:  []byte("token-1"),
		currentBuildID: func(int) string { return "old-revision" },
	}
	b := newPromotionBackend(handle, true)
	if err := b.PromoteCurrentVersion(context.Background()); err != nil {
		t.Fatalf("PromoteCurrentVersion: %v", err)
	}
	if got := handle.setCallCount(); got != 1 {
		t.Fatalf("expected 1 SetCurrentVersion call, got %d", got)
	}
	opts := handle.setCalls[0]
	if opts.BuildID != "revision-1" {
		t.Fatalf("BuildID = %q, want revision-1", opts.BuildID)
	}
	if opts.IgnoreMissingTaskQueues {
		t.Fatalf("IgnoreMissingTaskQueues should be false")
	}
	if string(opts.ConflictToken) != "token-1" {
		t.Fatalf("ConflictToken = %q, want token-1", string(opts.ConflictToken))
	}
}

func TestPromoteCurrentVersionSkipsWhenAlreadyCurrent(t *testing.T) {
	handle := &fakeDeploymentHandle{
		currentBuildID: func(int) string { return "revision-1" },
	}
	b := newPromotionBackend(handle, true)
	if err := b.PromoteCurrentVersion(context.Background()); err != nil {
		t.Fatalf("PromoteCurrentVersion: %v", err)
	}
	if got := handle.setCallCount(); got != 0 {
		t.Fatalf("expected no SetCurrentVersion call when already current, got %d", got)
	}
	if got := handle.describeCallCount(); got != 1 {
		t.Fatalf("expected 1 Describe call, got %d", got)
	}
}

func TestPromoteCurrentVersionConflictThenPoll(t *testing.T) {
	handle := &fakeDeploymentHandle{
		conflictToken: []byte("token-1"),
		setErr:        serviceerror.NewFailedPrecondition("another instance already promoted"),
		currentBuildID: func(call int) string {
			// call 0 is the initial pre-promotion Describe; the first poll
			// (call 1) already reflects the winning instance's build.
			if call == 0 {
				return "old-revision"
			}
			return "revision-1"
		},
	}
	b := newPromotionBackend(handle, true)
	if err := b.PromoteCurrentVersion(context.Background()); err != nil {
		t.Fatalf("PromoteCurrentVersion: %v", err)
	}
	if got := handle.setCallCount(); got != 1 {
		t.Fatalf("expected 1 SetCurrentVersion attempt, got %d", got)
	}
	if got := handle.describeCallCount(); got < 2 {
		t.Fatalf("expected the conflict poll to Describe at least twice, got %d", got)
	}
}

func TestPromoteCurrentVersionRunsOnce(t *testing.T) {
	handle := &fakeDeploymentHandle{
		conflictToken:  []byte("token-1"),
		currentBuildID: func(int) string { return "old-revision" },
	}
	b := newPromotionBackend(handle, true)
	for i := 0; i < 3; i++ {
		if err := b.PromoteCurrentVersion(context.Background()); err != nil {
			t.Fatalf("PromoteCurrentVersion #%d: %v", i, err)
		}
	}
	if got := handle.setCallCount(); got != 1 {
		t.Fatalf("expected promotion to run once, got %d SetCurrentVersion calls", got)
	}
}

func TestPromoteCurrentVersionCanceledByClose(t *testing.T) {
	polling := make(chan struct{}, 16)
	handle := &fakeDeploymentHandle{
		conflictToken: []byte("token-1"),
		setErr:        serviceerror.NewFailedPrecondition("another instance already promoted"),
		// Never matches, so the poll loop runs until the context is canceled.
		currentBuildID: func(int) string { return "old-revision" },
	}
	handle.onDescribe = func() {
		select {
		case polling <- struct{}{}:
		default:
		}
	}
	b := newPromotionBackend(handle, true)

	done := make(chan error, 1)
	go func() { done <- b.PromoteCurrentVersion(context.Background()) }()

	// Wait for the initial Describe and at least one poll Describe so we know
	// the conflict-poll loop is running before we cancel it via Close.
	<-polling
	<-polling

	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled from canceled promotion, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("promotion did not unwind after Close canceled it")
	}
}
