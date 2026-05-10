package temporal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Provider struct {
	gestalt.UnimplementedWorkflowProvider

	mu      sync.RWMutex
	name    string
	backend *temporalBackend
}

func New() *Provider {
	return &Provider{}
}

func (p *Provider) Configure(ctx context.Context, name string, raw map[string]any) error {
	if err := p.Close(); err != nil {
		return err
	}
	cfg, err := decodeConfig(raw)
	if err != nil {
		return fmt.Errorf("temporal workflow: %w", err)
	}
	state, err := openWorkflowStateStore(ctx, cfg.ScopeID)
	if err != nil {
		return fmt.Errorf("temporal workflow: open state store: %w", err)
	}
	host, err := gestalt.WorkflowHost()
	if err != nil {
		_ = state.Close()
		return fmt.Errorf("temporal workflow: connect workflow host: %w", err)
	}
	tc, err := client.Dial(client.Options{
		HostPort:    cfg.HostPort,
		Namespace:   cfg.Namespace,
		Credentials: client.NewAPIKeyStaticCredentials(cfg.APIKey),
	})
	if err != nil {
		_ = host.Close()
		_ = state.Close()
		return fmt.Errorf("temporal workflow: connect temporal: %w", err)
	}
	providerName := strings.TrimSpace(name)
	backend := newTemporalBackend(providerName, cfg, tc, &sdkWorkflowHost{client: host}, state)
	p.mu.Lock()
	p.name = providerName
	p.backend = backend
	p.mu.Unlock()
	return nil
}

type sdkWorkflowHost struct {
	client *gestalt.WorkflowHostClient
}

func (h *sdkWorkflowHost) InvokeOperation(ctx context.Context, input gestalt.InvokeWorkflowOperationInput) (*gestalt.InvokeWorkflowOperationResponse, error) {
	return h.client.InvokeOperation(ctx, input)
}

func (h *sdkWorkflowHost) Close() error {
	if h == nil || h.client == nil {
		return nil
	}
	return h.client.Close()
}

var _ workflowHost = (*sdkWorkflowHost)(nil)

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	p.mu.RLock()
	name := strings.TrimSpace(p.name)
	p.mu.RUnlock()
	if name == "" {
		name = "temporal"
	}
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindWorkflow,
		Name:        name,
		DisplayName: "Temporal Workflow",
		Description: "Workflow provider backed by Temporal Cloud.",
		Version:     providerVersion,
	}
}

func (p *Provider) Start(ctx context.Context) error {
	backend, err := p.requireBackend()
	if err != nil {
		return err
	}
	return backend.Start(ctx)
}

func (p *Provider) HealthCheck(ctx context.Context) error {
	backend, err := p.requireBackend()
	if err != nil {
		return err
	}
	return backend.HealthCheck(ctx)
}

func (p *Provider) Close() error {
	p.mu.Lock()
	backend := p.backend
	p.backend = nil
	p.mu.Unlock()
	if backend == nil {
		return nil
	}
	return backend.Close()
}

func (p *Provider) StartRun(ctx context.Context, req *gestalt.StartWorkflowProviderRunRequest) (*gestalt.BoundWorkflowRunInput, error) {
	backend, err := p.requireStartedBackend(ctx)
	if err != nil {
		return nil, err
	}
	return backend.StartRun(ctx, req)
}

func (p *Provider) GetRun(ctx context.Context, req *gestalt.GetWorkflowProviderRunRequest) (*gestalt.BoundWorkflowRunInput, error) {
	backend, err := p.requireBackend()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.GetRun(ctx, req)
}

func (p *Provider) ListRuns(ctx context.Context, req *gestalt.ListWorkflowProviderRunsRequest) (*gestalt.ListWorkflowProviderRunsResponse, error) {
	backend, err := p.requireBackend()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.ListRuns(ctx, req)
}

func (p *Provider) CancelRun(ctx context.Context, req *gestalt.CancelWorkflowProviderRunRequest) (*gestalt.BoundWorkflowRunInput, error) {
	backend, err := p.requireStartedBackend(ctx)
	if err != nil {
		return nil, err
	}
	return backend.CancelRun(ctx, req)
}

func (p *Provider) SignalRun(ctx context.Context, req *gestalt.SignalWorkflowProviderRunRequest) (*gestalt.SignalWorkflowRunResponse, error) {
	backend, err := p.requireStartedBackend(ctx)
	if err != nil {
		return nil, err
	}
	return backend.SignalRun(ctx, req)
}

func (p *Provider) SignalOrStartRun(ctx context.Context, req *gestalt.SignalOrStartWorkflowProviderRunRequest) (*gestalt.SignalWorkflowRunResponse, error) {
	backend, err := p.requireStartedBackend(ctx)
	if err != nil {
		return nil, err
	}
	return backend.SignalOrStartRun(ctx, req)
}

func (p *Provider) UpsertSchedule(ctx context.Context, req *gestalt.UpsertWorkflowProviderScheduleRequest) (*gestalt.BoundWorkflowScheduleInput, error) {
	backend, err := p.requireBackend()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.UpsertSchedule(ctx, req)
}

func (p *Provider) GetSchedule(ctx context.Context, req *gestalt.GetWorkflowProviderScheduleRequest) (*gestalt.BoundWorkflowScheduleInput, error) {
	backend, err := p.requireBackend()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.GetSchedule(ctx, req)
}

func (p *Provider) ListSchedules(ctx context.Context, req *gestalt.ListWorkflowProviderSchedulesRequest) (*gestalt.ListWorkflowProviderSchedulesResponse, error) {
	backend, err := p.requireBackend()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.ListSchedules(ctx, req)
}

func (p *Provider) DeleteSchedule(ctx context.Context, req *gestalt.DeleteWorkflowProviderScheduleRequest) error {
	backend, err := p.requireBackend()
	if err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.DeleteSchedule(ctx, req)
}

func (p *Provider) PauseSchedule(ctx context.Context, req *gestalt.PauseWorkflowProviderScheduleRequest) (*gestalt.BoundWorkflowScheduleInput, error) {
	backend, err := p.requireBackend()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.PauseSchedule(ctx, req)
}

func (p *Provider) ResumeSchedule(ctx context.Context, req *gestalt.ResumeWorkflowProviderScheduleRequest) (*gestalt.BoundWorkflowScheduleInput, error) {
	backend, err := p.requireBackend()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.ResumeSchedule(ctx, req)
}

func (p *Provider) UpsertEventTrigger(ctx context.Context, req *gestalt.UpsertWorkflowProviderEventTriggerRequest) (*gestalt.BoundWorkflowEventTriggerInput, error) {
	backend, err := p.requireBackend()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.UpsertEventTrigger(ctx, req)
}

func (p *Provider) GetEventTrigger(ctx context.Context, req *gestalt.GetWorkflowProviderEventTriggerRequest) (*gestalt.BoundWorkflowEventTriggerInput, error) {
	backend, err := p.requireBackend()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.GetEventTrigger(ctx, req)
}

func (p *Provider) ListEventTriggers(ctx context.Context, req *gestalt.ListWorkflowProviderEventTriggersRequest) (*gestalt.ListWorkflowProviderEventTriggersResponse, error) {
	backend, err := p.requireBackend()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.ListEventTriggers(ctx, req)
}

func (p *Provider) DeleteEventTrigger(ctx context.Context, req *gestalt.DeleteWorkflowProviderEventTriggerRequest) error {
	backend, err := p.requireBackend()
	if err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.DeleteEventTrigger(ctx, req)
}

func (p *Provider) PauseEventTrigger(ctx context.Context, req *gestalt.PauseWorkflowProviderEventTriggerRequest) (*gestalt.BoundWorkflowEventTriggerInput, error) {
	backend, err := p.requireBackend()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.PauseEventTrigger(ctx, req)
}

func (p *Provider) ResumeEventTrigger(ctx context.Context, req *gestalt.ResumeWorkflowProviderEventTriggerRequest) (*gestalt.BoundWorkflowEventTriggerInput, error) {
	backend, err := p.requireBackend()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.ResumeEventTrigger(ctx, req)
}

func (p *Provider) PutExecutionReference(ctx context.Context, req *gestalt.PutWorkflowExecutionReferenceRequest) (*gestalt.WorkflowExecutionReferenceInput, error) {
	backend, err := p.requireBackend()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.PutExecutionReference(ctx, req)
}

func (p *Provider) GetExecutionReference(ctx context.Context, req *gestalt.GetWorkflowExecutionReferenceRequest) (*gestalt.WorkflowExecutionReferenceInput, error) {
	backend, err := p.requireBackend()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.GetExecutionReference(ctx, req)
}

func (p *Provider) ListExecutionReferences(ctx context.Context, req *gestalt.ListWorkflowExecutionReferencesRequest) (*gestalt.ListWorkflowExecutionReferencesResponse, error) {
	backend, err := p.requireBackend()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.ListExecutionReferences(ctx, req)
}

func (p *Provider) PublishEvent(ctx context.Context, req *gestalt.PublishWorkflowProviderEventRequest) error {
	backend, err := p.requireStartedBackend(ctx)
	if err != nil {
		return err
	}
	return backend.PublishEvent(ctx, req)
}

func (p *Provider) requireBackend() (*temporalBackend, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.backend == nil {
		return nil, errors.New("temporal workflow: provider is not configured")
	}
	return p.backend, nil
}

func (p *Provider) requireStartedBackend(ctx context.Context) (*temporalBackend, error) {
	backend, err := p.requireBackend()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if err := backend.Start(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "start temporal workflow provider: %v", err)
	}
	return backend, nil
}

var _ gestalt.WorkflowProvider = (*Provider)(nil)
var _ gestalt.MetadataProvider = (*Provider)(nil)
var _ gestalt.HealthChecker = (*Provider)(nil)
var _ gestalt.Starter = (*Provider)(nil)
var _ gestalt.Closer = (*Provider)(nil)
