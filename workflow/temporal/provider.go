package temporal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	gestaltworkflow "github.com/valon-technologies/gestalt/sdk/go/workflow"
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
	db, err := gestalt.IndexedDB(ctx)
	if err != nil {
		return fmt.Errorf("temporal workflow: connect indexeddb: %w", err)
	}
	state, err := openWorkflowStateStore(ctx, cfg.ScopeID, db)
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("temporal workflow: open state store: %w", err)
	}
	tc, err := client.Dial(client.Options{
		HostPort:    cfg.HostPort,
		Namespace:   cfg.Namespace,
		Credentials: client.NewAPIKeyStaticCredentials(cfg.APIKey),
	})
	if err != nil {
		_ = state.Close()
		return fmt.Errorf("temporal workflow: connect temporal: %w", err)
	}
	providerName := strings.TrimSpace(name)
	backend := newTemporalBackend(providerName, cfg, tc, gestaltworkflow.New(gestaltworkflow.Config{}), state)
	p.mu.Lock()
	p.name = providerName
	p.backend = backend
	p.mu.Unlock()
	return nil
}

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

func (p *Provider) ApplyDefinition(ctx context.Context, req *gestalt.ApplyWorkflowProviderDefinitionRequest) (*gestalt.WorkflowDefinition, error) {
	backend, err := p.requireBackend()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.ApplyDefinition(ctx, req)
}

func (p *Provider) GetDefinition(ctx context.Context, req *gestalt.GetWorkflowProviderDefinitionRequest) (*gestalt.WorkflowDefinition, error) {
	backend, err := p.requireBackend()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.GetDefinition(ctx, req)
}

func (p *Provider) ListDefinitions(ctx context.Context, req *gestalt.ListWorkflowProviderDefinitionsRequest) (*gestalt.ListWorkflowProviderDefinitionsResponse, error) {
	backend, err := p.requireBackend()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.ListDefinitions(ctx, req)
}

func (p *Provider) SetDefinitionPaused(ctx context.Context, req *gestalt.SetWorkflowProviderDefinitionPausedRequest) (*gestalt.WorkflowDefinition, error) {
	backend, err := p.requireBackend()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.SetDefinitionPaused(ctx, req)
}

func (p *Provider) SetActivationPaused(ctx context.Context, req *gestalt.SetWorkflowProviderActivationPausedRequest) (*gestalt.WorkflowDefinition, error) {
	backend, err := p.requireBackend()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.SetActivationPaused(ctx, req)
}

func (p *Provider) DeleteDefinition(ctx context.Context, req *gestalt.DeleteWorkflowProviderDefinitionRequest) error {
	backend, err := p.requireBackend()
	if err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.DeleteDefinition(ctx, req)
}

func (p *Provider) StartRun(ctx context.Context, req *gestalt.StartWorkflowProviderRunRequest) (*gestalt.WorkflowRun, error) {
	backend, err := p.requireStartedBackend(ctx)
	if err != nil {
		return nil, err
	}
	return backend.StartRun(ctx, req)
}

func (p *Provider) GetRun(ctx context.Context, req *gestalt.GetWorkflowProviderRunRequest) (*gestalt.WorkflowRun, error) {
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

func (p *Provider) GetRunEvents(ctx context.Context, req *gestalt.GetWorkflowProviderRunEventsRequest) (*gestalt.GetWorkflowProviderRunEventsResponse, error) {
	backend, err := p.requireBackend()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.GetRunEvents(ctx, req)
}

func (p *Provider) GetRunOutput(ctx context.Context, req *gestalt.GetWorkflowProviderRunOutputRequest) (*gestalt.GetWorkflowProviderRunOutputResponse, error) {
	backend, err := p.requireBackend()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return backend.GetRunOutput(ctx, req)
}

func (p *Provider) CancelRun(ctx context.Context, req *gestalt.CancelWorkflowProviderRunRequest) (*gestalt.WorkflowRun, error) {
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

func (p *Provider) DeliverEvent(ctx context.Context, req *gestalt.DeliverWorkflowProviderEventRequest) (*gestalt.WorkflowEvent, error) {
	backend, err := p.requireStartedBackend(ctx)
	if err != nil {
		return nil, err
	}
	return backend.DeliverEvent(ctx, req)
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
