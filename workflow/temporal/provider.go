package temporal

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type workflowBackend interface {
	Start(context.Context) error
	Close() error
	HealthCheck(context.Context) error

	StartRun(context.Context, *proto.StartWorkflowProviderRunRequest) (*proto.BoundWorkflowRun, error)
	GetRun(context.Context, *proto.GetWorkflowProviderRunRequest) (*proto.BoundWorkflowRun, error)
	ListRuns(context.Context, *proto.ListWorkflowProviderRunsRequest) (*proto.ListWorkflowProviderRunsResponse, error)
	CancelRun(context.Context, *proto.CancelWorkflowProviderRunRequest) (*proto.BoundWorkflowRun, error)
	SignalRun(context.Context, *proto.SignalWorkflowProviderRunRequest) (*proto.SignalWorkflowRunResponse, error)
	SignalOrStartRun(context.Context, *proto.SignalOrStartWorkflowProviderRunRequest) (*proto.SignalWorkflowRunResponse, error)

	UpsertSchedule(context.Context, *proto.UpsertWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error)
	GetSchedule(context.Context, *proto.GetWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error)
	ListSchedules(context.Context, *proto.ListWorkflowProviderSchedulesRequest) (*proto.ListWorkflowProviderSchedulesResponse, error)
	DeleteSchedule(context.Context, *proto.DeleteWorkflowProviderScheduleRequest) (*emptypb.Empty, error)
	PauseSchedule(context.Context, *proto.PauseWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error)
	ResumeSchedule(context.Context, *proto.ResumeWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error)

	UpsertEventTrigger(context.Context, *proto.UpsertWorkflowProviderEventTriggerRequest) (*proto.BoundWorkflowEventTrigger, error)
	GetEventTrigger(context.Context, *proto.GetWorkflowProviderEventTriggerRequest) (*proto.BoundWorkflowEventTrigger, error)
	ListEventTriggers(context.Context, *proto.ListWorkflowProviderEventTriggersRequest) (*proto.ListWorkflowProviderEventTriggersResponse, error)
	DeleteEventTrigger(context.Context, *proto.DeleteWorkflowProviderEventTriggerRequest) (*emptypb.Empty, error)
	PauseEventTrigger(context.Context, *proto.PauseWorkflowProviderEventTriggerRequest) (*proto.BoundWorkflowEventTrigger, error)
	ResumeEventTrigger(context.Context, *proto.ResumeWorkflowProviderEventTriggerRequest) (*proto.BoundWorkflowEventTrigger, error)

	PutExecutionReference(context.Context, *proto.PutWorkflowExecutionReferenceRequest) (*proto.WorkflowExecutionReference, error)
	GetExecutionReference(context.Context, *proto.GetWorkflowExecutionReferenceRequest) (*proto.WorkflowExecutionReference, error)
	ListExecutionReferences(context.Context, *proto.ListWorkflowExecutionReferencesRequest) (*proto.ListWorkflowExecutionReferencesResponse, error)

	PublishEvent(context.Context, *proto.PublishWorkflowProviderEventRequest) (*emptypb.Empty, error)
}

type Provider struct {
	proto.UnimplementedWorkflowProviderServer

	mu      sync.RWMutex
	name    string
	cfg     config
	backend workflowBackend
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
	host, err := gestalt.WorkflowHost()
	if err != nil {
		return fmt.Errorf("temporal workflow: connect workflow host: %w", err)
	}
	tc, err := client.Dial(client.Options{
		HostPort:    cfg.HostPort,
		Namespace:   cfg.Namespace,
		Credentials: client.NewAPIKeyStaticCredentials(cfg.APIKey),
		Identity:    cfg.Identity,
	})
	if err != nil {
		_ = host.Close()
		return fmt.Errorf("temporal workflow: connect temporal: %w", err)
	}
	backend := newTemporalBackend(strings.TrimSpace(name), cfg, tc, host)
	p.mu.Lock()
	p.name = strings.TrimSpace(name)
	p.cfg = cfg
	p.backend = backend
	p.mu.Unlock()
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	name := p.providerName()
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

func (p *Provider) StartRun(ctx context.Context, req *proto.StartWorkflowProviderRunRequest) (*proto.BoundWorkflowRun, error) {
	backend, err := p.requireBackendStatus(ctx)
	if err != nil {
		return nil, err
	}
	return backend.StartRun(ctx, req)
}

func (p *Provider) GetRun(ctx context.Context, req *proto.GetWorkflowProviderRunRequest) (*proto.BoundWorkflowRun, error) {
	backend, err := p.requireBackendStatus(ctx)
	if err != nil {
		return nil, err
	}
	return backend.GetRun(ctx, req)
}

func (p *Provider) ListRuns(ctx context.Context, req *proto.ListWorkflowProviderRunsRequest) (*proto.ListWorkflowProviderRunsResponse, error) {
	backend, err := p.requireBackendStatus(ctx)
	if err != nil {
		return nil, err
	}
	return backend.ListRuns(ctx, req)
}

func (p *Provider) CancelRun(ctx context.Context, req *proto.CancelWorkflowProviderRunRequest) (*proto.BoundWorkflowRun, error) {
	backend, err := p.requireBackendStatus(ctx)
	if err != nil {
		return nil, err
	}
	return backend.CancelRun(ctx, req)
}

func (p *Provider) SignalRun(ctx context.Context, req *proto.SignalWorkflowProviderRunRequest) (*proto.SignalWorkflowRunResponse, error) {
	backend, err := p.requireBackendStatus(ctx)
	if err != nil {
		return nil, err
	}
	return backend.SignalRun(ctx, req)
}

func (p *Provider) SignalOrStartRun(ctx context.Context, req *proto.SignalOrStartWorkflowProviderRunRequest) (*proto.SignalWorkflowRunResponse, error) {
	backend, err := p.requireBackendStatus(ctx)
	if err != nil {
		return nil, err
	}
	return backend.SignalOrStartRun(ctx, req)
}

func (p *Provider) UpsertSchedule(ctx context.Context, req *proto.UpsertWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error) {
	backend, err := p.requireBackendStatus(ctx)
	if err != nil {
		return nil, err
	}
	return backend.UpsertSchedule(ctx, req)
}

func (p *Provider) GetSchedule(ctx context.Context, req *proto.GetWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error) {
	backend, err := p.requireBackendStatus(ctx)
	if err != nil {
		return nil, err
	}
	return backend.GetSchedule(ctx, req)
}

func (p *Provider) ListSchedules(ctx context.Context, req *proto.ListWorkflowProviderSchedulesRequest) (*proto.ListWorkflowProviderSchedulesResponse, error) {
	backend, err := p.requireBackendStatus(ctx)
	if err != nil {
		return nil, err
	}
	return backend.ListSchedules(ctx, req)
}

func (p *Provider) DeleteSchedule(ctx context.Context, req *proto.DeleteWorkflowProviderScheduleRequest) (*emptypb.Empty, error) {
	backend, err := p.requireBackendStatus(ctx)
	if err != nil {
		return nil, err
	}
	return backend.DeleteSchedule(ctx, req)
}

func (p *Provider) PauseSchedule(ctx context.Context, req *proto.PauseWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error) {
	backend, err := p.requireBackendStatus(ctx)
	if err != nil {
		return nil, err
	}
	return backend.PauseSchedule(ctx, req)
}

func (p *Provider) ResumeSchedule(ctx context.Context, req *proto.ResumeWorkflowProviderScheduleRequest) (*proto.BoundWorkflowSchedule, error) {
	backend, err := p.requireBackendStatus(ctx)
	if err != nil {
		return nil, err
	}
	return backend.ResumeSchedule(ctx, req)
}

func (p *Provider) UpsertEventTrigger(ctx context.Context, req *proto.UpsertWorkflowProviderEventTriggerRequest) (*proto.BoundWorkflowEventTrigger, error) {
	backend, err := p.requireBackendStatus(ctx)
	if err != nil {
		return nil, err
	}
	return backend.UpsertEventTrigger(ctx, req)
}

func (p *Provider) GetEventTrigger(ctx context.Context, req *proto.GetWorkflowProviderEventTriggerRequest) (*proto.BoundWorkflowEventTrigger, error) {
	backend, err := p.requireBackendStatus(ctx)
	if err != nil {
		return nil, err
	}
	return backend.GetEventTrigger(ctx, req)
}

func (p *Provider) ListEventTriggers(ctx context.Context, req *proto.ListWorkflowProviderEventTriggersRequest) (*proto.ListWorkflowProviderEventTriggersResponse, error) {
	backend, err := p.requireBackendStatus(ctx)
	if err != nil {
		return nil, err
	}
	return backend.ListEventTriggers(ctx, req)
}

func (p *Provider) DeleteEventTrigger(ctx context.Context, req *proto.DeleteWorkflowProviderEventTriggerRequest) (*emptypb.Empty, error) {
	backend, err := p.requireBackendStatus(ctx)
	if err != nil {
		return nil, err
	}
	return backend.DeleteEventTrigger(ctx, req)
}

func (p *Provider) PauseEventTrigger(ctx context.Context, req *proto.PauseWorkflowProviderEventTriggerRequest) (*proto.BoundWorkflowEventTrigger, error) {
	backend, err := p.requireBackendStatus(ctx)
	if err != nil {
		return nil, err
	}
	return backend.PauseEventTrigger(ctx, req)
}

func (p *Provider) ResumeEventTrigger(ctx context.Context, req *proto.ResumeWorkflowProviderEventTriggerRequest) (*proto.BoundWorkflowEventTrigger, error) {
	backend, err := p.requireBackendStatus(ctx)
	if err != nil {
		return nil, err
	}
	return backend.ResumeEventTrigger(ctx, req)
}

func (p *Provider) PutExecutionReference(ctx context.Context, req *proto.PutWorkflowExecutionReferenceRequest) (*proto.WorkflowExecutionReference, error) {
	backend, err := p.requireBackendStatus(ctx)
	if err != nil {
		return nil, err
	}
	return backend.PutExecutionReference(ctx, req)
}

func (p *Provider) GetExecutionReference(ctx context.Context, req *proto.GetWorkflowExecutionReferenceRequest) (*proto.WorkflowExecutionReference, error) {
	backend, err := p.requireBackendStatus(ctx)
	if err != nil {
		return nil, err
	}
	return backend.GetExecutionReference(ctx, req)
}

func (p *Provider) ListExecutionReferences(ctx context.Context, req *proto.ListWorkflowExecutionReferencesRequest) (*proto.ListWorkflowExecutionReferencesResponse, error) {
	backend, err := p.requireBackendStatus(ctx)
	if err != nil {
		return nil, err
	}
	return backend.ListExecutionReferences(ctx, req)
}

func (p *Provider) PublishEvent(ctx context.Context, req *proto.PublishWorkflowProviderEventRequest) (*emptypb.Empty, error) {
	backend, err := p.requireBackendStatus(ctx)
	if err != nil {
		return nil, err
	}
	return backend.PublishEvent(ctx, req)
}

func (p *Provider) providerName() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return strings.TrimSpace(p.name)
}

func (p *Provider) requireBackend() (workflowBackend, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.backend == nil {
		return nil, errors.New("temporal workflow: provider is not configured")
	}
	return p.backend, nil
}

func (p *Provider) requireBackendStatus(ctx context.Context) (workflowBackend, error) {
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
