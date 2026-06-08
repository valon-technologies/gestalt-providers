package helloworld

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	proto "github.com/valon-technologies/gestalt/server/rpc/protov1/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	providerVersion     = "0.0.1-alpha.1"
	defaultProviderName = "hello-world"
	providerDisplayName = "Test Provider"
	providerDescription = "Minimal kind=test provider used to validate provider registration and invocation."
	helloWorldMessage   = "HelloWorld"
)

type Provider struct {
	mu   sync.RWMutex
	name string
}

func New() *Provider {
	return &Provider{name: defaultProviderName}
}

func (p *Provider) Configure(_ context.Context, name string, _ map[string]any) error {
	name = strings.TrimSpace(name)
	if name == "" {
		name = defaultProviderName
	}
	p.mu.Lock()
	p.name = name
	p.mu.Unlock()
	return nil
}

func (p *Provider) Name() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.name == "" {
		return defaultProviderName
	}
	return p.name
}

func (p *Provider) HelloWorld(context.Context) (string, error) {
	return helloWorldMessage, nil
}

func Serve(ctx context.Context, provider *Provider) error {
	if provider == nil {
		provider = New()
	}
	return serveProvider(ctx, provider)
}

func __gestalt_serve_test(name string) error {
	provider := New()
	if err := provider.Configure(context.Background(), name, nil); err != nil {
		return err
	}
	return Serve(context.Background(), provider)
}

type lifecycleServer struct {
	proto.UnimplementedProviderLifecycleServer
	provider *Provider
}

func (s lifecycleServer) GetProviderIdentity(context.Context, *emptypb.Empty) (*proto.ProviderIdentity, error) {
	return &proto.ProviderIdentity{
		Kind:               proto.ProviderKind_PROVIDER_KIND_TEST,
		Name:               s.provider.Name(),
		DisplayName:        providerDisplayName,
		Description:        providerDescription,
		Version:            providerVersion,
		MinProtocolVersion: proto.CurrentProtocolVersion,
		MaxProtocolVersion: proto.CurrentProtocolVersion,
	}, nil
}

func (s lifecycleServer) ConfigureProvider(ctx context.Context, req *proto.ConfigureProviderRequest) (*proto.ConfigureProviderResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetProtocolVersion() != proto.CurrentProtocolVersion {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"host requested protocol version %d, provider requires %d",
			req.GetProtocolVersion(),
			proto.CurrentProtocolVersion,
		)
	}
	config := map[string]any{}
	if req.GetConfig() != nil {
		config = req.GetConfig().AsMap()
	}
	if err := s.provider.Configure(ctx, req.GetName(), config); err != nil {
		return nil, status.Errorf(codes.Unknown, "configure provider: %v", err)
	}
	return &proto.ConfigureProviderResponse{ProtocolVersion: proto.CurrentProtocolVersion}, nil
}

func (s lifecycleServer) HealthCheck(context.Context, *emptypb.Empty) (*proto.HealthCheckResponse, error) {
	return &proto.HealthCheckResponse{Ready: true}, nil
}

func (s lifecycleServer) StartProvider(context.Context, *emptypb.Empty) (*proto.StartRuntimeProviderResponse, error) {
	return &proto.StartRuntimeProviderResponse{ProtocolVersion: proto.CurrentProtocolVersion}, nil
}

type testServer struct {
	proto.UnimplementedTestProviderServer
	provider *Provider
}

func (s testServer) HelloWorld(ctx context.Context, _ *proto.HelloWorldRequest) (*proto.HelloWorldResponse, error) {
	message, err := s.provider.HelloWorld(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unknown, "hello world: %v", err)
	}
	return &proto.HelloWorldResponse{Message: message}, nil
}

func serveProvider(ctx context.Context, provider *Provider) error {
	socket := os.Getenv(proto.EnvProviderSocket)
	if socket == "" {
		return fmt.Errorf("%s is required", proto.EnvProviderSocket)
	}
	if err := os.Remove(socket); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove stale socket %q: %w", socket, err)
	}
	if dir := filepath.Dir(socket); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create socket directory %q: %w", dir, err)
		}
	}
	lis, err := net.Listen("unix", socket)
	if err != nil {
		return fmt.Errorf("listen on provider socket %q: %w", socket, err)
	}
	defer func() {
		_ = lis.Close()
		_ = os.Remove(socket)
	}()

	srv := grpc.NewServer()
	proto.RegisterProviderLifecycleServer(srv, lifecycleServer{provider: provider})
	proto.RegisterTestProviderServer(srv, testServer{provider: provider})

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		<-ctx.Done()
		srv.GracefulStop()
	}()
	if parentPID := providerParentPID(); parentPID > 0 {
		go watchProviderParent(parentPID, srv)
	}

	err = srv.Serve(lis)
	if ctx.Err() != nil {
		<-stopped
		return nil
	}
	return err
}

func providerParentPID() int {
	raw := os.Getenv(proto.EnvProviderParentPID)
	if raw == "" {
		return 0
	}
	pid, err := strconv.Atoi(raw)
	if err != nil || pid <= 0 {
		return 0
	}
	return pid
}

func watchProviderParent(parentPID int, srv *grpc.Server) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		if os.Getppid() == parentPID {
			continue
		}
		srv.GracefulStop()
		return
	}
}
