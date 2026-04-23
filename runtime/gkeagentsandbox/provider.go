package gkeagentsandbox

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	providerVersion      = "0.0.1-alpha.1"
	sessionStateReady    = "ready"
	sessionStateStarting = "starting"
	sessionStateRunning  = "running"
	sessionStateStopped  = "stopped"
	sessionStateFailed   = "failed"

	remoteBundleRoot = gestalt.HostedPluginBundleRoot
)

type Provider struct {
	proto.UnimplementedPluginRuntimeProviderServer

	name    string
	cfg     Config
	runtime sandboxRuntime

	nextID uint64

	mu       sync.Mutex
	sessions map[string]*session
	closed   bool
}

type session struct {
	id             string
	state          string
	metadata       map[string]string
	handle         sandboxHandle
	bindings       map[string]hostServiceBinding
	plugin         *plugin
	pluginStarting bool
	pluginTunnel   tunnel
}

type hostServiceBinding struct {
	id         string
	envVar     string
	dialTarget string
}

type plugin struct {
	id   string
	name string
}

func New() *Provider {
	return &Provider{
		sessions: make(map[string]*session),
	}
}

func (p *Provider) Configure(ctx context.Context, name string, raw map[string]any) error {
	cfg, err := decodeConfig(raw)
	if err != nil {
		return err
	}
	runtime, err := newKubernetesSandboxRuntime(cfg)
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		_ = runtime.Close()
		return fmt.Errorf("gke agent sandbox runtime: provider is closed")
	}
	oldRuntime := p.runtime
	p.name = strings.TrimSpace(name)
	p.cfg = cfg
	p.runtime = runtime
	if p.sessions == nil {
		p.sessions = make(map[string]*session)
	}
	if oldRuntime != nil {
		_ = oldRuntime.Close()
	}

	_ = ctx
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	name := p.name
	if strings.TrimSpace(name) == "" {
		name = "gkeAgentSandbox"
	}
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindRuntime,
		Name:        name,
		DisplayName: "GKE Agent Sandbox Runtime",
		Description: "Hosted executable-plugin runtime backed by GKE Agent Sandbox.",
		Version:     providerVersion,
	}
}

func (p *Provider) HealthCheck(ctx context.Context) error {
	runtime, _, err := p.configured()
	if err != nil {
		return err
	}
	return runtime.HealthCheck(ctx)
}

func (p *Provider) GetSupport(context.Context, *emptypb.Empty) (*proto.PluginRuntimeSupport, error) {
	return &proto.PluginRuntimeSupport{
		CanHostPlugins:    true,
		HostServiceAccess: proto.PluginRuntimeHostServiceAccess_PLUGIN_RUNTIME_HOST_SERVICE_ACCESS_NONE,
		EgressMode:        proto.PluginRuntimeEgressMode_PLUGIN_RUNTIME_EGRESS_MODE_NONE,
		LaunchMode:        proto.PluginRuntimeLaunchMode_PLUGIN_RUNTIME_LAUNCH_MODE_BUNDLE,
		ExecutionTarget: &proto.PluginRuntimeExecutionTarget{
			Goos:   "linux",
			Goarch: "amd64",
		},
	}, nil
}

func (p *Provider) StartSession(ctx context.Context, req *proto.StartPluginRuntimeSessionRequest) (*proto.PluginRuntimeSession, error) {
	runtime, cfg, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	template := strings.TrimSpace(req.GetTemplate())
	if template == "" {
		template = cfg.Template
	}
	image := strings.TrimSpace(req.GetImage())
	if template == "" && image == "" {
		return nil, status.Errorf(codes.InvalidArgument, "plugins.%s.runtime.image is required when no GKE Agent Sandbox template is configured", req.GetPluginName())
	}

	sessionID := p.newID("session")
	resourceName := sandboxResourceName(req.GetPluginName(), sessionID)
	handle, err := runtime.Start(ctx, startSandboxRequest{
		Name:       resourceName,
		PluginName: req.GetPluginName(),
		Namespace:  cfg.Namespace,
		Template:   template,
		Image:      image,
		Metadata:   cloneStringMap(req.GetMetadata()),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "start gke agent sandbox session: %v", err)
	}

	s := &session{
		id:       sessionID,
		state:    sessionStateReady,
		metadata: cloneStringMap(req.GetMetadata()),
		handle:   handle,
		bindings: make(map[string]hostServiceBinding),
	}
	if s.metadata == nil {
		s.metadata = map[string]string{}
	}
	s.metadata["kubernetes.namespace"] = handle.Namespace
	s.metadata["kubernetes.sandbox"] = handle.SandboxName
	if handle.ClaimName != "" {
		s.metadata["kubernetes.sandboxClaim"] = handle.ClaimName
	}
	if handle.PodName != "" {
		s.metadata["kubernetes.pod"] = handle.PodName
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		_ = runtime.Stop(context.Background(), handle)
		return nil, status.Error(codes.FailedPrecondition, "gke agent sandbox runtime is closed")
	}
	p.sessions[sessionID] = s
	return cloneSession(s), nil
}

func (p *Provider) GetSession(ctx context.Context, req *proto.GetPluginRuntimeSessionRequest) (*proto.PluginRuntimeSession, error) {
	runtime, _, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	p.mu.Lock()
	s, err := p.sessionLocked(req.GetSessionId())
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.NotFound, err.Error())
	}
	handle := s.handle
	current := cloneSession(s)
	p.mu.Unlock()

	refreshed, refreshErr := runtime.Get(ctx, handle)
	if refreshErr != nil {
		p.mu.Lock()
		if s, ok := p.sessions[req.GetSessionId()]; ok && s != nil {
			s.state = sessionStateFailed
			current = cloneSession(s)
		}
		p.mu.Unlock()
		return current, nil
	}
	if current.GetState() == sessionStateRunning {
		if err := runtime.Exec(ctx, refreshed, pluginHealthCommand(), nil); err != nil {
			p.mu.Lock()
			if s, ok := p.sessions[req.GetSessionId()]; ok && s != nil {
				s.state = sessionStateFailed
				current = cloneSession(s)
			}
			p.mu.Unlock()
			return current, nil
		}
	}

	p.mu.Lock()
	if s, ok := p.sessions[req.GetSessionId()]; ok && s != nil {
		s.handle = refreshed
		if s.state != sessionStateRunning && s.state != sessionStateStarting && s.state != sessionStateFailed {
			s.state = sessionStateReady
		}
		current = cloneSession(s)
	}
	p.mu.Unlock()
	return current, nil
}

func (p *Provider) StopSession(ctx context.Context, req *proto.StopPluginRuntimeSessionRequest) (*emptypb.Empty, error) {
	runtime, _, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	var (
		handle sandboxHandle
		tunnel tunnel
	)
	p.mu.Lock()
	s, ok := p.sessions[req.GetSessionId()]
	if ok && s != nil {
		handle = s.handle
		tunnel = s.pluginTunnel
	}
	p.mu.Unlock()

	var errs []error
	if tunnel != nil {
		errs = append(errs, tunnel.Close())
	}
	if handle.Name != "" {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), p.cfg.CleanupTimeout)
		_ = runtime.Exec(cleanupCtx, handle, pluginCleanupCommand(), nil)
		cleanupCancel()
	}
	if handle.Name != "" {
		errs = append(errs, runtime.Stop(ctx, handle))
	}
	if err := errors.Join(errs...); err != nil {
		return nil, status.Errorf(codes.Internal, "stop gke agent sandbox session: %v", err)
	}
	p.mu.Lock()
	if s, ok := p.sessions[req.GetSessionId()]; ok && s != nil {
		delete(p.sessions, req.GetSessionId())
		s.state = sessionStateStopped
	}
	p.mu.Unlock()
	return &emptypb.Empty{}, nil
}

func (p *Provider) BindHostService(_ context.Context, req *proto.BindPluginRuntimeHostServiceRequest) (*proto.PluginRuntimeHostServiceBinding, error) {
	if req.GetRelay() == nil || strings.TrimSpace(req.GetRelay().GetDialTarget()) == "" {
		return nil, status.Error(codes.InvalidArgument, "gke agent sandbox runtime requires relay-backed host service bindings")
	}
	if strings.TrimSpace(req.GetEnvVar()) == "" {
		return nil, status.Error(codes.InvalidArgument, "host service env var is required")
	}
	if !isRelayHostServiceEnv(req.GetEnvVar()) {
		return nil, status.Errorf(codes.Unimplemented, "gke agent sandbox runtime only supports relay-backed public host services, got %q", req.GetEnvVar())
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	s, err := p.sessionLocked(req.GetSessionId())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	if s.plugin != nil || s.pluginStarting {
		return nil, status.Errorf(codes.FailedPrecondition, "plugin runtime session %q already has a running plugin", req.GetSessionId())
	}
	id := p.newID("binding")
	binding := hostServiceBinding{
		id:         id,
		envVar:     req.GetEnvVar(),
		dialTarget: req.GetRelay().GetDialTarget(),
	}
	s.bindings[req.GetEnvVar()] = binding
	return &proto.PluginRuntimeHostServiceBinding{
		Id:        id,
		SessionId: s.id,
		EnvVar:    binding.envVar,
		Relay: &proto.PluginRuntimeHostServiceRelay{
			DialTarget: binding.dialTarget,
		},
	}, nil
}

func (p *Provider) StartPlugin(ctx context.Context, req *proto.StartHostedPluginRequest) (*proto.HostedPlugin, error) {
	if strings.TrimSpace(req.GetCommand()) == "" {
		return nil, status.Error(codes.InvalidArgument, "plugin command is required")
	}
	runtime, cfg, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	p.mu.Lock()
	s, err := p.sessionLocked(req.GetSessionId())
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.NotFound, err.Error())
	}
	if s.plugin != nil || s.pluginStarting {
		p.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "plugin runtime session %q already has a running plugin", req.GetSessionId())
	}
	handle := s.handle
	bindings := cloneBindings(s.bindings)
	s.pluginStarting = true
	s.state = sessionStateStarting
	p.mu.Unlock()

	launchOK := false
	defer func() {
		if launchOK {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), cfg.CleanupTimeout)
		_ = runtime.Exec(cleanupCtx, handle, pluginCleanupCommand(), nil)
		cleanupCancel()
		p.clearPluginStarting(req.GetSessionId())
	}()

	execCtx, cancel := context.WithTimeout(ctx, cfg.ExecTimeout)
	defer cancel()
	if req.GetBundleDir() != "" {
		if err := runtime.CopyBundle(execCtx, handle, req.GetBundleDir(), remoteBundleRoot); err != nil {
			return nil, status.Errorf(codes.Internal, "stage plugin bundle: %v", err)
		}
	}

	env := cloneStringMap(req.GetEnv())
	if env == nil {
		env = map[string]string{}
	}
	for _, binding := range bindings {
		env[binding.envVar] = binding.dialTarget
	}
	env[proto.EnvProviderSocket] = "/tmp/gestalt/plugin.sock"

	launchScript := buildLaunchScript(startProcessRequest{
		Command: req.GetCommand(),
		Args:    req.GetArgs(),
		Env:     env,
		Workdir: func() string {
			if req.GetBundleDir() != "" {
				return remoteBundleRoot
			}
			return ""
		}(),
		PluginPort: cfg.PluginPort,
		SocketPath: env[proto.EnvProviderSocket],
	})
	if err := runtime.Exec(execCtx, handle, []string{"sh", "-c", launchScript}, nil); err != nil {
		return nil, status.Errorf(codes.Internal, "start plugin process in gke agent sandbox: %v", err)
	}

	tunnel, err := runtime.ForwardPort(ctx, handle, cfg.PluginPort)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "open plugin gRPC tunnel: %v", err)
	}
	readyCtx, readyCancel := context.WithTimeout(ctx, cfg.PluginReadyTimeout)
	defer readyCancel()
	if err := waitForPluginReady(readyCtx, tunnel.DialTarget()); err != nil {
		_ = tunnel.Close()
		return nil, status.Errorf(codes.DeadlineExceeded, "wait for gke agent sandbox plugin gRPC endpoint: %v", err)
	}

	plugin := &plugin{
		id:   p.newID("plugin"),
		name: req.GetPluginName(),
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	s, err = p.sessionLocked(req.GetSessionId())
	if err != nil {
		_ = tunnel.Close()
		return nil, status.Error(codes.NotFound, err.Error())
	}
	if !s.pluginStarting || s.plugin != nil {
		_ = tunnel.Close()
		return nil, status.Errorf(codes.FailedPrecondition, "plugin runtime session %q already has a running plugin", req.GetSessionId())
	}
	s.plugin = plugin
	s.pluginStarting = false
	s.pluginTunnel = tunnel
	s.state = sessionStateRunning
	launchOK = true

	return &proto.HostedPlugin{
		Id:         plugin.id,
		SessionId:  s.id,
		PluginName: plugin.name,
		DialTarget: tunnel.DialTarget(),
	}, nil
}

func (p *Provider) Close() error {
	if p == nil {
		return nil
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	sessionIDs := make([]string, 0, len(p.sessions))
	for id := range p.sessions {
		sessionIDs = append(sessionIDs, id)
	}
	runtime := p.runtime
	p.mu.Unlock()

	var errs []error
	for _, id := range sessionIDs {
		stopCtx, cancel := context.WithTimeout(context.Background(), p.cfg.CleanupTimeout)
		_, err := p.StopSession(stopCtx, &proto.StopPluginRuntimeSessionRequest{SessionId: id})
		cancel()
		errs = append(errs, err)
	}
	if runtime != nil {
		errs = append(errs, runtime.Close())
	}
	return errors.Join(errs...)
}

func (p *Provider) clearPluginStarting(sessionID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s, ok := p.sessions[sessionID]; ok && s != nil && s.pluginStarting {
		s.pluginStarting = false
		if s.plugin == nil && s.state == sessionStateStarting {
			s.state = sessionStateReady
		}
	}
}

func (p *Provider) configured() (sandboxRuntime, Config, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.runtime == nil {
		return nil, Config{}, fmt.Errorf("gke agent sandbox runtime is not configured")
	}
	return p.runtime, p.cfg, nil
}

func (p *Provider) sessionLocked(sessionID string) (*session, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil, fmt.Errorf("plugin runtime session id is required")
	}
	s, ok := p.sessions[sessionID]
	if !ok || s == nil {
		return nil, fmt.Errorf("plugin runtime session %q not found", sessionID)
	}
	return s, nil
}

func (p *Provider) newID(prefix string) string {
	return fmt.Sprintf("%s-%06d", prefix, atomic.AddUint64(&p.nextID, 1))
}

func cloneSession(s *session) *proto.PluginRuntimeSession {
	if s == nil {
		return nil
	}
	return &proto.PluginRuntimeSession{
		Id:       s.id,
		State:    s.state,
		Metadata: cloneStringMap(s.metadata),
	}
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	maps.Copy(dst, src)
	return dst
}

func cloneBindings(src map[string]hostServiceBinding) []hostServiceBinding {
	if len(src) == 0 {
		return nil
	}
	dst := make([]hostServiceBinding, 0, len(src))
	for _, binding := range src {
		dst = append(dst, binding)
	}
	return dst
}

func tcpDialTarget(host string, port int) string {
	return "tcp://" + net.JoinHostPort(host, strconv.Itoa(port))
}

func isRelayHostServiceEnv(envVar string) bool {
	switch {
	case isIndexedDBSocketEnv(envVar):
		return true
	case envVar == gestalt.EnvCacheSocket || strings.HasPrefix(envVar, gestalt.EnvCacheSocket+"_"):
		return true
	case envVar == gestalt.EnvS3Socket || strings.HasPrefix(envVar, gestalt.EnvS3Socket+"_"):
		return true
	case envVar == gestalt.EnvAuthorizationSocket:
		return true
	case envVar == proto.EnvPluginInvokerSocket:
		return true
	case envVar == proto.EnvWorkflowManagerSocket:
		return true
	case envVar == proto.EnvAgentManagerSocket:
		return true
	default:
		return false
	}
}

func isIndexedDBSocketEnv(envVar string) bool {
	return envVar == gestalt.EnvIndexedDBSocket || strings.HasPrefix(envVar, gestalt.EnvIndexedDBSocket+"_")
}
