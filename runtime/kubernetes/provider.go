package kubernetes

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"maps"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	providerVersion      = "0.0.1-alpha.1"
	sessionStatePending  = "pending"
	sessionStateReady    = "ready"
	sessionStateStarting = "starting"
	sessionStateRunning  = "running"
	sessionStateStopped  = "stopped"
	sessionStateFailed   = "failed"
)

const envProviderSocket = "GESTALT_PLUGIN_SOCKET"
const envProviderName = "GESTALT_PLUGIN_NAME"

type Provider struct {
	name    string
	cfg     Config
	runtime runtimeBackend

	instanceID string
	nextID     uint64

	mu       sync.Mutex
	sessions map[string]*localSession
	closed   bool
}

type localSession struct {
	pluginTunnel tunnel
}

type hostServiceBinding struct {
	id         string
	envVar     string
	dialTarget string
}

func New() *Provider {
	return &Provider{
		instanceID: newProviderInstanceID(),
		sessions:   make(map[string]*localSession),
	}
}

func (p *Provider) Configure(ctx context.Context, name string, raw map[string]any) error {
	cfg, err := decodeConfig(raw)
	if err != nil {
		return err
	}
	runtime, err := newKubernetesRuntime(cfg)
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		_ = runtime.Close()
		return fmt.Errorf("kubernetes runtime: provider is closed")
	}
	oldRuntime := p.runtime
	p.name = strings.TrimSpace(name)
	p.cfg = cfg
	p.runtime = runtime
	if p.sessions == nil {
		p.sessions = make(map[string]*localSession)
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
		name = "kubernetes"
	}
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindRuntime,
		Name:        name,
		DisplayName: "Kubernetes Runtime",
		Description: "Hosted executable-plugin runtime backed by Kubernetes.",
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

func (p *Provider) GetSupport(context.Context) (gestalt.AppRuntimeSupport, error) {
	_, cfg, err := p.configured()
	if err != nil {
		return gestalt.AppRuntimeSupport{}, err
	}
	egressMode := gestalt.AppRuntimeEgressModeNone
	if cfg.HostnameEgress.Mode == hostnameEgressModePublicProxy {
		egressMode = gestalt.AppRuntimeEgressModeHostname
	}
	return gestalt.AppRuntimeSupport{
		CanHostApps: true,
		EgressMode:     egressMode,
	}, nil
}

func (p *Provider) StartSession(ctx context.Context, req gestalt.StartAppRuntimeSessionRequest) (gestalt.AppRuntimeSession, error) {
	runtime, cfg, err := p.configured()
	if err != nil {
		return gestalt.AppRuntimeSession{}, status.Error(codes.FailedPrecondition, err.Error())
	}

	template := strings.TrimSpace(req.Template)
	image := strings.TrimSpace(req.Image)
	if template == "" && image == "" {
		return gestalt.AppRuntimeSession{}, status.Errorf(codes.InvalidArgument, "plugins.%s.execution.runtime.image or execution.runtime.template is required when using the kubernetes runtime", req.AppName)
	}

	sessionID := runtimeResourceName(req.AppName, p.runtimeInstanceID(), p.newID("session"))
	session, err := runtime.Start(ctx, startRuntimeSessionRequest{
		Name:             sessionID,
		AppName:       req.AppName,
		Namespace:        cfg.Namespace,
		Template:         template,
		Image:            image,
		DockerConfigJSON: dockerConfigJSON(req.ImagePullAuth),
		Metadata:         cloneStringMap(req.Metadata),
	})
	if err != nil {
		return gestalt.AppRuntimeSession{}, status.Errorf(codes.Internal, "start kubernetes session: %v", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		_ = runtime.Stop(context.Background(), session.Handle)
		return gestalt.AppRuntimeSession{}, status.Error(codes.FailedPrecondition, "kubernetes runtime is closed")
	}
	if p.sessions == nil {
		p.sessions = make(map[string]*localSession)
	}
	p.sessions[session.ID] = &localSession{}
	return appRuntimeSession(session, sessionStateReady), nil
}

func (p *Provider) GetSession(ctx context.Context, sessionID string) (gestalt.AppRuntimeSession, error) {
	runtime, cfg, err := p.configured()
	if err != nil {
		return gestalt.AppRuntimeSession{}, status.Error(codes.FailedPrecondition, err.Error())
	}
	session, err := runtime.ResolveSession(ctx, cfg.Namespace, sessionID)
	if err != nil {
		return gestalt.AppRuntimeSession{}, status.Error(codes.NotFound, err.Error())
	}
	return appRuntimeSession(session, sessionStateForRuntime(ctx, runtime, session)), nil
}

func (p *Provider) ListSessions(ctx context.Context) ([]gestalt.AppRuntimeSession, error) {
	runtime, cfg, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	sessions, err := runtime.ListSessions(ctx, cfg.Namespace)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	sort.Slice(sessions, func(i, j int) bool {
		return sessions[i].ID < sessions[j].ID
	})
	out := make([]gestalt.AppRuntimeSession, 0, len(sessions))
	for _, session := range sessions {
		out = append(out, appRuntimeSession(session, sessionStateForRuntime(ctx, runtime, session)))
	}
	return out, nil
}

func (p *Provider) StopSession(ctx context.Context, sessionID string) error {
	runtime, cfg, err := p.configured()
	if err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	session, err := runtime.ResolveSession(ctx, cfg.Namespace, sessionID)
	if err != nil {
		return status.Error(codes.NotFound, err.Error())
	}

	if tunnel := p.localTunnel(session.ID); tunnel != nil {
		_ = tunnel.Close()
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), cfg.CleanupTimeout)
	_ = runtime.Exec(cleanupCtx, session.Handle, pluginCleanupCommand(), nil)
	cleanupCancel()

	if err := runtime.Stop(ctx, session.Handle); err != nil {
		return status.Errorf(codes.Internal, "stop kubernetes session: %v", err)
	}
	if err := runtime.DeleteHostnameEgressPolicy(ctx, session.Handle, hostnameEgressPolicyName(session.Handle)); err != nil {
		return status.Errorf(codes.Internal, "stop kubernetes session: %v", err)
	}
	p.clearLocalSession(session.ID)
	return nil
}

func (p *Provider) StartApp(ctx context.Context, req gestalt.StartHostedAppRequest) (gestalt.HostedApp, error) {
	if strings.TrimSpace(req.Command) == "" {
		return gestalt.HostedApp{}, status.Error(codes.InvalidArgument, "plugin command is required")
	}
	runtime, cfg, err := p.configured()
	if err != nil {
		return gestalt.HostedApp{}, status.Error(codes.FailedPrecondition, err.Error())
	}

	session, err := runtime.ResolveSession(ctx, cfg.Namespace, req.SessionID)
	if err != nil {
		return gestalt.HostedApp{}, status.Error(codes.NotFound, err.Error())
	}
	if state := sessionStateForRuntime(ctx, runtime, session); state == sessionStateRunning || state == sessionStateStarting {
		return gestalt.HostedApp{}, status.Errorf(codes.FailedPrecondition, "plugin runtime session %q already has a running plugin", req.SessionID)
	} else if state == sessionStateFailed {
		return gestalt.HostedApp{}, status.Errorf(codes.FailedPrecondition, "plugin runtime session %q has failed", req.SessionID)
	}

	holder := p.runtimeInstanceID() + "/" + p.newID("plugin-start")
	if err := runtime.AcquirePluginStartLease(ctx, session.Handle, holder, pluginStartLeaseDuration(cfg)); err != nil {
		if errors.Is(err, errPluginAlreadyStarted) {
			return gestalt.HostedApp{}, status.Errorf(codes.FailedPrecondition, "plugin runtime session %q already has a running plugin", req.SessionID)
		}
		return gestalt.HostedApp{}, status.Errorf(codes.Internal, "acquire kubernetes plugin start lease: %v", err)
	}
	leaseHeld := true
	releaseLease := func() {
		if !leaseHeld {
			return
		}
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), cfg.CleanupTimeout)
		_ = runtime.ReleasePluginStartLease(releaseCtx, session.Handle, holder)
		releaseCancel()
		leaseHeld = false
	}
	defer releaseLease()

	lockedSession, err := runtime.ResolveSession(ctx, cfg.Namespace, session.ID)
	if err != nil {
		return gestalt.HostedApp{}, status.Error(codes.NotFound, err.Error())
	}
	if state := sessionStateForRuntime(ctx, runtime, lockedSession); state == sessionStateRunning {
		return gestalt.HostedApp{}, status.Errorf(codes.FailedPrecondition, "plugin runtime session %q already has a running plugin", req.SessionID)
	} else if state == sessionStateFailed {
		return gestalt.HostedApp{}, status.Errorf(codes.FailedPrecondition, "plugin runtime session %q has failed", req.SessionID)
	}
	session = lockedSession
	handle := session.Handle

	launchOK := false
	hostnamePolicyName := ""
	execCleanupNeeded := false
	defer func() {
		if launchOK {
			return
		}
		cleanupSucceeded := true
		if execCleanupNeeded {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), cfg.CleanupTimeout)
			if err := runtime.Exec(cleanupCtx, handle, pluginCleanupCommand(), nil); err != nil {
				cleanupSucceeded = false
			}
			cleanupCancel()
		}
		if hostnamePolicyName != "" && cleanupSucceeded {
			deleteCtx, deleteCancel := context.WithTimeout(context.Background(), cfg.CleanupTimeout)
			_ = runtime.DeleteHostnameEgressPolicy(deleteCtx, handle, hostnamePolicyName)
			deleteCancel()
		}
	}()

	execCtx, cancel := context.WithTimeout(ctx, cfg.ExecTimeout)
	defer cancel()

	env := buildPluginEnv(req, "/tmp/gestalt/plugin.sock")
	if requiresHostnameEgress(req, env) {
		hostnameEgress, err := buildHostnameEgressConfig(env, session.Template)
		if err != nil {
			return gestalt.HostedApp{}, hostnameEgressStatus("", err)
		}
		hostnamePolicyName, err = runtime.EnsureHostnameEgressPolicy(execCtx, handle, hostnameEgress)
		if err != nil {
			return gestalt.HostedApp{}, hostnameEgressStatus("configure hosted hostname egress", err)
		}
	}

	launchScript := buildLaunchScript(startProcessRequest{
		Command:    req.Command,
		Args:       req.Args,
		Env:        env,
		PluginPort: cfg.PluginPort,
		SocketPath: env[envProviderSocket],
	})
	execCleanupNeeded = true
	if err := runtime.Exec(execCtx, handle, []string{"sh", "-c", launchScript}, nil); err != nil {
		return gestalt.HostedApp{}, status.Errorf(codes.Internal, "start plugin process in kubernetes: %v", err)
	}
	if err := waitForSocketProxyReady(execCtx, runtime, handle, cfg.PluginPort, env[envProviderSocket]); err != nil {
		return gestalt.HostedApp{}, status.Errorf(codes.DeadlineExceeded, "wait for in-runtime plugin socket proxy: %v", err)
	}

	tunnel, err := openPluginTunnel(ctx, runtime, handle, cfg)
	if err != nil {
		return gestalt.HostedApp{}, status.Errorf(codes.Internal, "open plugin gRPC connection: %v", err)
	}
	readyCtx, readyCancel := context.WithTimeout(ctx, cfg.PluginReadyTimeout)
	defer readyCancel()
	if err := waitForPluginReady(readyCtx, tunnel.DialTarget()); err != nil {
		_ = tunnel.Close()
		return gestalt.HostedApp{}, status.Errorf(codes.DeadlineExceeded, "wait for kubernetes plugin gRPC endpoint: %v", err)
	}
	if err := runtime.MarkPluginStarted(ctx, handle, holder, req.AppName); err != nil {
		_ = tunnel.Close()
		if errors.Is(err, errPluginAlreadyStarted) {
			return gestalt.HostedApp{}, status.Errorf(codes.FailedPrecondition, "plugin runtime session %q already has a running plugin", req.SessionID)
		}
		return gestalt.HostedApp{}, status.Errorf(codes.Internal, "mark kubernetes plugin started: %v", err)
	}

	p.setLocalTunnel(session.ID, tunnel)
	launchOK = true
	releaseLease()

	return gestalt.HostedApp{
		ID:         p.newID("plugin"),
		SessionID:  session.ID,
		AppName: req.AppName,
		DialTarget: tunnel.DialTarget(),
	}, nil
}

func openPluginTunnel(ctx context.Context, runtime runtimeBackend, handle runtimeHandle, cfg Config) (tunnel, error) {
	switch cfg.ConnectionMode {
	case connectionModeServiceDNS:
		return runtime.ServiceDNSDialTarget(ctx, handle, cfg.PluginPort)
	case connectionModePodIP:
		return runtime.PodIPDialTarget(ctx, handle, cfg.PluginPort)
	default:
		return runtime.ForwardPort(ctx, handle, cfg.PluginPort)
	}
}

func waitForSocketProxyReady(ctx context.Context, runtime runtimeBackend, handle runtimeHandle, port int, socketPath string) error {
	command := socketProxyReadyCommand(port, socketPath)
	var lastErr error
	for {
		if err := runtime.Exec(ctx, handle, command, nil); err == nil {
			return nil
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("%w: %v", ctx.Err(), lastErr)
			}
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
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
	localSessions := p.sessions
	p.sessions = make(map[string]*localSession)
	runtime := p.runtime
	p.mu.Unlock()

	for _, local := range localSessions {
		if local != nil && local.pluginTunnel != nil {
			_ = local.pluginTunnel.Close()
		}
	}
	if runtime != nil {
		return runtime.Close()
	}
	return nil
}

func (p *Provider) configured() (runtimeBackend, Config, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.runtime == nil {
		return nil, Config{}, fmt.Errorf("kubernetes runtime is not configured")
	}
	return p.runtime, p.cfg, nil
}

func (p *Provider) setLocalTunnel(sessionID string, tunnel tunnel) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sessions == nil {
		p.sessions = make(map[string]*localSession)
	}
	local := p.sessions[sessionID]
	if local == nil {
		local = &localSession{}
		p.sessions[sessionID] = local
	}
	local.pluginTunnel = tunnel
}

func (p *Provider) localTunnel(sessionID string) tunnel {
	p.mu.Lock()
	defer p.mu.Unlock()
	if local := p.sessions[sessionID]; local != nil {
		return local.pluginTunnel
	}
	return nil
}

func (p *Provider) clearLocalSession(sessionID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.sessions, sessionID)
}

func (p *Provider) newID(prefix string) string {
	return fmt.Sprintf("%s-%06d", prefix, atomic.AddUint64(&p.nextID, 1))
}

func (p *Provider) runtimeInstanceID() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if strings.TrimSpace(p.instanceID) == "" {
		p.instanceID = newProviderInstanceID()
	}
	return p.instanceID
}

func newProviderInstanceID() string {
	var bytes [4]byte
	if _, err := rand.Read(bytes[:]); err == nil {
		return fmt.Sprintf("%x", bytes[:])
	}
	return sanitizeDNSLabelValue(strconv.FormatInt(time.Now().UnixNano(), 36))
}

func appRuntimeSession(session runtimeSession, state string) gestalt.AppRuntimeSession {
	if state == "" {
		state = sessionStateReady
	}
	return gestalt.AppRuntimeSession{
		ID:       session.ID,
		State:    state,
		Metadata: cloneStringMap(session.Metadata),
	}
}

func sessionStateForRuntime(ctx context.Context, runtime runtimeBackend, session runtimeSession) string {
	if session.Failed {
		return sessionStateFailed
	}
	if session.PluginStarted {
		if err := runtime.Exec(ctx, session.Handle, pluginHealthCommand(), nil); err != nil {
			return sessionStateFailed
		}
		return sessionStateRunning
	}
	if !session.Handle.Ready {
		return sessionStatePending
	}
	if session.PluginStarting {
		return sessionStateStarting
	}
	return sessionStateReady
}

func pluginStartLeaseDuration(cfg Config) time.Duration {
	duration := cfg.ExecTimeout + cfg.PluginReadyTimeout + cfg.CleanupTimeout
	if duration < time.Minute {
		return time.Minute
	}
	return duration
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

func buildPluginEnv(req gestalt.StartHostedAppRequest, providerSocket string) map[string]string {
	env := cloneStringMap(req.Env)
	if env == nil {
		env = map[string]string{}
	}
	env[envProviderSocket] = providerSocket
	if appName := strings.TrimSpace(req.AppName); appName != "" {
		env[envProviderName] = appName
	}
	return env
}

func dockerConfigJSON(auth *gestalt.AppRuntimeImagePullAuth) string {
	if auth == nil {
		return ""
	}
	return strings.TrimSpace(auth.DockerConfigJSON)
}

func tcpDialTarget(host string, port int) string {
	return "tcp://" + net.JoinHostPort(host, strconv.Itoa(port))
}

func normalizeHostServiceEnvVar(envVar string) (string, error) {
	envVar = strings.TrimSpace(envVar)
	if envVar == "" {
		return "", fmt.Errorf("host service env var is required")
	}
	if !isHostServiceEnvVar(envVar) {
		return "", fmt.Errorf("host service env var %q is invalid", envVar)
	}
	return envVar, nil
}

func isHostServiceEnvVar(envVar string) bool {
	if envVar == "" {
		return false
	}
	for i, r := range envVar {
		switch {
		case r >= 'A' && r <= 'Z':
		case r >= 'a' && r <= 'z':
		case r == '_':
		case i > 0 && r >= '0' && r <= '9':
		default:
			return false
		}
	}
	first := envVar[0]
	return first == '_' || (first >= 'A' && first <= 'Z') || (first >= 'a' && first <= 'z')
}

var _ gestalt.AppRuntimeProvider = (*Provider)(nil)
var _ gestalt.MetadataProvider = (*Provider)(nil)
var _ gestalt.HealthChecker = (*Provider)(nil)
var _ gestalt.Closer = (*Provider)(nil)
