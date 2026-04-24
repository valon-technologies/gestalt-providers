package modal

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	modalclient "github.com/modal-labs/modal-client/go"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"gopkg.in/yaml.v3"
)

const (
	providerVersion     = "0.0.1-alpha.1"
	pluginGRPCPort      = 50051
	tunnelLookupTimeout = 30 * time.Second
	dialTimeout         = 15 * time.Second
	sessionStateReady   = "ready"
	sessionStateRunning = "running"
	sessionStateStopped = "stopped"
	sessionStateFailed  = "failed"

	authorizationSocketEnv = "GESTALT_AUTHORIZATION_SOCKET"
)

var sandboxNamePattern = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

type Config struct {
	App            string        `yaml:"app"`
	TokenID        string        `yaml:"tokenId,omitempty"`
	TokenSecret    string        `yaml:"tokenSecret,omitempty"`
	Environment    string        `yaml:"environment,omitempty"`
	CPU            float64       `yaml:"cpu,omitempty"`
	MemoryMiB      int           `yaml:"memoryMiB,omitempty"`
	MemoryLimitMiB int           `yaml:"memoryLimitMiB,omitempty"`
	Timeout        time.Duration `yaml:"timeout,omitempty"`
	IdleTimeout    time.Duration `yaml:"idleTimeout,omitempty"`
	Cloud          string        `yaml:"cloud,omitempty"`
	Regions        []string      `yaml:"regions,omitempty"`
}

type Provider struct {
	proto.UnimplementedPluginRuntimeProviderServer

	name   string
	client *modalclient.Client
	cfg    Config

	nextID uint64

	mu       sync.Mutex
	sessions map[string]*session
	closed   bool
}

type session struct {
	id       string
	state    string
	metadata map[string]string
	bindings map[string]string
	image    string
	sandbox  *modalclient.Sandbox
	tunnel   *modalclient.Tunnel
	plugin   *plugin
}

type plugin struct {
	id      string
	name    string
	process *modalclient.ContainerProcess
}

func New() *Provider {
	return &Provider{
		sessions: make(map[string]*session),
	}
}

func (p *Provider) Configure(_ context.Context, name string, raw map[string]any) error {
	cfg, err := decodeConfig(raw)
	if err != nil {
		return err
	}
	params := &modalclient.ClientParams{
		TokenID:     cfg.TokenID,
		TokenSecret: cfg.TokenSecret,
		Environment: cfg.Environment,
	}
	client, err := modalclient.NewClientWithOptions(params)
	if err != nil {
		return fmt.Errorf("modal runtime: create client: %w", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		client.Close()
		return fmt.Errorf("modal runtime: provider is closed")
	}
	oldClient := p.client
	p.name = name
	p.cfg = cfg
	p.client = client
	if p.sessions == nil {
		p.sessions = make(map[string]*session)
	}
	if oldClient != nil {
		oldClient.Close()
	}
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	name := p.name
	if strings.TrimSpace(name) == "" {
		name = "modal"
	}
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindRuntime,
		Name:        name,
		DisplayName: "Modal Runtime",
		Description: "Hosted executable-plugin runtime backed by Modal Sandboxes.",
		Version:     providerVersion,
	}
}

func (p *Provider) HealthCheck(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client == nil {
		return fmt.Errorf("modal runtime: not configured")
	}
	if strings.TrimSpace(p.cfg.App) == "" {
		return fmt.Errorf("modal runtime: app is required")
	}
	return nil
}

func (p *Provider) GetSupport(context.Context, *emptypb.Empty) (*proto.PluginRuntimeSupport, error) {
	return &proto.PluginRuntimeSupport{
		CanHostPlugins:    true,
		HostServiceAccess: proto.PluginRuntimeHostServiceAccess_PLUGIN_RUNTIME_HOST_SERVICE_ACCESS_NONE,
		EgressMode:        proto.PluginRuntimeEgressMode_PLUGIN_RUNTIME_EGRESS_MODE_HOSTNAME,
		LaunchMode:        proto.PluginRuntimeLaunchMode_PLUGIN_RUNTIME_LAUNCH_MODE_BUNDLE,
		ExecutionTarget: &proto.PluginRuntimeExecutionTarget{
			Goos:   "linux",
			Goarch: "amd64",
		},
	}, nil
}

func (p *Provider) StartSession(_ context.Context, req *proto.StartPluginRuntimeSessionRequest) (*proto.PluginRuntimeSession, error) {
	_, _, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if strings.TrimSpace(req.GetImage()) == "" {
		return nil, status.Errorf(codes.InvalidArgument, "plugins.%s.runtime.image is required when using the modal runtime", req.GetPluginName())
	}
	sessionID := p.newID("session")

	session := &session{
		id:       sessionID,
		state:    sessionStateReady,
		metadata: cloneStringMap(req.GetMetadata()),
		image:    strings.TrimSpace(req.GetImage()),
	}
	if session.metadata == nil {
		session.metadata = map[string]string{}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, status.Error(codes.FailedPrecondition, "modal runtime is closed")
	}
	p.sessions[sessionID] = session
	return cloneSession(session), nil
}

func (p *Provider) GetSession(ctx context.Context, req *proto.GetPluginRuntimeSessionRequest) (*proto.PluginRuntimeSession, error) {
	p.mu.Lock()
	session, err := p.sessionLocked(req.GetSessionId())
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.NotFound, err.Error())
	}
	sandbox := session.sandbox
	cloned := cloneSession(session)
	p.mu.Unlock()

	if sandbox == nil {
		return cloned, nil
	}
	if code, pollErr := sandbox.Poll(ctx); pollErr == nil && code != nil {
		p.mu.Lock()
		session, err = p.sessionLocked(req.GetSessionId())
		if err != nil {
			p.mu.Unlock()
			return nil, status.Error(codes.NotFound, err.Error())
		}
		if session.state != sessionStateStopped && session.state != sessionStateFailed {
			session.state = sessionStateStopped
		}
		cloned = cloneSession(session)
		p.mu.Unlock()
	}
	return cloned, nil
}

func (p *Provider) StopSession(ctx context.Context, req *proto.StopPluginRuntimeSessionRequest) (*emptypb.Empty, error) {
	var sandbox *modalclient.Sandbox

	p.mu.Lock()
	if session, ok := p.sessions[req.GetSessionId()]; ok {
		delete(p.sessions, req.GetSessionId())
		session.state = sessionStateStopped
		sandbox = session.sandbox
	}
	p.mu.Unlock()

	if sandbox != nil {
		if _, err := sandbox.Terminate(ctx, nil); err != nil {
			return nil, status.Errorf(codes.Internal, "terminate modal sandbox: %v", err)
		}
	}
	return &emptypb.Empty{}, nil
}

func (p *Provider) BindHostService(_ context.Context, req *proto.BindPluginRuntimeHostServiceRequest) (*proto.PluginRuntimeHostServiceBinding, error) {
	if strings.TrimSpace(req.GetEnvVar()) == "" {
		return nil, status.Error(codes.InvalidArgument, "host service env var is required")
	}
	if !isRelayHostServiceEnv(req.GetEnvVar()) {
		return nil, status.Errorf(codes.Unimplemented, "modal runtime only supports relay-backed public host services, got %q", req.GetEnvVar())
	}

	relay := req.GetRelay()
	dialTarget := ""
	switch {
	case relay != nil && strings.TrimSpace(relay.GetDialTarget()) != "":
		dialTarget = strings.TrimSpace(relay.GetDialTarget())
	default:
		return nil, status.Error(codes.Unimplemented, "modal runtime requires relay.dial_target for host services")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	session, err := p.sessionLocked(req.GetSessionId())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	if session.plugin != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "plugin runtime session %q already has a running plugin", req.GetSessionId())
	}
	if session.bindings == nil {
		session.bindings = map[string]string{}
	}
	session.bindings[req.GetEnvVar()] = dialTarget

	return &proto.PluginRuntimeHostServiceBinding{
		Id:        p.newID("binding"),
		SessionId: session.id,
		EnvVar:    req.GetEnvVar(),
		Relay:     relay,
	}, nil
}

func (p *Provider) StartPlugin(ctx context.Context, req *proto.StartHostedPluginRequest) (*proto.HostedPlugin, error) {
	if strings.TrimSpace(req.GetCommand()) == "" {
		return nil, status.Error(codes.InvalidArgument, "plugin command is required")
	}
	client, cfg, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	p.mu.Lock()
	session, err := p.sessionLocked(req.GetSessionId())
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.NotFound, err.Error())
	}
	if session.plugin != nil {
		p.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "plugin runtime session %q already has a running plugin", req.GetSessionId())
	}
	bindings := cloneStringMap(session.bindings)
	p.mu.Unlock()

	sandbox, tunnel, err := p.ensureSessionSandbox(ctx, client, cfg, req)
	if err != nil {
		return nil, err
	}
	launchOK := false
	defer func() {
		if launchOK {
			return
		}
		p.resetSessionSandbox(req.GetSessionId(), sandbox)
		_, _ = sandbox.Terminate(context.Background(), nil)
	}()

	command := req.GetCommand()
	if req.GetBundleDir() != "" {
		if err := uploadBundleDir(ctx, sandbox, req.GetBundleDir(), gestalt.HostedPluginBundleRoot); err != nil {
			return nil, status.Errorf(codes.Internal, "upload plugin bundle: %v", err)
		}
		if strings.HasPrefix(command, gestalt.HostedPluginBundleRoot+"/") || command == gestalt.HostedPluginBundleRoot {
			if err := runSandboxCommand(ctx, sandbox, []string{"chmod", "0755", command}); err != nil {
				return nil, status.Errorf(codes.Internal, "mark plugin entrypoint executable: %v", err)
			}
		}
	}

	env := cloneStringMap(req.GetEnv())
	if env == nil {
		env = map[string]string{}
	}
	for key, value := range bindings {
		env[key] = value
	}
	env[proto.EnvProviderSocket] = fmt.Sprintf("tcp://0.0.0.0:%d", pluginGRPCPort)

	process, err := sandbox.Exec(ctx, append([]string{command}, req.GetArgs()...), &modalclient.SandboxExecParams{
		Stdout: modalclient.Ignore,
		Stderr: modalclient.Ignore,
		Env:    env,
		Workdir: func() string {
			if req.GetBundleDir() != "" {
				return gestalt.HostedPluginBundleRoot
			}
			return ""
		}(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "start plugin process in modal sandbox: %v", err)
	}

	host, port := tunnel.TLSSocket()
	if err := waitForPluginReady(ctx, host, port); err != nil {
		return nil, status.Errorf(codes.DeadlineExceeded, "wait for modal plugin gRPC endpoint: %v", err)
	}

	plugin := &plugin{
		id:      p.newID("plugin"),
		name:    req.GetPluginName(),
		process: process,
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	session, err = p.sessionLocked(req.GetSessionId())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	session.plugin = plugin
	session.state = sessionStateRunning
	go p.watchPluginProcess(req.GetSessionId(), process)
	launchOK = true

	return &proto.HostedPlugin{
		Id:         plugin.id,
		SessionId:  session.id,
		PluginName: plugin.name,
		DialTarget: fmt.Sprintf("tls://%s", net.JoinHostPort(host, fmt.Sprintf("%d", port))),
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
	client := p.client
	p.mu.Unlock()

	var errs []error
	for _, id := range sessionIDs {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := p.StopSession(stopCtx, &proto.StopPluginRuntimeSessionRequest{SessionId: id})
		cancel()
		errs = append(errs, err)
	}
	if client != nil {
		client.Close()
	}
	return errors.Join(errs...)
}

func (p *Provider) configured() (*modalclient.Client, Config, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.client == nil {
		return nil, Config{}, fmt.Errorf("modal runtime is not configured")
	}
	if strings.TrimSpace(p.cfg.App) == "" {
		return nil, Config{}, fmt.Errorf("modal runtime app is required")
	}
	return p.client, p.cfg, nil
}

func (p *Provider) ensureSessionSandbox(ctx context.Context, client *modalclient.Client, cfg Config, req *proto.StartHostedPluginRequest) (*modalclient.Sandbox, *modalclient.Tunnel, error) {
	p.mu.Lock()
	session, err := p.sessionLocked(req.GetSessionId())
	if err != nil {
		p.mu.Unlock()
		return nil, nil, status.Error(codes.NotFound, err.Error())
	}
	if session.sandbox != nil && session.tunnel != nil {
		sandbox := session.sandbox
		tunnel := session.tunnel
		p.mu.Unlock()
		return sandbox, tunnel, nil
	}
	imageRef := strings.TrimSpace(session.image)
	sessionID := session.id
	bindings := cloneStringMap(session.bindings)
	p.mu.Unlock()

	if imageRef == "" {
		return nil, nil, status.Errorf(codes.FailedPrecondition, "plugin runtime session %q is missing a runtime image", req.GetSessionId())
	}
	createParams, err := buildSandboxCreateParams(ctx, cfg, req, sessionID, bindings)
	if err != nil {
		return nil, nil, status.Errorf(codes.FailedPrecondition, "configure modal sandbox egress: %v", err)
	}

	app, err := client.Apps.FromName(ctx, cfg.App, &modalclient.AppFromNameParams{
		Environment:     cfg.Environment,
		CreateIfMissing: true,
	})
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "lookup modal app %q: %v", cfg.App, err)
	}

	sandbox, err := client.Sandboxes.Create(ctx, app, client.Images.FromRegistry(imageRef, nil), createParams)
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "create modal sandbox: %v", err)
	}
	tunnels, err := sandbox.Tunnels(ctx, tunnelLookupTimeout)
	if err != nil {
		_, _ = sandbox.Terminate(context.Background(), nil)
		return nil, nil, status.Errorf(codes.Internal, "lookup modal sandbox tunnel: %v", err)
	}
	tunnel, ok := tunnels[pluginGRPCPort]
	if !ok || tunnel == nil {
		_, _ = sandbox.Terminate(context.Background(), nil)
		return nil, nil, status.Errorf(codes.Internal, "modal sandbox tunnel for port %d is unavailable", pluginGRPCPort)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	session, err = p.sessionLocked(req.GetSessionId())
	if err != nil {
		_, _ = sandbox.Terminate(context.Background(), nil)
		return nil, nil, status.Error(codes.NotFound, err.Error())
	}
	if session.sandbox != nil && session.tunnel != nil {
		_, _ = sandbox.Terminate(context.Background(), nil)
		return session.sandbox, session.tunnel, nil
	}
	session.sandbox = sandbox
	session.tunnel = tunnel
	session.state = sessionStateReady
	return sandbox, tunnel, nil
}

func buildSandboxCreateParams(ctx context.Context, cfg Config, req *proto.StartHostedPluginRequest, sessionID string, bindings map[string]string) (*modalclient.SandboxCreateParams, error) {
	params := &modalclient.SandboxCreateParams{
		CPU:            cfg.CPU,
		MemoryMiB:      cfg.MemoryMiB,
		MemoryLimitMiB: cfg.MemoryLimitMiB,
		Timeout:        cfg.Timeout,
		IdleTimeout:    cfg.IdleTimeout,
		Cloud:          cfg.Cloud,
		Regions:        slicesOrNil(cfg.Regions),
		H2Ports:        []int{pluginGRPCPort},
		Name:           sandboxName(req.GetPluginName(), sessionID),
	}
	if !requiresHostnameProxy(req, req.GetEnv(), bindings) {
		return params, nil
	}
	cidrs, err := egressProxyCIDRAllowlist(ctx, req.GetEnv())
	if err != nil {
		return nil, err
	}
	params.CIDRAllowlist = cidrs
	return params, nil
}

func egressProxyCIDRAllowlist(ctx context.Context, env map[string]string) ([]string, error) {
	proxyURL := strings.TrimSpace(env["HTTPS_PROXY"])
	if proxyURL == "" {
		proxyURL = strings.TrimSpace(env["HTTP_PROXY"])
	}
	if proxyURL == "" {
		return nil, fmt.Errorf("HTTP_PROXY or HTTPS_PROXY is required when hostname-based egress controls are enabled")
	}
	parsed, err := url.Parse(proxyURL)
	if err != nil {
		return nil, fmt.Errorf("parse proxy URL %q: %w", proxyURL, err)
	}
	host := strings.TrimSpace(parsed.Hostname())
	if host == "" {
		return nil, fmt.Errorf("proxy URL %q is missing a hostname", proxyURL)
	}
	ipAddrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("resolve proxy host %q: %w", host, err)
	}
	if len(ipAddrs) == 0 {
		return nil, fmt.Errorf("proxy host %q did not resolve to any IPs", host)
	}
	seen := map[string]struct{}{}
	cidrs := make([]string, 0, len(ipAddrs))
	for _, ipAddr := range ipAddrs {
		ip := ipAddr.IP
		if ip == nil {
			continue
		}
		cidr := ip.String() + "/128"
		if ip.To4() != nil {
			cidr = ip.String() + "/32"
		}
		if _, ok := seen[cidr]; ok {
			continue
		}
		seen[cidr] = struct{}{}
		cidrs = append(cidrs, cidr)
	}
	if len(cidrs) == 0 {
		return nil, fmt.Errorf("proxy host %q did not resolve to any usable IPs", host)
	}
	sort.Strings(cidrs)
	return cidrs, nil
}

func requiresHostnameProxy(req *proto.StartHostedPluginRequest, env map[string]string, bindings map[string]string) bool {
	if req == nil {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(req.GetDefaultAction()), "deny") {
		return true
	}
	for _, proxyEnv := range []string{"HTTPS_PROXY", "HTTP_PROXY"} {
		if strings.TrimSpace(env[proxyEnv]) != "" {
			return true
		}
	}
	relayHosts := relayHostnameSet(bindings)
	for _, host := range req.GetAllowedHosts() {
		if _, ok := relayHosts[normalizeHostname(host)]; !ok {
			return true
		}
	}
	return false
}

func relayHostnameSet(bindings map[string]string) map[string]struct{} {
	hosts := make(map[string]struct{}, len(bindings))
	for _, dialTarget := range bindings {
		host := hostnameFromDialTarget(dialTarget)
		if host == "" {
			continue
		}
		hosts[host] = struct{}{}
	}
	return hosts
}

func hostnameFromDialTarget(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return normalizeHostname(u.Hostname())
}

func normalizeHostname(host string) string {
	return strings.ToLower(strings.TrimSpace(host))
}

func (p *Provider) resetSessionSandbox(sessionID string, sandbox *modalclient.Sandbox) {
	if sandbox == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	session, ok := p.sessions[strings.TrimSpace(sessionID)]
	if !ok || session == nil {
		return
	}
	if session.sandbox != sandbox {
		return
	}
	session.sandbox = nil
	session.tunnel = nil
	if session.plugin == nil {
		session.state = sessionStateReady
	}
}

func (p *Provider) watchPluginProcess(sessionID string, process *modalclient.ContainerProcess) {
	if process == nil {
		return
	}
	code, err := process.Wait(context.Background())
	p.mu.Lock()
	defer p.mu.Unlock()
	session, ok := p.sessions[sessionID]
	if !ok || session == nil {
		return
	}
	if err != nil {
		session.state = sessionStateFailed
		return
	}
	if code == 0 {
		session.state = sessionStateStopped
		return
	}
	session.state = sessionStateFailed
}

func (p *Provider) sessionLocked(sessionID string) (*session, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil, fmt.Errorf("plugin runtime session id is required")
	}
	session, ok := p.sessions[sessionID]
	if !ok || session == nil {
		return nil, fmt.Errorf("plugin runtime session %q not found", sessionID)
	}
	return session, nil
}

func (p *Provider) newID(prefix string) string {
	return fmt.Sprintf("%s-%06d", prefix, atomic.AddUint64(&p.nextID, 1))
}

func decodeConfig(raw map[string]any) (Config, error) {
	values := raw
	if nested, ok := raw["config"]; ok && nested != nil {
		nestedMap, ok := nested.(map[string]any)
		if !ok {
			return Config{}, fmt.Errorf("modal runtime config must be an object")
		}
		values = nestedMap
	}
	data, err := yaml.Marshal(values)
	if err != nil {
		return Config{}, fmt.Errorf("modal runtime: encode config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("modal runtime: decode config: %w", err)
	}
	cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c *Config) Normalize() {
	if c == nil {
		return
	}
	c.App = strings.TrimSpace(c.App)
	c.TokenID = strings.TrimSpace(c.TokenID)
	c.TokenSecret = strings.TrimSpace(c.TokenSecret)
	c.Environment = strings.TrimSpace(c.Environment)
	c.Cloud = strings.TrimSpace(c.Cloud)
	for i := range c.Regions {
		c.Regions[i] = strings.TrimSpace(c.Regions[i])
	}
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.App) == "" {
		return fmt.Errorf("modal runtime app is required")
	}
	if (c.TokenID == "") != (c.TokenSecret == "") {
		return fmt.Errorf("modal runtime tokenId and tokenSecret must be set together")
	}
	if c.CPU < 0 {
		return fmt.Errorf("modal runtime cpu must be non-negative")
	}
	if c.MemoryMiB < 0 {
		return fmt.Errorf("modal runtime memoryMiB must be non-negative")
	}
	if c.MemoryLimitMiB < 0 {
		return fmt.Errorf("modal runtime memoryLimitMiB must be non-negative")
	}
	if c.Timeout < 0 {
		return fmt.Errorf("modal runtime timeout must be non-negative")
	}
	if c.IdleTimeout < 0 {
		return fmt.Errorf("modal runtime idleTimeout must be non-negative")
	}
	return nil
}

func cloneSession(session *session) *proto.PluginRuntimeSession {
	if session == nil {
		return nil
	}
	return &proto.PluginRuntimeSession{
		Id:       session.id,
		State:    session.state,
		Metadata: cloneStringMap(session.metadata),
	}
}

func uploadBundleDir(ctx context.Context, sandbox *modalclient.Sandbox, localDir, remoteDir string) error {
	if err := runSandboxCommand(ctx, sandbox, []string{"mkdir", "-p", remoteDir}); err != nil {
		return err
	}
	return filepath.Walk(localDir, func(localPath string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(localDir, localPath)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		remotePath := path.Join(remoteDir, filepath.ToSlash(rel))
		if info.IsDir() {
			return runSandboxCommand(ctx, sandbox, []string{"mkdir", "-p", remotePath})
		}
		if err := runSandboxCommand(ctx, sandbox, []string{"mkdir", "-p", path.Dir(remotePath)}); err != nil {
			return err
		}
		src, err := os.Open(localPath)
		if err != nil {
			return err
		}
		dst, err := sandbox.Open(ctx, remotePath, "wb")
		if err != nil {
			_ = src.Close()
			return err
		}
		if _, err := io.Copy(dst, src); err != nil {
			_ = src.Close()
			_ = dst.Close()
			return err
		}
		if err := src.Close(); err != nil {
			_ = dst.Close()
			return err
		}
		if err := dst.Flush(); err != nil {
			_ = dst.Close()
			return err
		}
		if err := dst.Close(); err != nil {
			return err
		}
		if info.Mode().Perm()&0o111 != 0 {
			if err := runSandboxCommand(ctx, sandbox, []string{"chmod", fmt.Sprintf("%03o", info.Mode().Perm()), remotePath}); err != nil {
				return err
			}
		}
		return nil
	})
}

func runSandboxCommand(ctx context.Context, sandbox *modalclient.Sandbox, argv []string) error {
	process, err := sandbox.Exec(ctx, argv, &modalclient.SandboxExecParams{
		Stdout: modalclient.Ignore,
		Stderr: modalclient.Ignore,
	})
	if err != nil {
		return err
	}
	code, err := process.Wait(ctx)
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("command %q exited with status %d", strings.Join(argv, " "), code)
	}
	return nil
}

func waitForPluginReady(ctx context.Context, host string, port int) error {
	deadlineCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	for {
		conn, err := dialTLSPlugin(deadlineCtx, host, port)
		if err == nil {
			client := proto.NewProviderLifecycleClient(conn)
			_, rpcErr := client.GetProviderIdentity(deadlineCtx, &emptypb.Empty{})
			_ = conn.Close()
			if rpcErr == nil {
				return nil
			}
			err = rpcErr
		}
		select {
		case <-deadlineCtx.Done():
			return fmt.Errorf("connect to %s:%d: %w", host, port, err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func dialTLSPlugin(_ context.Context, host string, port int) (*grpc.ClientConn, error) {
	if strings.TrimSpace(host) == "" || port <= 0 {
		return nil, fmt.Errorf("modal plugin tunnel is not configured")
	}
	address := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	conn, err := grpc.NewClient(
		"dns:///"+address,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: host,
			NextProtos: []string{"h2"},
		})),
	)
	if err != nil {
		return nil, err
	}
	conn.Connect()
	return conn, nil
}

func sandboxName(pluginName, sessionID string) string {
	name := strings.ToLower(strings.TrimSpace(pluginName))
	if name == "" {
		name = "plugin"
	}
	name = sandboxNamePattern.ReplaceAllString(name, "-")
	name = strings.Trim(name, "-")
	if name == "" {
		name = "plugin"
	}
	value := fmt.Sprintf("gestalt-%s-%s", name, sessionID)
	if len(value) <= 63 {
		return value
	}
	return strings.TrimRight(value[:63], "-")
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func isIndexedDBSocketEnv(envVar string) bool {
	envVar = strings.TrimSpace(envVar)
	return envVar == gestalt.EnvIndexedDBSocket || strings.HasPrefix(envVar, gestalt.EnvIndexedDBSocket+"_")
}

func isRelayHostServiceEnv(envVar string) bool {
	envVar = strings.TrimSpace(envVar)
	switch {
	case isIndexedDBSocketEnv(envVar):
		return true
	case envVar == gestalt.EnvAgentHostSocket:
		return true
	case envVar == gestalt.EnvCacheSocket || strings.HasPrefix(envVar, gestalt.EnvCacheSocket+"_"):
		return true
	case envVar == gestalt.EnvS3Socket || strings.HasPrefix(envVar, gestalt.EnvS3Socket+"_"):
		return true
	case envVar == authorizationSocketEnv:
		return true
	case envVar == proto.EnvPluginInvokerSocket:
		return true
	case envVar == proto.EnvWorkflowManagerSocket:
		return true
	default:
		return false
	}
}

func slicesOrNil[T any](in []T) []T {
	if len(in) == 0 {
		return nil
	}
	return append([]T(nil), in...)
}
