package modal

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	modalclient "github.com/modal-labs/modal-client/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	publicruntime "github.com/valon-technologies/gestalt/server/pluginruntime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"
	"gopkg.in/yaml.v3"
)

const (
	pluginGRPCPort      = 50051
	tunnelLookupTimeout = 30 * time.Second
	dialTimeout         = 15 * time.Second
)

var sandboxNamePattern = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

type Config struct {
	App            string        `yaml:"app"`
	Environment    string        `yaml:"environment,omitempty"`
	CPU            float64       `yaml:"cpu,omitempty"`
	MemoryMiB      int           `yaml:"memoryMiB,omitempty"`
	MemoryLimitMiB int           `yaml:"memoryLimitMiB,omitempty"`
	Timeout        time.Duration `yaml:"timeout,omitempty"`
	IdleTimeout    time.Duration `yaml:"idleTimeout,omitempty"`
	Cloud          string        `yaml:"cloud,omitempty"`
	Regions        []string      `yaml:"regions,omitempty"`
}

func DecodeConfig(node yaml.Node) (Config, error) {
	var cfg Config
	if err := node.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode modal runtime config: %w", err)
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
	c.Environment = strings.TrimSpace(c.Environment)
	c.Cloud = strings.TrimSpace(c.Cloud)
	for i := range c.Regions {
		c.Regions[i] = strings.TrimSpace(c.Regions[i])
	}
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.App) == "" {
		return fmt.Errorf("modal runtime config.app is required")
	}
	if c.CPU < 0 {
		return fmt.Errorf("modal runtime config.cpu must be non-negative")
	}
	if c.MemoryMiB < 0 {
		return fmt.Errorf("modal runtime config.memoryMiB must be non-negative")
	}
	if c.MemoryLimitMiB < 0 {
		return fmt.Errorf("modal runtime config.memoryLimitMiB must be non-negative")
	}
	if c.Timeout < 0 {
		return fmt.Errorf("modal runtime config.timeout must be non-negative")
	}
	if c.IdleTimeout < 0 {
		return fmt.Errorf("modal runtime config.idleTimeout must be non-negative")
	}
	return nil
}

type Provider struct {
	client *modalclient.Client
	cfg    Config

	nextID uint64

	mu       sync.Mutex
	sessions map[string]*session
	closed   bool
}

type session struct {
	id       string
	state    publicruntime.SessionState
	metadata map[string]string
	sandbox  *modalclient.Sandbox
	tunnel   *modalclient.Tunnel
	plugin   *plugin
	cleanup  func()
}

type plugin struct {
	id      string
	name    string
	host    string
	port    int
	process *modalclient.ContainerProcess
}

type hostedPluginConn struct {
	conn      *grpc.ClientConn
	lifecycle proto.ProviderLifecycleClient
	plugin    proto.IntegrationProviderClient
}

func New(cfg Config) (*Provider, error) {
	cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	client, err := modalclient.NewClient()
	if err != nil {
		return nil, fmt.Errorf("create Modal client: %w", err)
	}
	return &Provider{
		client:   client,
		cfg:      cfg,
		sessions: make(map[string]*session),
	}, nil
}

func (p *Provider) Capabilities(context.Context) (publicruntime.Capabilities, error) {
	return publicruntime.Capabilities{
		HostedPluginRuntime: true,
		ProviderGRPCTunnel:  true,
		CIDREgress:          true,
		ExecutionGOOS:       "linux",
		ExecutionGOARCH:     "amd64",
	}, nil
}

func (p *Provider) StartSession(ctx context.Context, req publicruntime.StartSessionRequest) (*publicruntime.Session, error) {
	if p == nil || p.client == nil {
		return nil, fmt.Errorf("modal plugin runtime is not configured")
	}
	if strings.TrimSpace(req.Image) == "" {
		return nil, fmt.Errorf("plugins.%s.runtime.image is required when using the modal runtime", req.PluginName)
	}

	app, err := p.client.Apps.FromName(ctx, p.cfg.App, &modalclient.AppFromNameParams{
		Environment:     p.cfg.Environment,
		CreateIfMissing: true,
	})
	if err != nil {
		return nil, fmt.Errorf("lookup modal app %q: %w", p.cfg.App, err)
	}

	image := p.client.Images.FromRegistry(req.Image, nil)
	sessionID := p.newID("session")
	sandbox, err := p.client.Sandboxes.Create(ctx, app, image, &modalclient.SandboxCreateParams{
		CPU:            p.cfg.CPU,
		MemoryMiB:      p.cfg.MemoryMiB,
		MemoryLimitMiB: p.cfg.MemoryLimitMiB,
		Timeout:        p.cfg.Timeout,
		IdleTimeout:    p.cfg.IdleTimeout,
		Cloud:          p.cfg.Cloud,
		Regions:        slicesOrNil(p.cfg.Regions),
		H2Ports:        []int{pluginGRPCPort},
		Name:           sandboxName(req.PluginName, sessionID),
	})
	if err != nil {
		return nil, fmt.Errorf("create modal sandbox: %w", err)
	}

	tunnels, err := sandbox.Tunnels(ctx, tunnelLookupTimeout)
	if err != nil {
		_, _ = sandbox.Terminate(context.Background(), nil)
		return nil, fmt.Errorf("lookup modal sandbox tunnel: %w", err)
	}
	tunnel, ok := tunnels[pluginGRPCPort]
	if !ok || tunnel == nil {
		_, _ = sandbox.Terminate(context.Background(), nil)
		return nil, fmt.Errorf("modal sandbox tunnel for port %d is unavailable", pluginGRPCPort)
	}

	session := &session{
		id:       sessionID,
		state:    publicruntime.SessionStateReady,
		metadata: cloneStringMap(req.Metadata),
		sandbox:  sandbox,
		tunnel:   tunnel,
	}
	if session.metadata == nil {
		session.metadata = map[string]string{}
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_, _ = sandbox.Terminate(context.Background(), nil)
		return nil, fmt.Errorf("modal plugin runtime is closed")
	}
	p.sessions[sessionID] = session
	p.mu.Unlock()
	return cloneSession(session), nil
}

func (p *Provider) GetSession(ctx context.Context, req publicruntime.GetSessionRequest) (*publicruntime.Session, error) {
	if p == nil {
		return nil, fmt.Errorf("modal plugin runtime is not configured")
	}
	p.mu.Lock()
	session, err := p.sessionLocked(req.SessionID)
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}
	sandbox := session.sandbox
	cloned := cloneSession(session)
	p.mu.Unlock()

	if sandbox == nil {
		return cloned, nil
	}
	if code, pollErr := sandbox.Poll(ctx); pollErr == nil && code != nil {
		p.mu.Lock()
		session, err = p.sessionLocked(req.SessionID)
		if err != nil {
			p.mu.Unlock()
			return nil, err
		}
		if session.state != publicruntime.SessionStateStopped && session.state != publicruntime.SessionStateFailed {
			session.state = publicruntime.SessionStateStopped
		}
		cloned = cloneSession(session)
		p.mu.Unlock()
	}
	return cloned, nil
}

func (p *Provider) StopSession(ctx context.Context, req publicruntime.StopSessionRequest) error {
	if p == nil {
		return nil
	}

	var (
		sandbox *modalclient.Sandbox
		cleanup func()
	)
	p.mu.Lock()
	if session, ok := p.sessions[req.SessionID]; ok {
		delete(p.sessions, req.SessionID)
		session.state = publicruntime.SessionStateStopped
		sandbox = session.sandbox
		cleanup = session.cleanup
	}
	p.mu.Unlock()

	var errs []error
	if sandbox != nil {
		if _, err := sandbox.Terminate(ctx, nil); err != nil {
			errs = append(errs, fmt.Errorf("terminate modal sandbox: %w", err))
		}
	}
	if cleanup != nil {
		cleanup()
	}
	return errors.Join(errs...)
}

func (p *Provider) BindHostService(context.Context, publicruntime.BindHostServiceRequest) (*publicruntime.HostServiceBinding, error) {
	return nil, fmt.Errorf("modal runtime does not support host service tunnels")
}

func (p *Provider) StartPlugin(ctx context.Context, req publicruntime.StartPluginRequest) (*publicruntime.HostedPlugin, error) {
	if p == nil {
		return nil, fmt.Errorf("modal plugin runtime is not configured")
	}
	if strings.TrimSpace(req.Command) == "" {
		return nil, fmt.Errorf("plugin command is required")
	}
	if req.DefaultAction == publicruntime.PolicyDeny || len(req.AllowedHosts) > 0 {
		return nil, fmt.Errorf("modal runtime does not support hostname-based egress controls")
	}

	p.mu.Lock()
	session, err := p.sessionLocked(req.SessionID)
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}
	if session.plugin != nil {
		p.mu.Unlock()
		return nil, fmt.Errorf("plugin runtime session %q already has a running plugin", req.SessionID)
	}
	sandbox := session.sandbox
	tunnel := session.tunnel
	p.mu.Unlock()

	if sandbox == nil || tunnel == nil {
		return nil, fmt.Errorf("plugin runtime session %q is not ready", req.SessionID)
	}
	if req.BundleDir != "" {
		if err := uploadBundleDir(ctx, sandbox, req.BundleDir, publicruntime.HostedPluginBundleRoot); err != nil {
			return nil, fmt.Errorf("upload plugin bundle: %w", err)
		}
		if strings.HasPrefix(req.Command, publicruntime.HostedPluginBundleRoot+"/") || req.Command == publicruntime.HostedPluginBundleRoot {
			if err := runSandboxCommand(ctx, sandbox, []string{"chmod", "0755", req.Command}); err != nil {
				return nil, fmt.Errorf("mark plugin entrypoint executable: %w", err)
			}
		}
	}

	env := cloneStringMap(req.Env)
	if env == nil {
		env = map[string]string{}
	}
	env[proto.EnvProviderSocket] = fmt.Sprintf("tcp://0.0.0.0:%d", pluginGRPCPort)

	process, err := sandbox.Exec(ctx, append([]string{req.Command}, req.Args...), &modalclient.SandboxExecParams{
		Stdout: modalclient.Ignore,
		Stderr: modalclient.Ignore,
		Env:    env,
		Workdir: func() string {
			if req.BundleDir != "" {
				return publicruntime.HostedPluginBundleRoot
			}
			return ""
		}(),
	})
	if err != nil {
		return nil, fmt.Errorf("start plugin process in modal sandbox: %w", err)
	}

	host, port := tunnel.TLSSocket()
	if err := waitForPluginReady(ctx, host, port); err != nil {
		return nil, fmt.Errorf("wait for modal plugin gRPC endpoint: %w", err)
	}

	plugin := &plugin{
		id:      p.newID("plugin"),
		name:    req.PluginName,
		host:    host,
		port:    port,
		process: process,
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	session, err = p.sessionLocked(req.SessionID)
	if err != nil {
		return nil, err
	}
	session.plugin = plugin
	session.cleanup = req.Cleanup
	session.state = publicruntime.SessionStateRunning
	go p.watchPluginProcess(req.SessionID, process)

	return &publicruntime.HostedPlugin{
		ID:         plugin.id,
		SessionID:  session.id,
		PluginName: plugin.name,
	}, nil
}

func (p *Provider) DialPlugin(ctx context.Context, req publicruntime.DialPluginRequest) (publicruntime.HostedPluginConn, error) {
	if p == nil {
		return nil, fmt.Errorf("modal plugin runtime is not configured")
	}

	p.mu.Lock()
	session, err := p.sessionLocked(req.SessionID)
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}
	if session.plugin == nil {
		p.mu.Unlock()
		return nil, fmt.Errorf("plugin runtime session %q has no started plugin", req.SessionID)
	}
	host := session.plugin.host
	port := session.plugin.port
	p.mu.Unlock()

	conn, err := dialPlugin(ctx, host, port)
	if err != nil {
		return nil, err
	}
	return &hostedPluginConn{
		conn:      conn,
		lifecycle: proto.NewProviderLifecycleClient(conn),
		plugin:    proto.NewIntegrationProviderClient(conn),
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
	p.mu.Unlock()

	var errs []error
	for _, id := range sessionIDs {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		errs = append(errs, p.StopSession(stopCtx, publicruntime.StopSessionRequest{SessionID: id}))
		cancel()
	}
	if p.client != nil {
		p.client.Close()
	}
	return errors.Join(errs...)
}

func (c *hostedPluginConn) Lifecycle() proto.ProviderLifecycleClient {
	if c == nil {
		return nil
	}
	return c.lifecycle
}

func (c *hostedPluginConn) Integration() proto.IntegrationProviderClient {
	if c == nil {
		return nil
	}
	return c.plugin
}

func (c *hostedPluginConn) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
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
		session.state = publicruntime.SessionStateFailed
		return
	}
	if code == 0 {
		session.state = publicruntime.SessionStateStopped
		return
	}
	session.state = publicruntime.SessionStateFailed
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

func cloneSession(session *session) *publicruntime.Session {
	if session == nil {
		return nil
	}
	return &publicruntime.Session{
		ID:       session.id,
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
		conn, err := dialPlugin(deadlineCtx, host, port)
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

func dialPlugin(_ context.Context, host string, port int) (*grpc.ClientConn, error) {
	if strings.TrimSpace(host) == "" || port <= 0 {
		return nil, fmt.Errorf("modal plugin tunnel is not configured")
	}
	address := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: host,
		NextProtos: []string{"h2"},
	}
	conn, err := grpc.NewClient(
		"dns:///"+address,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
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

func slicesOrNil[T any](in []T) []T {
	if len(in) == 0 {
		return nil
	}
	return append([]T(nil), in...)
}
