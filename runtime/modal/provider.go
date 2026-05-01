package modal

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	modalclient "github.com/modal-labs/modal-client/go"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"gopkg.in/yaml.v3"
)

const (
	providerVersion     = "0.0.1-alpha.14"
	pluginGRPCPort      = 50051
	tunnelLookupTimeout = 30 * time.Second
	dialTimeout         = 15 * time.Second
	launchDrainTimeout  = 3 * time.Second
	defaultSandboxTTL   = 5 * time.Minute
	drainBeforeExpiry   = 30 * time.Second
	sessionStateReady   = "ready"
	sessionStateRunning = "running"
	sessionStateStopped = "stopped"
	sessionStateFailed  = "failed"

	registryUsernameEnv = "REGISTRY_USERNAME"
	registryPasswordEnv = "REGISTRY_PASSWORD"

	modalSandboxTagSchemaVersion = "gestalt_schema_version"
	modalSandboxTagVersion       = "1"
	modalSandboxTagSessionID     = "gestalt_session_id"
	modalSandboxTagRuntime       = "gestalt_runtime_provider"
	modalSandboxTagProviderName  = "gestalt_provider_name"
	modalSandboxTagProviderKind  = "gestalt_provider_kind"
)

const (
	envProviderSocket                  = "GESTALT_PLUGIN_SOCKET"
	providerLifecycleGetIdentityMethod = "/gestalt.provider.v1.ProviderLifecycle/GetProviderIdentity"
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
	name   string
	client *modalclient.Client
	cfg    Config

	instanceID string
	nextID     uint64

	mu       sync.Mutex
	sessions map[string]*session
	closed   bool
}

type session struct {
	id                       string
	state                    string
	metadata                 map[string]string
	bindings                 map[string]string
	image                    string
	imageRegistryCredentials *imageRegistryCredentials
	startedAt                time.Time
	recommendedDrainAt       *time.Time
	expiresAt                *time.Time
	stateReason              string
	stateMessage             string
	sandbox                  *modalclient.Sandbox
	tunnel                   *modalclient.Tunnel
	plugin                   *plugin
	logSeq                   uint64
	restored                 bool
}

type plugin struct {
	id      string
	name    string
	process *modalclient.ContainerProcess
}

type imageRegistryCredentials struct {
	username string
	password string
}

func New() *Provider {
	return &Provider{
		instanceID: newInstanceID(),
		sessions:   make(map[string]*session),
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

func (p *Provider) GetSupport(context.Context) (gestalt.PluginRuntimeSupport, error) {
	return gestalt.PluginRuntimeSupport{
		CanHostPlugins: true,
		EgressMode:     gestalt.PluginRuntimeEgressModeHostname,
	}, nil
}

func (p *Provider) StartSession(_ context.Context, req gestalt.StartPluginRuntimeSessionRequest) (gestalt.PluginRuntimeSession, error) {
	_, _, err := p.configured()
	if err != nil {
		return gestalt.PluginRuntimeSession{}, status.Error(codes.FailedPrecondition, err.Error())
	}
	if strings.TrimSpace(req.Image) == "" {
		return gestalt.PluginRuntimeSession{}, status.Errorf(codes.InvalidArgument, "plugins.%s.runtime.image is required when using the modal runtime", req.PluginName)
	}
	imageRegistryCredentials, err := pluginRuntimeImagePullAuth(req.Image, req.ImagePullAuth)
	if err != nil {
		return gestalt.PluginRuntimeSession{}, status.Error(codes.InvalidArgument, err.Error())
	}
	sessionID := p.newID("session")

	session := &session{
		id:                       sessionID,
		state:                    sessionStateReady,
		metadata:                 cloneStringMap(req.Metadata),
		image:                    strings.TrimSpace(req.Image),
		imageRegistryCredentials: imageRegistryCredentials,
	}
	if session.metadata == nil {
		session.metadata = map[string]string{}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return gestalt.PluginRuntimeSession{}, status.Error(codes.FailedPrecondition, "modal runtime is closed")
	}
	p.sessions[sessionID] = session
	return cloneSession(session), nil
}

func pluginRuntimeImagePullAuth(image string, auth *gestalt.PluginRuntimeImagePullAuth) (*imageRegistryCredentials, error) {
	if auth == nil {
		return nil, nil
	}
	dockerConfigJSON := strings.TrimSpace(auth.DockerConfigJSON)
	if dockerConfigJSON == "" {
		return nil, fmt.Errorf("image_pull_auth.docker_config_json is required when image_pull_auth is set")
	}
	return registryCredentialsFromDockerConfig(image, dockerConfigJSON)
}

type dockerConfigFile struct {
	Auths map[string]dockerAuthConfig `json:"auths"`
}

type dockerAuthConfig struct {
	Username      string `json:"username"`
	Password      string `json:"password"`
	Auth          string `json:"auth"`
	IdentityToken string `json:"identitytoken"`
}

func registryCredentialsFromDockerConfig(image, dockerConfigJSON string) (*imageRegistryCredentials, error) {
	var cfg dockerConfigFile
	if err := json.Unmarshal([]byte(dockerConfigJSON), &cfg); err != nil {
		return nil, fmt.Errorf("image_pull_auth.docker_config_json must be valid Docker config JSON: %w", err)
	}
	if len(cfg.Auths) == 0 {
		return nil, fmt.Errorf(`image_pull_auth.docker_config_json must contain a non-empty "auths" object`)
	}
	registryHost := imageRegistryHost(image)
	auth, ok := dockerConfigAuthForRegistry(cfg.Auths, registryHost)
	if !ok {
		return nil, fmt.Errorf("image_pull_auth.docker_config_json does not contain credentials for registry %q", registryHost)
	}
	username, password, err := staticRegistryCredentials(auth)
	if err != nil {
		return nil, fmt.Errorf("image_pull_auth.docker_config_json credentials for registry %q: %w", registryHost, err)
	}
	return &imageRegistryCredentials{username: username, password: password}, nil
}

func dockerConfigAuthForRegistry(auths map[string]dockerAuthConfig, registryHost string) (dockerAuthConfig, bool) {
	normalizedRegistry := normalizeDockerConfigRegistry(registryHost)
	for key, auth := range auths {
		if normalizeDockerConfigRegistry(key) == normalizedRegistry {
			return auth, true
		}
	}
	return dockerAuthConfig{}, false
}

func imageRegistryHost(image string) string {
	image = strings.TrimSpace(image)
	if image == "" {
		return ""
	}
	first, _, hasSlash := strings.Cut(image, "/")
	if hasSlash && (strings.ContainsAny(first, ".:") || first == "localhost") {
		return strings.ToLower(first)
	}
	return "docker.io"
}

func normalizeDockerConfigRegistry(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if parsed, err := url.Parse(value); err == nil && parsed.Host != "" {
		value = parsed.Host + parsed.Path
	}
	value = strings.Trim(value, "/")
	value = strings.ToLower(value)
	switch value {
	case "https://index.docker.io/v1", "http://index.docker.io/v1", "index.docker.io/v1", "index.docker.io", "registry-1.docker.io":
		return "docker.io"
	}
	host, _, _ := strings.Cut(value, "/")
	return host
}

func staticRegistryCredentials(auth dockerAuthConfig) (string, string, error) {
	username := strings.TrimSpace(auth.Username)
	password := auth.Password
	if username == "" && strings.TrimSpace(auth.Auth) != "" {
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(auth.Auth))
		if err != nil {
			return "", "", fmt.Errorf("auth must be base64-encoded username:password: %w", err)
		}
		decodedUsername, decodedPassword, ok := strings.Cut(string(decoded), ":")
		if !ok {
			return "", "", fmt.Errorf("auth must decode to username:password")
		}
		username = strings.TrimSpace(decodedUsername)
		password = decodedPassword
	}
	if username == "" || strings.TrimSpace(password) == "" {
		if strings.TrimSpace(auth.IdentityToken) != "" {
			return "", "", fmt.Errorf("identitytoken-only Docker auth is not supported by the Modal static registry credential path")
		}
		return "", "", fmt.Errorf("username/password or auth is required")
	}
	return username, password, nil
}

func (p *Provider) GetSession(ctx context.Context, sessionID string) (gestalt.PluginRuntimeSession, error) {
	sessionID = strings.TrimSpace(sessionID)
	p.mu.Lock()
	session, err := p.sessionLocked(sessionID)
	if err == nil && session.restored {
		p.mu.Unlock()
		return p.restoreSession(ctx, sessionID)
	}
	if err != nil {
		p.mu.Unlock()
		return p.restoreSession(ctx, sessionID)
	}
	sandbox := session.sandbox
	cloned := cloneSession(session)
	p.mu.Unlock()

	if sandbox == nil {
		return cloned, nil
	}
	if code, pollErr := sandbox.Poll(ctx); pollErr == nil && code != nil {
		p.mu.Lock()
		session, err = p.sessionLocked(sessionID)
		if err != nil {
			p.mu.Unlock()
			return gestalt.PluginRuntimeSession{}, status.Error(codes.NotFound, err.Error())
		}
		if session.state != sessionStateStopped && session.state != sessionStateFailed {
			session.state = sessionStateStopped
			session.stateReason = "exited"
			session.stateMessage = fmt.Sprintf("modal sandbox process exited with status %d", *code)
		}
		cloned = cloneSession(session)
		p.mu.Unlock()
	}
	return cloned, nil
}

type modalSessionMatch struct {
	sandbox *modalclient.Sandbox
	tags    map[string]string
}

func (p *Provider) restoreSession(ctx context.Context, sessionID string) (gestalt.PluginRuntimeSession, error) {
	matches, err := p.findModalSessionSandboxes(ctx, sessionID)
	if err != nil {
		return gestalt.PluginRuntimeSession{}, err
	}
	if len(matches) == 0 {
		p.forgetRestoredSession(sessionID)
		return gestalt.PluginRuntimeSession{}, status.Errorf(codes.NotFound, "plugin runtime session %q not found", strings.TrimSpace(sessionID))
	}
	if len(matches) > 1 {
		return gestalt.PluginRuntimeSession{}, status.Errorf(codes.FailedPrecondition, "multiple active modal sandboxes found for plugin runtime session %q", strings.TrimSpace(sessionID))
	}
	match := matches[0]
	code, err := match.sandbox.Poll(ctx)
	if err != nil {
		if isModalNotFound(err) {
			p.forgetRestoredSession(sessionID)
			return gestalt.PluginRuntimeSession{}, status.Errorf(codes.NotFound, "plugin runtime session %q not found", strings.TrimSpace(sessionID))
		}
		return gestalt.PluginRuntimeSession{}, status.Errorf(codes.Unavailable, "poll restored modal sandbox: %v", err)
	}
	restored := restoredSessionFromModalSandbox(sessionID, match.sandbox, match.tags, code)

	p.mu.Lock()
	if p.sessions == nil {
		p.sessions = make(map[string]*session)
	}
	p.sessions[restored.id] = restored
	p.mu.Unlock()

	return cloneSession(restored), nil
}

func (p *Provider) terminateRestoredSession(ctx context.Context, sessionID string) error {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}
	matches, err := p.findModalSessionSandboxes(ctx, sessionID)
	if err != nil {
		return err
	}
	var errs []error
	for _, match := range matches {
		if match.sandbox == nil {
			continue
		}
		if _, err := match.sandbox.Terminate(ctx, nil); err != nil {
			if isModalNotFound(err) {
				continue
			}
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return status.Errorf(codes.Internal, "terminate modal sandbox for session %q: %v", sessionID, err)
	}
	return nil
}

func (p *Provider) findModalSessionSandboxes(ctx context.Context, sessionID string) ([]modalSessionMatch, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil, status.Error(codes.NotFound, "plugin runtime session id is required")
	}
	client, cfg, err := p.configured()
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	runtimeProvider := p.runtimeProviderName()
	app, err := client.Apps.FromName(ctx, cfg.App, &modalclient.AppFromNameParams{
		Environment:     cfg.Environment,
		CreateIfMissing: false,
	})
	if err != nil {
		if isModalNotFound(err) {
			return nil, nil
		}
		return nil, status.Errorf(codes.Internal, "lookup modal app %q: %v", cfg.App, err)
	}

	lookupTags := modalSessionLookupTags(sessionID, runtimeProvider)
	seq, err := client.Sandboxes.List(ctx, &modalclient.SandboxListParams{
		AppID:       app.AppID,
		Environment: cfg.Environment,
		Tags:        lookupTags,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list modal sandboxes for session %q: %v", sessionID, err)
	}

	var matches []modalSessionMatch
	for sandbox, iterErr := range seq {
		if iterErr != nil {
			return nil, status.Errorf(codes.Internal, "list modal sandboxes for session %q: %v", sessionID, iterErr)
		}
		if sandbox == nil {
			continue
		}
		tags, err := sandbox.GetTags(ctx)
		if err != nil {
			if isModalNotFound(err) {
				continue
			}
			return nil, status.Errorf(codes.Internal, "get modal sandbox tags for session %q: %v", sessionID, err)
		}
		if !modalSessionTagsMatch(tags, lookupTags) {
			continue
		}
		matches = append(matches, modalSessionMatch{
			sandbox: sandbox,
			tags:    tags,
		})
	}
	return matches, nil
}

func (p *Provider) forgetRestoredSession(sessionID string) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if session := p.sessions[sessionID]; session != nil && session.restored {
		delete(p.sessions, sessionID)
	}
}

func (p *Provider) ListSessions(ctx context.Context) ([]gestalt.PluginRuntimeSession, error) {
	p.mu.Lock()
	sessionIDs := make([]string, 0, len(p.sessions))
	for sessionID := range p.sessions {
		sessionIDs = append(sessionIDs, sessionID)
	}
	p.mu.Unlock()
	sort.Strings(sessionIDs)

	sessions := make([]gestalt.PluginRuntimeSession, 0, len(sessionIDs))
	for _, sessionID := range sessionIDs {
		session, err := p.GetSession(ctx, sessionID)
		if status.Code(err) == codes.NotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		sessions = append(sessions, session)
	}
	return sessions, nil
}

func (p *Provider) StopSession(ctx context.Context, sessionID string) error {
	var sandbox *modalclient.Sandbox
	var restored bool
	var found bool

	sessionID = strings.TrimSpace(sessionID)
	p.mu.Lock()
	if session, ok := p.sessions[sessionID]; ok {
		found = true
		delete(p.sessions, sessionID)
		session.state = sessionStateStopped
		sandbox = session.sandbox
		restored = session.restored
	}
	p.mu.Unlock()

	if sandbox != nil && !restored {
		if _, err := sandbox.Terminate(ctx, nil); err != nil {
			return status.Errorf(codes.Internal, "terminate modal sandbox: %v", err)
		}
		return nil
	}
	if found && !restored {
		return nil
	}
	if restored || !found {
		if err := p.terminateRestoredSession(ctx, sessionID); err != nil {
			return err
		}
	}
	return nil
}

func (p *Provider) StartPlugin(ctx context.Context, req gestalt.StartHostedPluginRequest) (gestalt.HostedPlugin, error) {
	if strings.TrimSpace(req.Command) == "" {
		return gestalt.HostedPlugin{}, status.Error(codes.InvalidArgument, "plugin command is required")
	}
	client, cfg, err := p.configured()
	if err != nil {
		return gestalt.HostedPlugin{}, status.Error(codes.FailedPrecondition, err.Error())
	}

	p.mu.Lock()
	session, err := p.sessionLocked(req.SessionID)
	if err != nil {
		p.mu.Unlock()
		return gestalt.HostedPlugin{}, status.Error(codes.NotFound, err.Error())
	}
	if session.plugin != nil {
		p.mu.Unlock()
		return gestalt.HostedPlugin{}, status.Errorf(codes.FailedPrecondition, "plugin runtime session %q already has a running plugin", req.SessionID)
	}
	if session.restored {
		p.mu.Unlock()
		return gestalt.HostedPlugin{}, status.Errorf(codes.FailedPrecondition, "plugin runtime session %q was restored from Modal and cannot launch a plugin", req.SessionID)
	}
	logs := newSessionLogSink(session.id, &session.logSeq, nil)
	p.mu.Unlock()
	logRuntimePhase(logs, "starting plugin %q", req.PluginName)

	sandbox, tunnel, err := p.ensureSessionSandbox(ctx, client, cfg, req, logs)
	if err != nil {
		logs.add(gestalt.RuntimeLogStreamRuntime, err.Error(), time.Now())
		return gestalt.HostedPlugin{}, err
	}
	launchOK := false
	defer func() {
		if launchOK {
			return
		}
		p.resetSessionSandbox(req.SessionID, sandbox)
		_, _ = sandbox.Terminate(context.Background(), nil)
	}()

	command := req.Command

	env := buildPluginEnv(req, fmt.Sprintf("tcp://0.0.0.0:%d", pluginGRPCPort))

	execArgv := append([]string{command}, req.Args...)
	startedAt := time.Now()
	logRuntimePhase(logs, "plugin exec: starting command=%q args=%d", command, len(req.Args))
	process, err := sandbox.Exec(ctx, execArgv, &modalclient.SandboxExecParams{
		Stdout: modalclient.Pipe,
		Stderr: modalclient.Pipe,
		Env:    env,
	})
	if err != nil {
		logRuntimePhase(logs, "plugin exec: failed after %s: %v", elapsed(startedAt), err)
		logs.add(gestalt.RuntimeLogStreamRuntime, "start plugin process in modal sandbox: "+err.Error(), time.Now())
		return gestalt.HostedPlugin{}, status.Errorf(codes.Internal, "start plugin process in modal sandbox: %v", err)
	}
	logRuntimePhase(logs, "plugin exec: process started in %s", elapsed(startedAt))
	stdoutDone := logs.stream(process.Stdout, gestalt.RuntimeLogStreamStdout)
	stderrDone := logs.stream(process.Stderr, gestalt.RuntimeLogStreamStderr)
	processDone := p.watchPluginProcess(req.SessionID, logs, process)

	host, port := tunnel.TLSSocket()
	startedAt = time.Now()
	logRuntimePhase(logs, "plugin gRPC readiness: waiting target=%s", net.JoinHostPort(host, fmt.Sprintf("%d", port)))
	if err := waitForPluginReady(ctx, host, port); err != nil {
		logRuntimePhase(logs, "plugin gRPC readiness: failed after %s: %v", elapsed(startedAt), err)
		logs.add(gestalt.RuntimeLogStreamRuntime, "wait for modal plugin gRPC endpoint: "+err.Error(), time.Now())
		p.markSessionLaunchFailed(req.SessionID, "readiness_failed", err.Error())
		p.resetSessionSandbox(req.SessionID, sandbox)
		_, _ = sandbox.Terminate(context.Background(), nil)
		waitForLaunchDrain(processDone, stdoutDone, stderrDone, launchDrainTimeout)
		launchOK = true
		return gestalt.HostedPlugin{}, status.Errorf(codes.DeadlineExceeded, "wait for modal plugin gRPC endpoint: %v", err)
	}
	logRuntimePhase(logs, "plugin gRPC readiness: ready in %s", elapsed(startedAt))

	plugin := &plugin{
		id:      p.newID("plugin"),
		name:    req.PluginName,
		process: process,
	}

	p.mu.Lock()
	session, err = p.sessionLocked(req.SessionID)
	if err != nil {
		p.mu.Unlock()
		return gestalt.HostedPlugin{}, status.Error(codes.NotFound, err.Error())
	}
	session.plugin = plugin
	session.state = sessionStateRunning
	session.stateReason = ""
	session.stateMessage = ""
	p.mu.Unlock()
	logs.add(gestalt.RuntimeLogStreamRuntime, fmt.Sprintf("plugin %q became ready", req.PluginName), time.Now())
	launchOK = true

	return gestalt.HostedPlugin{
		ID:         plugin.id,
		SessionID:  session.id,
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
		if p.sessions[id] != nil && p.sessions[id].restored {
			continue
		}
		sessionIDs = append(sessionIDs, id)
	}
	client := p.client
	p.mu.Unlock()

	var errs []error
	for _, id := range sessionIDs {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := p.StopSession(stopCtx, id)
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

func (p *Provider) ensureSessionSandbox(ctx context.Context, client *modalclient.Client, cfg Config, req gestalt.StartHostedPluginRequest, logs *sessionLogSink) (*modalclient.Sandbox, *modalclient.Tunnel, error) {
	p.mu.Lock()
	session, err := p.sessionLocked(req.SessionID)
	if err != nil {
		p.mu.Unlock()
		return nil, nil, status.Error(codes.NotFound, err.Error())
	}
	if session.sandbox != nil && session.tunnel != nil {
		sandbox := session.sandbox
		tunnel := session.tunnel
		p.mu.Unlock()
		logRuntimePhase(logs, "modal sandbox: reusing existing sandbox for session %q", req.SessionID)
		return sandbox, tunnel, nil
	}
	imageRef := strings.TrimSpace(session.image)
	sessionID := session.id
	metadata := cloneStringMap(session.metadata)
	imageRegistryCredentials := cloneImageRegistryCredentials(session.imageRegistryCredentials)
	p.mu.Unlock()

	if imageRef == "" {
		return nil, nil, status.Errorf(codes.FailedPrecondition, "plugin runtime session %q is missing a runtime image", req.SessionID)
	}
	startedAt := time.Now()
	logRuntimePhase(logs, "modal sandbox egress: configuring")
	createParams, err := buildSandboxCreateParams(ctx, cfg, req, sessionID)
	if err != nil {
		logRuntimePhase(logs, "modal sandbox egress: failed after %s: %v", elapsed(startedAt), err)
		return nil, nil, status.Errorf(codes.FailedPrecondition, "configure modal sandbox egress: %v", err)
	}
	logRuntimePhase(logs, "modal sandbox egress: configured in %s", elapsed(startedAt))

	startedAt = time.Now()
	logRuntimePhase(logs, "modal app lookup: starting app=%q environment=%q", cfg.App, cfg.Environment)
	app, err := client.Apps.FromName(ctx, cfg.App, &modalclient.AppFromNameParams{
		Environment:     cfg.Environment,
		CreateIfMissing: true,
	})
	if err != nil {
		logRuntimePhase(logs, "modal app lookup: failed after %s: %v", elapsed(startedAt), err)
		return nil, nil, status.Errorf(codes.Internal, "lookup modal app %q: %v", cfg.App, err)
	}
	logRuntimePhase(logs, "modal app lookup: completed in %s", elapsed(startedAt))

	startedAt = time.Now()
	logRuntimePhase(logs, "modal sandbox create: starting image=%q timeout=%s idle_timeout=%s", imageRef, configuredDuration(cfg.Timeout), configuredDuration(cfg.IdleTimeout))
	sandboxStartedAt := startedAt.UTC()
	imageParams, err := imageFromRegistryParams(ctx, client, cfg, imageRegistryCredentials, logs)
	if err != nil {
		logRuntimePhase(logs, "modal sandbox create: failed after %s: %v", elapsed(startedAt), err)
		return nil, nil, err
	}
	sandbox, err := client.Sandboxes.Create(ctx, app, client.Images.FromRegistry(imageRef, imageParams), createParams)
	if err != nil {
		logRuntimePhase(logs, "modal sandbox create: failed after %s: %v", elapsed(startedAt), err)
		return nil, nil, status.Errorf(codes.Internal, "create modal sandbox: %v", err)
	}
	logRuntimePhase(logs, "modal sandbox create: completed in %s", elapsed(startedAt))
	tags := modalSessionTags(sessionID, p.runtimeProviderName(), metadata)
	if err := sandbox.SetTags(ctx, tags); err != nil {
		logRuntimePhase(logs, "modal sandbox tag: failed after %s: %v", elapsed(startedAt), err)
		_, _ = sandbox.Terminate(context.Background(), nil)
		return nil, nil, status.Errorf(codes.Internal, "tag modal sandbox: %v", err)
	}
	logRuntimePhase(logs, "modal sandbox tag: completed")

	startedAt = time.Now()
	logRuntimePhase(logs, "modal sandbox tunnel lookup: starting port=%d", pluginGRPCPort)
	tunnels, err := sandbox.Tunnels(ctx, tunnelLookupTimeout)
	if err != nil {
		logRuntimePhase(logs, "modal sandbox tunnel lookup: failed after %s: %v", elapsed(startedAt), err)
		_, _ = sandbox.Terminate(context.Background(), nil)
		return nil, nil, status.Errorf(codes.Internal, "lookup modal sandbox tunnel: %v", err)
	}
	tunnel, ok := tunnels[pluginGRPCPort]
	if !ok || tunnel == nil {
		logRuntimePhase(logs, "modal sandbox tunnel lookup: port %d unavailable after %s", pluginGRPCPort, elapsed(startedAt))
		_, _ = sandbox.Terminate(context.Background(), nil)
		return nil, nil, status.Errorf(codes.Internal, "modal sandbox tunnel for port %d is unavailable", pluginGRPCPort)
	}
	host, port := tunnel.TLSSocket()
	logRuntimePhase(logs, "modal sandbox tunnel lookup: completed in %s target=%s", elapsed(startedAt), net.JoinHostPort(host, fmt.Sprintf("%d", port)))

	p.mu.Lock()
	session, err = p.sessionLocked(req.SessionID)
	if err != nil {
		p.mu.Unlock()
		_, _ = sandbox.Terminate(context.Background(), nil)
		return nil, nil, status.Error(codes.NotFound, err.Error())
	}
	if session.sandbox != nil && session.tunnel != nil {
		existingSandbox := session.sandbox
		existingTunnel := session.tunnel
		p.mu.Unlock()
		_, _ = sandbox.Terminate(context.Background(), nil)
		logRuntimePhase(logs, "modal sandbox: another sandbox was already registered for session %q", req.SessionID)
		return existingSandbox, existingTunnel, nil
	}
	session.sandbox = sandbox
	session.tunnel = tunnel
	session.state = sessionStateReady
	session.stateReason = ""
	session.stateMessage = ""
	session.startedAt = sandboxStartedAt
	session.expiresAt, session.recommendedDrainAt = modalSessionLifecycleDeadlines(session.startedAt, cfg)
	p.mu.Unlock()
	logRuntimePhase(logs, "modal sandbox: registered for session %q", req.SessionID)
	return sandbox, tunnel, nil
}

func imageFromRegistryParams(ctx context.Context, client *modalclient.Client, cfg Config, creds *imageRegistryCredentials, logs *sessionLogSink) (*modalclient.ImageFromRegistryParams, error) {
	if creds == nil {
		return nil, nil
	}
	startedAt := time.Now()
	logRuntimePhase(logs, "modal image registry credentials: creating ephemeral secret")
	secret, err := client.Secrets.FromMap(ctx, map[string]string{
		registryUsernameEnv: creds.username,
		registryPasswordEnv: creds.password,
	}, &modalclient.SecretFromMapParams{
		Environment: cfg.Environment,
	})
	if err != nil {
		logRuntimePhase(logs, "modal image registry credentials: failed after %s: %v", elapsed(startedAt), err)
		return nil, status.Errorf(codes.Internal, "create modal image registry secret: %v", err)
	}
	logRuntimePhase(logs, "modal image registry credentials: secret ready in %s", elapsed(startedAt))
	return &modalclient.ImageFromRegistryParams{Secret: secret}, nil
}

func buildSandboxCreateParams(ctx context.Context, cfg Config, req gestalt.StartHostedPluginRequest, sessionID string) (*modalclient.SandboxCreateParams, error) {
	params := &modalclient.SandboxCreateParams{
		CPU:            cfg.CPU,
		MemoryMiB:      cfg.MemoryMiB,
		MemoryLimitMiB: cfg.MemoryLimitMiB,
		Timeout:        cfg.Timeout,
		IdleTimeout:    cfg.IdleTimeout,
		Cloud:          cfg.Cloud,
		Regions:        slicesOrNil(cfg.Regions),
		H2Ports:        []int{pluginGRPCPort},
		Name:           sandboxName(req.PluginName, sessionID),
	}
	if !requiresHostnameProxy(req, req.Env) {
		return params, nil
	}
	cidrs, err := egressProxyCIDRAllowlist(ctx, req.Env)
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

func requiresHostnameProxy(req gestalt.StartHostedPluginRequest, env map[string]string) bool {
	if strings.EqualFold(strings.TrimSpace(req.DefaultAction), "deny") {
		return true
	}
	for _, proxyEnv := range []string{"HTTPS_PROXY", "HTTP_PROXY"} {
		if strings.TrimSpace(env[proxyEnv]) != "" {
			return true
		}
	}
	relayHosts := relayHostnameSetFromEnv(env)
	for _, host := range req.AllowedHosts {
		if _, ok := relayHosts[normalizeHostname(host)]; !ok {
			return true
		}
	}
	return false
}

func relayHostnameSetFromEnv(env map[string]string) map[string]struct{} {
	hosts := make(map[string]struct{}, len(env))
	for key, dialTarget := range env {
		if !isHostServiceSocketEnv(key) {
			continue
		}
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

func isHostServiceSocketEnv(key string) bool {
	return strings.HasSuffix(strings.TrimSpace(key), "_SOCKET")
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
	session.startedAt = time.Time{}
	session.recommendedDrainAt = nil
	session.expiresAt = nil
	if session.plugin == nil && session.state != sessionStateFailed {
		session.state = sessionStateReady
	}
}

func (p *Provider) markSessionLaunchFailed(sessionID, reason, message string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	session, ok := p.sessions[strings.TrimSpace(sessionID)]
	if !ok || session == nil {
		return
	}
	session.state = sessionStateFailed
	session.stateReason = strings.TrimSpace(reason)
	session.stateMessage = strings.TrimSpace(message)
}

func (p *Provider) watchPluginProcess(sessionID string, logs *sessionLogSink, process *modalclient.ContainerProcess) <-chan struct{} {
	done := make(chan struct{})
	if process == nil {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		code, err := process.Wait(context.Background())
		p.mu.Lock()
		session, ok := p.sessions[sessionID]
		if !ok || session == nil {
			p.mu.Unlock()
			return
		}
		message := ""
		stream := gestalt.RuntimeLogStreamRuntime
		if err != nil {
			session.state = sessionStateFailed
			session.stateReason = "wait_failed"
			message = "plugin process wait failed: " + err.Error()
			session.stateMessage = message
			p.mu.Unlock()
			logs.add(stream, message, time.Now())
			return
		}
		if code == 0 {
			message = "plugin process exited successfully"
			if session.state != sessionStateFailed {
				session.state = sessionStateStopped
				session.stateReason = "exited"
				session.stateMessage = message
			}
			p.mu.Unlock()
			logs.add(stream, message, time.Now())
			return
		}
		session.state = sessionStateFailed
		session.stateReason = "exited"
		message = fmt.Sprintf("plugin process exited with status %d", code)
		session.stateMessage = message
		p.mu.Unlock()
		logs.add(stream, message, time.Now())
	}()
	return done
}

func waitForLaunchDrain(processDone, stdoutDone, stderrDone <-chan struct{}, timeout time.Duration) {
	if timeout <= 0 {
		timeout = launchDrainTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for _, done := range []<-chan struct{}{processDone, stdoutDone, stderrDone} {
		if done == nil {
			continue
		}
		select {
		case <-done:
		case <-ctx.Done():
			return
		}
	}
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
	instanceID := strings.TrimSpace(p.instanceID)
	if instanceID == "" {
		instanceID = newInstanceID()
		p.instanceID = instanceID
	}
	return fmt.Sprintf("%s-%s-%06d", prefix, instanceID, atomic.AddUint64(&p.nextID, 1))
}

func newInstanceID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err == nil {
		return fmt.Sprintf("%x", buf[:])
	}
	return fmt.Sprintf("%08x", time.Now().UnixNano())
}

func (p *Provider) runtimeProviderName() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	name := strings.TrimSpace(p.name)
	if name == "" {
		return "modal"
	}
	return name
}

func modalSessionTags(sessionID, runtimeProvider string, metadata map[string]string) map[string]string {
	tags := modalSessionLookupTags(sessionID, runtimeProvider)
	if providerName := strings.TrimSpace(metadata["provider_name"]); providerName != "" {
		tags[modalSandboxTagProviderName] = providerName
	}
	if providerKind := strings.TrimSpace(metadata["provider_kind"]); providerKind != "" {
		tags[modalSandboxTagProviderKind] = providerKind
	}
	return tags
}

func modalSessionLookupTags(sessionID, runtimeProvider string) map[string]string {
	runtimeProvider = strings.TrimSpace(runtimeProvider)
	if runtimeProvider == "" {
		runtimeProvider = "modal"
	}
	return map[string]string{
		modalSandboxTagSchemaVersion: modalSandboxTagVersion,
		modalSandboxTagSessionID:     strings.TrimSpace(sessionID),
		modalSandboxTagRuntime:       runtimeProvider,
	}
}

func modalSessionTagsMatch(tags, want map[string]string) bool {
	if len(want) == 0 {
		return false
	}
	for key, value := range want {
		if strings.TrimSpace(tags[key]) != strings.TrimSpace(value) {
			return false
		}
	}
	return true
}

func isModalNotFound(err error) bool {
	if err == nil {
		return false
	}
	var notFound modalclient.NotFoundError
	return errors.As(err, &notFound) || status.Code(err) == codes.NotFound
}

func restoredSessionFromModalSandbox(sessionID string, sandbox *modalclient.Sandbox, tags map[string]string, code *int) *session {
	metadata := map[string]string{}
	if providerName := strings.TrimSpace(tags[modalSandboxTagProviderName]); providerName != "" {
		metadata["provider_name"] = providerName
	}
	if providerKind := strings.TrimSpace(tags[modalSandboxTagProviderKind]); providerKind != "" {
		metadata["provider_kind"] = providerKind
	}
	state := sessionStateRunning
	reason := "restored"
	message := "active Modal sandbox found; plugin process handle is unavailable after restore"
	if code != nil {
		reason = "exited"
		message = fmt.Sprintf("modal sandbox process exited with status %d", *code)
		if *code == 0 {
			state = sessionStateStopped
		} else {
			state = sessionStateFailed
		}
	}
	return &session{
		id:           strings.TrimSpace(sessionID),
		state:        state,
		metadata:     metadata,
		stateReason:  reason,
		stateMessage: message,
		sandbox:      sandbox,
		restored:     true,
	}
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

func cloneSession(session *session) gestalt.PluginRuntimeSession {
	if session == nil {
		return gestalt.PluginRuntimeSession{}
	}
	return gestalt.PluginRuntimeSession{
		ID:           session.id,
		State:        session.state,
		Metadata:     cloneStringMap(session.metadata),
		Lifecycle:    cloneSessionLifecycle(session),
		StateReason:  session.stateReason,
		StateMessage: session.stateMessage,
	}
}

func cloneSessionLifecycle(session *session) gestalt.PluginRuntimeSessionLifecycle {
	if session == nil || (session.startedAt.IsZero() && session.recommendedDrainAt == nil && session.expiresAt == nil) {
		return gestalt.PluginRuntimeSessionLifecycle{}
	}
	lifecycle := gestalt.PluginRuntimeSessionLifecycle{}
	if !session.startedAt.IsZero() {
		startedAt := session.startedAt.UTC()
		lifecycle.StartedAt = &startedAt
	}
	if session.recommendedDrainAt != nil {
		recommendedDrainAt := session.recommendedDrainAt.UTC()
		lifecycle.RecommendedDrainAt = &recommendedDrainAt
	}
	if session.expiresAt != nil {
		expiresAt := session.expiresAt.UTC()
		lifecycle.ExpiresAt = &expiresAt
	}
	return lifecycle
}

func modalSessionLifecycleDeadlines(startedAt time.Time, cfg Config) (*time.Time, *time.Time) {
	ttl := cfg.Timeout
	if ttl <= 0 {
		ttl = defaultSandboxTTL
	}
	expiresAt := startedAt.UTC().Add(ttl)
	recommendedDrainAt := expiresAt.Add(-drainBeforeExpiry)
	if !recommendedDrainAt.After(startedAt) {
		recommendedDrainAt = startedAt.Add(ttl / 2)
	}
	return &expiresAt, &recommendedDrainAt
}

func logRuntimePhase(logs *sessionLogSink, format string, args ...any) {
	if logs == nil {
		return
	}
	logs.add(
		gestalt.RuntimeLogStreamRuntime,
		fmt.Sprintf(format, args...),
		time.Now(),
	)
}

func elapsed(startedAt time.Time) time.Duration {
	if startedAt.IsZero() {
		return 0
	}
	return time.Since(startedAt).Round(time.Millisecond)
}

func configuredDuration(value time.Duration) string {
	if value <= 0 {
		return "modal-default"
	}
	return value.String()
}

func runSandboxCommand(ctx context.Context, sandbox *modalclient.Sandbox, argv []string, logs *sessionLogSink) error {
	startedAt := time.Now()
	logRuntimePhase(logs, "modal sandbox command: starting argv=%q", strings.Join(argv, " "))
	process, err := sandbox.Exec(ctx, argv, &modalclient.SandboxExecParams{
		Stdout: modalclient.Ignore,
		Stderr: modalclient.Ignore,
	})
	if err != nil {
		logRuntimePhase(logs, "modal sandbox command: failed starting argv=%q after %s: %v", strings.Join(argv, " "), elapsed(startedAt), err)
		return err
	}
	code, err := process.Wait(ctx)
	if err != nil {
		logRuntimePhase(logs, "modal sandbox command: failed waiting argv=%q after %s: %v", strings.Join(argv, " "), elapsed(startedAt), err)
		return err
	}
	if code != 0 {
		err := fmt.Errorf("command %q exited with status %d", strings.Join(argv, " "), code)
		logRuntimePhase(logs, "modal sandbox command: failed argv=%q after %s: %v", strings.Join(argv, " "), elapsed(startedAt), err)
		return err
	}
	logRuntimePhase(logs, "modal sandbox command: completed argv=%q in %s", strings.Join(argv, " "), elapsed(startedAt))
	return nil
}

func waitForPluginReady(ctx context.Context, host string, port int) error {
	deadlineCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	for {
		conn, err := dialTLSPlugin(deadlineCtx, host, port)
		if err == nil {
			rpcErr := conn.Invoke(deadlineCtx, providerLifecycleGetIdentityMethod, &emptypb.Empty{}, &emptypb.Empty{})
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
	name := strings.ToLower(strings.TrimSpace(sessionID))
	if name == "" {
		name = strings.ToLower(strings.TrimSpace(pluginName))
	}
	name = sandboxNamePattern.ReplaceAllString(name, "-")
	name = strings.Trim(name, "-")
	if name == "" {
		name = "session"
	}
	value := "gestalt-" + name
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

func buildPluginEnv(req gestalt.StartHostedPluginRequest, providerSocket string) map[string]string {
	env := cloneStringMap(req.Env)
	if env == nil {
		env = map[string]string{}
	}
	env[envProviderSocket] = providerSocket
	return env
}

func cloneImageRegistryCredentials(src *imageRegistryCredentials) *imageRegistryCredentials {
	if src == nil {
		return nil
	}
	return &imageRegistryCredentials{
		username: src.username,
		password: src.password,
	}
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

func slicesOrNil[T any](in []T) []T {
	if len(in) == 0 {
		return nil
	}
	return append([]T(nil), in...)
}

var _ gestalt.PluginRuntimeProvider = (*Provider)(nil)
var _ gestalt.MetadataProvider = (*Provider)(nil)
var _ gestalt.HealthChecker = (*Provider)(nil)
var _ gestalt.Closer = (*Provider)(nil)
