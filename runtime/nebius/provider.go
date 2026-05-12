package nebius

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nebius/gosdk"
	reader "github.com/nebius/gosdk/config/reader"
	commonpb "github.com/nebius/gosdk/proto/nebius/common/v1"
	computepb "github.com/nebius/gosdk/proto/nebius/compute/v1"
	computesvc "github.com/nebius/gosdk/services/nebius/compute/v1"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"golang.org/x/crypto/ssh"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
)

const (
	providerVersion      = "0.0.1-alpha.1"
	pluginGRPCPort       = 50051
	sessionStateReady    = "ready"
	sessionStateStarting = "starting"
	sessionStateRunning  = "running"
	sessionStateStopped  = "stopped"
	sessionStateFailed   = "failed"

	tokenEnv = "NEBIUS_IAM_TOKEN"
	sshPort  = 22
)

const envProviderSocket = "GESTALT_PLUGIN_SOCKET"

var resourceNamePattern = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

type Provider struct {
	name string
	sdk  *gosdk.SDK
	cfg  Config

	nextID uint64

	mu       sync.Mutex
	sessions map[string]*session
	closed   bool
}

type session struct {
	id             string
	state          string
	metadata       map[string]string
	bindings       map[string]string
	image          string
	instance       *instanceRef
	pluginStarting bool
	plugin         *plugin
}

type instanceRef struct {
	id       string
	name     string
	publicIP string
	sshUser  string
	signer   ssh.Signer
	client   *ssh.Client
	forward  *localForwarder
}

type plugin struct {
	id            string
	name          string
	containerName string
	dialTarget    string
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
	sdk, err := newSDK(ctx, cfg)
	if err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		_ = sdk.Close()
		return fmt.Errorf("nebius runtime: provider is closed")
	}
	oldSDK := p.sdk
	p.name = strings.TrimSpace(name)
	p.cfg = cfg
	p.sdk = sdk
	if p.sessions == nil {
		p.sessions = make(map[string]*session)
	}
	if oldSDK != nil {
		_ = oldSDK.Close()
	}
	return nil
}

func newSDK(ctx context.Context, cfg Config) (*gosdk.SDK, error) {
	opts := []gosdk.Option{
		gosdk.WithUserAgentPrefix("gestalt-runtime-nebius/" + providerVersion),
	}
	if cfg.ProjectID != "" {
		opts = append(opts, gosdk.WithParentID(cfg.ProjectID))
	}
	if cfg.Endpoint != "" {
		opts = append(opts, gosdk.WithDomain(cfg.Endpoint))
	}
	if token := strings.TrimSpace(os.Getenv(tokenEnv)); token != "" && cfg.ProjectID != "" {
		opts = append(opts, gosdk.WithCredentials(gosdk.IAMToken(token)))
	} else {
		opts = append(opts, gosdk.WithConfigReader(reader.NewConfigReader()))
	}
	sdk, err := gosdk.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("nebius runtime: create sdk: %w", err)
	}
	return sdk, nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	name := p.name
	if strings.TrimSpace(name) == "" {
		name = "nebius"
	}
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindRuntime,
		Name:        name,
		DisplayName: "Nebius Runtime",
		Description: "Hosted executable-plugin runtime backed by Nebius Compute VMs.",
		Version:     providerVersion,
	}
}

func (p *Provider) HealthCheck(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sdk == nil {
		return fmt.Errorf("nebius runtime: not configured")
	}
	if p.cfg.SubnetID == "" {
		return fmt.Errorf("nebius runtime: subnetID is required")
	}
	return nil
}

func (p *Provider) GetSupport(context.Context) (gestalt.PluginRuntimeSupport, error) {
	return gestalt.PluginRuntimeSupport{
		CanHostPlugins: true,
		EgressMode:     gestalt.PluginRuntimeEgressModeNone,
	}, nil
}

func (p *Provider) StartSession(_ context.Context, req gestalt.StartPluginRuntimeSessionRequest) (gestalt.PluginRuntimeSession, error) {
	_, _, err := p.configured()
	if err != nil {
		return gestalt.PluginRuntimeSession{}, status.Error(codes.FailedPrecondition, err.Error())
	}
	if strings.TrimSpace(req.Image) == "" {
		return gestalt.PluginRuntimeSession{}, status.Errorf(codes.InvalidArgument, "plugins.%s.runtime.image is required when using the nebius runtime", req.PluginName)
	}

	sessionID := p.newID("session")
	s := &session{
		id:       sessionID,
		state:    sessionStateReady,
		metadata: cloneStringMap(req.Metadata),
		image:    strings.TrimSpace(req.Image),
	}
	if s.metadata == nil {
		s.metadata = map[string]string{}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return gestalt.PluginRuntimeSession{}, status.Error(codes.FailedPrecondition, "nebius runtime is closed")
	}
	p.sessions[sessionID] = s
	return cloneSession(s), nil
}

func (p *Provider) GetSession(ctx context.Context, sessionID string) (gestalt.PluginRuntimeSession, error) {
	sdk, _, err := p.configured()
	if err != nil {
		return gestalt.PluginRuntimeSession{}, status.Error(codes.FailedPrecondition, err.Error())
	}
	sessionID = strings.TrimSpace(sessionID)

	p.mu.Lock()
	s, err := p.sessionLocked(sessionID)
	if err != nil {
		p.mu.Unlock()
		return gestalt.PluginRuntimeSession{}, status.Error(codes.NotFound, err.Error())
	}
	instanceID := ""
	if s.instance != nil {
		instanceID = s.instance.id
	}
	current := cloneSession(s)
	p.mu.Unlock()

	if instanceID == "" {
		return current, nil
	}
	instanceService := sdk.Services().Compute().V1().Instance()
	instance, getErr := instanceService.Get(ctx, &computepb.GetInstanceRequest{Id: instanceID})
	if getErr != nil {
		p.mu.Lock()
		if s, ok := p.sessions[sessionID]; ok && s != nil {
			s.state = sessionStateFailed
			current = cloneSession(s)
		}
		p.mu.Unlock()
		return current, nil
	}

	publicIP := instancePublicIP(instance)
	p.mu.Lock()
	if s, ok := p.sessions[sessionID]; ok && s != nil {
		if s.instance != nil && publicIP != "" {
			s.instance.publicIP = publicIP
			if s.metadata == nil {
				s.metadata = map[string]string{}
			}
			s.metadata["compute.public_ip"] = publicIP
		}
		switch instance.GetStatus().GetState() {
		case computepb.InstanceStatus_ERROR:
			s.state = sessionStateFailed
		case computepb.InstanceStatus_STOPPED, computepb.InstanceStatus_DELETING:
			s.state = sessionStateStopped
		case computepb.InstanceStatus_RUNNING:
			if s.plugin == nil && s.state != sessionStateFailed && s.state != sessionStateStarting {
				s.state = sessionStateReady
			}
		}
		current = cloneSession(s)
	}
	p.mu.Unlock()
	return current, nil
}

func (p *Provider) ListSessions(ctx context.Context) ([]gestalt.PluginRuntimeSession, error) {
	p.mu.Lock()
	sessionIDs := make([]string, 0, len(p.sessions))
	for sessionID := range p.sessions {
		sessionIDs = append(sessionIDs, sessionID)
	}
	p.mu.Unlock()
	sortStrings(sessionIDs)

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
	sdk, cfg, err := p.configured()
	if err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	sessionID = strings.TrimSpace(sessionID)

	var (
		inst *instanceRef
		s    *session
	)
	p.mu.Lock()
	if current, ok := p.sessions[sessionID]; ok && current != nil {
		s = current
		inst = s.instance
	}
	p.mu.Unlock()

	if inst != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cfg.CleanupTimeout)
		err := stopInstance(cleanupCtx, sdk, inst.id)
		cancel()
		if err != nil {
			p.mu.Lock()
			if current, ok := p.sessions[sessionID]; ok && current == s {
				current.state = sessionStateFailed
			}
			p.mu.Unlock()
			return status.Errorf(codes.Internal, "stop nebius runtime session: %v", err)
		}
		closeInstance(inst)
	}

	p.mu.Lock()
	if current, ok := p.sessions[sessionID]; ok && current == s {
		delete(p.sessions, sessionID)
		current.state = sessionStateStopped
	}
	p.mu.Unlock()
	return nil
}

func (p *Provider) StartPlugin(ctx context.Context, req gestalt.StartHostedPluginRequest) (gestalt.HostedPlugin, error) {
	if strings.TrimSpace(req.Command) == "" {
		return gestalt.HostedPlugin{}, status.Error(codes.InvalidArgument, "plugin command is required")
	}
	sdk, cfg, err := p.configured()
	if err != nil {
		return gestalt.HostedPlugin{}, status.Error(codes.FailedPrecondition, err.Error())
	}

	image, err := p.beginPluginStart(req.SessionID)
	if err != nil {
		return gestalt.HostedPlugin{}, err
	}

	inst, err := p.ensureSessionInstance(ctx, sdk, cfg, req)
	if err != nil {
		p.clearPluginStart(req.SessionID)
		return gestalt.HostedPlugin{}, err
	}
	launchOK := false
	defer func() {
		if launchOK {
			return
		}
		p.clearPluginStart(req.SessionID)
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cfg.CleanupTimeout)
		defer cancel()
		_ = cleanupRemoteContainer(cleanupCtx, inst.client, dockerContainerName(req.PluginName, req.SessionID))
		p.resetSessionInstance(req.SessionID, inst)
		_ = stopInstance(cleanupCtx, sdk, inst.id)
		closeInstance(inst)
	}()

	env := buildPluginEnv(req, fmt.Sprintf("tcp://127.0.0.1:%d", pluginGRPCPort))

	containerName := dockerContainerName(req.PluginName, req.SessionID)
	runCmd := buildDockerRunCommand(containerName, image, req.Command, req.Args, env)
	if _, err := runRemoteCommand(ctx, inst.client, runCmd); err != nil {
		return gestalt.HostedPlugin{}, status.Errorf(codes.Internal, "start plugin container in nebius vm: %v", err)
	}

	readyCtx, cancel := context.WithTimeout(ctx, cfg.PluginReadyTimeout)
	defer cancel()
	if err := waitForPluginReady(readyCtx, inst.forward.DialTarget()); err != nil {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), cfg.CleanupTimeout)
		defer cleanupCancel()
		_ = cleanupRemoteContainer(cleanupCtx, inst.client, containerName)
		return gestalt.HostedPlugin{}, status.Errorf(codes.DeadlineExceeded, "wait for nebius plugin gRPC endpoint: %v", err)
	}

	plugin := &plugin{
		id:            p.newID("plugin"),
		name:          req.PluginName,
		containerName: containerName,
		dialTarget:    inst.forward.DialTarget(),
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	s, err := p.sessionLocked(req.SessionID)
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cfg.CleanupTimeout)
		defer cancel()
		_ = cleanupRemoteContainer(cleanupCtx, inst.client, containerName)
		_ = stopInstance(cleanupCtx, sdk, inst.id)
		closeInstance(inst)
		return gestalt.HostedPlugin{}, status.Error(codes.NotFound, err.Error())
	}
	s.pluginStarting = false
	s.plugin = plugin
	s.state = sessionStateRunning
	go p.watchRemoteContainer(req.SessionID, inst, containerName)
	launchOK = true

	return gestalt.HostedPlugin{
		ID:         plugin.id,
		SessionID:  s.id,
		PluginName: plugin.name,
		DialTarget: plugin.dialTarget,
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
	sdk := p.sdk
	p.mu.Unlock()

	var errs []error
	for _, id := range sessionIDs {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := p.StopSession(stopCtx, id)
		cancel()
		errs = append(errs, err)
	}
	if sdk != nil {
		errs = append(errs, sdk.Close())
	}
	return errors.Join(errs...)
}

func (p *Provider) configured() (*gosdk.SDK, Config, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sdk == nil {
		return nil, Config{}, fmt.Errorf("nebius runtime is not configured")
	}
	if p.cfg.SubnetID == "" {
		return nil, Config{}, fmt.Errorf("nebius runtime subnetID is required")
	}
	return p.sdk, p.cfg, nil
}

func (p *Provider) beginPluginStart(sessionID string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	s, err := p.sessionLocked(sessionID)
	if err != nil {
		return "", status.Error(codes.NotFound, err.Error())
	}
	if s.plugin != nil || s.pluginStarting {
		return "", status.Errorf(codes.FailedPrecondition, "plugin runtime session %q already has a running plugin", sessionID)
	}
	s.pluginStarting = true
	s.state = sessionStateStarting
	return s.image, nil
}

func (p *Provider) clearPluginStart(sessionID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	s, ok := p.sessions[strings.TrimSpace(sessionID)]
	if !ok || s == nil {
		return
	}
	if !s.pluginStarting {
		return
	}
	s.pluginStarting = false
	if s.plugin == nil && s.state != sessionStateStopped && s.state != sessionStateFailed {
		s.state = sessionStateReady
	}
}

func (p *Provider) ensureSessionInstance(ctx context.Context, sdk *gosdk.SDK, cfg Config, req gestalt.StartHostedPluginRequest) (*instanceRef, error) {
	p.mu.Lock()
	s, err := p.sessionLocked(req.SessionID)
	if err != nil {
		p.mu.Unlock()
		return nil, status.Error(codes.NotFound, err.Error())
	}
	if s.instance != nil && s.instance.client != nil && s.instance.forward != nil {
		inst := s.instance
		p.mu.Unlock()
		return inst, nil
	}
	p.mu.Unlock()

	inst, err := startInstance(ctx, sdk, cfg, req.PluginName, req.SessionID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "start nebius vm: %v", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	s, err = p.sessionLocked(req.SessionID)
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cfg.CleanupTimeout)
		defer cancel()
		closeInstance(inst)
		_ = stopInstance(cleanupCtx, sdk, inst.id)
		return nil, status.Error(codes.NotFound, err.Error())
	}
	if s.instance != nil && s.instance.client != nil && s.instance.forward != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cfg.CleanupTimeout)
		defer cancel()
		closeInstance(inst)
		_ = stopInstance(cleanupCtx, sdk, inst.id)
		return s.instance, nil
	}
	s.instance = inst
	if s.metadata == nil {
		s.metadata = map[string]string{}
	}
	s.metadata["compute.instance_id"] = inst.id
	s.metadata["compute.instance_name"] = inst.name
	s.metadata["compute.public_ip"] = inst.publicIP
	s.state = sessionStateReady
	return inst, nil
}

func startInstance(ctx context.Context, sdk *gosdk.SDK, cfg Config, pluginName, sessionID string) (*instanceRef, error) {
	signer, authorizedKey, err := generateEphemeralSSHKey()
	if err != nil {
		return nil, fmt.Errorf("generate ssh key: %w", err)
	}
	hostKey, hostPrivatePEM, hostAuthorizedKey, err := generateEphemeralSSHHostKey()
	if err != nil {
		return nil, fmt.Errorf("generate host ssh key: %w", err)
	}

	instanceService := sdk.Services().Compute().V1().Instance()
	createReq, err := buildCreateInstanceRequest(cfg, pluginName, sessionID, authorizedKey, hostPrivatePEM, hostAuthorizedKey)
	if err != nil {
		return nil, err
	}
	op, err := instanceService.Create(ctx, createReq)
	if err != nil {
		return nil, fmt.Errorf("create instance: %w", err)
	}
	op, err = op.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("wait for instance create: %w", err)
	}
	instanceID := strings.TrimSpace(op.ResourceID())
	if instanceID == "" {
		return nil, fmt.Errorf("create instance: empty resource id")
	}
	launchOK := false
	defer func() {
		if launchOK {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), cfg.CleanupTimeout)
		defer cancel()
		_ = stopInstance(cleanupCtx, sdk, instanceID)
	}()

	instance, err := waitForInstanceReady(ctx, instanceService, instanceID, cfg.InstanceReadyTimeout)
	if err != nil {
		return nil, err
	}
	publicIP := instancePublicIP(instance)
	if publicIP == "" {
		return nil, fmt.Errorf("instance %q is running without a public IP", instanceID)
	}

	sshClient, err := waitForSSH(ctx, publicIP, cfg.Username, signer, hostKey, cfg.BootstrapTimeout)
	if err != nil {
		return nil, err
	}
	if err := waitForBootstrap(ctx, sshClient, cfg.BootstrapTimeout); err != nil {
		_ = sshClient.Close()
		return nil, err
	}
	forward, err := newLocalForwarder(sshClient, "127.0.0.1", pluginGRPCPort)
	if err != nil {
		_ = sshClient.Close()
		return nil, err
	}

	launchOK = true
	return &instanceRef{
		id:       instanceID,
		name:     instance.GetMetadata().GetName(),
		publicIP: publicIP,
		sshUser:  cfg.Username,
		signer:   signer,
		client:   sshClient,
		forward:  forward,
	}, nil
}

func buildCreateInstanceRequest(cfg Config, pluginName, sessionID, authorizedKey, hostPrivatePEM, hostAuthorizedKey string) (*computepb.CreateInstanceRequest, error) {
	diskType, err := cfg.diskTypeEnum()
	if err != nil {
		return nil, err
	}
	bootDisk := &computepb.AttachedDiskSpec{
		AttachMode: computepb.AttachedDiskSpec_READ_WRITE,
		Type: &computepb.AttachedDiskSpec_ManagedDisk{
			ManagedDisk: &computepb.ManagedDisk{
				Name: resourceName("disk", pluginName, sessionID),
				Spec: &computepb.DiskSpec{
					Size: &computepb.DiskSpec_SizeGibibytes{
						SizeGibibytes: cfg.BootDiskSizeGiB,
					},
					Type: diskType,
				},
			},
		},
	}
	switch {
	case cfg.BootDiskImageID != "":
		bootDisk.GetManagedDisk().Spec.Source = &computepb.DiskSpec_SourceImageId{
			SourceImageId: cfg.BootDiskImageID,
		}
	default:
		bootDisk.GetManagedDisk().Spec.Source = &computepb.DiskSpec_SourceImageFamily{
			SourceImageFamily: &computepb.SourceImageFamily{
				ImageFamily: cfg.BootDiskImageFamily,
				ParentId:    cfg.BootDiskImageProjectID,
			},
		}
	}

	securityGroups := make([]*computepb.SecurityGroup, 0, len(cfg.SecurityGroupIDs))
	for _, securityGroupID := range cfg.SecurityGroupIDs {
		securityGroups = append(securityGroups, &computepb.SecurityGroup{Id: securityGroupID})
	}
	cloudInitUserData, err := buildCloudInit(cfg.Username, authorizedKey, hostPrivatePEM, hostAuthorizedKey)
	if err != nil {
		return nil, err
	}

	spec := &computepb.InstanceSpec{
		Resources: &computepb.ResourcesSpec{
			Platform: cfg.Platform,
			Size: &computepb.ResourcesSpec_Preset{
				Preset: cfg.Preset,
			},
		},
		BootDisk:          bootDisk,
		CloudInitUserData: cloudInitUserData,
		NetworkInterfaces: []*computepb.NetworkInterfaceSpec{
			{
				Name:           "runtime0",
				SubnetId:       cfg.SubnetID,
				IpAddress:      &computepb.IPAddress{},
				SecurityGroups: securityGroups,
				PublicIpAddress: &computepb.PublicIPAddress{
					Static: cfg.PublicIPStatic,
				},
			},
		},
	}
	if cfg.ServiceAccountID != "" {
		spec.ServiceAccountId = cfg.ServiceAccountID
	}
	return &computepb.CreateInstanceRequest{
		Metadata: &commonpb.ResourceMetadata{
			Name:     resourceName("runtime", pluginName, sessionID),
			ParentId: cfg.ProjectID,
		},
		Spec: spec,
	}, nil
}

func buildCloudInit(username, authorizedKey, hostPrivatePEM, hostAuthorizedKey string) (string, error) {
	doc := struct {
		PackageUpdate bool     `yaml:"package_update"`
		Packages      []string `yaml:"packages,omitempty"`
		Users         []struct {
			Name              string   `yaml:"name"`
			Sudo              string   `yaml:"sudo,omitempty"`
			Shell             string   `yaml:"shell,omitempty"`
			SSHAuthorizedKeys []string `yaml:"ssh_authorized_keys,omitempty"`
		} `yaml:"users,omitempty"`
		SSHDeleteKeys bool              `yaml:"ssh_deletekeys"`
		SSHKeys       map[string]string `yaml:"ssh_keys,omitempty"`
		RunCmd        [][]string        `yaml:"runcmd,omitempty"`
	}{
		PackageUpdate: true,
		Packages:      []string{"docker.io"},
		Users: []struct {
			Name              string   `yaml:"name"`
			Sudo              string   `yaml:"sudo,omitempty"`
			Shell             string   `yaml:"shell,omitempty"`
			SSHAuthorizedKeys []string `yaml:"ssh_authorized_keys,omitempty"`
		}{
			{
				Name:              username,
				Sudo:              "ALL=(ALL) NOPASSWD:ALL",
				Shell:             "/bin/bash",
				SSHAuthorizedKeys: []string{strings.TrimSpace(authorizedKey)},
			},
		},
		SSHDeleteKeys: true,
		SSHKeys: map[string]string{
			"ed25519_private": hostPrivatePEM,
			"ed25519_public":  strings.TrimSpace(hostAuthorizedKey),
		},
		RunCmd: [][]string{
			{"sh", "-lc", "systemctl enable --now docker"},
			{"sh", "-lc", "mkdir -p " + shellQuote(path.Join("/home", username, ".gestalt-runtime"))},
		},
	}
	body, err := yaml.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("encode cloud-init: %w", err)
	}
	return "#cloud-config\n" + string(body), nil
}

func waitForInstanceReady(ctx context.Context, instanceService computesvc.InstanceService, instanceID string, timeout time.Duration) (*computepb.Instance, error) {
	deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var lastErr error
	for {
		instance, err := instanceService.Get(deadlineCtx, &computepb.GetInstanceRequest{Id: instanceID})
		if err == nil && instance != nil {
			switch instance.GetStatus().GetState() {
			case computepb.InstanceStatus_RUNNING:
				if instancePublicIP(instance) != "" {
					return instance, nil
				}
			case computepb.InstanceStatus_ERROR:
				return nil, fmt.Errorf("instance %q entered ERROR state", instanceID)
			}
			lastErr = fmt.Errorf("instance %q state is %s", instanceID, instance.GetStatus().GetState().String())
		} else if err != nil {
			lastErr = err
		}

		select {
		case <-deadlineCtx.Done():
			return nil, fmt.Errorf("wait for instance %q ready: %w", instanceID, lastErr)
		case <-time.After(2 * time.Second):
		}
	}
}

func instancePublicIP(instance *computepb.Instance) string {
	if instance == nil {
		return ""
	}
	for _, nic := range instance.GetStatus().GetNetworkInterfaces() {
		address := strings.TrimSpace(nic.GetPublicIpAddress().GetAddress())
		if address == "" {
			continue
		}
		if ip, _, ok := strings.Cut(address, "/"); ok {
			return strings.TrimSpace(ip)
		}
		return address
	}
	return ""
}

func stopInstance(ctx context.Context, sdk *gosdk.SDK, instanceID string) error {
	if sdk == nil || strings.TrimSpace(instanceID) == "" {
		return nil
	}
	instanceService := sdk.Services().Compute().V1().Instance()
	op, err := instanceService.Delete(ctx, &computepb.DeleteInstanceRequest{Id: instanceID})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil
		}
		return fmt.Errorf("delete instance %q: %w", instanceID, err)
	}
	if _, err := op.Wait(ctx); err != nil {
		if status.Code(err) == codes.NotFound {
			return nil
		}
		return fmt.Errorf("wait for instance %q delete: %w", instanceID, err)
	}
	return nil
}

func buildDockerRunCommand(containerName, image, command string, args []string, env map[string]string) string {
	argv := []string{"sudo", "docker", "run", "-d", "--rm", "--name", containerName, "--network", "host"}
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sortStrings(keys)
	for _, key := range keys {
		argv = append(argv, "-e", key+"="+env[key])
	}
	argv = append(argv, image, command)
	argv = append(argv, args...)
	cleanup := []string{"sh", "-lc", "sudo docker rm -f " + shellQuote(containerName) + " >/dev/null 2>&1 || true"}
	return joinShellCommand(cleanup) + " && " + joinShellCommand(argv)
}

func cleanupRemoteContainer(ctx context.Context, client *ssh.Client, containerName string) error {
	if client == nil || strings.TrimSpace(containerName) == "" {
		return nil
	}
	_, err := runRemoteCommand(ctx, client, "sh -lc "+shellQuote("sudo docker rm -f "+shellQuote(containerName)+" >/dev/null 2>&1 || true"))
	return err
}

func (p *Provider) watchRemoteContainer(sessionID string, inst *instanceRef, containerName string) {
	if inst == nil || inst.client == nil || strings.TrimSpace(containerName) == "" {
		return
	}
	output, err := runRemoteCommand(context.Background(), inst.client, "sudo docker wait "+shellQuote(containerName))

	p.mu.Lock()
	defer p.mu.Unlock()
	s, ok := p.sessions[sessionID]
	if !ok || s == nil {
		return
	}
	if err != nil {
		if s.state != sessionStateStopped {
			s.state = sessionStateFailed
		}
		return
	}
	code, convErr := strconv.Atoi(strings.TrimSpace(output))
	if convErr != nil {
		s.state = sessionStateFailed
		return
	}
	if code == 0 {
		s.state = sessionStateStopped
		return
	}
	s.state = sessionStateFailed
}

func closeInstance(inst *instanceRef) {
	if inst == nil {
		return
	}
	if inst.forward != nil {
		_ = inst.forward.Close()
	}
	if inst.client != nil {
		_ = inst.client.Close()
	}
}

func (p *Provider) resetSessionInstance(sessionID string, inst *instanceRef) {
	if inst == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	s, ok := p.sessions[strings.TrimSpace(sessionID)]
	if !ok || s == nil {
		return
	}
	if s.instance != inst {
		return
	}
	s.instance = nil
	if s.plugin == nil {
		s.state = sessionStateReady
	}
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

func cloneSession(s *session) gestalt.PluginRuntimeSession {
	if s == nil {
		return gestalt.PluginRuntimeSession{}
	}
	return gestalt.PluginRuntimeSession{
		ID:       s.id,
		State:    s.state,
		Metadata: cloneStringMap(s.metadata),
	}
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

func generateEphemeralSSHKey() (ssh.Signer, string, error) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, "", err
	}
	signer, err := ssh.NewSignerFromKey(privateKey)
	if err != nil {
		return nil, "", err
	}
	return signer, strings.TrimSpace(string(ssh.MarshalAuthorizedKey(signer.PublicKey()))), nil
}

func generateEphemeralSSHHostKey() (ssh.PublicKey, string, string, error) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, "", "", err
	}
	signer, err := ssh.NewSignerFromKey(privateKey)
	if err != nil {
		return nil, "", "", err
	}
	block, err := ssh.MarshalPrivateKey(privateKey, "gestalt-runtime-host")
	if err != nil {
		return nil, "", "", err
	}
	return signer.PublicKey(), string(pem.EncodeToMemory(block)), strings.TrimSpace(string(ssh.MarshalAuthorizedKey(signer.PublicKey()))), nil
}

func resourceName(prefix, pluginName, sessionID string) string {
	name := strings.ToLower(strings.TrimSpace(pluginName))
	if name == "" {
		name = "plugin"
	}
	name = resourceNamePattern.ReplaceAllString(name, "-")
	name = strings.Trim(name, "-")
	if name == "" {
		name = "plugin"
	}
	value := fmt.Sprintf("gestalt-%s-%s-%s", prefix, name, sessionID)
	if len(value) <= 63 {
		return value
	}
	return strings.TrimRight(value[:63], "-")
}

func dockerContainerName(pluginName, sessionID string) string {
	return resourceName("plugin", pluginName, sessionID)
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

func sortStrings(values []string) {
	if len(values) < 2 {
		return
	}
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}

var _ gestalt.PluginRuntimeProvider = (*Provider)(nil)
var _ gestalt.MetadataProvider = (*Provider)(nil)
var _ gestalt.HealthChecker = (*Provider)(nil)
var _ gestalt.Closer = (*Provider)(nil)
