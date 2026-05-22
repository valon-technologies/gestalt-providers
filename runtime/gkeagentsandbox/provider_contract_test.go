package gkeagentsandbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestRuntimeProviderContractLaunchesHostedPlugin(t *testing.T) {
	t.Parallel()

	pluginTarget := startPluginLifecycleServer(t)
	fake := &fakeSandboxRuntime{
		tunnel: &fakeTunnel{dialTarget: pluginTarget},
	}
	client := startRuntimeProviderServer(t, &Provider{
		name: "gkeAgentSandbox",
		cfg: Config{
			Namespace:           "runtime-system",
			PluginPort:          50051,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime:  fake,
		sessions: map[string]*localSession{},
	})

	ctx := context.Background()
	pluginName := "github_plugin.with-a-very-long-name-that-preserves-session-suffix"
	support, err := client.GetSupport(ctx)
	if err != nil {
		t.Fatalf("GetSupport: %v", err)
	}
	if !support.CanHostPlugins {
		t.Fatalf("GetSupport CanHostPlugins = false")
	}
	if got, want := support.EgressMode, gestalt.PluginRuntimeEgressModeHostname; got != want {
		t.Fatalf("GetSupport EgressMode = %v, want %v", got, want)
	}

	session, err := client.StartSession(ctx, gestalt.StartPluginRuntimeSessionRequest{
		PluginName: pluginName,
		Template:   "python-runtime",
		Metadata: map[string]string{
			"tenant": "dev",
		},
	})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if got, want := session.State, sessionStateReady; got != want {
		t.Fatalf("StartSession state = %q, want %q", got, want)
	}
	if got, want := session.Metadata["kubernetes.namespace"], "runtime-system"; got != want {
		t.Fatalf("StartSession metadata kubernetes.namespace = %q, want %q", got, want)
	}
	if got := session.Metadata["kubernetes.sandboxClaim"]; got == "" {
		t.Fatalf("StartSession metadata missing kubernetes.sandboxClaim")
	}

	fake.mu.Lock()
	startRequests := slices.Clone(fake.startRequests)
	fake.mu.Unlock()
	if len(startRequests) != 1 {
		t.Fatalf("runtime Start calls = %d, want 1", len(startRequests))
	}
	if got, want := startRequests[0].Template, "python-runtime"; got != want {
		t.Fatalf("runtime Start template = %q, want %q", got, want)
	}
	if len(startRequests[0].Name) > 63 {
		t.Fatalf("runtime Start resource name length = %d, want <= 63", len(startRequests[0].Name))
	}
	if strings.ContainsAny(startRequests[0].Name, "_.") {
		t.Fatalf("runtime Start resource name = %q, want DNS label characters only", startRequests[0].Name)
	}
	if !strings.HasSuffix(startRequests[0].Name, session.ID) {
		t.Fatalf("runtime Start resource name = %q, want suffix %q", startRequests[0].Name, session.ID)
	}
	if got, want := startRequests[0].Metadata["tenant"], "dev"; got != want {
		t.Fatalf("runtime Start metadata tenant = %q, want %q", got, want)
	}

	const hostServiceEnv = gestalt.EnvHostServiceSocket

	hosted, err := client.StartPlugin(ctx, gestalt.StartHostedPluginRequest{
		SessionID:  session.ID,
		PluginName: pluginName,
		Command:    "./plugin",
		Args:       []string{"--serve", "space value"},
		Env: map[string]string{
			"CUSTOM":                  "value",
			hostServiceEnv:            "tls://host-service-relay.gestalt.example:443",
			gestalt.EnvHostServiceToken: "host-service-token",
		},
	})
	if err != nil {
		t.Fatalf("StartPlugin: %v", err)
	}
	if got, want := hosted.DialTarget, pluginTarget; got != want {
		t.Fatalf("StartPlugin dial target = %q, want %q", got, want)
	}

	fake.mu.Lock()
	execCalls := slices.Clone(fake.execCalls)
	forwardPorts := slices.Clone(fake.forwardPorts)
	fake.mu.Unlock()
	if len(execCalls) < 2 {
		t.Fatalf("runtime Exec calls = %d, want launch and readiness checks", len(execCalls))
	}
	if !slices.Equal(execCalls[0].command[:2], []string{"sh", "-c"}) {
		t.Fatalf("runtime Exec command = %#v, want sh -c", execCalls[0].command)
	}
	launchScript := execCalls[0].command[2]
	for _, want := range []string{
		"command -v socat",
		"TCP-LISTEN:50051",
		"UNIX-CONNECT:'/tmp/gestalt/plugin.sock'",
		"'GESTALT_PLUGIN_SOCKET=/tmp/gestalt/plugin.sock'",
		"'GESTALT_HOST_SERVICE_SOCKET=tls://host-service-relay.gestalt.example:443'",
		"'GESTALT_HOST_SERVICE_TOKEN=host-service-token'",
		"'CUSTOM=value'",
		"'./plugin' '--serve' 'space value'",
	} {
		if !strings.Contains(launchScript, want) {
			t.Fatalf("launch script missing %q:\n%s", want, launchScript)
		}
	}
	readyScript := execCalls[1].command[2]
	for _, want := range []string{
		"test -S '/tmp/gestalt/plugin.sock'",
		"/tmp/gestalt-socket-proxy.pid",
		":C383",
		"/proc/net/tcp",
	} {
		if !strings.Contains(readyScript, want) {
			t.Fatalf("ready script missing %q:\n%s", want, readyScript)
		}
	}
	if !slices.Equal(forwardPorts, []int{50051}) {
		t.Fatalf("runtime ForwardPort calls = %#v, want [50051]", forwardPorts)
	}

	running, err := client.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if got, want := running.State, sessionStateRunning; got != want {
		t.Fatalf("GetSession state = %q, want %q", got, want)
	}

	if err := client.StopSession(ctx, session.ID); err != nil {
		t.Fatalf("StopSession: %v", err)
	}
	fake.mu.Lock()
	stopped := slices.Clone(fake.stopped)
	fake.mu.Unlock()
	if len(stopped) != 1 {
		t.Fatalf("runtime Stop calls = %d, want 1", len(stopped))
	}
	if !fake.tunnel.(*fakeTunnel).Closed() {
		t.Fatalf("plugin tunnel was not closed")
	}
}

func TestRuntimeProviderContractStartPluginRejectsStaleSessionBeforeLease(t *testing.T) {
	t.Parallel()

	fake := &fakeSandboxRuntime{verifyErr: errors.Join(errStaleRuntimeSession, errors.New("template image mismatch"))}
	provider := &Provider{
		name: "gkeAgentSandbox",
		cfg: Config{
			Namespace:           "runtime-system",
			PluginPort:          50051,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime:  fake,
		sessions: map[string]*localSession{},
	}
	client := startRuntimeProviderServer(t, provider)

	ctx := context.Background()
	session, err := client.StartSession(ctx, gestalt.StartPluginRuntimeSessionRequest{
		PluginName: "github",
		Template:   "python-runtime",
	})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	tunnel := &fakeTunnel{dialTarget: "tcp://127.0.0.1:1"}
	provider.setLocalTunnel(session.ID, tunnel)
	_, err = client.StartPlugin(ctx, gestalt.StartHostedPluginRequest{
		SessionID:  session.ID,
		PluginName: "github",
		Command:    "./plugin",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StartPlugin code = %v, want FailedPrecondition: %v", status.Code(err), err)
	}
	fake.mu.Lock()
	leases := len(fake.leases)
	fake.mu.Unlock()
	if leases != 0 {
		t.Fatalf("leases = %d, want no lease acquired for stale session", leases)
	}
	if !tunnel.Closed() {
		t.Fatal("stale compatibility error did not close the local tunnel")
	}
	if got := provider.localTunnel(session.ID); got != nil {
		t.Fatalf("local tunnel after stale compatibility error = %#v, want nil", got)
	}
}

func TestRuntimeProviderContractKeepsLocalTunnelOnTransientCompatibilityError(t *testing.T) {
	t.Parallel()

	fake := &fakeSandboxRuntime{verifyErr: status.Error(codes.Unavailable, "kube api temporarily unavailable")}
	provider := &Provider{
		name: "gkeAgentSandbox",
		cfg: Config{
			Namespace:           "runtime-system",
			PluginPort:          50051,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime:  fake,
		sessions: map[string]*localSession{},
	}
	client := startRuntimeProviderServer(t, provider)

	ctx := context.Background()
	session, err := client.StartSession(ctx, gestalt.StartPluginRuntimeSessionRequest{
		PluginName: "github",
		Template:   "python-runtime",
	})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	tunnel := &fakeTunnel{dialTarget: "tcp://127.0.0.1:1"}
	provider.setLocalTunnel(session.ID, tunnel)

	_, err = client.StartPlugin(ctx, gestalt.StartHostedPluginRequest{
		SessionID:  session.ID,
		PluginName: "github",
		Command:    "./plugin",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StartPlugin code = %v, want FailedPrecondition: %v", status.Code(err), err)
	}
	if tunnel.Closed() {
		t.Fatal("transient compatibility error closed the local tunnel")
	}
	if got := provider.localTunnel(session.ID); got == nil {
		t.Fatal("local tunnel after transient compatibility error = nil, want retained tunnel")
	}
}

func TestRuntimeProviderContractScopesKubernetesNamesByProviderInstance(t *testing.T) {
	t.Parallel()

	fakeA := &fakeSandboxRuntime{}
	clientA := startRuntimeProviderServer(t, &Provider{
		name:       "gkeAgentSandbox",
		instanceID: "runtime-a",
		cfg: Config{
			Namespace:           "runtime-system",
			PluginPort:          50051,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime:  fakeA,
		sessions: map[string]*localSession{},
	})
	fakeB := &fakeSandboxRuntime{}
	clientB := startRuntimeProviderServer(t, &Provider{
		name:       "gkeAgentSandbox",
		instanceID: "runtime-b",
		cfg: Config{
			Namespace:           "runtime-system",
			PluginPort:          50051,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime:  fakeB,
		sessions: map[string]*localSession{},
	})

	ctx := context.Background()
	sessionA, err := clientA.StartSession(ctx, gestalt.StartPluginRuntimeSessionRequest{
		PluginName: "simple",
		Template:   "python-runtime",
	})
	if err != nil {
		t.Fatalf("StartSession A: %v", err)
	}
	sessionB, err := clientB.StartSession(ctx, gestalt.StartPluginRuntimeSessionRequest{
		PluginName: "simple",
		Template:   "python-runtime",
	})
	if err != nil {
		t.Fatalf("StartSession B: %v", err)
	}
	if sessionA.ID == sessionB.ID {
		t.Fatalf("session ids both = %q, want provider-instance scoped session ids", sessionA.ID)
	}

	fakeA.mu.Lock()
	requestsA := slices.Clone(fakeA.startRequests)
	fakeA.mu.Unlock()
	fakeB.mu.Lock()
	requestsB := slices.Clone(fakeB.startRequests)
	fakeB.mu.Unlock()
	if len(requestsA) != 1 || len(requestsB) != 1 {
		t.Fatalf("runtime Start calls = (%d, %d), want one each", len(requestsA), len(requestsB))
	}
	nameA := requestsA[0].Name
	nameB := requestsB[0].Name
	if nameA == nameB {
		t.Fatalf("runtime Start resource names both = %q, want provider-instance scoped names", nameA)
	}
	if !strings.Contains(nameA, "runtime-a") || !strings.Contains(nameB, "runtime-b") {
		t.Fatalf("runtime Start resource names = (%q, %q), want provider instance ids", nameA, nameB)
	}
	if nameA != sessionA.ID || nameB != sessionB.ID {
		t.Fatalf("runtime Start resource names = (%q, %q), want session ids (%q, %q)", nameA, nameB, sessionA.ID, sessionB.ID)
	}
}

func TestRuntimeProviderContractPeerInstanceCanResolveListAndStopSession(t *testing.T) {
	t.Parallel()

	fake := &fakeSandboxRuntime{}
	cfg := Config{
		Namespace:           "runtime-system",
		PluginPort:          50051,
		SandboxReadyTimeout: 2 * time.Second,
		PluginReadyTimeout:  2 * time.Second,
		ExecTimeout:         2 * time.Second,
		CleanupTimeout:      2 * time.Second,
	}
	clientA := startRuntimeProviderServer(t, &Provider{
		name:       "gkeAgentSandbox",
		instanceID: "runtime-a",
		cfg:        cfg,
		runtime:    fake,
		sessions:   map[string]*localSession{},
	})
	clientB := startRuntimeProviderServer(t, &Provider{
		name:       "gkeAgentSandbox",
		instanceID: "runtime-b",
		cfg:        cfg,
		runtime:    fake,
		sessions:   map[string]*localSession{},
	})

	ctx := context.Background()
	session, err := clientA.StartSession(ctx, gestalt.StartPluginRuntimeSessionRequest{
		PluginName: "agent-provider",
		Template:   "python-runtime",
		Metadata:   map[string]string{"tenant": "dev"},
	})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	peerSession, err := clientB.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("peer GetSession: %v", err)
	}
	if peerSession.ID != session.ID {
		t.Fatalf("peer session ID = %q, want %q", peerSession.ID, session.ID)
	}
	if got, want := peerSession.Metadata["tenant"], "dev"; got != want {
		t.Fatalf("peer metadata tenant = %q, want %q", got, want)
	}

	listed, err := clientB.ListSessions(ctx)
	if err != nil {
		t.Fatalf("peer ListSessions: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != session.ID {
		t.Fatalf("peer ListSessions = %#v, want one session %q", listed, session.ID)
	}

	if err := clientB.StopSession(ctx, session.ID); err != nil {
		t.Fatalf("peer StopSession: %v", err)
	}
	if _, err := clientA.GetSession(ctx, session.ID); status.Code(err) != codes.NotFound {
		t.Fatalf("GetSession after peer StopSession code = %v, want NotFound: %v", status.Code(err), err)
	}
}

func TestRuntimeProviderContractListsSessions(t *testing.T) {
	t.Parallel()

	fake := &fakeSandboxRuntime{
		sessions: map[string]sandboxSession{
			"session-b": {
				ID:       "session-b",
				Metadata: map[string]string{"tenant": "beta"},
				Handle:   sandboxHandle{Name: "session-b", Namespace: "runtime-system", Mode: "claim", ClaimName: "session-b", Ready: true},
			},
			"session-a": {
				ID:       "session-a",
				Metadata: map[string]string{"tenant": "alpha"},
				Handle:   sandboxHandle{Name: "session-a", Namespace: "runtime-system", Mode: "claim", ClaimName: "session-a", Ready: true},
			},
		},
	}
	client := startRuntimeProviderServer(t, &Provider{
		name:    "gkeAgentSandbox",
		runtime: fake,
		cfg:     Config{Namespace: "runtime-system"},
	})

	sessions, err := client.ListSessions(context.Background())
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(sessions) != 2 {
		t.Fatalf("ListSessions len = %d, want 2", len(sessions))
	}
	if got, want := sessions[0].ID, "session-a"; got != want {
		t.Fatalf("first session id = %q, want %q", got, want)
	}
	if got, want := sessions[1].ID, "session-b"; got != want {
		t.Fatalf("second session id = %q, want %q", got, want)
	}
	if got, want := sessions[0].Metadata["tenant"], "alpha"; got != want {
		t.Fatalf("first session tenant = %q, want %q", got, want)
	}
	if got, want := sessions[1].Metadata["tenant"], "beta"; got != want {
		t.Fatalf("second session tenant = %q, want %q", got, want)
	}
}

func TestRuntimeProviderContractConfiguresHostnameEgressPolicyAndAgentHostRelay(t *testing.T) {
	t.Parallel()

	pluginTarget := startPluginLifecycleServer(t)
	fake := &fakeSandboxRuntime{
		tunnel: &fakeTunnel{dialTarget: pluginTarget},
	}
	client := startRuntimeProviderServer(t, &Provider{
		name: "gkeAgentSandbox",
		cfg: Config{
			Namespace:           "runtime-system",
			PluginPort:          50051,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime:  fake,
		sessions: map[string]*localSession{},
	})

	ctx := context.Background()
	session, err := client.StartSession(ctx, gestalt.StartPluginRuntimeSessionRequest{
		PluginName: "agent-provider",
		Template:   "python-runtime",
	})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	if _, err := client.StartPlugin(ctx, gestalt.StartHostedPluginRequest{
		SessionID:     session.ID,
		PluginName:    "agent-provider",
		Command:       "./plugin",
		AllowedHosts:  []string{"api.github.com"},
		DefaultAction: "deny",
		Env: map[string]string{
			"HTTPS_PROXY":                            "https://proxy.gestalt.example:9443",
			gestalt.EnvHostServiceSocket: "tls://host-service-relay.gestalt.example:7443",
			gestalt.EnvHostServiceToken:  "host-service-token",
		},
	}); err != nil {
		t.Fatalf("StartPlugin: %v", err)
	}

	fake.mu.Lock()
	policies := slices.Clone(fake.hostnamePolicies)
	execCalls := slices.Clone(fake.execCalls)
	fake.mu.Unlock()

	if len(policies) != 1 {
		t.Fatalf("hostname egress policies = %d, want 1", len(policies))
	}
	if got, want := policies[0].config.Template, "python-runtime"; got != want {
		t.Fatalf("hostname egress template = %q, want %q", got, want)
	}
	for _, want := range []hostnameEgressEndpoint{
		{Host: "proxy.gestalt.example", Port: 9443},
		{Host: "host-service-relay.gestalt.example", Port: 7443},
	} {
		if !slices.Contains(policies[0].config.Endpoints, want) {
			t.Fatalf("hostname egress endpoints = %#v, want %#v", policies[0].config.Endpoints, want)
		}
	}
	if len(execCalls) < 2 {
		t.Fatalf("runtime Exec calls = %d, want launch and readiness checks", len(execCalls))
	}
	launchScript := execCalls[0].command[2]
	for _, want := range []string{
		"'GESTALT_HOST_SERVICE_SOCKET=tls://host-service-relay.gestalt.example:7443'",
		"'GESTALT_HOST_SERVICE_TOKEN=host-service-token'",
	} {
		if !strings.Contains(launchScript, want) {
			t.Fatalf("launch script missing %q:\n%s", want, launchScript)
		}
	}

	if err := client.StopSession(ctx, session.ID); err != nil {
		t.Fatalf("StopSession: %v", err)
	}
	fake.mu.Lock()
	deletedPolicies := append([]string(nil), fake.deletedHostnamePolicies...)
	fake.mu.Unlock()
	if got, want := deletedPolicies, []string{policies[0].name}; !slices.Equal(got, want) {
		t.Fatalf("deleted hostname egress policies = %#v, want %#v", got, want)
	}
}

func TestRuntimeProviderContractSupportsPodIPConnectionMode(t *testing.T) {
	t.Parallel()

	pluginTarget := startPluginLifecycleServer(t)
	fake := &fakeSandboxRuntime{
		tunnel: &fakeTunnel{dialTarget: pluginTarget},
	}
	client := startRuntimeProviderServer(t, &Provider{
		name: "gkeAgentSandbox",
		cfg: Config{
			Namespace:           "runtime-system",
			PluginPort:          50051,
			ConnectionMode:      connectionModePodIP,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime:  fake,
		sessions: map[string]*localSession{},
	})

	ctx := context.Background()
	session, err := client.StartSession(ctx, gestalt.StartPluginRuntimeSessionRequest{
		PluginName: "simple-agent",
		Template:   "agent-runtime",
	})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	hosted, err := client.StartPlugin(ctx, gestalt.StartHostedPluginRequest{
		SessionID:  session.ID,
		PluginName: "simple-agent",
		Command:    "./plugin",
	})
	if err != nil {
		t.Fatalf("StartPlugin: %v", err)
	}
	if got, want := hosted.DialTarget, pluginTarget; got != want {
		t.Fatalf("StartPlugin dial target = %q, want %q", got, want)
	}

	fake.mu.Lock()
	forwardPorts := slices.Clone(fake.forwardPorts)
	podIPTargets := slices.Clone(fake.podIPTargets)
	fake.mu.Unlock()
	if len(forwardPorts) != 0 {
		t.Fatalf("runtime ForwardPort calls = %#v, want none", forwardPorts)
	}
	if !slices.Equal(podIPTargets, []int{50051}) {
		t.Fatalf("runtime PodIPDialTarget calls = %#v, want [50051]", podIPTargets)
	}
}

func TestRuntimeProviderContractInvalidatesLocalTunnelAfterPodIPOpenUnavailable(t *testing.T) {
	t.Parallel()

	fake := &fakeSandboxRuntime{
		podIPDialErr: status.Error(codes.Unavailable, "sandbox pod disappeared"),
	}
	provider := &Provider{
		name: "gkeAgentSandbox",
		cfg: Config{
			Namespace:           "runtime-system",
			PluginPort:          50051,
			ConnectionMode:      connectionModePodIP,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime:  fake,
		sessions: map[string]*localSession{},
	}
	client := startRuntimeProviderServer(t, provider)

	ctx := context.Background()
	session, err := client.StartSession(ctx, gestalt.StartPluginRuntimeSessionRequest{
		PluginName: "simple-agent",
		Template:   "agent-runtime",
	})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	oldTunnel := &fakeTunnel{dialTarget: "tcp://127.0.0.1:1"}
	provider.setLocalTunnel(session.ID, oldTunnel)
	_, err = client.StartPlugin(ctx, gestalt.StartHostedPluginRequest{
		SessionID:  session.ID,
		PluginName: "simple-agent",
		Command:    "./plugin",
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("StartPlugin code = %v, want Unavailable: %v", status.Code(err), err)
	}
	if !oldTunnel.Closed() {
		t.Fatalf("old local tunnel was not closed")
	}
	if tunnel := provider.localTunnel(session.ID); tunnel != nil {
		t.Fatalf("local tunnel after unavailable podIP open = %#v, want nil", tunnel)
	}
}

func TestRuntimeProviderContractSupportsServiceDNSConnectionMode(t *testing.T) {
	t.Parallel()

	pluginTarget := startPluginLifecycleServer(t)
	fake := &fakeSandboxRuntime{
		tunnel: &fakeTunnel{dialTarget: pluginTarget},
	}
	client := startRuntimeProviderServer(t, &Provider{
		name: "gkeAgentSandbox",
		cfg: Config{
			Namespace:           "runtime-system",
			PluginPort:          50051,
			ConnectionMode:      connectionModeServiceDNS,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime:  fake,
		sessions: map[string]*localSession{},
	})

	ctx := context.Background()
	session, err := client.StartSession(ctx, gestalt.StartPluginRuntimeSessionRequest{
		PluginName: "simple-agent",
		Template:   "agent-runtime",
	})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	hosted, err := client.StartPlugin(ctx, gestalt.StartHostedPluginRequest{
		SessionID:  session.ID,
		PluginName: "simple-agent",
		Command:    "./plugin",
	})
	if err != nil {
		t.Fatalf("StartPlugin: %v", err)
	}
	if got, want := hosted.DialTarget, pluginTarget; got != want {
		t.Fatalf("StartPlugin dial target = %q, want %q", got, want)
	}

	fake.mu.Lock()
	forwardPorts := slices.Clone(fake.forwardPorts)
	serviceDNSTargets := slices.Clone(fake.serviceDNSTargets)
	fake.mu.Unlock()
	if len(forwardPorts) != 0 {
		t.Fatalf("runtime ForwardPort calls = %#v, want none", forwardPorts)
	}
	if !slices.Equal(serviceDNSTargets, []int{50051}) {
		t.Fatalf("runtime ServiceDNSDialTarget calls = %#v, want [50051]", serviceDNSTargets)
	}
}

func TestRuntimeProviderContractRejectsHostnameEgressWithoutProxy(t *testing.T) {
	t.Parallel()

	fake := &fakeSandboxRuntime{
		tunnel: &fakeTunnel{dialTarget: startPluginLifecycleServer(t)},
	}
	client := startRuntimeProviderServer(t, &Provider{
		name: "gkeAgentSandbox",
		cfg: Config{
			Namespace:           "runtime-system",
			PluginPort:          50051,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime:  fake,
		sessions: map[string]*localSession{},
	})

	ctx := context.Background()
	session, err := client.StartSession(ctx, gestalt.StartPluginRuntimeSessionRequest{PluginName: "github", Template: "python-runtime"})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	_, err = client.StartPlugin(ctx, gestalt.StartHostedPluginRequest{
		SessionID:    session.ID,
		PluginName:   "github",
		Command:      "./plugin",
		AllowedHosts: []string{"api.github.com"},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StartPlugin code = %v, want FailedPrecondition: %v", status.Code(err), err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.hostnamePolicies) != 0 {
		t.Fatalf("hostname egress policies = %d, want 0", len(fake.hostnamePolicies))
	}
	if len(fake.execCalls) != 0 {
		t.Fatalf("runtime Exec calls = %d, want 0", len(fake.execCalls))
	}
}

func TestRuntimeProviderContractAllowsRelayOnlyAgentHostLaunchWithoutProxy(t *testing.T) {
	t.Parallel()

	pluginTarget := startPluginLifecycleServer(t)
	fake := &fakeSandboxRuntime{
		tunnel: &fakeTunnel{dialTarget: pluginTarget},
	}
	client := startRuntimeProviderServer(t, &Provider{
		name: "gkeAgentSandbox",
		cfg: Config{
			Namespace:           "runtime-system",
			PluginPort:          50051,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime:  fake,
		sessions: map[string]*localSession{},
	})

	ctx := context.Background()
	session, err := client.StartSession(ctx, gestalt.StartPluginRuntimeSessionRequest{
		PluginName: "agent-provider",
		Template:   "python-runtime",
	})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	if _, err := client.StartPlugin(ctx, gestalt.StartHostedPluginRequest{
		SessionID:  session.ID,
		PluginName: "agent-provider",
		Command:    "./plugin",
		AllowedHosts: []string{
			"host-service-relay.gestalt.example",
		},
		Env: map[string]string{
			gestalt.EnvHostServiceSocket: "tls://host-service-relay.gestalt.example:7443",
			gestalt.EnvHostServiceToken:  "host-service-token",
		},
	}); err != nil {
		t.Fatalf("StartPlugin: %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.hostnamePolicies) != 0 {
		t.Fatalf("hostname egress policies = %d, want 0 for relay-only launch", len(fake.hostnamePolicies))
	}
	if len(fake.execCalls) < 2 {
		t.Fatalf("runtime Exec calls = %d, want launch and readiness checks", len(fake.execCalls))
	}
	launchScript := fake.execCalls[0].command[2]
	for _, want := range []string{
		"'GESTALT_HOST_SERVICE_SOCKET=tls://host-service-relay.gestalt.example:7443'",
		"'GESTALT_HOST_SERVICE_TOKEN=host-service-token'",
	} {
		if !strings.Contains(launchScript, want) {
			t.Fatalf("launch script missing %q:\n%s", want, launchScript)
		}
	}
}

func TestRuntimeProviderContractKeepsHostnamePolicyWhenLaunchCleanupFails(t *testing.T) {
	t.Parallel()

	fake := &fakeSandboxRuntime{
		tunnel: &fakeTunnel{dialTarget: startPluginLifecycleServer(t)},
		execErrors: []error{
			nil,
			nil,
			status.Error(codes.Unavailable, "cleanup failed"),
		},
		forwardPortErr: status.Error(codes.Unavailable, "port-forward failed"),
	}
	client := startRuntimeProviderServer(t, &Provider{
		name: "gkeAgentSandbox",
		cfg: Config{
			Namespace:           "runtime-system",
			PluginPort:          50051,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime:  fake,
		sessions: map[string]*localSession{},
	})

	ctx := context.Background()
	session, err := client.StartSession(ctx, gestalt.StartPluginRuntimeSessionRequest{PluginName: "agent-provider", Template: "python-runtime"})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	_, err = client.StartPlugin(ctx, gestalt.StartHostedPluginRequest{
		SessionID:     session.ID,
		PluginName:    "agent-provider",
		Command:       "./plugin",
		AllowedHosts:  []string{"api.github.com"},
		DefaultAction: "deny",
		Env: map[string]string{
			"HTTPS_PROXY": "https://proxy.gestalt.example:9443",
		},
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("StartPlugin code = %v, want Unavailable: %v", status.Code(err), err)
	}

	fake.mu.Lock()
	if len(fake.deletedHostnamePolicies) != 0 {
		fake.mu.Unlock()
		t.Fatalf("deleted hostname egress policies = %#v, want none after failed cleanup", fake.deletedHostnamePolicies)
	}
	fake.mu.Unlock()

	running, err := client.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("GetSession after failed StartPlugin: %v", err)
	}
	if got, want := running.State, sessionStateReady; got != want {
		t.Fatalf("session state after failed StartPlugin = %q, want %q", got, want)
	}

	if err := client.StopSession(ctx, session.ID); err != nil {
		t.Fatalf("StopSession after failed StartPlugin: %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.deletedHostnamePolicies) != 1 {
		t.Fatalf("deleted hostname egress policies = %#v, want exactly one deletion on StopSession", fake.deletedHostnamePolicies)
	}
}

func TestRuntimeProviderContractRequiresImageWithoutTemplate(t *testing.T) {
	t.Parallel()

	fake := &fakeSandboxRuntime{}
	client := startRuntimeProviderServer(t, &Provider{
		name: "gkeAgentSandbox",
		cfg: Config{
			Namespace:           "runtime-system",
			PluginPort:          50051,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime:  fake,
		sessions: map[string]*localSession{},
	})

	_, err := client.StartSession(context.Background(), gestalt.StartPluginRuntimeSessionRequest{
		PluginName: "github",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("StartSession code = %v, want InvalidArgument: %v", status.Code(err), err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.startRequests) != 0 {
		t.Fatalf("runtime Start calls = %d, want 0", len(fake.startRequests))
	}
}

func TestRuntimeProviderContractRejectsConcurrentPluginLaunch(t *testing.T) {
	t.Parallel()

	pluginTarget := startPluginLifecycleServer(t)
	execEntered := make(chan struct{})
	releaseExec := make(chan struct{})
	fake := &fakeSandboxRuntime{
		tunnel:      &fakeTunnel{dialTarget: pluginTarget},
		execEntered: execEntered,
		blockExec:   releaseExec,
	}
	cfg := Config{
		Namespace:           "runtime-system",
		PluginPort:          50051,
		SandboxReadyTimeout: 2 * time.Second,
		PluginReadyTimeout:  2 * time.Second,
		ExecTimeout:         2 * time.Second,
		CleanupTimeout:      2 * time.Second,
	}
	clientA := startRuntimeProviderServer(t, &Provider{
		name:       "gkeAgentSandbox",
		instanceID: "runtime-a",
		cfg:        cfg,
		runtime:    fake,
		sessions:   map[string]*localSession{},
	})
	clientB := startRuntimeProviderServer(t, &Provider{
		name:       "gkeAgentSandbox",
		instanceID: "runtime-b",
		cfg:        cfg,
		runtime:    fake,
		sessions:   map[string]*localSession{},
	})

	ctx := context.Background()
	session, err := clientA.StartSession(ctx, gestalt.StartPluginRuntimeSessionRequest{PluginName: "github", Template: "python-runtime"})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	firstErr := make(chan error, 1)
	go func() {
		_, err := clientA.StartPlugin(ctx, gestalt.StartHostedPluginRequest{
			SessionID:  session.ID,
			PluginName: "github",
			Command:    "./plugin",
		})
		firstErr <- err
	}()

	select {
	case <-execEntered:
	case <-time.After(2 * time.Second):
		t.Fatalf("first StartPlugin did not reach runtime Exec")
	}

	_, err = clientB.StartPlugin(ctx, gestalt.StartHostedPluginRequest{
		SessionID:  session.ID,
		PluginName: "github",
		Command:    "./plugin",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("second StartPlugin code = %v, want FailedPrecondition: %v", status.Code(err), err)
	}

	close(releaseExec)
	if err := <-firstErr; err != nil {
		t.Fatalf("first StartPlugin: %v", err)
	}
}

func TestRuntimeProviderContractReportsFailedStateAfterPluginDeath(t *testing.T) {
	t.Parallel()

	pluginTarget := startPluginLifecycleServer(t)
	providerTunnel := &fakeTunnel{dialTarget: pluginTarget}
	fake := &fakeSandboxRuntime{
		tunnel:           providerTunnel,
		failHealthChecks: true,
	}
	provider := &Provider{
		name: "gkeAgentSandbox",
		cfg: Config{
			Namespace:           "runtime-system",
			PluginPort:          50051,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime:  fake,
		sessions: map[string]*localSession{},
	}
	client := startRuntimeProviderServer(t, provider)

	ctx := context.Background()
	session, err := client.StartSession(ctx, gestalt.StartPluginRuntimeSessionRequest{PluginName: "github", Template: "python-runtime"})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if _, err := client.StartPlugin(ctx, gestalt.StartHostedPluginRequest{
		SessionID:  session.ID,
		PluginName: "github",
		Command:    "./plugin",
	}); err != nil {
		t.Fatalf("StartPlugin: %v", err)
	}

	failed, err := client.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("GetSession after health failure: %v", err)
	}
	if got, want := failed.State, sessionStateFailed; got != want {
		t.Fatalf("GetSession state after health failure = %q, want %q", got, want)
	}
	if providerTunnel.Closed() {
		t.Fatal("GetSession health failure closed the local tunnel")
	}
	if tunnel := provider.localTunnel(session.ID); tunnel == nil {
		t.Fatal("local tunnel after health failure = nil, want retained tunnel")
	}
	stillFailed, err := client.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("GetSession after sticky failure: %v", err)
	}
	if got, want := stillFailed.State, sessionStateFailed; got != want {
		t.Fatalf("GetSession state after second refresh = %q, want %q", got, want)
	}
}

func TestRuntimeProviderContractKeepsLocalTunnelOnTransientResolveSessionError(t *testing.T) {
	t.Parallel()

	provider := &Provider{
		name: "gkeAgentSandbox",
		cfg: Config{
			Namespace:           "runtime-system",
			PluginPort:          50051,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime: &fakeSandboxRuntime{
			resolveErr: status.Error(codes.Unavailable, "kube api temporarily unavailable"),
		},
		sessions: map[string]*localSession{},
	}
	client := startRuntimeProviderServer(t, provider)
	tunnel := &fakeTunnel{dialTarget: "tcp://127.0.0.1:1"}
	provider.setLocalTunnel("session-1", tunnel)

	_, err := client.GetSession(context.Background(), "session-1")
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("GetSession code = %v, want Unavailable: %v", status.Code(err), err)
	}
	if tunnel.Closed() {
		t.Fatal("transient ResolveSession error closed the local tunnel")
	}
	if got := provider.localTunnel("session-1"); got == nil {
		t.Fatal("local tunnel after transient ResolveSession error = nil, want retained tunnel")
	}
}

func TestRuntimeProviderContractDoesNotTreatStructuralNotFoundAsMissingSession(t *testing.T) {
	t.Parallel()

	provider := &Provider{
		name: "gkeAgentSandbox",
		cfg: Config{
			Namespace:           "runtime-system",
			PluginPort:          50051,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime: &fakeSandboxRuntime{
			resolveErr: status.Error(codes.NotFound, `namespaces "runtime-system" not found`),
		},
		sessions: map[string]*localSession{},
	}
	client := startRuntimeProviderServer(t, provider)
	tunnel := &fakeTunnel{dialTarget: "tcp://127.0.0.1:1"}
	provider.setLocalTunnel("session-1", tunnel)

	_, err := client.GetSession(context.Background(), "session-1")
	if status.Code(err) != codes.Internal {
		t.Fatalf("GetSession code = %v, want Internal for structural NotFound: %v", status.Code(err), err)
	}
	if tunnel.Closed() {
		t.Fatal("structural NotFound closed the local tunnel")
	}
	if got := provider.localTunnel("session-1"); got == nil {
		t.Fatal("local tunnel after structural NotFound = nil, want retained tunnel")
	}
}

func TestRuntimeProviderContractDropsLocalTunnelWhenSessionGone(t *testing.T) {
	t.Parallel()

	provider := &Provider{
		name: "gkeAgentSandbox",
		cfg: Config{
			Namespace:           "runtime-system",
			PluginPort:          50051,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime: &fakeSandboxRuntime{
			resolveErr: errors.New(`plugin runtime session "session-1" not found`),
		},
		sessions: map[string]*localSession{},
	}
	client := startRuntimeProviderServer(t, provider)
	tunnel := &fakeTunnel{dialTarget: "tcp://127.0.0.1:1"}
	provider.setLocalTunnel("session-1", tunnel)

	_, err := client.GetSession(context.Background(), "session-1")
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetSession code = %v, want NotFound: %v", status.Code(err), err)
	}
	if !tunnel.Closed() {
		t.Fatal("missing session did not close the local tunnel")
	}
	if got := provider.localTunnel("session-1"); got != nil {
		t.Fatalf("local tunnel after missing session = %#v, want nil", got)
	}
}

func TestRuntimeProviderContractListSessionsReturnsUnavailableOnTransientRuntimeError(t *testing.T) {
	t.Parallel()

	client := startRuntimeProviderServer(t, &Provider{
		name: "gkeAgentSandbox",
		cfg: Config{
			Namespace: "runtime-system",
		},
		runtime: &fakeSandboxRuntime{
			listErr: context.DeadlineExceeded,
		},
	})

	_, err := client.ListSessions(context.Background())
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("ListSessions code = %v, want Unavailable: %v", status.Code(err), err)
	}
}

func TestRuntimeProviderContractStartPluginResolveSessionUnavailable(t *testing.T) {
	t.Parallel()

	client := startRuntimeProviderServer(t, &Provider{
		name: "gkeAgentSandbox",
		cfg: Config{
			Namespace:           "runtime-system",
			PluginPort:          50051,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime: &fakeSandboxRuntime{
			resolveErr: status.Error(codes.Unavailable, "kube api temporarily unavailable"),
		},
	})

	_, err := client.StartPlugin(context.Background(), gestalt.StartHostedPluginRequest{
		SessionID:  "session-1",
		PluginName: "github",
		Command:    "./plugin",
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("StartPlugin code = %v, want Unavailable: %v", status.Code(err), err)
	}
}

func TestRuntimeProviderContractCanRetryStopAfterDeleteFailure(t *testing.T) {
	t.Parallel()

	fake := &fakeSandboxRuntime{
		tunnel:       &fakeTunnel{dialTarget: startPluginLifecycleServer(t)},
		stopFailures: 1,
	}
	client := startRuntimeProviderServer(t, &Provider{
		name: "gkeAgentSandbox",
		cfg: Config{
			Namespace:           "runtime-system",
			PluginPort:          50051,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime:  fake,
		sessions: map[string]*localSession{},
	})

	ctx := context.Background()
	session, err := client.StartSession(ctx, gestalt.StartPluginRuntimeSessionRequest{PluginName: "github", Template: "python-runtime"})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if _, err := client.StartPlugin(ctx, gestalt.StartHostedPluginRequest{
		SessionID:     session.ID,
		PluginName:    "github",
		Command:       "./plugin",
		AllowedHosts:  []string{"api.github.com"},
		DefaultAction: "deny",
		Env: map[string]string{
			"HTTPS_PROXY": "https://proxy.gestalt.example:9443",
		},
	}); err != nil {
		t.Fatalf("StartPlugin: %v", err)
	}

	if err := client.StopSession(ctx, session.ID); status.Code(err) != codes.Internal {
		t.Fatalf("first StopSession code = %v, want Internal: %v", status.Code(err), err)
	}
	fake.mu.Lock()
	if len(fake.deletedHostnamePolicies) != 0 {
		fake.mu.Unlock()
		t.Fatalf("deleted hostname egress policies after failed StopSession = %#v, want none", fake.deletedHostnamePolicies)
	}
	fake.mu.Unlock()
	stillPresent, err := client.GetSession(ctx, session.ID)
	if err != nil {
		t.Fatalf("GetSession after failed StopSession: %v", err)
	}
	if got, want := stillPresent.State, sessionStateRunning; got != want {
		t.Fatalf("session state after failed StopSession = %q, want %q", got, want)
	}
	if err := client.StopSession(ctx, session.ID); err != nil {
		t.Fatalf("retry StopSession: %v", err)
	}
	fake.mu.Lock()
	if len(fake.deletedHostnamePolicies) != 1 {
		fake.mu.Unlock()
		t.Fatalf("deleted hostname egress policies after successful retry = %#v, want one delete", fake.deletedHostnamePolicies)
	}
	fake.mu.Unlock()
	if _, err := client.GetSession(ctx, session.ID); status.Code(err) != codes.NotFound {
		t.Fatalf("GetSession after successful StopSession code = %v, want NotFound: %v", status.Code(err), err)
	}
}

func TestProviderContractListSessionsDoesNotExecPerSession(t *testing.T) {
	t.Parallel()

	fake := &fakeSandboxRuntime{
		sessions: map[string]sandboxSession{
			"session-a": {
				ID:            "session-a",
				PluginStarted: true,
				Handle:        sandboxHandle{Name: "session-a", Namespace: "runtime-system", Mode: "claim", ClaimName: "session-a", PodName: "pod-a", Ready: true},
			},
			"session-b": {
				ID:            "session-b",
				PluginStarted: true,
				Handle:        sandboxHandle{Name: "session-b", Namespace: "runtime-system", Mode: "claim", ClaimName: "session-b", PodName: "pod-b", Ready: true},
			},
			"session-c": {
				ID:     "session-c",
				Handle: sandboxHandle{Name: "session-c", Namespace: "runtime-system", Mode: "claim", ClaimName: "session-c", PodName: "pod-c", Ready: true},
			},
		},
	}
	client := startRuntimeProviderServer(t, &Provider{
		name:    "gkeAgentSandbox",
		runtime: fake,
		cfg:     Config{Namespace: "runtime-system"},
	})

	sessions, err := client.ListSessions(context.Background())
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if got, want := len(sessions), 3; got != want {
		t.Fatalf("ListSessions returned %d sessions, want %d", got, want)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.healthCheckExecs != 0 {
		t.Fatalf("ListSessions issued %d plugin health Exec calls, want 0 (bulk path must not probe)", fake.healthCheckExecs)
	}
	if len(fake.execCalls) != 0 {
		t.Fatalf("ListSessions issued %d non-health Exec calls, want 0", len(fake.execCalls))
	}
	for _, session := range sessions {
		if session.State == "" {
			t.Fatalf("session %q State = empty", session.ID)
		}
	}
}

func TestProviderContractGetSessionExecsHealthProbe(t *testing.T) {
	t.Parallel()

	fake := &fakeSandboxRuntime{
		sessions: map[string]sandboxSession{
			"session-a": {
				ID:            "session-a",
				PluginStarted: true,
				Handle:        sandboxHandle{Name: "session-a", Namespace: "runtime-system", Mode: "claim", ClaimName: "session-a", PodName: "pod-a", Ready: true},
			},
		},
	}
	client := startRuntimeProviderServer(t, &Provider{
		name:    "gkeAgentSandbox",
		runtime: fake,
		cfg:     Config{Namespace: "runtime-system"},
	})

	if _, err := client.GetSession(context.Background(), "session-a"); err != nil {
		t.Fatalf("GetSession: %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.healthCheckExecs < 1 {
		t.Fatalf("GetSession issued %d plugin health Exec calls, want >= 1 (detail path must probe)", fake.healthCheckExecs)
	}
}

type fakeSandboxRuntime struct {
	mu sync.Mutex

	startRequests           []startSandboxRequest
	sessions                map[string]sandboxSession
	leases                  map[string]string
	execCalls               []execCall
	healthCheckExecs        int
	forwardPorts            []int
	podIPTargets            []int
	serviceDNSTargets       []int
	stopped                 []sandboxHandle
	tunnel                  tunnel
	hostnamePolicies        []hostnamePolicyCall
	deletedHostnamePolicies []string

	execEntered chan struct{}
	blockExec   chan struct{}
	blockOnce   sync.Once

	failHealthChecks  bool
	stopFailures      int
	hostnameEgressErr error
	execErrors        []error
	forwardPortErr    error
	podIPDialErr      error
	serviceDNSDialErr error
	resolveErr        error
	listErr           error
	verifyErr         error
}

type execCall struct {
	handle  sandboxHandle
	command []string
	stdin   string
}

type hostnamePolicyCall struct {
	handle sandboxHandle
	name   string
	config hostnameEgressConfig
}

func (f *fakeSandboxRuntime) HealthCheck(context.Context) error {
	return nil
}

func (f *fakeSandboxRuntime) Start(_ context.Context, req startSandboxRequest) (sandboxSession, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureLocked()
	f.startRequests = append(f.startRequests, req)
	mode := "direct"
	claimName := ""
	if req.Template != "" {
		mode = "claim"
		claimName = req.Name
	}
	handle := sandboxHandle{
		Name:        req.Name,
		Namespace:   req.Namespace,
		Mode:        mode,
		ClaimName:   claimName,
		SandboxName: req.Name + "-sandbox",
		PodName:     req.Name + "-pod",
		Ready:       true,
	}
	session := sandboxSession{
		ID:         req.Name,
		PluginName: req.PluginName,
		Template:   req.Template,
		Metadata:   cloneStringMap(req.Metadata),
		Handle:     handle,
	}
	if session.Metadata == nil {
		session.Metadata = map[string]string{}
	}
	addHandleMetadata(session.Metadata, handle)
	f.sessions[session.ID] = session
	return session, nil
}

func (f *fakeSandboxRuntime) ResolveSession(_ context.Context, _, sessionID string) (sandboxSession, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureLocked()
	if f.resolveErr != nil {
		return sandboxSession{}, f.resolveErr
	}
	session, ok := f.sessions[sessionID]
	if !ok {
		return sandboxSession{}, fmt.Errorf("plugin runtime session %q not found", sessionID)
	}
	session.Handle.Ready = true
	if !session.PluginStarted && f.leases[pluginStartLeaseName(session.Handle)] != "" {
		session.PluginStarting = true
	}
	return session, nil
}

func (f *fakeSandboxRuntime) ListSessions(context.Context, string) ([]sandboxSession, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureLocked()
	if f.listErr != nil {
		return nil, f.listErr
	}
	out := make([]sandboxSession, 0, len(f.sessions))
	for _, session := range f.sessions {
		session.Handle.Ready = true
		if !session.PluginStarted && f.leases[pluginStartLeaseName(session.Handle)] != "" {
			session.PluginStarting = true
		}
		out = append(out, session)
	}
	return out, nil
}

func (f *fakeSandboxRuntime) Stop(_ context.Context, handle sandboxHandle) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureLocked()
	if f.stopFailures > 0 {
		f.stopFailures--
		return status.Error(codes.Unavailable, "injected stop failure")
	}
	f.stopped = append(f.stopped, handle)
	delete(f.sessions, handle.Name)
	delete(f.leases, pluginStartLeaseName(handle))
	return nil
}

func (f *fakeSandboxRuntime) Exec(_ context.Context, handle sandboxHandle, command []string, stdin io.Reader) error {
	if slices.Equal(command, pluginHealthCommand()) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.ensureLocked()
		f.healthCheckExecs++
		if f.failHealthChecks {
			return status.Error(codes.Unavailable, "injected plugin health failure")
		}
		session, ok := f.sessions[handle.Name]
		if !ok || !session.PluginStarted {
			return status.Error(codes.Unavailable, "plugin is not running")
		}
		return nil
	}
	var stdinText string
	if stdin != nil {
		data, err := io.ReadAll(stdin)
		if err != nil {
			return err
		}
		stdinText = string(data)
	}
	f.mu.Lock()
	f.execCalls = append(f.execCalls, execCall{
		handle:  handle,
		command: slices.Clone(command),
		stdin:   stdinText,
	})
	var shouldBlock bool
	if f.blockExec != nil {
		f.blockOnce.Do(func() {
			shouldBlock = true
			if f.execEntered != nil {
				close(f.execEntered)
			}
		})
	}
	f.mu.Unlock()
	if shouldBlock {
		<-f.blockExec
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.execErrors) > 0 {
		err := f.execErrors[0]
		f.execErrors = f.execErrors[1:]
		return err
	}
	return nil
}

func (f *fakeSandboxRuntime) ForwardPort(_ context.Context, _ sandboxHandle, remotePort int) (tunnel, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forwardPortErr != nil {
		return nil, f.forwardPortErr
	}
	f.forwardPorts = append(f.forwardPorts, remotePort)
	return f.tunnel, nil
}

func (f *fakeSandboxRuntime) PodIPDialTarget(_ context.Context, _ sandboxHandle, remotePort int) (tunnel, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.podIPDialErr != nil {
		return nil, f.podIPDialErr
	}
	f.podIPTargets = append(f.podIPTargets, remotePort)
	return f.tunnel, nil
}

func (f *fakeSandboxRuntime) ServiceDNSDialTarget(_ context.Context, _ sandboxHandle, remotePort int) (tunnel, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.serviceDNSDialErr != nil {
		return nil, f.serviceDNSDialErr
	}
	f.serviceDNSTargets = append(f.serviceDNSTargets, remotePort)
	return f.tunnel, nil
}

func (f *fakeSandboxRuntime) EnsureHostnameEgressPolicy(_ context.Context, handle sandboxHandle, cfg hostnameEgressConfig) (string, error) {
	if f.hostnameEgressErr != nil {
		return "", f.hostnameEgressErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	name := handle.Name + "-egress"
	f.hostnamePolicies = append(f.hostnamePolicies, hostnamePolicyCall{
		handle: handle,
		name:   name,
		config: cfg,
	})
	return name, nil
}

func (f *fakeSandboxRuntime) DeleteHostnameEgressPolicy(_ context.Context, _ sandboxHandle, policyName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletedHostnamePolicies = append(f.deletedHostnamePolicies, policyName)
	return nil
}

func (f *fakeSandboxRuntime) AcquirePluginStartLease(_ context.Context, handle sandboxHandle, holder string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureLocked()
	if session, ok := f.sessions[handle.Name]; ok && session.PluginStarted {
		return errPluginAlreadyStarted
	}
	name := pluginStartLeaseName(handle)
	if f.leases[name] != "" {
		return errPluginAlreadyStarted
	}
	f.leases[name] = holder
	return nil
}

func (f *fakeSandboxRuntime) ReleasePluginStartLease(_ context.Context, handle sandboxHandle, holder string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureLocked()
	name := pluginStartLeaseName(handle)
	if f.leases[name] == holder {
		delete(f.leases, name)
	}
	return nil
}

func (f *fakeSandboxRuntime) MarkPluginStarted(_ context.Context, handle sandboxHandle, holder, pluginName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureLocked()
	if f.leases[pluginStartLeaseName(handle)] != holder {
		return errPluginAlreadyStarted
	}
	session, ok := f.sessions[handle.Name]
	if !ok {
		return status.Error(codes.NotFound, "sandbox stopped")
	}
	if session.PluginStarted {
		return errPluginAlreadyStarted
	}
	session.PluginStarted = true
	session.PluginName = pluginName
	f.sessions[handle.Name] = session
	return nil
}

func (f *fakeSandboxRuntime) VerifySessionCompatible(context.Context, sandboxSession) error {
	return f.verifyErr
}

func (f *fakeSandboxRuntime) Close() error {
	return nil
}

func (f *fakeSandboxRuntime) ensureLocked() {
	if f.sessions == nil {
		f.sessions = make(map[string]sandboxSession)
	}
	if f.leases == nil {
		f.leases = make(map[string]string)
	}
}

type fakeTunnel struct {
	mu         sync.Mutex
	dialTarget string
	closed     bool
}

func (t *fakeTunnel) DialTarget() string {
	return t.dialTarget
}

func (t *fakeTunnel) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return nil
}

func (t *fakeTunnel) Closed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

func startPluginLifecycleServer(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen plugin lifecycle server: %v", err)
	}
	server := grpc.NewServer()
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "gestalt.provider.v1.ProviderLifecycle",
		HandlerType: (*interface{})(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "GetProviderIdentity",
			Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
				var req emptypb.Empty
				if err := dec(&req); err != nil {
					return nil, err
				}
				if interceptor == nil {
					return &emptypb.Empty{}, nil
				}
				info := &grpc.UnaryServerInfo{
					Server:     srv,
					FullMethod: "/gestalt.provider.v1.ProviderLifecycle/GetProviderIdentity",
				}
				return interceptor(ctx, &req, info, func(context.Context, interface{}) (interface{}, error) {
					return &emptypb.Empty{}, nil
				})
			},
		}},
	}, struct{}{})
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Logf("plugin lifecycle server stopped: %v", err)
		}
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return tcpDialTarget("127.0.0.1", listener.Addr().(*net.TCPAddr).Port)
}

func startRuntimeProviderServer(t *testing.T, provider *Provider) *Provider {
	t.Helper()
	return provider
}
