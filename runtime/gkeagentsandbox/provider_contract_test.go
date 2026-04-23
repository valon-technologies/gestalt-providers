package gkeagentsandbox

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
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
			Template:            "python-runtime",
			PluginPort:          50051,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime:  fake,
		sessions: map[string]*session{},
	})

	ctx := context.Background()
	pluginName := "github_plugin.with-a-very-long-name-that-preserves-session-suffix"
	support, err := client.GetSupport(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetSupport: %v", err)
	}
	if !support.GetCanHostPlugins() {
		t.Fatalf("GetSupport CanHostPlugins = false")
	}
	if got, want := support.GetEgressMode(), proto.PluginRuntimeEgressMode_PLUGIN_RUNTIME_EGRESS_MODE_HOSTNAME; got != want {
		t.Fatalf("GetSupport EgressMode = %v, want %v", got, want)
	}

	session, err := client.StartSession(ctx, &proto.StartPluginRuntimeSessionRequest{
		PluginName: pluginName,
		Metadata: map[string]string{
			"tenant": "dev",
		},
	})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if got, want := session.GetState(), sessionStateReady; got != want {
		t.Fatalf("StartSession state = %q, want %q", got, want)
	}
	if got, want := session.GetMetadata()["kubernetes.namespace"], "runtime-system"; got != want {
		t.Fatalf("StartSession metadata kubernetes.namespace = %q, want %q", got, want)
	}
	if got := session.GetMetadata()["kubernetes.sandboxClaim"]; got == "" {
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
	if !strings.HasSuffix(startRequests[0].Name, session.GetId()) {
		t.Fatalf("runtime Start resource name = %q, want suffix %q", startRequests[0].Name, session.GetId())
	}
	if got, want := startRequests[0].Metadata["tenant"], "dev"; got != want {
		t.Fatalf("runtime Start metadata tenant = %q, want %q", got, want)
	}

	binding, err := client.BindHostService(ctx, &proto.BindPluginRuntimeHostServiceRequest{
		SessionId: session.GetId(),
		EnvVar:    "GESTALT_CACHE_SOCKET",
		Relay: &proto.PluginRuntimeHostServiceRelay{
			DialTarget: "https://gestaltd.example.internal/runtime/session/relay",
		},
	})
	if err != nil {
		t.Fatalf("BindHostService: %v", err)
	}
	if got, want := binding.GetRelay().GetDialTarget(), "https://gestaltd.example.internal/runtime/session/relay"; got != want {
		t.Fatalf("BindHostService relay = %q, want %q", got, want)
	}

	bundleDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(bundleDir, "plugin"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("write plugin bundle: %v", err)
	}
	hosted, err := client.StartPlugin(ctx, &proto.StartHostedPluginRequest{
		SessionId:  session.GetId(),
		PluginName: pluginName,
		Command:    "./plugin",
		Args:       []string{"--serve", "space value"},
		Env: map[string]string{
			"CUSTOM": "value",
		},
		BundleDir: bundleDir,
	})
	if err != nil {
		t.Fatalf("StartPlugin: %v", err)
	}
	if got, want := hosted.GetDialTarget(), pluginTarget; got != want {
		t.Fatalf("StartPlugin dial target = %q, want %q", got, want)
	}

	fake.mu.Lock()
	copyCalls := slices.Clone(fake.copyCalls)
	execCalls := slices.Clone(fake.execCalls)
	forwardPorts := slices.Clone(fake.forwardPorts)
	fake.mu.Unlock()
	if len(copyCalls) != 1 {
		t.Fatalf("runtime CopyBundle calls = %d, want 1", len(copyCalls))
	}
	if got, want := copyCalls[0].localDir, bundleDir; got != want {
		t.Fatalf("runtime CopyBundle localDir = %q, want %q", got, want)
	}
	if got, want := copyCalls[0].remoteDir, remoteBundleRoot; got != want {
		t.Fatalf("runtime CopyBundle remoteDir = %q, want %q", got, want)
	}
	if len(execCalls) != 1 {
		t.Fatalf("runtime Exec calls = %d, want 1", len(execCalls))
	}
	if !slices.Equal(execCalls[0].command[:2], []string{"sh", "-c"}) {
		t.Fatalf("runtime Exec command = %#v, want sh -c", execCalls[0].command)
	}
	launchScript := execCalls[0].command[2]
	for _, want := range []string{
		"command -v socat",
		"TCP-LISTEN:50051",
		"UNIX-CONNECT:'/tmp/gestalt/plugin.sock'",
		"cd '/workspace/plugin'",
		"'GESTALT_PLUGIN_SOCKET=/tmp/gestalt/plugin.sock'",
		"'GESTALT_CACHE_SOCKET=https://gestaltd.example.internal/runtime/session/relay'",
		"'CUSTOM=value'",
		"'./plugin' '--serve' 'space value'",
	} {
		if !strings.Contains(launchScript, want) {
			t.Fatalf("launch script missing %q:\n%s", want, launchScript)
		}
	}
	if !slices.Equal(forwardPorts, []int{50051}) {
		t.Fatalf("runtime ForwardPort calls = %#v, want [50051]", forwardPorts)
	}

	running, err := client.GetSession(ctx, &proto.GetPluginRuntimeSessionRequest{SessionId: session.GetId()})
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if got, want := running.GetState(), sessionStateRunning; got != want {
		t.Fatalf("GetSession state = %q, want %q", got, want)
	}

	if _, err := client.StopSession(ctx, &proto.StopPluginRuntimeSessionRequest{SessionId: session.GetId()}); err != nil {
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
			Template:            "python-runtime",
			PluginPort:          50051,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime:  fake,
		sessions: map[string]*session{},
	})

	ctx := context.Background()
	session, err := client.StartSession(ctx, &proto.StartPluginRuntimeSessionRequest{
		PluginName: "agent-provider",
	})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	for _, req := range []*proto.BindPluginRuntimeHostServiceRequest{
		{
			SessionId: session.GetId(),
			EnvVar:    proto.EnvAgentHostSocket,
			Relay: &proto.PluginRuntimeHostServiceRelay{
				DialTarget: "tls://agent-relay.gestalt.example:7443",
			},
		},
		{
			SessionId: session.GetId(),
			EnvVar:    proto.EnvAgentManagerSocket,
			Relay: &proto.PluginRuntimeHostServiceRelay{
				DialTarget: "tls://manager-relay.gestalt.example:443",
			},
		},
	} {
		if _, err := client.BindHostService(ctx, req); err != nil {
			t.Fatalf("BindHostService %s: %v", req.GetEnvVar(), err)
		}
	}

	bundleDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(bundleDir, "plugin"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("write plugin bundle: %v", err)
	}
	if _, err := client.StartPlugin(ctx, &proto.StartHostedPluginRequest{
		SessionId:     session.GetId(),
		PluginName:    "agent-provider",
		Command:       "./plugin",
		BundleDir:     bundleDir,
		AllowedHosts:  []string{"api.github.com"},
		DefaultAction: "deny",
		Env: map[string]string{
			"HTTPS_PROXY":                          "https://proxy.gestalt.example:9443",
			proto.EnvAgentHostSocket + "_TOKEN":    "agent-host-token",
			proto.EnvAgentManagerSocket + "_TOKEN": "agent-manager-token",
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
		{Host: "agent-relay.gestalt.example", Port: 7443},
		{Host: "manager-relay.gestalt.example", Port: 443},
	} {
		if !slices.Contains(policies[0].config.Endpoints, want) {
			t.Fatalf("hostname egress endpoints = %#v, want %#v", policies[0].config.Endpoints, want)
		}
	}
	if len(execCalls) != 1 {
		t.Fatalf("runtime Exec calls = %d, want 1", len(execCalls))
	}
	launchScript := execCalls[0].command[2]
	for _, want := range []string{
		"'GESTALT_AGENT_HOST_SOCKET=tls://agent-relay.gestalt.example:7443'",
		"'GESTALT_AGENT_HOST_SOCKET_TOKEN=agent-host-token'",
		"'GESTALT_AGENT_MANAGER_SOCKET=tls://manager-relay.gestalt.example:443'",
		"'GESTALT_AGENT_MANAGER_SOCKET_TOKEN=agent-manager-token'",
	} {
		if !strings.Contains(launchScript, want) {
			t.Fatalf("launch script missing %q:\n%s", want, launchScript)
		}
	}

	if _, err := client.StopSession(ctx, &proto.StopPluginRuntimeSessionRequest{SessionId: session.GetId()}); err != nil {
		t.Fatalf("StopSession: %v", err)
	}
	fake.mu.Lock()
	deletedPolicies := append([]string(nil), fake.deletedHostnamePolicies...)
	fake.mu.Unlock()
	if got, want := deletedPolicies, []string{policies[0].name}; !slices.Equal(got, want) {
		t.Fatalf("deleted hostname egress policies = %#v, want %#v", got, want)
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
			Template:            "python-runtime",
			PluginPort:          50051,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime:  fake,
		sessions: map[string]*session{},
	})

	ctx := context.Background()
	session, err := client.StartSession(ctx, &proto.StartPluginRuntimeSessionRequest{PluginName: "github"})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	_, err = client.StartPlugin(ctx, &proto.StartHostedPluginRequest{
		SessionId:    session.GetId(),
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
			Template:            "python-runtime",
			PluginPort:          50051,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime:  fake,
		sessions: map[string]*session{},
	})

	ctx := context.Background()
	session, err := client.StartSession(ctx, &proto.StartPluginRuntimeSessionRequest{
		PluginName: "agent-provider",
	})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if _, err := client.BindHostService(ctx, &proto.BindPluginRuntimeHostServiceRequest{
		SessionId: session.GetId(),
		EnvVar:    proto.EnvAgentHostSocket,
		Relay: &proto.PluginRuntimeHostServiceRelay{
			DialTarget: "tls://agent-relay.gestalt.example:7443",
		},
	}); err != nil {
		t.Fatalf("BindHostService agent host: %v", err)
	}

	bundleDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(bundleDir, "plugin"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("write plugin bundle: %v", err)
	}
	if _, err := client.StartPlugin(ctx, &proto.StartHostedPluginRequest{
		SessionId:  session.GetId(),
		PluginName: "agent-provider",
		Command:    "./plugin",
		BundleDir:  bundleDir,
		AllowedHosts: []string{
			"agent-relay.gestalt.example",
		},
		Env: map[string]string{
			proto.EnvAgentHostSocket + "_TOKEN": "agent-host-token",
		},
	}); err != nil {
		t.Fatalf("StartPlugin: %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.hostnamePolicies) != 0 {
		t.Fatalf("hostname egress policies = %d, want 0 for relay-only launch", len(fake.hostnamePolicies))
	}
	if len(fake.execCalls) != 1 {
		t.Fatalf("runtime Exec calls = %d, want 1", len(fake.execCalls))
	}
	launchScript := fake.execCalls[0].command[2]
	for _, want := range []string{
		"'GESTALT_AGENT_HOST_SOCKET=tls://agent-relay.gestalt.example:7443'",
		"'GESTALT_AGENT_HOST_SOCKET_TOKEN=agent-host-token'",
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
			status.Error(codes.Unavailable, "cleanup failed"),
		},
		forwardPortErr: status.Error(codes.Unavailable, "port-forward failed"),
	}
	client := startRuntimeProviderServer(t, &Provider{
		name: "gkeAgentSandbox",
		cfg: Config{
			Namespace:           "runtime-system",
			Template:            "python-runtime",
			PluginPort:          50051,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime:  fake,
		sessions: map[string]*session{},
	})

	ctx := context.Background()
	session, err := client.StartSession(ctx, &proto.StartPluginRuntimeSessionRequest{PluginName: "agent-provider"})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	bundleDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(bundleDir, "plugin"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("write plugin bundle: %v", err)
	}
	_, err = client.StartPlugin(ctx, &proto.StartHostedPluginRequest{
		SessionId:     session.GetId(),
		PluginName:    "agent-provider",
		Command:       "./plugin",
		BundleDir:     bundleDir,
		AllowedHosts:  []string{"api.github.com"},
		DefaultAction: "deny",
		Env: map[string]string{
			"HTTPS_PROXY": "https://proxy.gestalt.example:9443",
		},
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("StartPlugin code = %v, want Internal: %v", status.Code(err), err)
	}

	fake.mu.Lock()
	if len(fake.deletedHostnamePolicies) != 0 {
		fake.mu.Unlock()
		t.Fatalf("deleted hostname egress policies = %#v, want none after failed cleanup", fake.deletedHostnamePolicies)
	}
	fake.mu.Unlock()

	running, err := client.GetSession(ctx, &proto.GetPluginRuntimeSessionRequest{SessionId: session.GetId()})
	if err != nil {
		t.Fatalf("GetSession after failed StartPlugin: %v", err)
	}
	if got, want := running.GetState(), sessionStateReady; got != want {
		t.Fatalf("session state after failed StartPlugin = %q, want %q", got, want)
	}

	if _, err := client.StopSession(ctx, &proto.StopPluginRuntimeSessionRequest{SessionId: session.GetId()}); err != nil {
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
		sessions: map[string]*session{},
	})

	_, err := client.StartSession(context.Background(), &proto.StartPluginRuntimeSessionRequest{
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
	client := startRuntimeProviderServer(t, &Provider{
		name: "gkeAgentSandbox",
		cfg: Config{
			Namespace:           "runtime-system",
			Template:            "python-runtime",
			PluginPort:          50051,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime:  fake,
		sessions: map[string]*session{},
	})

	ctx := context.Background()
	session, err := client.StartSession(ctx, &proto.StartPluginRuntimeSessionRequest{PluginName: "github"})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}

	firstErr := make(chan error, 1)
	go func() {
		_, err := client.StartPlugin(ctx, &proto.StartHostedPluginRequest{
			SessionId:  session.GetId(),
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

	_, err = client.StartPlugin(ctx, &proto.StartHostedPluginRequest{
		SessionId:  session.GetId(),
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

func TestRuntimeProviderContractKeepsFailedStateStickyAfterPluginDeath(t *testing.T) {
	t.Parallel()

	pluginTarget := startPluginLifecycleServer(t)
	fake := &fakeSandboxRuntime{
		tunnel:           &fakeTunnel{dialTarget: pluginTarget},
		failHealthChecks: true,
	}
	client := startRuntimeProviderServer(t, &Provider{
		name: "gkeAgentSandbox",
		cfg: Config{
			Namespace:           "runtime-system",
			Template:            "python-runtime",
			PluginPort:          50051,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime:  fake,
		sessions: map[string]*session{},
	})

	ctx := context.Background()
	session, err := client.StartSession(ctx, &proto.StartPluginRuntimeSessionRequest{PluginName: "github"})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if _, err := client.StartPlugin(ctx, &proto.StartHostedPluginRequest{
		SessionId:  session.GetId(),
		PluginName: "github",
		Command:    "./plugin",
	}); err != nil {
		t.Fatalf("StartPlugin: %v", err)
	}

	failed, err := client.GetSession(ctx, &proto.GetPluginRuntimeSessionRequest{SessionId: session.GetId()})
	if err != nil {
		t.Fatalf("GetSession after health failure: %v", err)
	}
	if got, want := failed.GetState(), sessionStateFailed; got != want {
		t.Fatalf("GetSession state after health failure = %q, want %q", got, want)
	}
	stillFailed, err := client.GetSession(ctx, &proto.GetPluginRuntimeSessionRequest{SessionId: session.GetId()})
	if err != nil {
		t.Fatalf("GetSession after sticky failure: %v", err)
	}
	if got, want := stillFailed.GetState(), sessionStateFailed; got != want {
		t.Fatalf("GetSession state after second refresh = %q, want %q", got, want)
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
			Template:            "python-runtime",
			PluginPort:          50051,
			SandboxReadyTimeout: 2 * time.Second,
			PluginReadyTimeout:  2 * time.Second,
			ExecTimeout:         2 * time.Second,
			CleanupTimeout:      2 * time.Second,
		},
		runtime:  fake,
		sessions: map[string]*session{},
	})

	ctx := context.Background()
	session, err := client.StartSession(ctx, &proto.StartPluginRuntimeSessionRequest{PluginName: "github"})
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	bundleDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(bundleDir, "plugin"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("write plugin bundle: %v", err)
	}
	if _, err := client.StartPlugin(ctx, &proto.StartHostedPluginRequest{
		SessionId:     session.GetId(),
		PluginName:    "github",
		Command:       "./plugin",
		BundleDir:     bundleDir,
		AllowedHosts:  []string{"api.github.com"},
		DefaultAction: "deny",
		Env: map[string]string{
			"HTTPS_PROXY": "https://proxy.gestalt.example:9443",
		},
	}); err != nil {
		t.Fatalf("StartPlugin: %v", err)
	}

	if _, err := client.StopSession(ctx, &proto.StopPluginRuntimeSessionRequest{SessionId: session.GetId()}); status.Code(err) != codes.Internal {
		t.Fatalf("first StopSession code = %v, want Internal: %v", status.Code(err), err)
	}
	fake.mu.Lock()
	if len(fake.deletedHostnamePolicies) != 0 {
		fake.mu.Unlock()
		t.Fatalf("deleted hostname egress policies after failed StopSession = %#v, want none", fake.deletedHostnamePolicies)
	}
	fake.mu.Unlock()
	stillPresent, err := client.GetSession(ctx, &proto.GetPluginRuntimeSessionRequest{SessionId: session.GetId()})
	if err != nil {
		t.Fatalf("GetSession after failed StopSession: %v", err)
	}
	if got, want := stillPresent.GetState(), sessionStateRunning; got != want {
		t.Fatalf("session state after failed StopSession = %q, want %q", got, want)
	}
	if _, err := client.StopSession(ctx, &proto.StopPluginRuntimeSessionRequest{SessionId: session.GetId()}); err != nil {
		t.Fatalf("retry StopSession: %v", err)
	}
	fake.mu.Lock()
	if len(fake.deletedHostnamePolicies) != 1 {
		fake.mu.Unlock()
		t.Fatalf("deleted hostname egress policies after successful retry = %#v, want one delete", fake.deletedHostnamePolicies)
	}
	fake.mu.Unlock()
	if _, err := client.GetSession(ctx, &proto.GetPluginRuntimeSessionRequest{SessionId: session.GetId()}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetSession after successful StopSession code = %v, want NotFound: %v", status.Code(err), err)
	}
}

type fakeSandboxRuntime struct {
	mu sync.Mutex

	startRequests           []startSandboxRequest
	copyCalls               []copyBundleCall
	execCalls               []execCall
	forwardPorts            []int
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
}

type copyBundleCall struct {
	handle    sandboxHandle
	localDir  string
	remoteDir string
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

func (f *fakeSandboxRuntime) Start(_ context.Context, req startSandboxRequest) (sandboxHandle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.startRequests = append(f.startRequests, req)
	mode := "direct"
	claimName := ""
	if req.Template != "" {
		mode = "claim"
		claimName = req.Name
	}
	return sandboxHandle{
		Name:        req.Name,
		Namespace:   req.Namespace,
		Mode:        mode,
		ClaimName:   claimName,
		SandboxName: req.Name + "-sandbox",
		PodName:     req.Name + "-pod",
		Ready:       true,
	}, nil
}

func (f *fakeSandboxRuntime) Get(_ context.Context, handle sandboxHandle) (sandboxHandle, error) {
	handle.Ready = true
	return handle, nil
}

func (f *fakeSandboxRuntime) Stop(_ context.Context, handle sandboxHandle) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopFailures > 0 {
		f.stopFailures--
		return status.Error(codes.Unavailable, "injected stop failure")
	}
	f.stopped = append(f.stopped, handle)
	return nil
}

func (f *fakeSandboxRuntime) CopyBundle(_ context.Context, handle sandboxHandle, localDir, remoteDir string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.copyCalls = append(f.copyCalls, copyBundleCall{handle: handle, localDir: localDir, remoteDir: remoteDir})
	return nil
}

func (f *fakeSandboxRuntime) Exec(_ context.Context, handle sandboxHandle, command []string, stdin io.Reader) error {
	if slices.Equal(command, pluginHealthCommand()) && f.failHealthChecks {
		return status.Error(codes.Unavailable, "injected plugin health failure")
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

func (f *fakeSandboxRuntime) Close() error {
	return nil
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

type testPluginLifecycleServer struct {
	proto.UnimplementedProviderLifecycleServer
}

func (testPluginLifecycleServer) GetProviderIdentity(context.Context, *emptypb.Empty) (*proto.ProviderIdentity, error) {
	return &proto.ProviderIdentity{
		Kind:        proto.ProviderKind_PROVIDER_KIND_INTEGRATION,
		Name:        "contract-plugin",
		DisplayName: "Contract Plugin",
		Version:     "0.0.0-test",
	}, nil
}

func startPluginLifecycleServer(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen plugin lifecycle server: %v", err)
	}
	server := grpc.NewServer()
	proto.RegisterProviderLifecycleServer(server, testPluginLifecycleServer{})
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

func startRuntimeProviderServer(t *testing.T, provider *Provider) proto.PluginRuntimeProviderClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	proto.RegisterPluginRuntimeProviderServer(server, provider)
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Logf("runtime provider server stopped: %v", err)
		}
	}()

	conn, err := grpc.NewClient(
		"passthrough:///runtime-provider",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("connect runtime provider server: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
	})
	return proto.NewPluginRuntimeProviderClient(conn)
}
