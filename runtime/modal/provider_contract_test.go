package modal

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestRuntimeProviderContractAcceptsAgentHostRelayBinding(t *testing.T) {
	t.Parallel()

	provider := &Provider{
		name: "modal",
		sessions: map[string]*session{
			"session-1": {
				id:       "session-1",
				state:    sessionStateReady,
				metadata: map[string]string{"provider_kind": "agent"},
				bindings: map[string]string{},
			},
		},
	}
	client := startRuntimeProviderServer(t, provider)

	resp, err := client.BindHostService(context.Background(), &proto.BindPluginRuntimeHostServiceRequest{
		SessionId: "session-1",
		EnvVar:    gestalt.EnvAgentHostSocket,
		Relay: &proto.PluginRuntimeHostServiceRelay{
			DialTarget: "tls://gestaltd.example.test:443",
		},
	})
	if err != nil {
		t.Fatalf("BindHostService: %v", err)
	}
	if got, want := resp.GetEnvVar(), gestalt.EnvAgentHostSocket; got != want {
		t.Fatalf("BindHostService env = %q, want %q", got, want)
	}
	if got, want := resp.GetRelay().GetDialTarget(), "tls://gestaltd.example.test:443"; got != want {
		t.Fatalf("BindHostService relay = %q, want %q", got, want)
	}

	provider.mu.Lock()
	got := provider.sessions["session-1"].bindings[gestalt.EnvAgentHostSocket]
	provider.mu.Unlock()
	if got != "tls://gestaltd.example.test:443" {
		t.Fatalf("stored binding = %q, want tls://gestaltd.example.test:443", got)
	}
}

func TestRuntimeProviderContractRejectsUnknownRelayBinding(t *testing.T) {
	t.Parallel()

	provider := &Provider{
		name: "modal",
		sessions: map[string]*session{
			"session-1": {
				id:    "session-1",
				state: sessionStateReady,
			},
		},
	}
	client := startRuntimeProviderServer(t, provider)

	_, err := client.BindHostService(context.Background(), &proto.BindPluginRuntimeHostServiceRequest{
		SessionId: "session-1",
		EnvVar:    "GESTALT_UNKNOWN_SOCKET",
		Relay: &proto.PluginRuntimeHostServiceRelay{
			DialTarget: "tls://gestaltd.example.test:443",
		},
	})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("BindHostService code = %v, want Unimplemented: %v", status.Code(err), err)
	}
}

func TestRuntimeProviderContractRelayOnlyAgentHostLaunchSkipsHostnameProxy(t *testing.T) {
	t.Parallel()

	params, err := buildSandboxCreateParams(context.Background(), Config{}, &proto.StartHostedPluginRequest{
		PluginName:   "agent-provider",
		AllowedHosts: []string{"agent-relay.gestalt.example"},
	}, "session-1", map[string]string{
		gestalt.EnvAgentHostSocket: "tls://agent-relay.gestalt.example:7443",
	})
	if err != nil {
		t.Fatalf("buildSandboxCreateParams: %v", err)
	}
	if len(params.CIDRAllowlist) != 0 {
		t.Fatalf("CIDRAllowlist = %v, want none for relay-only agent host launch", params.CIDRAllowlist)
	}
}

func TestRuntimeProviderContractNonRelayAllowedHostStillRequiresProxy(t *testing.T) {
	t.Parallel()

	_, err := buildSandboxCreateParams(context.Background(), Config{}, &proto.StartHostedPluginRequest{
		PluginName:   "agent-provider",
		AllowedHosts: []string{"api.github.com"},
	}, "session-1", map[string]string{
		gestalt.EnvAgentHostSocket: "tls://agent-relay.gestalt.example:7443",
	})
	if err == nil || !strings.Contains(err.Error(), "HTTP_PROXY or HTTPS_PROXY is required") {
		t.Fatalf("buildSandboxCreateParams error = %v, want missing proxy precondition", err)
	}
}

func TestNewProviderIDsAreBootUnique(t *testing.T) {
	t.Parallel()

	first := New().newID("session")
	second := New().newID("session")

	if first == second {
		t.Fatalf("first session id = %q, second = %q; want boot-unique ids", first, second)
	}
	if !strings.HasPrefix(first, "session-") {
		t.Fatalf("first session id = %q, want session- prefix", first)
	}
	if !strings.HasPrefix(second, "session-") {
		t.Fatalf("second session id = %q, want session- prefix", second)
	}
}

func TestRuntimeProviderContractReturnsSessionDiagnostics(t *testing.T) {
	t.Parallel()

	logs := newSessionLogBuffer(0)
	logTime := time.Date(2026, time.April, 23, 21, 30, 0, 0, time.UTC)
	logs.add(proto.PluginRuntimeLogStream_PLUGIN_RUNTIME_LOG_STREAM_STDERR, "Traceback: boom", logTime)
	provider := &Provider{
		name: "modal",
		sessions: map[string]*session{
			"session-1": {
				id:    "session-1",
				state: sessionStateFailed,
				logs:  logs,
				metadata: map[string]string{
					"plugin": "agent",
				},
			},
		},
	}
	client := startRuntimeProviderServer(t, provider)

	resp, err := client.GetSessionDiagnostics(context.Background(), &proto.GetPluginRuntimeSessionDiagnosticsRequest{
		SessionId: "session-1",
	})
	if err != nil {
		t.Fatalf("GetSessionDiagnostics: %v", err)
	}
	if got := resp.GetSession().GetId(); got != "session-1" {
		t.Fatalf("GetSessionDiagnostics session id = %q, want session-1", got)
	}
	if len(resp.GetLogs()) != 1 {
		t.Fatalf("GetSessionDiagnostics logs len = %d, want 1", len(resp.GetLogs()))
	}
	if got := resp.GetLogs()[0].GetStream(); got != proto.PluginRuntimeLogStream_PLUGIN_RUNTIME_LOG_STREAM_STDERR {
		t.Fatalf("GetSessionDiagnostics stream = %v, want stderr", got)
	}
	if got := resp.GetLogs()[0].GetMessage(); got != "Traceback: boom" {
		t.Fatalf("GetSessionDiagnostics message = %q, want Traceback: boom", got)
	}
	if got := resp.GetLogs()[0].GetObservedAt().AsTime(); !got.Equal(logTime) {
		t.Fatalf("GetSessionDiagnostics observed_at = %s, want %s", got, logTime)
	}
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
