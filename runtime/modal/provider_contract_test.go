package modal

import (
	"context"
	"net"
	"strings"
	"testing"

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
