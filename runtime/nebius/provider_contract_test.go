package nebius

import (
	"context"
	"net"
	"testing"

	"github.com/nebius/gosdk"
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
		name: "nebius",
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

func TestRuntimeProviderContractPassesHostServiceEnv(t *testing.T) {
	t.Parallel()

	const hostServiceEnv = "HOST_SERVICE_ENDPOINT"
	if !isHostServiceEnvVar(hostServiceEnv) {
		t.Fatalf("%s should be accepted as a relay host service env", hostServiceEnv)
	}
	env := buildPluginEnv(&proto.StartHostedPluginRequest{
		Env: map[string]string{
			"CUSTOM": "value",
		},
	}, map[string]string{
		hostServiceEnv: "tls://host-service-relay.gestalt.example:443",
	}, "tcp://127.0.0.1:50051")

	if got, want := env[hostServiceEnv], "tls://host-service-relay.gestalt.example:443"; got != want {
		t.Fatalf("host service socket env = %q, want %q", got, want)
	}
	if got, want := env[proto.EnvProviderSocket], "tcp://127.0.0.1:50051"; got != want {
		t.Fatalf("provider socket env = %q, want %q", got, want)
	}
	if got, want := env["CUSTOM"], "value"; got != want {
		t.Fatalf("custom env = %q, want %q", got, want)
	}
}

func TestRuntimeProviderContractRejectsInvalidRelayBindingEnv(t *testing.T) {
	t.Parallel()

	provider := &Provider{
		name: "nebius",
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
		EnvVar:    "NOT-A-SOCKET",
		Relay: &proto.PluginRuntimeHostServiceRelay{
			DialTarget: "tls://gestaltd.example.test:443",
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("BindHostService code = %v, want InvalidArgument: %v", status.Code(err), err)
	}
}

func TestRuntimeProviderContractListsSessions(t *testing.T) {
	t.Parallel()

	provider := &Provider{
		name: "nebius",
		sdk:  &gosdk.SDK{},
		cfg:  Config{SubnetID: "subnet-1"},
		sessions: map[string]*session{
			"session-b": {
				id:       "session-b",
				state:    sessionStateReady,
				metadata: map[string]string{"tenant": "beta"},
			},
			"session-a": {
				id:       "session-a",
				state:    sessionStateReady,
				metadata: map[string]string{"tenant": "alpha"},
			},
		},
	}
	client := startRuntimeProviderServer(t, provider)

	resp, err := client.ListSessions(context.Background(), &proto.ListPluginRuntimeSessionsRequest{})
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	sessions := resp.GetSessions()
	if len(sessions) != 2 {
		t.Fatalf("ListSessions len = %d, want 2", len(sessions))
	}
	if got, want := sessions[0].GetId(), "session-a"; got != want {
		t.Fatalf("first session id = %q, want %q", got, want)
	}
	if got, want := sessions[1].GetId(), "session-b"; got != want {
		t.Fatalf("second session id = %q, want %q", got, want)
	}
	if got, want := sessions[0].GetMetadata()["tenant"], "alpha"; got != want {
		t.Fatalf("first session tenant = %q, want %q", got, want)
	}
	if got, want := sessions[1].GetMetadata()["tenant"], "beta"; got != want {
		t.Fatalf("second session tenant = %q, want %q", got, want)
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
