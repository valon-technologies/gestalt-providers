package nebius

import (
	"context"
	"testing"

	"github.com/nebius/gosdk"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func TestRuntimeProviderContractPassesHostServiceEnv(t *testing.T) {
	t.Parallel()

	const hostServiceEnv = gestalt.EnvAgentHostSocket
	if !isHostServiceEnvVar(hostServiceEnv) {
		t.Fatalf("%s should be accepted as a relay host service env", hostServiceEnv)
	}
	env := buildPluginEnv(gestalt.StartHostedPluginRequest{
		Env: map[string]string{
			"CUSTOM":       "value",
			hostServiceEnv: "tls://host-service-relay.gestalt.example:443",
		},
	}, "tcp://127.0.0.1:50051")

	if got, want := env[hostServiceEnv], "tls://host-service-relay.gestalt.example:443"; got != want {
		t.Fatalf("host service socket env = %q, want %q", got, want)
	}
	if got, want := env[envProviderSocket], "tcp://127.0.0.1:50051"; got != want {
		t.Fatalf("provider socket env = %q, want %q", got, want)
	}
	if got, want := env["CUSTOM"], "value"; got != want {
		t.Fatalf("custom env = %q, want %q", got, want)
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

func startRuntimeProviderServer(t *testing.T, provider *Provider) *Provider {
	t.Helper()
	return provider
}
