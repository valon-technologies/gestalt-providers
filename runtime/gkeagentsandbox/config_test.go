package gkeagentsandbox

import (
	"os"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestDecodeConfigAcceptsDurationStringsAndLockedDurationNumbers(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		raw  map[string]any
	}{
		{
			name: "duration strings",
			raw: map[string]any{
				"namespace":           "gestalt-runtime",
				"sandboxReadyTimeout": "3m",
				"pluginReadyTimeout":  "30s",
				"execTimeout":         "2m",
				"cleanupTimeout":      "30s",
			},
		},
		{
			name: "locked duration numbers",
			raw: map[string]any{
				"namespace":           "gestalt-runtime",
				"sandboxReadyTimeout": int64(3 * time.Minute),
				"pluginReadyTimeout":  int64(30 * time.Second),
				"execTimeout":         int64(2 * time.Minute),
				"cleanupTimeout":      int64(30 * time.Second),
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := decodeConfig(tc.raw)
			if err != nil {
				t.Fatalf("decodeConfig: %v", err)
			}
			if got, want := cfg.SandboxReadyTimeout, 3*time.Minute; got != want {
				t.Fatalf("SandboxReadyTimeout = %s, want %s", got, want)
			}
			if got, want := cfg.PluginReadyTimeout, 30*time.Second; got != want {
				t.Fatalf("PluginReadyTimeout = %s, want %s", got, want)
			}
			if got, want := cfg.ExecTimeout, 2*time.Minute; got != want {
				t.Fatalf("ExecTimeout = %s, want %s", got, want)
			}
			if got, want := cfg.CleanupTimeout, 30*time.Second; got != want {
				t.Fatalf("CleanupTimeout = %s, want %s", got, want)
			}
		})
	}
}

func TestConfigSchemaExposesRuntimeLifecycleFields(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("schemas/config.schema.yaml")
	if err != nil {
		t.Fatalf("read config schema: %v", err)
	}
	var schema struct {
		Properties map[string]any `yaml:"properties"`
	}
	if err := yaml.Unmarshal(data, &schema); err != nil {
		t.Fatalf("decode config schema: %v", err)
	}
	for _, field := range []string{
		"sessionTTL",
		"sessionDrainBefore",
		"warmPool",
		"enforceTemplateImageMatch",
		"staleSessionStartRetries",
	} {
		if _, ok := schema.Properties[field]; !ok {
			t.Fatalf("config schema missing %q", field)
		}
	}
}

func TestDecodeConfigNormalizesConnectionMode(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		raw  map[string]any
		want string
	}{
		{
			name: "default",
			raw:  map[string]any{},
			want: connectionModePortForward,
		},
		{
			name: "port forward alias",
			raw:  map[string]any{"connectionMode": "port-forward"},
			want: connectionModePortForward,
		},
		{
			name: "pod ip",
			raw:  map[string]any{"connectionMode": "podIP"},
			want: connectionModePodIP,
		},
		{
			name: "service dns",
			raw:  map[string]any{"connectionMode": "serviceDNS"},
			want: connectionModeServiceDNS,
		},
		{
			name: "in cluster alias",
			raw:  map[string]any{"connectionMode": "in-cluster"},
			want: connectionModeServiceDNS,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := decodeConfig(tc.raw)
			if err != nil {
				t.Fatalf("decodeConfig: %v", err)
			}
			if got := cfg.ConnectionMode; got != tc.want {
				t.Fatalf("ConnectionMode = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDecodeConfigAcceptsGKEADCConfig(t *testing.T) {
	t.Parallel()

	cfg, err := decodeConfig(map[string]any{
		"namespace":      "gestalt-runtime",
		"connectionMode": "portForward",
		"gke": map[string]any{
			"projectID": "gitlab-peach-street",
			"location":  "us-east4",
			"cluster":   "gestalt-agent-sandbox",
		},
	})
	if err != nil {
		t.Fatalf("decodeConfig: %v", err)
	}
	if got, want := cfg.GKE.ProjectID, "gitlab-peach-street"; got != want {
		t.Fatalf("GKE.ProjectID = %q, want %q", got, want)
	}
	if got, want := cfg.GKE.Location, "us-east4"; got != want {
		t.Fatalf("GKE.Location = %q, want %q", got, want)
	}
	if got, want := cfg.GKE.Cluster, "gestalt-agent-sandbox"; got != want {
		t.Fatalf("GKE.Cluster = %q, want %q", got, want)
	}
	if got, want := cfg.GKE.Endpoint, gkeEndpointPrivate; got != want {
		t.Fatalf("GKE.Endpoint = %q, want %q", got, want)
	}
}

func TestDecodeConfigRejectsInvalidGKEADCConfig(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		raw       map[string]any
		wantError string
	}{
		{
			name: "partial",
			raw: map[string]any{
				"gke": map[string]any{
					"projectID": "gitlab-peach-street",
					"location":  "us-east4",
				},
			},
			wantError: "gke.cluster is required",
		},
		{
			name: "invalid endpoint",
			raw: map[string]any{
				"gke": map[string]any{
					"projectID": "gitlab-peach-street",
					"location":  "us-east4",
					"cluster":   "gestalt-agent-sandbox",
					"endpoint":  "internal",
				},
			},
			wantError: "gke.endpoint must be",
		},
		{
			name: "kubeconfig conflict",
			raw: map[string]any{
				"kubeconfig": "/tmp/kubeconfig",
				"gke": map[string]any{
					"projectID": "gitlab-peach-street",
					"location":  "us-east4",
					"cluster":   "gestalt-agent-sandbox",
				},
			},
			wantError: "cannot be combined",
		},
		{
			name: "context conflict",
			raw: map[string]any{
				"context": "prod",
				"gke": map[string]any{
					"projectID": "gitlab-peach-street",
					"location":  "us-east4",
					"cluster":   "gestalt-agent-sandbox",
				},
			},
			wantError: "cannot be combined",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := decodeConfig(tc.raw)
			if err == nil {
				t.Fatalf("decodeConfig error = nil, want %q", tc.wantError)
			}
			if !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("decodeConfig error = %q, want substring %q", err.Error(), tc.wantError)
			}
		})
	}
}
