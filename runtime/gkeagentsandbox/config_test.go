package gkeagentsandbox

import (
	"testing"
	"time"
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
