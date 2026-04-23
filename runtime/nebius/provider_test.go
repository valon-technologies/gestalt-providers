package nebius

import (
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBeginPluginStartRejectsConcurrentLaunch(t *testing.T) {
	t.Parallel()

	provider := &Provider{
		sessions: map[string]*session{
			"session-1": {
				id:       "session-1",
				state:    sessionStateReady,
				bindings: map[string]string{"GESTALT_AGENT_HOST_SOCKET": "tls://example.test:443"},
				image:    "ghcr.io/example/runtime:latest",
			},
		},
	}

	bindings, image, err := provider.beginPluginStart("session-1")
	if err != nil {
		t.Fatalf("beginPluginStart first call: %v", err)
	}
	if image != "ghcr.io/example/runtime:latest" {
		t.Fatalf("image = %q", image)
	}
	if got := bindings["GESTALT_AGENT_HOST_SOCKET"]; got != "tls://example.test:443" {
		t.Fatalf("bindings env = %q", got)
	}

	_, _, err = provider.beginPluginStart("session-1")
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("beginPluginStart second call code = %v, want FailedPrecondition: %v", status.Code(err), err)
	}

	provider.clearPluginStart("session-1")
	_, _, err = provider.beginPluginStart("session-1")
	if err != nil {
		t.Fatalf("beginPluginStart after clear: %v", err)
	}
}

func TestGenerateEphemeralSSHHostKeyReturnsOpenSSHPEM(t *testing.T) {
	t.Parallel()

	hostKey, privatePEM, publicKey, err := generateEphemeralSSHHostKey()
	if err != nil {
		t.Fatalf("generateEphemeralSSHHostKey: %v", err)
	}
	if hostKey == nil {
		t.Fatal("hostKey is nil")
	}
	if !strings.Contains(privatePEM, "BEGIN OPENSSH PRIVATE KEY") {
		t.Fatalf("private PEM missing OpenSSH header: %q", privatePEM)
	}
	if !strings.HasPrefix(publicKey, "ssh-ed25519 ") {
		t.Fatalf("public key = %q, want ssh-ed25519", publicKey)
	}
}
