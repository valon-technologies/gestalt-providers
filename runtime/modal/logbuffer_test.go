package modal

import (
	"io"
	"strings"
	"testing"

	modalclient "github.com/modal-labs/modal-client/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
)

func TestSessionLogBufferStreamPreservesWhitespaceAndLargeLines(t *testing.T) {
	t.Parallel()

	reader, writer := io.Pipe()
	logs := newSessionLogBuffer(0)
	done := logs.stream(reader, proto.PluginRuntimeLogStream_PLUGIN_RUNTIME_LOG_STREAM_STDERR)

	go func() {
		_, _ = io.WriteString(writer, "  indented line\n\n")
		_, _ = io.WriteString(writer, strings.Repeat("x", maxSessionLogPendingBytes+17)+"\nnext line\n")
		_ = writer.Close()
	}()

	<-done
	entries, truncated := logs.snapshot(0)
	if truncated {
		t.Fatal("snapshot truncated = true, want false")
	}
	if len(entries) != 4 {
		t.Fatalf("snapshot len = %d, want 4", len(entries))
	}
	if got := entries[0].GetMessage(); got != "  indented line" {
		t.Fatalf("entries[0].message = %q, want leading whitespace preserved", got)
	}
	if got := entries[1].GetMessage(); got != "" {
		t.Fatalf("entries[1].message = %q, want blank separator line", got)
	}
	if got := len(entries[2].GetMessage()); got != maxSessionLogPendingBytes+17 {
		t.Fatalf("entries[2] len = %d, want %d", got, maxSessionLogPendingBytes+17)
	}
	if got := entries[3].GetMessage(); got != "next line" {
		t.Fatalf("entries[3].message = %q, want next line", got)
	}
}

func TestResetSessionSandboxPreservesFailedLaunchState(t *testing.T) {
	t.Parallel()

	sandbox := &modalclient.Sandbox{}
	provider := &Provider{
		sessions: map[string]*session{
			"session-1": {
				id:      "session-1",
				state:   sessionStateFailed,
				sandbox: sandbox,
				tunnel:  &modalclient.Tunnel{},
			},
		},
	}

	provider.resetSessionSandbox("session-1", sandbox)

	provider.mu.Lock()
	defer provider.mu.Unlock()
	session := provider.sessions["session-1"]
	if session == nil {
		t.Fatal("session missing after reset")
	}
	if got := session.state; got != sessionStateFailed {
		t.Fatalf("session.state = %q, want %q", got, sessionStateFailed)
	}
	if session.sandbox != nil {
		t.Fatal("session.sandbox != nil after reset")
	}
	if session.tunnel != nil {
		t.Fatal("session.tunnel != nil after reset")
	}
}
