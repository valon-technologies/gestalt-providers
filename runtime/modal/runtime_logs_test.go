package modal

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func TestSessionLogSinkAppendsLogsOverSDKTransport(t *testing.T) {
	host := &recordingRuntimeLogHostClient{}

	var seq uint64
	sink := newSessionLogSink("session-1", &seq, func() (runtimeLogHostClient, error) {
		return host, nil
	})
	sink.add(gestalt.RuntimeLogStreamRuntime, "runtime boot", time.Date(2026, time.April, 24, 3, 0, 0, 0, time.UTC))
	sink.add(gestalt.RuntimeLogStreamStderr, "stderr line\n", time.Date(2026, time.April, 24, 3, 0, 1, 0, time.UTC))

	calls := host.calls()
	if len(calls) != 2 {
		t.Fatalf("AppendLogs calls len = %d, want 2", len(calls))
	}
	if got := calls[0].sessionID; got != "session-1" {
		t.Fatalf("AppendLogs session_id = %q, want session-1", got)
	}
	if got := calls[0].entries[0].Stream; got != gestalt.RuntimeLogStreamRuntime {
		t.Fatalf("AppendLogs[0] stream = %v, want runtime", got)
	}
	if got := calls[0].entries[0].Message; got != "runtime boot" {
		t.Fatalf("AppendLogs[0] message = %q, want runtime boot", got)
	}
	if got := calls[0].entries[0].SourceSeq; got != 1 {
		t.Fatalf("AppendLogs[0] source_seq = %d, want 1", got)
	}
	if got := calls[1].entries[0].Stream; got != gestalt.RuntimeLogStreamStderr {
		t.Fatalf("AppendLogs[1] stream = %v, want stderr", got)
	}
	if got := calls[1].entries[0].Message; got != "stderr line\n" {
		t.Fatalf("AppendLogs[1] message = %q, want stderr line", got)
	}
	if got := calls[1].entries[0].SourceSeq; got != 2 {
		t.Fatalf("AppendLogs[1] source_seq = %d, want 2", got)
	}
}

func TestSessionLogSinkStreamsChunksOverSDKTransport(t *testing.T) {
	host := &recordingRuntimeLogHostClient{}

	var seq uint64
	sink := newSessionLogSink("session-2", &seq, func() (runtimeLogHostClient, error) {
		return host, nil
	})
	done := sink.stream(io.NopCloser(strings.NewReader("hello stdout\n")), gestalt.RuntimeLogStreamStdout)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stdout stream to flush")
	}

	calls := host.calls()
	if len(calls) != 1 {
		t.Fatalf("AppendLogs calls len = %d, want 1", len(calls))
	}
	if got := calls[0].entries[0].Stream; got != gestalt.RuntimeLogStreamStdout {
		t.Fatalf("AppendLogs stream = %v, want stdout", got)
	}
	if got := calls[0].entries[0].Message; got != "hello stdout\n" {
		t.Fatalf("AppendLogs message = %q, want hello stdout\\n", got)
	}
	if got := calls[0].entries[0].SourceSeq; got != 1 {
		t.Fatalf("AppendLogs source_seq = %d, want 1", got)
	}
}

type recordingRuntimeLogHostClient struct {
	mu      sync.Mutex
	records []appendLogsCall
}

type appendLogsCall struct {
	sessionID string
	entries   []gestalt.RuntimeLogEntry
}

func (c *recordingRuntimeLogHostClient) AppendLogs(_ context.Context, sessionID string, entries []gestalt.RuntimeLogEntry) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, appendLogsCall{
		sessionID: sessionID,
		entries:   append([]gestalt.RuntimeLogEntry(nil), entries...),
	})
	return nil
}

func (c *recordingRuntimeLogHostClient) Close() error {
	return nil
}

func (c *recordingRuntimeLogHostClient) calls() []appendLogsCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]appendLogsCall, len(c.records))
	for i, record := range c.records {
		out[i] = appendLogsCall{
			sessionID: record.sessionID,
			entries:   append([]gestalt.RuntimeLogEntry(nil), record.entries...),
		}
	}
	return out
}
