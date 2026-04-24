package modal

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc"
)

func TestSessionLogSinkAppendsLogsOverSDKTransport(t *testing.T) {
	server, target := startRuntimeLogHostServer(t)
	t.Setenv(gestalt.EnvRuntimeLogHostSocket, target)

	var seq uint64
	sink := newSessionLogSink("session-1", &seq, nil)
	sink.add(proto.PluginRuntimeLogStream_PLUGIN_RUNTIME_LOG_STREAM_RUNTIME, "runtime boot", time.Date(2026, time.April, 24, 3, 0, 0, 0, time.UTC))
	sink.add(proto.PluginRuntimeLogStream_PLUGIN_RUNTIME_LOG_STREAM_STDERR, "stderr line\n", time.Date(2026, time.April, 24, 3, 0, 1, 0, time.UTC))

	calls := server.calls()
	if len(calls) != 2 {
		t.Fatalf("AppendLogs calls len = %d, want 2", len(calls))
	}
	if got := calls[0].GetSessionId(); got != "session-1" {
		t.Fatalf("AppendLogs session_id = %q, want session-1", got)
	}
	if got := calls[0].GetLogs()[0].GetStream(); got != proto.PluginRuntimeLogStream_PLUGIN_RUNTIME_LOG_STREAM_RUNTIME {
		t.Fatalf("AppendLogs[0] stream = %v, want runtime", got)
	}
	if got := calls[0].GetLogs()[0].GetMessage(); got != "runtime boot" {
		t.Fatalf("AppendLogs[0] message = %q, want runtime boot", got)
	}
	if got := calls[0].GetLogs()[0].GetSourceSeq(); got != 1 {
		t.Fatalf("AppendLogs[0] source_seq = %d, want 1", got)
	}
	if got := calls[1].GetLogs()[0].GetStream(); got != proto.PluginRuntimeLogStream_PLUGIN_RUNTIME_LOG_STREAM_STDERR {
		t.Fatalf("AppendLogs[1] stream = %v, want stderr", got)
	}
	if got := calls[1].GetLogs()[0].GetMessage(); got != "stderr line\n" {
		t.Fatalf("AppendLogs[1] message = %q, want stderr line", got)
	}
	if got := calls[1].GetLogs()[0].GetSourceSeq(); got != 2 {
		t.Fatalf("AppendLogs[1] source_seq = %d, want 2", got)
	}
}

func TestSessionLogSinkStreamsChunksOverSDKTransport(t *testing.T) {
	server, target := startRuntimeLogHostServer(t)
	t.Setenv(gestalt.EnvRuntimeLogHostSocket, target)

	var seq uint64
	sink := newSessionLogSink("session-2", &seq, nil)
	done := sink.stream(io.NopCloser(strings.NewReader("hello stdout\n")), proto.PluginRuntimeLogStream_PLUGIN_RUNTIME_LOG_STREAM_STDOUT)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stdout stream to flush")
	}

	calls := server.calls()
	if len(calls) != 1 {
		t.Fatalf("AppendLogs calls len = %d, want 1", len(calls))
	}
	if got := calls[0].GetLogs()[0].GetStream(); got != proto.PluginRuntimeLogStream_PLUGIN_RUNTIME_LOG_STREAM_STDOUT {
		t.Fatalf("AppendLogs stream = %v, want stdout", got)
	}
	if got := calls[0].GetLogs()[0].GetMessage(); got != "hello stdout\n" {
		t.Fatalf("AppendLogs message = %q, want hello stdout\\n", got)
	}
	if got := calls[0].GetLogs()[0].GetSourceSeq(); got != 1 {
		t.Fatalf("AppendLogs source_seq = %d, want 1", got)
	}
}

type recordingRuntimeLogHostServer struct {
	proto.UnimplementedPluginRuntimeLogHostServer

	mu      sync.Mutex
	records []*proto.AppendPluginRuntimeLogsRequest
}

func (s *recordingRuntimeLogHostServer) AppendLogs(_ context.Context, req *proto.AppendPluginRuntimeLogsRequest) (*proto.AppendPluginRuntimeLogsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, cloneAppendLogsRequest(req))
	lastSeq := int64(0)
	for _, call := range s.records {
		lastSeq += int64(len(call.GetLogs()))
	}
	return &proto.AppendPluginRuntimeLogsResponse{LastSeq: lastSeq}, nil
}

func (s *recordingRuntimeLogHostServer) calls() []*proto.AppendPluginRuntimeLogsRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*proto.AppendPluginRuntimeLogsRequest, len(s.records))
	copy(out, s.records)
	return out
}

func startRuntimeLogHostServer(t *testing.T) (*recordingRuntimeLogHostServer, string) {
	t.Helper()

	server := &recordingRuntimeLogHostServer{}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen runtime log host: %v", err)
	}
	grpcServer := grpc.NewServer()
	proto.RegisterPluginRuntimeLogHostServer(grpcServer, server)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("runtime log host stopped: %v", err)
		}
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = lis.Close()
	})
	return server, "tcp://" + lis.Addr().String()
}

func cloneAppendLogsRequest(req *proto.AppendPluginRuntimeLogsRequest) *proto.AppendPluginRuntimeLogsRequest {
	if req == nil {
		return nil
	}
	out := &proto.AppendPluginRuntimeLogsRequest{
		SessionId: req.GetSessionId(),
		Logs:      make([]*proto.PluginRuntimeLogEntry, 0, len(req.GetLogs())),
	}
	for _, entry := range req.GetLogs() {
		if entry == nil {
			continue
		}
		out.Logs = append(out.Logs, &proto.PluginRuntimeLogEntry{
			Stream:     entry.GetStream(),
			Message:    entry.GetMessage(),
			ObservedAt: entry.GetObservedAt(),
			SourceSeq:  entry.GetSourceSeq(),
		})
	}
	return out
}
