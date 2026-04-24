package modal

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	logReaderChunkBytes    = 32 * 1024
	runtimeLogAppendTimout = 5 * time.Second
)

type runtimeLogHostClient interface {
	AppendLogs(ctx context.Context, req *proto.AppendPluginRuntimeLogsRequest) (*proto.AppendPluginRuntimeLogsResponse, error)
	Close() error
}

type runtimeLogHostOpener func() (runtimeLogHostClient, error)

type sessionLogSink struct {
	sessionID string
	counter   *uint64
	openHost  runtimeLogHostOpener
}

func newSessionLogSink(sessionID string, counter *uint64, openHost runtimeLogHostOpener) *sessionLogSink {
	if sessionID == "" || counter == nil {
		return nil
	}
	if openHost == nil {
		openHost = func() (runtimeLogHostClient, error) {
			return gestalt.RuntimeLogHost()
		}
	}
	return &sessionLogSink{
		sessionID: sessionID,
		counter:   counter,
		openHost:  openHost,
	}
}

func (s *sessionLogSink) add(stream proto.PluginRuntimeLogStream, message string, observedAt time.Time) {
	if s == nil || message == "" {
		return
	}
	if err := s.append([]*proto.PluginRuntimeLogEntry{{
		Stream:     normalizeLogStream(stream),
		Message:    message,
		ObservedAt: timestamppb.New(observedAt.UTC()),
		SourceSeq:  int64(atomic.AddUint64(s.counter, 1)),
	}}); err != nil {
		reportRuntimeLogAppendError(s.sessionID, err)
	}
}

func (s *sessionLogSink) stream(reader io.ReadCloser, stream proto.PluginRuntimeLogStream) <-chan struct{} {
	done := make(chan struct{})
	if s == nil || reader == nil {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		defer func() { _ = reader.Close() }()
		buf := make([]byte, logReaderChunkBytes)
		for {
			n, err := reader.Read(buf)
			if n > 0 {
				s.add(stream, string(append([]byte(nil), buf[:n]...)), time.Now())
			}
			if err == nil {
				continue
			}
			if err != io.EOF {
				s.add(proto.PluginRuntimeLogStream_PLUGIN_RUNTIME_LOG_STREAM_RUNTIME, "read process logs: "+err.Error(), time.Now())
			}
			return
		}
	}()
	return done
}

func (s *sessionLogSink) append(logs []*proto.PluginRuntimeLogEntry) error {
	if s == nil || len(logs) == 0 {
		return nil
	}
	client, err := s.openHost()
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), runtimeLogAppendTimout)
	defer cancel()

	_, err = client.AppendLogs(ctx, &proto.AppendPluginRuntimeLogsRequest{
		SessionId: s.sessionID,
		Logs:      logs,
	})
	return err
}

func reportRuntimeLogAppendError(sessionID string, err error) {
	if err == nil {
		return
	}
	_, _ = fmt.Fprintf(os.Stderr, "modal runtime: append session log for %q: %v\n", sessionID, err)
}

func normalizeLogStream(stream proto.PluginRuntimeLogStream) proto.PluginRuntimeLogStream {
	switch stream {
	case proto.PluginRuntimeLogStream_PLUGIN_RUNTIME_LOG_STREAM_STDOUT,
		proto.PluginRuntimeLogStream_PLUGIN_RUNTIME_LOG_STREAM_STDERR:
		return stream
	default:
		return proto.PluginRuntimeLogStream_PLUGIN_RUNTIME_LOG_STREAM_RUNTIME
	}
}
