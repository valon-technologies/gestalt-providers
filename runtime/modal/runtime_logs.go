package modal

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

const (
	logReaderChunkBytes    = 32 * 1024
	runtimeLogAppendTimout = 5 * time.Second
)

type runtimeLogHostClient interface {
	AppendLogs(ctx context.Context, sessionID string, entries []gestalt.RuntimeLogEntry) error
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

func (s *sessionLogSink) add(stream gestalt.RuntimeLogStream, message string, observedAt time.Time) {
	if s == nil || message == "" {
		return
	}
	if err := s.append([]gestalt.RuntimeLogEntry{{
		Stream:     normalizeLogStream(stream),
		Message:    message,
		ObservedAt: observedAt.UTC(),
		SourceSeq:  int64(atomic.AddUint64(s.counter, 1)),
	}}); err != nil {
		reportRuntimeLogAppendError(s.sessionID, err)
	}
}

func (s *sessionLogSink) stream(reader io.ReadCloser, stream gestalt.RuntimeLogStream) <-chan struct{} {
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
				s.add(gestalt.RuntimeLogStreamRuntime, "read process logs: "+err.Error(), time.Now())
			}
			return
		}
	}()
	return done
}

func (s *sessionLogSink) append(logs []gestalt.RuntimeLogEntry) error {
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

	return client.AppendLogs(ctx, s.sessionID, logs)
}

func reportRuntimeLogAppendError(sessionID string, err error) {
	if err == nil {
		return
	}
	_, _ = fmt.Fprintf(os.Stderr, "modal runtime: append session log for %q: %v\n", sessionID, err)
}

func normalizeLogStream(stream gestalt.RuntimeLogStream) gestalt.RuntimeLogStream {
	switch stream {
	case gestalt.RuntimeLogStreamStdout,
		gestalt.RuntimeLogStreamStderr:
		return stream
	default:
		return gestalt.RuntimeLogStreamRuntime
	}
}
