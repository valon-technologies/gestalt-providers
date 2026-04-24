package modal

import (
	"io"
	"sync"
	"time"

	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultSessionLogEntries  = 512
	maxSessionLogPendingBytes = 64 * 1024
	logReaderChunkBytes       = 32 * 1024
)

type sessionLogBuffer struct {
	mu      sync.Mutex
	max     int
	total   int
	entries []*proto.PluginRuntimeLogEntry
	pending map[proto.PluginRuntimeLogStream]string
}

func newSessionLogBuffer(maxEntries int) *sessionLogBuffer {
	if maxEntries <= 0 {
		maxEntries = defaultSessionLogEntries
	}
	return &sessionLogBuffer{
		max:     maxEntries,
		entries: make([]*proto.PluginRuntimeLogEntry, 0, maxEntries),
		pending: map[proto.PluginRuntimeLogStream]string{},
	}
}

func (b *sessionLogBuffer) add(stream proto.PluginRuntimeLogStream, message string, observedAt time.Time) {
	if b == nil {
		return
	}
	entry := &proto.PluginRuntimeLogEntry{
		Stream:     normalizeLogStream(stream),
		Message:    message,
		ObservedAt: timestamppb.New(observedAt.UTC()),
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.total++
	if len(b.entries) == b.max {
		copy(b.entries, b.entries[1:])
		b.entries[len(b.entries)-1] = entry
		return
	}
	b.entries = append(b.entries, entry)
}

func (b *sessionLogBuffer) stream(reader io.ReadCloser, stream proto.PluginRuntimeLogStream) <-chan struct{} {
	done := make(chan struct{})
	if b == nil || reader == nil {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		defer reader.Close()
		buf := make([]byte, logReaderChunkBytes)
		for {
			n, err := reader.Read(buf)
			if n > 0 {
				b.capture(stream, string(buf[:n]), time.Now())
			}
			if err == nil {
				continue
			}
			if err != io.EOF {
				b.add(proto.PluginRuntimeLogStream_PLUGIN_RUNTIME_LOG_STREAM_RUNTIME, "read process logs: "+err.Error(), time.Now())
			}
			return
		}
	}()
	return done
}

func (b *sessionLogBuffer) snapshot(tailEntries int) ([]*proto.PluginRuntimeLogEntry, bool) {
	if b == nil {
		return nil, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	entries := make([]*proto.PluginRuntimeLogEntry, 0, len(b.entries)+len(b.pending))
	entries = append(entries, b.entries...)
	for _, stream := range []proto.PluginRuntimeLogStream{
		proto.PluginRuntimeLogStream_PLUGIN_RUNTIME_LOG_STREAM_STDOUT,
		proto.PluginRuntimeLogStream_PLUGIN_RUNTIME_LOG_STREAM_STDERR,
		proto.PluginRuntimeLogStream_PLUGIN_RUNTIME_LOG_STREAM_RUNTIME,
	} {
		if fragment, ok := b.pending[stream]; ok && fragment != "" {
			entries = append(entries, &proto.PluginRuntimeLogEntry{
				Stream:     stream,
				Message:    fragment,
				ObservedAt: timestamppb.New(time.Now().UTC()),
			})
		}
	}
	if len(entries) == 0 {
		return nil, false
	}
	start := 0
	truncated := b.total > len(b.entries)
	if tailEntries > 0 && tailEntries < len(entries) {
		start = len(entries) - tailEntries
		truncated = true
	}
	out := make([]*proto.PluginRuntimeLogEntry, 0, len(entries)-start)
	for _, entry := range entries[start:] {
		if entry == nil {
			continue
		}
		out = append(out, cloneLogEntry(entry))
	}
	return out, truncated
}

func cloneLogEntry(entry *proto.PluginRuntimeLogEntry) *proto.PluginRuntimeLogEntry {
	if entry == nil {
		return nil
	}
	var observedAt *timestamppb.Timestamp
	if entry.GetObservedAt() != nil {
		observedAt = timestamppb.New(entry.GetObservedAt().AsTime())
	}
	return &proto.PluginRuntimeLogEntry{
		Stream:     entry.GetStream(),
		Message:    entry.GetMessage(),
		ObservedAt: observedAt,
	}
}

func (b *sessionLogBuffer) capture(stream proto.PluginRuntimeLogStream, chunk string, observedAt time.Time) {
	if b == nil || chunk == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	stream = normalizeLogStream(stream)
	pending := b.pending[stream] + chunk
	for {
		idx := indexByte(pending, '\n')
		if idx >= 0 {
			b.appendLocked(&proto.PluginRuntimeLogEntry{
				Stream:     stream,
				Message:    trimTrailingCarriageReturn(pending[:idx]),
				ObservedAt: timestamppb.New(observedAt.UTC()),
			})
			pending = pending[idx+1:]
			continue
		}
		if len(pending) <= maxSessionLogPendingBytes {
			break
		}
		b.appendLocked(&proto.PluginRuntimeLogEntry{
			Stream:     stream,
			Message:    pending[:maxSessionLogPendingBytes],
			ObservedAt: timestamppb.New(observedAt.UTC()),
		})
		pending = pending[maxSessionLogPendingBytes:]
	}
	b.pending[stream] = pending
}

func (b *sessionLogBuffer) appendLocked(entry *proto.PluginRuntimeLogEntry) {
	b.total++
	if len(b.entries) == b.max {
		copy(b.entries, b.entries[1:])
		b.entries[len(b.entries)-1] = entry
		return
	}
	b.entries = append(b.entries, entry)
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

func indexByte(s string, target byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == target {
			return i
		}
	}
	return -1
}

func trimTrailingCarriageReturn(s string) string {
	if len(s) > 0 && s[len(s)-1] == '\r' {
		return s[:len(s)-1]
	}
	return s
}
