package hostservicetest

import (
	"context"
	"os"
	"runtime"
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc"
)

// Start runs register on a unified host-service gRPC server until the test
// finishes and sets gestalt.EnvHostServiceSocket to the server target.
func Start(t *testing.T, register func(*grpc.Server)) {
	t.Helper()

	socketPath := SocketPath(t, "host-service.sock")
	t.Setenv(gestalt.EnvHostServiceSocket, "unix://"+socketPath)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- gestalt.ServeHostServiceGRPC(ctx, socketPath, register)
	}()
	runtime.Gosched()

	if !waitForSocket(socketPath, 2*time.Second) {
		select {
		case err := <-errCh:
			t.Fatalf("host service socket %q was not created; serve exited: %v", socketPath, err)
		default:
			cancel()
			select {
			case err := <-errCh:
				t.Fatalf("host service socket %q was not created; serve stopped after cancel: %v", socketPath, err)
			case <-time.After(2 * time.Second):
				t.Fatalf("host service socket %q was not created; serve did not exit", socketPath)
			}
		}
	}
	t.Cleanup(func() {
		cancel()
		WaitServeResult(t, errCh)
		_ = os.Remove(socketPath)
	})
}

// StartIndexedDB registers a single IndexedDB host service for the test.
func StartIndexedDB(t *testing.T, provider gestalt.IndexedDBProvider) {
	t.Helper()
	Start(t, func(srv *grpc.Server) {
		gestalt.RegisterIndexedDBHostService(srv, provider)
	})
}

// SocketPath returns a unix socket path with a short absolute path so tests
// stay within platform sun_path limits on macOS.
func SocketPath(t *testing.T, name string) string {
	t.Helper()

	f, err := os.CreateTemp("", "ghs-*.sock")
	if err != nil {
		t.Fatalf("CreateTemp(socket): %v", err)
	}
	socketPath := f.Name()
	if err := f.Close(); err != nil {
		t.Fatalf("Close(temp socket file): %v", err)
	}
	if err := os.Remove(socketPath); err != nil {
		t.Fatalf("Remove(temp socket file): %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(socketPath) })
	return socketPath
}

// WaitForSocket blocks until socketPath exists or the test times out.
func WaitForSocket(t *testing.T, socketPath string) {
	t.Helper()
	if !waitForSocket(socketPath, 2*time.Second) {
		t.Fatalf("socket %q was not created", socketPath)
	}
}

func waitForSocket(socketPath string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if _, err := os.Stat(socketPath); err == nil {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// WaitServeResult waits for a ServeHostServiceGRPC goroutine to exit after cancel.
func WaitServeResult(t *testing.T, errCh <-chan error) {
	t.Helper()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("host service serve returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("host service serve did not stop after context cancellation")
	}
}
