package hostservicetest

import (
	"context"
	"os"
	"path/filepath"
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

	WaitForSocket(t, socketPath)
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

// SocketPath returns a unix socket path in the test temp directory.
func SocketPath(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join(t.TempDir(), name)
}

// WaitForSocket blocks until socketPath exists or the test times out.
func WaitForSocket(t *testing.T, socketPath string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(socketPath); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("socket %q was not created", socketPath)
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
