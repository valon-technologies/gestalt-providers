package contracttest

import (
	"context"
	"os"
	"runtime"
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func startIndexedDBHost(t *testing.T, provider gestalt.IndexedDBProvider) {
	t.Helper()

	socketPath := hostSocketPath(t, "host-service.sock")
	t.Setenv("GESTALT_PROVIDER_SOCKET", socketPath)
	t.Setenv(gestalt.EnvHostServiceSocket, "unix://"+socketPath)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- gestalt.ServeIndexedDBProvider(ctx, provider)
	}()
	runtime.Gosched()

	if !waitForHostSocket(socketPath, 2*time.Second) {
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
		waitForHostServeResult(t, errCh)
		_ = os.Remove(socketPath)
	})
}

func hostSocketPath(t *testing.T, name string) string {
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

func waitForHostSocket(socketPath string, timeout time.Duration) bool {
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

func waitForHostServeResult(t *testing.T, errCh <-chan error) {
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
