package helloworld

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	proto "github.com/valon-technologies/gestalt/server/rpc/protov1/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestProviderLifecycleAndHelloWorld(t *testing.T) {
	provider := New()
	if got := provider.Name(); got != defaultProviderName {
		t.Fatalf("default provider name = %q, want %q", got, defaultProviderName)
	}
	if err := provider.Configure(context.Background(), "custom", map[string]any{}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	if got := provider.Name(); got != "custom" {
		t.Fatalf("configured provider name = %q, want custom", got)
	}
	message, err := provider.HelloWorld(context.Background())
	if err != nil {
		t.Fatalf("HelloWorld: %v", err)
	}
	if message != helloWorldMessage {
		t.Fatalf("HelloWorld = %q, want %q", message, helloWorldMessage)
	}

	lifecycle := lifecycleServer{provider: provider}
	identity, err := lifecycle.GetProviderIdentity(context.Background(), &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetProviderIdentity: %v", err)
	}
	if identity.GetKind() != proto.ProviderKind_PROVIDER_KIND_TEST {
		t.Fatalf("identity kind = %v, want PROVIDER_KIND_TEST", identity.GetKind())
	}
	if identity.GetName() != "custom" {
		t.Fatalf("identity name = %q, want custom", identity.GetName())
	}

	resp, err := lifecycle.ConfigureProvider(context.Background(), &proto.ConfigureProviderRequest{
		Name:            "configured",
		Config:          &structpb.Struct{},
		ProtocolVersion: proto.CurrentProtocolVersion,
	})
	if err != nil {
		t.Fatalf("ConfigureProvider: %v", err)
	}
	if resp.GetProtocolVersion() != proto.CurrentProtocolVersion {
		t.Fatalf("protocol version = %d, want %d", resp.GetProtocolVersion(), proto.CurrentProtocolVersion)
	}
	if got := provider.Name(); got != "configured" {
		t.Fatalf("provider name after ConfigureProvider = %q, want configured", got)
	}

	test := testServer{provider: provider}
	hello, err := test.HelloWorld(context.Background(), &proto.HelloWorldRequest{})
	if err != nil {
		t.Fatalf("server HelloWorld: %v", err)
	}
	if hello.GetMessage() != helloWorldMessage {
		t.Fatalf("server HelloWorld = %q, want %q", hello.GetMessage(), helloWorldMessage)
	}
}

func TestConfigureProviderRejectsProtocolMismatch(t *testing.T) {
	_, err := lifecycleServer{provider: New()}.ConfigureProvider(context.Background(), &proto.ConfigureProviderRequest{
		ProtocolVersion: proto.CurrentProtocolVersion + 1,
	})
	if err == nil {
		t.Fatal("ConfigureProvider protocol mismatch succeeded, want error")
	}
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("ConfigureProvider error code = %v, want FailedPrecondition", got)
	}
}

func TestServeProvider(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "provider.sock")
	t.Setenv(proto.EnvProviderSocket, socket)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, New())
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Serve returned error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Serve did not stop")
		}
	})

	waitForSocket(t, socket)
	conn, err := grpc.NewClient("unix://"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
	})

	lifecycle := proto.NewProviderLifecycleClient(conn)
	identity, err := lifecycle.GetProviderIdentity(context.Background(), &emptypb.Empty{})
	if err != nil {
		t.Fatalf("GetProviderIdentity RPC: %v", err)
	}
	if identity.GetKind() != proto.ProviderKind_PROVIDER_KIND_TEST {
		t.Fatalf("identity kind = %v, want PROVIDER_KIND_TEST", identity.GetKind())
	}

	client := proto.NewTestProviderClient(conn)
	resp, err := client.HelloWorld(context.Background(), &proto.HelloWorldRequest{})
	if err != nil {
		t.Fatalf("HelloWorld RPC: %v", err)
	}
	if resp.GetMessage() != helloWorldMessage {
		t.Fatalf("HelloWorld RPC = %q, want %q", resp.GetMessage(), helloWorldMessage)
	}
}

func waitForSocket(t *testing.T, socket string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(socket); err == nil {
			conn, err := net.DialTimeout("unix", socket, 100*time.Millisecond)
			if err == nil {
				_ = conn.Close()
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("socket %q was not ready", socket)
}
