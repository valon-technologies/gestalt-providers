package gkeagentsandbox

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

func waitForPluginReady(ctx context.Context, dialTarget string) error {
	network, address, err := parseLocalDialTarget(dialTarget)
	if err != nil {
		return err
	}
	var lastErr error
	for {
		conn, err := grpc.NewClient(
			address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, network, address)
			}),
		)
		if err == nil {
			client := proto.NewProviderLifecycleClient(conn)
			callCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			_, rpcErr := client.GetProviderIdentity(callCtx, &emptypb.Empty{})
			cancel()
			_ = conn.Close()
			if rpcErr == nil {
				return nil
			}
			lastErr = rpcErr
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("connect to hosted plugin %s: %w", dialTarget, lastErr)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func parseLocalDialTarget(target string) (string, string, error) {
	target = strings.TrimSpace(target)
	if strings.HasPrefix(target, "tcp://") {
		address := strings.TrimSpace(strings.TrimPrefix(target, "tcp://"))
		if address == "" {
			return "", "", fmt.Errorf("tcp dial target is missing host:port")
		}
		return "tcp", address, nil
	}
	return "", "", fmt.Errorf("unsupported gke agent sandbox plugin dial target %q", target)
}
