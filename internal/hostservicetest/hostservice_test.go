package hostservicetest

import (
	"os"
	"testing"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc"
)

func TestStartCreatesSocket(t *testing.T) {
	Start(t, func(srv *grpc.Server) {})
	if target := os.Getenv(gestalt.EnvHostServiceSocket); target == "" {
		t.Fatal("host service socket env not set")
	}
}
