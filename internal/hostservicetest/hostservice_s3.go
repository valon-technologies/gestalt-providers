package hostservicetest

import (
	"testing"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc"
)

// StartS3 registers a single S3 host service for the test.
func StartS3(t *testing.T, provider gestalt.S3Provider) {
	t.Helper()
	Start(t, func(srv *grpc.Server) {
		gestalt.RegisterS3HostService(srv, provider)
	})
}
