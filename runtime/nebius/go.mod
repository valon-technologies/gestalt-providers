// HOLDOUT: this provider uses the legacy PluginRuntimeHostService proto surface
// (BindHostService method, HostServiceAccess field, PluginRuntimeHostServiceRelay).
// Those types were removed in gestalt commit fbe40f109. Until this provider is
// modernized to the public-relay pattern, it needs a sibling gestalt checkout
// and is excluded from .gestalt-sdk-version bumps.

module github.com/valon-technologies/gestalt-providers/runtime/nebius

go 1.26

require (
	github.com/nebius/gosdk v0.2.10
	github.com/valon-technologies/gestalt/sdk/go v0.0.0-00010101000000-000000000000
	golang.org/x/crypto v0.47.0
	google.golang.org/grpc v1.80.0
	google.golang.org/protobuf v1.36.11
	gopkg.in/yaml.v3 v3.0.1
)

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.9-20250912141014-52f32327d4b0.1 // indirect
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/gofrs/flock v0.12.1 // indirect
	github.com/golang-jwt/jwt/v4 v4.5.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.1.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.39.0 // indirect
	go.opentelemetry.io/otel/metric v1.39.0 // indirect
	go.opentelemetry.io/otel/trace v1.39.0 // indirect
	golang.org/x/net v0.49.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/sys v0.40.0 // indirect
	golang.org/x/text v0.33.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260120221211-b8f7ae30c516 // indirect
)

replace github.com/valon-technologies/gestalt/sdk/go => ../../../gestalt/sdk/go
