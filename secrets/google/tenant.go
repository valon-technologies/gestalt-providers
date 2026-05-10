package google

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/metadata"
)

const (
	tenantIDMetadataKey    = "gestalt-tenant-id"
	tenantHostMetadataKey  = "gestalt-tenant-host"
	tenantBoundMetadataKey = "gestalt-tenant-bound"
)

type tenantScopeConfig struct {
	Mode              string `yaml:"mode"`
	Source            string `yaml:"source"`
	NamespaceTemplate string `yaml:"namespaceTemplate"`
	GlobalPrefix      string `yaml:"globalPrefix"`
}

type tenantScope struct {
	TenantID    string
	Host        string
	TenantBound bool
}

func (c tenantScopeConfig) requireTenant() (bool, error) {
	mode := strings.ToLower(strings.TrimSpace(firstNonEmpty(c.Mode, c.Source)))
	mode = strings.ReplaceAll(mode, "-", "_")
	switch mode {
	case "", "none", "disabled":
		return false, nil
	case "requestcontext", "request_context", "namespace":
		return true, nil
	default:
		return false, fmt.Errorf("tenantScope.mode must be requestContext or namespace")
	}
}

func tenantScopeFromContext(ctx context.Context) (tenantScope, bool) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if scope, ok := tenantScopeFromMetadata(md); ok {
			return scope, true
		}
	}
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		if scope, ok := tenantScopeFromMetadata(md); ok {
			return scope, true
		}
	}
	return tenantScope{}, false
}

func tenantScopeFromMetadata(md metadata.MD) (tenantScope, bool) {
	scope := tenantScope{
		TenantID:    strings.TrimSpace(firstMetadataValue(md, tenantIDMetadataKey)),
		Host:        strings.ToLower(strings.Trim(strings.TrimSpace(firstMetadataValue(md, tenantHostMetadataKey)), ".")),
		TenantBound: strings.EqualFold(firstMetadataValue(md, tenantBoundMetadataKey), "true"),
	}
	return scope, scope.TenantID != ""
}

func firstMetadataValue(md metadata.MD, key string) string {
	values := md.Get(key)
	if len(values) == 0 {
		return ""
	}
	return strings.TrimSpace(values[0])
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
