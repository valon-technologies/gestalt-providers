package indexeddb

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	tenantIDMetadataKey          = "gestalt-tenant-id"
	tenantHostMetadataKey        = "gestalt-tenant-host"
	tenantBoundMetadataKey       = "gestalt-tenant-bound"
	tenantPrincipalIDMetadataKey = "gestalt-tenant-principal-id"
)

type tenantScopeConfig struct {
	Mode   string `yaml:"mode"`
	Source string `yaml:"source"`
}

type tenantScope struct {
	TenantID    string
	Host        string
	TenantBound bool
	PrincipalID string
}

func (c tenantScopeConfig) isZero() bool {
	return strings.TrimSpace(c.Mode) == "" && strings.TrimSpace(c.Source) == ""
}

func (c tenantScopeConfig) requireTenant() (bool, error) {
	mode := strings.ToLower(strings.TrimSpace(firstNonEmpty(c.Mode, c.Source)))
	mode = strings.ReplaceAll(mode, "-", "_")
	switch mode {
	case "", "none", "disabled":
		return false, nil
	case "requestcontext", "request_context":
		return true, nil
	default:
		return false, fmt.Errorf("tenantScope.mode must be requestContext")
	}
}

func tenantOutgoingContext(ctx context.Context, requireTenant bool) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	scope, ok := tenantScopeFromContext(ctx)
	if !ok {
		if requireTenant {
			return nil, status.Error(codes.FailedPrecondition, "tenant scope is required")
		}
		return ctx, nil
	}
	if requireTenant && (scope.TenantID == "" || !scope.TenantBound) {
		return nil, status.Error(codes.FailedPrecondition, "tenant scope is required")
	}
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	setMetadataValue(md, tenantIDMetadataKey, scope.TenantID)
	setMetadataValue(md, tenantHostMetadataKey, scope.Host)
	if scope.TenantBound {
		setMetadataValue(md, tenantBoundMetadataKey, "true")
	}
	setMetadataValue(md, tenantPrincipalIDMetadataKey, scope.PrincipalID)
	return metadata.NewOutgoingContext(ctx, md), nil
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
		PrincipalID: strings.TrimSpace(firstMetadataValue(md, tenantPrincipalIDMetadataKey)),
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

func setMetadataValue(md metadata.MD, key, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		delete(md, key)
		return
	}
	md.Set(key, value)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
