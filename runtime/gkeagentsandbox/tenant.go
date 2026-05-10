package gkeagentsandbox

import (
	"context"
	"strings"

	"google.golang.org/grpc/metadata"
)

const (
	tenantIDMetadataKey          = "gestalt-tenant-id"
	tenantHostMetadataKey        = "gestalt-tenant-host"
	tenantBoundMetadataKey       = "gestalt-tenant-bound"
	tenantPrincipalIDMetadataKey = "gestalt-tenant-principal-id"

	sessionTenantIDMetadataKey   = "tenant.id"
	sessionTenantHostMetadataKey = "tenant.host"
	runtimeTenantLabel           = "gestalt.dev/tenant"
	runtimeTenantAnnotation      = "gestalt.dev/tenant-id"
	runtimeTenantHostAnnotation  = "gestalt.dev/tenant-host"
)

type tenantScope struct {
	TenantID    string
	Host        string
	TenantBound bool
	PrincipalID string
}

func addTenantMetadata(ctx context.Context, values map[string]string) map[string]string {
	scope, ok := tenantScopeFromContext(ctx)
	if !ok || scope.TenantID == "" || !scope.TenantBound {
		return values
	}
	if values == nil {
		values = map[string]string{}
	}
	values[sessionTenantIDMetadataKey] = scope.TenantID
	if scope.Host != "" {
		values[sessionTenantHostMetadataKey] = scope.Host
	}
	values[tenantIDMetadataKey] = scope.TenantID
	values[tenantBoundMetadataKey] = "true"
	if scope.Host != "" {
		values[tenantHostMetadataKey] = scope.Host
	}
	if scope.PrincipalID != "" {
		values[tenantPrincipalIDMetadataKey] = scope.PrincipalID
	}
	return values
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
