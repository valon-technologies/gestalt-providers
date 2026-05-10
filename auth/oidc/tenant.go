package oidc

import (
	"context"
	"strings"

	"google.golang.org/grpc/metadata"
)

const (
	tenantIDMetadataKey    = "gestalt-tenant-id"
	tenantHostMetadataKey  = "gestalt-tenant-host"
	tenantBoundMetadataKey = "gestalt-tenant-bound"
)

type tenantScope struct {
	TenantID    string
	Host        string
	TenantBound bool
}

func tenantScopeFromContext(ctx context.Context) (tenantScope, bool) {
	if ctx == nil {
		return tenantScope{}, false
	}
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
