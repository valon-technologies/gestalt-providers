package relationaldb

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	tenantIDMetadataKey        = "gestalt-tenant-id"
	tenantHostMetadataKey      = "gestalt-tenant-host"
	tenantBoundMetadataKey     = "gestalt-tenant-bound"
	tenantStoreKeyPrefix       = "__tenant__"
	defaultBackfillTenantID    = "vt"
	tenantModeRequestContext   = "request_context"
	tenantModeRequestContextUI = "requestContext"
)

type tenantScopeConfig struct {
	Mode             string              `yaml:"mode"`
	Source           string              `yaml:"source"`
	Storage          tenantStorageConfig `yaml:"storage"`
	BackfillTenantID string              `yaml:"backfillTenantId"`
}

type tenantStorageConfig struct {
	Strategy string `yaml:"strategy"`
	Column   string `yaml:"column"`
}

type tenantOptions struct {
	Enabled          bool
	BackfillTenantID string
}

type tenantScope struct {
	TenantID    string
	Host        string
	TenantBound bool
}

func (c tenantScopeConfig) isZero() bool {
	return strings.TrimSpace(c.Mode) == "" &&
		strings.TrimSpace(c.Source) == "" &&
		strings.TrimSpace(c.Storage.Strategy) == "" &&
		strings.TrimSpace(c.Storage.Column) == "" &&
		strings.TrimSpace(c.BackfillTenantID) == ""
}

func (c tenantScopeConfig) options() (tenantOptions, error) {
	mode := normalizeTenantMode(firstNonEmpty(c.Mode, c.Source))
	strategy := normalizeTenantStorageStrategy(c.Storage.Strategy)
	switch mode {
	case "":
		if strategy == "" && strings.TrimSpace(c.BackfillTenantID) == "" {
			return tenantOptions{}, nil
		}
		mode = tenantModeRequestContext
	case tenantModeRequestContext:
	default:
		return tenantOptions{}, fmt.Errorf("relationaldb: tenantScope.mode must be %q", tenantModeRequestContextUI)
	}

	if strategy != "" {
		switch strategy {
		case "store_name_prefix", "storeNamePrefix", "column":
		default:
			return tenantOptions{}, fmt.Errorf("relationaldb: tenantScope.storage.strategy must be storeNamePrefix")
		}
	}

	backfillTenantID := strings.TrimSpace(c.BackfillTenantID)
	if backfillTenantID == "" && strings.TrimSpace(c.Storage.Column) != "" {
		backfillTenantID = defaultBackfillTenantID
	}
	return tenantOptions{
		Enabled:          true,
		BackfillTenantID: backfillTenantID,
	}, nil
}

func normalizeTenantMode(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	normalized = strings.ReplaceAll(normalized, "-", "_")
	switch normalized {
	case "", "none", "disabled":
		return ""
	case "requestcontext", "request_context":
		return tenantModeRequestContext
	default:
		return strings.TrimSpace(value)
	}
}

func normalizeTenantStorageStrategy(value string) string {
	normalized := strings.TrimSpace(value)
	switch strings.ToLower(strings.ReplaceAll(normalized, "-", "_")) {
	case "", "none":
		return ""
	case "store_name_prefix", "storenameprefix":
		return "store_name_prefix"
	case "column", "tenant_id_column", "tenantidcolumn":
		return "column"
	default:
		return normalized
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (s *Store) scopedStoreName(ctx context.Context, storeName string) (string, error) {
	storeName = strings.TrimSpace(storeName)
	if storeName == "" {
		return "", status.Error(codes.InvalidArgument, "object store name is required")
	}
	if !s.tenant.Enabled {
		return storeName, nil
	}
	scope, ok := tenantScopeFromContext(ctx)
	if !ok || scope.TenantID == "" || !scope.TenantBound {
		return "", status.Error(codes.FailedPrecondition, "tenant scope is required for indexeddb operation")
	}
	return tenantScopedStoreName(scope.TenantID, storeName), nil
}

func tenantScopedStoreName(tenantID, storeName string) string {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(strings.TrimSpace(tenantID)))
	return tenantStoreKeyPrefix + encoded + "__" + storeName
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
