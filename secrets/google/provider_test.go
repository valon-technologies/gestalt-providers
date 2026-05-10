package google

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestCloseWithoutConfiguredClient(t *testing.T) {
	t.Parallel()

	var provider Provider
	if err := provider.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestResolveSecretNameAppliesTenantNamespace(t *testing.T) {
	p := &Provider{
		requireTenant: true,
		tenantScope: tenantScopeConfig{
			NamespaceTemplate: "tenants-{{ .TenantID }}-{{ .Name }}",
			GlobalPrefix:      "global/",
		},
	}

	got, err := p.resolveSecretName(tenantTestContext("acme"), "api-key")
	if err != nil {
		t.Fatalf("resolveSecretName: %v", err)
	}
	if got != "tenants-acme-api-key" {
		t.Fatalf("resolved name = %q, want tenants-acme-api-key", got)
	}
}

func TestResolveSecretNameAllowsExplicitGlobalPrefix(t *testing.T) {
	p := &Provider{
		requireTenant: true,
		tenantScope:   tenantScopeConfig{GlobalPrefix: "global/"},
	}

	got, err := p.resolveSecretName(context.Background(), "global/auth0-client-secret")
	if err != nil {
		t.Fatalf("resolveSecretName: %v", err)
	}
	if got != "auth0-client-secret" {
		t.Fatalf("resolved name = %q, want auth0-client-secret", got)
	}
}

func TestResolveSecretNameRequiresTenantScope(t *testing.T) {
	p := &Provider{requireTenant: true}
	if _, err := p.resolveSecretName(context.Background(), "api-key"); err == nil {
		t.Fatal("resolveSecretName without tenant succeeded, want error")
	}
}

func tenantTestContext(tenantID string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		tenantIDMetadataKey, tenantID,
		tenantBoundMetadataKey, "true",
	))
}
