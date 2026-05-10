package google

import (
	"context"
	"fmt"
	"strings"
	"time"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/valon-technologies/gestalt-providers/secrets/internal/configutil"
)

const (
	providerVersion = "0.0.1-alpha.3"
	defaultVersion  = "latest"
	defaultTimeout  = 10 * time.Second
)

type config struct {
	Project     string            `yaml:"project"`
	Version     string            `yaml:"version"`
	TenantScope tenantScopeConfig `yaml:"tenantScope"`
	Tenancy     tenantScopeConfig `yaml:"tenancy"`
}

type Provider struct {
	name          string
	client        *secretmanager.Client
	project       string
	version       string
	tenantScope   tenantScopeConfig
	requireTenant bool
}

func New() *Provider { return &Provider{} }

func (p *Provider) Configure(ctx context.Context, name string, raw map[string]any) error {
	var cfg config
	if err := configutil.Decode(raw, &cfg); err != nil {
		return fmt.Errorf("google secrets: %w", err)
	}
	if cfg.Project == "" {
		return fmt.Errorf("google secrets: project is required")
	}
	if cfg.Version == "" {
		cfg.Version = defaultVersion
	}
	if cfg.TenantScope.Mode == "" && cfg.TenantScope.Source == "" && cfg.Tenancy.Mode != "" {
		cfg.TenantScope = cfg.Tenancy
	}
	requireTenant, err := cfg.TenantScope.requireTenant()
	if err != nil {
		return fmt.Errorf("google secrets: %w", err)
	}
	if requireTenant && strings.TrimSpace(cfg.TenantScope.NamespaceTemplate) == "" {
		cfg.TenantScope.NamespaceTemplate = "tenants-{{ .TenantID }}-{{ .Name }}"
	}
	if strings.TrimSpace(cfg.TenantScope.GlobalPrefix) == "" {
		cfg.TenantScope.GlobalPrefix = "global/"
	}

	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("google secrets: creating client: %w", err)
	}

	p.name = name
	p.client = client
	p.project = cfg.Project
	p.version = cfg.Version
	p.tenantScope = cfg.TenantScope
	p.requireTenant = requireTenant
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindSecrets,
		Name:        p.name,
		DisplayName: "Google Secret Manager",
		Description: "Resolves secrets from Google Cloud Secret Manager.",
		Version:     providerVersion,
	}
}

func (p *Provider) GetSecret(ctx context.Context, name string) (string, error) {
	resolvedName, err := p.resolveSecretName(ctx, name)
	if err != nil {
		return "", err
	}
	if strings.Contains(resolvedName, "/") {
		return "", fmt.Errorf("invalid resolved secret name %q: must not contain '/'", resolvedName)
	}

	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	resourceName := fmt.Sprintf("projects/%s/secrets/%s/versions/%s", p.project, resolvedName, p.version)
	resp, err := p.client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{
		Name: resourceName,
	})
	if err != nil {
		if s, ok := status.FromError(err); ok && s.Code() == codes.NotFound {
			return "", fmt.Errorf("%w: %q in project %q", gestalt.ErrSecretNotFound, resolvedName, p.project)
		}
		return "", fmt.Errorf("accessing secret %q: %w", resolvedName, err)
	}
	if resp.Payload == nil {
		return "", fmt.Errorf("accessing secret %q: response payload is nil", resolvedName)
	}
	return string(resp.Payload.Data), nil
}

func (p *Provider) resolveSecretName(ctx context.Context, name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("secret name is required")
	}
	globalPrefix := strings.TrimSpace(p.tenantScope.GlobalPrefix)
	if globalPrefix == "" {
		globalPrefix = "global/"
	}
	if strings.HasPrefix(name, globalPrefix) {
		globalName := strings.TrimSpace(strings.TrimPrefix(name, globalPrefix))
		if globalName == "" || strings.Contains(globalName, "/") {
			return "", fmt.Errorf("invalid global secret name %q", name)
		}
		return globalName, nil
	}
	if strings.Contains(name, "/") {
		return "", fmt.Errorf("invalid secret name %q: must not contain '/'", name)
	}
	if !p.requireTenant {
		return name, nil
	}
	scope, ok := tenantScopeFromContext(ctx)
	if !ok || scope.TenantID == "" || !scope.TenantBound {
		return "", fmt.Errorf("tenant scope is required for secret %q", name)
	}
	template := strings.TrimSpace(p.tenantScope.NamespaceTemplate)
	if template == "" {
		template = "tenants-{{ .TenantID }}-{{ .Name }}"
	}
	resolved := strings.ReplaceAll(template, "{{ .TenantID }}", scope.TenantID)
	resolved = strings.ReplaceAll(resolved, "{{.TenantID}}", scope.TenantID)
	resolved = strings.ReplaceAll(resolved, "{{ .Name }}", name)
	resolved = strings.ReplaceAll(resolved, "{{.Name}}", name)
	return strings.Trim(resolved, "/"), nil
}

func (p *Provider) Close() error {
	if p.client == nil {
		return nil
	}
	return p.client.Close()
}

var _ gestalt.SecretsProvider = (*Provider)(nil)
var _ gestalt.MetadataProvider = (*Provider)(nil)
var _ gestalt.Closer = (*Provider)(nil)
