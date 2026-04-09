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
	providerVersion = "0.0.1-alpha.7"
	defaultVersion  = "latest"
	defaultTimeout  = 10 * time.Second
)

type config struct {
	Project string `yaml:"project"`
	Version string `yaml:"version"`
}

type Provider struct {
	name    string
	client  *secretmanager.Client
	project string
	version string
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
	if strings.Contains(name, "/") {
		return "", fmt.Errorf("invalid secret name %q: must not contain '/'", name)
	}

	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	resourceName := fmt.Sprintf("projects/%s/secrets/%s/versions/%s", p.project, name, p.version)
	resp, err := p.client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{
		Name: resourceName,
	})
	if err != nil {
		if s, ok := status.FromError(err); ok && s.Code() == codes.NotFound {
			return "", fmt.Errorf("%w: %q in project %q", gestalt.ErrSecretNotFound, name, p.project)
		}
		return "", fmt.Errorf("accessing secret %q: %w", name, err)
	}
	if resp.Payload == nil {
		return "", fmt.Errorf("accessing secret %q: response payload is nil", name)
	}
	return string(resp.Payload.Data), nil
}

func (p *Provider) Close() error {
	if p.client == nil {
		return nil
	}
	return p.client.Close()
}
