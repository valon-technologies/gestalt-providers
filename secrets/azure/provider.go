package azure

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"

	"github.com/valon-technologies/gestalt-providers/secrets/internal/configutil"
)

const (
	providerVersion = "0.0.1-alpha.1"
	defaultTimeout  = 10 * time.Second
)

type config struct {
	VaultURL string `yaml:"vault_url"`
	Version  string `yaml:"version"`
}

type Provider struct {
	name    string
	client  *azsecrets.Client
	version string
}

func New() *Provider { return &Provider{} }

func (p *Provider) Configure(_ context.Context, name string, raw map[string]any) error {
	var cfg config
	if err := configutil.Decode(raw, &cfg); err != nil {
		return fmt.Errorf("azure secrets: %w", err)
	}
	if cfg.VaultURL == "" {
		return fmt.Errorf("azure secrets: vault_url is required")
	}

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return fmt.Errorf("azure secrets: creating credentials: %w", err)
	}

	client, err := azsecrets.NewClient(cfg.VaultURL, cred, nil)
	if err != nil {
		return fmt.Errorf("azure secrets: creating client: %w", err)
	}

	p.name = name
	p.client = client
	p.version = cfg.Version
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindSecrets,
		Name:        p.name,
		DisplayName: "Azure Key Vault",
		Description: "Resolves secrets from Azure Key Vault.",
		Version:     providerVersion,
	}
}

func (p *Provider) GetSecret(ctx context.Context, name string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	resp, err := p.client.GetSecret(ctx, name, p.version, nil)
	if err != nil {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound {
			return "", fmt.Errorf("%w: %q", gestalt.ErrSecretNotFound, name)
		}
		return "", fmt.Errorf("accessing secret %q: %w", name, err)
	}
	if resp.Value == nil {
		return "", fmt.Errorf("secret %q has nil value", name)
	}
	return *resp.Value, nil
}
