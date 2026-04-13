package onepassword

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/1password/onepassword-sdk-go"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"

	"github.com/valon-technologies/gestalt-providers/secrets/internal/configutil"
)

const (
	providerVersion = "0.0.1-alpha.1"
	defaultField    = "credential"
	defaultTimeout  = 10 * time.Second
)

type config struct {
	ServiceAccountToken string `yaml:"serviceAccountToken"`
	Vault               string `yaml:"vault"`
	Field               string `yaml:"field"`
}

type Provider struct {
	name   string
	client *onepassword.Client
	vault  string
	field  string
}

func New() *Provider { return &Provider{} }

func (p *Provider) Configure(ctx context.Context, name string, raw map[string]any) error {
	var cfg config
	if err := configutil.Decode(raw, &cfg); err != nil {
		return fmt.Errorf("onepassword secrets: %w", err)
	}
	if cfg.ServiceAccountToken == "" {
		return fmt.Errorf("onepassword secrets: serviceAccountToken is required")
	}
	if cfg.Vault == "" {
		return fmt.Errorf("onepassword secrets: vault is required")
	}
	if cfg.Field == "" {
		cfg.Field = defaultField
	}

	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	client, err := onepassword.NewClient(ctx,
		onepassword.WithServiceAccountToken(cfg.ServiceAccountToken),
		onepassword.WithIntegrationInfo("Gestalt", providerVersion),
	)
	if err != nil {
		return fmt.Errorf("onepassword secrets: creating client: %w", err)
	}

	p.name = name
	p.client = client
	p.vault = cfg.Vault
	p.field = cfg.Field
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindSecrets,
		Name:        p.name,
		DisplayName: "1Password",
		Description: "Resolves secrets from 1Password.",
		Version:     providerVersion,
	}
}

func (p *Provider) GetSecret(ctx context.Context, name string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	reference := name
	if !strings.HasPrefix(name, "op://") {
		reference = fmt.Sprintf("op://%s/%s/%s", p.vault, name, p.field)
	}

	secret, err := p.client.Secrets().Resolve(ctx, reference)
	if err != nil {
		return "", fmt.Errorf("%w: %q", gestalt.ErrSecretNotFound, name)
	}
	return secret, nil
}
