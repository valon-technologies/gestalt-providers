package onepassword

import (
	"errors"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/1Password/connect-sdk-go/connect"
	op "github.com/1Password/connect-sdk-go/onepassword"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"

	"github.com/valon-technologies/gestalt-providers/secrets/internal/configutil"
)

const (
	providerVersion = "0.0.1-alpha.1"
	defaultField    = "credential"
	defaultTimeout  = 10 * time.Second
)

type config struct {
	Host  string `yaml:"host"`
	Token string `yaml:"token"`
	Vault string `yaml:"vault"`
	Field string `yaml:"field"`
}

type Provider struct {
	name   string
	client connect.Client
	vault  string
	field  string
}

func New() *Provider { return &Provider{} }

func (p *Provider) Configure(_ context.Context, name string, raw map[string]any) error {
	var cfg config
	if err := configutil.Decode(raw, &cfg); err != nil {
		return fmt.Errorf("onepassword secrets: %w", err)
	}
	if cfg.Host == "" {
		return fmt.Errorf("onepassword secrets: host is required")
	}
	if cfg.Token == "" {
		return fmt.Errorf("onepassword secrets: token is required")
	}
	if cfg.Vault == "" {
		return fmt.Errorf("onepassword secrets: vault is required")
	}
	if cfg.Field == "" {
		cfg.Field = defaultField
	}

	p.name = name
	p.client = connect.NewClientWithUserAgent(cfg.Host, cfg.Token, "gestalt/"+providerVersion)
	p.vault = cfg.Vault
	p.field = cfg.Field
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindSecrets,
		Name:        p.name,
		DisplayName: "1Password",
		Description: "Resolves secrets from 1Password Connect.",
		Version:     providerVersion,
	}
}

func (p *Provider) GetSecret(_ context.Context, name string) (string, error) {
	field := p.field
	if i := strings.LastIndex(name, "/"); i >= 0 {
		field = name[i+1:]
		name = name[:i]
	}

	item, err := p.client.GetItem(name, p.vault)
	if err != nil {
		var opErr *op.Error
		if errors.As(err, &opErr) && opErr.StatusCode == http.StatusNotFound {
			return "", fmt.Errorf("%w: %q", gestalt.ErrSecretNotFound, name)
		}
		return "", fmt.Errorf("accessing secret %q: %w", name, err)
	}

	value := item.GetValue(field)
	if value == "" {
		return "", fmt.Errorf("%w: %q (field %q)", gestalt.ErrSecretNotFound, name, field)
	}
	return value, nil
}
