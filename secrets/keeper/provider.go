package keeper

import (
	"context"
	"fmt"
	"strings"
	"time"

	ksm "github.com/keeper-security/secrets-manager-go/core"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"

	"github.com/valon-technologies/gestalt-providers/secrets/internal/configutil"
)

const (
	providerVersion = "0.0.1-alpha.1"
	defaultField    = "password"
	defaultTimeout  = 10 * time.Second
)

type config struct {
	Config string `yaml:"config"`
	Field  string `yaml:"field"`
}

type Provider struct {
	name   string
	client *ksm.SecretsManager
	field  string
}

func New() *Provider { return &Provider{} }

func (p *Provider) Configure(_ context.Context, name string, raw map[string]any) error {
	var cfg config
	if err := configutil.Decode(raw, &cfg); err != nil {
		return fmt.Errorf("keeper secrets: %w", err)
	}
	if cfg.Config == "" {
		return fmt.Errorf("keeper secrets: config is required")
	}
	if cfg.Field == "" {
		cfg.Field = defaultField
	}

	storage := ksm.NewMemoryKeyValueStorage(cfg.Config)
	client := ksm.NewSecretsManager(&ksm.ClientOptions{
		Config: storage,
	})

	p.name = name
	p.client = client
	p.field = cfg.Field
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindSecrets,
		Name:        p.name,
		DisplayName: "Keeper Secrets Manager",
		Description: "Resolves secrets from Keeper Secrets Manager.",
		Version:     providerVersion,
	}
}

func (p *Provider) GetSecret(_ context.Context, name string) (string, error) {
	if strings.Contains(name, "/") {
		return p.resolveNotation(name)
	}
	return p.resolveByUID(name)
}

func (p *Provider) resolveNotation(notation string) (string, error) {
	values, err := p.client.GetNotation(notation)
	if err != nil {
		return "", fmt.Errorf("accessing secret %q: %w", notation, err)
	}
	if len(values) == 0 {
		return "", fmt.Errorf("%w: %q", gestalt.ErrSecretNotFound, notation)
	}
	s, ok := values[0].(string)
	if !ok {
		return "", fmt.Errorf("secret %q: value is not a string", notation)
	}
	return s, nil
}

func (p *Provider) resolveByUID(uid string) (string, error) {
	records, err := p.client.GetSecrets([]string{uid})
	if err != nil {
		return "", fmt.Errorf("accessing secret %q: %w", uid, err)
	}
	if len(records) == 0 {
		return "", fmt.Errorf("%w: %q", gestalt.ErrSecretNotFound, uid)
	}
	return records[0].Password(), nil
}
