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

type notationClient interface {
	GetNotationResults(notation string) ([]string, error)
}

type Provider struct {
	name   string
	client notationClient
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
	if client == nil {
		return fmt.Errorf("keeper secrets: failed to initialize client (check config)")
	}

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

func (p *Provider) GetSecret(ctx context.Context, name string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	if strings.Contains(name, "/") {
		return p.resolveNotation(ctx, name)
	}
	return p.resolveByUID(ctx, name)
}

func (p *Provider) resolveNotation(ctx context.Context, notation string) (string, error) {
	type result struct {
		values []string
		err    error
	}

	resultCh := make(chan result, 1)
	go func() {
		values, err := p.client.GetNotationResults(notation)
		resultCh <- result{values: values, err: err}
	}()

	var (
		values []string
		err    error
	)
	select {
	case <-ctx.Done():
		return "", fmt.Errorf("accessing secret %q: %w", notation, ctx.Err())
	case res := <-resultCh:
		values = res.values
		err = res.err
	}

	if err != nil {
		msg := err.Error()
		if strings.Contains(msg, "no records match") || strings.Contains(msg, "has no fields matching") {
			return "", fmt.Errorf("%w: %q", gestalt.ErrSecretNotFound, notation)
		}
		return "", fmt.Errorf("accessing secret %q: %w", notation, err)
	}
	if len(values) == 0 {
		return "", fmt.Errorf("%w: %q", gestalt.ErrSecretNotFound, notation)
	}
	return values[0], nil
}

func (p *Provider) resolveByUID(ctx context.Context, uid string) (string, error) {
	notation := fmt.Sprintf("keeper://%s/field/%s", uid, p.field)
	return p.resolveNotation(ctx, notation)
}
