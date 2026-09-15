package relationaldb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/valon-technologies/gestalt-providers/secrets/internal/configutil"
	"github.com/valon-technologies/gestalt-providers/secrets/relationaldb/internal/secretstore"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

const (
	providerVersion = "0.0.1-alpha.1"
	defaultTimeout  = 10 * time.Second
)

type config struct {
	DSN    string `yaml:"dsn"`
	Schema string `yaml:"schema"`
	KMSKey string `yaml:"kmsKey"`
}

type Provider struct {
	name  string
	store *secretstore.Store
}

func New() *Provider { return &Provider{} }

func (p *Provider) Configure(ctx context.Context, name string, raw map[string]any) error {
	var cfg config
	if err := configutil.Decode(raw, &cfg); err != nil {
		return fmt.Errorf("relationaldb secrets: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	store, err := secretstore.Open(ctx, secretstore.Config{
		DSN:    cfg.DSN,
		Schema: cfg.Schema,
		KMSKey: cfg.KMSKey,
	})
	if err != nil {
		return fmt.Errorf("relationaldb secrets: %w", err)
	}

	p.name = strings.TrimSpace(name)
	p.store = store
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindSecrets,
		Name:        p.name,
		DisplayName: "Relational Database Secrets",
		Description: "Resolves KMS-encrypted deployment secrets from a relational database.",
		Version:     providerVersion,
	}
}

func (p *Provider) GetSecret(ctx context.Context, name string) (string, error) {
	if p.store == nil {
		return "", fmt.Errorf("relationaldb secrets: provider is not configured")
	}

	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	return p.store.Get(ctx, name)
}

func (p *Provider) Close() error {
	if p.store == nil {
		return nil
	}
	return p.store.Close()
}

var _ gestalt.SecretsProvider = (*Provider)(nil)
var _ gestalt.MetadataProvider = (*Provider)(nil)
var _ gestalt.Closer = (*Provider)(nil)
