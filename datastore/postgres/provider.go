package postgres

import (
	"context"
	"fmt"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt-providers/datastore/internal/configutil"
)

const providerVersion = "0.0.1-alpha.1"

type config struct {
	DSN string `yaml:"dsn"`
}

type Provider struct {
	*Store
}

func New() *Provider { return &Provider{} }

func (p *Provider) Configure(_ context.Context, _ string, raw map[string]any) error {
	var cfg config
	if err := configutil.Decode(raw, &cfg); err != nil {
		return fmt.Errorf("postgres datastore: %w", err)
	}
	store, err := NewStore(cfg.DSN)
	if err != nil {
		return err
	}
	p.Store = store
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindDatastore,
		Name:        "postgres",
		DisplayName: "PostgreSQL",
		Description: "PostgreSQL datastore provider for production deployments.",
		Version:     providerVersion,
	}
}
