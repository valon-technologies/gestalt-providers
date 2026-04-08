package mongodb

import (
	"context"
	"fmt"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt-providers/datastore/internal/configutil"
)

const (
	providerVersion = "0.0.1-alpha.1"
	defaultDatabase = "gestalt"
)

type config struct {
	URI      string `yaml:"uri"`
	Database string `yaml:"database"`
}

type Provider struct {
	*Store
}

func New() *Provider { return &Provider{} }

func (p *Provider) Configure(_ context.Context, _ string, raw map[string]any) error {
	var cfg config
	if err := configutil.Decode(raw, &cfg); err != nil {
		return fmt.Errorf("mongodb datastore: %w", err)
	}
	if cfg.Database == "" {
		cfg.Database = defaultDatabase
	}
	store, err := NewStore(cfg.URI, cfg.Database)
	if err != nil {
		return err
	}
	p.Store = store
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindDatastore,
		Name:        "mongodb",
		DisplayName: "MongoDB",
		Description: "MongoDB datastore provider.",
		Version:     providerVersion,
	}
}
