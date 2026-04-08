package dynamodb

import (
	"context"
	"fmt"

	"github.com/valon-technologies/gestalt-providers/datastore/internal/configutil"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

const providerVersion = "0.0.1-alpha.1"

type yamlConfig struct {
	Table    string `yaml:"table"`
	Region   string `yaml:"region"`
	Endpoint string `yaml:"endpoint"`
}

type Provider struct {
	*Store
}

func New() *Provider { return &Provider{} }

func (p *Provider) Configure(_ context.Context, _ string, raw map[string]any) error {
	var cfg yamlConfig
	if err := configutil.Decode(raw, &cfg); err != nil {
		return fmt.Errorf("dynamodb datastore: %w", err)
	}
	if cfg.Table == "" {
		cfg.Table = "gestalt"
	}
	store, err := NewStore(Config{
		Table:    cfg.Table,
		Region:   cfg.Region,
		Endpoint: cfg.Endpoint,
	})
	if err != nil {
		return err
	}
	p.Store = store
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindDatastore,
		Name:        "dynamodb",
		DisplayName: "DynamoDB",
		Description: "Amazon DynamoDB datastore provider.",
		Version:     providerVersion,
	}
}
