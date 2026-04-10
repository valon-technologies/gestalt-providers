package relationaldb

import (
	"context"
	"fmt"

	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"gopkg.in/yaml.v3"
)

type config struct {
	DSN string `yaml:"dsn"`
}

type Provider struct {
	proto.UnimplementedIndexedDBServer
	*Store
}

func New() *Provider { return &Provider{} }

func (p *Provider) Configure(_ context.Context, _ string, raw map[string]any) error {
	var cfg config
	data, err := yaml.Marshal(raw)
	if err != nil {
		return fmt.Errorf("relationaldb: marshal config: %w", err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("relationaldb: decode config: %w", err)
	}
	if cfg.DSN == "" {
		return fmt.Errorf("relationaldb: dsn is required")
	}
	store, err := NewStore(cfg.DSN)
	if err != nil {
		return err
	}
	p.Store = store
	return nil
}
