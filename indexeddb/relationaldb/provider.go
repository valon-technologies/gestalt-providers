package relationaldb

import (
	"context"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

type config struct {
	DSN               string `yaml:"dsn"`
	TablePrefix       string `yaml:"table_prefix"`
	Prefix            string `yaml:"prefix"`
	LegacyTablePrefix string `yaml:"legacy_table_prefix"`
	Schema            string `yaml:"schema"`
}

type Provider struct {
	*Store
}

func New() *Provider { return &Provider{Store: &Store{}} }

func (c config) storeOptions() (storeOptions, error) {
	options := storeOptions{TablePrefix: defaultTablePrefix}

	switch {
	case c.TablePrefix != "" && c.Prefix != "" && c.TablePrefix != c.Prefix:
		return options, fmt.Errorf("relationaldb: prefix and table_prefix must match when both are set")
	case c.TablePrefix != "":
		options.TablePrefix = c.TablePrefix
	case c.Prefix != "":
		options.TablePrefix = c.Prefix
	}

	if c.LegacyTablePrefix != "" {
		options.LegacyTablePrefix = c.LegacyTablePrefix
	}

	if c.Schema != "" {
		options.Schema = c.Schema
	}

	options.TablePrefix = strings.TrimSpace(options.TablePrefix)
	options.LegacyTablePrefix = strings.TrimSpace(options.LegacyTablePrefix)
	options.Schema = strings.TrimSpace(options.Schema)
	return options, nil
}

func (p *Provider) Configure(_ context.Context, _ string, raw map[string]any) error {
	if _, ok := raw["legacy_prefix"]; ok {
		return fmt.Errorf("relationaldb: legacy_prefix is no longer supported; use legacy_table_prefix")
	}
	if _, ok := raw["namespace"]; ok {
		return fmt.Errorf("relationaldb: namespace is no longer supported; use schema")
	}

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
	options, err := cfg.storeOptions()
	if err != nil {
		return err
	}
	store, err := newStoreWithOptions(cfg.DSN, options)
	if err != nil {
		return err
	}
	p.Store = store
	return nil
}
