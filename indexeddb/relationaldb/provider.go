package relationaldb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type config struct {
	DSN         string           `yaml:"dsn"`
	TablePrefix string           `yaml:"table_prefix"`
	Prefix      string           `yaml:"prefix"`
	Schema      string           `yaml:"schema"`
	Connection  connectionConfig `yaml:"connection"`
}

type connectionConfig struct {
	MaxOpenConns    *int           `yaml:"max_open_conns"`
	MaxIdleConns    *int           `yaml:"max_idle_conns"`
	ConnMaxLifetime *time.Duration `yaml:"conn_max_lifetime"`
	ConnMaxIdleTime *time.Duration `yaml:"conn_max_idle_time"`
	PingTimeout     *time.Duration `yaml:"ping_timeout"`
	RetryAttempts   *int           `yaml:"retry_attempts"`
	RetryBackoff    *time.Duration `yaml:"retry_backoff"`
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

	if c.Schema != "" {
		options.Schema = c.Schema
	}

	options.TablePrefix = strings.TrimSpace(options.TablePrefix)
	options.Schema = strings.TrimSpace(options.Schema)
	connectionOptions, err := c.Connection.options()
	if err != nil {
		return options, err
	}
	options.Connection = connectionOptions
	return options, nil
}

func (c connectionConfig) options() (connectionOptions, error) {
	options := connectionOptions{
		MaxOpenConns:    c.MaxOpenConns,
		MaxIdleConns:    c.MaxIdleConns,
		ConnMaxLifetime: c.ConnMaxLifetime,
		ConnMaxIdleTime: c.ConnMaxIdleTime,
		PingTimeout:     c.PingTimeout,
		RetryAttempts:   c.RetryAttempts,
		RetryBackoff:    c.RetryBackoff,
	}

	if c.MaxOpenConns != nil && *c.MaxOpenConns < 0 {
		return options, fmt.Errorf("relationaldb: connection.max_open_conns must be >= 0")
	}
	if c.MaxIdleConns != nil && *c.MaxIdleConns < 0 {
		return options, fmt.Errorf("relationaldb: connection.max_idle_conns must be >= 0")
	}
	if c.MaxOpenConns != nil && c.MaxIdleConns != nil && *c.MaxOpenConns > 0 && *c.MaxIdleConns > *c.MaxOpenConns {
		return options, fmt.Errorf("relationaldb: connection.max_idle_conns must be <= connection.max_open_conns when max_open_conns is set")
	}
	if c.ConnMaxLifetime != nil && *c.ConnMaxLifetime < 0 {
		return options, fmt.Errorf("relationaldb: connection.conn_max_lifetime must be >= 0")
	}
	if c.ConnMaxIdleTime != nil && *c.ConnMaxIdleTime < 0 {
		return options, fmt.Errorf("relationaldb: connection.conn_max_idle_time must be >= 0")
	}
	if c.PingTimeout != nil && *c.PingTimeout < 0 {
		return options, fmt.Errorf("relationaldb: connection.ping_timeout must be >= 0")
	}
	if c.RetryAttempts != nil && *c.RetryAttempts < 0 {
		return options, fmt.Errorf("relationaldb: connection.retry_attempts must be >= 0")
	}
	if c.RetryBackoff != nil && *c.RetryBackoff < 0 {
		return options, fmt.Errorf("relationaldb: connection.retry_backoff must be >= 0")
	}
	return options, nil
}

func (p *Provider) Configure(_ context.Context, _ string, raw map[string]any) error {
	if _, ok := raw["legacy_table_prefix"]; ok {
		return fmt.Errorf("relationaldb: legacy_table_prefix is no longer supported")
	}
	if _, ok := raw["legacy_prefix"]; ok {
		return fmt.Errorf("relationaldb: legacy_prefix is no longer supported")
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
	if p.Store != nil {
		_ = p.Store.Close()
	}
	p.Store = store
	return nil
}
