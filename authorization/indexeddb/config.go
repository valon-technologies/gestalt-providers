package indexeddb

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

type config struct {
	IndexedDB     string            `yaml:"indexeddb"`
	TenantScope   tenantScopeConfig `yaml:"tenantScope"`
	Tenancy       tenantScopeConfig `yaml:"tenancy"`
	RequireTenant bool
}

func decodeConfig(raw map[string]any) (config, error) {
	var cfg config
	data, err := yaml.Marshal(raw)
	if err != nil {
		return cfg, fmt.Errorf("marshal config: %w", err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("decode config: %w", err)
	}
	cfg.IndexedDB = strings.TrimSpace(cfg.IndexedDB)
	cfg.TenantScope = cfg.mergedTenantScope()
	requireTenant, err := cfg.TenantScope.requireTenant()
	if err != nil {
		return cfg, err
	}
	cfg.RequireTenant = requireTenant
	return cfg, nil
}

func (c config) mergedTenantScope() tenantScopeConfig {
	if !c.TenantScope.isZero() {
		return c.TenantScope
	}
	return c.Tenancy
}
