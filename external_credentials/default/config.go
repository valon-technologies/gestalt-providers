package externalcredentials

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

type config struct {
	IndexedDB     string `yaml:"indexeddb"`
	EncryptionKey string `yaml:"encryptionKey"`
}

func decodeConfig(raw map[string]any) (config, error) {
	if raw == nil {
		raw = map[string]any{}
	}

	var cfg config
	data, err := yaml.Marshal(raw)
	if err != nil {
		return cfg, fmt.Errorf("marshal config: %w", err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("decode config: %w", err)
	}

	cfg.IndexedDB = strings.TrimSpace(cfg.IndexedDB)
	cfg.EncryptionKey = strings.TrimSpace(cfg.EncryptionKey)
	if cfg.EncryptionKey == "" {
		return cfg, fmt.Errorf("encryptionKey is required")
	}
	return cfg, nil
}
