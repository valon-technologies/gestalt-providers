package indexeddb

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

type config struct {
	IndexedDB string `yaml:"indexeddb"`
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
	return cfg, nil
}
