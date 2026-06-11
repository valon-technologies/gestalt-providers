package configutil

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

func Decode(config map[string]any, out any) error {
	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	if err := yaml.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode config: %w", err)
	}
	return nil
}
