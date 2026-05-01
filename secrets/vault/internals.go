package vault

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// decodeConfig decodes a raw `map[string]any` config into a typed struct via
// yaml round-trip. Inlined from the previous secrets/internal/configutil package.
func decodeConfig(config map[string]any, out any) error {
	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	if err := yaml.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode config: %w", err)
	}
	return nil
}
