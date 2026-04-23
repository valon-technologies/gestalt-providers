package nebius

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	computepb "github.com/nebius/gosdk/proto/nebius/compute/v1"
	"gopkg.in/yaml.v3"
)

const (
	defaultPlatform             = "cpu-d3"
	defaultPreset               = "4vcpu-16gb"
	defaultBootDiskSizeGiB      = int64(30)
	defaultBootDiskType         = "network_ssd"
	defaultBootDiskImageFamily  = "ubuntu24.04-driverless"
	defaultUsername             = "gestalt"
	defaultInstanceReadyTimeout = 10 * time.Minute
	defaultBootstrapTimeout     = 10 * time.Minute
	defaultPluginReadyTimeout   = 30 * time.Second
	defaultCleanupTimeout       = 2 * time.Minute
)

var linuxUsernamePattern = regexp.MustCompile(`^[a-z_][a-z0-9_-]{0,31}$`)

type Config struct {
	ProjectID              string        `yaml:"projectID,omitempty"`
	Endpoint               string        `yaml:"endpoint,omitempty"`
	SubnetID               string        `yaml:"subnetID"`
	SecurityGroupIDs       []string      `yaml:"securityGroupIDs,omitempty"`
	Platform               string        `yaml:"platform,omitempty"`
	Preset                 string        `yaml:"preset,omitempty"`
	ServiceAccountID       string        `yaml:"serviceAccountID,omitempty"`
	BootDiskSizeGiB        int64         `yaml:"bootDiskSizeGiB,omitempty"`
	BootDiskType           string        `yaml:"bootDiskType,omitempty"`
	BootDiskImageID        string        `yaml:"bootDiskImageID,omitempty"`
	BootDiskImageFamily    string        `yaml:"bootDiskImageFamily,omitempty"`
	BootDiskImageProjectID string        `yaml:"bootDiskImageProjectID,omitempty"`
	PublicIPStatic         bool          `yaml:"publicIPStatic,omitempty"`
	Username               string        `yaml:"username,omitempty"`
	InstanceReadyTimeout   time.Duration `yaml:"instanceReadyTimeout,omitempty"`
	BootstrapTimeout       time.Duration `yaml:"bootstrapTimeout,omitempty"`
	PluginReadyTimeout     time.Duration `yaml:"pluginReadyTimeout,omitempty"`
	CleanupTimeout         time.Duration `yaml:"cleanupTimeout,omitempty"`
}

func decodeConfig(raw map[string]any) (Config, error) {
	values := raw
	if nested, ok := raw["config"]; ok && nested != nil {
		nestedMap, ok := nested.(map[string]any)
		if !ok {
			return Config{}, fmt.Errorf("nebius runtime config must be an object")
		}
		values = nestedMap
	}
	normalized, err := normalizeConfigValues(values)
	if err != nil {
		return Config{}, err
	}
	data, err := yaml.Marshal(normalized)
	if err != nil {
		return Config{}, fmt.Errorf("nebius runtime: encode config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("nebius runtime: decode config: %w", err)
	}
	cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func normalizeConfigValues(values map[string]any) (map[string]any, error) {
	normalized := make(map[string]any, len(values))
	for key, value := range values {
		normalized[key] = value
	}
	for _, key := range []string{
		"instanceReadyTimeout",
		"bootstrapTimeout",
		"pluginReadyTimeout",
		"cleanupTimeout",
	} {
		value, ok := normalized[key]
		if !ok {
			continue
		}
		duration, err := parseDurationConfigValue(key, value)
		if err != nil {
			return nil, err
		}
		normalized[key] = int64(duration)
	}
	return normalized, nil
}

func parseDurationConfigValue(key string, value any) (time.Duration, error) {
	switch typed := value.(type) {
	case nil:
		return 0, nil
	case time.Duration:
		return typed, nil
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return 0, nil
		}
		duration, err := time.ParseDuration(trimmed)
		if err != nil {
			return 0, fmt.Errorf("nebius runtime %s must be a Go duration string: %w", key, err)
		}
		return duration, nil
	default:
		return 0, fmt.Errorf("nebius runtime %s must be a Go duration string", key)
	}
}

func (c *Config) Normalize() {
	if c == nil {
		return
	}
	c.ProjectID = strings.TrimSpace(c.ProjectID)
	c.Endpoint = strings.TrimSpace(c.Endpoint)
	c.SubnetID = strings.TrimSpace(c.SubnetID)
	for i := range c.SecurityGroupIDs {
		c.SecurityGroupIDs[i] = strings.TrimSpace(c.SecurityGroupIDs[i])
	}
	c.Platform = strings.TrimSpace(c.Platform)
	if c.Platform == "" {
		c.Platform = defaultPlatform
	}
	c.Preset = strings.TrimSpace(c.Preset)
	if c.Preset == "" {
		c.Preset = defaultPreset
	}
	c.ServiceAccountID = strings.TrimSpace(c.ServiceAccountID)
	c.BootDiskType = strings.TrimSpace(c.BootDiskType)
	if c.BootDiskType == "" {
		c.BootDiskType = defaultBootDiskType
	}
	c.BootDiskImageID = strings.TrimSpace(c.BootDiskImageID)
	c.BootDiskImageFamily = strings.TrimSpace(c.BootDiskImageFamily)
	if c.BootDiskImageID == "" && c.BootDiskImageFamily == "" {
		c.BootDiskImageFamily = defaultBootDiskImageFamily
	}
	c.BootDiskImageProjectID = strings.TrimSpace(c.BootDiskImageProjectID)
	if c.BootDiskSizeGiB == 0 {
		c.BootDiskSizeGiB = defaultBootDiskSizeGiB
	}
	c.Username = strings.TrimSpace(c.Username)
	if c.Username == "" {
		c.Username = defaultUsername
	}
	if c.InstanceReadyTimeout == 0 {
		c.InstanceReadyTimeout = defaultInstanceReadyTimeout
	}
	if c.BootstrapTimeout == 0 {
		c.BootstrapTimeout = defaultBootstrapTimeout
	}
	if c.PluginReadyTimeout == 0 {
		c.PluginReadyTimeout = defaultPluginReadyTimeout
	}
	if c.CleanupTimeout == 0 {
		c.CleanupTimeout = defaultCleanupTimeout
	}
}

func (c Config) Validate() error {
	if c.SubnetID == "" {
		return fmt.Errorf("nebius runtime subnetID is required")
	}
	if c.Platform == "" {
		return fmt.Errorf("nebius runtime platform is required")
	}
	if c.Preset == "" {
		return fmt.Errorf("nebius runtime preset is required")
	}
	if c.BootDiskSizeGiB <= 0 {
		return fmt.Errorf("nebius runtime bootDiskSizeGiB must be positive")
	}
	if _, err := c.diskTypeEnum(); err != nil {
		return err
	}
	if c.BootDiskImageID != "" && c.BootDiskImageFamily != "" {
		return fmt.Errorf("nebius runtime bootDiskImageID and bootDiskImageFamily are mutually exclusive")
	}
	if c.BootDiskImageID == "" && c.BootDiskImageFamily == "" {
		return fmt.Errorf("nebius runtime boot disk image is required")
	}
	if c.Username == "" {
		return fmt.Errorf("nebius runtime username is required")
	}
	if !linuxUsernamePattern.MatchString(c.Username) {
		return fmt.Errorf("nebius runtime username %q is not a valid Linux username", c.Username)
	}
	for _, securityGroupID := range c.SecurityGroupIDs {
		if securityGroupID == "" {
			return fmt.Errorf("nebius runtime securityGroupIDs must not contain empty values")
		}
	}
	if c.InstanceReadyTimeout < 0 {
		return fmt.Errorf("nebius runtime instanceReadyTimeout must be non-negative")
	}
	if c.BootstrapTimeout < 0 {
		return fmt.Errorf("nebius runtime bootstrapTimeout must be non-negative")
	}
	if c.PluginReadyTimeout < 0 {
		return fmt.Errorf("nebius runtime pluginReadyTimeout must be non-negative")
	}
	if c.CleanupTimeout < 0 {
		return fmt.Errorf("nebius runtime cleanupTimeout must be non-negative")
	}
	return nil
}

func (c Config) diskTypeEnum() (computepb.DiskSpec_DiskType, error) {
	switch strings.ToUpper(strings.ReplaceAll(c.BootDiskType, "-", "_")) {
	case "NETWORK_SSD":
		return computepb.DiskSpec_NETWORK_SSD, nil
	case "NETWORK_HDD":
		return computepb.DiskSpec_NETWORK_HDD, nil
	case "NETWORK_SSD_NON_REPLICATED":
		return computepb.DiskSpec_NETWORK_SSD_NON_REPLICATED, nil
	case "NETWORK_SSD_IO_M3":
		return computepb.DiskSpec_NETWORK_SSD_IO_M3, nil
	default:
		return computepb.DiskSpec_UNSPECIFIED, fmt.Errorf("nebius runtime bootDiskType %q is unsupported", c.BootDiskType)
	}
}
