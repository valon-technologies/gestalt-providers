package temporal

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	providerVersion = "0.0.1-alpha.21"

	defaultWorkflowRunTimeout               = 5 * time.Minute
	defaultWorkflowTaskTimeout              = 10 * time.Second
	defaultActivityStartToCloseTimeout      = 5 * time.Minute
	defaultScheduleCatchupWindow            = time.Minute
	defaultIdempotencyRetention             = 7 * 24 * time.Hour
	defaultWorkerDeploymentPromotionTimeout = 30 * time.Second
)

const (
	versioningBehaviorAutoUpgrade = "autoUpgrade"

	promotionModeNone    = "none"
	promotionModeCurrent = "current"
	promotionModeRamping = "ramping"
)

type config struct {
	IndexedDB                   string           `yaml:"indexeddb"`
	HostPort                    string           `yaml:"hostPort"`
	Namespace                   string           `yaml:"namespace"`
	APIKey                      string           `yaml:"apiKey"`
	TaskQueue                   string           `yaml:"taskQueue"`
	ScopeID                     string           `yaml:"scopeID"`
	Identity                    string           `yaml:"identity"`
	WorkflowRunTimeout          time.Duration    `yaml:"workflowRunTimeout"`
	WorkflowTaskTimeout         time.Duration    `yaml:"workflowTaskTimeout"`
	ActivityStartToCloseTimeout time.Duration    `yaml:"activityStartToCloseTimeout"`
	ScheduleCatchupWindow       time.Duration    `yaml:"scheduleCatchupWindow"`
	IdempotencyRetention        time.Duration    `yaml:"idempotencyRetention"`
	Versioning                  versioningConfig `yaml:"versioning"`
}

type versioningConfig struct {
	Enabled                   bool            `yaml:"enabled"`
	DeploymentName            string          `yaml:"deploymentName"`
	BuildID                   string          `yaml:"buildID"`
	BuildIDEnv                string          `yaml:"buildIDEnv"`
	DefaultVersioningBehavior string          `yaml:"defaultVersioningBehavior"`
	Promotion                 promotionConfig `yaml:"promotion"`
	ResolvedBuildID           string          `yaml:"-"`
}

type promotionConfig struct {
	Mode                string        `yaml:"mode"`
	Timeout             time.Duration `yaml:"timeout"`
	RampPercentage      *float32      `yaml:"rampPercentage"`
	AllowReplaceCurrent bool          `yaml:"allowReplaceCurrent"`
}

func decodeConfig(raw map[string]any) (config, error) {
	cfg := config{
		WorkflowRunTimeout:          defaultWorkflowRunTimeout,
		WorkflowTaskTimeout:         defaultWorkflowTaskTimeout,
		ActivityStartToCloseTimeout: defaultActivityStartToCloseTimeout,
		ScheduleCatchupWindow:       defaultScheduleCatchupWindow,
		IdempotencyRetention:        defaultIdempotencyRetention,
		Versioning: versioningConfig{
			Promotion: promotionConfig{
				Mode:    promotionModeNone,
				Timeout: defaultWorkerDeploymentPromotionTimeout,
			},
		},
	}
	if len(raw) > 0 {
		data, err := yaml.Marshal(raw)
		if err != nil {
			return config{}, fmt.Errorf("marshal config: %w", err)
		}
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return config{}, fmt.Errorf("decode config: %w", err)
		}
	}
	cfg.IndexedDB = strings.TrimSpace(cfg.IndexedDB)
	cfg.HostPort = strings.TrimSpace(cfg.HostPort)
	cfg.Namespace = strings.TrimSpace(cfg.Namespace)
	cfg.APIKey = strings.TrimSpace(cfg.APIKey)
	cfg.TaskQueue = strings.TrimSpace(cfg.TaskQueue)
	cfg.ScopeID = strings.TrimSpace(cfg.ScopeID)
	cfg.Identity = strings.TrimSpace(cfg.Identity)
	cfg.Versioning = normalizeVersioningConfig(cfg.Versioning)

	if cfg.HostPort == "" {
		return config{}, fmt.Errorf("hostPort is required")
	}
	if cfg.Namespace == "" {
		return config{}, fmt.Errorf("namespace is required")
	}
	if cfg.APIKey == "" {
		return config{}, fmt.Errorf("apiKey is required")
	}
	if cfg.TaskQueue == "" {
		return config{}, fmt.Errorf("taskQueue is required")
	}
	if cfg.ScopeID == "" {
		return config{}, fmt.Errorf("scopeID is required")
	}
	if cfg.WorkflowRunTimeout <= 0 {
		return config{}, fmt.Errorf("workflowRunTimeout must be positive")
	}
	if cfg.WorkflowTaskTimeout <= 0 {
		return config{}, fmt.Errorf("workflowTaskTimeout must be positive")
	}
	if cfg.ActivityStartToCloseTimeout <= 0 {
		return config{}, fmt.Errorf("activityStartToCloseTimeout must be positive")
	}
	if cfg.ScheduleCatchupWindow <= 0 {
		return config{}, fmt.Errorf("scheduleCatchupWindow must be positive")
	}
	if cfg.IdempotencyRetention <= 0 {
		return config{}, fmt.Errorf("idempotencyRetention must be positive")
	}
	if err := validateVersioningConfig(&cfg.Versioning); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func normalizeVersioningConfig(cfg versioningConfig) versioningConfig {
	cfg.DeploymentName = strings.TrimSpace(cfg.DeploymentName)
	cfg.BuildID = strings.TrimSpace(cfg.BuildID)
	cfg.BuildIDEnv = strings.TrimSpace(cfg.BuildIDEnv)
	cfg.DefaultVersioningBehavior = strings.TrimSpace(cfg.DefaultVersioningBehavior)
	cfg.Promotion.Mode = strings.TrimSpace(cfg.Promotion.Mode)
	if cfg.Promotion.Mode == "" {
		cfg.Promotion.Mode = promotionModeNone
	}
	if cfg.Promotion.Timeout == 0 {
		cfg.Promotion.Timeout = defaultWorkerDeploymentPromotionTimeout
	}
	return cfg
}

func validateVersioningConfig(cfg *versioningConfig) error {
	if cfg == nil || !cfg.Enabled {
		return nil
	}
	if cfg.DeploymentName == "" {
		return fmt.Errorf("versioning.deploymentName is required when versioning is enabled")
	}
	if strings.Contains(cfg.DeploymentName, ".") {
		return fmt.Errorf("versioning.deploymentName cannot contain %q", ".")
	}
	hasBuildID := cfg.BuildID != ""
	hasBuildIDEnv := cfg.BuildIDEnv != ""
	if hasBuildID == hasBuildIDEnv {
		return fmt.Errorf("exactly one of versioning.buildID or versioning.buildIDEnv is required when versioning is enabled")
	}
	if hasBuildIDEnv {
		cfg.ResolvedBuildID = strings.TrimSpace(os.Getenv(cfg.BuildIDEnv))
		if cfg.ResolvedBuildID == "" {
			return fmt.Errorf("versioning.buildIDEnv %q is not set or is empty", cfg.BuildIDEnv)
		}
	} else {
		cfg.ResolvedBuildID = cfg.BuildID
	}
	if cfg.ResolvedBuildID == "" {
		return fmt.Errorf("versioning build ID resolved to an empty value")
	}
	if cfg.DefaultVersioningBehavior != versioningBehaviorAutoUpgrade {
		return fmt.Errorf("versioning.defaultVersioningBehavior must be %q", versioningBehaviorAutoUpgrade)
	}
	switch cfg.Promotion.Mode {
	case promotionModeNone:
		if cfg.Promotion.RampPercentage != nil {
			return fmt.Errorf("versioning.promotion.rampPercentage is only valid when mode is %q", promotionModeRamping)
		}
	case promotionModeCurrent:
		if cfg.Promotion.RampPercentage != nil {
			return fmt.Errorf("versioning.promotion.rampPercentage is only valid when mode is %q", promotionModeRamping)
		}
	case promotionModeRamping:
		if cfg.Promotion.RampPercentage == nil {
			return fmt.Errorf("versioning.promotion.rampPercentage is required when mode is %q", promotionModeRamping)
		}
		if *cfg.Promotion.RampPercentage <= 0 || *cfg.Promotion.RampPercentage > 100 {
			return fmt.Errorf("versioning.promotion.rampPercentage must be greater than 0 and no more than 100")
		}
	default:
		return fmt.Errorf("versioning.promotion.mode must be one of %q, %q, or %q", promotionModeNone, promotionModeCurrent, promotionModeRamping)
	}
	if cfg.Promotion.Timeout <= 0 {
		return fmt.Errorf("versioning.promotion.timeout must be positive")
	}
	return nil
}
