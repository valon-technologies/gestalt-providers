package temporal

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	providerVersion = "0.0.1-alpha.1"

	defaultWorkflowRunTimeout          = 5 * time.Minute
	defaultWorkflowTaskTimeout         = 10 * time.Second
	defaultActivityStartToCloseTimeout = 5 * time.Minute
	defaultScheduleCatchupWindow       = time.Minute
	defaultIdempotencyRetention        = 7 * 24 * time.Hour
)

type config struct {
	HostPort                    string           `yaml:"hostPort"`
	Namespace                   string           `yaml:"namespace"`
	APIKey                      string           `yaml:"apiKey"`
	TaskQueue                   string           `yaml:"taskQueue"`
	ScopeID                     string           `yaml:"scopeID"`
	WorkflowRunTimeout          time.Duration    `yaml:"workflowRunTimeout"`
	ActivityStartToCloseTimeout time.Duration    `yaml:"activityStartToCloseTimeout"`
	ScheduleCatchupWindow       time.Duration    `yaml:"scheduleCatchupWindow"`
	Versioning                  versioningConfig `yaml:"versioning"`
}

type versioningConfig struct {
	DeploymentName string `yaml:"deploymentName"`
	BuildID        string `yaml:"buildID"`
	// SetCurrentOnStart promotes this build to the deployment's current version
	// when the provider starts. Enabled in prod so a freshly deployed gestaltd
	// promotes itself; left false for local dev and tests so they never mutate
	// Temporal Cloud routing.
	SetCurrentOnStart bool `yaml:"setCurrentOnStart"`
}

func decodeConfig(raw map[string]any) (config, error) {
	cfg := config{
		WorkflowRunTimeout:          defaultWorkflowRunTimeout,
		ActivityStartToCloseTimeout: defaultActivityStartToCloseTimeout,
		ScheduleCatchupWindow:       defaultScheduleCatchupWindow,
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
	cfg.HostPort = strings.TrimSpace(cfg.HostPort)
	cfg.Namespace = strings.TrimSpace(cfg.Namespace)
	cfg.APIKey = strings.TrimSpace(cfg.APIKey)
	cfg.TaskQueue = strings.TrimSpace(cfg.TaskQueue)
	cfg.ScopeID = strings.TrimSpace(cfg.ScopeID)
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
	if cfg.ActivityStartToCloseTimeout <= 0 {
		return config{}, fmt.Errorf("activityStartToCloseTimeout must be positive")
	}
	if cfg.ScheduleCatchupWindow <= 0 {
		return config{}, fmt.Errorf("scheduleCatchupWindow must be positive")
	}
	if err := validateVersioningConfig(&cfg.Versioning); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func normalizeVersioningConfig(cfg versioningConfig) versioningConfig {
	cfg.DeploymentName = strings.TrimSpace(cfg.DeploymentName)
	cfg.BuildID = strings.TrimSpace(cfg.BuildID)
	return cfg
}

func validateVersioningConfig(cfg *versioningConfig) error {
	if cfg.DeploymentName == "" {
		return fmt.Errorf("versioning.deploymentName is required")
	}
	if strings.Contains(cfg.DeploymentName, ".") {
		return fmt.Errorf("versioning.deploymentName cannot contain %q", ".")
	}
	if cfg.BuildID == "" {
		return fmt.Errorf("versioning.buildID is required")
	}
	return nil
}
