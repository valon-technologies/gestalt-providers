package temporal

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	providerVersion = "0.0.1-alpha.8"

	defaultWorkflowRunTimeout          = 5 * time.Minute
	defaultWorkflowTaskTimeout         = 10 * time.Second
	defaultActivityStartToCloseTimeout = 5 * time.Minute
	defaultScheduleCatchupWindow       = time.Minute
	defaultIndexShardCount             = 64
)

type config struct {
	IndexedDB                   string        `yaml:"indexeddb"`
	HostPort                    string        `yaml:"hostPort"`
	Namespace                   string        `yaml:"namespace"`
	APIKey                      string        `yaml:"apiKey"`
	TaskQueue                   string        `yaml:"taskQueue"`
	ScopeID                     string        `yaml:"scopeID"`
	Identity                    string        `yaml:"identity"`
	WorkflowRunTimeout          time.Duration `yaml:"workflowRunTimeout"`
	WorkflowTaskTimeout         time.Duration `yaml:"workflowTaskTimeout"`
	ActivityStartToCloseTimeout time.Duration `yaml:"activityStartToCloseTimeout"`
	ScheduleCatchupWindow       time.Duration `yaml:"scheduleCatchupWindow"`
	IndexShardCount             int           `yaml:"indexShardCount"`
}

func decodeConfig(raw map[string]any) (config, error) {
	cfg := config{
		WorkflowRunTimeout:          defaultWorkflowRunTimeout,
		WorkflowTaskTimeout:         defaultWorkflowTaskTimeout,
		ActivityStartToCloseTimeout: defaultActivityStartToCloseTimeout,
		ScheduleCatchupWindow:       defaultScheduleCatchupWindow,
		IndexShardCount:             defaultIndexShardCount,
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
	if cfg.IndexShardCount <= 0 {
		return config{}, fmt.Errorf("indexShardCount must be positive")
	}
	return cfg, nil
}
