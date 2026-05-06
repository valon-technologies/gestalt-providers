package kubernetes

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	defaultNamespace           = "default"
	defaultContainer           = "runtime"
	defaultPluginPort          = 50051
	defaultConnectionMode      = connectionModePortForward
	defaultGKEEndpoint         = gkeEndpointPrivate
	defaultSessionReadyTimeout = 3 * time.Minute
	defaultPluginReadyTimeout  = 30 * time.Second
	defaultExecTimeout         = 2 * time.Minute
	defaultCleanupTimeout      = 30 * time.Second
	defaultRuntimeClassName    = "gvisor"
	defaultCPURequest          = "250m"
	defaultMemoryRequest       = "512Mi"
	defaultCPULimit            = "1000m"
	defaultMemoryLimit         = "1Gi"
	defaultRunAsUser           = int64(65532)

	connectionModePortForward = "portForward"
	connectionModePodIP       = "podIP"
	connectionModeServiceDNS  = "serviceDNS"

	gkeEndpointPrivate = "private"
	gkeEndpointPublic  = "public"

	hostnameEgressModeDisabled    = "disabled"
	hostnameEgressModePublicProxy = "publicProxy"

	imagePullPolicyAlways       = "Always"
	imagePullPolicyIfNotPresent = "IfNotPresent"
	imagePullPolicyNever        = "Never"
)

type Config struct {
	Namespace           string               `yaml:"namespace,omitempty"`
	Container           string               `yaml:"container,omitempty"`
	Kubeconfig          string               `yaml:"kubeconfig,omitempty"`
	Context             string               `yaml:"context,omitempty"`
	GKE                 GKEConfig            `yaml:"gke,omitempty"`
	PluginPort          int                  `yaml:"pluginPort,omitempty"`
	ConnectionMode      string               `yaml:"connectionMode,omitempty"`
	SessionReadyTimeout time.Duration        `yaml:"sessionReadyTimeout,omitempty"`
	PluginReadyTimeout  time.Duration        `yaml:"pluginReadyTimeout,omitempty"`
	ExecTimeout         time.Duration        `yaml:"execTimeout,omitempty"`
	CleanupTimeout      time.Duration        `yaml:"cleanupTimeout,omitempty"`
	HostnameEgress      HostnameEgressConfig `yaml:"hostnameEgress,omitempty"`
	PodDefaults         PodDefaultsConfig    `yaml:"podDefaults,omitempty"`
}

type GKEConfig struct {
	ProjectID string `yaml:"projectID,omitempty"`
	Location  string `yaml:"location,omitempty"`
	Cluster   string `yaml:"cluster,omitempty"`
	Endpoint  string `yaml:"endpoint,omitempty"`
}

type HostnameEgressConfig struct {
	Mode string `yaml:"mode,omitempty"`
}

type PodDefaultsConfig struct {
	ServiceAccountName string             `yaml:"serviceAccountName,omitempty"`
	RuntimeClassName   string             `yaml:"runtimeClassName,omitempty"`
	Command            []string           `yaml:"command,omitempty"`
	Args               []string           `yaml:"args,omitempty"`
	ImagePullPolicy    string             `yaml:"imagePullPolicy,omitempty"`
	NodeSelector       map[string]string  `yaml:"nodeSelector,omitempty"`
	Tolerations        []TolerationConfig `yaml:"tolerations,omitempty"`
	CPURequest         string             `yaml:"cpuRequest,omitempty"`
	MemoryRequest      string             `yaml:"memoryRequest,omitempty"`
	CPULimit           string             `yaml:"cpuLimit,omitempty"`
	MemoryLimit        string             `yaml:"memoryLimit,omitempty"`
	RunAsUser          *int64             `yaml:"runAsUser,omitempty"`
}

type TolerationConfig struct {
	Key      string `yaml:"key,omitempty"`
	Operator string `yaml:"operator,omitempty"`
	Value    string `yaml:"value,omitempty"`
	Effect   string `yaml:"effect,omitempty"`
}

func decodeConfig(raw map[string]any) (Config, error) {
	values := raw
	if nested, ok := raw["config"]; ok && nested != nil {
		nestedMap, ok := nested.(map[string]any)
		if !ok {
			return Config{}, fmt.Errorf("kubernetes runtime config must be an object")
		}
		values = nestedMap
	}
	normalized, err := normalizeConfigValues(values)
	if err != nil {
		return Config{}, err
	}
	data, err := yaml.Marshal(normalized)
	if err != nil {
		return Config{}, fmt.Errorf("kubernetes runtime: encode config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("kubernetes runtime: decode config: %w", err)
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
		"sessionReadyTimeout",
		"pluginReadyTimeout",
		"execTimeout",
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
		normalized[key] = duration.String()
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
			return 0, fmt.Errorf("kubernetes runtime %s must be a Go duration string: %w", key, err)
		}
		return duration, nil
	case int:
		return time.Duration(typed), nil
	case int64:
		return time.Duration(typed), nil
	case float64:
		return time.Duration(typed), nil
	default:
		return 0, fmt.Errorf("kubernetes runtime %s must be a Go duration string", key)
	}
}

func (c *Config) Normalize() {
	if c == nil {
		return
	}
	c.Namespace = strings.TrimSpace(c.Namespace)
	if c.Namespace == "" {
		c.Namespace = defaultNamespace
	}
	c.Container = strings.TrimSpace(c.Container)
	if c.Container == "" {
		c.Container = defaultContainer
	}
	c.Kubeconfig = expandHome(strings.TrimSpace(c.Kubeconfig))
	c.Context = strings.TrimSpace(c.Context)
	c.GKE.Normalize()
	if c.PluginPort == 0 {
		c.PluginPort = defaultPluginPort
	}
	c.ConnectionMode = normalizeConnectionMode(c.ConnectionMode)
	if c.ConnectionMode == "" {
		c.ConnectionMode = defaultConnectionMode
	}
	if c.SessionReadyTimeout == 0 {
		c.SessionReadyTimeout = defaultSessionReadyTimeout
	}
	if c.PluginReadyTimeout == 0 {
		c.PluginReadyTimeout = defaultPluginReadyTimeout
	}
	if c.ExecTimeout == 0 {
		c.ExecTimeout = defaultExecTimeout
	}
	if c.CleanupTimeout == 0 {
		c.CleanupTimeout = defaultCleanupTimeout
	}
	c.HostnameEgress.Normalize()
	c.PodDefaults.Normalize()
}

func (c Config) Validate() error {
	if !isDNSLabel(c.Namespace) {
		return fmt.Errorf("kubernetes runtime namespace %q is not a valid Kubernetes DNS label", c.Namespace)
	}
	if !isDNSLabel(c.Container) {
		return fmt.Errorf("kubernetes runtime container %q is not a valid Kubernetes DNS label", c.Container)
	}
	if c.PluginPort <= 0 || c.PluginPort > 65535 {
		return fmt.Errorf("kubernetes runtime pluginPort must be between 1 and 65535")
	}
	switch c.ConnectionMode {
	case connectionModePortForward, connectionModePodIP, connectionModeServiceDNS:
	default:
		return fmt.Errorf("kubernetes runtime connectionMode must be %q, %q, or %q", connectionModePortForward, connectionModePodIP, connectionModeServiceDNS)
	}
	if c.SessionReadyTimeout < 0 {
		return fmt.Errorf("kubernetes runtime sessionReadyTimeout must be non-negative")
	}
	if c.PluginReadyTimeout < 0 {
		return fmt.Errorf("kubernetes runtime pluginReadyTimeout must be non-negative")
	}
	if c.ExecTimeout < 0 {
		return fmt.Errorf("kubernetes runtime execTimeout must be non-negative")
	}
	if c.CleanupTimeout < 0 {
		return fmt.Errorf("kubernetes runtime cleanupTimeout must be non-negative")
	}
	if c.GKE.IsConfigured() && (c.Kubeconfig != "" || c.Context != "") {
		return fmt.Errorf("kubernetes runtime gke config cannot be combined with kubeconfig or context")
	}
	if err := c.GKE.Validate(); err != nil {
		return err
	}
	if err := c.HostnameEgress.Validate(); err != nil {
		return err
	}
	return c.PodDefaults.Validate()
}

func (c *GKEConfig) Normalize() {
	if c == nil {
		return
	}
	c.ProjectID = strings.TrimSpace(c.ProjectID)
	c.Location = strings.TrimSpace(c.Location)
	c.Cluster = strings.TrimSpace(c.Cluster)
	c.Endpoint = strings.TrimSpace(c.Endpoint)
	if c.IsConfigured() && c.Endpoint == "" {
		c.Endpoint = defaultGKEEndpoint
	}
}

func (c GKEConfig) IsConfigured() bool {
	return c.ProjectID != "" || c.Location != "" || c.Cluster != "" || c.Endpoint != ""
}

func (c GKEConfig) Validate() error {
	if !c.IsConfigured() {
		return nil
	}
	if c.ProjectID == "" {
		return fmt.Errorf("kubernetes runtime gke.projectID is required")
	}
	if c.Location == "" {
		return fmt.Errorf("kubernetes runtime gke.location is required")
	}
	if c.Cluster == "" {
		return fmt.Errorf("kubernetes runtime gke.cluster is required")
	}
	switch c.Endpoint {
	case gkeEndpointPrivate, gkeEndpointPublic:
		return nil
	default:
		return fmt.Errorf("kubernetes runtime gke.endpoint must be %q or %q", gkeEndpointPrivate, gkeEndpointPublic)
	}
}

func (c GKEConfig) clusterResourceName() string {
	return fmt.Sprintf("projects/%s/locations/%s/clusters/%s", c.ProjectID, c.Location, c.Cluster)
}

func (c *HostnameEgressConfig) Normalize() {
	if c == nil {
		return
	}
	c.Mode = normalizeHostnameEgressMode(c.Mode)
	if c.Mode == "" {
		c.Mode = hostnameEgressModeDisabled
	}
}

func (c HostnameEgressConfig) Validate() error {
	switch c.Mode {
	case hostnameEgressModeDisabled, hostnameEgressModePublicProxy:
		return nil
	default:
		return fmt.Errorf("kubernetes runtime hostnameEgress.mode must be %q or %q", hostnameEgressModeDisabled, hostnameEgressModePublicProxy)
	}
}

func (c *PodDefaultsConfig) Normalize() {
	if c == nil {
		return
	}
	c.ServiceAccountName = strings.TrimSpace(c.ServiceAccountName)
	c.RuntimeClassName = strings.TrimSpace(c.RuntimeClassName)
	if c.RuntimeClassName == "" {
		c.RuntimeClassName = defaultRuntimeClassName
	}
	c.ImagePullPolicy = normalizeImagePullPolicy(c.ImagePullPolicy)
	if c.ImagePullPolicy == "" {
		c.ImagePullPolicy = imagePullPolicyIfNotPresent
	}
	c.CPURequest = strings.TrimSpace(c.CPURequest)
	if c.CPURequest == "" {
		c.CPURequest = defaultCPURequest
	}
	c.MemoryRequest = strings.TrimSpace(c.MemoryRequest)
	if c.MemoryRequest == "" {
		c.MemoryRequest = defaultMemoryRequest
	}
	c.CPULimit = strings.TrimSpace(c.CPULimit)
	if c.CPULimit == "" {
		c.CPULimit = defaultCPULimit
	}
	c.MemoryLimit = strings.TrimSpace(c.MemoryLimit)
	if c.MemoryLimit == "" {
		c.MemoryLimit = defaultMemoryLimit
	}
	for i := range c.Command {
		c.Command[i] = strings.TrimSpace(c.Command[i])
	}
	for i := range c.Args {
		c.Args[i] = strings.TrimSpace(c.Args[i])
	}
	for key, value := range c.NodeSelector {
		delete(c.NodeSelector, key)
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key != "" && value != "" {
			c.NodeSelector[key] = value
		}
	}
	for i := range c.Tolerations {
		c.Tolerations[i].Normalize()
	}
}

func (c PodDefaultsConfig) Validate() error {
	if c.RuntimeClassName == "" {
		return fmt.Errorf("kubernetes runtime podDefaults.runtimeClassName is required")
	}
	if c.CPURequest == "" || c.MemoryRequest == "" || c.CPULimit == "" || c.MemoryLimit == "" {
		return fmt.Errorf("kubernetes runtime podDefaults resource requests and limits are required")
	}
	for key, value := range map[string]string{
		"podDefaults.cpuRequest":    c.CPURequest,
		"podDefaults.memoryRequest": c.MemoryRequest,
		"podDefaults.cpuLimit":      c.CPULimit,
		"podDefaults.memoryLimit":   c.MemoryLimit,
	} {
		if _, err := resource.ParseQuantity(value); err != nil {
			return fmt.Errorf("kubernetes runtime parse %s: %w", key, err)
		}
	}
	switch c.ImagePullPolicy {
	case imagePullPolicyAlways, imagePullPolicyIfNotPresent, imagePullPolicyNever:
	default:
		return fmt.Errorf("kubernetes runtime podDefaults.imagePullPolicy must be %q, %q, or %q", imagePullPolicyAlways, imagePullPolicyIfNotPresent, imagePullPolicyNever)
	}
	return nil
}

func (c *TolerationConfig) Normalize() {
	if c == nil {
		return
	}
	c.Key = strings.TrimSpace(c.Key)
	c.Operator = strings.TrimSpace(c.Operator)
	c.Value = strings.TrimSpace(c.Value)
	c.Effect = strings.TrimSpace(c.Effect)
}

func normalizeConnectionMode(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	normalized = strings.ReplaceAll(normalized, "-", "")
	normalized = strings.ReplaceAll(normalized, "_", "")
	switch normalized {
	case "":
		return ""
	case "portforward":
		return connectionModePortForward
	case "podip":
		return connectionModePodIP
	case "servicedns", "service", "headlessservice", "incluster":
		return connectionModeServiceDNS
	default:
		return strings.TrimSpace(value)
	}
}

func normalizeHostnameEgressMode(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	normalized = strings.ReplaceAll(normalized, "-", "")
	normalized = strings.ReplaceAll(normalized, "_", "")
	switch normalized {
	case "":
		return ""
	case "disabled", "disable", "none", "off", "false":
		return hostnameEgressModeDisabled
	case "publicproxy", "proxy", "hostname", "enabled", "true":
		return hostnameEgressModePublicProxy
	default:
		return strings.TrimSpace(value)
	}
}

func normalizeImagePullPolicy(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	normalized = strings.ReplaceAll(normalized, "-", "")
	normalized = strings.ReplaceAll(normalized, "_", "")
	switch normalized {
	case "":
		return ""
	case "always":
		return imagePullPolicyAlways
	case "ifnotpresent":
		return imagePullPolicyIfNotPresent
	case "never":
		return imagePullPolicyNever
	default:
		return strings.TrimSpace(value)
	}
}

func expandHome(path string) string {
	if path == "" || path == "~" || !strings.HasPrefix(path, "~/") {
		return path
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return path
	}
	return home + strings.TrimPrefix(path, "~")
}

func isDNSLabel(value string) bool {
	if len(value) == 0 || len(value) > 63 {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' {
			continue
		}
		if c == '-' && i > 0 && i < len(value)-1 {
			continue
		}
		return false
	}
	return true
}

func isDNSSubdomain(value string) bool {
	if len(value) == 0 || len(value) > 253 {
		return false
	}
	parts := strings.Split(value, ".")
	for _, part := range parts {
		if !isDNSLabel(part) {
			return false
		}
	}
	return true
}
