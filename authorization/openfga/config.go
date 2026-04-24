package openfga

import (
	"fmt"
	"net/url"
	"strings"

	"gopkg.in/yaml.v3"
)

type config struct {
	APIURL         string `yaml:"apiUrl"`
	StoreID        string `yaml:"storeId"`
	APIToken       string `yaml:"apiToken"`
	ClientID       string `yaml:"clientId"`
	ClientSecret   string `yaml:"clientSecret"`
	APITokenIssuer string `yaml:"apiTokenIssuer"`
	APIAudience    string `yaml:"apiAudience"`
	Scopes         string `yaml:"scopes"`
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

	cfg.APIURL = strings.TrimSpace(cfg.APIURL)
	cfg.StoreID = strings.TrimSpace(cfg.StoreID)
	cfg.APIToken = strings.TrimSpace(cfg.APIToken)
	cfg.ClientID = strings.TrimSpace(cfg.ClientID)
	cfg.ClientSecret = strings.TrimSpace(cfg.ClientSecret)
	cfg.APITokenIssuer = strings.TrimSpace(cfg.APITokenIssuer)
	cfg.APIAudience = strings.TrimSpace(cfg.APIAudience)
	cfg.Scopes = strings.TrimSpace(cfg.Scopes)

	if cfg.APIURL == "" {
		return cfg, fmt.Errorf("apiUrl is required")
	}
	parsed, err := url.Parse(cfg.APIURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return cfg, fmt.Errorf("apiUrl must be a valid absolute URL")
	}
	if cfg.StoreID == "" {
		return cfg, fmt.Errorf("storeId is required")
	}

	hasToken := cfg.APIToken != ""
	hasClientCreds := cfg.ClientID != "" || cfg.ClientSecret != "" || cfg.APITokenIssuer != "" || cfg.APIAudience != "" || cfg.Scopes != ""
	if hasToken && hasClientCreds {
		return cfg, fmt.Errorf("apiToken cannot be combined with client credential settings")
	}
	if hasClientCreds {
		switch {
		case cfg.ClientID == "":
			return cfg, fmt.Errorf("clientId is required when using client credentials")
		case cfg.ClientSecret == "":
			return cfg, fmt.Errorf("clientSecret is required when using client credentials")
		case cfg.APITokenIssuer == "":
			return cfg, fmt.Errorf("apiTokenIssuer is required when using client credentials")
		case cfg.APIAudience == "" && cfg.Scopes == "":
			return cfg, fmt.Errorf("either apiAudience or scopes is required when using client credentials")
		}
	}

	return cfg, nil
}
