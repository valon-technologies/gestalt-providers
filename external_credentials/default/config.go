package externalcredentials

import (
	"fmt"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"gopkg.in/yaml.v3"
)

type config struct {
	IndexedDB         string                  `yaml:"indexeddb"`
	EncryptionKey     string                  `yaml:"encryptionKey"`
	CredentialRefresh credentialRefreshConfig `yaml:"credentialRefresh"`
}

type credentialRefreshConfig struct {
	Targets []credentialRefreshTarget `yaml:"targets"`
}

type credentialRefreshTarget struct {
	Provider                    string `yaml:"provider"`
	Connection                  string `yaml:"connection"`
	ConnectionID                string `yaml:"connectionId"`
	RefreshInterval             string `yaml:"refreshInterval"`
	RefreshBeforeExpiry         string `yaml:"refreshBeforeExpiry"`
	RefreshIntervalDuration     time.Duration
	RefreshBeforeExpiryDuration time.Duration
	Auth                        *gestalt.ExternalCredentialAuthConfig `yaml:"-"`
	RawAuth                     credentialRefreshAuthConfig           `yaml:"auth"`
	ConnectionParams            map[string]string                     `yaml:"connectionParams"`
}

type credentialRefreshAuthConfig struct {
	Type                 string                                 `yaml:"type"`
	Token                string                                 `yaml:"token"`
	TokenPrefix          string                                 `yaml:"tokenPrefix"`
	GrantType            string                                 `yaml:"grantType"`
	TokenURL             string                                 `yaml:"tokenUrl"`
	ClientID             string                                 `yaml:"clientId"`
	ClientSecret         string                                 `yaml:"clientSecret"`
	ClientAuth           string                                 `yaml:"clientAuth"`
	TokenExchange        string                                 `yaml:"tokenExchange"`
	Scopes               []string                               `yaml:"scopes"`
	ScopeParam           string                                 `yaml:"scopeParam"`
	ScopeSeparator       string                                 `yaml:"scopeSeparator"`
	TokenParams          map[string]string                      `yaml:"tokenParams"`
	RefreshParams        map[string]string                      `yaml:"refreshParams"`
	AcceptHeader         string                                 `yaml:"acceptHeader"`
	AccessTokenPath      string                                 `yaml:"accessTokenPath"`
	TokenExchangeDrivers []credentialRefreshTokenExchangeDriver `yaml:"tokenExchangeDrivers"`
}

type credentialRefreshTokenExchangeDriver struct {
	Type            string            `yaml:"type"`
	TargetPrincipal string            `yaml:"targetPrincipal"`
	Scopes          []string          `yaml:"scopes"`
	LifetimeSeconds int32             `yaml:"lifetimeSeconds"`
	Endpoint        string            `yaml:"endpoint"`
	Params          map[string]string `yaml:"params"`
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
	if err := normalizeCredentialRefreshConfig(&cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func normalizeCredentialRefreshConfig(cfg *config) error {
	if cfg == nil {
		return nil
	}
	for i := range cfg.CredentialRefresh.Targets {
		target := &cfg.CredentialRefresh.Targets[i]
		target.Provider = strings.TrimSpace(target.Provider)
		target.Connection = strings.TrimSpace(target.Connection)
		target.ConnectionID = strings.TrimSpace(target.ConnectionID)
		target.RefreshInterval = strings.TrimSpace(target.RefreshInterval)
		target.RefreshBeforeExpiry = strings.TrimSpace(target.RefreshBeforeExpiry)
		if target.Provider == "" {
			return fmt.Errorf("credentialRefresh.targets[%d].provider is required", i)
		}
		if target.Connection == "" {
			return fmt.Errorf("credentialRefresh.targets[%d].connection is required", i)
		}
		if target.ConnectionID == "" {
			return fmt.Errorf("credentialRefresh.targets[%d].connectionId is required", i)
		}
		interval, err := time.ParseDuration(target.RefreshInterval)
		if err != nil || interval <= 0 {
			if err == nil {
				err = fmt.Errorf("duration must be positive")
			}
			return fmt.Errorf("credentialRefresh.targets[%d].refreshInterval: %w", i, err)
		}
		before, err := time.ParseDuration(target.RefreshBeforeExpiry)
		if err != nil || before <= 0 {
			if err == nil {
				err = fmt.Errorf("duration must be positive")
			}
			return fmt.Errorf("credentialRefresh.targets[%d].refreshBeforeExpiry: %w", i, err)
		}
		target.RefreshIntervalDuration = interval
		target.RefreshBeforeExpiryDuration = before
		target.Auth = target.RawAuth.toProto()
		if target.Auth.GetType() != "oauth2" {
			return fmt.Errorf("credentialRefresh.targets[%d].auth.type must be oauth2", i)
		}
		if strings.TrimSpace(target.Auth.GetTokenUrl()) == "" {
			return fmt.Errorf("credentialRefresh.targets[%d].auth.tokenUrl is required", i)
		}
	}
	return nil
}

func (auth credentialRefreshAuthConfig) toProto() *gestalt.ExternalCredentialAuthConfig {
	drivers := make([]*gestalt.ExternalCredentialTokenExchangeDriver, 0, len(auth.TokenExchangeDrivers))
	for _, driver := range auth.TokenExchangeDrivers {
		drivers = append(drivers, &gestalt.ExternalCredentialTokenExchangeDriver{
			Type:            driver.Type,
			TargetPrincipal: driver.TargetPrincipal,
			Scopes:          append([]string(nil), driver.Scopes...),
			LifetimeSeconds: driver.LifetimeSeconds,
			Endpoint:        driver.Endpoint,
			Params:          cloneStringMap(driver.Params),
		})
	}
	return &gestalt.ExternalCredentialAuthConfig{
		Type:                 auth.Type,
		Token:                auth.Token,
		TokenPrefix:          auth.TokenPrefix,
		GrantType:            auth.GrantType,
		TokenUrl:             auth.TokenURL,
		ClientId:             auth.ClientID,
		ClientSecret:         auth.ClientSecret,
		ClientAuth:           auth.ClientAuth,
		TokenExchange:        auth.TokenExchange,
		Scopes:               append([]string(nil), auth.Scopes...),
		ScopeParam:           auth.ScopeParam,
		ScopeSeparator:       auth.ScopeSeparator,
		TokenParams:          cloneStringMap(auth.TokenParams),
		RefreshParams:        cloneStringMap(auth.RefreshParams),
		AcceptHeader:         auth.AcceptHeader,
		AccessTokenPath:      auth.AccessTokenPath,
		TokenExchangeDrivers: drivers,
	}
}
