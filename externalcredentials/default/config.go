package externalcredentials

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"gopkg.in/yaml.v3"
)

type config struct {
	IndexedDB           string                     `yaml:"indexeddb"`
	EncryptionKey       string                     `yaml:"encryptionKey"`
	ResolvedConnections []resolvedConnectionConfig `yaml:"resolvedConnections"`
	RefreshTargets      []credentialRefreshTarget  `yaml:"-"`
}

type resolvedConnectionConfig struct {
	Provider          string                       `yaml:"provider"`
	Connection        string                       `yaml:"connection"`
	ConnectionID      string                       `yaml:"connectionId"`
	Mode              string                       `yaml:"mode"`
	Auth              resolvedConnectionAuthConfig `yaml:"auth"`
	ConnectionParams  map[string]string            `yaml:"connectionParams"`
	CredentialRefresh *credentialRefreshConfig     `yaml:"credentialRefresh"`
}

type credentialRefreshConfig struct {
	RefreshInterval     string `yaml:"refreshInterval"`
	RefreshBeforeExpiry string `yaml:"refreshBeforeExpiry"`
}

type credentialRefreshTarget struct {
	Provider                    string
	Connection                  string
	ConnectionID                string
	RefreshIntervalDuration     time.Duration
	RefreshBeforeExpiryDuration time.Duration
	Auth                        *gestalt.ExternalCredentialAuthConfig
	ConnectionParams            map[string]string
}

type resolvedConnectionAuthConfig struct {
	Type                 string                                  `yaml:"type"`
	Token                string                                  `yaml:"token"`
	TokenPrefix          string                                  `yaml:"tokenPrefix"`
	GrantType            string                                  `yaml:"grantType"`
	RefreshToken         string                                  `yaml:"refreshToken"`
	TokenURL             string                                  `yaml:"tokenUrl"`
	ClientID             string                                  `yaml:"clientId"`
	ClientSecret         string                                  `yaml:"clientSecret"`
	ClientAuth           string                                  `yaml:"clientAuth"`
	TokenExchange        string                                  `yaml:"tokenExchange"`
	Scopes               []string                                `yaml:"scopes"`
	ScopeParam           string                                  `yaml:"scopeParam"`
	ScopeSeparator       string                                  `yaml:"scopeSeparator"`
	TokenParams          map[string]string                       `yaml:"tokenParams"`
	RefreshParams        map[string]string                       `yaml:"refreshParams"`
	AcceptHeader         string                                  `yaml:"acceptHeader"`
	AccessTokenPath      string                                  `yaml:"accessTokenPath"`
	TokenExchangeDrivers []resolvedConnectionTokenExchangeDriver `yaml:"tokenExchangeDrivers"`
}

type resolvedConnectionTokenExchangeDriver struct {
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
	targets, err := buildCredentialRefreshTargets(cfg.ResolvedConnections)
	if err != nil {
		return cfg, err
	}
	cfg.RefreshTargets = targets
	return cfg, nil
}

func buildCredentialRefreshTargets(connections []resolvedConnectionConfig) ([]credentialRefreshTarget, error) {
	targetsByConnectionID := map[string]credentialRefreshTarget{}
	for i := range connections {
		conn := connections[i]
		if conn.CredentialRefresh == nil {
			continue
		}
		target, err := credentialRefreshTargetFromResolvedConnection(i, conn)
		if err != nil {
			return nil, err
		}
		existing, ok := targetsByConnectionID[target.ConnectionID]
		if !ok {
			targetsByConnectionID[target.ConnectionID] = target
			continue
		}
		merged, err := mergeCredentialRefreshTarget(existing, target)
		if err != nil {
			return nil, err
		}
		targetsByConnectionID[target.ConnectionID] = merged
	}
	if len(targetsByConnectionID) == 0 {
		return nil, nil
	}
	targets := make([]credentialRefreshTarget, 0, len(targetsByConnectionID))
	for _, target := range targetsByConnectionID {
		targets = append(targets, target)
	}
	return targets, nil
}

func credentialRefreshTargetFromResolvedConnection(index int, conn resolvedConnectionConfig) (credentialRefreshTarget, error) {
	conn.Provider = strings.TrimSpace(conn.Provider)
	conn.Connection = strings.TrimSpace(conn.Connection)
	conn.ConnectionID = strings.TrimSpace(conn.ConnectionID)
	conn.Mode = strings.TrimSpace(conn.Mode)
	if conn.Provider == "" {
		return credentialRefreshTarget{}, fmt.Errorf("resolvedConnections[%d].provider is required", index)
	}
	if conn.Connection == "" {
		return credentialRefreshTarget{}, fmt.Errorf("resolvedConnections[%d].connection is required", index)
	}
	if conn.ConnectionID == "" {
		return credentialRefreshTarget{}, fmt.Errorf("resolvedConnections[%d].connectionId is required", index)
	}
	switch conn.Mode {
	case "subject", "user":
	default:
		return credentialRefreshTarget{}, fmt.Errorf("resolvedConnections[%d].credentialRefresh requires mode subject", index)
	}
	refresh := conn.CredentialRefresh
	interval, err := parsePositiveDuration(refresh.RefreshInterval)
	if err != nil {
		return credentialRefreshTarget{}, fmt.Errorf("resolvedConnections[%d].credentialRefresh.refreshInterval: %w", index, err)
	}
	before, err := parsePositiveDuration(refresh.RefreshBeforeExpiry)
	if err != nil {
		return credentialRefreshTarget{}, fmt.Errorf("resolvedConnections[%d].credentialRefresh.refreshBeforeExpiry: %w", index, err)
	}
	auth := conn.Auth.authConfig()
	if err := validateCredentialAuthConfig(conn.Mode, auth); err != nil {
		return credentialRefreshTarget{}, fmt.Errorf("resolvedConnections[%d].auth: %w", index, err)
	}
	switch auth.GetType() {
	case "oauth2", "manual":
	default:
		return credentialRefreshTarget{}, fmt.Errorf("resolvedConnections[%d].credentialRefresh supports auth.type oauth2 or manual, got %q", index, auth.GetType())
	}
	if strings.TrimSpace(auth.GetTokenUrl()) == "" {
		return credentialRefreshTarget{}, fmt.Errorf("resolvedConnections[%d].auth.tokenUrl is required for credentialRefresh", index)
	}
	if auth.GetType() == "oauth2" {
		if strings.TrimSpace(auth.GetClientId()) == "" {
			return credentialRefreshTarget{}, fmt.Errorf("resolvedConnections[%d].auth.clientId is required for credentialRefresh", index)
		}
		if strings.TrimSpace(auth.GetClientSecret()) == "" {
			return credentialRefreshTarget{}, fmt.Errorf("resolvedConnections[%d].auth.clientSecret is required for credentialRefresh", index)
		}
	}
	return credentialRefreshTarget{
		Provider:                    conn.Provider,
		Connection:                  conn.Connection,
		ConnectionID:                conn.ConnectionID,
		RefreshIntervalDuration:     interval,
		RefreshBeforeExpiryDuration: before,
		Auth:                        auth,
		ConnectionParams:            cloneStringMap(conn.ConnectionParams),
	}, nil
}

func parsePositiveDuration(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, err
	}
	if duration <= 0 {
		return 0, fmt.Errorf("duration must be positive")
	}
	return duration, nil
}

func mergeCredentialRefreshTarget(existing, next credentialRefreshTarget) (credentialRefreshTarget, error) {
	if !credentialRefreshTargetsCompatible(existing, next) {
		return credentialRefreshTarget{}, fmt.Errorf("resolvedConnections duplicate connectionId %q has conflicting credential refresh config", existing.ConnectionID)
	}
	merged := existing
	if next.RefreshIntervalDuration < merged.RefreshIntervalDuration {
		merged.RefreshIntervalDuration = next.RefreshIntervalDuration
	}
	if next.RefreshBeforeExpiryDuration > merged.RefreshBeforeExpiryDuration {
		merged.RefreshBeforeExpiryDuration = next.RefreshBeforeExpiryDuration
	}
	return merged, nil
}

func credentialRefreshTargetsCompatible(a, b credentialRefreshTarget) bool {
	return refreshAuthComparable(a.Auth) == refreshAuthComparable(b.Auth) &&
		reflect.DeepEqual(a.Auth.GetRefreshParams(), b.Auth.GetRefreshParams()) &&
		reflect.DeepEqual(a.Auth.GetTokenParams(), b.Auth.GetTokenParams()) &&
		reflect.DeepEqual(a.ConnectionParams, b.ConnectionParams)
}

type credentialRefreshAuthComparable struct {
	Type            string
	GrantType       string
	TokenURL        string
	ClientID        string
	ClientSecret    string
	RefreshToken    string
	ClientAuth      string
	TokenExchange   string
	AcceptHeader    string
	AccessTokenPath string
}

func refreshAuthComparable(auth *gestalt.ExternalCredentialAuthConfig) credentialRefreshAuthComparable {
	if auth == nil {
		return credentialRefreshAuthComparable{}
	}
	return credentialRefreshAuthComparable{
		Type:            auth.GetType(),
		GrantType:       auth.GetGrantType(),
		TokenURL:        auth.GetTokenUrl(),
		ClientID:        auth.GetClientId(),
		ClientSecret:    auth.GetClientSecret(),
		RefreshToken:    auth.GetRefreshToken(),
		ClientAuth:      auth.GetClientAuth(),
		TokenExchange:   auth.GetTokenExchange(),
		AcceptHeader:    auth.GetAcceptHeader(),
		AccessTokenPath: auth.GetAccessTokenPath(),
	}
}

func (auth resolvedConnectionAuthConfig) authConfig() *gestalt.ExternalCredentialAuthConfig {
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
		RefreshToken:         auth.RefreshToken,
		TokenURL:             auth.TokenURL,
		ClientID:             auth.ClientID,
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
