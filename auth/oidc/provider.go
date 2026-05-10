package oidc

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/valon-technologies/gestalt-providers/auth/internal/configutil"
	"github.com/valon-technologies/gestalt-providers/auth/internal/userinfo"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"golang.org/x/oauth2"
)

const (
	providerVersion             = "0.0.1-alpha.4"
	defaultSessionTTL           = 24 * time.Hour
	defaultDisplayName          = "SSO"
	defaultHTTPTimeout          = 10 * time.Second
	defaultPKCEVerifierTTL      = time.Hour
	defaultPKCEVerifierMaxItems = 10_000
)

type discoveryDocument struct {
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
	UserinfoEndpoint      string `json:"userinfo_endpoint"`
}

type config struct {
	IssuerURL            string         `yaml:"issuerUrl"`
	ClientID             string         `yaml:"clientId"`
	ClientSecret         string         `yaml:"clientSecret"`
	RedirectURL          string         `yaml:"redirectUrl"`
	AllowedDomains       []string       `yaml:"allowedDomains"`
	Scopes               []string       `yaml:"scopes"`
	SessionTTL           time.Duration  `yaml:"sessionTtl"`
	PKCE                 bool           `yaml:"pkce"`
	DisplayName          string         `yaml:"displayName"`
	AllowInsecureHTTP    bool           `yaml:"allowInsecureHttp"`
	PKCEVerifierTTL      time.Duration  `yaml:"pkceVerifierTtl"`
	PKCEVerifierMaxItems int            `yaml:"pkceVerifierMaxItems"`
	TenantConfig         tenantSettings `yaml:"tenantConfig"`
	TenantSettings       tenantSettings `yaml:"tenantSettings"`
}

type tenantSettings struct {
	Source  string                    `yaml:"source"`
	Tenants map[string]tenantOIDCInfo `yaml:"tenants"`
}

type tenantOIDCInfo struct {
	RedirectURL    string   `yaml:"redirectUrl"`
	AllowedDomains []string `yaml:"allowedDomains"`
}

type resolvedOIDCInfo struct {
	TenantID       string
	Host           string
	RedirectURL    string
	AllowedDomains []string
	TenantScoped   bool
}

type pkceVerifierEntry struct {
	verifier  string
	tenantID  string
	host      string
	expiresAt time.Time
}

type Provider struct {
	cfg                  config
	doc                  discoveryDocument
	httpClient           *http.Client
	pkceMu               sync.Mutex
	pkceVerifiers        map[string]pkceVerifierEntry
	pkceVerifierTTL      time.Duration
	pkceVerifierMaxItems int
	now                  func() time.Time
}

func New() *Provider {
	return &Provider{
		httpClient:           &http.Client{Timeout: defaultHTTPTimeout},
		pkceVerifiers:        make(map[string]pkceVerifierEntry),
		pkceVerifierTTL:      defaultPKCEVerifierTTL,
		pkceVerifierMaxItems: defaultPKCEVerifierMaxItems,
		now:                  time.Now,
	}
}

func (p *Provider) Configure(ctx context.Context, _ string, raw map[string]any) error {
	var cfg config
	if err := configutil.Decode(raw, &cfg); err != nil {
		return fmt.Errorf("oidc auth: %w", err)
	}
	if cfg.IssuerURL == "" {
		return fmt.Errorf("oidc auth: issuerUrl is required")
	}
	if cfg.ClientID == "" {
		return fmt.Errorf("oidc auth: clientId is required")
	}
	if _, ok := raw["pkceVerifierTtl"]; ok && cfg.PKCEVerifierTTL <= 0 {
		return fmt.Errorf("oidc auth: pkceVerifierTtl must be greater than 0 when set")
	}
	if _, ok := raw["pkceVerifierMaxItems"]; ok && cfg.PKCEVerifierMaxItems <= 0 {
		return fmt.Errorf("oidc auth: pkceVerifierMaxItems must be greater than 0 when set")
	}
	if err := validateEndpointURL("issuerUrl", cfg.IssuerURL, cfg.AllowInsecureHTTP); err != nil {
		return err
	}
	cfg.TenantSettings = cfg.mergedTenantSettings()
	if err := validateTenantSettings(cfg); err != nil {
		return err
	}
	doc, err := discover(ctx, p.httpClient, cfg.IssuerURL, cfg.AllowInsecureHTTP)
	if err != nil {
		return err
	}
	p.cfg = cfg
	p.doc = doc
	if cfg.PKCEVerifierTTL > 0 {
		p.pkceVerifierTTL = cfg.PKCEVerifierTTL
	} else {
		p.pkceVerifierTTL = defaultPKCEVerifierTTL
	}
	if cfg.PKCEVerifierMaxItems > 0 {
		p.pkceVerifierMaxItems = cfg.PKCEVerifierMaxItems
	} else {
		p.pkceVerifierMaxItems = defaultPKCEVerifierMaxItems
	}
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	displayName := p.cfg.DisplayName
	if displayName == "" {
		displayName = defaultDisplayName
	}
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindAuthentication,
		Name:        "oidc",
		DisplayName: displayName,
		Description: "Authenticate users with an OpenID Connect provider.",
		Version:     providerVersion,
	}
}

func (cfg config) mergedTenantSettings() tenantSettings {
	if !cfg.TenantConfig.isZero() {
		return cfg.TenantConfig
	}
	return cfg.TenantSettings
}

func (s tenantSettings) isZero() bool {
	return strings.TrimSpace(s.Source) == "" && len(s.Tenants) == 0
}

func (p *Provider) SessionTTL() time.Duration {
	if p.cfg.SessionTTL > 0 {
		return p.cfg.SessionTTL
	}
	return defaultSessionTTL
}

func (p *Provider) BeginLogin(ctx context.Context, req *gestalt.BeginLoginRequest) (*gestalt.BeginLoginResponse, error) {
	resolved, err := p.resolveOIDCInfo(ctx, req.CallbackUrl)
	if err != nil {
		return nil, err
	}
	oauthCfg := p.oauthConfig(resolved.RedirectURL)
	if !p.cfg.PKCE {
		return &gestalt.BeginLoginResponse{
			AuthorizationUrl: oauthCfg.AuthCodeURL(req.HostState, oauth2.AccessTypeOffline),
		}, nil
	}

	verifier, err := generateVerifier()
	if err != nil {
		return nil, fmt.Errorf("oidc auth: generate verifier: %w", err)
	}
	if req.HostState == "" {
		return nil, fmt.Errorf("oidc auth: host state is required when pkce is enabled")
	}
	if err := p.storePKCEVerifier(resolved, req.HostState, verifier); err != nil {
		return nil, err
	}
	challenge := computeS256Challenge(verifier)
	authURL := oauthCfg.AuthCodeURL(
		req.HostState,
		oauth2.AccessTypeOffline,
		oauth2.SetAuthURLParam("code_challenge", challenge),
		oauth2.SetAuthURLParam("code_challenge_method", "S256"),
	)
	return &gestalt.BeginLoginResponse{
		AuthorizationUrl: authURL,
	}, nil
}

func (p *Provider) CompleteLogin(ctx context.Context, req *gestalt.CompleteLoginRequest) (*gestalt.AuthenticatedUser, error) {
	resolved, err := p.resolveOIDCInfo(ctx, req.CallbackUrl)
	if err != nil {
		return nil, err
	}
	oauthCfg := p.oauthConfig(resolved.RedirectURL)
	ctx = context.WithValue(ctx, oauth2.HTTPClient, p.httpClient)
	opts := []oauth2.AuthCodeOption{}
	pkceState := ""
	if p.cfg.PKCE {
		pkceState = req.Query["state"]
		if pkceState == "" {
			return nil, fmt.Errorf("oidc auth: state is required when pkce is enabled")
		}
		verifier, ok := p.pkceVerifier(resolved, pkceState)
		if !ok {
			return nil, fmt.Errorf("oidc auth: pkce verifier not found for state")
		}
		opts = append(opts, oauth2.SetAuthURLParam("code_verifier", verifier))
	}
	tok, err := oauthCfg.Exchange(ctx, req.Query["code"], opts...)
	if err != nil {
		return nil, fmt.Errorf("oidc auth: exchange code: %w", err)
	}
	if pkceState != "" {
		p.deletePKCEVerifier(resolved, pkceState)
	}
	return p.fetchUserInfo(ctx, tok.AccessToken, resolved.AllowedDomains)
}

func (p *Provider) ValidateExternalToken(ctx context.Context, token string) (*gestalt.AuthenticatedUser, error) {
	if token == "" {
		return nil, fmt.Errorf("oidc auth: token is required")
	}
	resolved, err := p.resolveOIDCInfo(ctx, "")
	if err != nil {
		return nil, err
	}
	return p.fetchUserInfo(ctx, token, resolved.AllowedDomains)
}

func (p *Provider) oauthConfig(redirectURL string) *oauth2.Config {
	scopes := p.cfg.Scopes
	if len(scopes) == 0 {
		scopes = []string{"openid", "email", "profile"}
	}
	return &oauth2.Config{
		ClientID:     p.cfg.ClientID,
		ClientSecret: p.cfg.ClientSecret,
		RedirectURL:  redirectURL,
		Scopes:       append([]string(nil), scopes...),
		Endpoint: oauth2.Endpoint{
			AuthURL:  p.doc.AuthorizationEndpoint,
			TokenURL: p.doc.TokenEndpoint,
		},
	}
}

func (p *Provider) resolveOIDCInfo(ctx context.Context, callbackURL string) (resolvedOIDCInfo, error) {
	tenantScoped := strings.EqualFold(strings.TrimSpace(p.cfg.TenantSettings.Source), "config") || len(p.cfg.TenantSettings.Tenants) > 0
	if !tenantScoped {
		redirectURL := p.cfg.RedirectURL
		if redirectURL == "" {
			redirectURL = callbackURL
		}
		return resolvedOIDCInfo{
			RedirectURL:    redirectURL,
			AllowedDomains: append([]string(nil), p.cfg.AllowedDomains...),
		}, nil
	}

	scope, ok := tenantScopeFromContext(ctx)
	if !ok || scope.TenantID == "" || !scope.TenantBound {
		return resolvedOIDCInfo{}, fmt.Errorf("oidc auth: tenant scope is required")
	}
	info, ok := p.cfg.TenantSettings.Tenants[scope.TenantID]
	if !ok {
		return resolvedOIDCInfo{}, fmt.Errorf("oidc auth: tenant %q is not configured", scope.TenantID)
	}
	redirectURL := strings.TrimSpace(info.RedirectURL)
	if redirectURL == "" {
		redirectURL = callbackURL
	}
	if err := validateTenantHost(scope, redirectURL); err != nil {
		return resolvedOIDCInfo{}, err
	}
	return resolvedOIDCInfo{
		TenantID:       scope.TenantID,
		Host:           scope.Host,
		RedirectURL:    redirectURL,
		AllowedDomains: append([]string(nil), info.AllowedDomains...),
		TenantScoped:   true,
	}, nil
}

func (p *Provider) fetchUserInfo(ctx context.Context, token string, allowedDomains []string) (*gestalt.AuthenticatedUser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.doc.UserinfoEndpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("oidc auth: create userinfo request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("oidc auth: fetch userinfo: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("oidc auth: userinfo returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var info struct {
		Sub           string `json:"sub"`
		Email         string `json:"email"`
		Name          string `json:"name"`
		Picture       string `json:"picture"`
		EmailVerified any    `json:"email_verified"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("oidc auth: decode userinfo: %w", err)
	}
	if !userinfo.EmailVerified(info.EmailVerified) {
		return nil, fmt.Errorf("oidc auth: email %s is not verified", info.Email)
	}
	if err := userinfo.CheckAllowedDomains("oidc", allowedDomains, info.Email); err != nil {
		return nil, err
	}
	return &gestalt.AuthenticatedUser{
		Subject:       info.Sub,
		Email:         info.Email,
		EmailVerified: true,
		DisplayName:   info.Name,
		AvatarUrl:     info.Picture,
	}, nil
}

func validateTenantSettings(cfg config) error {
	if !strings.EqualFold(strings.TrimSpace(cfg.TenantSettings.Source), "config") && len(cfg.TenantSettings.Tenants) == 0 {
		return nil
	}
	for tenantID, info := range cfg.TenantSettings.Tenants {
		tenantID = strings.TrimSpace(tenantID)
		if tenantID == "" {
			return fmt.Errorf("oidc auth: tenantSettings tenant id is required")
		}
		if info.RedirectURL != "" {
			if err := validateEndpointURL("tenantSettings.redirectUrl", info.RedirectURL, cfg.AllowInsecureHTTP); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateTenantHost(scope tenantScope, redirectURL string) error {
	if scope.Host == "" || redirectURL == "" {
		return nil
	}
	parsed, err := url.Parse(redirectURL)
	if err != nil || parsed.Hostname() == "" {
		return nil
	}
	redirectHost := strings.ToLower(strings.Trim(parsed.Host, "."))
	if redirectHost != scope.Host {
		return fmt.Errorf("oidc auth: tenant %q resolved for host %q cannot use redirect host %q", scope.TenantID, scope.Host, redirectHost)
	}
	return nil
}

func discover(ctx context.Context, client *http.Client, issuerURL string, allowInsecureHTTP bool) (discoveryDocument, error) {
	wellKnown := strings.TrimRight(issuerURL, "/") + "/.well-known/openid-configuration"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, wellKnown, nil)
	if err != nil {
		return discoveryDocument{}, fmt.Errorf("oidc auth: build discovery request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return discoveryDocument{}, fmt.Errorf("oidc auth: discover issuer: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return discoveryDocument{}, fmt.Errorf("oidc auth: discovery returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var doc discoveryDocument
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return discoveryDocument{}, fmt.Errorf("oidc auth: decode discovery document: %w", err)
	}
	if doc.AuthorizationEndpoint == "" || doc.TokenEndpoint == "" || doc.UserinfoEndpoint == "" {
		return discoveryDocument{}, fmt.Errorf("oidc auth: discovery document is missing required endpoints")
	}
	if err := validateEndpointURL("authorization_endpoint", doc.AuthorizationEndpoint, allowInsecureHTTP); err != nil {
		return discoveryDocument{}, err
	}
	if err := validateEndpointURL("token_endpoint", doc.TokenEndpoint, allowInsecureHTTP); err != nil {
		return discoveryDocument{}, err
	}
	if err := validateEndpointURL("userinfo_endpoint", doc.UserinfoEndpoint, allowInsecureHTTP); err != nil {
		return discoveryDocument{}, err
	}
	return doc, nil
}

func validateEndpointURL(fieldName, rawURL string, allowInsecureHTTP bool) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("oidc auth: %s must be a valid URL: %w", fieldName, err)
	}
	if parsed.Scheme == "" {
		return fmt.Errorf("oidc auth: %s must be a valid absolute URL", fieldName)
	}
	if parsed.Hostname() == "" {
		return fmt.Errorf("oidc auth: %s must include a host", fieldName)
	}

	switch parsed.Scheme {
	case "https":
		return nil
	case "http":
		if !allowInsecureHTTP {
			return fmt.Errorf("oidc auth: %s must use https unless allowInsecureHttp is true for loopback/local development", fieldName)
		}
		if !isLoopbackHost(parsed.Hostname()) {
			return fmt.Errorf("oidc auth: %s may use http only for loopback/local development hosts when allowInsecureHttp is true", fieldName)
		}
		return nil
	default:
		return fmt.Errorf("oidc auth: %s must use https", fieldName)
	}
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func generateVerifier() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

func computeS256Challenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func (p *Provider) storePKCEVerifier(resolved resolvedOIDCInfo, hostState, verifier string) error {
	p.pkceMu.Lock()
	defer p.pkceMu.Unlock()
	now := p.currentTime()
	p.evictExpiredPKCEVerifiersLocked(now)
	key := pkceVerifierKey(resolved, hostState)
	if _, ok := p.pkceVerifiers[key]; !ok && len(p.pkceVerifiers) >= p.maxPKCEVerifierItems() {
		return fmt.Errorf("oidc auth: too many in-flight PKCE login attempts; increase pkceVerifierMaxItems or wait for older attempts to complete")
	}
	p.pkceVerifiers[key] = pkceVerifierEntry{
		verifier:  verifier,
		tenantID:  resolved.TenantID,
		host:      resolved.Host,
		expiresAt: now.Add(p.pkceTTL()),
	}
	return nil
}

func (p *Provider) pkceVerifier(resolved resolvedOIDCInfo, hostState string) (string, bool) {
	p.pkceMu.Lock()
	defer p.pkceMu.Unlock()
	now := p.currentTime()
	p.evictExpiredPKCEVerifiersLocked(now)
	key := pkceVerifierKey(resolved, hostState)
	entry, ok := p.pkceVerifiers[key]
	if !ok {
		return "", false
	}
	if !entry.expiresAt.After(now) {
		delete(p.pkceVerifiers, key)
		return "", false
	}
	return entry.verifier, true
}

func (p *Provider) deletePKCEVerifier(resolved resolvedOIDCInfo, hostState string) {
	p.pkceMu.Lock()
	defer p.pkceMu.Unlock()
	delete(p.pkceVerifiers, pkceVerifierKey(resolved, hostState))
}

func (p *Provider) evictExpiredPKCEVerifiersLocked(now time.Time) {
	for hostState, entry := range p.pkceVerifiers {
		if !entry.expiresAt.After(now) {
			delete(p.pkceVerifiers, hostState)
		}
	}
}

func pkceVerifierKey(resolved resolvedOIDCInfo, hostState string) string {
	if !resolved.TenantScoped {
		return hostState
	}
	return resolved.TenantID + "\x00" + resolved.Host + "\x00" + hostState
}

func (p *Provider) pkceTTL() time.Duration {
	if p.pkceVerifierTTL > 0 {
		return p.pkceVerifierTTL
	}
	return defaultPKCEVerifierTTL
}

func (p *Provider) maxPKCEVerifierItems() int {
	if p.pkceVerifierMaxItems > 0 {
		return p.pkceVerifierMaxItems
	}
	return defaultPKCEVerifierMaxItems
}

func (p *Provider) currentTime() time.Time {
	if p.now != nil {
		return p.now()
	}
	return time.Now()
}

var _ gestalt.AuthenticationProvider = (*Provider)(nil)
var _ gestalt.ExternalTokenValidator = (*Provider)(nil)
var _ gestalt.SessionTTLProvider = (*Provider)(nil)
var _ gestalt.MetadataProvider = (*Provider)(nil)
