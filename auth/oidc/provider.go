package oidc

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt-providers/auth/internal/configutil"
	"golang.org/x/oauth2"
)

const (
	providerVersion      = "0.0.1-alpha.1"
	defaultSessionTTL    = 24 * time.Hour
	defaultDisplayName   = "SSO"
	defaultHTTPTimeout   = 10 * time.Second
)

type discoveryDocument struct {
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
	UserinfoEndpoint      string `json:"userinfo_endpoint"`
}

type config struct {
	IssuerURL      string        `yaml:"issuer_url"`
	ClientID       string        `yaml:"client_id"`
	ClientSecret   string        `yaml:"client_secret"`
	RedirectURL    string        `yaml:"redirect_url"`
	AllowedDomains []string      `yaml:"allowed_domains"`
	Scopes         []string      `yaml:"scopes"`
	SessionTTL     time.Duration `yaml:"session_ttl"`
	PKCE           bool          `yaml:"pkce"`
	DisplayName    string        `yaml:"display_name"`
}

type Provider struct {
	cfg        config
	doc        discoveryDocument
	httpClient *http.Client
}

func New() *Provider {
	return &Provider{httpClient: &http.Client{Timeout: defaultHTTPTimeout}}
}

func (p *Provider) Configure(ctx context.Context, _ string, raw map[string]any) error {
	var cfg config
	if err := configutil.Decode(raw, &cfg); err != nil {
		return fmt.Errorf("oidc auth: %w", err)
	}
	if cfg.IssuerURL == "" {
		return fmt.Errorf("oidc auth: issuer_url is required")
	}
	if cfg.ClientID == "" {
		return fmt.Errorf("oidc auth: client_id is required")
	}
	doc, err := discover(ctx, p.httpClient, cfg.IssuerURL)
	if err != nil {
		return err
	}
	p.cfg = cfg
	p.doc = doc
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	displayName := p.cfg.DisplayName
	if displayName == "" {
		displayName = defaultDisplayName
	}
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindAuth,
		Name:        "oidc",
		DisplayName: displayName,
		Description: "Authenticate users with an OpenID Connect provider.",
		Version:     providerVersion,
	}
}

func (p *Provider) SessionTTL() time.Duration {
	if p.cfg.SessionTTL > 0 {
		return p.cfg.SessionTTL
	}
	return defaultSessionTTL
}

func (p *Provider) BeginLogin(_ context.Context, req gestalt.BeginLoginRequest) (*gestalt.BeginLoginResponse, error) {
	oauthCfg := p.oauthConfig(req.CallbackURL)
	if !p.cfg.PKCE {
		return &gestalt.BeginLoginResponse{
			AuthorizationURL: oauthCfg.AuthCodeURL(req.HostState, oauth2.AccessTypeOffline),
		}, nil
	}

	verifier, err := generateVerifier()
	if err != nil {
		return nil, fmt.Errorf("oidc auth: generate verifier: %w", err)
	}
	challenge := computeS256Challenge(verifier)
	authURL := oauthCfg.AuthCodeURL(
		req.HostState,
		oauth2.AccessTypeOffline,
		oauth2.SetAuthURLParam("code_challenge", challenge),
		oauth2.SetAuthURLParam("code_challenge_method", "S256"),
	)
	return &gestalt.BeginLoginResponse{
		AuthorizationURL: authURL,
		ProviderState:    []byte(verifier),
	}, nil
}

func (p *Provider) CompleteLogin(ctx context.Context, req gestalt.CompleteLoginRequest) (*gestalt.AuthenticatedUser, error) {
	oauthCfg := p.oauthConfig(req.CallbackURL)
	ctx = context.WithValue(ctx, oauth2.HTTPClient, p.httpClient)
	opts := []oauth2.AuthCodeOption{}
	if p.cfg.PKCE {
		if len(req.ProviderState) == 0 {
			return nil, fmt.Errorf("oidc auth: provider state is required when pkce is enabled")
		}
		opts = append(opts, oauth2.SetAuthURLParam("code_verifier", string(req.ProviderState)))
	}
	tok, err := oauthCfg.Exchange(ctx, req.Query["code"], opts...)
	if err != nil {
		return nil, fmt.Errorf("oidc auth: exchange code: %w", err)
	}
	return p.fetchUserInfo(ctx, tok.AccessToken)
}

func (p *Provider) ValidateExternalToken(ctx context.Context, token string) (*gestalt.AuthenticatedUser, error) {
	if token == "" {
		return nil, fmt.Errorf("oidc auth: token is required")
	}
	return p.fetchUserInfo(ctx, token)
}

func (p *Provider) oauthConfig(callbackURL string) *oauth2.Config {
	redirectURL := p.cfg.RedirectURL
	if redirectURL == "" {
		redirectURL = callbackURL
	}
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

func (p *Provider) fetchUserInfo(ctx context.Context, token string) (*gestalt.AuthenticatedUser, error) {
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
	if !emailVerified(info.EmailVerified) {
		return nil, fmt.Errorf("oidc auth: email %s is not verified", info.Email)
	}
	if err := checkAllowedDomains(p.cfg.AllowedDomains, info.Email); err != nil {
		return nil, err
	}
	return &gestalt.AuthenticatedUser{
		Subject:       info.Sub,
		Email:         info.Email,
		EmailVerified: true,
		DisplayName:   info.Name,
		AvatarURL:     info.Picture,
	}, nil
}

func discover(ctx context.Context, client *http.Client, issuerURL string) (discoveryDocument, error) {
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
	return doc, nil
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

func emailVerified(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return !strings.EqualFold(v, "false")
	default:
		return false
	}
}

func checkAllowedDomains(allowed []string, email string) error {
	if len(allowed) == 0 {
		return nil
	}
	at := strings.LastIndex(email, "@")
	if at < 0 || at == len(email)-1 {
		return fmt.Errorf("oidc auth: invalid email %q", email)
	}
	domain := strings.ToLower(email[at+1:])
	for _, allowedDomain := range allowed {
		if strings.EqualFold(strings.TrimSpace(allowedDomain), domain) {
			return nil
		}
	}
	return fmt.Errorf("oidc auth: email domain %q is not allowed", domain)
}
