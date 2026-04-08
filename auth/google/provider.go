package google

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt-providers/auth/internal/configutil"
	"golang.org/x/oauth2"
	googleoauth "golang.org/x/oauth2/google"
)

const (
	providerVersion   = "0.0.1-alpha.1"
	defaultSessionTTL = 24 * time.Hour
	userinfoURL       = "https://www.googleapis.com/oauth2/v3/userinfo"
)

type config struct {
	ClientID       string        `yaml:"client_id"`
	ClientSecret   string        `yaml:"client_secret"`
	RedirectURL    string        `yaml:"redirect_url"`
	AllowedDomains []string      `yaml:"allowed_domains"`
	SessionTTL     time.Duration `yaml:"session_ttl"`
}

type Provider struct {
	cfg        config
	httpClient *http.Client
}

func New() *Provider {
	return &Provider{httpClient: &http.Client{Timeout: 10 * time.Second}}
}

func (p *Provider) Configure(_ context.Context, _ string, raw map[string]any) error {
	var cfg config
	if err := configutil.Decode(raw, &cfg); err != nil {
		return fmt.Errorf("google auth: %w", err)
	}
	if cfg.ClientID == "" {
		return fmt.Errorf("google auth: client_id is required")
	}
	if cfg.ClientSecret == "" {
		return fmt.Errorf("google auth: client_secret is required")
	}
	p.cfg = cfg
	return nil
}

func (p *Provider) Metadata() gestalt.ProviderMetadata {
	return gestalt.ProviderMetadata{
		Kind:        gestalt.ProviderKindAuth,
		Name:        "google",
		DisplayName: "Google",
		Description: "Authenticate users with Google OAuth and validate Google bearer tokens.",
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
	return &gestalt.BeginLoginResponse{
		AuthorizationURL: oauthCfg.AuthCodeURL(req.HostState, oauth2.AccessTypeOffline),
	}, nil
}

func (p *Provider) CompleteLogin(ctx context.Context, req gestalt.CompleteLoginRequest) (*gestalt.AuthenticatedUser, error) {
	oauthCfg := p.oauthConfig(req.CallbackURL)
	ctx = context.WithValue(ctx, oauth2.HTTPClient, p.httpClient)
	tok, err := oauthCfg.Exchange(ctx, req.Query["code"])
	if err != nil {
		return nil, fmt.Errorf("google auth: exchange code: %w", err)
	}
	return p.fetchUserInfo(ctx, tok.AccessToken)
}

func (p *Provider) ValidateExternalToken(ctx context.Context, token string) (*gestalt.AuthenticatedUser, error) {
	if token == "" {
		return nil, fmt.Errorf("google auth: token is required")
	}
	return p.fetchUserInfo(ctx, token)
}

func (p *Provider) oauthConfig(callbackURL string) *oauth2.Config {
	redirectURL := p.cfg.RedirectURL
	if redirectURL == "" {
		redirectURL = callbackURL
	}
	return &oauth2.Config{
		ClientID:     p.cfg.ClientID,
		ClientSecret: p.cfg.ClientSecret,
		RedirectURL:  redirectURL,
		Scopes:       []string{"openid", "email", "profile"},
		Endpoint:     googleoauth.Endpoint,
	}
}

func (p *Provider) fetchUserInfo(ctx context.Context, token string) (*gestalt.AuthenticatedUser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, userinfoURL, nil)
	if err != nil {
		return nil, fmt.Errorf("google auth: create userinfo request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("google auth: fetch userinfo: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("google auth: userinfo returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var info struct {
		Sub           string `json:"sub"`
		Email         string `json:"email"`
		Name          string `json:"name"`
		Picture       string `json:"picture"`
		EmailVerified any    `json:"email_verified"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("google auth: decode userinfo: %w", err)
	}
	if !emailVerified(info.EmailVerified) {
		return nil, fmt.Errorf("google auth: email %s is not verified", info.Email)
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

func emailVerified(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(v, "true")
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
		return fmt.Errorf("google auth: invalid email %q", email)
	}
	domain := strings.ToLower(email[at+1:])
	for _, allowedDomain := range allowed {
		if strings.EqualFold(strings.TrimSpace(allowedDomain), domain) {
			return nil
		}
	}
	return fmt.Errorf("google auth: email domain %q is not allowed", domain)
}
