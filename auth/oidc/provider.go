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
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	"golang.org/x/oauth2"
)

const (
	providerVersion             = "0.0.1-alpha.1"
	defaultSessionTTL           = 24 * time.Hour
	defaultDisplayName          = "SSO"
	defaultHTTPTimeout          = 10 * time.Second
	defaultPKCEVerifierTTL      = time.Hour
	defaultPKCEVerifierMaxItems = 10_000
	grantTypeAuthorizationCode  = "authorization_code"
	grantTypeTokenExchange      = "urn:ietf:params:oauth:grant-type:token-exchange"
	subjectTokenTypeAccessToken = "urn:ietf:params:oauth:token-type:access_token"
)

type discoveryDocument struct {
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
	UserinfoEndpoint      string `json:"userinfo_endpoint"`
}

type config struct {
	IssuerURL            string        `yaml:"issuerUrl"`
	ClientID             string        `yaml:"clientId"`
	ClientSecret         string        `yaml:"clientSecret"`
	RedirectURL          string        `yaml:"redirectUrl"`
	AllowedDomains       []string      `yaml:"allowedDomains"`
	Scopes               []string      `yaml:"scopes"`
	SessionTTL           time.Duration `yaml:"sessionTtl"`
	PKCE                 bool          `yaml:"pkce"`
	DisplayName          string        `yaml:"displayName"`
	AllowInsecureHTTP    bool          `yaml:"allowInsecureHttp"`
	PKCEVerifierTTL      time.Duration `yaml:"pkceVerifierTtl"`
	PKCEVerifierMaxItems int           `yaml:"pkceVerifierMaxItems"`
	IndexedDB            string        `yaml:"indexeddb"`
}

type pkceVerifierEntry struct {
	verifier  string
	expiresAt time.Time
}

type pendingOAuthSession struct {
	state        string
	redirectURI  string
	clientID     string
	scope        string
	pkceVerifier string
	expiresAt    time.Time
}

type authenticatedUserInfo struct {
	Sub         string
	Email       string
	DisplayName string
	AvatarURL   string
}

type Provider struct {
	cfg                  config
	doc                  discoveryDocument
	httpClient           *http.Client
	grants               *grantStore
	grantsDB             indexeddb.Database
	openIndexedDB        func(context.Context, ...string) (indexeddb.Database, error)
	pkceMu               sync.Mutex
	pkceVerifiers        map[string]pkceVerifierEntry
	pendingMu            sync.Mutex
	pendingOAuth         map[string]pendingOAuthSession
	pkceVerifierTTL      time.Duration
	pkceVerifierMaxItems int
	now                  func() time.Time
}

func New() *Provider {
	return &Provider{
		httpClient:           &http.Client{Timeout: defaultHTTPTimeout},
		pkceVerifiers:        make(map[string]pkceVerifierEntry),
		pendingOAuth:         make(map[string]pendingOAuthSession),
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
	doc, err := discover(ctx, p.httpClient, cfg.IssuerURL, cfg.AllowInsecureHTTP)
	if err != nil {
		return err
	}
	p.cfg = cfg
	p.doc = doc
	binding := strings.TrimSpace(cfg.IndexedDB)
	if binding == "" {
		return fmt.Errorf("oidc auth: indexeddb is required")
	}
	open := gestalt.IndexedDB
	if p.openIndexedDB != nil {
		open = p.openIndexedDB
	}
	if p.grantsDB != nil {
		if err := p.closeGrantsDB(); err != nil {
			return fmt.Errorf("oidc auth: close indexeddb: %w", err)
		}
	}
	db, err := open(ctx, binding)
	if err != nil {
		return fmt.Errorf("oidc auth: open indexeddb: %w", err)
	}
	grants, err := openGrantStore(ctx, db, p.now)
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("oidc auth: open grant store: %w", err)
	}
	p.grantsDB = db
	p.grants = grants
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

func (p *Provider) SessionTTL() time.Duration {
	if p.cfg.SessionTTL > 0 {
		return p.cfg.SessionTTL
	}
	return defaultSessionTTL
}

func (p *Provider) Authorize(_ context.Context, req *gestalt.AuthorizeRequest) (*gestalt.AuthorizeResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("oidc auth: authorize request is required")
	}
	if strings.TrimSpace(req.ResponseType) != "" && req.ResponseType != "code" {
		return nil, fmt.Errorf("oidc auth: unsupported response_type %q", req.ResponseType)
	}
	callbackURL := req.RedirectURI
	if callbackURL == "" {
		return nil, fmt.Errorf("oidc auth: redirect_uri is required")
	}
	state := strings.TrimSpace(req.State)
	if p.cfg.PKCE && state == "" {
		return nil, fmt.Errorf("oidc auth: state is required when pkce is enabled")
	}
	if state == "" {
		generated, err := generateVerifier()
		if err != nil {
			return nil, fmt.Errorf("oidc auth: generate state: %w", err)
		}
		state = generated
	}

	oauthCfg := p.oauthConfig(callbackURL, req.Scope)
	opts := []oauth2.AuthCodeOption{oauth2.AccessTypeOffline}
	pkceVerifier := ""
	if p.cfg.PKCE {
		verifier, err := generateVerifier()
		if err != nil {
			return nil, fmt.Errorf("oidc auth: generate verifier: %w", err)
		}
		if err := p.storePKCEVerifier(state, verifier); err != nil {
			return nil, err
		}
		pkceVerifier = verifier
		challenge := computeS256Challenge(verifier)
		opts = append(opts,
			oauth2.SetAuthURLParam("code_challenge", challenge),
			oauth2.SetAuthURLParam("code_challenge_method", "S256"),
		)
	}

	authURL := oauthCfg.AuthCodeURL(state, opts...)
	if err := p.storePendingOAuth(pendingOAuthSession{
		state:        state,
		redirectURI:  callbackURL,
		clientID:     strings.TrimSpace(req.ClientID),
		scope:        strings.TrimSpace(req.Scope),
		pkceVerifier: pkceVerifier,
	}); err != nil {
		return nil, err
	}
	return &gestalt.AuthorizeResponse{RedirectURI: authURL}, nil
}

func (p *Provider) Token(ctx context.Context, req *gestalt.TokenRequest) (*gestalt.TokenResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("oidc auth: token request is required")
	}
	grantType := strings.TrimSpace(req.GrantType)
	if grantType == "" {
		grantType = grantTypeAuthorizationCode
	}
	switch grantType {
	case grantTypeAuthorizationCode:
		return p.tokenAuthorizationCode(ctx, req)
	case grantTypeTokenExchange:
		return p.tokenExchange(ctx, req)
	default:
		return nil, fmt.Errorf("oidc auth: unsupported grant_type %q", req.GrantType)
	}
}

func (p *Provider) tokenAuthorizationCode(ctx context.Context, req *gestalt.TokenRequest) (*gestalt.TokenResponse, error) {
	if _, err := p.grantStore(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.Code) == "" {
		return nil, fmt.Errorf("oidc auth: code is required")
	}
	if strings.TrimSpace(req.RedirectURI) == "" {
		return nil, fmt.Errorf("oidc auth: redirect_uri is required")
	}
	if strings.TrimSpace(req.State) == "" {
		return nil, fmt.Errorf("oidc auth: state is required")
	}

	pending, err := p.pendingOAuthForToken(req)
	if err != nil {
		return nil, err
	}
	defer p.deletePendingOAuth(pending.state)

	oauthCfg := p.oauthConfig(req.RedirectURI, pending.scope)
	ctx = context.WithValue(ctx, oauth2.HTTPClient, p.httpClient)
	opts := []oauth2.AuthCodeOption{}
	if pending.pkceVerifier != "" {
		opts = append(opts, oauth2.SetAuthURLParam("code_verifier", pending.pkceVerifier))
		if pending.state != "" {
			p.deletePKCEVerifier(pending.state)
		}
	}
	tok, err := oauthCfg.Exchange(ctx, req.Code, opts...)
	if err != nil {
		return nil, fmt.Errorf("oidc auth: exchange code: %w", err)
	}

	user, err := p.fetchUserInfo(ctx, tok.AccessToken)
	if err != nil {
		return nil, err
	}
	subject := subjectForVerifiedEmail(user.Email)
	if subject == "" {
		return nil, fmt.Errorf("oidc auth: userinfo missing verified email")
	}
	clientID := strings.TrimSpace(req.ClientID)
	if clientID == "" {
		clientID = defaultOAuthClientID
	}
	issued, err := p.grants.issue(ctx, subject, pending.scope, clientID, grantCategorySession, p.SessionTTL())
	if err != nil {
		return nil, err
	}
	return &gestalt.TokenResponse{
		AccessToken: issued.accessToken,
		TokenType:   "Bearer",
		ExpiresIn:   issued.expiresIn,
		Scope:       pending.scope,
		GrantID:     issued.grantID,
	}, nil
}

func (p *Provider) tokenExchange(ctx context.Context, req *gestalt.TokenRequest) (*gestalt.TokenResponse, error) {
	if _, err := p.grantStore(); err != nil {
		return nil, err
	}
	subjectToken := strings.TrimSpace(req.SubjectToken)
	if subjectToken == "" {
		return nil, fmt.Errorf("oidc auth: subject_token is required")
	}
	tokenType := strings.TrimSpace(req.SubjectTokenType)
	if tokenType != "" && tokenType != subjectTokenTypeAccessToken {
		return nil, fmt.Errorf("oidc auth: unsupported subject_token_type %q", req.SubjectTokenType)
	}
	introspectResp, err := p.Introspect(ctx, &gestalt.IntrospectRequest{Token: subjectToken})
	if err != nil {
		return nil, err
	}
	if introspectResp == nil || !introspectResp.Active || strings.TrimSpace(introspectResp.Subject) == "" {
		return nil, fmt.Errorf("oidc auth: subject_token is inactive")
	}
	clientID := strings.TrimSpace(req.ClientID)
	if clientID == "" {
		clientID = defaultOAuthClientID
	}
	issuedScope, err := attenuateScope(introspectResp.Scope, req.Scope)
	if err != nil {
		return nil, err
	}
	issued, err := p.grants.issue(ctx, introspectResp.Subject, issuedScope, clientID, grantCategoryAPIToken, p.SessionTTL())
	if err != nil {
		return nil, err
	}
	return &gestalt.TokenResponse{
		AccessToken: issued.accessToken,
		TokenType:   "Bearer",
		ExpiresIn:   issued.expiresIn,
		Scope:       issuedScope,
		GrantID:     issued.grantID,
	}, nil
}

func (p *Provider) closeGrantsDB() error {
	db := p.grantsDB
	p.grantsDB = nil
	p.grants = nil
	if db == nil {
		return nil
	}
	return db.Close()
}

// Close releases provider-owned IndexedDB resources.
func (p *Provider) Close() error {
	return p.closeGrantsDB()
}

func (p *Provider) Introspect(ctx context.Context, req *gestalt.IntrospectRequest) (*gestalt.IntrospectResponse, error) {
	if req == nil || strings.TrimSpace(req.Token) == "" {
		return &gestalt.IntrospectResponse{Active: false}, nil
	}
	grants, err := p.grantStore()
	if err != nil {
		return nil, err
	}
	resp := grants.introspect(ctx, strings.TrimSpace(req.Token))
	return &resp, nil
}

func (p *Provider) ListGrants(ctx context.Context, _ *gestalt.ListGrantsRequest) (*gestalt.ListGrantsResponse, error) {
	grants, err := p.grantStore()
	if err != nil {
		return nil, err
	}
	subject, err := p.callerSubject(ctx)
	if err != nil {
		return nil, err
	}
	return &gestalt.ListGrantsResponse{GrantIDs: grants.listGrantIDs(ctx, subject)}, nil
}

func (p *Provider) GetGrant(ctx context.Context, req *gestalt.GetGrantRequest) (*gestalt.GetGrantResponse, error) {
	if req == nil || strings.TrimSpace(req.GrantID) == "" {
		return nil, fmt.Errorf("oidc auth: grant_id is required")
	}
	grants, err := p.grantStore()
	if err != nil {
		return nil, err
	}
	subject, err := p.callerSubject(ctx)
	if err != nil {
		return nil, err
	}
	return grants.getGrant(ctx, strings.TrimSpace(req.GrantID), subject)
}

func (p *Provider) RevokeGrant(ctx context.Context, req *gestalt.RevokeGrantRequest) (*gestalt.RevokeGrantResponse, error) {
	if req == nil || strings.TrimSpace(req.GrantID) == "" {
		return nil, fmt.Errorf("oidc auth: grant_id is required")
	}
	grants, err := p.grantStore()
	if err != nil {
		return nil, err
	}
	subject, err := p.callerSubject(ctx)
	if err != nil {
		return nil, err
	}
	if err := grants.revokeGrant(ctx, strings.TrimSpace(req.GrantID), subject); err != nil {
		return nil, err
	}
	return &gestalt.RevokeGrantResponse{}, nil
}

func (p *Provider) grantStore() (*grantStore, error) {
	if p.grants == nil {
		return nil, fmt.Errorf("oidc auth: provider is not configured")
	}
	return p.grants, nil
}

func (p *Provider) callerSubject(ctx context.Context) (string, error) {
	call := gestalt.AuthCallContextFromContext(ctx)
	if call.Introspection != nil && call.Introspection.Active && strings.TrimSpace(call.Introspection.Subject) != "" {
		return strings.TrimSpace(call.Introspection.Subject), nil
	}
	token := strings.TrimSpace(call.CallerBearerToken)
	if token == "" {
		return "", fmt.Errorf("oidc auth: caller bearer token is required")
	}
	resp, err := p.Introspect(ctx, &gestalt.IntrospectRequest{Token: token})
	if err != nil {
		return "", err
	}
	if resp == nil || !resp.Active || strings.TrimSpace(resp.Subject) == "" {
		return "", fmt.Errorf("oidc auth: caller token is inactive")
	}
	return strings.TrimSpace(resp.Subject), nil
}

func (p *Provider) oauthConfig(callbackURL, requestScope string) *oauth2.Config {
	redirectURL := p.cfg.RedirectURL
	if redirectURL == "" {
		redirectURL = callbackURL
	}
	return &oauth2.Config{
		ClientID:     p.cfg.ClientID,
		ClientSecret: p.cfg.ClientSecret,
		RedirectURL:  redirectURL,
		Scopes:       oauthScopes(requestScope, p.cfg.Scopes),
		Endpoint: oauth2.Endpoint{
			AuthURL:  p.doc.AuthorizationEndpoint,
			TokenURL: p.doc.TokenEndpoint,
		},
	}
}

func oauthScopes(requestScope string, cfgScopes []string) []string {
	if scope := strings.TrimSpace(requestScope); scope != "" {
		return strings.Fields(scope)
	}
	if len(cfgScopes) > 0 {
		return append([]string(nil), cfgScopes...)
	}
	return []string{"openid", "email", "profile"}
}

func (p *Provider) fetchUserInfo(ctx context.Context, token string) (*authenticatedUserInfo, error) {
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
	if err := userinfo.CheckAllowedDomains("oidc", p.cfg.AllowedDomains, info.Email); err != nil {
		return nil, err
	}
	return &authenticatedUserInfo{
		Sub:         info.Sub,
		Email:       info.Email,
		DisplayName: info.Name,
		AvatarURL:   info.Picture,
	}, nil
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

func (p *Provider) storePKCEVerifier(hostState, verifier string) error {
	p.pkceMu.Lock()
	defer p.pkceMu.Unlock()
	now := p.currentTime()
	p.evictExpiredPKCEVerifiersLocked(now)
	if _, ok := p.pkceVerifiers[hostState]; !ok && len(p.pkceVerifiers) >= p.maxPKCEVerifierItems() {
		return fmt.Errorf("oidc auth: too many in-flight PKCE login attempts; increase pkceVerifierMaxItems or wait for older attempts to complete")
	}
	p.pkceVerifiers[hostState] = pkceVerifierEntry{
		verifier:  verifier,
		expiresAt: now.Add(p.pkceTTL()),
	}
	return nil
}

func (p *Provider) pkceVerifier(hostState string) (string, bool) {
	p.pkceMu.Lock()
	defer p.pkceMu.Unlock()
	now := p.currentTime()
	p.evictExpiredPKCEVerifiersLocked(now)
	entry, ok := p.pkceVerifiers[hostState]
	if !ok {
		return "", false
	}
	if !entry.expiresAt.After(now) {
		delete(p.pkceVerifiers, hostState)
		return "", false
	}
	return entry.verifier, true
}

func (p *Provider) deletePKCEVerifier(hostState string) {
	p.pkceMu.Lock()
	defer p.pkceMu.Unlock()
	delete(p.pkceVerifiers, hostState)
}

func (p *Provider) evictExpiredPKCEVerifiersLocked(now time.Time) {
	for hostState, entry := range p.pkceVerifiers {
		if !entry.expiresAt.After(now) {
			delete(p.pkceVerifiers, hostState)
		}
	}
}

func (p *Provider) storePendingOAuth(session pendingOAuthSession) error {
	if session.state == "" {
		return fmt.Errorf("oidc auth: pending oauth state is required")
	}
	p.pendingMu.Lock()
	defer p.pendingMu.Unlock()
	now := p.currentTime()
	p.evictExpiredPendingOAuthLocked(now)
	session.expiresAt = now.Add(p.pkceTTL())
	p.pendingOAuth[session.state] = session
	return nil
}

func (p *Provider) pendingOAuthForToken(req *gestalt.TokenRequest) (pendingOAuthSession, error) {
	state := strings.TrimSpace(req.State)
	if state == "" {
		return pendingOAuthSession{}, fmt.Errorf("oidc auth: state is required")
	}
	p.pendingMu.Lock()
	defer p.pendingMu.Unlock()
	now := p.currentTime()
	p.evictExpiredPendingOAuthLocked(now)

	pending, ok := p.pendingOAuth[state]
	if !ok {
		return pendingOAuthSession{}, fmt.Errorf("oidc auth: pending authorization not found")
	}
	if pending.redirectURI != req.RedirectURI {
		return pendingOAuthSession{}, fmt.Errorf("oidc auth: pending authorization redirect_uri mismatch")
	}
	if clientID := strings.TrimSpace(req.ClientID); clientID != "" && pending.clientID != "" && pending.clientID != clientID {
		return pendingOAuthSession{}, fmt.Errorf("oidc auth: pending authorization client_id mismatch")
	}
	return pending, nil
}

func (p *Provider) deletePendingOAuth(state string) {
	if state == "" {
		return
	}
	p.pendingMu.Lock()
	defer p.pendingMu.Unlock()
	delete(p.pendingOAuth, state)
}

func (p *Provider) evictExpiredPendingOAuthLocked(now time.Time) {
	for state, pending := range p.pendingOAuth {
		if !pending.expiresAt.After(now) {
			delete(p.pendingOAuth, state)
		}
	}
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
var _ gestalt.MetadataProvider = (*Provider)(nil)
