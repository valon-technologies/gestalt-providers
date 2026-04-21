package oidc

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
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
	providerVersion             = "0.0.1-alpha.3"
	defaultSessionTTL           = 24 * time.Hour
	defaultDisplayName          = "SSO"
	defaultHTTPTimeout          = 10 * time.Second
	defaultIDTokenClockSkew     = time.Minute
	defaultPKCEVerifierTTL      = time.Hour
	defaultPKCEVerifierMaxItems = 10_000
)

type discoveryDocument struct {
	Issuer                string   `json:"issuer"`
	AuthorizationEndpoint string   `json:"authorization_endpoint"`
	TokenEndpoint         string   `json:"token_endpoint"`
	UserinfoEndpoint      string   `json:"userinfo_endpoint"`
	JWKSURI               string   `json:"jwks_uri"`
	IDTokenSigningAlgs    []string `json:"id_token_signing_alg_values_supported"`
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
}

type pkceVerifierEntry struct {
	verifier  string
	expiresAt time.Time
}

type audienceClaim []string

type idTokenHeader struct {
	Alg string `json:"alg"`
	Kid string `json:"kid"`
	Typ string `json:"typ"`
}

type idTokenClaims struct {
	Iss           string        `json:"iss"`
	Sub           string        `json:"sub"`
	Aud           audienceClaim `json:"aud"`
	Exp           json.Number   `json:"exp"`
	Iat           json.Number   `json:"iat"`
	Nbf           json.Number   `json:"nbf"`
	Azp           string        `json:"azp"`
	Email         string        `json:"email"`
	Name          string        `json:"name"`
	Picture       string        `json:"picture"`
	EmailVerified any           `json:"email_verified"`
}

type userClaims struct {
	Subject       string
	Email         string
	DisplayName   string
	AvatarURL     string
	EmailVerified any
}

type jsonWebKeySet struct {
	Keys []jsonWebKey `json:"keys"`
}

type jsonWebKey struct {
	Kty string `json:"kty"`
	Use string `json:"use"`
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	N   string `json:"n"`
	E   string `json:"e"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
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

func (p *Provider) SessionTTL() time.Duration {
	if p.cfg.SessionTTL > 0 {
		return p.cfg.SessionTTL
	}
	return defaultSessionTTL
}

func (p *Provider) BeginLogin(_ context.Context, req *gestalt.BeginLoginRequest) (*gestalt.BeginLoginResponse, error) {
	oauthCfg := p.oauthConfig(req.CallbackUrl)
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
	if err := p.storePKCEVerifier(req.HostState, verifier); err != nil {
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
	oauthCfg := p.oauthConfig(req.CallbackUrl)
	ctx = context.WithValue(ctx, oauth2.HTTPClient, p.httpClient)
	opts := []oauth2.AuthCodeOption{}
	pkceState := ""
	if p.cfg.PKCE {
		pkceState = req.Query["state"]
		if pkceState == "" {
			return nil, fmt.Errorf("oidc auth: state is required when pkce is enabled")
		}
		verifier, ok := p.pkceVerifier(pkceState)
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
		p.deletePKCEVerifier(pkceState)
	}
	rawIDToken, err := idTokenFromToken(tok)
	if err != nil {
		return nil, err
	}
	claims, err := p.validateIDToken(ctx, rawIDToken)
	if err != nil {
		return nil, err
	}
	if tok.AccessToken != "" && p.doc.UserinfoEndpoint != "" {
		info, err := p.fetchUserInfo(ctx, tok.AccessToken)
		if err == nil {
			if info.Subject != claims.Subject {
				return nil, fmt.Errorf("oidc auth: userinfo subject %q does not match id_token subject %q", info.Subject, claims.Subject)
			}
			claims = claims.merge(info)
		}
	}
	return p.authenticatedUserFromClaims(claims)
}

func (p *Provider) ValidateExternalToken(ctx context.Context, token string) (*gestalt.AuthenticatedUser, error) {
	if token == "" {
		return nil, fmt.Errorf("oidc auth: token is required")
	}
	if looksLikeJWT(token) {
		claims, err := p.validateIDToken(ctx, token)
		if err != nil {
			if p.doc.UserinfoEndpoint == "" {
				return nil, err
			}
			info, userInfoErr := p.fetchUserInfo(ctx, token)
			if userInfoErr != nil {
				return nil, fmt.Errorf("oidc auth: validate external JWT as id_token: %v; userinfo fallback: %w", err, userInfoErr)
			}
			return p.authenticatedUserFromClaims(info)
		}
		return p.authenticatedUserFromClaims(claims)
	}
	if p.doc.UserinfoEndpoint == "" {
		return nil, fmt.Errorf("oidc auth: userinfo_endpoint is not configured; external token must be an id_token")
	}
	claims, err := p.fetchUserInfo(ctx, token)
	if err != nil {
		return nil, err
	}
	return p.authenticatedUserFromClaims(claims)
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

func (p *Provider) fetchUserInfo(ctx context.Context, token string) (userClaims, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.doc.UserinfoEndpoint, nil)
	if err != nil {
		return userClaims{}, fmt.Errorf("oidc auth: create userinfo request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return userClaims{}, fmt.Errorf("oidc auth: fetch userinfo: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return userClaims{}, fmt.Errorf("oidc auth: userinfo returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var info struct {
		Sub           string `json:"sub"`
		Email         string `json:"email"`
		Name          string `json:"name"`
		Picture       string `json:"picture"`
		EmailVerified any    `json:"email_verified"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return userClaims{}, fmt.Errorf("oidc auth: decode userinfo: %w", err)
	}
	return userClaims{
		Subject:       info.Sub,
		Email:         info.Email,
		DisplayName:   info.Name,
		AvatarURL:     info.Picture,
		EmailVerified: info.EmailVerified,
	}, nil
}

func (p *Provider) validateIDToken(ctx context.Context, rawIDToken string) (userClaims, error) {
	signingInput, signature, header, claims, err := parseIDToken(rawIDToken)
	if err != nil {
		return userClaims{}, err
	}
	if err := p.validateIDTokenHeader(header); err != nil {
		return userClaims{}, err
	}
	if err := p.verifyIDTokenSignature(ctx, signingInput, signature, header); err != nil {
		return userClaims{}, err
	}
	if err := p.validateIDTokenClaims(claims); err != nil {
		return userClaims{}, err
	}
	return claims.userClaims(), nil
}

func (p *Provider) validateIDTokenHeader(header idTokenHeader) error {
	if header.Alg == "" {
		return fmt.Errorf("oidc auth: id_token is missing alg header")
	}
	if strings.EqualFold(header.Alg, "none") {
		return fmt.Errorf("oidc auth: unsigned id_token values are not supported")
	}
	if !p.isAllowedIDTokenAlg(header.Alg) {
		return fmt.Errorf("oidc auth: id_token alg %q is not allowed by discovery metadata", header.Alg)
	}
	return nil
}

func (p *Provider) validateIDTokenClaims(claims idTokenClaims) error {
	if claims.Iss == "" {
		return fmt.Errorf("oidc auth: id_token iss is required")
	}
	if claims.Iss != p.doc.Issuer {
		return fmt.Errorf("oidc auth: id_token issuer %q does not match discovery issuer %q", claims.Iss, p.doc.Issuer)
	}
	if claims.Sub == "" {
		return fmt.Errorf("oidc auth: id_token sub is required")
	}
	if !claims.Aud.contains(p.cfg.ClientID) {
		return fmt.Errorf("oidc auth: id_token aud %q does not include clientId %q", claims.Aud, p.cfg.ClientID)
	}
	if len(claims.Aud) > 1 {
		for _, aud := range claims.Aud {
			if aud != p.cfg.ClientID {
				return fmt.Errorf("oidc auth: id_token aud %q contains an untrusted audience %q", claims.Aud, aud)
			}
		}
	}
	if claims.Azp != "" && claims.Azp != p.cfg.ClientID {
		return fmt.Errorf("oidc auth: id_token azp %q does not match clientId %q", claims.Azp, p.cfg.ClientID)
	}
	now := p.currentTime()
	exp, err := parseNumericDate("exp", claims.Exp)
	if err != nil {
		return err
	}
	if exp.Before(now.Add(-defaultIDTokenClockSkew)) {
		return fmt.Errorf("oidc auth: id_token expired at %s", exp.UTC().Format(time.RFC3339))
	}
	iat, err := parseNumericDate("iat", claims.Iat)
	if err != nil {
		return err
	}
	if iat.After(now.Add(defaultIDTokenClockSkew)) {
		return fmt.Errorf("oidc auth: id_token iat %s is too far in the future", iat.UTC().Format(time.RFC3339))
	}
	if claims.Nbf != "" {
		nbf, err := parseNumericDate("nbf", claims.Nbf)
		if err != nil {
			return err
		}
		if nbf.After(now.Add(defaultIDTokenClockSkew)) {
			return fmt.Errorf("oidc auth: id_token nbf %s is in the future", nbf.UTC().Format(time.RFC3339))
		}
	}
	return nil
}

func (p *Provider) verifyIDTokenSignature(ctx context.Context, signingInput, signature []byte, header idTokenHeader) error {
	if strings.HasPrefix(header.Alg, "HS") {
		return verifyHMACSignature(header.Alg, signingInput, signature, []byte(p.cfg.ClientSecret))
	}
	keys, err := p.fetchJWKS(ctx)
	if err != nil {
		return err
	}
	candidates := matchingJWKs(keys.Keys, header)
	if len(candidates) == 0 {
		return fmt.Errorf("oidc auth: no jwk matched id_token kid %q and alg %q", header.Kid, header.Alg)
	}
	var lastErr error
	for _, key := range candidates {
		if err := verifyJWKSignature(key, header.Alg, signingInput, signature); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	if lastErr != nil {
		return fmt.Errorf("oidc auth: verify id_token signature: %w", lastErr)
	}
	return fmt.Errorf("oidc auth: verify id_token signature: no usable key")
}

func (p *Provider) fetchJWKS(ctx context.Context) (jsonWebKeySet, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.doc.JWKSURI, nil)
	if err != nil {
		return jsonWebKeySet{}, fmt.Errorf("oidc auth: create jwks request: %w", err)
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return jsonWebKeySet{}, fmt.Errorf("oidc auth: fetch jwks: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return jsonWebKeySet{}, fmt.Errorf("oidc auth: jwks returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var set jsonWebKeySet
	if err := json.NewDecoder(resp.Body).Decode(&set); err != nil {
		return jsonWebKeySet{}, fmt.Errorf("oidc auth: decode jwks: %w", err)
	}
	if len(set.Keys) == 0 {
		return jsonWebKeySet{}, fmt.Errorf("oidc auth: jwks returned no keys")
	}
	return set, nil
}

func (p *Provider) authenticatedUserFromClaims(claims userClaims) (*gestalt.AuthenticatedUser, error) {
	if claims.Subject == "" {
		return nil, fmt.Errorf("oidc auth: subject is required")
	}
	if !userinfo.EmailVerified(claims.EmailVerified) {
		return nil, fmt.Errorf("oidc auth: email %s is not verified", claims.Email)
	}
	if err := userinfo.CheckAllowedDomains("oidc", p.cfg.AllowedDomains, claims.Email); err != nil {
		return nil, err
	}
	return &gestalt.AuthenticatedUser{
		Subject:       claims.Subject,
		Email:         claims.Email,
		EmailVerified: true,
		DisplayName:   claims.DisplayName,
		AvatarUrl:     claims.AvatarURL,
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
	if doc.Issuer == "" || doc.AuthorizationEndpoint == "" || doc.TokenEndpoint == "" || doc.JWKSURI == "" {
		return discoveryDocument{}, fmt.Errorf("oidc auth: discovery document is missing required issuer or endpoints")
	}
	if err := validateEndpointURL("issuer", doc.Issuer, allowInsecureHTTP); err != nil {
		return discoveryDocument{}, err
	}
	if !sameIssuer(issuerURL, doc.Issuer) {
		return discoveryDocument{}, fmt.Errorf("oidc auth: discovery issuer %q does not match configured issuerUrl %q", doc.Issuer, issuerURL)
	}
	if err := validateEndpointURL("authorization_endpoint", doc.AuthorizationEndpoint, allowInsecureHTTP); err != nil {
		return discoveryDocument{}, err
	}
	if err := validateEndpointURL("token_endpoint", doc.TokenEndpoint, allowInsecureHTTP); err != nil {
		return discoveryDocument{}, err
	}
	if err := validateEndpointURL("jwks_uri", doc.JWKSURI, allowInsecureHTTP); err != nil {
		return discoveryDocument{}, err
	}
	if doc.UserinfoEndpoint != "" {
		if err := validateEndpointURL("userinfo_endpoint", doc.UserinfoEndpoint, allowInsecureHTTP); err != nil {
			return discoveryDocument{}, err
		}
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

func idTokenFromToken(tok *oauth2.Token) (string, error) {
	raw := tok.Extra("id_token")
	switch v := raw.(type) {
	case string:
		if strings.TrimSpace(v) == "" {
			return "", fmt.Errorf("oidc auth: token response returned an empty id_token")
		}
		return v, nil
	case []byte:
		if len(v) == 0 {
			return "", fmt.Errorf("oidc auth: token response returned an empty id_token")
		}
		return string(v), nil
	case nil:
		return "", fmt.Errorf("oidc auth: token response is missing id_token")
	default:
		return "", fmt.Errorf("oidc auth: token response returned non-string id_token of type %T", raw)
	}
}

func looksLikeJWT(token string) bool {
	parts := strings.Split(token, ".")
	return len(parts) == 3 && parts[0] != "" && parts[1] != "" && parts[2] != ""
}

func parseIDToken(raw string) ([]byte, []byte, idTokenHeader, idTokenClaims, error) {
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return nil, nil, idTokenHeader{}, idTokenClaims{}, fmt.Errorf("oidc auth: id_token must be a JWT")
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, nil, idTokenHeader{}, idTokenClaims{}, fmt.Errorf("oidc auth: decode id_token header: %w", err)
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, nil, idTokenHeader{}, idTokenClaims{}, fmt.Errorf("oidc auth: decode id_token claims: %w", err)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, nil, idTokenHeader{}, idTokenClaims{}, fmt.Errorf("oidc auth: decode id_token signature: %w", err)
	}
	var header idTokenHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return nil, nil, idTokenHeader{}, idTokenClaims{}, fmt.Errorf("oidc auth: decode id_token header JSON: %w", err)
	}
	var claims idTokenClaims
	decoder := json.NewDecoder(bytes.NewReader(payloadBytes))
	decoder.UseNumber()
	if err := decoder.Decode(&claims); err != nil {
		return nil, nil, idTokenHeader{}, idTokenClaims{}, fmt.Errorf("oidc auth: decode id_token claims JSON: %w", err)
	}
	return []byte(parts[0] + "." + parts[1]), signature, header, claims, nil
}

func parseNumericDate(field string, value json.Number) (time.Time, error) {
	if value == "" {
		return time.Time{}, fmt.Errorf("oidc auth: id_token %s is required", field)
	}
	seconds, err := value.Float64()
	if err != nil {
		return time.Time{}, fmt.Errorf("oidc auth: id_token %s must be a numeric date: %w", field, err)
	}
	whole, frac := math.Modf(seconds)
	return time.Unix(int64(whole), int64(frac*float64(time.Second))).UTC(), nil
}

func (p *Provider) isAllowedIDTokenAlg(alg string) bool {
	if len(p.doc.IDTokenSigningAlgs) == 0 {
		return alg == "RS256"
	}
	for _, supportedAlg := range p.doc.IDTokenSigningAlgs {
		if supportedAlg == alg {
			return true
		}
	}
	return false
}

func matchingJWKs(keys []jsonWebKey, header idTokenHeader) []jsonWebKey {
	matches := make([]jsonWebKey, 0, len(keys))
	for _, key := range keys {
		if key.Use != "" && key.Use != "sig" {
			continue
		}
		if header.Kid != "" && key.Kid != header.Kid {
			continue
		}
		if key.Alg != "" && key.Alg != header.Alg {
			continue
		}
		matches = append(matches, key)
	}
	return matches
}

func verifyHMACSignature(alg string, signingInput, signature, secret []byte) error {
	if len(secret) == 0 {
		return fmt.Errorf("oidc auth: clientSecret is required to validate %s id_token values", alg)
	}
	hash, err := jwtHashForAlg(alg)
	if err != nil {
		return err
	}
	mac := hmac.New(hash.New, secret)
	_, _ = mac.Write(signingInput)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return fmt.Errorf("oidc auth: HMAC signature mismatch")
	}
	return nil
}

func verifyJWKSignature(key jsonWebKey, alg string, signingInput, signature []byte) error {
	switch {
	case strings.HasPrefix(alg, "RS"):
		pub, err := rsaPublicKeyFromJWK(key)
		if err != nil {
			return err
		}
		hash, err := jwtHashForAlg(alg)
		if err != nil {
			return err
		}
		digest, err := hashJWTInput(hash, signingInput)
		if err != nil {
			return err
		}
		return rsa.VerifyPKCS1v15(pub, hash, digest, signature)
	case strings.HasPrefix(alg, "PS"):
		pub, err := rsaPublicKeyFromJWK(key)
		if err != nil {
			return err
		}
		hash, err := jwtHashForAlg(alg)
		if err != nil {
			return err
		}
		digest, err := hashJWTInput(hash, signingInput)
		if err != nil {
			return err
		}
		return rsa.VerifyPSS(pub, hash, digest, signature, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash, Hash: hash})
	case strings.HasPrefix(alg, "ES"):
		pub, err := ecdsaPublicKeyFromJWK(key)
		if err != nil {
			return err
		}
		hash, err := jwtHashForAlg(alg)
		if err != nil {
			return err
		}
		digest, err := hashJWTInput(hash, signingInput)
		if err != nil {
			return err
		}
		if !verifyJWSECDSASignature(pub, signature, digest) {
			return fmt.Errorf("oidc auth: ECDSA signature mismatch")
		}
		return nil
	case alg == "EdDSA":
		pub, err := ed25519PublicKeyFromJWK(key)
		if err != nil {
			return err
		}
		if !ed25519.Verify(pub, signingInput, signature) {
			return fmt.Errorf("oidc auth: EdDSA signature mismatch")
		}
		return nil
	default:
		return fmt.Errorf("oidc auth: unsupported id_token alg %q", alg)
	}
}

func verifyJWSECDSASignature(pub *ecdsa.PublicKey, signature, digest []byte) bool {
	size := (pub.Curve.Params().BitSize + 7) / 8
	if len(signature) != size*2 {
		return false
	}
	r := new(big.Int).SetBytes(signature[:size])
	s := new(big.Int).SetBytes(signature[size:])
	return ecdsa.Verify(pub, digest, r, s)
}

func jwtHashForAlg(alg string) (crypto.Hash, error) {
	switch alg {
	case "HS256", "RS256", "PS256", "ES256":
		return crypto.SHA256, nil
	case "HS384", "RS384", "PS384", "ES384":
		return crypto.SHA384, nil
	case "HS512", "RS512", "PS512", "ES512":
		return crypto.SHA512, nil
	default:
		return 0, fmt.Errorf("oidc auth: unsupported id_token alg %q", alg)
	}
}

func hashJWTInput(hash crypto.Hash, signingInput []byte) ([]byte, error) {
	if !hash.Available() {
		return nil, fmt.Errorf("oidc auth: hash %s is unavailable", hash.String())
	}
	h := hash.New()
	_, _ = h.Write(signingInput)
	return h.Sum(nil), nil
}

func rsaPublicKeyFromJWK(key jsonWebKey) (*rsa.PublicKey, error) {
	if key.Kty != "RSA" {
		return nil, fmt.Errorf("oidc auth: jwk %q is not an RSA key", key.Kid)
	}
	n, err := decodeBase64URLBigInt("n", key.N)
	if err != nil {
		return nil, err
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(key.E)
	if err != nil {
		return nil, fmt.Errorf("oidc auth: decode jwk e: %w", err)
	}
	e := new(big.Int).SetBytes(eBytes)
	if !e.IsInt64() || e.Int64() <= 0 {
		return nil, fmt.Errorf("oidc auth: jwk e must be a positive integer")
	}
	return &rsa.PublicKey{N: n, E: int(e.Int64())}, nil
}

func ecdsaPublicKeyFromJWK(key jsonWebKey) (*ecdsa.PublicKey, error) {
	if key.Kty != "EC" {
		return nil, fmt.Errorf("oidc auth: jwk %q is not an EC key", key.Kid)
	}
	curve, err := namedCurve(key.Crv)
	if err != nil {
		return nil, err
	}
	x, err := decodeBase64URLBigInt("x", key.X)
	if err != nil {
		return nil, err
	}
	y, err := decodeBase64URLBigInt("y", key.Y)
	if err != nil {
		return nil, err
	}
	if !curve.IsOnCurve(x, y) {
		return nil, fmt.Errorf("oidc auth: jwk %q contains a point outside curve %s", key.Kid, key.Crv)
	}
	return &ecdsa.PublicKey{Curve: curve, X: x, Y: y}, nil
}

func ed25519PublicKeyFromJWK(key jsonWebKey) (ed25519.PublicKey, error) {
	if key.Kty != "OKP" {
		return nil, fmt.Errorf("oidc auth: jwk %q is not an OKP key", key.Kid)
	}
	if key.Crv != "Ed25519" {
		return nil, fmt.Errorf("oidc auth: jwk %q uses unsupported OKP curve %q", key.Kid, key.Crv)
	}
	x, err := base64.RawURLEncoding.DecodeString(key.X)
	if err != nil {
		return nil, fmt.Errorf("oidc auth: decode jwk x: %w", err)
	}
	if len(x) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("oidc auth: jwk %q has invalid Ed25519 key length %d", key.Kid, len(x))
	}
	return ed25519.PublicKey(x), nil
}

func namedCurve(name string) (elliptic.Curve, error) {
	switch name {
	case "P-256":
		return elliptic.P256(), nil
	case "P-384":
		return elliptic.P384(), nil
	case "P-521":
		return elliptic.P521(), nil
	default:
		return nil, fmt.Errorf("oidc auth: unsupported EC curve %q", name)
	}
}

func decodeBase64URLBigInt(field, value string) (*big.Int, error) {
	if value == "" {
		return nil, fmt.Errorf("oidc auth: jwk %s is required", field)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("oidc auth: decode jwk %s: %w", field, err)
	}
	if len(decoded) == 0 {
		return nil, fmt.Errorf("oidc auth: jwk %s is empty", field)
	}
	return new(big.Int).SetBytes(decoded), nil
}

func sameIssuer(a, b string) bool {
	return strings.TrimRight(a, "/") == strings.TrimRight(b, "/")
}

func (c idTokenClaims) userClaims() userClaims {
	return userClaims{
		Subject:       c.Sub,
		Email:         c.Email,
		DisplayName:   c.Name,
		AvatarURL:     c.Picture,
		EmailVerified: c.EmailVerified,
	}
}

func (c userClaims) merge(other userClaims) userClaims {
	if other.Subject != "" {
		c.Subject = other.Subject
	}
	if other.Email != "" {
		c.Email = other.Email
	}
	if other.DisplayName != "" {
		c.DisplayName = other.DisplayName
	}
	if other.AvatarURL != "" {
		c.AvatarURL = other.AvatarURL
	}
	if other.EmailVerified != nil {
		c.EmailVerified = other.EmailVerified
	}
	return c
}

func (a *audienceClaim) UnmarshalJSON(data []byte) error {
	var single string
	if err := json.Unmarshal(data, &single); err == nil {
		*a = audienceClaim{single}
		return nil
	}
	var many []string
	if err := json.Unmarshal(data, &many); err == nil {
		*a = audienceClaim(many)
		return nil
	}
	return fmt.Errorf("aud must be a string or array of strings")
}

func (a audienceClaim) contains(value string) bool {
	for _, candidate := range a {
		if candidate == value {
			return true
		}
	}
	return false
}

func (a audienceClaim) String() string {
	return strings.Join(a, ",")
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
