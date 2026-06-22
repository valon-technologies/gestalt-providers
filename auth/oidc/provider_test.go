package oidc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	oidcfake "github.com/valon-technologies/gestalt-providers/auth/oidc/internal/fake"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

func TestAuthorizePKCEDoesNotExposeVerifier(t *testing.T) {
	p := New()
	attachGrantStore(t, p)
	p.cfg = config{
		ClientID: "client-id",
		PKCE:     true,
	}
	p.doc = discoveryDocument{
		AuthorizationEndpoint: "https://issuer.example/auth",
		TokenEndpoint:         "https://issuer.example/token",
		UserinfoEndpoint:      "https://issuer.example/userinfo",
	}

	resp, err := p.Authorize(context.Background(), &gestalt.AuthorizeRequest{
		ResponseType: "code",
		ClientID:     defaultOAuthClientID,
		RedirectURI:  "https://gestalt.example/callback",
		State:        "host-state",
	})
	if err != nil {
		t.Fatalf("Authorize() error = %v", err)
	}
	if !strings.Contains(resp.RedirectURI, "code_challenge=") {
		t.Fatalf("Authorize() redirect URI missing code_challenge: %s", resp.RedirectURI)
	}
	if _, ok := p.pkceVerifier("host-state"); !ok {
		t.Fatal("Authorize() did not retain verifier server-side")
	}
}

func TestTokenPKCEUsesStoredVerifier(t *testing.T) {
	const hostState = "host-state"

	var gotCodeVerifier string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			if err := r.ParseForm(); err != nil {
				t.Fatalf("ParseForm() error = %v", err)
			}
			gotCodeVerifier = r.FormValue("code_verifier")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "upstream-access-token",
				"token_type":   "Bearer",
				"id_token":     testIDToken("user-123", "User Example"),
			})
		case "/userinfo":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"sub":            "user-123",
				"email":          "user@example.com",
				"name":           "User Example",
				"picture":        "https://issuer.example/avatar.png",
				"email_verified": "true",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	p := New()
	attachGrantStore(t, p)
	p.httpClient = server.Client()
	p.cfg = config{
		ClientID: "client-id",
		PKCE:     true,
	}
	p.doc = discoveryDocument{
		AuthorizationEndpoint: server.URL + "/auth",
		TokenEndpoint:         server.URL + "/token",
		UserinfoEndpoint:      server.URL + "/userinfo",
	}

	_, err := p.Authorize(context.Background(), &gestalt.AuthorizeRequest{
		ResponseType: "code",
		ClientID:     defaultOAuthClientID,
		RedirectURI:  "https://gestalt.example/callback",
		State:        hostState,
	})
	if err != nil {
		t.Fatalf("Authorize() error = %v", err)
	}
	wantCodeVerifier, ok := p.pkceVerifier(hostState)
	if !ok {
		t.Fatal("Authorize() did not retain verifier")
	}

	tokenResp, err := p.Token(context.Background(), &gestalt.TokenRequest{
		GrantType:   grantTypeAuthorizationCode,
		Code:        "auth-code",
		RedirectURI: "https://gestalt.example/callback",
		ClientID:    defaultOAuthClientID,
		State:       hostState,
	})
	if err != nil {
		t.Fatalf("Token() error = %v", err)
	}
	if gotCodeVerifier != wantCodeVerifier {
		t.Fatalf("Token() used code_verifier %q, want %q", gotCodeVerifier, wantCodeVerifier)
	}
	if _, ok := p.pkceVerifier(hostState); ok {
		t.Fatal("Token() left verifier cached after successful exchange")
	}
	if tokenResp.AccessToken == "" {
		t.Fatal("Token() returned empty access token")
	}
	if tokenResp.GrantID == "" {
		t.Fatal("Token() returned empty grant_id")
	}

	introspectResp, err := p.Introspect(context.Background(), &gestalt.IntrospectRequest{Token: tokenResp.AccessToken})
	if err != nil {
		t.Fatalf("Introspect() error = %v", err)
	}
	if !introspectResp.Active {
		t.Fatal("Introspect() expected active token")
	}
	if introspectResp.Subject != "user:user@example.com" {
		t.Fatalf("Introspect() subject = %q, want %q", introspectResp.Subject, "user:user@example.com")
	}
	if introspectResp.ClientID != defaultOAuthClientID {
		t.Fatalf("Introspect() client_id = %q, want %q", introspectResp.ClientID, defaultOAuthClientID)
	}

	userInfoCtx := gestalt.WithIdentityCallContext(context.Background(), gestalt.IdentityCallContext{
		CallerBearerToken: tokenResp.AccessToken,
	})
	userInfoResp, err := p.UserInfo(userInfoCtx, &gestalt.UserInfoRequest{})
	if err != nil {
		t.Fatalf("UserInfo() error = %v", err)
	}
	if userInfoResp.Email != "user@example.com" {
		t.Fatalf("UserInfo() email = %q, want user@example.com", userInfoResp.Email)
	}
	if userInfoResp.Name != "User Example" {
		t.Fatalf("UserInfo() name = %q, want User Example", userInfoResp.Name)
	}
}

func TestIntrospectInactiveAfterRevokeGrant(t *testing.T) {
	p := New()
	attachGrantStore(t, p)
	ctx := context.Background()
	subject := "user:user@example.com"
	issued, err := p.grants.issue(ctx, subject, "openid email", defaultOAuthClientID, grantCategoryAPIToken, time.Hour)
	if err != nil {
		t.Fatalf("issue() error = %v", err)
	}
	grantID, accessToken := issued.grantID, issued.accessToken

	revokeCtx := gestalt.WithIdentityCallContext(ctx, gestalt.IdentityCallContext{
		CallerBearerToken: accessToken,
	})
	if _, err := p.RevokeGrant(revokeCtx, &gestalt.RevokeGrantRequest{GrantID: grantID}); err != nil {
		t.Fatalf("RevokeGrant() error = %v", err)
	}

	resp, err := p.Introspect(context.Background(), &gestalt.IntrospectRequest{Token: accessToken})
	if err != nil {
		t.Fatalf("Introspect() error = %v", err)
	}
	if resp.Active {
		t.Fatal("Introspect() expected inactive token after revoke")
	}
}

func TestListGrantsScopesToCaller(t *testing.T) {
	p := New()
	attachGrantStore(t, p)
	ctx := context.Background()
	issued, err := p.grants.issue(ctx, "user:owner@example.com", "openid", defaultOAuthClientID, grantCategoryAPIToken, time.Hour)
	if err != nil {
		t.Fatalf("issue() error = %v", err)
	}
	grantID, token := issued.grantID, issued.accessToken
	otherIssued, err := p.grants.issue(ctx, "user:other@example.com", "openid", defaultOAuthClientID, grantCategoryAPIToken, time.Hour)
	if err != nil {
		t.Fatalf("issue(other) error = %v", err)
	}
	otherGrantID := otherIssued.grantID

	callCtx := gestalt.WithIdentityCallContext(ctx, gestalt.IdentityCallContext{
		CallerBearerToken: token,
	})
	resp, err := p.ListGrants(callCtx, &gestalt.ListGrantsRequest{})
	if err != nil {
		t.Fatalf("ListGrants() error = %v", err)
	}
	if len(resp.GrantIDs) != 1 || resp.GrantIDs[0] != grantID {
		t.Fatalf("ListGrants() = %v, want [%q]", resp.GrantIDs, grantID)
	}
	if _, err := p.GetGrant(callCtx, &gestalt.GetGrantRequest{GrantID: grantID}); err != nil {
		t.Fatalf("GetGrant(owned) error = %v", err)
	}
	_, otherErr := p.GetGrant(callCtx, &gestalt.GetGrantRequest{GrantID: otherGrantID})
	if otherErr == nil {
		t.Fatal("GetGrant(other) error = nil, want not found")
	}
	if code, ok := gestalt.StatusCodeOf(otherErr); !ok || code != gestalt.CodeNotFound {
		t.Fatalf("GetGrant(other) error = %v, want not_found status", otherErr)
	}
}

func TestConfigureRejectsInsecureIssuerURLByDefault(t *testing.T) {
	p := New()

	err := p.Configure(context.Background(), "", map[string]any{
		"issuerUrl": "http://127.0.0.1:8080",
		"clientId":  "client-id",
	})
	if err == nil {
		t.Fatal("Configure() error = nil, want rejection for insecure issuerUrl")
	}
	if !strings.Contains(err.Error(), "issuerUrl must use https") {
		t.Fatalf("Configure() error = %v, want issuerUrl https validation", err)
	}
}

func TestConfigureRejectsInsecureNonLoopbackIssuerEvenWhenOptedIn(t *testing.T) {
	p := New()

	err := p.Configure(context.Background(), "", map[string]any{
		"issuerUrl":         "http://issuer.example",
		"clientId":          "client-id",
		"allowInsecureHttp": true,
	})
	if err == nil {
		t.Fatal("Configure() error = nil, want rejection for non-loopback insecure issuerUrl")
	}
	if !strings.Contains(err.Error(), "issuerUrl may use http only for loopback/local development hosts") {
		t.Fatalf("Configure() error = %v, want non-loopback rejection", err)
	}
}

func TestConfigureAllowsInsecureLoopbackIssuerWhenOptedIn(t *testing.T) {
	server := newDiscoveryServer(t, discoveryDocument{
		AuthorizationEndpoint: "",
		TokenEndpoint:         "",
		UserinfoEndpoint:      "",
	})
	defer server.Close()

	p := New()
	p.httpClient = server.Client()
	attachTestIndexedDB(t, p)

	err := p.Configure(context.Background(), "", map[string]any{
		"issuerUrl":         server.URL,
		"clientId":          "client-id",
		"allowInsecureHttp": true,
		"indexeddb":         "default",
	})
	if err != nil {
		t.Fatalf("Configure() error = %v", err)
	}
	if p.doc.AuthorizationEndpoint != server.URL+"/auth" {
		t.Fatalf("Configure() authorization_endpoint = %q, want %q", p.doc.AuthorizationEndpoint, server.URL+"/auth")
	}
}

func TestConfigureRejectsInsecureDiscoveryEndpointsByDefault(t *testing.T) {
	server := newTLSDiscoveryServer(t, discoveryDocument{
		AuthorizationEndpoint: "http://127.0.0.1:8080/auth",
		TokenEndpoint:         "https://issuer.example/token",
		UserinfoEndpoint:      "https://issuer.example/userinfo",
	})
	defer server.Close()

	p := New()
	p.httpClient = server.Client()

	err := p.Configure(context.Background(), "", map[string]any{
		"issuerUrl": server.URL,
		"clientId":  "client-id",
	})
	if err == nil {
		t.Fatal("Configure() error = nil, want rejection for insecure discovery endpoint")
	}
	if !strings.Contains(err.Error(), "authorization_endpoint must use https") {
		t.Fatalf("Configure() error = %v, want authorization_endpoint https validation", err)
	}
}

func TestConfigureRejectsInsecureNonLoopbackDiscoveryEndpointEvenWhenOptedIn(t *testing.T) {
	server := newTLSDiscoveryServer(t, discoveryDocument{
		AuthorizationEndpoint: "http://issuer.example/auth",
		TokenEndpoint:         "https://issuer.example/token",
		UserinfoEndpoint:      "https://issuer.example/userinfo",
	})
	defer server.Close()

	p := New()
	p.httpClient = server.Client()

	err := p.Configure(context.Background(), "", map[string]any{
		"issuerUrl":         server.URL,
		"clientId":          "client-id",
		"allowInsecureHttp": true,
	})
	if err == nil {
		t.Fatal("Configure() error = nil, want rejection for non-loopback insecure discovery endpoint")
	}
	if !strings.Contains(err.Error(), "authorization_endpoint may use http only for loopback/local development hosts") {
		t.Fatalf("Configure() error = %v, want non-loopback discovery endpoint rejection", err)
	}
}

func TestPKCEVerifierExpires(t *testing.T) {
	p := New()
	now := time.Unix(1_700_000_000, 0)
	p.now = func() time.Time { return now }
	p.pkceVerifierTTL = 10 * time.Minute

	if err := p.storePKCEVerifier("host-state", "verifier"); err != nil {
		t.Fatalf("storePKCEVerifier() error = %v", err)
	}
	if got, ok := p.pkceVerifier("host-state"); !ok || got != "verifier" {
		t.Fatalf("pkceVerifier() = (%q, %t), want (%q, true)", got, ok, "verifier")
	}

	now = now.Add(10*time.Minute + time.Second)

	if got, ok := p.pkceVerifier("host-state"); ok || got != "" {
		t.Fatalf("pkceVerifier() after TTL = (%q, %t), want (\"\", false)", got, ok)
	}
	if len(p.pkceVerifiers) != 0 {
		t.Fatalf("pkceVerifiers size = %d, want 0 after expiration", len(p.pkceVerifiers))
	}
}

func TestPKCEVerifierRejectsNewEntryWhenCacheIsFull(t *testing.T) {
	p := New()
	now := time.Unix(1_700_000_000, 0)
	p.now = func() time.Time { return now }
	p.pkceVerifierTTL = 10 * time.Minute
	p.pkceVerifierMaxItems = 2

	if err := p.storePKCEVerifier("state-1", "verifier-1"); err != nil {
		t.Fatalf("storePKCEVerifier(state-1) error = %v", err)
	}
	if err := p.storePKCEVerifier("state-2", "verifier-2"); err != nil {
		t.Fatalf("storePKCEVerifier(state-2) error = %v", err)
	}
	err := p.storePKCEVerifier("state-3", "verifier-3")
	if err == nil {
		t.Fatal("storePKCEVerifier(state-3) error = nil, want cache full rejection")
	}
	if !strings.Contains(err.Error(), "too many in-flight PKCE login attempts") {
		t.Fatalf("storePKCEVerifier(state-3) error = %v, want cache full rejection", err)
	}

	if got, ok := p.pkceVerifier("state-1"); !ok || got != "verifier-1" {
		t.Fatalf("pkceVerifier(state-1) = (%q, %t), want (%q, true)", got, ok, "verifier-1")
	}
	if got, ok := p.pkceVerifier("state-2"); !ok || got != "verifier-2" {
		t.Fatalf("pkceVerifier(state-2) = (%q, %t), want (%q, true)", got, ok, "verifier-2")
	}
	if got, ok := p.pkceVerifier("state-3"); ok || got != "" {
		t.Fatalf("pkceVerifier(state-3) = (%q, %t), want (\"\", false)", got, ok)
	}
}

func TestConfigureAppliesPKCECacheConfig(t *testing.T) {
	server := newTLSDiscoveryServer(t, discoveryDocument{
		AuthorizationEndpoint: "https://issuer.example/auth",
		TokenEndpoint:         "https://issuer.example/token",
		UserinfoEndpoint:      "https://issuer.example/userinfo",
	})
	defer server.Close()

	p := New()
	p.httpClient = server.Client()
	attachTestIndexedDB(t, p)

	err := p.Configure(context.Background(), "", map[string]any{
		"issuerUrl":            server.URL,
		"clientId":             "client-id",
		"pkceVerifierTtl":      "90m",
		"pkceVerifierMaxItems": 2048,
		"indexeddb":            "default",
	})
	if err != nil {
		t.Fatalf("Configure() error = %v", err)
	}
	if got := p.pkceTTL(); got != 90*time.Minute {
		t.Fatalf("pkceTTL() = %s, want %s", got, 90*time.Minute)
	}
	if got := p.maxPKCEVerifierItems(); got != 2048 {
		t.Fatalf("maxPKCEVerifierItems() = %d, want %d", got, 2048)
	}
}

func TestConfigureRejectsInvalidPKCECacheConfig(t *testing.T) {
	server := newTLSDiscoveryServer(t, discoveryDocument{
		AuthorizationEndpoint: "https://issuer.example/auth",
		TokenEndpoint:         "https://issuer.example/token",
		UserinfoEndpoint:      "https://issuer.example/userinfo",
	})
	defer server.Close()

	tests := []struct {
		name    string
		raw     map[string]any
		wantErr string
	}{
		{
			name: "ttl",
			raw: map[string]any{
				"issuerUrl":       server.URL,
				"clientId":        "client-id",
				"pkceVerifierTtl": "0s",
			},
			wantErr: "pkceVerifierTtl must be greater than 0",
		},
		{
			name: "max items",
			raw: map[string]any{
				"issuerUrl":            server.URL,
				"clientId":             "client-id",
				"pkceVerifierMaxItems": 0,
			},
			wantErr: "pkceVerifierMaxItems must be greater than 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New()
			p.httpClient = server.Client()

			err := p.Configure(context.Background(), "", tt.raw)
			if err == nil {
				t.Fatal("Configure() error = nil, want validation failure")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Configure() error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestTokenExchangeRejectsInactiveSubjectToken(t *testing.T) {
	p := New()
	attachGrantStore(t, p)
	_, err := p.Token(context.Background(), &gestalt.TokenRequest{
		GrantType:        grantTypeTokenExchange,
		SubjectToken:     "inactive-token",
		SubjectTokenType: subjectTokenTypeAccessToken,
	})
	if err == nil {
		t.Fatal("Token() error = nil, want inactive subject_token rejection")
	}
	if !strings.Contains(err.Error(), "inactive") {
		t.Fatalf("Token() error = %v, want inactive subject_token", err)
	}
}

func TestPendingOAuthCorrelatesByState(t *testing.T) {
	p := New()
	attachGrantStore(t, p)
	p.cfg = config{ClientID: "client-id"}
	p.doc = discoveryDocument{
		AuthorizationEndpoint: "https://issuer.example/auth",
		TokenEndpoint:         "https://issuer.example/token",
		UserinfoEndpoint:      "https://issuer.example/userinfo",
	}

	_, err := p.Authorize(context.Background(), &gestalt.AuthorizeRequest{
		ResponseType: "code",
		ClientID:     defaultOAuthClientID,
		RedirectURI:  "https://gestalt.example/callback",
		State:        "state-a",
		Scope:        "openid",
	})
	if err != nil {
		t.Fatalf("Authorize(state-a) error = %v", err)
	}
	_, err = p.Authorize(context.Background(), &gestalt.AuthorizeRequest{
		ResponseType: "code",
		ClientID:     defaultOAuthClientID,
		RedirectURI:  "https://gestalt.example/callback",
		State:        "state-b",
		Scope:        "profile",
	})
	if err != nil {
		t.Fatalf("Authorize(state-b) error = %v", err)
	}

	pending, err := p.grants.pendingOAuthForToken(context.Background(), &gestalt.TokenRequest{
		RedirectURI: "https://gestalt.example/callback",
		State:       "state-b",
	})
	if err != nil {
		t.Fatalf("pendingOAuthForToken() error = %v", err)
	}
	if pending.scope != "profile" {
		t.Fatalf("pending scope = %q, want %q", pending.scope, "profile")
	}
}

func TestTokenAuthorizationCodeUsesSharedPendingOAuthStore(t *testing.T) {
	ctx := context.Background()
	db := oidcfake.NewIndexedDB()

	var gotCode string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			if err := r.ParseForm(); err != nil {
				t.Fatalf("ParseForm() error = %v", err)
			}
			gotCode = r.FormValue("code")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "upstream-access-token",
				"token_type":   "Bearer",
				"id_token":     testIDToken("user-123", "User Example"),
			})
		case "/userinfo":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"sub":            "user-123",
				"email":          "user@example.com",
				"name":           "User Example",
				"email_verified": "true",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	authorizer := New()
	authorizer.cfg = config{ClientID: "client-id"}
	authorizer.doc = discoveryDocument{
		AuthorizationEndpoint: server.URL + "/auth",
		TokenEndpoint:         server.URL + "/token",
		UserinfoEndpoint:      server.URL + "/userinfo",
	}
	store, err := openGrantStore(ctx, db, authorizer.now)
	if err != nil {
		t.Fatalf("open authorizer grant store: %v", err)
	}
	authorizer.grants = store
	authorizer.grantsDB = db

	_, err = authorizer.Authorize(ctx, &gestalt.AuthorizeRequest{
		ResponseType: "code",
		ClientID:     defaultOAuthClientID,
		RedirectURI:  "https://gestalt.example/callback",
		State:        "shared-state",
		Scope:        "profile email",
	})
	if err != nil {
		t.Fatalf("Authorize() error = %v", err)
	}

	redeemer := New()
	redeemer.httpClient = server.Client()
	redeemer.cfg = authorizer.cfg
	redeemer.doc = authorizer.doc
	store, err = openGrantStore(ctx, db, redeemer.now)
	if err != nil {
		t.Fatalf("open redeemer grant store: %v", err)
	}
	redeemer.grants = store
	redeemer.grantsDB = db
	claimsStore, err := openClaimsStore(ctx, db, redeemer.now)
	if err != nil {
		t.Fatalf("open redeemer claims store: %v", err)
	}
	redeemer.claims = claimsStore
	redeemer.validateIDTokenFn = func(_ context.Context, rawIDToken string) (*idTokenClaims, error) {
		return parseUnverifiedIDTokenClaims(rawIDToken)
	}

	resp, err := redeemer.Token(ctx, &gestalt.TokenRequest{
		GrantType:   grantTypeAuthorizationCode,
		Code:        "auth-code",
		RedirectURI: "https://gestalt.example/callback",
		ClientID:    defaultOAuthClientID,
		State:       "shared-state",
	})
	if err != nil {
		t.Fatalf("Token() error = %v", err)
	}
	if gotCode != "auth-code" {
		t.Fatalf("token endpoint code = %q, want auth-code", gotCode)
	}
	if resp.AccessToken == "" {
		t.Fatal("Token() returned empty access token")
	}
	if _, err := db.ObjectStore(pendingOAuthStoreName).Get(ctx, "shared-state"); !errors.Is(err, gestalt.ErrNotFound) {
		t.Fatalf("pending OAuth after Token() error = %v, want ErrNotFound", err)
	}
}

func TestGrantStorePersistsAcrossRestart(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	db := oidcfake.NewIndexedDB()

	p := New()
	p.now = func() time.Time { return now }
	store, err := openGrantStore(ctx, db, p.now)
	if err != nil {
		t.Fatalf("openGrantStore() error = %v", err)
	}
	p.grants = store

	subject := "user:persisted"
	issued, err := p.grants.issue(ctx, subject, "openid", defaultOAuthClientID, grantCategoryAPIToken, time.Hour)
	if err != nil {
		t.Fatalf("issue() error = %v", err)
	}
	grantID, accessToken := issued.grantID, issued.accessToken

	restarted := New()
	restarted.now = p.now
	restartedStore, err := openGrantStore(ctx, db, restarted.now)
	if err != nil {
		t.Fatalf("openGrantStore(restart) error = %v", err)
	}
	restarted.grants = restartedStore

	resp := restarted.grants.introspect(ctx, accessToken)
	if !resp.Active {
		t.Fatal("introspect after restart expected active token")
	}
	if resp.Subject != subject {
		t.Fatalf("introspect subject = %q, want %q", resp.Subject, subject)
	}
	ids := restarted.grants.listGrantIDs(ctx, subject)
	if len(ids) != 1 || ids[0] != grantID {
		t.Fatalf("listGrantIDs() = %v, want [%q]", ids, grantID)
	}
}

func TestConfigureRequiresIndexedDB(t *testing.T) {
	server := newTLSDiscoveryServer(t, discoveryDocument{
		AuthorizationEndpoint: "https://issuer.example/auth",
		TokenEndpoint:         "https://issuer.example/token",
		UserinfoEndpoint:      "https://issuer.example/userinfo",
	})
	defer server.Close()

	p := New()
	p.httpClient = server.Client()
	err := p.Configure(context.Background(), "", map[string]any{
		"issuerUrl": server.URL,
		"clientId":  "client-id",
	})
	if err == nil {
		t.Fatal("Configure() error = nil, want indexeddb required")
	}
	if !strings.Contains(err.Error(), "indexeddb is required") {
		t.Fatalf("Configure() error = %v, want indexeddb required", err)
	}
}

func TestConfigureFailsWhenIndexedDBUnavailable(t *testing.T) {
	server := newTLSDiscoveryServer(t, discoveryDocument{
		AuthorizationEndpoint: "https://issuer.example/auth",
		TokenEndpoint:         "https://issuer.example/token",
		UserinfoEndpoint:      "https://issuer.example/userinfo",
	})
	defer server.Close()

	p := New()
	p.httpClient = server.Client()
	p.openIndexedDB = func(context.Context, ...string) (indexeddb.Database, error) {
		return nil, errors.New("indexeddb unavailable")
	}
	err := p.Configure(context.Background(), "", map[string]any{
		"issuerUrl": server.URL,
		"clientId":  "client-id",
		"indexeddb": "default",
	})
	if err == nil {
		t.Fatal("Configure() error = nil, want indexeddb open failure")
	}
	if !strings.Contains(err.Error(), "open indexeddb") {
		t.Fatalf("Configure() error = %v, want indexeddb open failure", err)
	}
}

func TestGrantStorePersistsOnlyTokenHashes(t *testing.T) {
	ctx := context.Background()
	p := New()
	db := attachGrantStoreWithDB(t, p)

	issued, err := p.grants.issue(ctx, "user:hash-only@example.com", "openid", defaultOAuthClientID, grantCategoryAPIToken, time.Hour)
	if err != nil {
		t.Fatalf("issue() error = %v", err)
	}
	accessToken := issued.accessToken
	tokenHash := hashToken(accessToken)

	record, err := db.ObjectStore(tokenHashStoreName).Get(ctx, tokenHash)
	if err != nil {
		t.Fatalf("Get(token hash) error = %v", err)
	}
	if _, ok := record["access_token"]; ok {
		t.Fatalf("token record contains access_token field: %#v", record)
	}
	if got := recordString(record, "id"); got != tokenHash {
		t.Fatalf("token record id = %q, want hash %q", got, tokenHash)
	}
}

func TestTokenExchangeAttenuatesScope(t *testing.T) {
	p := New()
	attachGrantStore(t, p)
	ctx := context.Background()
	subject := "user:owner@example.com"

	tests := []struct {
		name          string
		sourceScope   string
		requested     string
		wantScope     string
		wantErrSubstr string
	}{
		{
			name:        "same scope succeeds",
			sourceScope: "openid",
			requested:   "openid",
			wantScope:   "openid",
		},
		{
			name:          "broader requested scope fails",
			sourceScope:   "deal-hub:read",
			requested:     "deal-hub:write",
			wantErrSubstr: "exceeds subject token scope",
		},
		{
			name:        "narrower requested scope succeeds",
			sourceScope: "deal-hub:read deal-hub:write",
			requested:   "deal-hub:read",
			wantScope:   "deal-hub:read",
		},
		{
			name:        "empty requested inherits restricted source scope",
			sourceScope: "deal-hub:read",
			requested:   "",
			wantScope:   "deal-hub:read",
		},
		{
			name:        "unrestricted source accepts requested scope",
			sourceScope: "",
			requested:   "deal-hub:write",
			wantScope:   "deal-hub:write",
		},
		{
			name:        "unrestricted source with empty requested stays unrestricted",
			sourceScope: "",
			requested:   "",
			wantScope:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			issued, err := p.grants.issue(ctx, subject, tt.sourceScope, defaultOAuthClientID, grantCategorySession, time.Hour)
			if err != nil {
				t.Fatalf("issue() error = %v", err)
			}
			beforeIDs := p.grants.listGrantIDs(ctx, subject)

			tokenResp, err := p.Token(ctx, &gestalt.TokenRequest{
				GrantType:        grantTypeTokenExchange,
				SubjectToken:     issued.accessToken,
				SubjectTokenType: subjectTokenTypeAccessToken,
				Scope:            tt.requested,
			})
			if tt.wantErrSubstr != "" {
				if err == nil {
					t.Fatal("Token() error = nil, want scope attenuation failure")
				}
				if !strings.Contains(err.Error(), tt.wantErrSubstr) {
					t.Fatalf("Token() error = %v, want substring %q", err, tt.wantErrSubstr)
				}
				afterIDs := p.grants.listGrantIDs(ctx, subject)
				if len(afterIDs) != len(beforeIDs) {
					t.Fatalf("grant count = %d, want %d after rejected exchange", len(afterIDs), len(beforeIDs))
				}
				return
			}
			if err != nil {
				t.Fatalf("Token() error = %v", err)
			}
			if tokenResp.Scope != tt.wantScope {
				t.Fatalf("Token() scope = %q, want %q", tokenResp.Scope, tt.wantScope)
			}
			introspectResp, err := p.Introspect(ctx, &gestalt.IntrospectRequest{Token: tokenResp.AccessToken})
			if err != nil {
				t.Fatalf("Introspect() error = %v", err)
			}
			if !introspectResp.Active {
				t.Fatal("Introspect() expected active exchanged token")
			}
			if introspectResp.Scope != tt.wantScope {
				t.Fatalf("Introspect() scope = %q, want %q", introspectResp.Scope, tt.wantScope)
			}
		})
	}
}

func TestIssuePersistsGrantAndTokenHashTransactionally(t *testing.T) {
	ctx := context.Background()
	p := New()
	db := attachGrantStoreWithDB(t, p)

	issued, err := p.grants.issue(ctx, "user:tx@example.com", "openid", defaultOAuthClientID, grantCategoryAPIToken, time.Hour)
	if err != nil {
		t.Fatalf("issue() error = %v", err)
	}
	if issued.accessToken == "" || issued.grantID == "" {
		t.Fatal("issue() returned empty token material")
	}

	grantRecord, err := db.ObjectStore(grantStoreName).Get(ctx, issued.grantID)
	if err != nil {
		t.Fatalf("Get(grant) error = %v", err)
	}
	if recordString(grantRecord, "subject") != "user:tx@example.com" {
		t.Fatalf("grant subject = %q, want user:tx@example.com", recordString(grantRecord, "subject"))
	}
	tokenHash := hashToken(issued.accessToken)
	if _, err := db.ObjectStore(tokenHashStoreName).Get(ctx, tokenHash); err != nil {
		t.Fatalf("Get(token hash) error = %v", err)
	}
	resp, err := p.Introspect(ctx, &gestalt.IntrospectRequest{Token: issued.accessToken})
	if err != nil {
		t.Fatalf("Introspect() error = %v", err)
	}
	if !resp.Active {
		t.Fatal("Introspect() expected active token after transactional issue")
	}
}

func TestIssueFailsWhenGrantAddFails(t *testing.T) {
	ctx := context.Background()
	db := oidcfake.NewIndexedDB()
	db.TransactionAddHook = func(storeName string, _ gestalt.Record) error {
		if storeName == grantStoreName {
			return errors.New("grant add failed")
		}
		return nil
	}
	store, err := openGrantStore(ctx, db, time.Now)
	if err != nil {
		t.Fatalf("openGrantStore() error = %v", err)
	}
	issued, err := store.issue(ctx, "user:fail@example.com", "openid", defaultOAuthClientID, grantCategoryAPIToken, time.Hour)
	if err == nil {
		t.Fatal("issue() error = nil, want grant add failure")
	}
	if issued != nil {
		t.Fatalf("issue() = %#v, want nil on failure", issued)
	}
}

func TestIssueFailsWhenTokenHashAddFails(t *testing.T) {
	ctx := context.Background()
	db := oidcfake.NewIndexedDB()
	db.TransactionAddHook = func(storeName string, _ gestalt.Record) error {
		if storeName == tokenHashStoreName {
			return errors.New("token hash add failed")
		}
		return nil
	}
	store, err := openGrantStore(ctx, db, time.Now)
	if err != nil {
		t.Fatalf("openGrantStore() error = %v", err)
	}
	issued, err := store.issue(ctx, "user:fail@example.com", "openid", defaultOAuthClientID, grantCategoryAPIToken, time.Hour)
	if err == nil {
		t.Fatal("issue() error = nil, want token hash add failure")
	}
	if issued != nil {
		t.Fatalf("issue() = %#v, want nil on failure", issued)
	}
	ids := store.listGrantIDs(ctx, "user:fail@example.com")
	if len(ids) != 0 {
		t.Fatalf("listGrantIDs() = %v, want no grants after aborted issue", ids)
	}
}

func TestIssueFailsWhenTransactionCommitFails(t *testing.T) {
	ctx := context.Background()
	db := oidcfake.NewIndexedDB()
	db.TransactionCommitHook = func() error {
		return errors.New("commit failed")
	}
	store, err := openGrantStore(ctx, db, time.Now)
	if err != nil {
		t.Fatalf("openGrantStore() error = %v", err)
	}
	issued, err := store.issue(ctx, "user:fail@example.com", "openid", defaultOAuthClientID, grantCategoryAPIToken, time.Hour)
	if err == nil {
		t.Fatal("issue() error = nil, want commit failure")
	}
	if issued != nil {
		t.Fatalf("issue() = %#v, want nil on failure", issued)
	}
	ids := store.listGrantIDs(ctx, "user:fail@example.com")
	if len(ids) != 0 {
		t.Fatalf("listGrantIDs() = %v, want no grants after failed commit", ids)
	}
}

func TestProviderCloseIsIdempotent(t *testing.T) {
	p := New()
	db := attachTestIndexedDB(t, p)
	p.grantsDB = db
	store, err := openGrantStore(context.Background(), db, time.Now)
	if err != nil {
		t.Fatalf("openGrantStore() error = %v", err)
	}
	p.grants = store

	if err := p.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if p.grantsDB != nil || p.grants != nil {
		t.Fatal("Close() did not clear grants state")
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close() second call error = %v", err)
	}
}

func TestConfigureClosesPreviousIndexedDB(t *testing.T) {
	server := newTLSDiscoveryServer(t, discoveryDocument{
		AuthorizationEndpoint: "https://issuer.example/auth",
		TokenEndpoint:         "https://issuer.example/token",
		UserinfoEndpoint:      "https://issuer.example/userinfo",
	})
	defer server.Close()

	firstDB := oidcfake.NewIndexedDB()
	secondDB := oidcfake.NewIndexedDB()
	call := 0

	p := New()
	p.httpClient = server.Client()
	p.openIndexedDB = func(_ context.Context, _ ...string) (indexeddb.Database, error) {
		call++
		if call == 1 {
			return firstDB, nil
		}
		return secondDB, nil
	}

	if err := p.Configure(context.Background(), "", map[string]any{
		"issuerUrl": server.URL,
		"clientId":  "client-id",
		"indexeddb": "default",
	}); err != nil {
		t.Fatalf("Configure(first) error = %v", err)
	}
	if p.grantsDB != firstDB {
		t.Fatal("Configure(first) did not retain first database")
	}

	firstDB.CloseHook = func() error {
		return errors.New("close failed")
	}
	err := p.Configure(context.Background(), "", map[string]any{
		"issuerUrl": server.URL,
		"clientId":  "client-id",
		"indexeddb": "default",
	})
	if err == nil {
		t.Fatal("Configure(second) error = nil, want close failure propagated")
	}
	if !strings.Contains(err.Error(), "close indexeddb") {
		t.Fatalf("Configure(second) error = %v, want close indexeddb wrapper", err)
	}
}

func newDiscoveryServer(t *testing.T, doc discoveryDocument) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/.well-known/openid-configuration" {
			http.NotFound(w, r)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(doc)
	}))
	if doc.AuthorizationEndpoint == "" {
		doc.AuthorizationEndpoint = server.URL + "/auth"
	}
	if doc.TokenEndpoint == "" {
		doc.TokenEndpoint = server.URL + "/token"
	}
	if doc.UserinfoEndpoint == "" {
		doc.UserinfoEndpoint = server.URL + "/userinfo"
	}
	return server
}

func newTLSDiscoveryServer(t *testing.T, doc discoveryDocument) *httptest.Server {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/.well-known/openid-configuration" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(doc)
	}))
	return server
}

func attachTestIndexedDB(t *testing.T, p *Provider) *oidcfake.IndexedDB {
	t.Helper()
	db := oidcfake.NewIndexedDB()
	p.openIndexedDB = func(_ context.Context, _ ...string) (indexeddb.Database, error) {
		return db, nil
	}
	return db
}

func attachGrantStore(t *testing.T, p *Provider) {
	t.Helper()
	attachGrantStoreWithDB(t, p)
}

func attachGrantStoreWithDB(t *testing.T, p *Provider) *oidcfake.IndexedDB {
	t.Helper()
	db := oidcfake.NewIndexedDB()
	store, err := openGrantStore(context.Background(), db, p.now)
	if err != nil {
		t.Fatalf("openGrantStore() error = %v", err)
	}
	claims, err := openClaimsStore(context.Background(), db, p.now)
	if err != nil {
		t.Fatalf("openClaimsStore() error = %v", err)
	}
	p.grants = store
	p.claims = claims
	p.grantsDB = db
	p.validateIDTokenFn = func(_ context.Context, rawIDToken string) (*idTokenClaims, error) {
		return parseUnverifiedIDTokenClaims(rawIDToken)
	}
	return db
}

func testIDToken(sub, name string) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	payloadBytes, _ := json.Marshal(map[string]string{
		"sub":  sub,
		"name": name,
	})
	payload := base64.RawURLEncoding.EncodeToString(payloadBytes)
	return header + "." + payload + ".test-signature"
}

func TestGrantManagementExcludesSessionGrants(t *testing.T) {
	p := New()
	attachGrantStore(t, p)
	ctx := context.Background()
	subject := "user:owner@example.com"

	sessionIssued, err := p.grants.issue(ctx, subject, "openid", defaultOAuthClientID, grantCategorySession, time.Hour)
	if err != nil {
		t.Fatalf("issue(session) error = %v", err)
	}
	apiIssued, err := p.grants.issue(ctx, subject, "deal-hub:read", defaultOAuthClientID, grantCategoryAPIToken, time.Hour)
	if err != nil {
		t.Fatalf("issue(api_token) error = %v", err)
	}

	callCtx := gestalt.WithIdentityCallContext(ctx, gestalt.IdentityCallContext{
		CallerBearerToken: apiIssued.accessToken,
	})
	listResp, err := p.ListGrants(callCtx, &gestalt.ListGrantsRequest{})
	if err != nil {
		t.Fatalf("ListGrants() error = %v", err)
	}
	if len(listResp.GrantIDs) != 1 || listResp.GrantIDs[0] != apiIssued.grantID {
		t.Fatalf("ListGrants() = %v, want only api token grant %q", listResp.GrantIDs, apiIssued.grantID)
	}
	if _, err := p.GetGrant(callCtx, &gestalt.GetGrantRequest{GrantID: sessionIssued.grantID}); err == nil {
		t.Fatal("GetGrant(session) error = nil, want not found")
	}
	if _, err := p.RevokeGrant(callCtx, &gestalt.RevokeGrantRequest{GrantID: sessionIssued.grantID}); err == nil {
		t.Fatal("RevokeGrant(session) error = nil, want not found")
	}

	sessionIntrospect, err := p.Introspect(context.Background(), &gestalt.IntrospectRequest{Token: sessionIssued.accessToken})
	if err != nil {
		t.Fatalf("Introspect(session) error = %v", err)
	}
	if !sessionIntrospect.Active {
		t.Fatal("Introspect(session) expected active token")
	}
}

func TestFarFutureSentinelExpiryIntrospectsActive(t *testing.T) {
	const farFutureGrantExpiryRFC3339 = "9999-12-31T23:59:59Z"

	p := New()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	p.now = func() time.Time { return now }
	attachGrantStore(t, p)
	ctx := context.Background()
	sentinel, err := time.Parse(time.RFC3339, farFutureGrantExpiryRFC3339)
	if err != nil {
		t.Fatalf("parse sentinel: %v", err)
	}

	grantID := "grant-legacy"
	accessToken := "legacy-plaintext-token"
	tokenHash := hashToken(accessToken)
	db := p.grantsDB
	if err := db.ObjectStore(grantStoreName).Add(ctx, gestalt.Record{
		"id":         grantID,
		"subject":    "user:legacy@example.com",
		"scope":      "deal-hub:read",
		"client_id":  defaultOAuthClientID,
		"created_at": now,
		"expires_at": sentinel,
		"revoked":    false,
		"category":   grantCategoryAPIToken,
	}); err != nil {
		t.Fatalf("Add(grant) error = %v", err)
	}
	if err := db.ObjectStore(tokenHashStoreName).Add(ctx, gestalt.Record{
		"id":         tokenHash,
		"grant_id":   grantID,
		"subject":    "user:legacy@example.com",
		"scope":      "deal-hub:read",
		"client_id":  defaultOAuthClientID,
		"expires_at": sentinel,
	}); err != nil {
		t.Fatalf("Add(token hash) error = %v", err)
	}

	resp, err := p.Introspect(ctx, &gestalt.IntrospectRequest{Token: accessToken})
	if err != nil {
		t.Fatalf("Introspect() error = %v", err)
	}
	if !resp.Active {
		t.Fatal("Introspect() expected active token for far-future sentinel expiry")
	}
}

func TestTokenRejectsMismatchedUserInfoSub(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "upstream-access-token",
				"token_type":   "Bearer",
				"id_token":     testIDToken("user-123", "User Example"),
			})
		case "/userinfo":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"sub":            "other-sub",
				"email":          "user@example.com",
				"name":           "User Example",
				"email_verified": "true",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	p := New()
	attachGrantStore(t, p)
	p.httpClient = server.Client()
	p.cfg = config{ClientID: "client-id"}
	p.doc = discoveryDocument{
		AuthorizationEndpoint: server.URL + "/auth",
		TokenEndpoint:         server.URL + "/token",
		UserinfoEndpoint:      server.URL + "/userinfo",
	}

	_, err := p.Authorize(context.Background(), &gestalt.AuthorizeRequest{
		ResponseType: "code",
		ClientID:     defaultOAuthClientID,
		RedirectURI:  "https://gestalt.example/callback",
		State:        "state",
	})
	if err != nil {
		t.Fatalf("Authorize() error = %v", err)
	}

	_, err = p.Token(context.Background(), &gestalt.TokenRequest{
		GrantType:   grantTypeAuthorizationCode,
		Code:        "auth-code",
		RedirectURI: "https://gestalt.example/callback",
		ClientID:    defaultOAuthClientID,
		State:       "state",
	})
	if err == nil || !strings.Contains(err.Error(), "userinfo sub does not match id_token sub") {
		t.Fatalf("Token() error = %v, want userinfo sub mismatch", err)
	}
}

func TestClaimsStoreDoesNotClearExistingName(t *testing.T) {
	ctx := context.Background()
	db := oidcfake.NewIndexedDB()
	store, err := openClaimsStore(ctx, db, time.Now)
	if err != nil {
		t.Fatalf("openClaimsStore() error = %v", err)
	}
	subject := "user:user@example.com"
	if err := store.upsert(ctx, subjectClaimsRecord{
		Subject: subject,
		Email:   "user@example.com",
		Name:    "Stored Name",
	}); err != nil {
		t.Fatalf("initial upsert: %v", err)
	}
	if err := store.upsert(ctx, subjectClaimsRecord{
		Subject: subject,
		Email:   "user@example.com",
		Name:    "",
	}); err != nil {
		t.Fatalf("empty-name upsert: %v", err)
	}
	record, err := store.get(ctx, subject)
	if err != nil {
		t.Fatalf("get() error = %v", err)
	}
	if record.Name != "Stored Name" {
		t.Fatalf("name = %q, want Stored Name", record.Name)
	}
}
