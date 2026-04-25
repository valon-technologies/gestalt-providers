package oidc

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func TestBeginLoginPKCEDoesNotExposeVerifier(t *testing.T) {
	p := New()
	p.cfg = config{
		ClientID: "client-id",
		PKCE:     true,
	}
	p.doc = discoveryDocument{
		AuthorizationEndpoint: "https://issuer.example/auth",
		TokenEndpoint:         "https://issuer.example/token",
		UserinfoEndpoint:      "https://issuer.example/userinfo",
	}

	resp, err := p.BeginLogin(context.Background(), &gestalt.BeginLoginRequest{
		CallbackUrl: "https://gestalt.example/callback",
		HostState:   "host-state",
	})
	if err != nil {
		t.Fatalf("BeginLogin() error = %v", err)
	}
	if len(resp.ProviderState) != 0 {
		t.Fatalf("BeginLogin() exposed ProviderState = %q", string(resp.ProviderState))
	}
	if !strings.Contains(resp.AuthorizationUrl, "code_challenge=") {
		t.Fatalf("BeginLogin() authorization URL missing code_challenge: %s", resp.AuthorizationUrl)
	}
	if _, ok := p.pkceVerifier("host-state"); !ok {
		t.Fatal("BeginLogin() did not retain verifier server-side")
	}
}

func TestCompleteLoginPKCEUsesStoredVerifier(t *testing.T) {
	const hostState = "host-state"

	now := time.Unix(1_700_000_000, 0)
	signingKey := mustGenerateRSAKey(t)
	const keyID = "test-key"

	var gotCodeVerifier string
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			if err := r.ParseForm(); err != nil {
				t.Fatalf("ParseForm() error = %v", err)
			}
			gotCodeVerifier = r.FormValue("code_verifier")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "access-token",
				"token_type":   "Bearer",
				"id_token": mustSignRS256IDToken(t, signingKey, keyID, map[string]any{
					"iss":            server.URL,
					"sub":            "user-123",
					"aud":            "client-id",
					"exp":            now.Add(time.Hour).Unix(),
					"iat":            now.Unix(),
					"email":          "idtoken@example.com",
					"email_verified": true,
				}),
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
		case "/jwks":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"keys": []any{rsaJWK(signingKey, keyID)},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	p := New()
	p.httpClient = server.Client()
	p.now = func() time.Time { return now }
	p.cfg = config{
		ClientID: "client-id",
		PKCE:     true,
	}
	p.doc = discoveryDocument{
		Issuer:                server.URL,
		AuthorizationEndpoint: server.URL + "/auth",
		TokenEndpoint:         server.URL + "/token",
		UserinfoEndpoint:      server.URL + "/userinfo",
		JWKSURI:               server.URL + "/jwks",
		IDTokenSigningAlgs:    []string{"RS256"},
	}

	resp, err := p.BeginLogin(context.Background(), &gestalt.BeginLoginRequest{
		CallbackUrl: "https://gestalt.example/callback",
		HostState:   hostState,
	})
	if err != nil {
		t.Fatalf("BeginLogin() error = %v", err)
	}
	if len(resp.ProviderState) != 0 {
		t.Fatalf("BeginLogin() exposed ProviderState = %q", string(resp.ProviderState))
	}
	wantCodeVerifier, ok := p.pkceVerifier(hostState)
	if !ok {
		t.Fatal("BeginLogin() did not retain verifier")
	}

	user, err := p.CompleteLogin(context.Background(), &gestalt.CompleteLoginRequest{
		Query: map[string]string{
			"code":  "auth-code",
			"state": hostState,
		},
		CallbackUrl: "https://gestalt.example/callback",
	})
	if err != nil {
		t.Fatalf("CompleteLogin() error = %v", err)
	}
	if gotCodeVerifier != wantCodeVerifier {
		t.Fatalf("CompleteLogin() used code_verifier %q, want %q", gotCodeVerifier, wantCodeVerifier)
	}
	if _, ok := p.pkceVerifier(hostState); ok {
		t.Fatal("CompleteLogin() left verifier cached after successful exchange")
	}
	if user.Subject != "user-123" {
		t.Fatalf("CompleteLogin() subject = %q, want %q", user.Subject, "user-123")
	}
	if user.Email != "user@example.com" {
		t.Fatalf("CompleteLogin() email = %q, want %q", user.Email, "user@example.com")
	}
}

func TestCompleteLoginUsesIDTokenClaimsWithoutUserInfo(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	signingKey := mustGenerateRSAKey(t)
	const keyID = "test-key"

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "access-token",
				"token_type":   "Bearer",
				"id_token": mustSignRS256IDToken(t, signingKey, keyID, map[string]any{
					"iss":            server.URL,
					"sub":            "user-123",
					"aud":            "client-id",
					"exp":            now.Add(time.Hour).Unix(),
					"iat":            now.Unix(),
					"email":          "user@example.com",
					"name":           "User Example",
					"picture":        "https://issuer.example/avatar.png",
					"email_verified": true,
				}),
			})
		case "/jwks":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"keys": []any{rsaJWK(signingKey, keyID)},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	p := New()
	p.httpClient = server.Client()
	p.now = func() time.Time { return now }
	p.cfg = config{ClientID: "client-id"}
	p.doc = discoveryDocument{
		Issuer:                server.URL,
		AuthorizationEndpoint: server.URL + "/auth",
		TokenEndpoint:         server.URL + "/token",
		JWKSURI:               server.URL + "/jwks",
		IDTokenSigningAlgs:    []string{"RS256"},
	}

	user, err := p.CompleteLogin(context.Background(), &gestalt.CompleteLoginRequest{
		Query: map[string]string{
			"code": "auth-code",
		},
		CallbackUrl: "https://gestalt.example/callback",
	})
	if err != nil {
		t.Fatalf("CompleteLogin() error = %v", err)
	}
	if user.Subject != "user-123" {
		t.Fatalf("CompleteLogin() subject = %q, want %q", user.Subject, "user-123")
	}
	if user.Email != "user@example.com" {
		t.Fatalf("CompleteLogin() email = %q, want %q", user.Email, "user@example.com")
	}
	if user.DisplayName != "User Example" {
		t.Fatalf("CompleteLogin() display name = %q, want %q", user.DisplayName, "User Example")
	}
}

func TestCompleteLoginRejectsUserInfoSubjectMismatch(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	signingKey := mustGenerateRSAKey(t)
	const keyID = "test-key"

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "access-token",
				"token_type":   "Bearer",
				"id_token": mustSignRS256IDToken(t, signingKey, keyID, map[string]any{
					"iss":            server.URL,
					"sub":            "user-123",
					"aud":            "client-id",
					"exp":            now.Add(time.Hour).Unix(),
					"iat":            now.Unix(),
					"email":          "user@example.com",
					"email_verified": true,
				}),
			})
		case "/userinfo":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"sub":            "different-user",
				"email":          "user@example.com",
				"email_verified": true,
			})
		case "/jwks":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"keys": []any{rsaJWK(signingKey, keyID)},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	p := New()
	p.httpClient = server.Client()
	p.now = func() time.Time { return now }
	p.cfg = config{ClientID: "client-id"}
	p.doc = discoveryDocument{
		Issuer:                server.URL,
		AuthorizationEndpoint: server.URL + "/auth",
		TokenEndpoint:         server.URL + "/token",
		UserinfoEndpoint:      server.URL + "/userinfo",
		JWKSURI:               server.URL + "/jwks",
		IDTokenSigningAlgs:    []string{"RS256"},
	}

	_, err := p.CompleteLogin(context.Background(), &gestalt.CompleteLoginRequest{
		Query: map[string]string{
			"code": "auth-code",
		},
		CallbackUrl: "https://gestalt.example/callback",
	})
	if err == nil {
		t.Fatal("CompleteLogin() error = nil, want userinfo sub mismatch rejection")
	}
	if !strings.Contains(err.Error(), "userinfo subject") {
		t.Fatalf("CompleteLogin() error = %v, want userinfo subject mismatch", err)
	}
}

func TestValidateExternalTokenSupportsIDTokensAndAccessTokens(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	signingKey := mustGenerateRSAKey(t)
	const keyID = "test-key"

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/userinfo":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"sub":            "user-123",
				"email":          "userinfo@example.com",
				"name":           "User Info",
				"email_verified": true,
			})
		case "/jwks":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"keys": []any{rsaJWK(signingKey, keyID)},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	p := New()
	p.httpClient = server.Client()
	p.now = func() time.Time { return now }
	p.cfg = config{ClientID: "client-id"}
	p.doc = discoveryDocument{
		Issuer:             server.URL,
		UserinfoEndpoint:   server.URL + "/userinfo",
		JWKSURI:            server.URL + "/jwks",
		IDTokenSigningAlgs: []string{"RS256"},
	}

	t.Run("id token", func(t *testing.T) {
		token := mustSignRS256IDToken(t, signingKey, keyID, map[string]any{
			"iss":            server.URL,
			"sub":            "user-123",
			"aud":            "client-id",
			"exp":            now.Add(time.Hour).Unix(),
			"iat":            now.Unix(),
			"email":          "idtoken@example.com",
			"email_verified": true,
		})

		user, err := p.ValidateExternalToken(context.Background(), token)
		if err != nil {
			t.Fatalf("ValidateExternalToken() error = %v", err)
		}
		if user.Email != "idtoken@example.com" {
			t.Fatalf("ValidateExternalToken() email = %q, want %q", user.Email, "idtoken@example.com")
		}
	})

	t.Run("jwt access token fallback", func(t *testing.T) {
		token := mustSignRS256IDToken(t, signingKey, keyID, map[string]any{
			"iss":            server.URL,
			"sub":            "user-123",
			"aud":            "other-client",
			"exp":            now.Add(time.Hour).Unix(),
			"iat":            now.Unix(),
			"email":          "jwt-access@example.com",
			"email_verified": true,
		})

		user, err := p.ValidateExternalToken(context.Background(), token)
		if err != nil {
			t.Fatalf("ValidateExternalToken() error = %v", err)
		}
		if user.Email != "userinfo@example.com" {
			t.Fatalf("ValidateExternalToken() email = %q, want %q", user.Email, "userinfo@example.com")
		}
	})

	t.Run("opaque access token", func(t *testing.T) {
		user, err := p.ValidateExternalToken(context.Background(), "opaque-access-token")
		if err != nil {
			t.Fatalf("ValidateExternalToken() error = %v", err)
		}
		if user.Email != "userinfo@example.com" {
			t.Fatalf("ValidateExternalToken() email = %q, want %q", user.Email, "userinfo@example.com")
		}
	})
}

func TestCompleteLoginUsesIDTokenClaimsWhenUserInfoFails(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	signingKey := mustGenerateRSAKey(t)
	const keyID = "test-key"

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "access-token",
				"token_type":   "Bearer",
				"id_token": mustSignRS256IDToken(t, signingKey, keyID, map[string]any{
					"iss":            server.URL,
					"sub":            "user-123",
					"aud":            "client-id",
					"exp":            now.Add(time.Hour).Unix(),
					"iat":            now.Unix(),
					"email":          "user@example.com",
					"name":           "User Example",
					"picture":        "https://issuer.example/avatar.png",
					"email_verified": true,
				}),
			})
		case "/userinfo":
			http.Error(w, "userinfo disabled for this client", http.StatusForbidden)
		case "/jwks":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"keys": []any{rsaJWK(signingKey, keyID)},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	p := New()
	p.httpClient = server.Client()
	p.now = func() time.Time { return now }
	p.cfg = config{ClientID: "client-id"}
	p.doc = discoveryDocument{
		Issuer:                server.URL,
		AuthorizationEndpoint: server.URL + "/auth",
		TokenEndpoint:         server.URL + "/token",
		UserinfoEndpoint:      server.URL + "/userinfo",
		JWKSURI:               server.URL + "/jwks",
		IDTokenSigningAlgs:    []string{"RS256"},
	}

	user, err := p.CompleteLogin(context.Background(), &gestalt.CompleteLoginRequest{
		Query: map[string]string{
			"code": "auth-code",
		},
		CallbackUrl: "https://gestalt.example/callback",
	})
	if err != nil {
		t.Fatalf("CompleteLogin() error = %v", err)
	}
	if user.Subject != "user-123" {
		t.Fatalf("CompleteLogin() subject = %q, want %q", user.Subject, "user-123")
	}
	if user.Email != "user@example.com" {
		t.Fatalf("CompleteLogin() email = %q, want %q", user.Email, "user@example.com")
	}
}

func TestValidateExternalTokenSupportsES256IDTokens(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	signingKey := mustGenerateECDSAKey(t)
	const keyID = "test-key"

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/jwks":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"keys": []any{ecdsaJWK(signingKey, keyID)},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	p := New()
	p.httpClient = server.Client()
	p.now = func() time.Time { return now }
	p.cfg = config{ClientID: "client-id"}
	p.doc = discoveryDocument{
		Issuer:             server.URL,
		JWKSURI:            server.URL + "/jwks",
		IDTokenSigningAlgs: []string{"ES256"},
	}

	token := mustSignES256IDToken(t, signingKey, keyID, map[string]any{
		"iss":            server.URL,
		"sub":            "user-123",
		"aud":            "client-id",
		"exp":            now.Add(time.Hour).Unix(),
		"iat":            now.Unix(),
		"email":          "es256@example.com",
		"email_verified": true,
	})

	user, err := p.ValidateExternalToken(context.Background(), token)
	if err != nil {
		t.Fatalf("ValidateExternalToken() error = %v", err)
	}
	if user.Email != "es256@example.com" {
		t.Fatalf("ValidateExternalToken() email = %q, want %q", user.Email, "es256@example.com")
	}
}

func TestValidateExternalTokenRejectsAdditionalAudiences(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	signingKey := mustGenerateRSAKey(t)
	const keyID = "test-key"

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/jwks":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"keys": []any{rsaJWK(signingKey, keyID)},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	p := New()
	p.httpClient = server.Client()
	p.now = func() time.Time { return now }
	p.cfg = config{ClientID: "client-id"}
	p.doc = discoveryDocument{
		Issuer:             server.URL,
		JWKSURI:            server.URL + "/jwks",
		IDTokenSigningAlgs: []string{"RS256"},
	}

	token := mustSignRS256IDToken(t, signingKey, keyID, map[string]any{
		"iss":            server.URL,
		"sub":            "user-123",
		"aud":            []string{"client-id", "other-client"},
		"azp":            "client-id",
		"exp":            now.Add(time.Hour).Unix(),
		"iat":            now.Unix(),
		"email":          "user@example.com",
		"email_verified": true,
	})

	_, err := p.ValidateExternalToken(context.Background(), token)
	if err == nil {
		t.Fatal("ValidateExternalToken() error = nil, want additional audience rejection")
	}
	if !strings.Contains(err.Error(), "untrusted audience") {
		t.Fatalf("ValidateExternalToken() error = %v, want untrusted audience rejection", err)
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
	server := newDiscoveryServer(t, discoveryDocument{})
	defer server.Close()

	p := New()
	p.httpClient = server.Client()

	err := p.Configure(context.Background(), "", map[string]any{
		"issuerUrl":         server.URL,
		"clientId":          "client-id",
		"allowInsecureHttp": true,
	})
	if err != nil {
		t.Fatalf("Configure() error = %v", err)
	}
	if p.doc.AuthorizationEndpoint != server.URL+"/auth" {
		t.Fatalf("Configure() authorization_endpoint = %q, want %q", p.doc.AuthorizationEndpoint, server.URL+"/auth")
	}
	if p.doc.JWKSURI != server.URL+"/jwks" {
		t.Fatalf("Configure() jwks_uri = %q, want %q", p.doc.JWKSURI, server.URL+"/jwks")
	}
}

func TestConfigureAllowsDiscoveryWithoutUserInfoEndpoint(t *testing.T) {
	var server *httptest.Server
	doc := discoveryDocument{
		AuthorizationEndpoint: "https://issuer.example/auth",
		TokenEndpoint:         "https://issuer.example/token",
		JWKSURI:               "https://issuer.example/jwks",
	}
	server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/.well-known/openid-configuration" {
			http.NotFound(w, r)
			return
		}
		doc.Issuer = server.URL
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(doc)
	}))
	defer server.Close()

	p := New()
	p.httpClient = server.Client()

	err := p.Configure(context.Background(), "", map[string]any{
		"issuerUrl": server.URL,
		"clientId":  "client-id",
	})
	if err != nil {
		t.Fatalf("Configure() error = %v", err)
	}
	if p.doc.UserinfoEndpoint != "" {
		t.Fatalf("Configure() userinfo_endpoint = %q, want empty", p.doc.UserinfoEndpoint)
	}
}

func TestConfigureRejectsInsecureDiscoveryEndpointsByDefault(t *testing.T) {
	server := newTLSDiscoveryServer(t, discoveryDocument{
		AuthorizationEndpoint: "http://127.0.0.1:8080/auth",
		TokenEndpoint:         "https://issuer.example/token",
		UserinfoEndpoint:      "https://issuer.example/userinfo",
		JWKSURI:               "https://issuer.example/jwks",
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
		JWKSURI:               "https://issuer.example/jwks",
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
		JWKSURI:               "https://issuer.example/jwks",
	})
	defer server.Close()

	p := New()
	p.httpClient = server.Client()

	err := p.Configure(context.Background(), "", map[string]any{
		"issuerUrl":            server.URL,
		"clientId":             "client-id",
		"pkceVerifierTtl":      "90m",
		"pkceVerifierMaxItems": 2048,
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
		JWKSURI:               "https://issuer.example/jwks",
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

func mustGenerateRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey() error = %v", err)
	}
	return key
}

func mustGenerateECDSAKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa.GenerateKey() error = %v", err)
	}
	return key
}

func mustSignRS256IDToken(t *testing.T, key *rsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	headerSegment := mustJWTPart(t, map[string]any{
		"alg": "RS256",
		"kid": kid,
		"typ": "JWT",
	})
	claimsSegment := mustJWTPart(t, claims)
	signingInput := headerSegment + "." + claimsSegment
	digest := sha256.Sum256([]byte(signingInput))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatalf("rsa.SignPKCS1v15() error = %v", err)
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func mustSignES256IDToken(t *testing.T, key *ecdsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	headerSegment := mustJWTPart(t, map[string]any{
		"alg": "ES256",
		"kid": kid,
		"typ": "JWT",
	})
	claimsSegment := mustJWTPart(t, claims)
	signingInput := headerSegment + "." + claimsSegment
	digest := sha256.Sum256([]byte(signingInput))
	r, s, err := ecdsa.Sign(rand.Reader, key, digest[:])
	if err != nil {
		t.Fatalf("ecdsa.Sign() error = %v", err)
	}
	size := (key.Curve.Params().BitSize + 7) / 8
	signature := make([]byte, size*2)
	copy(signature[size-len(r.Bytes()):size], r.Bytes())
	copy(signature[2*size-len(s.Bytes()):], s.Bytes())
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func mustJWTPart(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(data)
}

func rsaJWK(key *rsa.PrivateKey, kid string) map[string]any {
	return map[string]any{
		"kty": "RSA",
		"use": "sig",
		"alg": "RS256",
		"kid": kid,
		"n":   base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
	}
}

func ecdsaJWK(key *ecdsa.PrivateKey, kid string) map[string]any {
	return map[string]any{
		"kty": "EC",
		"use": "sig",
		"alg": "ES256",
		"kid": kid,
		"crv": "P-256",
		"x":   base64.RawURLEncoding.EncodeToString(key.X.Bytes()),
		"y":   base64.RawURLEncoding.EncodeToString(key.Y.Bytes()),
	}
}

func newDiscoveryServer(t *testing.T, doc discoveryDocument) *httptest.Server {
	t.Helper()
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/.well-known/openid-configuration" {
			http.NotFound(w, r)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(doc)
	}))
	if doc.Issuer == "" {
		doc.Issuer = server.URL
	}
	if doc.AuthorizationEndpoint == "" {
		doc.AuthorizationEndpoint = server.URL + "/auth"
	}
	if doc.TokenEndpoint == "" {
		doc.TokenEndpoint = server.URL + "/token"
	}
	if doc.UserinfoEndpoint == "" {
		doc.UserinfoEndpoint = server.URL + "/userinfo"
	}
	if doc.JWKSURI == "" {
		doc.JWKSURI = server.URL + "/jwks"
	}
	return server
}

func newTLSDiscoveryServer(t *testing.T, doc discoveryDocument) *httptest.Server {
	t.Helper()
	var server *httptest.Server
	server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/.well-known/openid-configuration" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(doc)
	}))
	if doc.Issuer == "" {
		doc.Issuer = server.URL
	}
	if doc.AuthorizationEndpoint == "" {
		doc.AuthorizationEndpoint = server.URL + "/auth"
	}
	if doc.TokenEndpoint == "" {
		doc.TokenEndpoint = server.URL + "/token"
	}
	if doc.UserinfoEndpoint == "" {
		doc.UserinfoEndpoint = server.URL + "/userinfo"
	}
	if doc.JWKSURI == "" {
		doc.JWKSURI = server.URL + "/jwks"
	}
	return server
}
