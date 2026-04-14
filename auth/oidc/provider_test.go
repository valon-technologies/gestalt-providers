package oidc

import (
	"context"
	"encoding/json"
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
				"access_token": "access-token",
				"token_type":   "Bearer",
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
	if user.Email != "user@example.com" {
		t.Fatalf("CompleteLogin() email = %q, want %q", user.Email, "user@example.com")
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
