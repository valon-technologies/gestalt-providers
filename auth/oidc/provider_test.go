package oidc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
