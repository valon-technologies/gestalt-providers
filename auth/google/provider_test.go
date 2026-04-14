package google

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestValidateExternalTokenRedactsUnverifiedEmail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/userinfo" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"sub":            "user-123",
			"email":          "user@example.com",
			"name":           "User Example",
			"picture":        "https://issuer.example/avatar.png",
			"email_verified": false,
		})
	}))
	defer server.Close()

	p := New()
	p.httpClient = server.Client()
	originalUserinfoURL := userinfoURL
	userinfoURL = server.URL + "/userinfo"
	defer func() { userinfoURL = originalUserinfoURL }()

	_, err := p.ValidateExternalToken(context.Background(), "access-token")
	if err == nil {
		t.Fatal("ValidateExternalToken() error = nil, want non-nil")
	}
	if got := err.Error(); got != "google auth: email is not verified" {
		t.Fatalf("ValidateExternalToken() error = %q, want %q", got, "google auth: email is not verified")
	}
	if strings.Contains(err.Error(), "user@example.com") {
		t.Fatalf("ValidateExternalToken() leaked email address in error: %q", err.Error())
	}
}
