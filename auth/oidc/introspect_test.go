package oidc

import (
	"context"
	"strings"
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func TestIntrospectEmptyTokenInactive(t *testing.T) {
	p := New()
	attachGrantStore(t, p)

	resp, err := p.Introspect(context.Background(), &gestalt.IntrospectRequest{Token: ""})
	if err != nil {
		t.Fatalf("Introspect() error = %v", err)
	}
	if resp == nil || resp.Active {
		t.Fatalf("Introspect() = %#v, want inactive", resp)
	}
}

func TestIntrospectUnknownTokenInactive(t *testing.T) {
	p := New()
	attachGrantStore(t, p)

	resp, err := p.Introspect(context.Background(), &gestalt.IntrospectRequest{Token: "unknown-token"})
	if err != nil {
		t.Fatalf("Introspect() error = %v", err)
	}
	if resp == nil || resp.Active {
		t.Fatalf("Introspect() = %#v, want inactive", resp)
	}
}

func TestIntrospectSessionAndAPITokensShareOnePath(t *testing.T) {
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
	if strings.HasPrefix(sessionIssued.accessToken, apiTokenPrefix) {
		t.Fatalf("session token = %q, want no %q prefix", sessionIssued.accessToken, apiTokenPrefix)
	}
	if !strings.HasPrefix(apiIssued.accessToken, apiTokenPrefix) {
		t.Fatalf("api token = %q, want %q prefix", apiIssued.accessToken, apiTokenPrefix)
	}

	for _, tc := range []struct {
		name        string
		token       string
		wantScope   string
		wantSubject string
	}{
		{
			name:        "session grant",
			token:       sessionIssued.accessToken,
			wantScope:   "openid",
			wantSubject: subject,
		},
		{
			name:        "api grant",
			token:       apiIssued.accessToken,
			wantScope:   "deal-hub:read",
			wantSubject: subject,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := p.Introspect(ctx, &gestalt.IntrospectRequest{Token: tc.token})
			if err != nil {
				t.Fatalf("Introspect() error = %v", err)
			}
			if resp == nil || !resp.Active {
				t.Fatalf("Introspect() = %#v, want active", resp)
			}
			if resp.Subject != tc.wantSubject {
				t.Fatalf("Introspect() subject = %q, want %q", resp.Subject, tc.wantSubject)
			}
			if resp.Scope != tc.wantScope {
				t.Fatalf("Introspect() scope = %q, want %q", resp.Scope, tc.wantScope)
			}
			if resp.ClientID != defaultOAuthClientID {
				t.Fatalf("Introspect() client_id = %q, want %q", resp.ClientID, defaultOAuthClientID)
			}
		})
	}
}

func TestIntrospectExpiredTokenInactive(t *testing.T) {
	p := New()
	now := time.Unix(1_700_000_000, 0)
	p.now = func() time.Time { return now }
	attachGrantStore(t, p)
	ctx := context.Background()

	issued, err := p.grants.issue(ctx, "user:expired@example.com", "openid", defaultOAuthClientID, grantCategoryAPIToken, time.Minute)
	if err != nil {
		t.Fatalf("issue() error = %v", err)
	}

	p.now = func() time.Time { return now.Add(2 * time.Minute) }
	resp, err := p.Introspect(ctx, &gestalt.IntrospectRequest{Token: issued.accessToken})
	if err != nil {
		t.Fatalf("Introspect() error = %v", err)
	}
	if resp == nil || resp.Active {
		t.Fatalf("Introspect() = %#v, want inactive for expired token", resp)
	}
}

func TestTokenExchangeIntrospectsViaUnifiedPath(t *testing.T) {
	p := New()
	attachGrantStore(t, p)
	ctx := context.Background()

	sessionIssued, err := p.grants.issue(ctx, "user:exchange@example.com", "deal-hub:read", defaultOAuthClientID, grantCategorySession, time.Hour)
	if err != nil {
		t.Fatalf("issue(session) error = %v", err)
	}

	tokenResp, err := p.Token(ctx, &gestalt.TokenRequest{
		GrantType:        grantTypeTokenExchange,
		SubjectToken:     sessionIssued.accessToken,
		SubjectTokenType: subjectTokenTypeAccessToken,
		Scope:            "deal-hub:read",
	})
	if err != nil {
		t.Fatalf("Token() error = %v", err)
	}
	if !strings.HasPrefix(tokenResp.AccessToken, apiTokenPrefix) {
		t.Fatalf("exchanged token = %q, want %q prefix", tokenResp.AccessToken, apiTokenPrefix)
	}

	resp, err := p.Introspect(ctx, &gestalt.IntrospectRequest{Token: tokenResp.AccessToken})
	if err != nil {
		t.Fatalf("Introspect() error = %v", err)
	}
	if resp == nil || !resp.Active {
		t.Fatalf("Introspect() = %#v, want active exchanged token", resp)
	}
	if resp.Subject != "user:exchange@example.com" {
		t.Fatalf("Introspect() subject = %q, want user:exchange@example.com", resp.Subject)
	}
}
