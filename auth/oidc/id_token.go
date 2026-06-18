package oidc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
)

type idTokenClaims struct {
	Sub  string
	Name string
}

type idTokenValidator func(ctx context.Context, rawIDToken string) (*idTokenClaims, error)

func (p *Provider) validateIDToken(ctx context.Context, rawIDToken string) (*idTokenClaims, error) {
	rawIDToken = strings.TrimSpace(rawIDToken)
	if rawIDToken == "" {
		return nil, fmt.Errorf("oidc auth: id_token is required")
	}
	if p.validateIDTokenFn != nil {
		return p.validateIDTokenFn(ctx, rawIDToken)
	}
	if p.oidcProvider == nil {
		provider, err := oidc.NewProvider(ctx, p.cfg.IssuerURL)
		if err != nil {
			return nil, fmt.Errorf("oidc auth: initialize issuer provider: %w", err)
		}
		p.oidcProvider = provider
	}
	if p.oidcProvider == nil {
		return nil, fmt.Errorf("oidc auth: provider is not configured")
	}
	verifier := p.oidcProvider.Verifier(&oidc.Config{ClientID: p.cfg.ClientID})
	token, err := verifier.Verify(ctx, rawIDToken)
	if err != nil {
		return nil, fmt.Errorf("oidc auth: validate id_token: %w", err)
	}
	var claims struct {
		Sub  string `json:"sub"`
		Name string `json:"name"`
	}
	if err := token.Claims(&claims); err != nil {
		return nil, fmt.Errorf("oidc auth: decode id_token claims: %w", err)
	}
	if strings.TrimSpace(claims.Sub) == "" {
		return nil, fmt.Errorf("oidc auth: id_token missing sub")
	}
	return &idTokenClaims{
		Sub:  strings.TrimSpace(claims.Sub),
		Name: strings.TrimSpace(claims.Name),
	}, nil
}

func parseUnverifiedIDTokenClaims(rawIDToken string) (*idTokenClaims, error) {
	parts := strings.Split(rawIDToken, ".")
	if len(parts) < 2 {
		return nil, fmt.Errorf("oidc auth: invalid id_token")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("oidc auth: decode id_token payload: %w", err)
	}
	var claims struct {
		Sub  string `json:"sub"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, fmt.Errorf("oidc auth: decode id_token claims: %w", err)
	}
	if strings.TrimSpace(claims.Sub) == "" {
		return nil, fmt.Errorf("oidc auth: id_token missing sub")
	}
	return &idTokenClaims{
		Sub:  strings.TrimSpace(claims.Sub),
		Name: strings.TrimSpace(claims.Name),
	}, nil
}
