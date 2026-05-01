package oidc

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// decodeConfig decodes a raw `map[string]any` config into a typed struct via
// yaml round-trip. Inlined from the previous auth/internal/configutil package.
func decodeConfig(config map[string]any, out any) error {
	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	if err := yaml.Unmarshal(data, out); err != nil {
		return fmt.Errorf("decode config: %w", err)
	}
	return nil
}

// emailVerified returns true only for explicit verified values.
// Inlined from the previous auth/internal/userinfo package.
func emailVerified(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(strings.TrimSpace(v), "true")
	default:
		return false
	}
}

// checkAllowedDomains validates that an email's domain is in the allowed list.
// An empty allowed list permits any domain.
// Inlined from the previous auth/internal/userinfo package.
func checkAllowedDomains(provider string, allowed []string, email string) error {
	if len(allowed) == 0 {
		return nil
	}
	at := strings.LastIndex(email, "@")
	if at < 0 || at == len(email)-1 {
		return fmt.Errorf("%s auth: invalid email %q", provider, email)
	}
	domain := strings.ToLower(email[at+1:])
	for _, allowedDomain := range allowed {
		if strings.EqualFold(strings.TrimSpace(allowedDomain), domain) {
			return nil
		}
	}
	return fmt.Errorf("%s auth: email domain %q is not allowed", provider, domain)
}
