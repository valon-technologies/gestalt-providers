package userinfo

import (
	"fmt"
	"strings"
)

// EmailVerified returns true only for explicit verified values.
func EmailVerified(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(strings.TrimSpace(v), "true")
	default:
		return false
	}
}

func CheckAllowedDomains(provider string, allowed []string, email string) error {
	if len(allowed) == 0 {
		return nil
	}
	at := strings.LastIndex(email, "@")
	if at < 0 || at == len(email)-1 {
		return fmt.Errorf("%s auth: invalid email", provider)
	}
	domain := strings.ToLower(email[at+1:])
	for _, allowedDomain := range allowed {
		if strings.EqualFold(strings.TrimSpace(allowedDomain), domain) {
			return nil
		}
	}
	return fmt.Errorf("%s auth: email domain %q is not allowed", provider, domain)
}
