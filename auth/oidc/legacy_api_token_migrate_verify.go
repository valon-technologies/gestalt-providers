package oidc

import (
	"fmt"
	"strings"
)

func VerifyLegacyHashFormat(hashed string) error {
	hashed = strings.TrimSpace(hashed)
	if hashed == "" {
		return fmt.Errorf("legacy hashed_token is empty")
	}
	if len(hashed) != 64 {
		return fmt.Errorf("legacy hashed_token length %d, want 64 hex chars (sha256)", len(hashed))
	}
	for _, r := range hashed {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') {
			continue
		}
		return fmt.Errorf("legacy hashed_token contains non-hex character %q", r)
	}
	return nil
}
