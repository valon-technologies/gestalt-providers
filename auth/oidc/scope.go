package oidc

import (
	"fmt"
	"strings"
)

func normalizeScope(scope string) []string {
	parts := strings.Fields(strings.TrimSpace(scope))
	seen := make(map[string]struct{}, len(parts))
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		out = append(out, part)
	}
	return out
}

func attenuateScope(sourceScope, requestedScope string) (string, error) {
	source := normalizeScope(sourceScope)
	requested := normalizeScope(requestedScope)

	if len(source) == 0 {
		return strings.Join(requested, " "), nil
	}
	if len(requested) == 0 {
		return strings.Join(source, " "), nil
	}

	sourceSet := make(map[string]struct{}, len(source))
	for _, scope := range source {
		sourceSet[scope] = struct{}{}
	}
	for _, scope := range requested {
		if _, ok := sourceSet[scope]; !ok {
			return "", fmt.Errorf("oidc auth: requested scope %q exceeds subject token scope", scope)
		}
	}
	return strings.Join(requested, " "), nil
}
