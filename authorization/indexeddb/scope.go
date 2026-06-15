package indexeddb

import (
	"fmt"
	"strings"
)

func bearerScopeFromSubject(subject *Subject) string {
	if subject == nil || subject.Properties == nil {
		return ""
	}
	raw, ok := subject.Properties["scope"]
	if !ok || raw == nil {
		return ""
	}
	value, ok := raw.(string)
	if !ok {
		return strings.TrimSpace(fmt.Sprint(raw))
	}
	return strings.TrimSpace(value)
}

func scopeAllowsAccess(scope, providerName, operationName string) bool {
	scope = strings.TrimSpace(scope)
	providerName = strings.TrimSpace(providerName)
	operationName = strings.TrimSpace(operationName)
	if scope == "" {
		return true
	}
	for _, token := range strings.Fields(scope) {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		if token == providerName {
			return true
		}
		if operationName != "" && token == providerName+":"+operationName {
			return true
		}
	}
	return false
}
