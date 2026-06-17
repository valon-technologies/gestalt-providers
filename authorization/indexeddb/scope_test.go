package indexeddb

import "testing"

func TestScopeAllowsAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		scope     string
		provider  string
		operation string
		want      bool
	}{
		{name: "empty scope allows all", scope: "", provider: "github", operation: "list_repos", want: true},
		{name: "provider scope allows any operation", scope: "github", provider: "github", operation: "list_repos", want: true},
		{name: "provider scope denies other provider", scope: "github", provider: "slack", operation: "post_message", want: false},
		{name: "operation scope allows exact match", scope: "github:list_repos", provider: "github", operation: "list_repos", want: true},
		{name: "operation scope denies other operation", scope: "github:list_repos", provider: "github", operation: "create_issue", want: false},
		{name: "multiple scopes allow second provider", scope: "github slack", provider: "slack", operation: "post_message", want: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := scopeAllowsAccess(tt.scope, tt.provider, tt.operation); got != tt.want {
				t.Fatalf("scopeAllowsAccess(%q, %q, %q) = %v, want %v", tt.scope, tt.provider, tt.operation, got, tt.want)
			}
		})
	}
}

func TestBearerScopeFromSubject(t *testing.T) {
	t.Parallel()

	subject := &Subject{
		Type: "subject",
		Id:   "user:fixture@example.com",
		Properties: map[string]any{
			"scope": "github slack",
		},
	}
	if got := bearerScopeFromSubject(subject); got != "github slack" {
		t.Fatalf("bearerScopeFromSubject() = %q, want %q", got, "github slack")
	}
}
