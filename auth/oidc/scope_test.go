package oidc

import "testing"

func TestAttenuateScope(t *testing.T) {
	tests := []struct {
		name      string
		source    string
		requested string
		want      string
		wantErr   bool
	}{
		{
			name:      "same scope",
			source:    "deal-hub:read",
			requested: "deal-hub:read",
			want:      "deal-hub:read",
		},
		{
			name:      "broader requested scope",
			source:    "deal-hub:read",
			requested: "deal-hub:write",
			wantErr:   true,
		},
		{
			name:      "narrower requested scope",
			source:    "deal-hub:read deal-hub:write",
			requested: "deal-hub:read",
			want:      "deal-hub:read",
		},
		{
			name:      "inherit restricted source scope",
			source:    "deal-hub:read",
			requested: "",
			want:      "deal-hub:read",
		},
		{
			name:      "unrestricted source with requested scope",
			source:    "",
			requested: "deal-hub:write",
			want:      "deal-hub:write",
		},
		{
			name:      "unrestricted source with empty requested scope",
			source:    "",
			requested: "",
			want:      "",
		},
		{
			name:      "deduplicates requested scopes",
			source:    "deal-hub:read deal-hub:write",
			requested: "deal-hub:read deal-hub:read",
			want:      "deal-hub:read",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := attenuateScope(tt.source, tt.requested)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("attenuateScope() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("attenuateScope() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("attenuateScope() = %q, want %q", got, tt.want)
			}
		})
	}
}
