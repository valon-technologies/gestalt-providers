package vault

import "testing"

func TestValidateAddressURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		address           string
		allowInsecureHTTP bool
		wantErr           string
	}{
		{
			name:    "accepts https address",
			address: "https://vault.example.com",
		},
		{
			name:              "accepts localhost http when enabled",
			address:           "http://localhost:8200",
			allowInsecureHTTP: true,
		},
		{
			name:              "accepts loopback ipv4 http when enabled",
			address:           "http://127.0.0.1:8200",
			allowInsecureHTTP: true,
		},
		{
			name:              "accepts loopback ipv6 http when enabled",
			address:           "http://[::1]:8200",
			allowInsecureHTTP: true,
		},
		{
			name:    "rejects insecure address by default",
			address: "http://localhost:8200",
			wantErr: "vault secrets: address must use https unless allowInsecureHttp is true for local loopback development",
		},
		{
			name:              "rejects non loopback http when enabled",
			address:           "http://example.com",
			allowInsecureHTTP: true,
			wantErr:           "vault secrets: address may use http only for localhost or loopback IPs when allowInsecureHttp is true",
		},
		{
			name:    "rejects non http scheme",
			address: "ftp://example.com",
			wantErr: "vault secrets: address must use https",
		},
		{
			name:    "rejects relative address",
			address: "/vault",
			wantErr: "vault secrets: address must be an absolute URL",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateAddressURL(tt.address, tt.allowInsecureHTTP)
			if tt.wantErr == "" && err != nil {
				t.Fatalf("validateAddressURL(%q, %t) error = %v", tt.address, tt.allowInsecureHTTP, err)
			}
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("validateAddressURL(%q, %t) error = nil, want %q", tt.address, tt.allowInsecureHTTP, tt.wantErr)
				}
				if err.Error() != tt.wantErr {
					t.Fatalf("validateAddressURL(%q, %t) error = %q, want %q", tt.address, tt.allowInsecureHTTP, err.Error(), tt.wantErr)
				}
			}
		})
	}
}
