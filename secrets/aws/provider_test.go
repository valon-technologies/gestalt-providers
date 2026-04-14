package aws

import "testing"

func TestValidateEndpointURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		endpoint          string
		allowInsecureHTTP bool
		wantErr           string
	}{
		{
			name:     "accepts https endpoint",
			endpoint: "https://secretsmanager.us-east-1.amazonaws.com",
		},
		{
			name:              "accepts localhost http when enabled",
			endpoint:          "http://localhost:4566",
			allowInsecureHTTP: true,
		},
		{
			name:              "accepts loopback ipv4 http when enabled",
			endpoint:          "http://127.0.0.1:4566",
			allowInsecureHTTP: true,
		},
		{
			name:              "accepts loopback ipv6 http when enabled",
			endpoint:          "http://[::1]:4566",
			allowInsecureHTTP: true,
		},
		{
			name:     "rejects insecure endpoint by default",
			endpoint: "http://localhost:4566",
			wantErr:  "aws secrets: endpoint must use https unless allowInsecureHttp is true for local loopback development",
		},
		{
			name:              "rejects non loopback http when enabled",
			endpoint:          "http://example.com",
			allowInsecureHTTP: true,
			wantErr:           "aws secrets: endpoint may use http only for localhost or loopback IPs when allowInsecureHttp is true",
		},
		{
			name:     "rejects non http scheme",
			endpoint: "ftp://example.com",
			wantErr:  "aws secrets: endpoint must use https",
		},
		{
			name:     "rejects relative endpoint",
			endpoint: "/localstack",
			wantErr:  "aws secrets: endpoint must be an absolute URL",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateEndpointURL(tt.endpoint, tt.allowInsecureHTTP)
			if tt.wantErr == "" && err != nil {
				t.Fatalf("validateEndpointURL(%q, %t) error = %v", tt.endpoint, tt.allowInsecureHTTP, err)
			}
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("validateEndpointURL(%q, %t) error = nil, want %q", tt.endpoint, tt.allowInsecureHTTP, tt.wantErr)
				}
				if err.Error() != tt.wantErr {
					t.Fatalf("validateEndpointURL(%q, %t) error = %q, want %q", tt.endpoint, tt.allowInsecureHTTP, err.Error(), tt.wantErr)
				}
			}
		})
	}
}
