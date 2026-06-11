package onepassword

import (
	"context"
	"errors"
	"testing"
	"time"

	op "github.com/1Password/connect-sdk-go/onepassword"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

type stubClient struct {
	getItem func(itemQuery, vaultQuery string) (*op.Item, error)
}

func (s stubClient) GetItem(itemQuery, vaultQuery string) (*op.Item, error) {
	return s.getItem(itemQuery, vaultQuery)
}

func TestValidateHostURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		host              string
		allowInsecureHTTP bool
		wantErr           string
	}{
		{
			name: "accepts https host",
			host: "https://connect.example.com",
		},
		{
			name:              "accepts localhost http when enabled",
			host:              "http://localhost:8080",
			allowInsecureHTTP: true,
		},
		{
			name:              "accepts loopback ipv4 http when enabled",
			host:              "http://127.0.0.1:8080",
			allowInsecureHTTP: true,
		},
		{
			name:    "rejects insecure host by default",
			host:    "http://localhost:8080",
			wantErr: "onepassword secrets: host must use https unless allowInsecureHttp is true for local loopback development",
		},
		{
			name:              "rejects non loopback http when enabled",
			host:              "http://example.com",
			allowInsecureHTTP: true,
			wantErr:           "onepassword secrets: host may use http only for localhost or loopback IPs when allowInsecureHttp is true",
		},
		{
			name:    "rejects relative host",
			host:    "/connect",
			wantErr: "onepassword secrets: host must be an absolute URL",
		},
		{
			name:    "rejects non http scheme",
			host:    "ftp://connect.example.com",
			wantErr: "onepassword secrets: host must use https",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateHostURL(tt.host, tt.allowInsecureHTTP)
			if tt.wantErr == "" && err != nil {
				t.Fatalf("validateHostURL(%q, %t) error = %v", tt.host, tt.allowInsecureHTTP, err)
			}
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("validateHostURL(%q, %t) error = nil, want %q", tt.host, tt.allowInsecureHTTP, tt.wantErr)
				}
				if err.Error() != tt.wantErr {
					t.Fatalf("validateHostURL(%q, %t) error = %q, want %q", tt.host, tt.allowInsecureHTTP, err.Error(), tt.wantErr)
				}
			}
		})
	}
}

func TestGetSecretAllowsEmptyFieldValue(t *testing.T) {
	t.Parallel()

	provider := Provider{
		client: stubClient{
			getItem: func(itemQuery, vaultQuery string) (*op.Item, error) {
				return &op.Item{
					Fields: []*op.ItemField{
						{Label: "password", Value: ""},
					},
				}, nil
			},
		},
		vault: "vault-id",
		field: "password",
	}

	value, err := provider.GetSecret(context.Background(), "item-name")
	if err != nil {
		t.Fatalf("GetSecret() error = %v", err)
	}
	if value != "" {
		t.Fatalf("GetSecret() value = %q, want empty string", value)
	}
}

func TestGetSecretMissingFieldReturnsNotFound(t *testing.T) {
	t.Parallel()

	provider := Provider{
		client: stubClient{
			getItem: func(itemQuery, vaultQuery string) (*op.Item, error) {
				return &op.Item{
					Fields: []*op.ItemField{
						{Label: "username", Value: "alice"},
					},
				}, nil
			},
		},
		vault: "vault-id",
		field: "password",
	}

	_, err := provider.GetSecret(context.Background(), "item-name")
	if !errors.Is(err, gestalt.ErrSecretNotFound) {
		t.Fatalf("GetSecret() error = %v, want ErrSecretNotFound", err)
	}
}

func TestGetSecretFallsBackToLiteralItemNameContainingSlash(t *testing.T) {
	t.Parallel()

	provider := Provider{
		client: stubClient{
			getItem: func(itemQuery, vaultQuery string) (*op.Item, error) {
				switch itemQuery {
				case "folder":
					return nil, errors.New("Found 0 item(s)")
				case "folder/item":
					return &op.Item{
						Fields: []*op.ItemField{
							{Label: "password", Value: "secret-value"},
						},
					}, nil
				default:
					return nil, errors.New("unexpected item query")
				}
			},
		},
		vault: "vault-id",
		field: "password",
	}

	value, err := provider.GetSecret(context.Background(), "folder/item")
	if err != nil {
		t.Fatalf("GetSecret() error = %v", err)
	}
	if value != "secret-value" {
		t.Fatalf("GetSecret() value = %q, want %q", value, "secret-value")
	}
}

func TestGetSecretHonorsContextTimeout(t *testing.T) {
	t.Parallel()

	blocked := make(chan struct{})
	provider := Provider{
		client: stubClient{
			getItem: func(itemQuery, vaultQuery string) (*op.Item, error) {
				<-blocked
				return nil, nil
			},
		},
		vault: "vault-id",
		field: "password",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := provider.GetSecret(ctx, "item-name")
	close(blocked)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("GetSecret() error = %v, want context deadline exceeded", err)
	}
}
