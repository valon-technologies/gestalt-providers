package sqlite

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func TestMigrateCreatesOAuthRegistrationsTable(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "sqlite.db"))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	registration := &gestalt.OAuthRegistration{
		AuthServerURL:         "https://issuer.example",
		RedirectURI:           "http://localhost/callback",
		ClientID:              "client-id",
		ClientSecretSealed:    []byte{0x01, 0x02, 0x03, 0x04},
		AuthorizationEndpoint: "https://issuer.example/oauth/authorize",
		TokenEndpoint:         "https://issuer.example/oauth/token",
		ScopesSupported:       "openid email profile",
		DiscoveredAt:          time.Now().UTC().Truncate(time.Second),
	}
	if err := store.PutOAuthRegistration(ctx, registration); err != nil {
		t.Fatalf("PutOAuthRegistration() error = %v", err)
	}

	got, err := store.GetOAuthRegistration(ctx, registration.AuthServerURL, registration.RedirectURI)
	if err != nil {
		t.Fatalf("GetOAuthRegistration() error = %v", err)
	}
	if got == nil {
		t.Fatal("GetOAuthRegistration() returned nil")
	}
	if got.ClientID != registration.ClientID {
		t.Fatalf("GetOAuthRegistration() client id = %q, want %q", got.ClientID, registration.ClientID)
	}
	if !bytes.Equal(got.ClientSecretSealed, registration.ClientSecretSealed) {
		t.Fatalf("GetOAuthRegistration() client secret = %v, want %v", got.ClientSecretSealed, registration.ClientSecretSealed)
	}
}
