package externalcredentials

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	extfake "github.com/valon-technologies/gestalt-providers/externalcredentials/default/internal/fake"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testIndexedDBOptions struct {
	seedStore bool
}

type testIndexedDBs struct {
	defaultDB indexeddb.Database
	archiveDB indexeddb.Database
}

func TestExternalCredentialProviderRoundTrip(t *testing.T) {
	provider := New()
	dbs := startTestIndexedDBs(t, testIndexedDBOptions{seedStore: true})
	configureProvider(t, provider, dbs, map[string]any{
		"encryptionKey": "provider-roundtrip-key",
	})

	if err := provider.HealthCheck(context.Background()); err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	meta := provider.Metadata()
	if meta.Kind != gestalt.ProviderKindExternalCredential || meta.Name != "default" {
		t.Fatalf("metadata = %+v, want external credential provider named default", meta)
	}

	grantExpiresAt := time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)
	secretExpiresAt := grantExpiresAt.Add(24 * time.Hour)
	opaqueFields := map[string]string{"api_key": "opaque-secret", "region": "us"}

	for _, tc := range []struct {
		name            string
		subject         string
		audience        string
		kind            string
		encryptedFields []string
		plaintextFields []string
		build           func(qualifier string) *gestalt.ExternalCredential
		assert          func(t *testing.T, got *gestalt.ExternalCredential)
	}{
		{
			name:            "grant",
			subject:         "user:grant-roundtrip",
			audience:        "slack:default",
			kind:            kindGrant,
			encryptedFields: []string{"access_token_encrypted", "refresh_token_encrypted"},
			plaintextFields: []string{"access_token", "refresh_token"},
			build: func(qualifier string) *gestalt.ExternalCredential {
				return &gestalt.ExternalCredential{
					Subject:      "user:grant-roundtrip",
					Audience:     "slack:default",
					Qualifier:    qualifier,
					MetadataJSON: `{"team":"acme"}`,
					Grant: &gestalt.ExternalCredentialGrant{
						AccessToken:       "xoxb-123",
						RefreshToken:      "refresh-123",
						Scope:             "channels:read chat:write",
						ExpiresAt:         testTimePtr(grantExpiresAt),
						RefreshErrorCount: 2,
					},
				}
			},
			assert: func(t *testing.T, got *gestalt.ExternalCredential) {
				grant := got.GetGrant()
				if grant.GetAccessToken() != "xoxb-123" || grant.GetRefreshToken() != "refresh-123" {
					t.Fatalf("grant tokens = access:%q refresh:%q", grant.GetAccessToken(), grant.GetRefreshToken())
				}
				if grant.GetScope() != "channels:read chat:write" || grant.GetRefreshErrorCount() != 2 {
					t.Fatalf("grant = scope:%q errors:%d", grant.GetScope(), grant.GetRefreshErrorCount())
				}
				if grant.GetExpiresAt() == nil || !grant.GetExpiresAt().Equal(grantExpiresAt) {
					t.Fatalf("grant expires_at = %v, want %v", grant.GetExpiresAt(), grantExpiresAt)
				}
				if got.GetMetadataJson() != `{"team":"acme"}` {
					t.Fatalf("metadata_json = %q", got.GetMetadataJson())
				}
			},
		},
		{
			name:            "client_info",
			subject:         "user:client-roundtrip",
			audience:        "registry:default",
			kind:            kindClientInfo,
			encryptedFields: []string{"client_secret_encrypted"},
			plaintextFields: []string{"client_secret"},
			build: func(qualifier string) *gestalt.ExternalCredential {
				return &gestalt.ExternalCredential{
					Subject:   "user:client-roundtrip",
					Audience:  "registry:default",
					Qualifier: qualifier,
					Client: &gestalt.ExternalCredentialClientInfo{
						ClientID:              "client-abc",
						ClientSecret:          "client-secret-xyz",
						ClientSecretExpiresAt: testTimePtr(secretExpiresAt),
					},
				}
			},
			assert: func(t *testing.T, got *gestalt.ExternalCredential) {
				client := got.GetClient()
				if client.GetClientId() != "client-abc" || client.GetClientSecret() != "client-secret-xyz" {
					t.Fatalf("client = id:%q secret:%q", client.GetClientId(), client.GetClientSecret())
				}
				if client.GetClientSecretExpiresAt() == nil || !client.GetClientSecretExpiresAt().Equal(secretExpiresAt) {
					t.Fatalf("client_secret_expires_at = %v, want %v", client.GetClientSecretExpiresAt(), secretExpiresAt)
				}
			},
		},
		{
			name:            "opaque",
			subject:         "user:opaque-roundtrip",
			audience:        "vertex:default",
			kind:            kindOpaque,
			encryptedFields: []string{"fields_encrypted"},
			plaintextFields: []string{"fields"},
			build: func(qualifier string) *gestalt.ExternalCredential {
				return &gestalt.ExternalCredential{
					Subject:   "user:opaque-roundtrip",
					Audience:  "vertex:default",
					Qualifier: qualifier,
					Opaque:    &gestalt.ExternalCredentialOpaque{Fields: opaqueFields},
				}
			},
			assert: func(t *testing.T, got *gestalt.ExternalCredential) {
				if !reflect.DeepEqual(got.GetOpaque().GetFields(), opaqueFields) {
					t.Fatalf("opaque fields = %#v, want %#v", got.GetOpaque().GetFields(), opaqueFields)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			created, err := provider.CreateCredential(ctx, &gestalt.CreateExternalCredentialRequest{Credential: tc.build("q1")})
			if err != nil {
				t.Fatalf("CreateCredential: %v", err)
			}
			if created.GetId() == "" {
				t.Fatal("CreateCredential returned empty id")
			}
			if created.GetCreatedAt() == nil || created.GetUpdatedAt() == nil {
				t.Fatalf("timestamps = created:%v updated:%v, want both set", created.GetCreatedAt(), created.GetUpdatedAt())
			}

			raw, err := dbs.indexedDB("").ObjectStore(storeName).Get(ctx, created.GetId())
			if err != nil {
				t.Fatalf("Get(raw): %v", err)
			}
			if got, _ := raw["kind"].(string); got != tc.kind {
				t.Fatalf("kind = %q, want %q", got, tc.kind)
			}
			for _, field := range tc.encryptedFields {
				if got, _ := raw[field].(string); got == "" {
					t.Fatalf("%s = %q, want ciphertext", field, got)
				}
			}
			for _, field := range tc.plaintextFields {
				if _, ok := raw[field]; ok {
					t.Fatalf("raw record stored plaintext %s: %+v", field, raw)
				}
			}

			got, err := provider.GetCredential(ctx, &gestalt.GetExternalCredentialRequest{Subject: tc.subject, Audience: tc.audience, Qualifier: "q1"})
			if err != nil {
				t.Fatalf("GetCredential: %v", err)
			}
			tc.assert(t, got)

			if _, err := provider.UpsertCredential(ctx, &gestalt.UpsertExternalCredentialRequest{Credential: tc.build("q2")}); err != nil {
				t.Fatalf("UpsertCredential(second qualifier): %v", err)
			}

			listed, err := provider.ListCredentials(ctx, &gestalt.ListExternalCredentialsRequest{Subject: tc.subject, Audience: tc.audience})
			if err != nil {
				t.Fatalf("ListCredentials(subject+audience): %v", err)
			}
			if len(listed.GetCredentials()) != 2 {
				t.Fatalf("credentials len = %d, want 2", len(listed.GetCredentials()))
			}
			bySubject, err := provider.ListCredentials(ctx, &gestalt.ListExternalCredentialsRequest{Subject: tc.subject})
			if err != nil {
				t.Fatalf("ListCredentials(subject): %v", err)
			}
			if len(bySubject.GetCredentials()) != 2 {
				t.Fatalf("subject credentials len = %d, want 2", len(bySubject.GetCredentials()))
			}

			if err := provider.DeleteCredential(ctx, &gestalt.DeleteExternalCredentialRequest{ID: created.GetId()}); err != nil {
				t.Fatalf("DeleteCredential: %v", err)
			}
			if err := provider.DeleteCredential(ctx, &gestalt.DeleteExternalCredentialRequest{ID: created.GetId()}); err != nil {
				t.Fatalf("DeleteCredential(second): %v", err)
			}

			if _, err := provider.GetCredential(ctx, &gestalt.GetExternalCredentialRequest{Subject: tc.subject, Audience: tc.audience, Qualifier: "q1"}); status.Code(err) != codes.NotFound {
				t.Fatalf("GetCredential after delete code = %v, want %v", status.Code(err), codes.NotFound)
			}
			remaining, err := provider.ListCredentials(ctx, &gestalt.ListExternalCredentialsRequest{Subject: tc.subject, Audience: tc.audience})
			if err != nil {
				t.Fatalf("ListCredentials(after delete): %v", err)
			}
			if len(remaining.GetCredentials()) != 1 {
				t.Fatalf("remaining credentials len = %d, want 1", len(remaining.GetCredentials()))
			}
		})
	}
}

func TestExternalCredentialProviderUpsertPreservesCreatedAtOnUpdate(t *testing.T) {
	provider := New()
	dbs := startTestIndexedDBs(t, testIndexedDBOptions{seedStore: true})
	configureProvider(t, provider, dbs, map[string]any{
		"encryptionKey": "provider-upsert-created-at-key",
	})

	createdAt := time.Date(2026, time.June, 1, 10, 0, 0, 0, time.UTC)
	now := createdAt
	provider.now = func() time.Time { return now }

	created, err := provider.CreateCredential(context.Background(), &gestalt.CreateExternalCredentialRequest{
		Credential: grantCredential("user:upsert-created-at", "gmail:default", "default", "first-access-token", "refresh-token", createdAt.Add(time.Hour)),
	})
	if err != nil {
		t.Fatalf("CreateCredential: %v", err)
	}

	now = createdAt.Add(2 * time.Hour)
	updated, err := provider.UpsertCredential(context.Background(), &gestalt.UpsertExternalCredentialRequest{
		Credential: grantCredential("user:upsert-created-at", "gmail:default", "default", "rotated-access-token", "refresh-token", createdAt.Add(3*time.Hour)),
	})
	if err != nil {
		t.Fatalf("UpsertCredential(update): %v", err)
	}
	if updated.GetId() != created.GetId() {
		t.Fatalf("id = %q, want %q preserved", updated.GetId(), created.GetId())
	}
	if updated.GetCreatedAt() == nil || !updated.GetCreatedAt().Equal(createdAt) {
		t.Fatalf("created_at = %v, want %v preserved", updated.GetCreatedAt(), createdAt)
	}
	if updated.GetUpdatedAt() == nil || !updated.GetUpdatedAt().Equal(now) {
		t.Fatalf("updated_at = %v, want %v", updated.GetUpdatedAt(), now)
	}
	if updated.GetGrant().GetAccessToken() != "rotated-access-token" {
		t.Fatalf("access token = %q, want rotated-access-token", updated.GetGrant().GetAccessToken())
	}
}

func TestExternalCredentialProviderCreateCredentialConflict(t *testing.T) {
	provider := New()
	dbs := startTestIndexedDBs(t, testIndexedDBOptions{seedStore: true})
	configureProvider(t, provider, dbs, map[string]any{
		"encryptionKey": "provider-create-conflict-key",
	})

	expiresAt := time.Now().Add(time.Hour)
	if _, err := provider.CreateCredential(context.Background(), &gestalt.CreateExternalCredentialRequest{
		Credential: grantCredential("user:create-conflict", "gmail:default", "default", "first-access-token", "refresh-token", expiresAt),
	}); err != nil {
		t.Fatalf("CreateCredential: %v", err)
	}

	_, err := provider.CreateCredential(context.Background(), &gestalt.CreateExternalCredentialRequest{
		Credential: grantCredential("user:create-conflict", "gmail:default", "default", "second-access-token", "refresh-token", expiresAt),
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("CreateCredential(duplicate key) code = %v, want %v (err=%v)", status.Code(err), codes.AlreadyExists, err)
	}

	if _, err := provider.CreateCredential(context.Background(), &gestalt.CreateExternalCredentialRequest{
		Credential: grantCredential("user:create-conflict", "gmail:default", "secondary", "third-access-token", "refresh-token", expiresAt),
	}); err != nil {
		t.Fatalf("CreateCredential(distinct qualifier): %v", err)
	}
}

func TestExternalCredentialProviderCreateCredentialConcurrentSameKey(t *testing.T) {
	const workers = 8

	provider := New()
	dbs := startTestIndexedDBs(t, testIndexedDBOptions{seedStore: true})
	configureProvider(t, provider, dbs, map[string]any{
		"encryptionKey": "provider-create-concurrent-key",
	})

	start := make(chan struct{})
	var wg sync.WaitGroup
	var created, conflicted atomic.Int32
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := provider.CreateCredential(context.Background(), &gestalt.CreateExternalCredentialRequest{
				Credential: grantCredential("user:create-concurrent", "gmail:default", "default", fmt.Sprintf("access-token-%d", i), "refresh-token", time.Now().Add(time.Hour)),
			})
			switch {
			case err == nil:
				created.Add(1)
			case status.Code(err) == codes.AlreadyExists:
				conflicted.Add(1)
			default:
				t.Errorf("CreateCredential(worker %d): %v", i, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if created.Load() != 1 || conflicted.Load() != workers-1 {
		t.Fatalf("created = %d conflicted = %d, want 1 and %d", created.Load(), conflicted.Load(), workers-1)
	}
	count, err := dbs.indexedDB("").ObjectStore(storeName).Count(context.Background(), nil)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 1 {
		t.Fatalf("stored records = %d, want exactly 1", count)
	}
}

func TestExternalCredentialProviderResolveClientInfoNotResolvable(t *testing.T) {
	provider := New()
	dbs := startTestIndexedDBs(t, testIndexedDBOptions{seedStore: true})
	configureProvider(t, provider, dbs, map[string]any{
		"encryptionKey": "provider-client-resolve-key",
	})

	seedCredential(t, provider, &gestalt.ExternalCredential{
		Subject:   "user:client-resolve",
		Audience:  "registry:default",
		Qualifier: "default",
		Client: &gestalt.ExternalCredentialClientInfo{
			ClientID:     "client-abc",
			ClientSecret: "client-secret-xyz",
		},
	})

	_, err := provider.ResolveCredential(context.Background(), resolveRequest("user:client-resolve", "registry:default", "default", nil))
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ResolveCredential code = %v, want %v (err=%v)", status.Code(err), codes.InvalidArgument, err)
	}
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "not resolvable") {
		t.Fatalf("ResolveCredential message = %q, want not-resolvable explanation", msg)
	}
}

func TestExternalCredentialProviderResolveOpaqueReturnsFields(t *testing.T) {
	provider := New()
	dbs := startTestIndexedDBs(t, testIndexedDBOptions{seedStore: true})
	configureProvider(t, provider, dbs, map[string]any{
		"encryptionKey": "provider-opaque-fields-key",
	})

	fields := map[string]string{"api_key": "opaque-secret", "region": "us"}
	seedCredential(t, provider, &gestalt.ExternalCredential{
		Subject:      "user:opaque-fields",
		Audience:     "vertex:default",
		Qualifier:    "default",
		MetadataJSON: `{"tenant":"acme"}`,
		Opaque:       &gestalt.ExternalCredentialOpaque{Fields: fields},
	})

	req := resolveRequest("user:opaque-fields", "vertex:default", "default", nil)
	req.ConnectionParams = map[string]string{"endpoint": "https://vertex.example.test", "tenant": "request-tenant"}
	resolved, err := provider.ResolveCredential(context.Background(), req)
	if err != nil {
		t.Fatalf("ResolveCredential: %v", err)
	}

	var gotFields map[string]string
	if err := json.Unmarshal([]byte(resolved.GetToken()), &gotFields); err != nil {
		t.Fatalf("Unmarshal(token): %v", err)
	}
	if !reflect.DeepEqual(gotFields, fields) {
		t.Fatalf("token fields = %#v, want %#v", gotFields, fields)
	}
	if resolved.GetExpiresAt() != nil {
		t.Fatalf("expires_at = %v, want nil without token endpoint", resolved.GetExpiresAt())
	}
	if resolved.GetParams()["tenant"] != "acme" || resolved.GetParams()["endpoint"] != "https://vertex.example.test" {
		t.Fatalf("params = %#v, want metadata tenant and request endpoint", resolved.GetParams())
	}
	if !reflect.DeepEqual(resolved.GetCredential().GetOpaque().GetFields(), fields) {
		t.Fatalf("credential fields = %#v, want %#v", resolved.GetCredential().GetOpaque().GetFields(), fields)
	}
}

func TestExternalCredentialProviderResolveOpaqueMintsToken(t *testing.T) {
	const mintExpiresIn = 3600

	provider := New()
	dbs := startTestIndexedDBs(t, testIndexedDBOptions{seedStore: true})
	configureProvider(t, provider, dbs, map[string]any{
		"encryptionKey": "provider-opaque-mint-key",
	})

	mintTime := time.Date(2026, time.June, 12, 12, 0, 0, 0, time.UTC)
	now := mintTime
	provider.now = func() time.Time { return now }

	var mintCalls atomic.Int32
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := mintCalls.Add(1)
		if r.URL.Path != "/tenant/acme/token" {
			t.Errorf("path = %q, want /tenant/acme/token", r.URL.Path)
		}
		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("Decode(payload): %v", err)
		}
		if payload["api_key"] != "mint-secret" {
			t.Errorf("payload api_key = %q, want mint-secret", payload["api_key"])
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"access_token":"minted-token-%d","expires_in":%d}`, call, mintExpiresIn)
	}))
	defer tokenServer.Close()

	fields := map[string]string{"api_key": "mint-secret"}
	seedCredential(t, provider, &gestalt.ExternalCredential{
		Subject:      "user:opaque-mint",
		Audience:     "vertex:default",
		Qualifier:    "default",
		MetadataJSON: `{"tenant":"acme"}`,
		Opaque:       &gestalt.ExternalCredentialOpaque{Fields: fields},
	})

	stored := getCredential(t, provider, "user:opaque-mint", "vertex:default", "default")
	rawBefore, err := dbs.indexedDB("").ObjectStore(storeName).Get(context.Background(), stored.GetId())
	if err != nil {
		t.Fatalf("Get(raw before): %v", err)
	}

	auth := &gestalt.ExternalCredentialAuthConfig{
		Type:          "manual",
		TokenURL:      tokenServer.URL + "/tenant/{tenant}/token",
		TokenExchange: "json",
	}

	first, err := provider.ResolveCredential(context.Background(), resolveRequest("user:opaque-mint", "vertex:default", "default", auth))
	if err != nil {
		t.Fatalf("ResolveCredential(first): %v", err)
	}
	if first.GetToken() != "minted-token-1" {
		t.Fatalf("first token = %q, want minted-token-1", first.GetToken())
	}
	wantExpiry := mintTime.Add(mintExpiresIn * time.Second)
	if first.GetExpiresAt() == nil || !first.GetExpiresAt().Equal(wantExpiry) {
		t.Fatalf("first expires_at = %v, want %v", first.GetExpiresAt(), wantExpiry)
	}
	if first.GetParams()["tenant"] != "acme" {
		t.Fatalf("params = %#v, want tenant metadata projected", first.GetParams())
	}

	now = mintTime.Add(time.Minute)
	second, err := provider.ResolveCredential(context.Background(), resolveRequest("user:opaque-mint", "vertex:default", "default", auth))
	if err != nil {
		t.Fatalf("ResolveCredential(second): %v", err)
	}
	if second.GetToken() != "minted-token-2" {
		t.Fatalf("second token = %q, want minted-token-2 (minted per resolve)", second.GetToken())
	}
	if second.GetExpiresAt() == nil || !second.GetExpiresAt().Equal(now.Add(mintExpiresIn*time.Second)) {
		t.Fatalf("second expires_at = %v, want %v", second.GetExpiresAt(), now.Add(mintExpiresIn*time.Second))
	}
	if mintCalls.Load() != 2 {
		t.Fatalf("mint calls = %d, want one per resolve", mintCalls.Load())
	}

	rawAfter, err := dbs.indexedDB("").ObjectStore(storeName).Get(context.Background(), stored.GetId())
	if err != nil {
		t.Fatalf("Get(raw after): %v", err)
	}
	if !reflect.DeepEqual(rawAfter, rawBefore) {
		t.Fatalf("stored record changed after mint: before=%+v after=%+v", rawBefore, rawAfter)
	}
	got := getCredential(t, provider, "user:opaque-mint", "vertex:default", "default")
	if !reflect.DeepEqual(got.GetOpaque().GetFields(), fields) {
		t.Fatalf("stored fields = %#v, want %#v untouched", got.GetOpaque().GetFields(), fields)
	}
}

func TestExternalCredentialProviderInitializesObjectStore(t *testing.T) {
	provider := New()
	dbs := startTestIndexedDBs(t, testIndexedDBOptions{seedStore: false})
	configureProvider(t, provider, dbs, map[string]any{
		"encryptionKey": "provider-initialize-store-key",
	})

	if err := provider.HealthCheck(context.Background()); err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}

	if _, err := provider.UpsertCredential(context.Background(), &gestalt.UpsertExternalCredentialRequest{
		Credential: grantCredential("user:initialize", "gmail:default", "primary", "access-token", "refresh-token", time.Now().Add(time.Hour)),
	}); err != nil {
		t.Fatalf("UpsertCredential: %v", err)
	}
}

func TestExternalCredentialProviderManualTokenExchange(t *testing.T) {
	provider := New()
	dbs := startTestIndexedDBs(t, testIndexedDBOptions{seedStore: true})
	configureProvider(t, provider, dbs, map[string]any{
		"encryptionKey": "provider-manual-exchange-key",
	})

	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("content-type = %q, want application/json", got)
		}
		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("Decode(payload): %v", err)
		}
		if payload["api_key"] != "manual-secret" || payload["audience"] != "vertex" {
			t.Errorf("payload = %#v, want manual api_key and configured audience", payload)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"token":"manual-access-token"},"refresh_token":"provider-refresh-token","expires_in":3600,"token_type":"Bearer"}`))
	}))
	defer tokenServer.Close()

	credentialJSON := `{"api_key":"manual-secret"}`
	exchanged, err := provider.ExchangeCredential(context.Background(), &gestalt.ExchangeExternalCredentialRequest{
		Provider:       "kimi",
		Connection:     "default",
		ConnectionID:   "kimi:default",
		CredentialJSON: credentialJSON,
		Auth: &gestalt.ExternalCredentialAuthConfig{
			Type:            "manual",
			TokenURL:        tokenServer.URL,
			TokenExchange:   "json",
			AccessTokenPath: "data.token",
			TokenParams: map[string]string{
				"audience": "vertex",
			},
		},
	})
	if err != nil {
		t.Fatalf("ExchangeCredential: %v", err)
	}
	tokenResp := exchanged.GetTokenResponse()
	if tokenResp.GetAccessToken() != "manual-access-token" {
		t.Fatalf("access_token = %q, want manual-access-token", tokenResp.GetAccessToken())
	}
	if tokenResp.GetRefreshSource() != credentialJSON {
		t.Fatalf("refresh_source = %q, want original credential JSON", tokenResp.GetRefreshSource())
	}
	if tokenResp.GetRefreshToken() != "provider-refresh-token" || tokenResp.GetExpiresIn() != 3600 {
		t.Fatalf("token response = %#v, want refresh token and expires_in preserved", tokenResp)
	}
	if tokenResp.GetExtraJson() == "" {
		t.Fatal("extra_json is empty, want raw response captured")
	}
}

func TestExternalCredentialProviderResolveInvalidGrantDeletesStoredCredential(t *testing.T) {
	for _, tc := range []struct {
		name      string
		expiresAt time.Time
	}{
		{name: "expired", expiresAt: time.Now().Add(-1 * time.Minute)},
		{name: "unexpired within refresh threshold", expiresAt: time.Now().Add(2 * time.Minute)},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			provider := New()
			dbs := startTestIndexedDBs(t, testIndexedDBOptions{seedStore: true})

			configureProvider(t, provider, dbs, map[string]any{
				"encryptionKey": "provider-invalid-grant-delete-key-" + strings.ReplaceAll(tc.name, " ", "-"),
			})

			tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := r.ParseForm(); err != nil {
					t.Errorf("ParseForm: %v", err)
				}
				if r.Form.Get("grant_type") != "refresh_token" {
					t.Errorf("grant_type = %q, want refresh_token", r.Form.Get("grant_type"))
				}
				if r.Form.Get("refresh_token") != "revoked-refresh-token" {
					t.Errorf("refresh_token = %q, want revoked-refresh-token", r.Form.Get("refresh_token"))
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"invalid_grant","error_description":"revoked-refresh-token secret should not leak"}`))
			}))
			defer tokenServer.Close()

			subject := "user:invalid-grant-" + strings.ReplaceAll(tc.name, " ", "-")
			seedCredential(t, provider, grantCredential(subject, "gmail:default", "default", "old-access-token", "revoked-refresh-token", tc.expiresAt))

			_, err := provider.ResolveCredential(context.Background(), resolveRequest(subject, "gmail:default", "default", oauthAuth(tokenServer.URL)))
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("ResolveCredential code = %v, want %v (err=%v)", status.Code(err), codes.Unauthenticated, err)
			}
			if msg := status.Convert(err).Message(); strings.Contains(msg, "revoked-refresh-token") || strings.Contains(msg, "secret should not leak") {
				t.Fatalf("ResolveCredential error leaked token endpoint body: %q", msg)
			}

			_, err = provider.GetCredential(context.Background(), &gestalt.GetExternalCredentialRequest{Subject: subject, Audience: "gmail:default", Qualifier: "default"})
			if status.Code(err) != codes.NotFound {
				t.Fatalf("GetCredential after invalid_grant code = %v, want %v", status.Code(err), codes.NotFound)
			}
		})
	}
}

func TestExternalCredentialProviderResolveTransientRefreshFailureRetainsStoredCredential(t *testing.T) {
	for _, tc := range []struct {
		name        string
		expiresAt   time.Time
		wantResolve bool
	}{
		{name: "unexpired within refresh threshold", expiresAt: time.Now().Add(2 * time.Minute), wantResolve: true},
		{name: "expired", expiresAt: time.Now().Add(-1 * time.Minute), wantResolve: false},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			provider := New()
			dbs := startTestIndexedDBs(t, testIndexedDBOptions{seedStore: true})

			configureProvider(t, provider, dbs, map[string]any{
				"encryptionKey": "provider-transient-refresh-key-" + strings.ReplaceAll(tc.name, " ", "-"),
			})

			tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"error":"temporarily_unavailable","error_description":"transient secret should not leak"}`))
			}))
			defer tokenServer.Close()

			subject := "user:transient-" + strings.ReplaceAll(tc.name, " ", "-")
			seedCredential(t, provider, grantCredential(subject, "gmail:default", "default", "old-access-token", "refresh-token", tc.expiresAt))

			resolved, err := provider.ResolveCredential(context.Background(), resolveRequest(subject, "gmail:default", "default", oauthAuth(tokenServer.URL)))
			if tc.wantResolve {
				if err != nil {
					t.Fatalf("ResolveCredential: %v", err)
				}
				if resolved.GetToken() != "old-access-token" {
					t.Fatalf("resolved token = %q, want old-access-token", resolved.GetToken())
				}
			} else if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("ResolveCredential code = %v, want %v (err=%v)", status.Code(err), codes.Unauthenticated, err)
			} else if msg := status.Convert(err).Message(); strings.Contains(msg, "transient secret should not leak") {
				t.Fatalf("ResolveCredential error leaked token endpoint body: %q", msg)
			}

			got := getCredential(t, provider, subject, "gmail:default", "default")
			if got.GetGrant().GetAccessToken() != "old-access-token" || got.GetGrant().GetRefreshErrorCount() != 1 {
				t.Fatalf("retained credential = access:%q errors:%d, want old token and one error", got.GetGrant().GetAccessToken(), got.GetGrant().GetRefreshErrorCount())
			}
		})
	}
}

func TestExternalCredentialProviderCredentialMaintenanceRefreshesDueTargets(t *testing.T) {
	provider := New()
	dbs := startTestIndexedDBs(t, testIndexedDBOptions{seedStore: true})

	var refreshCalls atomic.Int32
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		refreshCalls.Add(1)
		if err := r.ParseForm(); err != nil {
			t.Errorf("ParseForm: %v", err)
		}
		if r.Form.Get("grant_type") != "refresh_token" {
			t.Errorf("grant_type = %q, want refresh_token", r.Form.Get("grant_type"))
		}
		if r.Form.Get("refresh_token") != "due-refresh-token" {
			t.Errorf("refresh_token = %q, want due-refresh-token", r.Form.Get("refresh_token"))
		}
		if r.Form.Get("client_id") != "client-id" || r.Form.Get("client_secret") != "client-secret" {
			t.Errorf("client credentials = %q/%q", r.Form.Get("client_id"), r.Form.Get("client_secret"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"maintained-access-token","refresh_token":"maintained-refresh-token","expires_in":3600}`))
	}))
	defer tokenServer.Close()

	configureProvider(t, provider, dbs, credentialRefreshProviderConfig("maintenance-refresh-key", tokenServer.URL))

	now := time.Now()
	seedCredential(t, provider, grantCredential("user:due", "gmail:default", "default", "old-due-access-token", "due-refresh-token", now.Add(5*time.Minute)))
	seedCredential(t, provider, grantCredential("user:future", "gmail:default", "default", "future-access-token", "future-refresh-token", now.Add(2*time.Hour)))
	seedCredential(t, provider, grantCredential("user:slack", "slack:default", "default", "slack-access-token", "slack-refresh-token", now.Add(5*time.Minute)))
	seedCredential(t, provider, &gestalt.ExternalCredential{
		Subject:   "user:opaque-target",
		Audience:  "gmail:default",
		Qualifier: "default",
		Opaque:    &gestalt.ExternalCredentialOpaque{Fields: map[string]string{"api_key": "opaque-secret"}},
	})
	if err := dbs.indexedDB("").ObjectStore(storeName).Put(context.Background(), gestalt.Record{
		"id":                      "corrupt-non-target",
		"subject":                 "user:corrupt",
		"audience":                "slack:default",
		"qualifier":               "corrupt",
		"kind":                    kindGrant,
		"access_token_encrypted":  "not-ciphertext",
		"refresh_token_encrypted": "not-ciphertext",
		"created_at":              now,
		"updated_at":              now,
	}); err != nil {
		t.Fatalf("Put(corrupt non-target): %v", err)
	}

	stats := provider.runCredentialRefreshOnce(context.Background())
	if stats.Errors != 0 || stats.Refreshed != 1 {
		t.Fatalf("maintenance stats = %+v, want one refresh and no errors", stats)
	}
	if refreshCalls.Load() != 1 {
		t.Fatalf("refreshCalls = %d, want one due target refresh", refreshCalls.Load())
	}

	gotDue := getCredential(t, provider, "user:due", "gmail:default", "default")
	if gotDue.GetGrant().GetAccessToken() != "maintained-access-token" || gotDue.GetGrant().GetRefreshToken() != "maintained-refresh-token" || gotDue.GetGrant().GetRefreshErrorCount() != 0 {
		t.Fatalf("due credential = access:%q refresh:%q errors:%d", gotDue.GetGrant().GetAccessToken(), gotDue.GetGrant().GetRefreshToken(), gotDue.GetGrant().GetRefreshErrorCount())
	}
	gotFuture := getCredential(t, provider, "user:future", "gmail:default", "default")
	if gotFuture.GetGrant().GetAccessToken() != "future-access-token" {
		t.Fatalf("future access token = %q, want unchanged", gotFuture.GetGrant().GetAccessToken())
	}
	gotSlack := getCredential(t, provider, "user:slack", "slack:default", "default")
	if gotSlack.GetGrant().GetAccessToken() != "slack-access-token" {
		t.Fatalf("non-target access token = %q, want unchanged", gotSlack.GetGrant().GetAccessToken())
	}
	gotOpaque := getCredential(t, provider, "user:opaque-target", "gmail:default", "default")
	if gotOpaque.GetOpaque().GetFields()["api_key"] != "opaque-secret" {
		t.Fatalf("opaque target = %#v, want untouched by grant sweep", gotOpaque.GetOpaque().GetFields())
	}
}

func TestExternalCredentialProviderCredentialMaintenanceRejectsConflictingResolvedConnections(t *testing.T) {
	provider := New()

	cfg := credentialRefreshProviderConfig("maintenance-conflict-key", "https://token-a.example.test")
	connections := cfg["resolvedConnections"].([]any)
	duplicate := map[string]any{
		"provider":     "google_calendar",
		"connection":   "default",
		"connectionId": "gmail:default",
		"mode":         "subject",
		"auth": map[string]any{
			"type":         "oauth2",
			"tokenUrl":     "https://token-b.example.test",
			"clientId":     "client-id",
			"clientSecret": "client-secret",
		},
		"credentialRefresh": map[string]any{
			"refreshInterval":     "30m",
			"refreshBeforeExpiry": "45m",
		},
	}
	cfg["resolvedConnections"] = append(connections, duplicate)
	err := provider.Configure(context.Background(), "default", cfg)
	if err == nil {
		t.Fatal("Configure error = nil, want conflicting connectionId rejection")
	}
	if !strings.Contains(err.Error(), "conflicting credential refresh config") {
		t.Fatalf("Configure error = %v, want conflicting credential refresh config", err)
	}
}

func TestExternalCredentialProviderCredentialMaintenanceAcceptsSubjectMode(t *testing.T) {
	provider := New()
	dbs := startTestIndexedDBs(t, testIndexedDBOptions{seedStore: true})

	cfg := credentialRefreshProviderConfig("maintenance-subject-mode-key", "https://token.example.test")
	connections := cfg["resolvedConnections"].([]any)
	target := connections[0].(map[string]any)
	target["mode"] = "subject"

	configureProvider(t, provider, dbs, cfg)
}

func TestExternalCredentialProviderCredentialMaintenanceRejectsUnsupportedAuthConfig(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(auth map[string]any)
		want   string
	}{
		{
			name:   "unknown token exchange",
			mutate: func(auth map[string]any) { auth["tokenExchange"] = "xml" },
			want:   "unknown tokenExchange",
		},
		{
			name:   "manual auth type",
			mutate: func(auth map[string]any) { auth["type"] = "manual" },
			want:   "supports auth.type oauth2",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			provider := New()

			cfg := credentialRefreshProviderConfig("maintenance-invalid-auth-key", "https://token.example.test")
			auth := cfg["resolvedConnections"].([]any)[0].(map[string]any)["auth"].(map[string]any)
			tc.mutate(auth)
			err := provider.Configure(context.Background(), "default", cfg)
			if err == nil {
				t.Fatal("Configure error = nil, want unsupported auth rejection")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Configure error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestExternalCredentialProviderCredentialMaintenanceSharesResolveSingleflight(t *testing.T) {
	provider := New()
	dbs := startTestIndexedDBs(t, testIndexedDBOptions{seedStore: true})
	configureProvider(t, provider, dbs, map[string]any{
		"encryptionKey": "maintenance-singleflight-key",
	})

	seedCredential(t, provider, grantCredential("user:singleflight", "gmail:default", "default", "old-access-token", "rotating-refresh-token", time.Now().Add(5*time.Minute)))

	firstRequestStarted := make(chan struct{})
	releaseFirstRequest := make(chan struct{})
	var requestCount atomic.Int32
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		count := requestCount.Add(1)
		if count == 1 {
			close(firstRequestStarted)
			<-releaseFirstRequest
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"access_token":"singleflight-access-token","refresh_token":"rotated-refresh-token","expires_in":3600}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_grant"}`))
	}))
	defer tokenServer.Close()

	auth := oauthAuth(tokenServer.URL)
	target := credentialRefreshTarget{
		Provider:                    "gmail",
		Connection:                  "default",
		ConnectionID:                "gmail:default",
		RefreshBeforeExpiryDuration: 30 * time.Minute,
		Auth:                        auth,
	}
	st, err := provider.configuredStore()
	if err != nil {
		t.Fatalf("configuredStore: %v", err)
	}
	maintenanceDone := make(chan credentialRefreshStats, 1)
	go func() {
		maintenanceDone <- provider.runCredentialRefreshOnceWith(context.Background(), st, []credentialRefreshTarget{target})
	}()
	select {
	case <-firstRequestStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("maintenance refresh did not reach token endpoint")
	}

	resolveDone := make(chan error, 1)
	go func() {
		_, err := provider.ResolveCredential(context.Background(), resolveRequest("user:singleflight", "gmail:default", "default", auth))
		resolveDone <- err
	}()
	time.Sleep(50 * time.Millisecond)
	close(releaseFirstRequest)

	stats := <-maintenanceDone
	if stats.Errors != 0 || stats.Refreshed != 1 {
		t.Fatalf("maintenance stats = %+v, want one successful refresh", stats)
	}
	if err := <-resolveDone; err != nil {
		t.Fatalf("ResolveCredential: %v", err)
	}
	if requestCount.Load() != 1 {
		t.Fatalf("token endpoint requests = %d, want singleflight to share one refresh", requestCount.Load())
	}
	got := getCredential(t, provider, "user:singleflight", "gmail:default", "default")
	if got.GetGrant().GetAccessToken() != "singleflight-access-token" || got.GetGrant().GetRefreshToken() != "rotated-refresh-token" {
		t.Fatalf("credential = access:%q refresh:%q", got.GetGrant().GetAccessToken(), got.GetGrant().GetRefreshToken())
	}
}

func TestExternalCredentialProviderCredentialMaintenancePreservesTransientFailures(t *testing.T) {
	provider := New()
	dbs := startTestIndexedDBs(t, testIndexedDBOptions{seedStore: true})

	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"temporarily_unavailable","error_description":"transient secret should not leak"}`))
	}))
	defer tokenServer.Close()

	configureProvider(t, provider, dbs, credentialRefreshProviderConfig("maintenance-transient-key", tokenServer.URL))

	seedCredential(t, provider, grantCredential("user:maintenance-transient", "gmail:default", "default", "old-access-token", "refresh-token", time.Now().Add(5*time.Minute)))

	_ = provider.runCredentialRefreshOnce(context.Background())

	got := getCredential(t, provider, "user:maintenance-transient", "gmail:default", "default")
	if got.GetGrant().GetAccessToken() != "old-access-token" || got.GetGrant().GetRefreshToken() != "refresh-token" || got.GetGrant().GetRefreshErrorCount() != 1 {
		t.Fatalf("credential after transient failure = access:%q refresh:%q errors:%d", got.GetGrant().GetAccessToken(), got.GetGrant().GetRefreshToken(), got.GetGrant().GetRefreshErrorCount())
	}
}

func TestExternalCredentialProviderCredentialMaintenanceDeletesInvalidGrant(t *testing.T) {
	provider := New()
	dbs := startTestIndexedDBs(t, testIndexedDBOptions{seedStore: true})

	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_grant","error_description":"revoked secret should not leak"}`))
	}))
	defer tokenServer.Close()

	configureProvider(t, provider, dbs, credentialRefreshProviderConfig("maintenance-invalid-grant-key", tokenServer.URL))

	seedCredential(t, provider, grantCredential("user:maintenance-invalid-grant", "gmail:default", "default", "old-access-token", "revoked-refresh-token", time.Now().Add(5*time.Minute)))

	_ = provider.runCredentialRefreshOnce(context.Background())

	_, err := provider.GetCredential(context.Background(), &gestalt.GetExternalCredentialRequest{Subject: "user:maintenance-invalid-grant", Audience: "gmail:default", Qualifier: "default"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetCredential after invalid_grant code = %v, want %v", status.Code(err), codes.NotFound)
	}
}

func TestExternalCredentialProviderCredentialMaintenanceLifecycleCancelsLoops(t *testing.T) {
	provider := New()
	dbs := startTestIndexedDBs(t, testIndexedDBOptions{seedStore: true})
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"access-token","expires_in":3600}`))
	}))
	defer tokenServer.Close()

	configureProvider(t, provider, dbs, credentialRefreshProviderConfig("maintenance-lifecycle-key", tokenServer.URL))
	provider.mu.RLock()
	firstDone := provider.refreshDone
	provider.mu.RUnlock()
	if firstDone == nil {
		t.Fatal("first refresh loop was not started")
	}

	configureProvider(t, provider, dbs, credentialRefreshProviderConfig("maintenance-lifecycle-key-2", tokenServer.URL))
	select {
	case <-firstDone:
	case <-time.After(2 * time.Second):
		t.Fatal("first refresh loop was not canceled on reconfigure")
	}
	provider.mu.RLock()
	secondDone := provider.refreshDone
	provider.mu.RUnlock()
	if secondDone == nil {
		t.Fatal("second refresh loop was not started")
	}

	if err := provider.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case <-secondDone:
	case <-time.After(2 * time.Second):
		t.Fatal("second refresh loop was not canceled on close")
	}
}

func TestExternalCredentialProviderTokenEndpointErrorsAreSanitized(t *testing.T) {
	for _, tc := range []struct {
		name        string
		body        string
		want        string
		notContains []string
	}{
		{
			name:        "oauth error code",
			body:        `{"error":"invalid_grant","error_description":"oauth description secret"}`,
			want:        "invalid_grant",
			notContains: []string{"oauth description secret"},
		},
		{
			name:        "html body",
			body:        `<html>html secret</html>`,
			notContains: []string{"html secret", "<html>"},
		},
		{
			name:        "malicious error code",
			body:        `{"error":"invalid grant secret","error_description":"malicious description secret"}`,
			notContains: []string{"invalid grant secret", "malicious description secret"},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			provider := New()
			dbs := startTestIndexedDBs(t, testIndexedDBOptions{seedStore: true})

			configureProvider(t, provider, dbs, map[string]any{
				"encryptionKey": "provider-token-error-sanitize-key-" + strings.ReplaceAll(tc.name, " ", "-"),
			})

			tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer tokenServer.Close()

			_, err := provider.ExchangeCredential(context.Background(), &gestalt.ExchangeExternalCredentialRequest{
				Provider:       "manual",
				Connection:     "default",
				ConnectionID:   "manual:default",
				CredentialJSON: `{"api_key":"manual-secret"}`,
				Auth: &gestalt.ExternalCredentialAuthConfig{
					Type:          "manual",
					TokenURL:      tokenServer.URL,
					TokenExchange: "json",
				},
			})
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("ExchangeCredential code = %v, want %v (err=%v)", status.Code(err), codes.Unavailable, err)
			}
			msg := status.Convert(err).Message()
			if tc.want != "" && !strings.Contains(msg, tc.want) {
				t.Fatalf("ExchangeCredential message = %q, want it to contain %q", msg, tc.want)
			}
			for _, forbidden := range append(tc.notContains, "manual-secret") {
				if strings.Contains(msg, forbidden) {
					t.Fatalf("ExchangeCredential message leaked %q: %q", forbidden, msg)
				}
			}
		})
	}
}

func TestExternalCredentialProviderReadsExistingCiphertextFormat(t *testing.T) {
	provider := New()
	dbs := startTestIndexedDBs(t, testIndexedDBOptions{seedStore: true})

	const encryptionKey = "provider-ciphertext-key"
	configureProvider(t, provider, dbs, map[string]any{
		"encryptionKey": encryptionKey,
	})

	key := deriveKey(encryptionKey)
	accessEnc := mustEncryptWithNonce(t, key, []byte("0123456789ab"), "seeded-access-token")
	refreshEnc := mustEncryptWithNonce(t, key, []byte("abcdefghijkl"), "seeded-refresh-token")
	createdAt := time.Date(2026, time.April, 24, 12, 0, 0, 0, time.UTC)
	updatedAt := createdAt.Add(time.Minute)

	if err := dbs.indexedDB("").ObjectStore(storeName).Put(context.Background(), gestalt.Record{
		"id":                      "cred-seeded-1",
		"subject":                 "user:user-seeded",
		"audience":                "slack:default",
		"qualifier":               "workspace-1",
		"kind":                    kindGrant,
		"access_token_encrypted":  accessEnc,
		"refresh_token_encrypted": refreshEnc,
		"scope":                   "channels:read",
		"refresh_error_count":     int64(1),
		"metadata_json":           `{"seeded":true}`,
		"created_at":              createdAt,
		"updated_at":              updatedAt,
	}); err != nil {
		t.Fatalf("Put(seed raw record): %v", err)
	}

	got, err := provider.GetCredential(context.Background(), &gestalt.GetExternalCredentialRequest{
		Subject:   "user:user-seeded",
		Audience:  "slack:default",
		Qualifier: "workspace-1",
	})
	if err != nil {
		t.Fatalf("GetCredential(seed): %v", err)
	}
	grant := got.GetGrant()
	if grant.GetAccessToken() != "seeded-access-token" {
		t.Fatalf("access token = %q, want %q", grant.GetAccessToken(), "seeded-access-token")
	}
	if grant.GetRefreshToken() != "seeded-refresh-token" {
		t.Fatalf("refresh token = %q, want %q", grant.GetRefreshToken(), "seeded-refresh-token")
	}
	if grant.GetScope() != "channels:read" || grant.GetRefreshErrorCount() != 1 {
		t.Fatalf("grant = scope:%q errors:%d", grant.GetScope(), grant.GetRefreshErrorCount())
	}
	if got.GetCreatedAt() == nil || !got.GetCreatedAt().Equal(createdAt) {
		t.Fatalf("created_at = %v, want %v", got.GetCreatedAt(), createdAt)
	}
}

func TestExternalCredentialProviderValidation(t *testing.T) {
	provider := New()
	dbs := startTestIndexedDBs(t, testIndexedDBOptions{seedStore: true})
	configureProvider(t, provider, dbs, map[string]any{
		"encryptionKey": "provider-validation-key",
	})

	ctx := context.Background()

	if _, err := provider.ListCredentials(ctx, &gestalt.ListExternalCredentialsRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ListCredentials(no subject) code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
	if _, err := provider.GetCredential(ctx, &gestalt.GetExternalCredentialRequest{Audience: "gmail:default"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GetCredential(no subject) code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
	if _, err := provider.CreateCredential(ctx, &gestalt.CreateExternalCredentialRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateCredential(no credential) code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
	if err := provider.DeleteCredential(ctx, &gestalt.DeleteExternalCredentialRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("DeleteCredential(no id) code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}

	if _, err := provider.CreateCredential(ctx, &gestalt.CreateExternalCredentialRequest{
		Credential: &gestalt.ExternalCredential{Subject: "user:validation", Audience: "gmail:default"},
	}); err == nil || !strings.Contains(err.Error(), "exactly one of") {
		t.Fatalf("CreateCredential(no payload) error = %v, want exactly-one-of rejection", err)
	}
	if _, err := provider.UpsertCredential(ctx, &gestalt.UpsertExternalCredentialRequest{
		Credential: &gestalt.ExternalCredential{
			Subject:  "user:validation",
			Audience: "gmail:default",
			Grant:    &gestalt.ExternalCredentialGrant{AccessToken: "access-token"},
			Opaque:   &gestalt.ExternalCredentialOpaque{Fields: map[string]string{"api_key": "secret"}},
		},
	}); err == nil || !strings.Contains(err.Error(), "exactly one of") {
		t.Fatalf("UpsertCredential(two payloads) error = %v, want exactly-one-of rejection", err)
	}
	if _, err := provider.UpsertCredential(ctx, &gestalt.UpsertExternalCredentialRequest{
		Credential: &gestalt.ExternalCredential{
			Audience: "gmail:default",
			Grant:    &gestalt.ExternalCredentialGrant{AccessToken: "access-token"},
		},
	}); err == nil || !strings.Contains(err.Error(), "subject is required") {
		t.Fatalf("UpsertCredential(no subject) error = %v, want subject-required rejection", err)
	}
}

func TestExternalCredentialProviderUsesNamedIndexedDBBinding(t *testing.T) {
	provider := New()
	dbs := startTestIndexedDBs(t, testIndexedDBOptions{seedStore: true})
	configureProvider(t, provider, dbs, map[string]any{
		"encryptionKey": "provider-named-indexeddb-key",
		"indexeddb":     "archive",
	})

	created, err := provider.UpsertCredential(context.Background(), &gestalt.UpsertExternalCredentialRequest{
		Credential: grantCredential("user:named-indexeddb", "github:default", "org-1", "gho_named", "refresh-token", time.Now().Add(time.Hour)),
	})
	if err != nil {
		t.Fatalf("UpsertCredential: %v", err)
	}

	raw, err := dbs.indexedDB("archive").ObjectStore(storeName).Get(context.Background(), created.GetId())
	if err != nil {
		t.Fatalf("named indexeddb Get: %v", err)
	}
	if got, _ := raw["access_token_encrypted"].(string); got == "" {
		t.Fatalf("named indexeddb access_token_encrypted = %q, want ciphertext", got)
	}
}

func startTestIndexedDBs(t *testing.T, opts testIndexedDBOptions) *testIndexedDBs {
	t.Helper()

	defaultDB := extfake.NewIndexedDB()
	archiveDB := extfake.NewIndexedDB()
	if opts.seedStore {
		if err := seedExternalCredentialStoreOnClient(context.Background(), defaultDB); err != nil {
			t.Fatalf("seedExternalCredentialStoreOnClient: %v", err)
		}
	}
	if _, err := archiveDB.CreateObjectStore(context.Background(), storeName, externalCredentialSchema()); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		t.Fatalf("CreateObjectStore(archive): %v", err)
	}

	return &testIndexedDBs{
		defaultDB: defaultDB,
		archiveDB: archiveDB,
	}
}

func (s *testIndexedDBs) indexedDB(binding string) indexeddb.Database {
	if binding == "archive" {
		return s.archiveDB
	}
	return s.defaultDB
}

func seedExternalCredentialStoreOnClient(ctx context.Context, client indexeddb.Database) error {
	if _, err := client.CreateObjectStore(ctx, storeName, externalCredentialSchema()); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		return err
	}
	return nil
}

func credentialRefreshProviderConfig(encryptionKey, tokenURL string) map[string]any {
	return map[string]any{
		"encryptionKey": encryptionKey,
		"resolvedConnections": []any{
			map[string]any{
				"provider":     "gmail",
				"connection":   "default",
				"connectionId": "gmail:default",
				"mode":         "subject",
				"auth": map[string]any{
					"type":         "oauth2",
					"tokenUrl":     tokenURL,
					"clientId":     "client-id",
					"clientSecret": "client-secret",
				},
				"credentialRefresh": map[string]any{
					"refreshInterval":     "1h",
					"refreshBeforeExpiry": "30m",
				},
			},
		},
	}
}

func grantCredential(subject, audience, qualifier, accessToken, refreshToken string, expiresAt time.Time) *gestalt.ExternalCredential {
	return &gestalt.ExternalCredential{
		Subject:   subject,
		Audience:  audience,
		Qualifier: qualifier,
		Grant: &gestalt.ExternalCredentialGrant{
			AccessToken:  accessToken,
			RefreshToken: refreshToken,
			ExpiresAt:    testTimePtr(expiresAt),
		},
	}
}

func resolveRequest(subject, connectionID, instance string, auth *gestalt.ExternalCredentialAuthConfig) *gestalt.ResolveExternalCredentialRequest {
	return &gestalt.ResolveExternalCredentialRequest{
		Mode:                "subject",
		CredentialSubjectID: subject,
		ConnectionID:        connectionID,
		Instance:            instance,
		Auth:                auth,
	}
}

func oauthAuth(tokenURL string) *gestalt.ExternalCredentialAuthConfig {
	return &gestalt.ExternalCredentialAuthConfig{
		Type:         "oauth2",
		TokenURL:     tokenURL,
		ClientID:     "client-id",
		ClientSecret: "client-secret",
	}
}

func seedCredential(t *testing.T, provider *Provider, credential *gestalt.ExternalCredential) {
	t.Helper()
	if _, err := provider.UpsertCredential(context.Background(), &gestalt.UpsertExternalCredentialRequest{Credential: credential}); err != nil {
		t.Fatalf("UpsertCredential(seed %s/%s): %v", credential.GetSubject(), credential.GetAudience(), err)
	}
}

func getCredential(t *testing.T, provider *Provider, subject, audience, qualifier string) *gestalt.ExternalCredential {
	t.Helper()
	got, err := provider.GetCredential(context.Background(), &gestalt.GetExternalCredentialRequest{
		Subject:   subject,
		Audience:  audience,
		Qualifier: qualifier,
	})
	if err != nil {
		t.Fatalf("GetCredential(%s/%s/%s): %v", subject, audience, qualifier, err)
	}
	return got
}

func testTimePtr(value time.Time) *time.Time {
	value = value.UTC()
	return &value
}

func configureProvider(t *testing.T, provider *Provider, dbs *testIndexedDBs, raw map[string]any) {
	t.Helper()

	cfg, err := decodeConfig(raw)
	if err != nil {
		t.Fatalf("configureProvider: %v", err)
	}
	st, err := openStore(context.Background(), cfg, dbs.indexedDB(cfg.IndexedDB))
	if err != nil {
		t.Fatalf("configureProvider: %v", err)
	}
	provider.configureStore(cfg, st)
	t.Cleanup(func() { _ = provider.Close() })
}

func mustEncryptWithNonce(t *testing.T, key, nonce []byte, plaintext string) string {
	t.Helper()

	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("aes.NewCipher: %v", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatalf("cipher.NewGCM: %v", err)
	}
	if len(nonce) != gcm.NonceSize() {
		t.Fatalf("nonce size = %d, want %d", len(nonce), gcm.NonceSize())
	}
	ciphertext := gcm.Seal(append([]byte{}, nonce...), nonce, []byte(plaintext), nil)
	return base64.StdEncoding.EncodeToString(ciphertext)
}
