package externalcredentials

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	relationaldb "github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestExternalCredentialProviderRoundTrip(t *testing.T) {
	startTestIndexedDBBackend(t)
	lifecycle, providerConn := startTestProviderServer(t)
	defer func() { _ = providerConn.Close() }()

	configureProvider(t, lifecycle, map[string]any{
		"encryptionKey": "provider-roundtrip-key",
	})

	health, err := lifecycle.HealthCheck(context.Background(), &emptypb.Empty{}, grpc.WaitForReady(true))
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if !health.GetReady() {
		t.Fatalf("ready = false, message = %q", health.GetMessage())
	}

	meta, err := lifecycle.GetProviderIdentity(context.Background(), &emptypb.Empty{}, grpc.WaitForReady(true))
	if err != nil {
		t.Fatalf("GetProviderIdentity: %v", err)
	}
	if meta.GetKind() != proto.ProviderKind_PROVIDER_KIND_EXTERNAL_CREDENTIAL {
		t.Fatalf("kind = %v, want %v", meta.GetKind(), proto.ProviderKind_PROVIDER_KIND_EXTERNAL_CREDENTIAL)
	}
	if meta.GetName() != "default" {
		t.Fatalf("name = %q, want %q", meta.GetName(), "default")
	}

	client, err := gestalt.ExternalCredentials()
	if err != nil {
		t.Fatalf("ExternalCredentials: %v", err)
	}
	defer func() { _ = client.Close() }()

	lookup := &proto.ExternalCredentialLookup{
		SubjectId:    "user:user-123",
		ConnectionId: "slack:default",
		Instance:     "workspace-1",
	}

	created, err := client.UpsertCredential(context.Background(), &proto.UpsertExternalCredentialRequest{
		Credential: &proto.ExternalCredential{
			SubjectId:         lookup.GetSubjectId(),
			ConnectionId:      lookup.GetConnectionId(),
			Instance:          lookup.GetInstance(),
			AccessToken:       "xoxb-123",
			RefreshToken:      "refresh-123",
			Scopes:            "channels:read chat:write",
			RefreshErrorCount: 2,
			MetadataJson:      `{"team":"acme"}`,
		},
	})
	if err != nil {
		t.Fatalf("UpsertCredential(create): %v", err)
	}
	if created.GetId() == "" {
		t.Fatal("UpsertCredential(create) returned empty id")
	}
	if created.GetCreatedAt() == nil || created.GetUpdatedAt() == nil {
		t.Fatalf("timestamps = created:%v updated:%v, want both set", created.GetCreatedAt(), created.GetUpdatedAt())
	}

	db, err := gestalt.IndexedDB()
	if err != nil {
		t.Fatalf("IndexedDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	raw, err := db.ObjectStore(storeName).Get(context.Background(), created.GetId())
	if err != nil {
		t.Fatalf("Get(raw): %v", err)
	}
	if got, _ := raw["access_token_encrypted"].(string); got == "" {
		t.Fatalf("access_token_encrypted = %q, want ciphertext", got)
	}
	if got, _ := raw["refresh_token_encrypted"].(string); got == "" {
		t.Fatalf("refresh_token_encrypted = %q, want ciphertext", got)
	}
	if _, ok := raw["access_token"]; ok {
		t.Fatalf("raw record stored plaintext access_token: %+v", raw)
	}

	got, err := client.GetCredential(context.Background(), &proto.GetExternalCredentialRequest{Lookup: lookup})
	if err != nil {
		t.Fatalf("GetCredential: %v", err)
	}
	if got.GetAccessToken() != "xoxb-123" || got.GetRefreshToken() != "refresh-123" {
		t.Fatalf("tokens = access:%q refresh:%q", got.GetAccessToken(), got.GetRefreshToken())
	}

	_, err = client.UpsertCredential(context.Background(), &proto.UpsertExternalCredentialRequest{
		Credential: &proto.ExternalCredential{
			SubjectId:    lookup.GetSubjectId(),
			ConnectionId: lookup.GetConnectionId(),
			Instance:     "workspace-2",
			AccessToken:  "xoxb-456",
			RefreshToken: "refresh-456",
		},
	})
	if err != nil {
		t.Fatalf("UpsertCredential(second instance): %v", err)
	}

	listed, err := client.ListCredentials(context.Background(), &proto.ListExternalCredentialsRequest{
		SubjectId:    lookup.GetSubjectId(),
		ConnectionId: lookup.GetConnectionId(),
	})
	if err != nil {
		t.Fatalf("ListCredentials(connection): %v", err)
	}
	if len(listed.GetCredentials()) != 2 {
		t.Fatalf("credentials len = %d, want 2", len(listed.GetCredentials()))
	}

	filtered, err := client.ListCredentials(context.Background(), &proto.ListExternalCredentialsRequest{
		SubjectId:    lookup.GetSubjectId(),
		ConnectionId: lookup.GetConnectionId(),
		Instance:     lookup.GetInstance(),
	})
	if err != nil {
		t.Fatalf("ListCredentials(instance): %v", err)
	}
	if len(filtered.GetCredentials()) != 1 || filtered.GetCredentials()[0].GetId() != created.GetId() {
		t.Fatalf("filtered credentials = %#v, want [%q]", filtered.GetCredentials(), created.GetId())
	}

	if err := client.DeleteCredential(context.Background(), &proto.DeleteExternalCredentialRequest{Id: created.GetId()}); err != nil {
		t.Fatalf("DeleteCredential: %v", err)
	}
	if err := client.DeleteCredential(context.Background(), &proto.DeleteExternalCredentialRequest{Id: created.GetId()}); err != nil {
		t.Fatalf("DeleteCredential(second): %v", err)
	}

	_, err = client.GetCredential(context.Background(), &proto.GetExternalCredentialRequest{Lookup: lookup})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetCredential after delete code = %v, want %v", status.Code(err), codes.NotFound)
	}
}

func TestExternalCredentialProviderManualTokenExchange(t *testing.T) {
	startTestIndexedDBBackend(t)
	lifecycle, providerConn := startTestProviderServer(t)
	defer func() { _ = providerConn.Close() }()

	configureProvider(t, lifecycle, map[string]any{
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

	client, err := gestalt.ExternalCredentials()
	if err != nil {
		t.Fatalf("ExternalCredentials: %v", err)
	}
	defer func() { _ = client.Close() }()

	credentialJSON := `{"api_key":"manual-secret"}`
	exchanged, err := client.ExchangeCredential(context.Background(), &proto.ExchangeExternalCredentialRequest{
		Provider:       "kimi",
		Connection:     "default",
		ConnectionId:   "kimi:default",
		CredentialJson: credentialJSON,
		Auth: &proto.ExternalCredentialAuthConfig{
			Type:            "manual",
			TokenUrl:        tokenServer.URL,
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

func TestExternalCredentialProviderResolvePlatformRefreshToken(t *testing.T) {
	startTestIndexedDBBackend(t)
	lifecycle, providerConn := startTestProviderServer(t)
	defer func() { _ = providerConn.Close() }()

	configureProvider(t, lifecycle, map[string]any{
		"encryptionKey": "provider-platform-refresh-token-key",
	})

	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/tenant/acme/token" {
			t.Errorf("path = %q, want /tenant/acme/token", r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Errorf("ParseForm: %v", err)
		}
		if r.Form.Get("grant_type") != "refresh_token" {
			t.Errorf("grant_type = %q, want refresh_token", r.Form.Get("grant_type"))
		}
		if r.Form.Get("refresh_token") != "platform-refresh-token" {
			t.Errorf("refresh_token = %q, want platform refresh token", r.Form.Get("refresh_token"))
		}
		if r.Form.Get("client_id") != "client-id" || r.Form.Get("client_secret") != "client-secret" {
			t.Errorf("client credentials = %q/%q", r.Form.Get("client_id"), r.Form.Get("client_secret"))
		}
		if r.Form.Get("audience") != "gmail-platform" {
			t.Errorf("audience = %q, want gmail-platform", r.Form.Get("audience"))
		}
		if r.Form.Get("scope") != "" {
			t.Errorf("scope = %q, want empty refresh-token request scope", r.Form.Get("scope"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"platform-access-token","expires_in":3600}`))
	}))
	defer tokenServer.Close()

	client, err := gestalt.ExternalCredentials()
	if err != nil {
		t.Fatalf("ExternalCredentials: %v", err)
	}
	defer func() { _ = client.Close() }()

	resolved, err := client.ResolveCredential(context.Background(), &proto.ResolveExternalCredentialRequest{
		Provider:     "gmail",
		Connection:   "platform",
		ConnectionId: "gmail:platform",
		Mode:         "platform",
		Auth: &proto.ExternalCredentialAuthConfig{
			Type:         "oauth2",
			GrantType:    "refresh_token",
			TokenUrl:     tokenServer.URL + "/tenant/{tenant}/token",
			ClientId:     "client-id",
			ClientSecret: "client-secret",
			RefreshToken: "platform-refresh-token",
			RefreshParams: map[string]string{
				"audience": "gmail-platform",
			},
		},
		ConnectionParams: map[string]string{"tenant": "acme"},
	})
	if err != nil {
		t.Fatalf("ResolveCredential(platform refresh_token): %v", err)
	}
	if resolved.GetToken() != "platform-access-token" {
		t.Fatalf("token = %q, want platform-access-token", resolved.GetToken())
	}
	if resolved.GetExpiresAt() == nil {
		t.Fatal("expires_at is nil, want token expiry")
	}
	if resolved.GetParams()["tenant"] != "acme" {
		t.Fatalf("params = %#v, want tenant copied", resolved.GetParams())
	}
}

func TestExternalCredentialProviderResolvePlatformRefreshTokenRequiresRefreshToken(t *testing.T) {
	startTestIndexedDBBackend(t)
	lifecycle, providerConn := startTestProviderServer(t)
	defer func() { _ = providerConn.Close() }()

	configureProvider(t, lifecycle, map[string]any{
		"encryptionKey": "provider-platform-refresh-token-required-key",
	})

	client, err := gestalt.ExternalCredentials()
	if err != nil {
		t.Fatalf("ExternalCredentials: %v", err)
	}
	defer func() { _ = client.Close() }()

	_, err = client.ResolveCredential(context.Background(), &proto.ResolveExternalCredentialRequest{
		Provider:     "gmail",
		Connection:   "platform",
		ConnectionId: "gmail:platform",
		Mode:         "platform",
		Auth: &proto.ExternalCredentialAuthConfig{
			Type:         "oauth2",
			GrantType:    "refresh_token",
			TokenUrl:     "https://oauth2.googleapis.com/token",
			ClientId:     "client-id",
			ClientSecret: "client-secret",
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ResolveCredential code = %v, want %v (err=%v)", status.Code(err), codes.InvalidArgument, err)
	}
	if !strings.Contains(status.Convert(err).Message(), "auth.refreshToken is required") {
		t.Fatalf("ResolveCredential error = %v, want refreshToken required", err)
	}
}

func TestExternalCredentialProviderResolveRefreshesStoredManualCredential(t *testing.T) {
	startTestIndexedDBBackend(t)
	lifecycle, providerConn := startTestProviderServer(t)
	defer func() { _ = providerConn.Close() }()

	configureProvider(t, lifecycle, map[string]any{
		"encryptionKey": "provider-resolve-refresh-key",
	})

	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tenant/acme/token" {
			t.Errorf("path = %q, want /tenant/acme/token", r.URL.Path)
		}
		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("Decode(payload): %v", err)
		}
		if payload["api_key"] != "refresh-secret" {
			t.Errorf("payload api_key = %q, want refresh-secret", payload["api_key"])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"refreshed-access-token","expires_in":1800}`))
	}))
	defer tokenServer.Close()

	client, err := gestalt.ExternalCredentials()
	if err != nil {
		t.Fatalf("ExternalCredentials: %v", err)
	}
	defer func() { _ = client.Close() }()

	refreshSource := `{"api_key":"refresh-secret"}`
	_, err = client.UpsertCredential(context.Background(), &proto.UpsertExternalCredentialRequest{
		Credential: &proto.ExternalCredential{
			SubjectId:    "user:user-refresh",
			ConnectionId: "kimi:default",
			Instance:     "default",
			AccessToken:  "expired-access-token",
			RefreshToken: refreshSource,
			ExpiresAt:    timestamppb.New(time.Now().Add(-time.Minute)),
			MetadataJson: `{"tenant":"acme"}`,
		},
	})
	if err != nil {
		t.Fatalf("UpsertCredential(seed): %v", err)
	}

	resolved, err := client.ResolveCredential(context.Background(), &proto.ResolveExternalCredentialRequest{
		Provider:            "kimi",
		Connection:          "default",
		ConnectionId:        "kimi:default",
		Mode:                "user",
		CredentialSubjectId: "user:user-refresh",
		Instance:            "default",
		Auth: &proto.ExternalCredentialAuthConfig{
			Type:          "manual",
			TokenUrl:      tokenServer.URL + "/tenant/{tenant}/token",
			TokenExchange: "json",
		},
	})
	if err != nil {
		t.Fatalf("ResolveCredential: %v", err)
	}
	if resolved.GetToken() != "refreshed-access-token" {
		t.Fatalf("token = %q, want refreshed-access-token", resolved.GetToken())
	}
	if resolved.GetCredential() == nil {
		t.Fatal("credential is nil, want refreshed stored credential")
	}
	if resolved.GetCredential().GetRefreshToken() != refreshSource {
		t.Fatalf("refresh_token = %q, want refresh source preserved", resolved.GetCredential().GetRefreshToken())
	}
	if resolved.GetParams()["tenant"] != "acme" {
		t.Fatalf("params = %#v, want tenant metadata projected", resolved.GetParams())
	}

	got, err := client.GetCredential(context.Background(), &proto.GetExternalCredentialRequest{
		Lookup: &proto.ExternalCredentialLookup{
			SubjectId:    "user:user-refresh",
			ConnectionId: "kimi:default",
			Instance:     "default",
		},
	})
	if err != nil {
		t.Fatalf("GetCredential(refreshed): %v", err)
	}
	if got.GetAccessToken() != "refreshed-access-token" || got.GetRefreshErrorCount() != 0 {
		t.Fatalf("stored credential = access:%q errors:%d, want refreshed token and cleared errors", got.GetAccessToken(), got.GetRefreshErrorCount())
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
			startTestIndexedDBBackend(t)
			lifecycle, providerConn := startTestProviderServer(t)
			defer func() { _ = providerConn.Close() }()

			configureProvider(t, lifecycle, map[string]any{
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

			client, err := gestalt.ExternalCredentials()
			if err != nil {
				t.Fatalf("ExternalCredentials: %v", err)
			}
			defer func() { _ = client.Close() }()

			lookup := &proto.ExternalCredentialLookup{
				SubjectId:    "user:user-invalid-grant-" + strings.ReplaceAll(tc.name, " ", "-"),
				ConnectionId: "gmail:default",
				Instance:     "default",
			}
			_, err = client.UpsertCredential(context.Background(), &proto.UpsertExternalCredentialRequest{
				Credential: &proto.ExternalCredential{
					SubjectId:    lookup.GetSubjectId(),
					ConnectionId: lookup.GetConnectionId(),
					Instance:     lookup.GetInstance(),
					AccessToken:  "old-access-token",
					RefreshToken: "revoked-refresh-token",
					ExpiresAt:    timestamppb.New(tc.expiresAt),
				},
			})
			if err != nil {
				t.Fatalf("UpsertCredential(seed): %v", err)
			}

			_, err = client.ResolveCredential(context.Background(), &proto.ResolveExternalCredentialRequest{
				Provider:            "gmail",
				Connection:          "default",
				ConnectionId:        lookup.GetConnectionId(),
				Mode:                "user",
				CredentialSubjectId: lookup.GetSubjectId(),
				Instance:            lookup.GetInstance(),
				Auth: &proto.ExternalCredentialAuthConfig{
					Type:         "oauth2",
					TokenUrl:     tokenServer.URL,
					ClientId:     "client-id",
					ClientSecret: "client-secret",
				},
			})
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("ResolveCredential code = %v, want %v (err=%v)", status.Code(err), codes.Unauthenticated, err)
			}
			if msg := status.Convert(err).Message(); strings.Contains(msg, "revoked-refresh-token") || strings.Contains(msg, "secret should not leak") {
				t.Fatalf("ResolveCredential error leaked token endpoint body: %q", msg)
			}

			_, err = client.GetCredential(context.Background(), &proto.GetExternalCredentialRequest{Lookup: lookup})
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
			startTestIndexedDBBackend(t)
			lifecycle, providerConn := startTestProviderServer(t)
			defer func() { _ = providerConn.Close() }()

			configureProvider(t, lifecycle, map[string]any{
				"encryptionKey": "provider-transient-refresh-key-" + strings.ReplaceAll(tc.name, " ", "-"),
			})

			tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"error":"temporarily_unavailable","error_description":"transient secret should not leak"}`))
			}))
			defer tokenServer.Close()

			client, err := gestalt.ExternalCredentials()
			if err != nil {
				t.Fatalf("ExternalCredentials: %v", err)
			}
			defer func() { _ = client.Close() }()

			lookup := &proto.ExternalCredentialLookup{
				SubjectId:    "user:user-transient-" + strings.ReplaceAll(tc.name, " ", "-"),
				ConnectionId: "gmail:default",
				Instance:     "default",
			}
			_, err = client.UpsertCredential(context.Background(), &proto.UpsertExternalCredentialRequest{
				Credential: &proto.ExternalCredential{
					SubjectId:    lookup.GetSubjectId(),
					ConnectionId: lookup.GetConnectionId(),
					Instance:     lookup.GetInstance(),
					AccessToken:  "old-access-token",
					RefreshToken: "refresh-token",
					ExpiresAt:    timestamppb.New(tc.expiresAt),
				},
			})
			if err != nil {
				t.Fatalf("UpsertCredential(seed): %v", err)
			}

			resolved, err := client.ResolveCredential(context.Background(), &proto.ResolveExternalCredentialRequest{
				Provider:            "gmail",
				Connection:          "default",
				ConnectionId:        lookup.GetConnectionId(),
				Mode:                "user",
				CredentialSubjectId: lookup.GetSubjectId(),
				Instance:            lookup.GetInstance(),
				Auth: &proto.ExternalCredentialAuthConfig{
					Type:         "oauth2",
					TokenUrl:     tokenServer.URL,
					ClientId:     "client-id",
					ClientSecret: "client-secret",
				},
			})
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

			got, err := client.GetCredential(context.Background(), &proto.GetExternalCredentialRequest{Lookup: lookup})
			if err != nil {
				t.Fatalf("GetCredential(retained): %v", err)
			}
			if got.GetAccessToken() != "old-access-token" || got.GetRefreshErrorCount() != 1 {
				t.Fatalf("retained credential = access:%q errors:%d, want old token and one error", got.GetAccessToken(), got.GetRefreshErrorCount())
			}
		})
	}
}

func TestExternalCredentialProviderCredentialMaintenanceRefreshesDueTargets(t *testing.T) {
	startTestIndexedDBBackend(t)
	provider := New()
	lifecycle, providerConn := startTestProviderServerWithProvider(t, provider)
	defer func() { _ = providerConn.Close() }()

	var refreshCalls int
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		refreshCalls++
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

	configureProvider(t, lifecycle, credentialRefreshProviderConfig("maintenance-refresh-key", tokenServer.URL))

	client, err := gestalt.ExternalCredentials()
	if err != nil {
		t.Fatalf("ExternalCredentials: %v", err)
	}
	defer func() { _ = client.Close() }()

	now := time.Now()
	seedCredential(t, client, &proto.ExternalCredential{
		SubjectId:    "user:due",
		ConnectionId: "gmail:default",
		Instance:     "default",
		AccessToken:  "old-due-access-token",
		RefreshToken: "due-refresh-token",
		ExpiresAt:    timestamppb.New(now.Add(5 * time.Minute)),
	})
	seedCredential(t, client, &proto.ExternalCredential{
		SubjectId:    "user:future",
		ConnectionId: "gmail:default",
		Instance:     "default",
		AccessToken:  "future-access-token",
		RefreshToken: "future-refresh-token",
		ExpiresAt:    timestamppb.New(now.Add(2 * time.Hour)),
	})
	seedCredential(t, client, &proto.ExternalCredential{
		SubjectId:    "user:slack",
		ConnectionId: "slack:default",
		Instance:     "default",
		AccessToken:  "slack-access-token",
		RefreshToken: "slack-refresh-token",
		ExpiresAt:    timestamppb.New(now.Add(5 * time.Minute)),
	})
	db, err := gestalt.IndexedDB()
	if err != nil {
		t.Fatalf("IndexedDB: %v", err)
	}
	defer func() { _ = db.Close() }()
	if err := db.ObjectStore(storeName).Put(context.Background(), gestalt.Record{
		"id":                      "corrupt-non-target",
		"subject_id":              "user:corrupt",
		"connection_id":           "slack:default",
		"instance":                "default",
		"access_token_encrypted":  "not-ciphertext",
		"refresh_token_encrypted": "not-ciphertext",
		"created_at":              now,
		"updated_at":              now,
	}); err != nil {
		t.Fatalf("Put(corrupt non-target): %v", err)
	}

	stats := provider.runCredentialRefreshOnce(context.Background())
	if stats.Errors != 0 {
		t.Fatalf("maintenance stats = %+v, want no errors", stats)
	}
	if refreshCalls != 1 {
		t.Fatalf("refreshCalls = %d, want one due target refresh", refreshCalls)
	}

	gotDue := getCredential(t, client, "user:due", "gmail:default", "default")
	if gotDue.GetAccessToken() != "maintained-access-token" || gotDue.GetRefreshToken() != "maintained-refresh-token" || gotDue.GetRefreshErrorCount() != 0 {
		t.Fatalf("due credential = access:%q refresh:%q errors:%d", gotDue.GetAccessToken(), gotDue.GetRefreshToken(), gotDue.GetRefreshErrorCount())
	}
	gotFuture := getCredential(t, client, "user:future", "gmail:default", "default")
	if gotFuture.GetAccessToken() != "future-access-token" {
		t.Fatalf("future access token = %q, want unchanged", gotFuture.GetAccessToken())
	}
	gotSlack := getCredential(t, client, "user:slack", "slack:default", "default")
	if gotSlack.GetAccessToken() != "slack-access-token" {
		t.Fatalf("non-target access token = %q, want unchanged", gotSlack.GetAccessToken())
	}
}

func TestExternalCredentialProviderCredentialMaintenanceRejectsConflictingResolvedConnections(t *testing.T) {
	startTestIndexedDBBackend(t)
	lifecycle, providerConn := startTestProviderServer(t)
	defer func() { _ = providerConn.Close() }()

	cfg := credentialRefreshProviderConfig("maintenance-conflict-key", "https://token-a.example.test")
	connections := cfg["resolvedConnections"].([]any)
	duplicate := map[string]any{
		"provider":     "google_calendar",
		"connection":   "default",
		"connectionId": "gmail:default",
		"mode":         "user",
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
	pbConfig, err := structpb.NewStruct(cfg)
	if err != nil {
		t.Fatalf("structpb.NewStruct: %v", err)
	}
	_, err = lifecycle.ConfigureProvider(context.Background(), &proto.ConfigureProviderRequest{
		Name:            "default",
		Config:          pbConfig,
		ProtocolVersion: proto.CurrentProtocolVersion,
	}, grpc.WaitForReady(true))
	if err == nil {
		t.Fatal("ConfigureProvider error = nil, want conflicting connectionId rejection")
	}
	if !strings.Contains(err.Error(), "conflicting credential refresh config") {
		t.Fatalf("ConfigureProvider error = %v, want conflicting credential refresh config", err)
	}
}

func TestExternalCredentialProviderCredentialMaintenanceRejectsUnsupportedAuthConfig(t *testing.T) {
	startTestIndexedDBBackend(t)
	lifecycle, providerConn := startTestProviderServer(t)
	defer func() { _ = providerConn.Close() }()

	cfg := credentialRefreshProviderConfig("maintenance-invalid-auth-key", "https://token.example.test")
	connections := cfg["resolvedConnections"].([]any)
	target := connections[0].(map[string]any)
	auth := target["auth"].(map[string]any)
	auth["tokenExchange"] = "xml"
	pbConfig, err := structpb.NewStruct(cfg)
	if err != nil {
		t.Fatalf("structpb.NewStruct: %v", err)
	}
	_, err = lifecycle.ConfigureProvider(context.Background(), &proto.ConfigureProviderRequest{
		Name:            "default",
		Config:          pbConfig,
		ProtocolVersion: proto.CurrentProtocolVersion,
	}, grpc.WaitForReady(true))
	if err == nil {
		t.Fatal("ConfigureProvider error = nil, want unsupported auth rejection")
	}
	if !strings.Contains(err.Error(), "unknown tokenExchange") {
		t.Fatalf("ConfigureProvider error = %v, want unknown tokenExchange", err)
	}
}

func TestExternalCredentialProviderCredentialMaintenanceScansImmediately(t *testing.T) {
	startTestIndexedDBBackend(t)
	provider := New()
	if err := provider.Configure(context.Background(), "default", map[string]any{
		"encryptionKey": "maintenance-immediate-key",
	}); err != nil {
		t.Fatalf("Configure(seed): %v", err)
	}
	defer func() { _ = provider.Close() }()

	_, err := provider.UpsertCredential(context.Background(), &proto.UpsertExternalCredentialRequest{
		Credential: &proto.ExternalCredential{
			SubjectId:    "user:immediate",
			ConnectionId: "gmail:default",
			Instance:     "default",
			AccessToken:  "old-access-token",
			RefreshToken: "immediate-refresh-token",
			ExpiresAt:    timestamppb.New(time.Now().Add(5 * time.Minute)),
		},
	})
	if err != nil {
		t.Fatalf("UpsertCredential(seed): %v", err)
	}

	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"immediate-access-token","expires_in":3600}`))
	}))
	defer tokenServer.Close()

	if err := provider.Configure(context.Background(), "default", credentialRefreshProviderConfig("maintenance-immediate-key", tokenServer.URL)); err != nil {
		t.Fatalf("Configure(refresh): %v", err)
	}
	waitForCondition(t, 2*time.Second, func() (bool, error) {
		got, err := provider.GetCredential(context.Background(), &proto.GetExternalCredentialRequest{
			Lookup: &proto.ExternalCredentialLookup{
				SubjectId:    "user:immediate",
				ConnectionId: "gmail:default",
				Instance:     "default",
			},
		})
		if err != nil {
			return false, err
		}
		if got.GetAccessToken() != "immediate-access-token" {
			return false, fmt.Errorf("access token = %q, want immediate-access-token", got.GetAccessToken())
		}
		return true, nil
	})
}

func TestExternalCredentialProviderCredentialMaintenanceSharesResolveSingleflight(t *testing.T) {
	startTestIndexedDBBackend(t)
	provider := New()
	if err := provider.Configure(context.Background(), "default", map[string]any{
		"encryptionKey": "maintenance-singleflight-key",
	}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	defer func() { _ = provider.Close() }()

	_, err := provider.UpsertCredential(context.Background(), &proto.UpsertExternalCredentialRequest{
		Credential: &proto.ExternalCredential{
			SubjectId:    "user:singleflight",
			ConnectionId: "gmail:default",
			Instance:     "default",
			AccessToken:  "old-access-token",
			RefreshToken: "rotating-refresh-token",
			ExpiresAt:    timestamppb.New(time.Now().Add(5 * time.Minute)),
		},
	})
	if err != nil {
		t.Fatalf("UpsertCredential(seed): %v", err)
	}

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

	auth := &proto.ExternalCredentialAuthConfig{
		Type:         "oauth2",
		TokenUrl:     tokenServer.URL,
		ClientId:     "client-id",
		ClientSecret: "client-secret",
	}
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
		_, err := provider.ResolveCredential(context.Background(), &proto.ResolveExternalCredentialRequest{
			Provider:            "gmail",
			Connection:          "default",
			ConnectionId:        "gmail:default",
			Mode:                "user",
			CredentialSubjectId: "user:singleflight",
			Instance:            "default",
			Auth:                auth,
		})
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
	got, err := provider.GetCredential(context.Background(), &proto.GetExternalCredentialRequest{
		Lookup: &proto.ExternalCredentialLookup{
			SubjectId:    "user:singleflight",
			ConnectionId: "gmail:default",
			Instance:     "default",
		},
	})
	if err != nil {
		t.Fatalf("GetCredential: %v", err)
	}
	if got.GetAccessToken() != "singleflight-access-token" || got.GetRefreshToken() != "rotated-refresh-token" {
		t.Fatalf("credential = access:%q refresh:%q", got.GetAccessToken(), got.GetRefreshToken())
	}
}

func TestExternalCredentialProviderCredentialMaintenancePreservesTransientFailures(t *testing.T) {
	startTestIndexedDBBackend(t)
	provider := New()
	lifecycle, providerConn := startTestProviderServerWithProvider(t, provider)
	defer func() { _ = providerConn.Close() }()

	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"temporarily_unavailable","error_description":"transient secret should not leak"}`))
	}))
	defer tokenServer.Close()

	configureProvider(t, lifecycle, credentialRefreshProviderConfig("maintenance-transient-key", tokenServer.URL))
	client, err := gestalt.ExternalCredentials()
	if err != nil {
		t.Fatalf("ExternalCredentials: %v", err)
	}
	defer func() { _ = client.Close() }()
	seedCredential(t, client, &proto.ExternalCredential{
		SubjectId:    "user:transient",
		ConnectionId: "gmail:default",
		Instance:     "default",
		AccessToken:  "old-access-token",
		RefreshToken: "refresh-token",
		ExpiresAt:    timestamppb.New(time.Now().Add(5 * time.Minute)),
	})

	_ = provider.runCredentialRefreshOnce(context.Background())

	got := getCredential(t, client, "user:transient", "gmail:default", "default")
	if got.GetAccessToken() != "old-access-token" || got.GetRefreshToken() != "refresh-token" || got.GetRefreshErrorCount() != 1 {
		t.Fatalf("credential after transient failure = access:%q refresh:%q errors:%d", got.GetAccessToken(), got.GetRefreshToken(), got.GetRefreshErrorCount())
	}
}

func TestExternalCredentialProviderCredentialMaintenanceDeletesInvalidGrant(t *testing.T) {
	startTestIndexedDBBackend(t)
	provider := New()
	lifecycle, providerConn := startTestProviderServerWithProvider(t, provider)
	defer func() { _ = providerConn.Close() }()

	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_grant","error_description":"revoked secret should not leak"}`))
	}))
	defer tokenServer.Close()

	configureProvider(t, lifecycle, credentialRefreshProviderConfig("maintenance-invalid-grant-key", tokenServer.URL))
	client, err := gestalt.ExternalCredentials()
	if err != nil {
		t.Fatalf("ExternalCredentials: %v", err)
	}
	defer func() { _ = client.Close() }()
	seedCredential(t, client, &proto.ExternalCredential{
		SubjectId:    "user:invalid-grant",
		ConnectionId: "gmail:default",
		Instance:     "default",
		AccessToken:  "old-access-token",
		RefreshToken: "revoked-refresh-token",
		ExpiresAt:    timestamppb.New(time.Now().Add(5 * time.Minute)),
	})

	_ = provider.runCredentialRefreshOnce(context.Background())

	_, err = client.GetCredential(context.Background(), &proto.GetExternalCredentialRequest{
		Lookup: &proto.ExternalCredentialLookup{
			SubjectId:    "user:invalid-grant",
			ConnectionId: "gmail:default",
			Instance:     "default",
		},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetCredential after invalid_grant code = %v, want %v", status.Code(err), codes.NotFound)
	}
}

func TestExternalCredentialProviderCredentialMaintenanceLifecycleCancelsLoops(t *testing.T) {
	startTestIndexedDBBackend(t)
	provider := New()
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"access-token","expires_in":3600}`))
	}))
	defer tokenServer.Close()

	if err := provider.Configure(context.Background(), "default", credentialRefreshProviderConfig("maintenance-lifecycle-key", tokenServer.URL)); err != nil {
		t.Fatalf("Configure(first): %v", err)
	}
	provider.mu.RLock()
	firstDone := provider.refreshDone
	provider.mu.RUnlock()
	if firstDone == nil {
		t.Fatal("first refresh loop was not started")
	}

	if err := provider.Configure(context.Background(), "default", credentialRefreshProviderConfig("maintenance-lifecycle-key-2", tokenServer.URL)); err != nil {
		t.Fatalf("Configure(second): %v", err)
	}
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
			startTestIndexedDBBackend(t)
			lifecycle, providerConn := startTestProviderServer(t)
			defer func() { _ = providerConn.Close() }()

			configureProvider(t, lifecycle, map[string]any{
				"encryptionKey": "provider-token-error-sanitize-key-" + strings.ReplaceAll(tc.name, " ", "-"),
			})

			tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer tokenServer.Close()

			client, err := gestalt.ExternalCredentials()
			if err != nil {
				t.Fatalf("ExternalCredentials: %v", err)
			}
			defer func() { _ = client.Close() }()

			_, err = client.ExchangeCredential(context.Background(), &proto.ExchangeExternalCredentialRequest{
				Provider:       "manual",
				Connection:     "default",
				ConnectionId:   "manual:default",
				CredentialJson: `{"api_key":"manual-secret"}`,
				Auth: &proto.ExternalCredentialAuthConfig{
					Type:          "manual",
					TokenUrl:      tokenServer.URL,
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

func TestExternalCredentialProviderGoogleServiceAccountImpersonation(t *testing.T) {
	startTestIndexedDBBackend(t)
	lifecycle, providerConn := startTestProviderServer(t)
	defer func() { _ = providerConn.Close() }()

	configureProvider(t, lifecycle, map[string]any{
		"encryptionKey": "provider-google-impersonation-key",
	})

	expireTime := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	iamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer source-adc-token" {
			t.Errorf("authorization = %q, want source token bearer", got)
		}
		var payload struct {
			Scope    []string `json:"scope"`
			Lifetime string   `json:"lifetime"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("Decode(payload): %v", err)
		}
		if len(payload.Scope) != 1 || payload.Scope[0] != "https://www.googleapis.com/auth/cloud-platform" {
			t.Errorf("scope = %#v, want cloud-platform scope", payload.Scope)
		}
		if payload.Lifetime != "900s" {
			t.Errorf("lifetime = %q, want 900s", payload.Lifetime)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"accessToken":"impersonated-access-token","expireTime":%q}`, expireTime)
	}))
	defer iamServer.Close()

	client, err := gestalt.ExternalCredentials()
	if err != nil {
		t.Fatalf("ExternalCredentials: %v", err)
	}
	defer func() { _ = client.Close() }()

	resolved, err := client.ResolveCredential(context.Background(), &proto.ResolveExternalCredentialRequest{
		Provider:     "vertex",
		Connection:   "kimi",
		ConnectionId: "vertex:kimi",
		Mode:         "platform",
		Auth: &proto.ExternalCredentialAuthConfig{
			TokenExchangeDrivers: []*proto.ExternalCredentialTokenExchangeDriver{
				{
					Type:            "google_service_account_impersonation",
					TargetPrincipal: "gestalt-agent@example.iam.gserviceaccount.com",
					Endpoint:        iamServer.URL,
					Scopes:          []string{"https://www.googleapis.com/auth/cloud-platform"},
					LifetimeSeconds: 900,
					Params: map[string]string{
						"sourceAccessToken": "source-adc-token",
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("ResolveCredential(google impersonation): %v", err)
	}
	if resolved.GetToken() != "impersonated-access-token" {
		t.Fatalf("token = %q, want impersonated-access-token", resolved.GetToken())
	}
	if resolved.GetExpiresAt() == nil {
		t.Fatal("expires_at is nil, want IAM expireTime mapped")
	}
}

func TestExternalCredentialProviderRestorePreservesTimestamps(t *testing.T) {
	startTestIndexedDBBackend(t)
	lifecycle, providerConn := startTestProviderServer(t)
	defer func() { _ = providerConn.Close() }()

	configureProvider(t, lifecycle, map[string]any{
		"encryptionKey": "provider-restore-key",
	})

	client, err := gestalt.ExternalCredentials()
	if err != nil {
		t.Fatalf("ExternalCredentials: %v", err)
	}
	defer func() { _ = client.Close() }()

	createdAt := time.Unix(1_700_000_000, 0).UTC()
	updatedAt := time.Unix(1_700_000_001, 0).UTC()

	stored, err := client.UpsertCredential(context.Background(), &proto.UpsertExternalCredentialRequest{
		PreserveTimestamps: true,
		Credential: &proto.ExternalCredential{
			Id:           "cred-restore-1",
			SubjectId:    "user:user-restore",
			ConnectionId: "github:default",
			Instance:     "org-1",
			AccessToken:  "gho_123",
			CreatedAt:    timestamppb.New(createdAt),
			UpdatedAt:    timestamppb.New(updatedAt),
		},
	})
	if err != nil {
		t.Fatalf("UpsertCredential(restore): %v", err)
	}
	if got := stored.GetCreatedAt().AsTime(); !got.Equal(createdAt) {
		t.Fatalf("created_at = %v, want %v", got, createdAt)
	}
	if got := stored.GetUpdatedAt().AsTime(); !got.Equal(updatedAt) {
		t.Fatalf("updated_at = %v, want %v", got, updatedAt)
	}
}

func TestExternalCredentialProviderReadsExistingCiphertextFormat(t *testing.T) {
	startTestIndexedDBBackend(t)
	lifecycle, providerConn := startTestProviderServer(t)
	defer func() { _ = providerConn.Close() }()

	const encryptionKey = "provider-ciphertext-key"
	configureProvider(t, lifecycle, map[string]any{
		"encryptionKey": encryptionKey,
	})

	db, err := gestalt.IndexedDB()
	if err != nil {
		t.Fatalf("IndexedDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	key := deriveKey(encryptionKey)
	accessEnc := mustEncryptWithNonce(t, key, []byte("0123456789ab"), "seeded-access-token")
	refreshEnc := mustEncryptWithNonce(t, key, []byte("abcdefghijkl"), "seeded-refresh-token")
	createdAt := time.Date(2026, time.April, 24, 12, 0, 0, 0, time.UTC)
	updatedAt := createdAt.Add(time.Minute)

	if err := db.ObjectStore(storeName).Put(context.Background(), gestalt.Record{
		"id":                      "cred-seeded-1",
		"subject_id":              "user:user-seeded",
		"connection_id":           "slack:default",
		"instance":                "workspace-1",
		"access_token_encrypted":  accessEnc,
		"refresh_token_encrypted": refreshEnc,
		"scopes":                  "channels:read",
		"refresh_error_count":     int64(1),
		"metadata_json":           `{"seeded":true}`,
		"created_at":              createdAt,
		"updated_at":              updatedAt,
	}); err != nil {
		t.Fatalf("Put(seed raw record): %v", err)
	}

	client, err := gestalt.ExternalCredentials()
	if err != nil {
		t.Fatalf("ExternalCredentials: %v", err)
	}
	defer func() { _ = client.Close() }()

	got, err := client.GetCredential(context.Background(), &proto.GetExternalCredentialRequest{
		Lookup: &proto.ExternalCredentialLookup{
			SubjectId:    "user:user-seeded",
			ConnectionId: "slack:default",
			Instance:     "workspace-1",
		},
	})
	if err != nil {
		t.Fatalf("GetCredential(seed): %v", err)
	}
	if got.GetAccessToken() != "seeded-access-token" {
		t.Fatalf("access token = %q, want %q", got.GetAccessToken(), "seeded-access-token")
	}
	if got.GetRefreshToken() != "seeded-refresh-token" {
		t.Fatalf("refresh token = %q, want %q", got.GetRefreshToken(), "seeded-refresh-token")
	}
}

func TestExternalCredentialProviderValidation(t *testing.T) {
	startTestIndexedDBBackend(t)
	lifecycle, providerConn := startTestProviderServer(t)
	defer func() { _ = providerConn.Close() }()

	configureProvider(t, lifecycle, map[string]any{
		"encryptionKey": "provider-validation-key",
	})

	client, err := gestalt.ExternalCredentials()
	if err != nil {
		t.Fatalf("ExternalCredentials: %v", err)
	}
	defer func() { _ = client.Close() }()

	if _, err := client.ListCredentials(context.Background(), &proto.ListExternalCredentialsRequest{}); err == nil {
		t.Fatal("ListCredentials without subject_id succeeded, want error")
	} else if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ListCredentials code = %v, want %v", status.Code(err), codes.InvalidArgument)
	}
}

func TestExternalCredentialProviderUsesNamedIndexedDBBinding(t *testing.T) {
	startTestIndexedDBBackend(t)
	startTestIndexedDBBackendAtEnv(t, gestalt.IndexedDBSocketEnv("archive"), "external_credentials_archive.sqlite")
	lifecycle, providerConn := startTestProviderServer(t)
	defer func() { _ = providerConn.Close() }()

	configureProvider(t, lifecycle, map[string]any{
		"encryptionKey": "provider-named-indexeddb-key",
		"indexeddb":     "archive",
	})

	client, err := gestalt.ExternalCredentials()
	if err != nil {
		t.Fatalf("ExternalCredentials: %v", err)
	}
	defer func() { _ = client.Close() }()

	created, err := client.UpsertCredential(context.Background(), &proto.UpsertExternalCredentialRequest{
		Credential: &proto.ExternalCredential{
			SubjectId:    "user:user-named-indexeddb",
			ConnectionId: "github:default",
			Instance:     "org-1",
			AccessToken:  "gho_named",
		},
	})
	if err != nil {
		t.Fatalf("UpsertCredential: %v", err)
	}

	defaultDB, err := gestalt.IndexedDB()
	if err != nil {
		t.Fatalf("IndexedDB(default): %v", err)
	}
	defer func() { _ = defaultDB.Close() }()

	namedDB, err := gestalt.IndexedDB("archive")
	if err != nil {
		t.Fatalf("IndexedDB(archive): %v", err)
	}
	defer func() { _ = namedDB.Close() }()

	if _, err := defaultDB.ObjectStore(storeName).Get(context.Background(), created.GetId()); !errors.Is(err, gestalt.ErrNotFound) {
		t.Fatalf("default indexeddb Get error = %v, want %v", err, gestalt.ErrNotFound)
	}
	raw, err := namedDB.ObjectStore(storeName).Get(context.Background(), created.GetId())
	if err != nil {
		t.Fatalf("named indexeddb Get: %v", err)
	}
	if got, _ := raw["access_token_encrypted"].(string); got == "" {
		t.Fatalf("named indexeddb access_token_encrypted = %q, want ciphertext", got)
	}
}

func startTestIndexedDBBackend(t *testing.T) {
	t.Helper()
	startTestIndexedDBBackendAtEnv(t, gestalt.EnvIndexedDBSocket, "external_credentials.sqlite")
}

func startTestIndexedDBBackendAtEnv(t *testing.T, envName, sqliteName string) {
	t.Helper()

	socketPath := newSocketPath(t, "indexeddb.sock")
	store := relationaldb.New()
	if err := store.Configure(context.Background(), "external_credentials_state", map[string]any{
		"dsn": "file:" + filepath.Join(t.TempDir(), sqliteName) + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)",
	}); err != nil {
		t.Fatalf("relationaldb.Configure: %v", err)
	}
	seedExternalCredentialStore(t, store)

	t.Setenv(proto.EnvProviderSocket, socketPath)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- gestalt.ServeIndexedDBProvider(ctx, store)
	}()
	conn := newUnixConn(t, socketPath)
	_ = conn.Close()
	t.Cleanup(func() {
		cancel()
		waitServeResult(t, errCh)
		_ = os.Remove(socketPath)
	})

	t.Setenv(envName, socketPath)
}

func seedExternalCredentialStore(t *testing.T, store *relationaldb.Provider) {
	t.Helper()
	if err := store.CreateObjectStore(context.Background(), storeName, externalCredentialTestSchema()); err != nil && !errors.Is(err, gestalt.ErrAlreadyExists) {
		t.Fatalf("CreateObjectStore(%s): %v", storeName, err)
	}
}

func externalCredentialTestSchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Indexes: []gestalt.IndexSchema{
			{Name: indexBySubject, KeyPath: []string{"subject_id"}},
			{Name: indexBySubjectConnection, KeyPath: []string{"subject_id", "connection_id"}},
			{Name: indexByLookup, KeyPath: []string{"subject_id", "connection_id", "instance"}, Unique: true},
		},
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true},
			{Name: "subject_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "connection_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "instance", Type: gestalt.TypeString},
			{Name: "access_token_encrypted", Type: gestalt.TypeString},
			{Name: "refresh_token_encrypted", Type: gestalt.TypeString},
			{Name: "scopes", Type: gestalt.TypeString},
			{Name: "expires_at", Type: gestalt.TypeTime},
			{Name: "last_refreshed_at", Type: gestalt.TypeTime},
			{Name: "refresh_error_count", Type: gestalt.TypeInt},
			{Name: "metadata_json", Type: gestalt.TypeString},
			{Name: "created_at", Type: gestalt.TypeTime},
			{Name: "updated_at", Type: gestalt.TypeTime},
		},
	}
}

func startTestProviderServer(t *testing.T) (proto.ProviderLifecycleClient, *grpc.ClientConn) {
	return startTestProviderServerWithProvider(t, New())
}

func startTestProviderServerWithProvider(t *testing.T, provider *Provider) (proto.ProviderLifecycleClient, *grpc.ClientConn) {
	t.Helper()

	socketPath := newSocketPath(t, "external-credentials.sock")
	t.Setenv(proto.EnvProviderSocket, socketPath)
	t.Setenv(gestalt.EnvExternalCredentialSocket, socketPath)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- gestalt.ServeExternalCredentialProvider(ctx, provider)
	}()
	t.Cleanup(func() {
		cancel()
		waitServeResult(t, errCh)
	})

	conn := newUnixConn(t, socketPath)
	return proto.NewProviderLifecycleClient(conn), conn
}

func credentialRefreshProviderConfig(encryptionKey, tokenURL string) map[string]any {
	return map[string]any{
		"encryptionKey": encryptionKey,
		"resolvedConnections": []any{
			map[string]any{
				"provider":     "gmail",
				"connection":   "default",
				"connectionId": "gmail:default",
				"mode":         "user",
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

func seedCredential(t *testing.T, client *gestalt.ExternalCredentialClient, credential *proto.ExternalCredential) {
	t.Helper()
	if _, err := client.UpsertCredential(context.Background(), &proto.UpsertExternalCredentialRequest{Credential: credential}); err != nil {
		t.Fatalf("UpsertCredential(seed %s/%s): %v", credential.GetSubjectId(), credential.GetConnectionId(), err)
	}
}

func getCredential(t *testing.T, client *gestalt.ExternalCredentialClient, subjectID, connectionID, instance string) *proto.ExternalCredential {
	t.Helper()
	got, err := client.GetCredential(context.Background(), &proto.GetExternalCredentialRequest{
		Lookup: &proto.ExternalCredentialLookup{
			SubjectId:    subjectID,
			ConnectionId: connectionID,
			Instance:     instance,
		},
	})
	if err != nil {
		t.Fatalf("GetCredential(%s/%s/%s): %v", subjectID, connectionID, instance, err)
	}
	return got
}

func configureProvider(t *testing.T, lifecycle proto.ProviderLifecycleClient, cfg map[string]any) {
	t.Helper()

	pbConfig, err := structpb.NewStruct(cfg)
	if err != nil {
		t.Fatalf("structpb.NewStruct: %v", err)
	}
	resp, err := lifecycle.ConfigureProvider(context.Background(), &proto.ConfigureProviderRequest{
		Name:            "default",
		Config:          pbConfig,
		ProtocolVersion: proto.CurrentProtocolVersion,
	}, grpc.WaitForReady(true))
	if err != nil {
		t.Fatalf("ConfigureProvider: %v", err)
	}
	if resp.GetProtocolVersion() != proto.CurrentProtocolVersion {
		t.Fatalf("protocol_version = %d, want %d", resp.GetProtocolVersion(), proto.CurrentProtocolVersion)
	}
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

func waitForCondition(t *testing.T, timeout time.Duration, fn func() (bool, error)) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		ok, err := fn()
		if ok {
			return
		}
		if err != nil {
			lastErr = err
		}
		if time.Now().After(deadline) {
			if lastErr != nil {
				t.Fatalf("condition was not met within %s: %v", timeout, lastErr)
			}
			t.Fatalf("condition was not met within %s", timeout)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func newSocketPath(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join("/tmp", fmt.Sprintf("gestalt-%d-%d-%s", os.Getpid(), time.Now().UnixNano(), name))
}

func newUnixConn(t *testing.T, socket string) *grpc.ClientConn {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(socket); err == nil {
			conn, dialErr := grpc.NewClient(
				"passthrough:///"+socket,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, "unix", addr)
				}),
			)
			if dialErr != nil {
				t.Fatalf("grpc.NewClient: %v", dialErr)
			}
			conn.Connect()
			t.Cleanup(func() { _ = conn.Close() })
			return conn
		}
		if time.Now().After(deadline) {
			t.Fatalf("socket %q was not created", socket)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func waitServeResult(t *testing.T, errCh <-chan error) {
	t.Helper()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("serve returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serve did not stop after context cancellation")
	}
}
