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

func startTestProviderServer(t *testing.T) (proto.ProviderLifecycleClient, *grpc.ClientConn) {
	t.Helper()

	socketPath := newSocketPath(t, "external-credentials.sock")
	t.Setenv(proto.EnvProviderSocket, socketPath)
	t.Setenv(gestalt.EnvExternalCredentialSocket, socketPath)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- gestalt.ServeExternalCredentialProvider(ctx, New())
	}()
	t.Cleanup(func() {
		cancel()
		waitServeResult(t, errCh)
	})

	conn := newUnixConn(t, socketPath)
	return proto.NewProviderLifecycleClient(conn), conn
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
