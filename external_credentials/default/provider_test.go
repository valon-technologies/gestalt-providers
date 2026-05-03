package externalcredentials

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
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

func TestExternalCredentialProviderBackfillsLegacyConnectionRecords(t *testing.T) {
	startTestIndexedDBBackend(t)
	lifecycle, providerConn := startTestProviderServer(t)
	defer func() { _ = providerConn.Close() }()

	const encryptionKey = "provider-legacy-connection-key"
	db, err := gestalt.IndexedDB()
	if err != nil {
		t.Fatalf("IndexedDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx := context.Background()
	if err := db.CreateObjectStore(ctx, legacyStoreName, legacyExternalCredentialSchema()); err != nil {
		t.Fatalf("CreateObjectStore(%s): %v", legacyStoreName, err)
	}

	key := deriveKey(encryptionKey)
	createdAt := time.Date(2026, time.May, 3, 4, 0, 0, 0, time.UTC)
	updatedAt := createdAt.Add(time.Minute)
	bigqueryAccess := mustEncryptWithNonce(t, key, []byte("legacybqacc1"), "bigquery-access-token")
	bigqueryRefresh := mustEncryptWithNonce(t, key, []byte("legacybqref1"), "bigquery-refresh-token")
	slackAccess := mustEncryptWithNonce(t, key, []byte("legacyslack1"), "slack-access-token")
	if err := db.ObjectStore(legacyStoreName).Put(ctx, gestalt.Record{
		"id":                      "legacy-bigquery",
		"subject_id":              "user:user-legacy",
		"integration":             "bigquery",
		"connection":              "default",
		"instance":                "default",
		"access_token_encrypted":  bigqueryAccess,
		"refresh_token_encrypted": bigqueryRefresh,
		"scopes":                  "https://www.googleapis.com/auth/bigquery",
		"metadata_json":           `{"source":"legacy"}`,
		"created_at":              createdAt,
		"updated_at":              updatedAt,
	}); err != nil {
		t.Fatalf("Put(legacy bigquery): %v", err)
	}
	if err := db.ObjectStore(legacyStoreName).Put(ctx, gestalt.Record{
		"id":                     "legacy-slack",
		"subject_id":             "user:user-legacy",
		"integration":            "slack",
		"connection":             "default",
		"instance":               "workspace-1",
		"access_token_encrypted": slackAccess,
		"created_at":             createdAt,
		"updated_at":             updatedAt,
	}); err != nil {
		t.Fatalf("Put(legacy slack): %v", err)
	}

	configureProvider(t, lifecycle, map[string]any{
		"encryptionKey": encryptionKey,
	})
	configureProvider(t, lifecycle, map[string]any{
		"encryptionKey": encryptionKey,
	})

	client, err := gestalt.ExternalCredentials()
	if err != nil {
		t.Fatalf("ExternalCredentials: %v", err)
	}
	defer func() { _ = client.Close() }()

	listed, err := client.ListCredentials(ctx, &proto.ListExternalCredentialsRequest{
		SubjectId:    "user:user-legacy",
		ConnectionId: "bigquery:default",
	})
	if err != nil {
		t.Fatalf("ListCredentials(bigquery): %v", err)
	}
	if len(listed.GetCredentials()) != 1 {
		t.Fatalf("bigquery credentials len = %d, want 1", len(listed.GetCredentials()))
	}
	got := listed.GetCredentials()[0]
	if got.GetId() != "legacy-bigquery" {
		t.Fatalf("credential id = %q, want legacy-bigquery", got.GetId())
	}
	if got.GetAccessToken() != "bigquery-access-token" || got.GetRefreshToken() != "bigquery-refresh-token" {
		t.Fatalf("tokens = access:%q refresh:%q", got.GetAccessToken(), got.GetRefreshToken())
	}
	if got.GetConnectionId() != "bigquery:default" || got.GetInstance() != "default" {
		t.Fatalf("lookup = connection:%q instance:%q", got.GetConnectionId(), got.GetInstance())
	}

	all, err := client.ListCredentials(ctx, &proto.ListExternalCredentialsRequest{
		SubjectId: "user:user-legacy",
	})
	if err != nil {
		t.Fatalf("ListCredentials(subject): %v", err)
	}
	if len(all.GetCredentials()) != 2 {
		t.Fatalf("subject credentials len = %d, want 2", len(all.GetCredentials()))
	}
	if _, err := db.ObjectStore(legacyStoreName).Get(ctx, "legacy-bigquery"); !errors.Is(err, gestalt.ErrNotFound) {
		t.Fatalf("legacy bigquery Get error = %v, want %v", err, gestalt.ErrNotFound)
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

func legacyExternalCredentialSchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Indexes: []gestalt.IndexSchema{
			{Name: "by_subject", KeyPath: []string{"subject_id"}},
			{Name: "by_subject_integration", KeyPath: []string{"subject_id", "integration"}},
			{Name: "by_subject_connection", KeyPath: []string{"subject_id", "integration", "connection"}},
			{Name: "by_lookup", KeyPath: []string{"subject_id", "integration", "connection", "instance"}, Unique: true},
		},
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true},
			{Name: "subject_id", Type: gestalt.TypeString, NotNull: true},
			{Name: "integration", Type: gestalt.TypeString, NotNull: true},
			{Name: "connection", Type: gestalt.TypeString, NotNull: true},
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
