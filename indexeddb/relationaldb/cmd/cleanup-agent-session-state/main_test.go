package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	relationaldb "github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func TestDryRunReportsLegacyAgentSessionStateWithoutDeleting(t *testing.T) {
	ctx := context.Background()
	dsn := seedCleanupDB(t)
	opts := testOptions(dsn)

	var out bytes.Buffer
	if err := run(ctx, &out, opts); err != nil {
		t.Fatalf("run dry-run: %v", err)
	}
	if !strings.Contains(out.String(), "mode: dry-run") {
		t.Fatalf("dry-run output missing mode: %s", out.String())
	}
	if !strings.Contains(out.String(), "agent_session_relationships: 2") {
		t.Fatalf("dry-run output missing relationship count: %s", out.String())
	}

	provider := openTestProvider(t, dsn)
	assertRelationshipCount(t, provider, agentSessionResourceType, 2)
	assertRelationshipCount(t, provider, "plugin", 1)
	assertStoreCount(t, provider, agentSessionRouteStore, true, 1)
	assertStoreCount(t, provider, agentTurnRouteStore, true, 1)
}

func TestExecuteDeletesOnlyLegacyAgentSessionState(t *testing.T) {
	ctx := context.Background()
	dsn := seedCleanupDB(t)
	opts := testOptions(dsn)
	opts.Execute = true
	opts.MaxDelete = 4
	opts.BackupConfirmation = "backup-operation-1"
	opts.ProviderVisibilityDeployedRef = "gestalt-providers@abc123"
	opts.OldGestaltdDrained = true

	var out bytes.Buffer
	if err := run(ctx, &out, opts); err != nil {
		t.Fatalf("run execute: %v", err)
	}
	if !strings.Contains(out.String(), "mode: execute") {
		t.Fatalf("execute output missing mode: %s", out.String())
	}
	if !strings.Contains(out.String(), "deleted_agent_session_relationships: 2") {
		t.Fatalf("execute output missing delete count: %s", out.String())
	}

	provider := openTestProvider(t, dsn)
	assertRelationshipCount(t, provider, agentSessionResourceType, 0)
	assertRelationshipCount(t, provider, "plugin", 1)
	assertStoreCount(t, provider, agentSessionRouteStore, false, 0)
	assertStoreCount(t, provider, agentTurnRouteStore, false, 0)
}

func TestExecuteRequiresMaxDeleteCoveringAllRows(t *testing.T) {
	ctx := context.Background()
	dsn := seedCleanupDB(t)
	opts := testOptions(dsn)
	opts.Execute = true
	opts.MaxDelete = 3
	opts.BackupConfirmation = "backup-operation-1"
	opts.ProviderVisibilityDeployedRef = "gestalt-providers@abc123"
	opts.OldGestaltdDrained = true

	var out bytes.Buffer
	err := run(ctx, &out, opts)
	if err == nil {
		t.Fatal("expected max-delete guard to fail")
	}
	if !strings.Contains(err.Error(), "refusing to delete 4 records") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExecuteRequiresExactMaxDelete(t *testing.T) {
	ctx := context.Background()
	dsn := seedCleanupDB(t)
	opts := testOptions(dsn)
	opts.Execute = true
	opts.MaxDelete = 5
	opts.BackupConfirmation = "backup-operation-1"
	opts.ProviderVisibilityDeployedRef = "gestalt-providers@abc123"
	opts.OldGestaltdDrained = true

	var out bytes.Buffer
	err := run(ctx, &out, opts)
	if err == nil {
		t.Fatal("expected oversized max-delete guard to fail")
	}
	if !strings.Contains(err.Error(), "rerun dry-run and pass the exact count") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExecuteRequiresOperationalConfirmations(t *testing.T) {
	tests := []struct {
		name string
		edit func(*options)
		want string
	}{
		{
			name: "backup",
			edit: func(opts *options) { opts.BackupConfirmation = "" },
			want: "--backup-confirmation is required",
		},
		{
			name: "provider_visibility",
			edit: func(opts *options) { opts.ProviderVisibilityDeployedRef = "" },
			want: "--provider-visibility-deployed-ref is required",
		},
		{
			name: "old_gestaltd_drained",
			edit: func(opts *options) { opts.OldGestaltdDrained = false },
			want: "--old-gestaltd-drained is required",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dsn := seedCleanupDB(t)
			opts := testOptions(dsn)
			opts.Execute = true
			opts.MaxDelete = 4
			opts.BackupConfirmation = "backup-operation-1"
			opts.ProviderVisibilityDeployedRef = "gestalt-providers@abc123"
			opts.OldGestaltdDrained = true
			tc.edit(&opts)

			var out bytes.Buffer
			err := run(ctx, &out, opts)
			if err == nil {
				t.Fatal("expected execute confirmation guard to fail")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestExecuteCleansMetadataOnlyRouteStoresWithZeroMaxDelete(t *testing.T) {
	ctx := context.Background()
	dsn := seedMetadataOnlyRouteDB(t)
	opts := testOptions(dsn)
	opts.Execute = true
	opts.MaxDelete = 0
	opts.BackupConfirmation = "backup-operation-1"
	opts.ProviderVisibilityDeployedRef = "gestalt-providers@abc123"
	opts.OldGestaltdDrained = true

	var out bytes.Buffer
	if err := run(ctx, &out, opts); err != nil {
		t.Fatalf("run execute: %v", err)
	}

	provider := openTestProvider(t, dsn)
	assertRelationshipCount(t, provider, agentSessionResourceType, 0)
	assertStoreCount(t, provider, agentSessionRouteStore, false, 0)
	assertStoreCount(t, provider, agentTurnRouteStore, false, 0)
}

func TestDeleteEmptyObjectStoreMetadataRefusesStoresWithRows(t *testing.T) {
	dsn := seedCleanupDB(t)
	opts := testOptions(dsn)
	err := deleteEmptyObjectStoreMetadata(context.Background(), dsn, opts, agentSessionRouteStore)
	if err == nil {
		t.Fatal("expected non-empty route store metadata delete to fail")
	}
	if !strings.Contains(err.Error(), "raw rows remain") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPreflightRequiresRelationshipsByResourceIndex(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + t.TempDir() + "/cleanup-missing-index.sqlite"
	provider := openTestProvider(t, dsn)
	if err := provider.CreateObjectStore(ctx, relationshipsStore, gestalt.ObjectStoreSchema{}); err != nil {
		t.Fatalf("CreateObjectStore relationships: %v", err)
	}

	opts := testOptions(dsn)
	var out bytes.Buffer
	err := run(ctx, &out, opts)
	if err == nil {
		t.Fatal("expected missing by_resource index to fail")
	}
	if !strings.Contains(err.Error(), "missing by_resource index") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMySQLSchemaQualification(t *testing.T) {
	if got := qualifyTable(relationaldb.DialectMySQL, "gestaltd", "_gestalt_stores"); got != "`gestaltd`.`_gestalt_stores`" {
		t.Fatalf("qualifyTable MySQL = %q", got)
	}
	if got := rebind(relationaldb.BindQuestion, "SELECT * FROM t WHERE a = ?"); got != "SELECT * FROM t WHERE a = ?" {
		t.Fatalf("rebind MySQL = %q", got)
	}
}

func seedCleanupDB(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	dsn := "file:" + t.TempDir() + "/cleanup.sqlite"
	provider := openTestProvider(t, dsn)

	if err := provider.CreateObjectStore(ctx, relationshipsStore, gestalt.ObjectStoreSchema{
		Indexes: []gestalt.IndexSchema{
			{Name: "by_subject", KeyPath: []string{"subject_type", "subject_id"}},
			{Name: relationshipsIndex, KeyPath: []string{"resource_type", "resource_id"}},
			{Name: "by_subject_resource", KeyPath: []string{"subject_type", "subject_id", "resource_type", "resource_id"}},
		},
	}); err != nil {
		t.Fatalf("CreateObjectStore relationships: %v", err)
	}
	putRecord(t, provider, relationshipsStore, gestalt.Record{
		"id": "rel-1", "subject_type": "user", "subject_id": "u1", "resource_type": agentSessionResourceType, "resource_id": "s1",
	})
	putRecord(t, provider, relationshipsStore, gestalt.Record{
		"id": "rel-2", "subject_type": "user", "subject_id": "u2", "resource_type": agentSessionResourceType, "resource_id": "s2",
	})
	putRecord(t, provider, relationshipsStore, gestalt.Record{
		"id": "rel-3", "subject_type": "user", "subject_id": "u3", "resource_type": "plugin", "resource_id": "p1",
	})

	if err := provider.CreateObjectStore(ctx, agentSessionRouteStore, gestalt.ObjectStoreSchema{}); err != nil {
		t.Fatalf("CreateObjectStore session routes: %v", err)
	}
	putRecord(t, provider, agentSessionRouteStore, gestalt.Record{"id": "s1", "provider": "gke"})

	if err := provider.CreateObjectStore(ctx, agentTurnRouteStore, gestalt.ObjectStoreSchema{}); err != nil {
		t.Fatalf("CreateObjectStore turn routes: %v", err)
	}
	putRecord(t, provider, agentTurnRouteStore, gestalt.Record{"id": "t1", "session_id": "s1", "provider": "gke"})
	return dsn
}

func seedMetadataOnlyRouteDB(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	dsn := "file:" + t.TempDir() + "/cleanup-metadata-only.sqlite"
	provider := openTestProvider(t, dsn)

	if err := provider.CreateObjectStore(ctx, relationshipsStore, gestalt.ObjectStoreSchema{
		Indexes: []gestalt.IndexSchema{
			{Name: relationshipsIndex, KeyPath: []string{"resource_type", "resource_id"}},
		},
	}); err != nil {
		t.Fatalf("CreateObjectStore relationships: %v", err)
	}
	if err := provider.CreateObjectStore(ctx, agentSessionRouteStore, gestalt.ObjectStoreSchema{}); err != nil {
		t.Fatalf("CreateObjectStore session routes: %v", err)
	}
	if err := provider.CreateObjectStore(ctx, agentTurnRouteStore, gestalt.ObjectStoreSchema{}); err != nil {
		t.Fatalf("CreateObjectStore turn routes: %v", err)
	}
	return dsn
}

func testOptions(dsn string) options {
	return options{
		DSN:              dsn,
		Project:          "test",
		Instance:         "test",
		Secret:           "test",
		Schema:           "",
		ExpectedProject:  "test",
		ExpectedInstance: "test",
		ExpectedSecret:   "test",
		ExpectedSchema:   "",
		Timeout:          time.Minute,
	}
}

func openTestProvider(t *testing.T, dsn string) *relationaldb.Provider {
	t.Helper()
	provider := relationaldb.New()
	if err := provider.Configure(context.Background(), "test", map[string]any{"dsn": dsn}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	return provider
}

func putRecord(t *testing.T, provider *relationaldb.Provider, store string, record gestalt.Record) {
	t.Helper()
	if err := provider.Put(context.Background(), gestalt.IndexedDBRecordRequest{Store: store, Record: record}); err != nil {
		t.Fatalf("Put %s: %v", store, err)
	}
}

func assertRelationshipCount(t *testing.T, provider *relationaldb.Provider, resourceType string, want int64) {
	t.Helper()
	got, err := provider.IndexCount(context.Background(), gestalt.IndexedDBIndexQueryRequest{
		Store:  relationshipsStore,
		Index:  relationshipsIndex,
		Values: []any{resourceType},
	})
	if err != nil {
		t.Fatalf("IndexCount relationships %s: %v", resourceType, err)
	}
	if got != want {
		t.Fatalf("IndexCount relationships %s = %d, want %d", resourceType, got, want)
	}
}

func assertStoreCount(t *testing.T, provider *relationaldb.Provider, store string, wantExists bool, want int64) {
	t.Helper()
	got, err := countStore(context.Background(), provider, store)
	if err != nil {
		t.Fatalf("countStore %s: %v", store, err)
	}
	if got.Exists != wantExists || got.Records != want {
		t.Fatalf("countStore %s = %+v, want exists=%t records=%d", store, got, wantExists, want)
	}
}
