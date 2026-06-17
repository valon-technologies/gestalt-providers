package oidc

import (
	"context"
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func TestVerifyLegacyHashFormat(t *testing.T) {
	if err := VerifyLegacyHashFormat(hashToken("gst_api_example")); err != nil {
		t.Fatalf("VerifyLegacyHashFormat() error = %v", err)
	}
	if err := VerifyLegacyHashFormat("not-a-sha256-hash"); err == nil {
		t.Fatal("VerifyLegacyHashFormat() error = nil, want invalid hash")
	}
}

func TestLegacyMigrationDryRunMapsActiveRows(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	p := New()
	p.now = func() time.Time { return now }
	attachGrantStore(t, p)

	row := LegacyAPITokenRow{
		ID:          "legacy-1",
		OwnerKind:   "user",
		OwnerID:     "user-123",
		HashedToken: hashToken("gst_api_plaintext"),
		Scopes:      "deal-hub:read",
		CreatedAt:   now.Add(-time.Hour),
	}
	status, err := p.grants.migrateLegacyAPIToken(ctx, row, LegacyMigrationPhaseDryRun, now)
	if err != nil {
		t.Fatalf("migrateLegacyAPIToken() error = %v", err)
	}
	if status != "dry_run" && status != "dry_run_sentinel" {
		t.Fatalf("status = %q, want dry_run or dry_run_sentinel", status)
	}
	if _, err := p.grants.grants.Get(ctx, row.ID); err == nil {
		t.Fatal("dry-run wrote grant row")
	}
}

func TestLegacyMigrationSkipsExpiredRows(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	expired := now.Add(-time.Hour)
	_, _, err := legacyExpiryForRow(LegacyAPITokenRow{ID: "legacy-expired", ExpiresAt: &expired}, now)
	if err == nil {
		t.Fatal("legacyExpiryForRow() error = nil, want expired failure")
	}
}

func TestLegacyMigrationNullExpiryUsesSentinel(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	expiresAt, usedSentinel, err := legacyExpiryForRow(LegacyAPITokenRow{ID: "legacy-never"}, now)
	if err != nil {
		t.Fatalf("legacyExpiryForRow() error = %v", err)
	}
	if !usedSentinel {
		t.Fatal("expected sentinel mapping")
	}
	if expiresAt.Format(time.RFC3339) != legacyNonExpiringExpiry {
		t.Fatalf("expiresAt = %s, want %s", expiresAt.Format(time.RFC3339), legacyNonExpiringExpiry)
	}
}

func TestLegacyMigrationIdempotentWhenMatchingRowsExist(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	p := New()
	p.now = func() time.Time { return now }
	attachGrantStore(t, p)

	row := LegacyAPITokenRow{
		ID:          "legacy-2",
		OwnerKind:   "user",
		OwnerID:     "user-456",
		HashedToken: hashToken("gst_api_other"),
		Scopes:      "deal-hub:write",
		CreatedAt:   now.Add(-time.Hour),
	}
	if _, err := p.grants.migrateLegacyAPIToken(ctx, row, LegacyMigrationPhaseApply, now); err != nil {
		t.Fatalf("first apply error = %v", err)
	}
	status, err := p.grants.migrateLegacyAPIToken(ctx, row, LegacyMigrationPhaseApply, now)
	if err != nil {
		t.Fatalf("second apply error = %v", err)
	}
	if status != "already_migrated" {
		t.Fatalf("status = %q, want already_migrated", status)
	}
}

func TestLegacyMigrationRunnerReadsIndexedDBStore(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	p := New()
	p.now = func() time.Time { return now }
	db := attachGrantStoreWithDB(t, p)

	expired := now.Add(-time.Hour)
	if _, err := db.CreateObjectStore(ctx, legacyAPITokenStoreName, gestalt.ObjectStoreOptions{}); err != nil {
		t.Fatalf("CreateObjectStore() error = %v", err)
	}
	legacyStore := db.ObjectStore(legacyAPITokenStoreName)
	for _, rec := range []gestalt.Record{
		{
			"id":           "legacy-active",
			"owner_kind":   "user",
			"owner_id":     "user-123",
			"hashed_token": hashToken("gst_api_active"),
			"scopes":       "deal-hub:read",
			"created_at":   now.Add(-2 * time.Hour),
			"updated_at":   now.Add(-2 * time.Hour),
		},
		{
			"id":           "legacy-expired",
			"owner_kind":   "user",
			"owner_id":     "user-456",
			"hashed_token": hashToken("gst_api_expired"),
			"scopes":       "deal-hub:read",
			"expires_at":   expired,
			"created_at":   now.Add(-48 * time.Hour),
			"updated_at":   now.Add(-48 * time.Hour),
		},
	} {
		if err := legacyStore.Add(ctx, rec); err != nil {
			t.Fatalf("legacyStore.Add() error = %v", err)
		}
	}

	result, err := RunLegacyAPITokenMigration(ctx, db, p.grants, LegacyMigrationRunnerOptions{
		Phase:         LegacyMigrationPhaseDryRun,
		MigrationTime: now,
	})
	if err != nil {
		t.Fatalf("RunLegacyAPITokenMigration() error = %v", err)
	}
	if result.TotalLegacyRows != 2 {
		t.Fatalf("TotalLegacyRows = %d, want 2", result.TotalLegacyRows)
	}
	if result.ActiveRows != 1 {
		t.Fatalf("ActiveRows = %d, want 1", result.ActiveRows)
	}
	if result.SkippedExpired != 1 {
		t.Fatalf("SkippedExpired = %d, want 1", result.SkippedExpired)
	}
}

func TestLegacyMigrationConflictsOnDifferentDestinationGrant(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	p := New()
	p.now = func() time.Time { return now }
	attachGrantStore(t, p)

	row := LegacyAPITokenRow{
		ID:          "legacy-3",
		OwnerKind:   "user",
		OwnerID:     "user-789",
		HashedToken: hashToken("gst_api_conflict"),
		Scopes:      "deal-hub:read",
		CreatedAt:   now.Add(-time.Hour),
	}
	if _, err := p.grants.issue(ctx, "user:other@example.com", "other", defaultOAuthClientID, grantCategoryAPIToken, time.Hour); err != nil {
		t.Fatalf("issue() error = %v", err)
	}
	if err := p.grants.grants.Put(ctx, map[string]any{
		"id":         row.ID,
		"subject":    "user:other@example.com",
		"scope":      "other",
		"client_id":  defaultOAuthClientID,
		"created_at": now,
		"expires_at": now.Add(time.Hour),
		"revoked":    false,
		"category":   grantCategoryAPIToken,
	}); err != nil {
		t.Fatalf("Put(conflict grant) error = %v", err)
	}
	if _, err := p.grants.migrateLegacyAPIToken(ctx, row, LegacyMigrationPhaseApply, now); err == nil {
		t.Fatal("migrateLegacyAPIToken() error = nil, want conflict")
	}
}
