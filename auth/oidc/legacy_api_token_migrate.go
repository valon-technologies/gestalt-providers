package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

type LegacyAPITokenRow struct {
	ID                  string
	OwnerKind           string
	OwnerID             string
	CredentialSubjectID string
	HashedToken         string
	Scopes              string
	PermissionsJSON     string
	ExpiresAt           *time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

type LegacyMigrationPhase string

const (
	LegacyMigrationPhaseDryRun LegacyMigrationPhase = "dry-run"
	LegacyMigrationPhaseApply  LegacyMigrationPhase = "apply"
)

type LegacyMigrationOptions struct {
	Phase            LegacyMigrationPhase
	MigrationTime    time.Time
	Limit            int
	VerifyHashFormat func(hashed string) error
}

type LegacyMigrationResult struct {
	TotalLegacyRows        int
	ActiveRows             int
	SkippedExpired         int
	SkippedAlreadyMigrated int
	AppliedRows            int
	SubjectFailures        []string
	ScopeFailures          []string
	Conflicts              []string
	SampleGrantIDs         []string
	NullExpirySentinel     int
}

func (s *grantStore) migrateLegacyAPIToken(ctx context.Context, row LegacyAPITokenRow, phase LegacyMigrationPhase, migrationTime time.Time) (string, error) {
	if row.ID == "" {
		return "", fmt.Errorf("missing legacy id")
	}
	if strings.TrimSpace(row.HashedToken) == "" {
		return "", fmt.Errorf("missing legacy hashed_token for id %q", row.ID)
	}
	subject, err := legacySubjectForRow(row)
	if err != nil {
		return "", err
	}
	scope, err := legacyScopeForRow(row)
	if err != nil {
		return "", err
	}
	expiresAt, usedSentinel, err := legacyExpiryForRow(row, migrationTime)
	if err != nil {
		return "", err
	}
	createdAt := row.CreatedAt
	if createdAt.IsZero() {
		createdAt = row.UpdatedAt
	}
	if createdAt.IsZero() {
		createdAt = migrationTime
	}

	grantRecord := gestalt.Record{
		"id":         row.ID,
		"subject":    subject,
		"scope":      scope,
		"client_id":  defaultOAuthClientID,
		"created_at": createdAt.UTC(),
		"expires_at": expiresAt.UTC(),
		"revoked":    false,
		"category":   grantCategoryAPIToken,
	}
	tokenRecord := gestalt.Record{
		"id":         row.HashedToken,
		"grant_id":   row.ID,
		"subject":    subject,
		"scope":      scope,
		"client_id":  defaultOAuthClientID,
		"expires_at": expiresAt.UTC(),
	}

	existingGrant, grantErr := s.grants.Get(ctx, row.ID)
	if grantErr == nil {
		if recordsEquivalent(existingGrant, grantRecord) {
			existingToken, tokenErr := s.tokens.Get(ctx, row.HashedToken)
			if tokenErr == nil && recordsEquivalent(existingToken, tokenRecord) {
				return "already_migrated", nil
			}
			if tokenErr != nil && errors.Is(tokenErr, gestalt.ErrNotFound) {
				if phase == LegacyMigrationPhaseApply {
					if err := s.tokens.Add(ctx, tokenRecord); err != nil {
						return "", fmt.Errorf("add missing token hash for grant %q: %w", row.ID, err)
					}
				}
				return "already_migrated", nil
			}
		}
		return "", fmt.Errorf("destination grant %q already exists with different data", row.ID)
	}
	if grantErr != nil && !errors.Is(grantErr, gestalt.ErrNotFound) {
		return "", fmt.Errorf("read destination grant %q: %w", row.ID, grantErr)
	}
	existingToken, tokenErr := s.tokens.Get(ctx, row.HashedToken)
	if tokenErr == nil {
		if recordsEquivalent(existingToken, tokenRecord) {
			return "already_migrated", nil
		}
		return "", fmt.Errorf("destination token hash %q already exists with different data", row.HashedToken)
	}
	if tokenErr != nil && !errors.Is(tokenErr, gestalt.ErrNotFound) {
		return "", fmt.Errorf("read destination token hash %q: %w", row.HashedToken, tokenErr)
	}

	if phase != LegacyMigrationPhaseApply {
		if usedSentinel {
			return "dry_run_sentinel", nil
		}
		return "dry_run", nil
	}

	tx, err := s.db.Transaction(ctx, []string{grantStoreName, tokenHashStoreName}, indexeddb.TransactionReadwrite, indexeddb.TransactionOptions{})
	if err != nil {
		return "", fmt.Errorf("begin migration transaction: %w", err)
	}
	grantStore := tx.ObjectStore(grantStoreName)
	tokenStore := tx.ObjectStore(tokenHashStoreName)
	if err := grantStore.Add(ctx, grantRecord); err != nil {
		_ = tx.Abort(ctx)
		return "", fmt.Errorf("persist migrated grant %q: %w", row.ID, err)
	}
	if err := tokenStore.Add(ctx, tokenRecord); err != nil {
		_ = tx.Abort(ctx)
		return "", fmt.Errorf("persist migrated token hash %q: %w", row.ID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit migration for grant %q: %w", row.ID, err)
	}
	if usedSentinel {
		return "applied_sentinel", nil
	}
	return "applied", nil
}

func legacySubjectForRow(row LegacyAPITokenRow) (string, error) {
	if subject := strings.TrimSpace(row.CredentialSubjectID); subject != "" {
		return subject, nil
	}
	switch strings.TrimSpace(row.OwnerKind) {
	case "user":
		ownerID := strings.TrimSpace(row.OwnerID)
		if ownerID == "" {
			return "", fmt.Errorf("legacy row %q missing owner_id for user owner", row.ID)
		}
		return "user:" + ownerID, nil
	case "subject":
		ownerID := strings.TrimSpace(row.OwnerID)
		if ownerID == "" {
			return "", fmt.Errorf("legacy row %q missing owner_id for subject owner", row.ID)
		}
		return ownerID, nil
	default:
		return "", fmt.Errorf("legacy row %q has unsupported owner_kind %q", row.ID, row.OwnerKind)
	}
}

func legacyScopeForRow(row LegacyAPITokenRow) (string, error) {
	if scope := strings.TrimSpace(row.Scopes); scope != "" {
		return scope, nil
	}
	if strings.TrimSpace(row.PermissionsJSON) == "" {
		return "", nil
	}
	var permissions []struct {
		App     string   `json:"app"`
		Actions []string `json:"actions"`
	}
	if err := json.Unmarshal([]byte(row.PermissionsJSON), &permissions); err != nil {
		return "", fmt.Errorf("legacy row %q permissions_json: %w", row.ID, err)
	}
	parts := make([]string, 0, len(permissions))
	for _, perm := range permissions {
		app := strings.TrimSpace(perm.App)
		if app == "" {
			continue
		}
		if len(perm.Actions) == 0 {
			parts = append(parts, app)
			continue
		}
		for _, action := range perm.Actions {
			action = strings.TrimSpace(action)
			if action == "" {
				continue
			}
			parts = append(parts, app+":"+action)
		}
	}
	return strings.Join(parts, " "), nil
}

func legacyExpiryForRow(row LegacyAPITokenRow, migrationTime time.Time) (time.Time, bool, error) {
	if row.ExpiresAt == nil {
		sentinel, err := time.Parse(time.RFC3339, legacyNonExpiringExpiry)
		if err != nil {
			return time.Time{}, false, fmt.Errorf("parse legacy non-expiring sentinel: %w", err)
		}
		return sentinel, true, nil
	}
	if !row.ExpiresAt.After(migrationTime) {
		return time.Time{}, false, fmt.Errorf("legacy row %q is expired", row.ID)
	}
	return row.ExpiresAt.UTC(), false, nil
}

func recordsEquivalent(existing, want gestalt.Record) bool {
	for key, wantValue := range want {
		gotValue := existing[key]
		switch wantValue := wantValue.(type) {
		case time.Time:
			gotTime, ok := gotValue.(time.Time)
			if !ok {
				if gotPtr, ok := gotValue.(*time.Time); ok && gotPtr != nil {
					gotTime = gotPtr.UTC()
				} else {
					return false
				}
			}
			if !gotTime.UTC().Equal(wantValue.UTC()) {
				return false
			}
		default:
			if fmt.Sprint(gotValue) != fmt.Sprint(wantValue) {
				return false
			}
		}
	}
	return true
}

func isLegacyRowActive(row LegacyAPITokenRow, migrationTime time.Time) bool {
	return row.ExpiresAt == nil || row.ExpiresAt.After(migrationTime)
}
