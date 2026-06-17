package oidc

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

type LegacyMigrationRunnerOptions struct {
	Phase         LegacyMigrationPhase
	MigrationTime time.Time
	Limit         int
}

func RunLegacyAPITokenMigration(ctx context.Context, db indexeddb.Database, store *grantStore, opts LegacyMigrationRunnerOptions) (*LegacyMigrationResult, error) {
	if db == nil {
		return nil, fmt.Errorf("indexeddb database is required")
	}
	if store == nil {
		return nil, fmt.Errorf("grant store is required")
	}
	if opts.Phase != LegacyMigrationPhaseDryRun && opts.Phase != LegacyMigrationPhaseApply {
		return nil, fmt.Errorf("unsupported migration phase %q", opts.Phase)
	}
	migrationTime := opts.MigrationTime
	if migrationTime.IsZero() {
		migrationTime = time.Now().UTC()
	}

	rows, err := loadLegacyAPITokenRows(ctx, db, opts.Limit)
	if err != nil {
		return nil, err
	}

	result := &LegacyMigrationResult{}
	for _, row := range rows {
		result.TotalLegacyRows++
		if !isLegacyRowActive(row, migrationTime) {
			result.SkippedExpired++
			continue
		}
		result.ActiveRows++

		if err := VerifyLegacyHashFormat(row.HashedToken); err != nil {
			return result, fmt.Errorf("legacy row %q hash format: %w", row.ID, err)
		}

		status, err := store.migrateLegacyAPIToken(ctx, row, opts.Phase, migrationTime)
		if err != nil {
			msg := fmt.Sprintf("%s: %v", row.ID, err)
			switch {
			case strings.Contains(err.Error(), "permissions_json"):
				result.ScopeFailures = append(result.ScopeFailures, msg)
			case strings.Contains(err.Error(), "unsupported owner_kind"),
				strings.Contains(err.Error(), "missing owner_id"):
				result.SubjectFailures = append(result.SubjectFailures, msg)
			case strings.Contains(err.Error(), "destination"):
				result.Conflicts = append(result.Conflicts, msg)
				if opts.Phase == LegacyMigrationPhaseApply {
					return result, fmt.Errorf("destination conflict: %s", msg)
				}
			default:
				return result, err
			}
			continue
		}
		switch status {
		case "already_migrated":
			result.SkippedAlreadyMigrated++
		case "dry_run", "dry_run_sentinel":
			if status == "dry_run_sentinel" {
				result.NullExpirySentinel++
			}
		case "applied", "applied_sentinel":
			result.AppliedRows++
			if status == "applied_sentinel" {
				result.NullExpirySentinel++
			}
		default:
			return result, fmt.Errorf("unexpected migration status %q for row %q", status, row.ID)
		}
		if len(result.SampleGrantIDs) < 10 {
			result.SampleGrantIDs = append(result.SampleGrantIDs, row.ID)
		}
	}
	return result, nil
}
