package oidc

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

func RunLegacyAPITokenMigrationCLI(args []string) int {
	fs := flag.NewFlagSet("migrate-legacy-api-tokens", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var (
		dsnFlag           = fs.String("dsn", "", "MySQL DSN for gestaltd relationaldb (e.g. from Secret Manager gestalt-mysql-dsn-east4)")
		schemaFlag        = fs.String("schema", "gestaltd", "MySQL schema for relationaldb-backed IndexedDB stores")
		phaseFlag         = fs.String("phase", "dry-run", "Migration phase: dry-run or apply")
		limitFlag         = fs.Int("limit", 0, "Optional row limit for staged validation")
		migrationTimeFlag = fs.String("migration-time", "", "Optional fixed migration timestamp (RFC3339) for deterministic runs")
	)
	if err := fs.Parse(args); err != nil {
		return 2
	}

	dsn := strings.TrimSpace(*dsnFlag)
	if dsn == "" {
		dsn = strings.TrimSpace(os.Getenv("GESTALT_MYSQL_DSN"))
	}
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "dsn is required (-dsn or GESTALT_MYSQL_DSN)")
		return 2
	}

	phase := LegacyMigrationPhase(strings.TrimSpace(*phaseFlag))
	if phase != LegacyMigrationPhaseDryRun && phase != LegacyMigrationPhaseApply {
		fmt.Fprintf(os.Stderr, "unsupported phase %q (want dry-run or apply)\n", phase)
		return 2
	}

	migrationTime := time.Now().UTC()
	if raw := strings.TrimSpace(*migrationTimeFlag); raw != "" {
		parsed, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			fmt.Fprintf(os.Stderr, "parse migration-time: %v\n", err)
			return 2
		}
		migrationTime = parsed.UTC()
	}

	ctx := context.Background()
	host, err := OpenLegacyMigrationHost(ctx, dsn, strings.TrimSpace(*schemaFlag))
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	}
	defer host.Cleanup()

	result, err := RunLegacyAPITokenMigration(ctx, host.DB, host.Store, LegacyMigrationRunnerOptions{
		Phase:         phase,
		MigrationTime: migrationTime,
		Limit:         *limitFlag,
	})
	printLegacyMigrationResult(result, phase)
	if err != nil {
		fmt.Fprintf(os.Stderr, "migration failed: %v\n", err)
		return 1
	}
	if len(result.Conflicts) > 0 && phase == LegacyMigrationPhaseApply {
		return 1
	}
	return 0
}

func printLegacyMigrationResult(result *LegacyMigrationResult, phase LegacyMigrationPhase) {
	if result == nil {
		return
	}
	fmt.Printf("phase: %s\n", phase)
	fmt.Printf("total_legacy_rows: %d\n", result.TotalLegacyRows)
	fmt.Printf("active_rows: %d\n", result.ActiveRows)
	fmt.Printf("skipped_expired: %d\n", result.SkippedExpired)
	fmt.Printf("skipped_already_migrated: %d\n", result.SkippedAlreadyMigrated)
	fmt.Printf("applied_rows: %d\n", result.AppliedRows)
	fmt.Printf("null_expiry_sentinel: %d\n", result.NullExpirySentinel)
	fmt.Printf("subject_failures: %d\n", len(result.SubjectFailures))
	for _, msg := range result.SubjectFailures {
		fmt.Printf("  subject_failure: %s\n", msg)
	}
	fmt.Printf("scope_failures: %d\n", len(result.ScopeFailures))
	for _, msg := range result.ScopeFailures {
		fmt.Printf("  scope_failure: %s\n", msg)
	}
	fmt.Printf("conflicts: %d\n", len(result.Conflicts))
	for _, msg := range result.Conflicts {
		fmt.Printf("  conflict: %s\n", msg)
	}
	if len(result.SampleGrantIDs) > 0 {
		fmt.Printf("sample_grant_ids: %s\n", strings.Join(result.SampleGrantIDs, ", "))
	}
}
