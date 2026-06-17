package relationaldb

import (
	"context"
	"fmt"
	"os"
	"testing"
)

// TestMigrateOrderedKeyOps is a throwaway, operator-run helper for the
// index_key_ord migration. The MySQL 8 prod account uses mysql_native_password,
// which the local mysql CLI (9.x) cannot load, so DDL/inspection go through the
// same Go driver the provider uses.
//
// Gated by GESTALT_MIGRATE_DSN; GESTALT_MIGRATE_SCHEMA/PREFIX must match the
// datastore config (prod main-db uses schema "gestaltd"). Select the step with
// GESTALT_MIGRATE_PHASE: inspect | add-column | swap-index.
//
//	GESTALT_MIGRATE_DSN='gestalt_user:PASS@tcp(127.0.0.1:13306)/gestalt' \
//	GESTALT_MIGRATE_SCHEMA='gestaltd' GESTALT_MIGRATE_PHASE='inspect' \
//	  go test -run TestMigrateOrderedKeyOps -count=1 -v .
func TestMigrateOrderedKeyOps(t *testing.T) {
	dsn := os.Getenv("GESTALT_MIGRATE_DSN")
	if dsn == "" {
		t.Skip("set GESTALT_MIGRATE_DSN to run ordered-key migration ops")
	}
	phase := os.Getenv("GESTALT_MIGRATE_PHASE")
	s, err := newStoreWithOptions(dsn, storeOptions{
		Schema:      os.Getenv("GESTALT_MIGRATE_SCHEMA"),
		TablePrefix: os.Getenv("GESTALT_MIGRATE_PREFIX"),
	})
	if err != nil {
		t.Fatalf("newStoreWithOptions: %v", err)
	}
	defer s.Close()
	ctx := context.Background()
	tables := []string{s.genericIndexTable(), s.genericUniqueIndexTable()}

	switch phase {
	case "inspect":
		for _, table := range tables {
			migrateInspect(ctx, t, s, table)
		}
	case "add-column":
		for _, table := range tables {
			migrateAddColumn(ctx, t, s, table)
		}
	case "swap-index":
		for _, table := range tables {
			migrateSwapScanIndex(ctx, t, s, table)
		}
	case "diagnose":
		for _, table := range tables {
			migrateDiagnose(ctx, t, s, table)
		}
	default:
		t.Fatalf("set GESTALT_MIGRATE_PHASE to inspect|add-column|swap-index (got %q)", phase)
	}
}

func migrateInspect(ctx context.Context, t *testing.T, s *Store, table string) {
	t.Helper()
	hasOrd := columnExists(ctx, t, s, table, "index_key_ord")
	total := scalarInt(ctx, t, s, "SELECT COUNT(*) FROM "+table)
	nulls := int64(-1)
	if hasOrd {
		nulls = scalarInt(ctx, t, s, "SELECT COUNT(*) FROM "+table+" WHERE "+quoteIdent(s.dialect, "index_key_ord")+" IS NULL")
	}
	t.Logf("%s: rows=%d index_key_ord_present=%v null_ord=%d", table, total, hasOrd, nulls)
	rows, err := s.query(ctx, "SELECT INDEX_NAME, GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX), GROUP_CONCAT(IFNULL(SUB_PART,0) ORDER BY SEQ_IN_INDEX) "+
		"FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? GROUP BY INDEX_NAME", s.schemaName, baseTableName(table))
	if err != nil {
		t.Fatalf("inspect indexes %s: %v", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, cols, subs string
		if err := rows.Scan(&name, &cols, &subs); err != nil {
			t.Fatalf("scan index: %v", err)
		}
		t.Logf("  index %s cols=(%s) subparts=(%s)", name, cols, subs)
	}
}

// migrateDiagnose scans every row and reports those whose stored key cannot be
// encoded by encodeOrderedKey (e.g. legacy nil keys), grouped by store+index.
func migrateDiagnose(ctx context.Context, t *testing.T, s *Store, table string) {
	t.Helper()
	rows, err := s.query(ctx, "SELECT "+quoteIdent(s.dialect, "store_name")+", "+quoteIdent(s.dialect, "index_name")+", "+
		quoteIdent(s.dialect, "index_key_bytes")+" FROM "+table)
	if err != nil {
		t.Fatalf("scan %s: %v", table, err)
	}
	defer rows.Close()
	type stat struct {
		count   int
		example []byte
	}
	bad := map[string]*stat{}
	total, badTotal := 0, 0
	for rows.Next() {
		var storeName, indexName string
		var raw []byte
		if err := rows.Scan(&storeName, &indexName, &raw); err != nil {
			t.Fatalf("scan row: %v", err)
		}
		total++
		native, derr := decodeKeyValue(raw)
		_, eerr := encodeOrderedKey(native)
		if derr != nil || eerr != nil {
			badTotal++
			key := storeName + " / " + indexName
			st := bad[key]
			if st == nil {
				st = &stat{example: raw}
				bad[key] = st
			}
			st.count++
		}
	}
	t.Logf("%s: scanned=%d unencodable=%d", table, total, badTotal)
	for k, st := range bad {
		t.Logf("  [%s] count=%d example_bytes=%x", k, st.count, st.example)
	}
}

func migrateAddColumn(ctx context.Context, t *testing.T, s *Store, table string) {
	t.Helper()
	if columnExists(ctx, t, s, table, "index_key_ord") {
		t.Logf("%s: index_key_ord already present, skipping ADD COLUMN", table)
		return
	}
	stmt := "ALTER TABLE " + table + " ADD COLUMN " + quoteIdent(s.dialect, "index_key_ord") + " LONGBLOB NULL"
	if _, err := s.exec(ctx, stmt); err != nil {
		t.Fatalf("add column %s: %v", table, err)
	}
	t.Logf("%s: added index_key_ord LONGBLOB NULL", table)
}

func migrateSwapScanIndex(ctx context.Context, t *testing.T, s *Store, table string) {
	t.Helper()
	name := scanIndexName(ctx, t, s, table)
	if name == "" {
		t.Fatalf("%s: no scan index found", table)
	}
	stmt := fmt.Sprintf("ALTER TABLE %s DROP INDEX %s, ADD INDEX %s (%s, %s, %s(255)), ALGORITHM=INPLACE, LOCK=NONE",
		table, quoteIdent(s.dialect, name), quoteIdent(s.dialect, name),
		quoteIdent(s.dialect, "store_name"), quoteIdent(s.dialect, "index_name"), quoteIdent(s.dialect, "index_key_ord"))
	if _, err := s.exec(ctx, stmt); err != nil {
		t.Fatalf("swap scan index %s: %v", table, err)
	}
	t.Logf("%s: swapped %s to (store_name, index_name, index_key_ord(255))", table, name)
}

// scanIndexName finds the existing scan index: keyed on (store_name, index_name)
// with index_name as the second column and no index_key_hash/ord, i.e. not the
// lookup/record indexes.
func scanIndexName(ctx context.Context, t *testing.T, s *Store, table string) string {
	t.Helper()
	rows, err := s.query(ctx, "SELECT INDEX_NAME, GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) cols "+
		"FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? GROUP BY INDEX_NAME", s.schemaName, baseTableName(table))
	if err != nil {
		t.Fatalf("find scan index %s: %v", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, cols string
		if err := rows.Scan(&name, &cols); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if cols == "store_name,index_name" || cols == "store_name,index_name,index_key_ord" {
			return name
		}
	}
	return ""
}

func columnExists(ctx context.Context, t *testing.T, s *Store, table, column string) bool {
	t.Helper()
	return scalarInt(ctx, t, s, "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?",
		s.schemaName, baseTableName(table), column) > 0
}

func scalarInt(ctx context.Context, t *testing.T, s *Store, query string, args ...any) int64 {
	t.Helper()
	rows, err := s.query(ctx, query, args...)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	defer rows.Close()
	var n int64
	if rows.Next() {
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan int: %v", err)
		}
	}
	return n
}
