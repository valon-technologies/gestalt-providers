package relationaldb

import (
	"context"
	"os"
	"strconv"
	"testing"
)

// TestBackfillOrderedKey is a throwaway, operator-run migration: it populates
// index_key_ord for pre-existing index rows that predate the order-preserving
// column. It is skipped unless GESTALT_BACKFILL_DSN points at the target
// database, so it never runs in CI.
//
// Run it against the live DB AFTER the column has been added nullable and
// BEFORE it is set NOT NULL. GESTALT_BACKFILL_SCHEMA/PREFIX must match the
// target datastore's config (production main-db uses schema "gestaltd"):
//
//	GESTALT_BACKFILL_DSN='mysql://user:pass@tcp(127.0.0.1:3306)/gestaltd' \
//	GESTALT_BACKFILL_SCHEMA='gestaltd' \
//	  go test -run TestBackfillOrderedKey -count=1 -v .
//
// It is idempotent (only touches index_key_ord IS NULL rows) and reuses the
// exact encodeOrderedKey the write path uses, so backfilled bytes match new
// writes. Both the non-unique and unique index tables are covered.
func TestBackfillOrderedKey(t *testing.T) {
	dsn := os.Getenv("GESTALT_BACKFILL_DSN")
	if dsn == "" {
		t.Skip("set GESTALT_BACKFILL_DSN to run the ordered-key backfill against a target database")
	}

	store, err := newStoreWithOptions(dsn, storeOptions{
		Schema:      os.Getenv("GESTALT_BACKFILL_SCHEMA"),
		TablePrefix: os.Getenv("GESTALT_BACKFILL_PREFIX"),
	})
	if err != nil {
		t.Fatalf("newStoreWithOptions: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	for _, table := range []string{store.genericIndexTable(), store.genericUniqueIndexTable()} {
		updated, err := backfillOrderedKeyTable(ctx, store, table)
		if err != nil {
			t.Fatalf("backfill %s: %v", table, err)
		}
		remaining, err := countNullOrderedKey(ctx, store, table)
		if err != nil {
			t.Fatalf("count nulls %s: %v", table, err)
		}
		if remaining != 0 {
			t.Fatalf("%s still has %d rows with index_key_ord IS NULL after backfill", table, remaining)
		}
		t.Logf("backfilled %s: %d distinct index keys updated, 0 nulls remaining", table, updated)
	}
}

const backfillSelectBatch = 1000

// backfillOrderedKeyTable populates index_key_ord for every row in table whose
// value is NULL. Rows that share an index key share index_key_bytes (and thus
// the same ordered encoding), so a single UPDATE keyed on
// (store_name, index_name, index_key_hash) fills all of them at once.
func backfillOrderedKeyTable(ctx context.Context, s *Store, table string) (int, error) {
	selectSQL := "SELECT " +
		quoteIdent(s.dialect, "store_name") + ", " +
		quoteIdent(s.dialect, "index_name") + ", " +
		quoteIdent(s.dialect, "index_key_hash") + ", " +
		quoteIdent(s.dialect, "index_key_bytes") +
		" FROM " + quoteTableName(s.dialect, table) +
		" WHERE " + quoteIdent(s.dialect, "index_key_ord") + " IS NULL" +
		" LIMIT " + strconv.Itoa(backfillSelectBatch)

	updateSQL := "UPDATE " + quoteTableName(s.dialect, table) +
		" SET " + quoteIdent(s.dialect, "index_key_ord") + " = ?" +
		" WHERE " + quoteIdent(s.dialect, "store_name") + " = ?" +
		" AND " + quoteIdent(s.dialect, "index_name") + " = ?" +
		" AND " + quoteIdent(s.dialect, "index_key_hash") + " = ?" +
		" AND " + quoteIdent(s.dialect, "index_key_ord") + " IS NULL"

	updated := 0
	for {
		batch, err := scanBackfillBatch(ctx, s, selectSQL)
		if err != nil {
			return updated, err
		}
		if len(batch) == 0 {
			return updated, nil
		}
		seen := make(map[string]struct{}, len(batch))
		for _, row := range batch {
			dedupeKey := row.storeName + "\x00" + row.indexName + "\x00" + string(row.indexKeyHash)
			if _, ok := seen[dedupeKey]; ok {
				continue
			}
			seen[dedupeKey] = struct{}{}

			native, err := decodeKeyValue(row.indexKeyBytes)
			if err != nil {
				return updated, err
			}
			ord, err := encodeOrderedKey(native)
			if err != nil {
				return updated, err
			}
			if _, err := s.exec(ctx, updateSQL, ord, row.storeName, row.indexName, row.indexKeyHash); err != nil {
				return updated, err
			}
			updated++
		}
	}
}

type backfillRow struct {
	storeName     string
	indexName     string
	indexKeyHash  []byte
	indexKeyBytes []byte
}

func scanBackfillBatch(ctx context.Context, s *Store, selectSQL string) ([]backfillRow, error) {
	rows, err := s.query(ctx, selectSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []backfillRow
	for rows.Next() {
		var row backfillRow
		if err := rows.Scan(&row.storeName, &row.indexName, &row.indexKeyHash, &row.indexKeyBytes); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func countNullOrderedKey(ctx context.Context, s *Store, table string) (int64, error) {
	rows, err := s.query(ctx, "SELECT COUNT(*) FROM "+quoteTableName(s.dialect, table)+
		" WHERE "+quoteIdent(s.dialect, "index_key_ord")+" IS NULL")
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var count int64
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return 0, err
		}
	}
	return count, rows.Err()
}
