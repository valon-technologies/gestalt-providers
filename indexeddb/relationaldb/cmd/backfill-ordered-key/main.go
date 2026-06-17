// Command backfill-ordered-key populates index_key_ord on existing generic index
// tables. Run manually against a live database after adding the column and
// before deploying the range-scan provider build.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

func main() {
	dsn := flag.String("dsn", os.Getenv("GESTALT_RELATIONALDB_DSN"), "database DSN")
	prefix := flag.String("prefix", "_gestalt_", "relationaldb table prefix")
	batchSize := flag.Int("batch", 500, "rows per SELECT batch")
	dryRun := flag.Bool("dry-run", false, "compute encodings without writing")
	flag.Parse()

	if strings.TrimSpace(*dsn) == "" {
		log.Fatal("missing -dsn or GESTALT_RELATIONALDB_DSN")
	}

	db, err := sql.Open("pgx", *dsn)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	tables := []string{
		*prefix + "index_entries",
		*prefix + "unique_index_entries",
	}

	for _, table := range tables {
		if err := backfillTable(ctx, db, table, *batchSize, *dryRun); err != nil {
			log.Fatalf("backfill %s: %v", table, err)
		}
	}
}

func backfillTable(ctx context.Context, db *sql.DB, table string, batchSize int, dryRun bool) error {
	var remaining int64
	if err := db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE index_key_ord IS NULL`, quoteTable(table)),
	).Scan(&remaining); err != nil {
		return fmt.Errorf("count null rows: %w", err)
	}
	log.Printf("%s: %d rows need backfill", table, remaining)
	if remaining == 0 {
		return nil
	}

	const rowIDColumn = "ctid"
	for {
		rows, err := db.QueryContext(ctx, fmt.Sprintf(
			`SELECT %s, index_key_bytes FROM %s WHERE index_key_ord IS NULL LIMIT $1`,
			rowIDColumn, quoteTable(table),
		), batchSize)
		if err != nil {
			return fmt.Errorf("select batch: %w", err)
		}

		type pending struct {
			id   string
			ord  []byte
			raw  []byte
		}
		var batch []pending
		for rows.Next() {
			var id string
			var raw []byte
			if err := rows.Scan(&id, &raw); err != nil {
				rows.Close()
				return fmt.Errorf("scan row: %w", err)
			}
			key, err := indexeddb.DecodeIndexedDBKey(raw)
			if err != nil {
				rows.Close()
				return fmt.Errorf("decode key: %w", err)
			}
			ord, err := relationaldb.EncodeOrderedKeyForBackfill(key)
			if err != nil {
				rows.Close()
				return fmt.Errorf("encode ordered key: %w", err)
			}
			batch = append(batch, pending{id: id, ord: ord, raw: raw})
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close select batch: %w", err)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate batch: %w", err)
		}
		if len(batch) == 0 {
			break
		}

		if !dryRun {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return fmt.Errorf("begin tx: %w", err)
			}
			for _, row := range batch {
				if _, err := tx.ExecContext(ctx,
					fmt.Sprintf(`UPDATE %s SET index_key_ord = $1 WHERE %s = $2`, quoteTable(table), rowIDColumn),
					row.ord, row.id,
				); err != nil {
					_ = tx.Rollback()
					return fmt.Errorf("update row: %w", err)
				}
			}
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("commit batch: %w", err)
			}
		}

		log.Printf("%s: backfilled %d rows", table, len(batch))
		time.Sleep(10 * time.Millisecond)
	}

	var left int64
	if err := db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE index_key_ord IS NULL`, quoteTable(table)),
	).Scan(&left); err != nil {
		return fmt.Errorf("count remaining null rows: %w", err)
	}
	if left != 0 {
		return fmt.Errorf("%d rows still missing index_key_ord", left)
	}
	log.Printf("%s: backfill complete", table)
	return nil
}

func quoteTable(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
