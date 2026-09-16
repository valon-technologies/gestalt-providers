package relationaldb

import (
	"context"
	"fmt"
	"strings"
)

const genericSQLPageSize = 1000

func (s *Store) ensurePrimaryKeyOrderColumn(ctx context.Context) error {
	for _, table := range []string{s.genericRecordsTable(), s.genericIndexTable(), s.genericUniqueIndexTable()} {
		if err := s.ensureOrderedColumn(ctx, table); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ensureOrderedColumn(ctx context.Context, table string) error {
	// Qualify the column so SQLite cannot treat an unknown quoted identifier
	// as a string literal and incorrectly skip the migration.
	rows, err := s.query(ctx, "SELECT records."+quoteIdent(s.dialect, "pk_ord")+" FROM "+quoteTableName(s.dialect, table)+" AS records WHERE 1 = 0")
	if err == nil {
		return rows.Close()
	}
	add := " ADD COLUMN "
	if s.dialect == dialectSQLServer {
		add = " ADD "
	}
	_, err = s.exec(ctx, "ALTER TABLE "+quoteTableName(s.dialect, table)+add+quoteIdent(s.dialect, "pk_ord")+" "+sqlType(s.dialect, 5, false)+" NULL")
	if err != nil {
		return fmt.Errorf("add ordered primary key column: %w", err)
	}
	return nil
}

func createGenericPrimaryOrderIndexSQL(d dialect, table string) string {
	name := portableIndexName(table, "primary_page")
	switch d {
	case dialectMySQL:
		return fmt.Sprintf("CREATE INDEX %s ON %s (%s, (%s), %s)", quoteIdent(d, name), quoteTableName(d, table), quoteIdent(d, "store_name"), primaryOrderExpression(d, quoteIdent(d, "pk_ord")), quoteIdent(d, "pk_hash"))
	case dialectPostgres:
		return fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (%s, (%s), %s)", quoteIdent(d, name), quoteTableName(d, table), quoteIdent(d, "store_name"), primaryOrderExpression(d, quoteIdent(d, "pk_ord")), quoteIdent(d, "pk_hash"))
	case dialectSQLServer:
		return createColumnsIndexSQL(d, table, name, []string{"store_name"}, false)
	default:
		return createColumnsIndexSQL(d, table, name, []string{"store_name", "pk_ord", "pk_hash"}, false)
	}
}

// Bound the sort key where the database limits BLOB sorting or index entries.
// Range predicates still compare the full key, and reads finish the last prefix
// group before sorting and applying the requested count.
func primaryOrderExpression(d dialect, value string) string {
	switch d {
	case dialectMySQL:
		return fmt.Sprintf("CAST(SUBSTRING(%s, 1, %d) AS BINARY(%d))", value, orderedKeyIndexPrefixLen, orderedKeyIndexPrefixLen)
	case dialectPostgres:
		return fmt.Sprintf("SUBSTRING(CAST(%s AS BYTEA) FROM 1 FOR %d)", value, orderedKeyIndexPrefixLen)
	default:
		return value
	}
}

func sqlPageLimit(d dialect, query string, count int) string {
	if d == dialectSQLServer {
		return fmt.Sprintf("%s OFFSET 0 ROWS FETCH NEXT %d ROWS ONLY", query, count)
	}
	return fmt.Sprintf("%s LIMIT %d", query, count)
}

// BackfillPrimaryKeyOrder fills legacy keys using short, independently committed
// updates. It is safe to restart: NULL is the checkpoint and concurrent writes
// win because the update also checks the original key bytes. Drain older writers
// and repeat the pass to include keys inserted behind its scan position.
func BackfillPrimaryKeyOrder(ctx context.Context, dsn string, options Options) (int64, error) {
	s, err := connectStoreWithOptions(ctx, dsn, options.storeOptions())
	if err != nil {
		return 0, err
	}
	defer s.Close()
	return s.backfillPrimaryKeyOrder(ctx)
}

func (s *Store) backfillPrimaryKeyOrder(ctx context.Context) (int64, error) {
	var updated int64
	for _, table := range []string{s.genericRecordsTable(), s.genericIndexTable(), s.genericUniqueIndexTable()} {
		n, err := s.backfillOrderedTable(ctx, table)
		updated += n
		if err != nil {
			return updated, err
		}
	}
	return updated, nil
}

func (s *Store) backfillOrderedTable(ctx context.Context, table string) (int64, error) {
	q := func(name string) string { return quoteIdent(s.dialect, name) }
	var updated int64
	var lastStore string
	var lastHash []byte
	for {
		stmt := "SELECT " + q("store_name") + ", " + q("pk_hash") + ", " + q("pk_bytes") + " FROM " + quoteTableName(s.dialect, table) + " WHERE " + q("pk_ord") + " IS NULL"
		var args []any
		if lastHash != nil {
			stmt += " AND (" + q("store_name") + " > ? OR (" + q("store_name") + " = ? AND " + q("pk_hash") + " > ?))"
			args = append(args, lastStore, lastStore, lastHash)
		}
		stmt += " ORDER BY " + q("store_name") + ", " + q("pk_hash")
		// Seven parameters per key keep each update below SQLite's legacy limit.
		rows, err := s.query(ctx, sqlPageLimit(s.dialect, stmt, 128), args...)
		if err != nil {
			return updated, err
		}
		type legacyKey struct {
			store     string
			hash, raw []byte
		}
		var page []legacyKey
		for rows.Next() {
			var row legacyKey
			if err := rows.Scan(&row.store, &row.hash, &row.raw); err != nil {
				rows.Close()
				return updated, err
			}
			page = append(page, row)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return updated, err
		}
		if len(page) == 0 {
			return updated, nil
		}
		cases := make([]string, 0, len(page))
		matches := make([]string, 0, len(page))
		args = nil
		var matchArgs []any
		for _, row := range page {
			key, err := decodeKeyValue(row.raw)
			if err != nil {
				return updated, err
			}
			ordered, err := encodeOrderedKey(key)
			if err != nil {
				return updated, err
			}
			match := q("store_name") + " = ? AND " + q("pk_hash") + " = ? AND " + q("pk_bytes") + " = ?"
			cases = append(cases, "WHEN "+match+" THEN ?")
			args = append(args, row.store, row.hash, row.raw, ordered)
			matches = append(matches, "("+match+")")
			matchArgs = append(matchArgs, row.store, row.hash, row.raw)
		}
		stmt = "UPDATE " + quoteTableName(s.dialect, table) + " SET " + q("pk_ord") + " = CASE " + strings.Join(cases, " ") + " ELSE " + q("pk_ord") + " END WHERE " + q("pk_ord") + " IS NULL AND (" + strings.Join(matches, " OR ") + ")"
		result, err := s.exec(ctx, stmt, append(args, matchArgs...)...)
		if err != nil {
			return updated, err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return updated, err
		}
		updated += n
		last := page[len(page)-1]
		lastStore, lastHash = last.store, last.hash
	}
}
