package relationaldb

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/valon-technologies/gestalt/sdk/go/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const genericSQLPageSize = 1000

func (s *Store) ensurePrimaryKeyOrderColumn(ctx context.Context) error {
	// Qualify the column so SQLite cannot treat an unknown quoted identifier
	// as a string literal and incorrectly skip the migration.
	rows, err := s.query(ctx, "SELECT records."+quoteIdent(s.dialect, "pk_ord")+" FROM "+quoteTableName(s.dialect, s.genericRecordsTable())+" AS records WHERE 1 = 0")
	if err == nil {
		return rows.Close()
	}
	add := " ADD COLUMN "
	if s.dialect == dialectSQLServer {
		add = " ADD "
	}
	_, err = s.exec(ctx, "ALTER TABLE "+quoteTableName(s.dialect, s.genericRecordsTable())+add+quoteIdent(s.dialect, "pk_ord")+" "+sqlType(s.dialect, 5, false)+" NULL")
	if err != nil {
		return fmt.Errorf("add ordered primary key column: %w", err)
	}
	return nil
}

func createGenericPrimaryOrderIndexSQL(d dialect, table string) string {
	name := portableIndexName(table, "primary_order")
	switch d {
	case dialectMySQL:
		return fmt.Sprintf("CREATE INDEX %s ON %s (%s, %s(%d))", quoteIdent(d, name), quoteTableName(d, table), quoteIdent(d, "store_name"), quoteIdent(d, "pk_ord"), orderedKeyIndexPrefixLen)
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
		return fmt.Sprintf("CAST(%s AS BINARY(%d))", value, orderedKeyIndexPrefixLen)
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

func (s *Store) countPrimaryRange(ctx context.Context, store string, query *client.IndexedDBQuery) (int64, error) {
	if err := s.requireOrderedPrimaryKeys(ctx, store); err != nil {
		return 0, err
	}
	lo, hi, loOpen, hiOpen, err := orderedQueryBounds(query)
	if err != nil {
		return 0, err
	}
	bounds := genericIndexRange{lo: lo, hi: hi, loOpen: loOpen, hiOpen: hiOpen}
	q := func(name string) string { return quoteIdent(s.dialect, name) }
	stmt := "SELECT COUNT(*) FROM " + quoteTableName(s.dialect, s.genericRecordsTable()) + " WHERE " + q("store_name") + " = ? AND " + q("pk_ord") + " IS NOT NULL"
	args := []any{store}
	if lo != nil || hi != nil {
		predicate, values := genericIndexRangePredicate(q("pk_ord"), bounds)
		stmt += " AND " + predicate
		args = append(args, values...)
	}
	var count int64
	err = s.scanOne(ctx, stmt, args, &count)
	return count, err
}

// Old writers must be drained and the backfill completed before this provider
// serves reads. Missing ordered keys are an upgrade error, never a read fallback.
func (s *Store) requireOrderedPrimaryKeys(ctx context.Context, store string) error {
	q := func(name string) string { return quoteIdent(s.dialect, name) }
	stmt := "SELECT 1 FROM " + quoteTableName(s.dialect, s.genericRecordsTable()) + " WHERE " + q("pk_ord") + " IS NULL"
	var args []any
	if store != "" {
		stmt += " AND " + q("store_name") + " = ?"
		args = append(args, store)
	}
	var incomplete int
	if err := s.scanOne(ctx, "SELECT CASE WHEN EXISTS ("+stmt+") THEN 1 ELSE 0 END", args, &incomplete); err != nil {
		return err
	}
	if incomplete != 0 {
		return status.Error(codes.FailedPrecondition, "ordered primary-key backfill is incomplete; drain older writers and run migrate --backfill-primary-keys")
	}
	return nil
}

// BackfillPrimaryKeyOrder fills legacy keys using short, independently committed
// updates. It is safe to restart: NULL is the checkpoint and concurrent writes
// win because the update also checks the original key bytes.
func BackfillPrimaryKeyOrder(ctx context.Context, dsn string, options Options) (int64, error) {
	s, err := connectStoreWithOptions(ctx, dsn, options.storeOptions())
	if err != nil {
		return 0, err
	}
	defer s.Close()
	return s.backfillPrimaryKeyOrder(ctx)
}

func (s *Store) backfillPrimaryKeyOrder(ctx context.Context) (int64, error) {
	q := func(name string) string { return quoteIdent(s.dialect, name) }
	var updated int64
	for {
		stmt := "SELECT " + q("store_name") + ", " + q("pk_hash") + ", " + q("pk_bytes") + " FROM " + quoteTableName(s.dialect, s.genericRecordsTable()) + " WHERE " + q("pk_ord") + " IS NULL ORDER BY " + q("store_name") + ", " + q("pk_hash")
		rows, err := s.query(ctx, sqlPageLimit(s.dialect, stmt, genericSQLPageSize))
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
		for _, row := range page {
			key, err := decodeKeyValue(row.raw)
			if err != nil {
				return updated, err
			}
			ordered, err := encodeOrderedKey(key)
			if err != nil {
				return updated, err
			}
			result, err := s.exec(ctx, "UPDATE "+quoteTableName(s.dialect, s.genericRecordsTable())+" SET "+q("pk_ord")+" = ? WHERE "+q("store_name")+" = ? AND "+q("pk_hash")+" = ? AND "+q("pk_bytes")+" = ? AND "+q("pk_ord")+" IS NULL", ordered, row.store, row.hash, row.raw)
			if err != nil {
				return updated, err
			}
			n, err := result.RowsAffected()
			if err != nil {
				return updated, err
			}
			updated += n
		}
	}
}

// All keys are migrated before reads begin. There is one SQL range-scan path.
func (s *Store) loadPrimaryRows(ctx context.Context, store string, query *client.IndexedDBQuery, keysOnly bool, count *uint32, ordered bool) ([]genericRecordRow, error) {
	if err := s.requireOrderedPrimaryKeys(ctx, store); err != nil {
		return nil, err
	}
	lo, hi, loOpen, hiOpen, err := orderedQueryBounds(query)
	if err != nil {
		return nil, err
	}
	bounds := genericIndexRange{lo: lo, hi: hi, loOpen: loOpen, hiOpen: hiOpen}
	var out []genericRecordRow
	mode := ""
	if ordered {
		mode = "ordered"
	}
	err = s.scanPrimaryRows(ctx, store, mode, bounds, keysOnly, count, func(row genericRecordRow) error {
		out = append(out, row)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if ordered {
		sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].pkOrd, out[j].pkOrd) < 0 })
	}
	return limitRecords(out, count), nil
}

func (s *Store) scanPrimaryRows(ctx context.Context, store, mode string, bounds genericIndexRange, keysOnly bool, count *uint32, visit func(genericRecordRow) error) error {
	q := func(name string) string { return quoteIdent(s.dialect, name) }
	blob := q("record_blob")
	if keysOnly {
		blob = "NULL"
	}
	base := "SELECT " + q("pk_hash") + ", " + q("pk_bytes") + ", " + blob + ", " + q("pk_ord") + " FROM " + quoteTableName(s.dialect, s.genericRecordsTable()) + " WHERE " + q("store_name") + " = ?"
	args := []any{store}
	order := q("pk_hash")
	orderedKey := primaryOrderExpression(s.dialect, q("pk_ord"))
	orderParam := primaryOrderExpression(s.dialect, "?")
	prefixOrder := mode == "ordered" && orderedKey != q("pk_ord")
	if mode == "ordered" {
		base += " AND " + q("pk_ord") + " IS NOT NULL"
		if bounds.lo != nil || bounds.hi != nil {
			predicate, values := genericIndexRangePredicate(q("pk_ord"), bounds)
			base += " AND " + predicate
			args = append(args, values...)
		}
		order = orderedKey + ", " + q("pk_hash")
	}
	var last genericRecordRow
	read := uint64(0)
	for {
		limit := genericSQLPageSize
		finishPrefix := false
		if count != nil {
			if read >= uint64(*count) {
				if !prefixOrder || *count == 0 {
					return nil
				}
				finishPrefix = true
			} else {
				limit = min(limit, int(uint64(*count)-read))
			}
		}
		stmt := base
		pageArgs := append([]any(nil), args...)
		if last.pkHash != nil {
			if mode == "ordered" {
				if finishPrefix {
					stmt += " AND (" + orderedKey + " = " + orderParam + " AND " + q("pk_hash") + " > ?)"
					pageArgs = append(pageArgs, last.pkOrd, last.pkHash)
				} else {
					stmt += " AND (" + orderedKey + " > " + orderParam + " OR (" + orderedKey + " = " + orderParam + " AND " + q("pk_hash") + " > ?))"
					pageArgs = append(pageArgs, last.pkOrd, last.pkOrd, last.pkHash)
				}
			} else {
				stmt += " AND " + q("pk_hash") + " > ?"
				pageArgs = append(pageArgs, last.pkHash)
			}
		}
		rows, err := s.query(ctx, sqlPageLimit(s.dialect, stmt+" ORDER BY "+order, limit), pageArgs...)
		if err != nil {
			return fmt.Errorf("load primary-key page: %w", err)
		}
		// Close each SQL result before invoking callbacks; a callback may write.
		page := make([]genericRecordRow, 0, limit)
		for rows.Next() {
			var row genericRecordRow
			if err := rows.Scan(&row.pkHash, &row.pkBytes, &row.recordBlob, &row.pkOrd); err != nil {
				rows.Close()
				return err
			}
			page = append(page, row)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, row := range page {
			if err := visit(row); err != nil {
				return err
			}
		}
		read += uint64(len(page))
		if len(page) < limit {
			return nil
		}
		last = page[len(page)-1]
	}
}
