package relationaldb

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"sort"

	"github.com/valon-technologies/gestalt/sdk/go/client"
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
	case dialectSQLServer:
		return createColumnsIndexSQL(d, table, name, []string{"store_name"}, false)
	default:
		return createColumnsIndexSQL(d, table, name, []string{"store_name", "pk_ord", "pk_hash"}, false)
	}
}

func sqlPageLimit(d dialect, query string, count int) string {
	if d == dialectSQLServer {
		return fmt.Sprintf("%s OFFSET 0 ROWS FETCH NEXT %d ROWS ONLY", query, count)
	}
	return fmt.Sprintf("%s LIMIT %d", query, count)
}

func (s *Store) countPrimaryRange(ctx context.Context, store string, query *client.IndexedDBQuery) (int64, error) {
	var count int64
	err := s.withPrimaryReadSnapshot(ctx, func(ctx context.Context) error {
		var err error
		count, err = s.countPrimaryRangeSnapshot(ctx, store, query)
		return err
	})
	return count, err
}

// A backfill must not move a row from the legacy set into the ordered set
// between the two reads. Reuse explicit IndexedDB transactions when present.
func (s *Store) withPrimaryReadSnapshot(ctx context.Context, read func(context.Context) error) error {
	if _, ok := txFromContext(ctx); ok {
		return read(ctx)
	}
	if err := s.checkLifecycle(ctx); err != nil {
		return err
	}
	isolation := sql.LevelRepeatableRead
	if s.dialect == dialectSQLite || s.dialect == dialectSQLServer {
		isolation = sql.LevelSerializable
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: isolation, ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := read(contextWithTx(ctx, tx, nil)); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) countPrimaryRangeSnapshot(ctx context.Context, store string, query *client.IndexedDBQuery) (int64, error) {
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
	rows, err := s.query(ctx, stmt, args...)
	if err != nil {
		return 0, err
	}
	var count int64
	if rows.Next() {
		err = rows.Scan(&count)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return 0, err
	}
	err = s.scanPrimaryRows(ctx, store, "legacy", bounds, true, nil, func(row genericRecordRow) error {
		key, err := decodeKeyValue(row.pkBytes)
		if err != nil {
			return err
		}
		ord, err := encodeOrderedKey(key)
		if err != nil {
			return err
		}
		if lo != nil && (bytes.Compare(ord, lo) < 0 || loOpen && bytes.Equal(ord, lo)) {
			return nil
		}
		if hi != nil && (bytes.Compare(ord, hi) > 0 || hiOpen && bytes.Equal(ord, hi)) {
			return nil
		}
		count++
		return nil
	})
	return count, err
}

// BackfillPrimaryKeyOrder fills legacy keys using short, independently committed
// updates. It is safe to restart: NULL is the checkpoint and concurrent writes
// win because the update also checks the original key bytes.
func BackfillPrimaryKeyOrder(ctx context.Context, dsn string, options Options) (int64, error) {
	s, err := openStoreWithOptions(ctx, dsn, options.storeOptions())
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

// During a rolling upgrade, old writers can still insert NULL pk_ord values.
// Merge those rows with the SQL-ordered page until the resumable backfill has
// caught up. Never omit legacy rows or limit them in hash order.
func (s *Store) loadPrimaryRows(ctx context.Context, store string, query *client.IndexedDBQuery, keysOnly bool, count *uint32, ordered bool) ([]genericRecordRow, error) {
	var rows []genericRecordRow
	err := s.withPrimaryReadSnapshot(ctx, func(ctx context.Context) error {
		var err error
		rows, err = s.loadPrimaryRowsSnapshot(ctx, store, query, keysOnly, count, ordered)
		return err
	})
	return rows, err
}

func (s *Store) loadPrimaryRowsSnapshot(ctx context.Context, store string, query *client.IndexedDBQuery, keysOnly bool, count *uint32, ordered bool) ([]genericRecordRow, error) {
	var bounds genericIndexRange
	if query != nil {
		lo, hi, loOpen, hiOpen, err := orderedQueryBounds(query)
		if err != nil {
			return nil, err
		}
		bounds = genericIndexRange{lo: lo, hi: hi, loOpen: loOpen, hiOpen: hiOpen}
	}
	var out []genericRecordRow
	collect := func(row genericRecordRow) error {
		if row.pkOrd == nil {
			key, err := decodeKeyValue(row.pkBytes)
			if err != nil {
				return err
			}
			row.pkOrd, err = encodeOrderedKey(key)
			if err != nil {
				return err
			}
		}
		if bounds.lo != nil && (bytes.Compare(row.pkOrd, bounds.lo) < 0 || bounds.loOpen && bytes.Equal(row.pkOrd, bounds.lo)) {
			return nil
		}
		if bounds.hi != nil && (bytes.Compare(row.pkOrd, bounds.hi) > 0 || bounds.hiOpen && bytes.Equal(row.pkOrd, bounds.hi)) {
			return nil
		}
		out = append(out, row)
		// Keep bounded memory for a limited request even with many legacy rows.
		if ordered && count != nil && len(out) >= int(*count)+genericSQLPageSize {
			sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].pkOrd, out[j].pkOrd) < 0 })
			out = out[:int(*count)]
		}
		return nil
	}
	if !ordered {
		err := s.scanPrimaryRows(ctx, store, "", bounds, keysOnly, nil, collect)
		return out, err
	}
	if err := s.scanPrimaryRows(ctx, store, "ordered", bounds, keysOnly, count, collect); err != nil {
		return nil, err
	}
	if err := s.scanPrimaryRows(ctx, store, "legacy", bounds, keysOnly, nil, collect); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].pkOrd, out[j].pkOrd) < 0 })
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
	orderedKey := q("pk_ord")
	if s.dialect == dialectMySQL {
		// A fixed-width binary expression avoids max_sort_length truncation
		// of LONGBLOBs. Finish the boundary prefix group before applying count.
		orderedKey = "CAST(" + orderedKey + " AS BINARY(255))"
	}
	if mode == "ordered" {
		base += " AND " + q("pk_ord") + " IS NOT NULL"
		if bounds.lo != nil || bounds.hi != nil {
			predicate, values := genericIndexRangePredicate(q("pk_ord"), bounds)
			base += " AND " + predicate
			args = append(args, values...)
		}
		order = orderedKey + ", " + q("pk_hash")
	} else if mode == "legacy" {
		base += " AND " + q("pk_ord") + " IS NULL"
	}
	var last genericRecordRow
	read := uint64(0)
	for {
		limit := genericSQLPageSize
		finishPrefix := false
		if count != nil {
			if read >= uint64(*count) {
				if s.dialect != dialectMySQL || mode != "ordered" || *count == 0 {
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
				lastOrder := last.pkOrd
				if s.dialect == dialectMySQL {
					lastOrder = make([]byte, 255)
					copy(lastOrder, last.pkOrd)
				}
				if finishPrefix {
					stmt += " AND (" + orderedKey + " = ? AND " + q("pk_hash") + " > ?)"
					pageArgs = append(pageArgs, lastOrder, last.pkHash)
				} else {
					stmt += " AND (" + orderedKey + " > ? OR (" + orderedKey + " = ? AND " + q("pk_hash") + " > ?))"
					pageArgs = append(pageArgs, lastOrder, lastOrder, last.pkHash)
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
