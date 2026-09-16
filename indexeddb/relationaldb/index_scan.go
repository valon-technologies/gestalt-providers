package relationaldb

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Keep exact full-key predicates while giving prefix indexes a seek bound.
func orderedRangePredicate(d dialect, column string, r genericIndexRange) (string, []any) {
	predicate, args := genericIndexRangePredicate(column, r)
	prefix := primaryOrderExpression(d, column)
	if prefix == column {
		return predicate, args
	}
	parameter := primaryOrderExpression(d, "?")
	if r.lo != nil {
		predicate += " AND " + prefix + " >= " + parameter
		args = append(args, r.lo)
	}
	if r.hi != nil {
		predicate += " AND " + prefix + " <= " + parameter
		args = append(args, r.hi)
	}
	return predicate, args
}

func (s *Store) scanGenericIndexRowsByRanges(ctx context.Context, table, store, index string, ranges []genericIndexRange, count *uint32, visit func(genericIndexRow) error) error {
	q := func(name string) string { return quoteIdent(s.dialect, name) }
	base := "SELECT " + strings.Join([]string{q("index_name"), q("index_key_hash"), q("index_key_bytes"), q("index_key_ord"), q("pk_hash"), q("pk_bytes"), q("pk_ord")}, ", ") +
		" FROM " + quoteTableName(s.dialect, table) + " WHERE " + q("store_name") + " = ? AND " + q("index_name") + " = ?"
	args := []any{store, index}
	for _, r := range ranges {
		if r.lo == nil && r.hi == nil {
			ranges = nil
			break
		}
	}
	if len(ranges) > 0 {
		predicates := make([]string, len(ranges))
		for i, r := range ranges {
			predicate, values := orderedRangePredicate(s.dialect, q("index_key_ord"), r)
			predicates[i] = "(" + predicate + ")"
			args = append(args, values...)
		}
		base += " AND (" + strings.Join(predicates, " OR ") + ")"
	}
	indexOrder := primaryOrderExpression(s.dialect, q("index_key_ord"))
	primaryOrder := primaryOrderExpression(s.dialect, q("pk_ord"))
	parameter := primaryOrderExpression(s.dialect, "?")
	order := indexOrder + ", " + primaryOrder + ", " + q("pk_hash") + ", " + q("index_key_hash")
	var last *genericIndexRow
	read := uint64(0)
	for {
		stmt := base
		pageArgs := append([]any(nil), args...)
		limit := genericSQLPageSize
		if count != nil {
			if read < uint64(*count) {
				limit = min(limit, int(uint64(*count)-read))
			} else {
				if last == nil || indexOrder == q("index_key_ord") ||
					(len(last.indexKeyOrd) < orderedKeyIndexPrefixLen && len(last.pkOrd) < orderedKeyIndexPrefixLen) {
					return nil
				}
				// Only truncated boundary keys need completion before the caller
				// sorts full keys. Ordinary and duplicate short keys stop at count.
				stmt += " AND " + indexOrder + " = " + parameter
				pageArgs = append(pageArgs, last.indexKeyOrd)
				if len(last.indexKeyOrd) < orderedKeyIndexPrefixLen {
					stmt += " AND " + primaryOrder + " = " + parameter
					pageArgs = append(pageArgs, last.pkOrd)
				}
			}
		}
		if last != nil {
			stmt += " AND (" + indexOrder + " > " + parameter + " OR (" + indexOrder + " = " + parameter +
				" AND (" + primaryOrder + " > " + parameter + " OR (" + primaryOrder + " = " + parameter +
				" AND (" + q("pk_hash") + " > ? OR (" + q("pk_hash") + " = ? AND " + q("index_key_hash") + " > ?))))))"
			pageArgs = append(pageArgs, last.indexKeyOrd, last.indexKeyOrd, last.pkOrd, last.pkOrd, last.pkHash, last.pkHash, last.indexKeyHash)
		}
		rows, err := s.query(ctx, sqlPageLimit(s.dialect, stmt+" ORDER BY "+order, limit), pageArgs...)
		if err != nil {
			return status.Errorf(codes.Internal, "load index page: %v", err)
		}
		page := make([]genericIndexRow, 0, limit)
		for rows.Next() {
			row, err := scanGenericIndexRow(rows)
			if err != nil {
				rows.Close()
				return err
			}
			if row.pkOrd == nil {
				rows.Close()
				return status.Error(codes.FailedPrecondition, "ordered primary-key backfill is incomplete")
			}
			page = append(page, row)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if len(page) == 0 {
			return nil
		}
		// PostgreSQL sorts NULL last. Seek missing keys in the boundary
		// group before a limit or continuation can skip those legacy rows.
		check := "SELECT CASE WHEN " + q("pk_ord") + " IS NULL THEN 1 ELSE 0 END FROM " + quoteTableName(s.dialect, table) +
			" WHERE " + q("store_name") + " = ? AND " + q("index_name") + " = ? AND " + indexOrder + " = " + parameter +
			" ORDER BY " + nullsFirstOrder(s.dialect, primaryOrder, q("pk_hash"), q("index_key_hash"))
		check = "SELECT COALESCE((" + sqlPageLimit(s.dialect, check, 1) + "), 0)"
		var incomplete int
		if err := s.scanOne(ctx, check, []any{store, index, page[len(page)-1].indexKeyOrd}, &incomplete); err != nil {
			return err
		}
		if incomplete != 0 {
			return orderedPrimaryKeyUpgradeError()
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
		last = &page[len(page)-1]
	}
}
