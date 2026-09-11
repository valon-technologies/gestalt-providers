package relationaldb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"strings"

	cursorutil "github.com/valon-technologies/gestalt-providers/indexeddb/internal/cursorutil"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/client"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type genericRecordRow struct {
	pkHash     []byte
	pkBytes    []byte
	recordBlob []byte
}

const (
	genericRecordPKBatchSize = 500
	// Each range uses up to two parameters; 400 stays below portable SQL limits.
	genericIndexRangeBatchSize = 400
)

type genericIndexRow struct {
	indexName     string
	indexKeyHash  []byte
	indexKeyBytes []byte
	indexKeyOrd   []byte
	pkHash        []byte
	pkBytes       []byte
}

type genericIndexRange struct {
	lo, hi         []byte
	loOpen, hiOpen bool
}

type encodedKey struct {
	value any
	raw   []byte
	hash  []byte
	ord   []byte
}

func encodeKeyValue(value any) (encodedKey, error) {
	raw, err := indexeddb.EncodeIndexedDBKey(value)
	if err != nil {
		return encodedKey{}, status.Errorf(codes.InvalidArgument, "encode key: %v", err)
	}
	ord, err := encodeOrderedKey(value)
	if err != nil {
		return encodedKey{}, status.Errorf(codes.InvalidArgument, "encode ordered key: %v", err)
	}
	sum := sha256.Sum256(raw)
	return encodedKey{
		value: value,
		raw:   raw,
		hash:  append([]byte(nil), sum[:]...),
		ord:   ord,
	}, nil
}

func decodeKeyValue(raw []byte) (any, error) {
	value, err := indexeddb.DecodeIndexedDBKey(raw)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "decode key value: %v", err)
	}
	return value, nil
}

func cloneBytes(raw []byte) []byte {
	return append([]byte(nil), raw...)
}

func extractGenericPrimaryKey(record gestalt.Record, m *storeMeta) (encodedKey, error) {
	if len(m.columns) == 0 {
		id, err := extractStringID(record)
		if err != nil {
			return encodedKey{}, err
		}
		return encodeKeyValue(id)
	}
	value, err := extractPrimaryKeyValue(record, m.pkCol)
	if err != nil {
		return encodedKey{}, status.Errorf(codes.InvalidArgument, "record id: %v", err)
	}
	return encodeKeyValue(value)
}

func coerceStringPrimaryKey(raw string, m *storeMeta) (any, error) {
	if len(m.columns) == 0 {
		return raw, nil
	}
	return coerceStoredValue(raw, columnType(m, m.pkCol))
}

func coerceStoredValue(value any, colType int32) (any, error) {
	switch colType {
	case 1:
		return toInt64(value)
	case 2:
		return toFloat64(value)
	case 3:
		return toBool(value)
	case 4:
		return coerceSQLTime(value)
	case 5:
		switch v := value.(type) {
		case []byte:
			return append([]byte(nil), v...), nil
		case string:
			return parseStoredBytes(v), nil
		default:
			return nil, fmt.Errorf("expected []byte or string, got %T", value)
		}
	default:
		return fmt.Sprint(value), nil
	}
}

func buildGenericIndexRows(record gestalt.Record, m *storeMeta, primary encodedKey) ([]genericIndexRow, []genericIndexRow, error) {
	uniqueRows := make([]genericIndexRow, 0, len(m.indexes))
	nonUniqueRows := make([]genericIndexRow, 0, len(m.indexes))
	for i := range m.indexes {
		idx := &m.indexes[i]
		key, ok, err := indexKeyFromRecord(record, idx)
		if err != nil {
			return nil, nil, status.Errorf(codes.InvalidArgument, "record index key: %v", err)
		}
		if !ok {
			continue
		}
		encoded, err := encodeKeyValue(key)
		if err != nil {
			return nil, nil, err
		}
		row := genericIndexRow{
			indexName:     idx.Name,
			indexKeyHash:  cloneBytes(encoded.hash),
			indexKeyBytes: cloneBytes(encoded.raw),
			indexKeyOrd:   cloneBytes(encoded.ord),
			pkHash:        cloneBytes(primary.hash),
			pkBytes:       cloneBytes(primary.raw),
		}
		if idx.Unique {
			uniqueRows = append(uniqueRows, row)
		} else {
			nonUniqueRows = append(nonUniqueRows, row)
		}
	}
	return uniqueRows, nonUniqueRows, nil
}

func (s *Store) loadGenericRecordByHash(ctx context.Context, tx *sql.Tx, store string, hash []byte) (*genericRecordRow, error) {
	return s.loadGenericRecordByHashWithLock(ctx, tx, store, hash, false)
}

func (s *Store) loadGenericRecordByHashForUpdate(ctx context.Context, tx *sql.Tx, store string, hash []byte) (*genericRecordRow, error) {
	return s.loadGenericRecordByHashWithLock(ctx, tx, store, hash, true)
}

func (s *Store) loadGenericRecordByHashWithLock(ctx context.Context, tx *sql.Tx, store string, hash []byte, forUpdate bool) (*genericRecordRow, error) {
	table := quoteTableName(s.dialect, s.genericRecordsTable())
	if forUpdate && s.dialect == dialectSQLServer {
		table += " WITH (UPDLOCK, HOLDLOCK)"
	}
	query := "SELECT " +
		quoteIdent(s.dialect, "pk_hash") + ", " +
		quoteIdent(s.dialect, "pk_bytes") + ", " +
		quoteIdent(s.dialect, "record_blob") +
		" FROM " + table +
		" WHERE " + quoteIdent(s.dialect, "store_name") + " = ? AND " + quoteIdent(s.dialect, "pk_hash") + " = ?"
	if forUpdate && s.dialect != dialectSQLite && s.dialect != dialectSQLServer {
		query += " FOR UPDATE"
	}
	row := tx.QueryRowContext(ctx, s.q(query), store, hash)
	var out genericRecordRow
	if err := row.Scan(&out.pkHash, &out.pkBytes, &out.recordBlob); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, status.Errorf(codes.Internal, "load record: %v", err)
	}
	return &out, nil
}

func (s *Store) loadGenericRecordByHashDirect(ctx context.Context, store string, hash []byte) (*genericRecordRow, error) {
	query := "SELECT " +
		quoteIdent(s.dialect, "pk_hash") + ", " +
		quoteIdent(s.dialect, "pk_bytes") + ", " +
		quoteIdent(s.dialect, "record_blob") +
		" FROM " + quoteTableName(s.dialect, s.genericRecordsTable()) +
		" WHERE " + quoteIdent(s.dialect, "store_name") + " = ? AND " + quoteIdent(s.dialect, "pk_hash") + " = ?"
	rows, err := s.query(ctx, query, store, hash)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load record: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, status.Errorf(codes.Internal, "iterate record lookup: %v", err)
		}
		return nil, nil
	}
	var out genericRecordRow
	if err := rows.Scan(&out.pkHash, &out.pkBytes, &out.recordBlob); err != nil {
		return nil, status.Errorf(codes.Internal, "scan record lookup: %v", err)
	}
	return &out, nil
}

func (s *Store) loadGenericRecordByPrimaryDirect(ctx context.Context, store string, hash, primary []byte) (*genericRecordRow, error) {
	query := "SELECT " +
		quoteIdent(s.dialect, "pk_hash") + ", " +
		quoteIdent(s.dialect, "pk_bytes") + ", " +
		quoteIdent(s.dialect, "record_blob") +
		" FROM " + quoteTableName(s.dialect, s.genericRecordsTable()) +
		" WHERE " + quoteIdent(s.dialect, "store_name") + " = ? AND " + quoteIdent(s.dialect, "pk_hash") + " = ? AND " + quoteIdent(s.dialect, "pk_bytes") + " = ?"
	rows, err := s.query(ctx, query, store, hash, primary)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load record: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, status.Errorf(codes.Internal, "iterate record lookup: %v", err)
		}
		return nil, nil
	}
	var out genericRecordRow
	if err := rows.Scan(&out.pkHash, &out.pkBytes, &out.recordBlob); err != nil {
		return nil, status.Errorf(codes.Internal, "scan record lookup: %v", err)
	}
	return &out, nil
}

func (s *Store) loadAllGenericRecords(ctx context.Context, store string) ([]genericRecordRow, error) {
	rows, err := s.query(ctx,
		"SELECT "+quoteIdent(s.dialect, "pk_hash")+", "+quoteIdent(s.dialect, "pk_bytes")+", "+quoteIdent(s.dialect, "record_blob")+
			" FROM "+quoteTableName(s.dialect, s.genericRecordsTable())+
			" WHERE "+quoteIdent(s.dialect, "store_name")+" = ?",
		store,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load records: %v", err)
	}
	defer rows.Close()

	var out []genericRecordRow
	for rows.Next() {
		var row genericRecordRow
		if err := rows.Scan(&row.pkHash, &row.pkBytes, &row.recordBlob); err != nil {
			return nil, status.Errorf(codes.Internal, "scan records: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate records: %v", err)
	}
	return out, nil
}

func (s *Store) countGenericRecords(ctx context.Context, store string) (int64, error) {
	var count int64
	if err := s.scanOne(ctx,
		"SELECT COUNT(*) FROM "+quoteTableName(s.dialect, s.genericRecordsTable())+
			" WHERE "+quoteIdent(s.dialect, "store_name")+" = ?",
		[]any{store},
		&count,
	); err != nil {
		return 0, status.Errorf(codes.Internal, "count records: %v", err)
	}
	return count, nil
}

func scanGenericIndexRow(scanner interface {
	Scan(dest ...any) error
}) (genericIndexRow, error) {
	var row genericIndexRow
	if err := scanner.Scan(&row.indexName, &row.indexKeyHash, &row.indexKeyBytes, &row.indexKeyOrd, &row.pkHash, &row.pkBytes); err != nil {
		return genericIndexRow{}, err
	}
	return row, nil
}

func (s *Store) loadGenericIndexRowsByRange(ctx context.Context, table, store, index string, lo, hi []byte, loOpen, hiOpen bool) ([]genericIndexRow, error) {
	return s.loadGenericIndexRowsByRanges(ctx, table, store, index, []genericIndexRange{{
		lo: lo, hi: hi, loOpen: loOpen, hiOpen: hiOpen,
	}})
}

func (s *Store) loadGenericIndexRowsByRanges(ctx context.Context, table, store, index string, ranges []genericIndexRange) ([]genericIndexRow, error) {
	if len(ranges) <= genericIndexRangeBatchSize {
		var out []genericIndexRow
		err := s.scanGenericIndexRowsByRanges(ctx, table, store, index, ranges, func(row genericIndexRow) error {
			out = append(out, row)
			return nil
		})
		return out, err
	}

	var out []genericIndexRow
	seen := make(map[[2]string]struct{})
	for start := 0; start < len(ranges); start += genericIndexRangeBatchSize {
		end := min(start+genericIndexRangeBatchSize, len(ranges))
		err := s.scanGenericIndexRowsByRanges(ctx, table, store, index, ranges[start:end], func(row genericIndexRow) error {
			key := [2]string{string(row.indexKeyBytes), string(row.pkBytes)}
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				out = append(out, row)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *Store) scanGenericIndexRowsByRange(ctx context.Context, table, store, index string, lo, hi []byte, loOpen, hiOpen bool, visit func(genericIndexRow) error) error {
	return s.scanGenericIndexRowsByRanges(ctx, table, store, index, []genericIndexRange{{
		lo: lo, hi: hi, loOpen: loOpen, hiOpen: hiOpen,
	}}, visit)
}

func (s *Store) scanGenericIndexRowsByRanges(ctx context.Context, table, store, index string, ranges []genericIndexRange, visit func(genericIndexRow) error) error {
	var query strings.Builder
	query.WriteString("SELECT ")
	query.WriteString(quoteIdent(s.dialect, "index_name"))
	query.WriteString(", ")
	query.WriteString(quoteIdent(s.dialect, "index_key_hash"))
	query.WriteString(", ")
	query.WriteString(quoteIdent(s.dialect, "index_key_bytes"))
	query.WriteString(", ")
	query.WriteString(quoteIdent(s.dialect, "index_key_ord"))
	query.WriteString(", ")
	query.WriteString(quoteIdent(s.dialect, "pk_hash"))
	query.WriteString(", ")
	query.WriteString(quoteIdent(s.dialect, "pk_bytes"))
	query.WriteString(" FROM ")
	query.WriteString(quoteTableName(s.dialect, table))
	query.WriteString(" WHERE ")
	query.WriteString(quoteIdent(s.dialect, "store_name"))
	query.WriteString(" = ? AND ")
	query.WriteString(quoteIdent(s.dialect, "index_name"))
	query.WriteString(" = ?")

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
			var predicateArgs []any
			predicates[i], predicateArgs = genericIndexRangePredicate(quoteIdent(s.dialect, "index_key_ord"), r)
			args = append(args, predicateArgs...)
		}
		query.WriteString(" AND (" + strings.Join(predicates, " OR ") + ")")
	}
	rows, err := s.query(ctx, query.String(), args...)
	if err != nil {
		return status.Errorf(codes.Internal, "load index rows by range: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		row, err := scanGenericIndexRow(rows)
		if err != nil {
			return status.Errorf(codes.Internal, "scan index rows by range: %v", err)
		}
		if err := visit(row); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return status.Errorf(codes.Internal, "iterate index rows by range: %v", err)
	}
	return nil
}

func genericIndexRangePredicate(column string, r genericIndexRange) (string, []any) {
	if bytes.Equal(r.lo, r.hi) && !r.loOpen && !r.hiOpen {
		return "(" + column + " = ?)", []any{r.lo}
	}
	predicates := make([]string, 0, 2)
	args := make([]any, 0, 2)
	if r.lo != nil {
		op := " >= ?"
		if r.loOpen {
			op = " > ?"
		}
		predicates = append(predicates, column+op)
		args = append(args, r.lo)
	}
	if r.hi != nil {
		op := " <= ?"
		if r.hiOpen {
			op = " < ?"
		}
		predicates = append(predicates, column+op)
		args = append(args, r.hi)
	}
	return "(" + strings.Join(predicates, " AND ") + ")", args
}

func (s *Store) loadGenericRecordRowsByPKHashes(ctx context.Context, store string, hashes [][]byte, includeBlob bool) (map[string]genericRecordRow, error) {
	if len(hashes) == 0 {
		return map[string]genericRecordRow{}, nil
	}
	unique := make([][]byte, 0, len(hashes))
	seen := make(map[string]struct{}, len(hashes))
	for _, hash := range hashes {
		key := string(hash)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, hash)
	}

	out := make(map[string]genericRecordRow, len(unique))
	for start := 0; start < len(unique); start += genericRecordPKBatchSize {
		end := start + genericRecordPKBatchSize
		if end > len(unique) {
			end = len(unique)
		}
		batch := unique[start:end]

		var query strings.Builder
		query.WriteString("SELECT ")
		query.WriteString(quoteIdent(s.dialect, "pk_hash"))
		query.WriteString(", ")
		query.WriteString(quoteIdent(s.dialect, "pk_bytes"))
		if includeBlob {
			query.WriteString(", ")
			query.WriteString(quoteIdent(s.dialect, "record_blob"))
		}
		query.WriteString(" FROM ")
		query.WriteString(quoteTableName(s.dialect, s.genericRecordsTable()))
		query.WriteString(" WHERE ")
		query.WriteString(quoteIdent(s.dialect, "store_name"))
		query.WriteString(" = ? AND ")
		query.WriteString(quoteIdent(s.dialect, "pk_hash"))
		query.WriteString(" IN (")
		args := make([]any, 0, len(batch)+1)
		args = append(args, store)
		for i, hash := range batch {
			if i > 0 {
				query.WriteString(", ")
			}
			query.WriteString("?")
			args = append(args, hash)
		}
		query.WriteString(")")

		rows, err := s.query(ctx, query.String(), args...)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "load records by pk hash: %v", err)
		}
		for rows.Next() {
			var row genericRecordRow
			dest := []any{&row.pkHash, &row.pkBytes}
			if includeBlob {
				dest = append(dest, &row.recordBlob)
			}
			if err := rows.Scan(dest...); err != nil {
				rows.Close()
				return nil, status.Errorf(codes.Internal, "scan records by pk hash: %v", err)
			}
			out[genericRecordLookupKey(row.pkHash, row.pkBytes)] = row
		}
		if err := rows.Close(); err != nil {
			return nil, status.Errorf(codes.Internal, "close records by pk hash: %v", err)
		}
		if err := rows.Err(); err != nil {
			return nil, status.Errorf(codes.Internal, "iterate records by pk hash: %v", err)
		}
	}
	return out, nil
}

func (s *Store) loadGenericIndexRows(ctx context.Context, table, store, index string) ([]genericIndexRow, error) {
	return s.loadGenericIndexRowsForQueries(ctx, table, store, index, nil)
}

func (s *Store) loadGenericIndexRowsForQueries(ctx context.Context, table, store, index string, queries []*client.IndexedDBQuery) ([]genericIndexRow, error) {
	ranges := make([]genericIndexRange, len(queries))
	for i, query := range queries {
		lo, hi, loOpen, hiOpen, err := orderedQueryBounds(query)
		if err != nil {
			return nil, err
		}
		ranges[i] = genericIndexRange{lo, hi, loOpen, hiOpen}
	}
	return s.loadGenericIndexRowsByRanges(ctx, table, store, index, ranges)
}

func orderedQueryBounds(query *client.IndexedDBQuery) (lo, hi []byte, loOpen, hiOpen bool, err error) {
	if exactKey, ok := queryExactKey(query); ok {
		key, err := encodeOrderedKey(exactKey)
		return key, key, false, false, err
	}
	kr, ok := queryKeyRange(query)
	if !ok {
		return nil, nil, false, false, status.Error(codes.InvalidArgument, "unsupported index query")
	}
	lo, hi, loOpen, hiOpen, err = orderedBounds(kr)
	if err != nil {
		err = status.Errorf(codes.InvalidArgument, "index range bounds: %v", err)
	}
	return
}

func (s *Store) deleteGenericIndexRowsByPrimaryKey(ctx context.Context, tx *sql.Tx, store string, pkHash []byte) error {
	statements := []string{
		"DELETE FROM " + quoteTableName(s.dialect, s.genericIndexTable()) +
			" WHERE " + quoteIdent(s.dialect, "store_name") + " = ? AND " + quoteIdent(s.dialect, "pk_hash") + " = ?",
		"DELETE FROM " + quoteTableName(s.dialect, s.genericUniqueIndexTable()) +
			" WHERE " + quoteIdent(s.dialect, "store_name") + " = ? AND " + quoteIdent(s.dialect, "pk_hash") + " = ?",
	}
	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, s.q(stmt), store, pkHash); err != nil {
			return status.Errorf(codes.Internal, "delete index rows: %v", err)
		}
	}
	return nil
}

func (s *Store) clearGenericStoreTables(ctx context.Context, tx *sql.Tx, store string) error {
	if err := s.clearGenericIndexTables(ctx, tx, store); err != nil {
		return err
	}
	statements := []string{
		"DELETE FROM " + quoteTableName(s.dialect, s.genericRecordsTable()) + " WHERE " + quoteIdent(s.dialect, "store_name") + " = ?",
	}
	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, s.q(stmt), store); err != nil {
			return status.Errorf(codes.Internal, "clear store: %v", err)
		}
	}
	return nil
}

func (s *Store) clearGenericIndexTables(ctx context.Context, tx *sql.Tx, store string) error {
	statements := []string{
		"DELETE FROM " + quoteTableName(s.dialect, s.genericIndexTable()) + " WHERE " + quoteIdent(s.dialect, "store_name") + " = ?",
		"DELETE FROM " + quoteTableName(s.dialect, s.genericUniqueIndexTable()) + " WHERE " + quoteIdent(s.dialect, "store_name") + " = ?",
	}
	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, s.q(stmt), store); err != nil {
			return status.Errorf(codes.Internal, "clear index rows: %v", err)
		}
	}
	return nil
}

func (s *Store) insertGenericUniqueIndexRow(ctx context.Context, tx *sql.Tx, store string, row genericIndexRow) error {
	stmt := "INSERT INTO " + quoteTableName(s.dialect, s.genericUniqueIndexTable()) +
		" (" + quoteIdent(s.dialect, "store_name") + ", " +
		quoteIdent(s.dialect, "index_name") + ", " +
		quoteIdent(s.dialect, "index_key_hash") + ", " +
		quoteIdent(s.dialect, "index_key_bytes") + ", " +
		quoteIdent(s.dialect, "index_key_ord") + ", " +
		quoteIdent(s.dialect, "pk_hash") + ", " +
		quoteIdent(s.dialect, "pk_bytes") + ") VALUES (?, ?, ?, ?, ?, ?, ?)"
	existingKeyBytes, existingPKBytes, found, err := s.loadExistingGenericUniqueIndexRow(ctx, tx, store, row.indexName, row.indexKeyHash)
	if err != nil {
		return err
	}
	if found {
		return classifyGenericUniqueIndexConflict(existingKeyBytes, existingPKBytes, row)
	}

	if s.dialect == dialectPostgres {
		return s.upsertPostgresGenericUniqueIndexRow(ctx, tx, store, row)
	}

	_, err = tx.ExecContext(ctx, s.q(stmt), store, row.indexName, row.indexKeyHash, row.indexKeyBytes, row.indexKeyOrd, row.pkHash, row.pkBytes)
	if err == nil {
		return nil
	}
	if !isDuplicateErr(err) {
		return status.Errorf(codes.Internal, "insert unique index row: %v", err)
	}

	existingKeyBytes, existingPKBytes, found, err = s.loadExistingGenericUniqueIndexRow(ctx, tx, store, row.indexName, row.indexKeyHash)
	if err != nil {
		return err
	}
	if !found {
		return status.Error(codes.AlreadyExists, "unique index conflict")
	}
	return classifyGenericUniqueIndexConflict(existingKeyBytes, existingPKBytes, row)
}

func (s *Store) upsertPostgresGenericUniqueIndexRow(ctx context.Context, tx *sql.Tx, store string, row genericIndexRow) error {
	table := quoteTableName(s.dialect, s.genericUniqueIndexTable())
	stmt := "INSERT INTO " + table +
		" (" + quoteIdent(s.dialect, "store_name") + ", " +
		quoteIdent(s.dialect, "index_name") + ", " +
		quoteIdent(s.dialect, "index_key_hash") + ", " +
		quoteIdent(s.dialect, "index_key_bytes") + ", " +
		quoteIdent(s.dialect, "index_key_ord") + ", " +
		quoteIdent(s.dialect, "pk_hash") + ", " +
		quoteIdent(s.dialect, "pk_bytes") + ") VALUES (?, ?, ?, ?, ?, ?, ?)" +
		" ON CONFLICT (" + quoteIdent(s.dialect, "store_name") + ", " +
		quoteIdent(s.dialect, "index_name") + ", " +
		quoteIdent(s.dialect, "index_key_hash") + ")" +
		" DO UPDATE SET " +
		quoteIdent(s.dialect, "index_key_bytes") + " = " + table + "." + quoteIdent(s.dialect, "index_key_bytes") + ", " +
		quoteIdent(s.dialect, "index_key_ord") + " = " + table + "." + quoteIdent(s.dialect, "index_key_ord") + ", " +
		quoteIdent(s.dialect, "pk_bytes") + " = " + table + "." + quoteIdent(s.dialect, "pk_bytes") +
		" RETURNING " + quoteIdent(s.dialect, "index_key_bytes") + ", " + quoteIdent(s.dialect, "pk_bytes")

	var existingKeyBytes []byte
	var existingPKBytes []byte
	if err := tx.QueryRowContext(ctx, s.q(stmt), store, row.indexName, row.indexKeyHash, row.indexKeyBytes, row.indexKeyOrd, row.pkHash, row.pkBytes).Scan(&existingKeyBytes, &existingPKBytes); err != nil {
		return status.Errorf(codes.Internal, "upsert unique index row: %v", err)
	}
	return classifyGenericUniqueIndexConflict(existingKeyBytes, existingPKBytes, row)
}

func (s *Store) loadExistingGenericUniqueIndexRow(ctx context.Context, tx *sql.Tx, store, indexName string, indexKeyHash []byte) ([]byte, []byte, bool, error) {
	query := "SELECT " + quoteIdent(s.dialect, "index_key_bytes") + ", " + quoteIdent(s.dialect, "pk_bytes") +
		" FROM " + quoteTableName(s.dialect, s.genericUniqueIndexTable()) +
		" WHERE " + quoteIdent(s.dialect, "store_name") + " = ? AND " +
		quoteIdent(s.dialect, "index_name") + " = ? AND " +
		quoteIdent(s.dialect, "index_key_hash") + " = ?"
	var existingKeyBytes []byte
	var existingPKBytes []byte
	if scanErr := tx.QueryRowContext(ctx, s.q(query), store, indexName, indexKeyHash).Scan(&existingKeyBytes, &existingPKBytes); scanErr != nil {
		if scanErr == sql.ErrNoRows {
			return nil, nil, false, nil
		}
		return nil, nil, false, status.Errorf(codes.Internal, "load conflicting unique index row: %v", scanErr)
	}
	return existingKeyBytes, existingPKBytes, true, nil
}

func classifyGenericUniqueIndexConflict(existingKeyBytes, existingPKBytes []byte, row genericIndexRow) error {
	if !bytes.Equal(existingKeyBytes, row.indexKeyBytes) {
		return status.Error(codes.Internal, "unique index hash collision")
	}
	if bytes.Equal(existingPKBytes, row.pkBytes) {
		return nil
	}
	return status.Error(codes.AlreadyExists, "unique index conflict")
}

func (s *Store) insertGenericIndexRows(ctx context.Context, tx *sql.Tx, table, store string, rows []genericIndexRow) error {
	if len(rows) == 0 {
		return nil
	}
	stmt := "INSERT INTO " + quoteTableName(s.dialect, table) +
		" (" + quoteIdent(s.dialect, "store_name") + ", " +
		quoteIdent(s.dialect, "index_name") + ", " +
		quoteIdent(s.dialect, "index_key_hash") + ", " +
		quoteIdent(s.dialect, "index_key_bytes") + ", " +
		quoteIdent(s.dialect, "index_key_ord") + ", " +
		quoteIdent(s.dialect, "pk_hash") + ", " +
		quoteIdent(s.dialect, "pk_bytes") + ") VALUES (?, ?, ?, ?, ?, ?, ?)"
	for _, row := range rows {
		if _, err := tx.ExecContext(ctx, s.q(stmt), store, row.indexName, row.indexKeyHash, row.indexKeyBytes, row.indexKeyOrd, row.pkHash, row.pkBytes); err != nil {
			return status.Errorf(codes.Internal, "insert index row: %v", err)
		}
	}
	return nil
}

func (s *Store) insertGenericRecord(ctx context.Context, tx *sql.Tx, store string, primary encodedKey, payload []byte) error {
	if s.dialect == dialectPostgres {
		return s.insertPostgresGenericRecord(ctx, tx, store, primary, payload)
	}
	stmt := "INSERT INTO " + quoteTableName(s.dialect, s.genericRecordsTable()) +
		" (" + quoteIdent(s.dialect, "store_name") + ", " +
		quoteIdent(s.dialect, "pk_hash") + ", " +
		quoteIdent(s.dialect, "pk_bytes") + ", " +
		quoteIdent(s.dialect, "record_blob") + ") VALUES (?, ?, ?, ?)"
	_, err := tx.ExecContext(ctx, s.q(stmt), store, primary.hash, primary.raw, payload)
	if err == nil {
		return nil
	}
	if !isDuplicateErr(err) {
		return status.Errorf(codes.Internal, "insert record: %v", err)
	}
	existing, loadErr := s.loadGenericRecordByHash(ctx, tx, store, primary.hash)
	if loadErr != nil {
		return loadErr
	}
	return classifyGenericRecordInsertConflict(existing, primary)
}

func (s *Store) insertPostgresGenericRecord(ctx context.Context, tx *sql.Tx, store string, primary encodedKey, payload []byte) error {
	stmt := "INSERT INTO " + quoteTableName(s.dialect, s.genericRecordsTable()) +
		" (" + quoteIdent(s.dialect, "store_name") + ", " +
		quoteIdent(s.dialect, "pk_hash") + ", " +
		quoteIdent(s.dialect, "pk_bytes") + ", " +
		quoteIdent(s.dialect, "record_blob") + ") VALUES (?, ?, ?, ?)" +
		" ON CONFLICT (" + quoteIdent(s.dialect, "store_name") + ", " + quoteIdent(s.dialect, "pk_hash") + ") DO NOTHING"
	result, err := tx.ExecContext(ctx, s.q(stmt), store, primary.hash, primary.raw, payload)
	if err != nil {
		return status.Errorf(codes.Internal, "insert record: %v", err)
	}
	rows, _ := result.RowsAffected()
	if rows > 0 {
		return nil
	}
	existing, loadErr := s.loadGenericRecordByHash(ctx, tx, store, primary.hash)
	if loadErr != nil {
		return loadErr
	}
	return classifyGenericRecordInsertConflict(existing, primary)
}

func classifyGenericRecordInsertConflict(existing *genericRecordRow, primary encodedKey) error {
	if existing == nil {
		// A concurrent insert may win the unique-key race without being visible in
		// this transaction snapshot. Surface that as a normal duplicate so callers
		// can retry and observe the committed row in a fresh transaction.
		return status.Error(codes.AlreadyExists, "already exists")
	}
	if bytes.Equal(existing.pkBytes, primary.raw) {
		return status.Error(codes.AlreadyExists, "already exists")
	}
	return status.Error(codes.Internal, "primary key hash collision")
}

func (s *Store) upsertGenericRecord(ctx context.Context, tx *sql.Tx, store string, primary encodedKey, payload []byte, existing *genericRecordRow) error {
	if existing == nil {
		var err error
		existing, err = s.loadGenericRecordByHash(ctx, tx, store, primary.hash)
		if err != nil {
			return err
		}
	}
	if existing != nil && !bytes.Equal(existing.pkBytes, primary.raw) {
		return status.Error(codes.Internal, "primary key hash collision")
	}

	updateStmt := "UPDATE " + quoteTableName(s.dialect, s.genericRecordsTable()) +
		" SET " + quoteIdent(s.dialect, "pk_bytes") + " = ?, " + quoteIdent(s.dialect, "record_blob") + " = ?" +
		" WHERE " + quoteIdent(s.dialect, "store_name") + " = ? AND " + quoteIdent(s.dialect, "pk_hash") + " = ?"
	if existing != nil {
		result, err := tx.ExecContext(ctx, s.q(updateStmt), primary.raw, payload, store, primary.hash)
		if err != nil {
			return status.Errorf(codes.Internal, "update record: %v", err)
		}
		if rows, rowsErr := result.RowsAffected(); rowsErr == nil && rows == 0 {
			current, loadErr := s.loadGenericRecordByHashForUpdate(ctx, tx, store, primary.hash)
			if loadErr != nil {
				return loadErr
			}
			if current == nil {
				return status.Error(codes.Aborted, "record was deleted during update")
			}
			if !bytes.Equal(current.pkBytes, primary.raw) {
				return status.Error(codes.Internal, "primary key hash collision")
			}
		}
		return nil
	}
	return s.insertGenericRecord(ctx, tx, store, primary, payload)
}

func (s *Store) addGeneric(ctx context.Context, store string, m *storeMeta, record gestalt.Record) error {
	primary, err := extractGenericPrimaryKey(record, m)
	if err != nil {
		return err
	}
	payload, err := marshalRecordBlob(record)
	if err != nil {
		return err
	}
	uniqueRows, nonUniqueRows, err := buildGenericIndexRows(record, m, primary)
	if err != nil {
		return err
	}
	return s.withTx(ctx, func(txCtx context.Context, tx *sql.Tx) error {
		if err := s.insertGenericRecord(txCtx, tx, store, primary, payload); err != nil {
			return err
		}
		for _, row := range uniqueRows {
			if err := s.insertGenericUniqueIndexRow(txCtx, tx, store, row); err != nil {
				return err
			}
		}
		return s.insertGenericIndexRows(txCtx, tx, s.genericIndexTable(), store, nonUniqueRows)
	})
}

func (s *Store) putGeneric(ctx context.Context, store string, m *storeMeta, record gestalt.Record) error {
	primary, err := extractGenericPrimaryKey(record, m)
	if err != nil {
		return err
	}
	payload, err := marshalRecordBlob(record)
	if err != nil {
		return err
	}
	uniqueRows, nonUniqueRows, err := buildGenericIndexRows(record, m, primary)
	if err != nil {
		return err
	}
	return s.withTx(ctx, func(txCtx context.Context, tx *sql.Tx) error {
		existing, err := s.loadGenericRecordByHashForUpdate(txCtx, tx, store, primary.hash)
		if err != nil {
			return err
		}
		if existing != nil && !bytes.Equal(existing.pkBytes, primary.raw) {
			return status.Error(codes.Internal, "primary key hash collision")
		}
		if err := s.deleteGenericIndexRowsByPrimaryKey(txCtx, tx, store, primary.hash); err != nil {
			return err
		}
		for _, row := range uniqueRows {
			if err := s.insertGenericUniqueIndexRow(txCtx, tx, store, row); err != nil {
				return err
			}
		}
		if err := s.insertGenericIndexRows(txCtx, tx, s.genericIndexTable(), store, nonUniqueRows); err != nil {
			return err
		}
		return s.upsertGenericRecord(txCtx, tx, store, primary, payload, existing)
	})
}

func (s *Store) deleteGeneric(ctx context.Context, store string, m *storeMeta, rawKey string) error {
	value, err := coerceStringPrimaryKey(rawKey, m)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "marshal primary key: %v", err)
	}
	return s.deleteGenericByValue(ctx, store, value)
}

func (s *Store) deleteGenericByValue(ctx context.Context, store string, value any) error {
	primary, err := encodeKeyValue(value)
	if err != nil {
		return err
	}
	return s.withTx(ctx, func(txCtx context.Context, tx *sql.Tx) error {
		existing, err := s.loadGenericRecordByHashForUpdate(txCtx, tx, store, primary.hash)
		if err != nil {
			return err
		}
		if existing != nil && !bytes.Equal(existing.pkBytes, primary.raw) {
			return status.Error(codes.Internal, "primary key hash collision")
		}
		if err := s.deleteGenericIndexRowsByPrimaryKey(txCtx, tx, store, primary.hash); err != nil {
			return err
		}
		if existing == nil {
			return nil
		}
		stmt := "DELETE FROM " + quoteTableName(s.dialect, s.genericRecordsTable()) +
			" WHERE " + quoteIdent(s.dialect, "store_name") + " = ? AND " + quoteIdent(s.dialect, "pk_hash") + " = ?"
		if _, err := tx.ExecContext(txCtx, s.q(stmt), store, primary.hash); err != nil {
			return status.Errorf(codes.Internal, "delete record: %v", err)
		}
		return nil
	})
}

func (s *Store) clearGeneric(ctx context.Context, store string) error {
	return s.withTx(ctx, func(txCtx context.Context, tx *sql.Tx) error {
		return s.clearGenericStoreTables(txCtx, tx, store)
	})
}

func (s *Store) deleteGenericEntries(ctx context.Context, store string, entries []cursorutil.Entry) (int64, error) {
	seen := make(map[string]struct{}, len(entries))
	var deleted int64
	err := s.withTx(ctx, func(txCtx context.Context, tx *sql.Tx) error {
		for _, entry := range entries {
			primary, err := encodeKeyValue(entry.PrimaryKeyValue)
			if err != nil {
				return err
			}
			seenKey := string(primary.hash) + string(primary.raw)
			if _, ok := seen[seenKey]; ok {
				continue
			}
			seen[seenKey] = struct{}{}

			existing, err := s.loadGenericRecordByHashForUpdate(txCtx, tx, store, primary.hash)
			if err != nil {
				return err
			}
			if existing != nil && !bytes.Equal(existing.pkBytes, primary.raw) {
				return status.Error(codes.Internal, "primary key hash collision")
			}
			if err := s.deleteGenericIndexRowsByPrimaryKey(txCtx, tx, store, primary.hash); err != nil {
				return err
			}
			if existing == nil {
				continue
			}
			stmt := "DELETE FROM " + quoteTableName(s.dialect, s.genericRecordsTable()) +
				" WHERE " + quoteIdent(s.dialect, "store_name") + " = ? AND " + quoteIdent(s.dialect, "pk_hash") + " = ?"
			result, err := tx.ExecContext(txCtx, s.q(stmt), store, primary.hash)
			if err != nil {
				return status.Errorf(codes.Internal, "delete record: %v", err)
			}
			rowsDeleted, _ := result.RowsAffected()
			deleted += rowsDeleted
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return deleted, nil
}

func (s *Store) genericGet(ctx context.Context, store string, m *storeMeta, rawKey string) (gestalt.Record, error) {
	value, err := coerceStringPrimaryKey(rawKey, m)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "lookup key: %v", err)
	}
	primary, err := encodeKeyValue(value)
	if err != nil {
		return nil, err
	}
	row, err := s.loadGenericRecordByHashDirect(ctx, store, primary.hash)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, status.Error(codes.NotFound, "not found")
	}
	if !bytes.Equal(row.pkBytes, primary.raw) {
		return nil, status.Error(codes.Internal, "primary key hash collision")
	}
	return unmarshalRecordBlob(row.recordBlob)
}

func (s *Store) genericObjectStoreEntries(ctx context.Context, store string, m *storeMeta, query *client.IndexedDBQuery, keysOnly bool) ([]cursorutil.Entry, error) {
	rows, err := s.loadAllGenericRecords(ctx, store)
	if err != nil {
		return nil, err
	}

	entries := make([]cursorutil.Entry, 0, len(rows))
	for _, row := range rows {
		primaryKeyValue, err := decodeKeyValue(row.pkBytes)
		if err != nil {
			return nil, err
		}
		var record gestalt.Record
		if !keysOnly {
			record, err = unmarshalRecordBlob(row.recordBlob)
			if err != nil {
				return nil, err
			}
		}
		entries = append(entries, cursorutil.Entry{
			Key:             primaryKeyValue,
			PrimaryKey:      fmt.Sprint(primaryKeyValue),
			PrimaryKeyValue: primaryKeyValue,
			Record:          record,
		})
	}
	entries, err = filterEntriesByQuery(entries, query)
	if err != nil {
		return nil, err
	}
	sortObjectStoreEntries(entries)
	return entries, nil
}

func (s *Store) genericIndexEntries(ctx context.Context, store string, idx *gestalt.IndexSchema, queries []*client.IndexedDBQuery, keysOnly bool) ([]cursorutil.Entry, error) {
	table := s.genericIndexTable()
	if idx.Unique {
		table = s.genericUniqueIndexTable()
	}
	rows, err := s.loadGenericIndexRowsForQueries(ctx, table, store, idx.Name, queries)
	if err != nil {
		return nil, err
	}

	entries, err := s.indexEntriesFromRows(ctx, store, rows, keysOnly)
	if err != nil {
		return nil, err
	}
	entries, err = filterEntriesByQueries(entries, queries)
	if err != nil {
		return nil, err
	}
	sortIndexEntries(entries)
	return entries, nil
}

func (s *Store) indexEntriesFromRows(ctx context.Context, store string, rows []genericIndexRow, keysOnly bool) ([]cursorutil.Entry, error) {
	recordByPrimary := map[string]genericRecordRow{}
	if !keysOnly {
		hashes := make([][]byte, len(rows))
		for i, row := range rows {
			hashes[i] = row.pkHash
		}
		var err error
		recordByPrimary, err = s.loadGenericRecordRowsByPKHashes(ctx, store, hashes, true)
		if err != nil {
			return nil, err
		}
	}

	entries := make([]cursorutil.Entry, 0, len(rows))
	for _, row := range rows {
		indexKeyValue, err := decodeKeyValue(row.indexKeyBytes)
		if err != nil {
			return nil, err
		}
		primaryKeyValue, err := decodeKeyValue(row.pkBytes)
		if err != nil {
			return nil, err
		}
		entry := cursorutil.Entry{
			Key:             indexKeyValue,
			PrimaryKey:      fmt.Sprint(primaryKeyValue),
			PrimaryKeyValue: primaryKeyValue,
		}
		if !keysOnly {
			recordRow, ok := recordByPrimary[genericRecordLookupKey(row.pkHash, row.pkBytes)]
			if !ok {
				return nil, status.Error(codes.Internal, "index row points to missing record")
			}
			record, err := unmarshalRecordBlob(recordRow.recordBlob)
			if err != nil {
				return nil, err
			}
			entry.Record = record
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func genericRecordLookupKey(pkHash, pkBytes []byte) string {
	return string(pkHash) + ":" + string(pkBytes)
}
