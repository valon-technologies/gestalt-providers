package relationaldb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"

	cursorutil "github.com/valon-technologies/gestalt-providers/indexeddb/internal/cursorutil"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	gproto "google.golang.org/protobuf/proto"
)

var deterministicProtoMarshal = gproto.MarshalOptions{Deterministic: true}

type genericRecordRow struct {
	pkHash     []byte
	pkBytes    []byte
	recordBlob []byte
}

type genericIndexRow struct {
	indexName     string
	indexKeyHash  []byte
	indexKeyBytes []byte
	pkHash        []byte
	pkBytes       []byte
}

type encodedKey struct {
	value any
	raw   []byte
	hash  []byte
}

func usesGenericStorage(m *storeMeta) bool {
	return m != nil && m.storageVersion == storageVersionGeneric
}

func encodeKeyValue(value any) (encodedKey, error) {
	kv, err := gestalt.AnyToKeyValue(value)
	if err != nil {
		return encodedKey{}, status.Errorf(codes.InvalidArgument, "encode key: %v", err)
	}
	raw, err := deterministicProtoMarshal.Marshal(kv)
	if err != nil {
		return encodedKey{}, status.Errorf(codes.Internal, "marshal key: %v", err)
	}
	sum := sha256.Sum256(raw)
	return encodedKey{
		value: value,
		raw:   raw,
		hash:  append([]byte(nil), sum[:]...),
	}, nil
}

func decodeKeyValue(raw []byte) (any, error) {
	kv := &proto.KeyValue{}
	if err := gproto.Unmarshal(raw, kv); err != nil {
		return nil, status.Errorf(codes.Internal, "decode key: %v", err)
	}
	value, err := gestalt.KeyValueToAny(kv)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "decode key value: %v", err)
	}
	return value, nil
}

func cloneBytes(raw []byte) []byte {
	return append([]byte(nil), raw...)
}

func extractGenericPrimaryKey(record *proto.Record, m *storeMeta) (encodedKey, error) {
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

func primaryKeyLookupArg(raw string, m *storeMeta) (any, error) {
	value, err := coerceStringPrimaryKey(raw, m)
	if err != nil {
		return nil, err
	}
	if usesGenericStorage(m) {
		return value, nil
	}
	return anyToSQLArg(value, columnType(m, m.pkCol))
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

func buildGenericIndexRows(record *proto.Record, m *storeMeta, primary encodedKey) ([]genericIndexRow, []genericIndexRow, error) {
	uniqueRows := make([]genericIndexRow, 0, len(m.indexes))
	nonUniqueRows := make([]genericIndexRow, 0, len(m.indexes))
	for _, idx := range m.indexes {
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
			indexName:     idx.GetName(),
			indexKeyHash:  cloneBytes(encoded.hash),
			indexKeyBytes: cloneBytes(encoded.raw),
			pkHash:        cloneBytes(primary.hash),
			pkBytes:       cloneBytes(primary.raw),
		}
		if idx.GetUnique() {
			uniqueRows = append(uniqueRows, row)
		} else {
			nonUniqueRows = append(nonUniqueRows, row)
		}
	}
	return uniqueRows, nonUniqueRows, nil
}

func (s *Store) loadGenericRecordByHash(ctx context.Context, tx *sql.Tx, store string, hash []byte) (*genericRecordRow, error) {
	query := "SELECT " +
		quoteIdent(s.dialect, "pk_hash") + ", " +
		quoteIdent(s.dialect, "pk_bytes") + ", " +
		quoteIdent(s.dialect, "record_blob") +
		" FROM " + quoteTableName(s.dialect, s.genericRecordsTable()) +
		" WHERE " + quoteIdent(s.dialect, "store_name") + " = ? AND " + quoteIdent(s.dialect, "pk_hash") + " = ?"
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

func (s *Store) loadGenericIndexRows(ctx context.Context, table, store, index string) ([]genericIndexRow, error) {
	rows, err := s.query(ctx,
		"SELECT "+quoteIdent(s.dialect, "index_name")+", "+quoteIdent(s.dialect, "index_key_hash")+", "+quoteIdent(s.dialect, "index_key_bytes")+", "+
			quoteIdent(s.dialect, "pk_hash")+", "+quoteIdent(s.dialect, "pk_bytes")+
			" FROM "+quoteTableName(s.dialect, table)+
			" WHERE "+quoteIdent(s.dialect, "store_name")+" = ? AND "+quoteIdent(s.dialect, "index_name")+" = ?",
		store,
		index,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load index rows: %v", err)
	}
	defer rows.Close()

	var out []genericIndexRow
	for rows.Next() {
		var row genericIndexRow
		if err := rows.Scan(&row.indexName, &row.indexKeyHash, &row.indexKeyBytes, &row.pkHash, &row.pkBytes); err != nil {
			return nil, status.Errorf(codes.Internal, "scan index rows: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate index rows: %v", err)
	}
	return out, nil
}

func (s *Store) loadGenericIndexRowsByHash(ctx context.Context, table, store, index string, hash []byte) ([]genericIndexRow, error) {
	rows, err := s.query(ctx,
		"SELECT "+quoteIdent(s.dialect, "index_name")+", "+quoteIdent(s.dialect, "index_key_hash")+", "+quoteIdent(s.dialect, "index_key_bytes")+", "+
			quoteIdent(s.dialect, "pk_hash")+", "+quoteIdent(s.dialect, "pk_bytes")+
			" FROM "+quoteTableName(s.dialect, table)+
			" WHERE "+quoteIdent(s.dialect, "store_name")+" = ? AND "+
			quoteIdent(s.dialect, "index_name")+" = ? AND "+
			quoteIdent(s.dialect, "index_key_hash")+" = ?",
		store,
		index,
		hash,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load index rows: %v", err)
	}
	defer rows.Close()

	var out []genericIndexRow
	for rows.Next() {
		var row genericIndexRow
		if err := rows.Scan(&row.indexName, &row.indexKeyHash, &row.indexKeyBytes, &row.pkHash, &row.pkBytes); err != nil {
			return nil, status.Errorf(codes.Internal, "scan index rows: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "iterate index rows: %v", err)
	}
	return out, nil
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
	statements := []string{
		"DELETE FROM " + quoteTableName(s.dialect, s.genericIndexTable()) + " WHERE " + quoteIdent(s.dialect, "store_name") + " = ?",
		"DELETE FROM " + quoteTableName(s.dialect, s.genericUniqueIndexTable()) + " WHERE " + quoteIdent(s.dialect, "store_name") + " = ?",
		"DELETE FROM " + quoteTableName(s.dialect, s.genericRecordsTable()) + " WHERE " + quoteIdent(s.dialect, "store_name") + " = ?",
	}
	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, s.q(stmt), store); err != nil {
			return status.Errorf(codes.Internal, "clear store: %v", err)
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
		quoteIdent(s.dialect, "pk_hash") + ", " +
		quoteIdent(s.dialect, "pk_bytes") + ") VALUES (?, ?, ?, ?, ?, ?)"
	_, err := tx.ExecContext(ctx, s.q(stmt), store, row.indexName, row.indexKeyHash, row.indexKeyBytes, row.pkHash, row.pkBytes)
	if err == nil {
		return nil
	}
	if !isDuplicateErr(err) {
		return status.Errorf(codes.Internal, "insert unique index row: %v", err)
	}

	query := "SELECT " + quoteIdent(s.dialect, "index_key_bytes") + ", " + quoteIdent(s.dialect, "pk_bytes") +
		" FROM " + quoteTableName(s.dialect, s.genericUniqueIndexTable()) +
		" WHERE " + quoteIdent(s.dialect, "store_name") + " = ? AND " +
		quoteIdent(s.dialect, "index_name") + " = ? AND " +
		quoteIdent(s.dialect, "index_key_hash") + " = ?"
	var existingKeyBytes []byte
	var existingPKBytes []byte
	if scanErr := tx.QueryRowContext(ctx, s.q(query), store, row.indexName, row.indexKeyHash).Scan(&existingKeyBytes, &existingPKBytes); scanErr != nil {
		return status.Errorf(codes.Internal, "load conflicting unique index row: %v", scanErr)
	}
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
		quoteIdent(s.dialect, "pk_hash") + ", " +
		quoteIdent(s.dialect, "pk_bytes") + ") VALUES (?, ?, ?, ?, ?, ?)"
	for _, row := range rows {
		if _, err := tx.ExecContext(ctx, s.q(stmt), store, row.indexName, row.indexKeyHash, row.indexKeyBytes, row.pkHash, row.pkBytes); err != nil {
			return status.Errorf(codes.Internal, "insert index row: %v", err)
		}
	}
	return nil
}

func (s *Store) insertGenericRecord(ctx context.Context, tx *sql.Tx, store string, primary encodedKey, payload []byte) error {
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
	if existing != nil && bytes.Equal(existing.pkBytes, primary.raw) {
		return status.Error(codes.AlreadyExists, "already exists")
	}
	return status.Error(codes.Internal, "primary key hash collision")
}

func (s *Store) upsertGenericRecord(ctx context.Context, tx *sql.Tx, store string, primary encodedKey, payload []byte) error {
	existing, err := s.loadGenericRecordByHash(ctx, tx, store, primary.hash)
	if err != nil {
		return err
	}
	if existing != nil && !bytes.Equal(existing.pkBytes, primary.raw) {
		return status.Error(codes.Internal, "primary key hash collision")
	}

	updateStmt := "UPDATE " + quoteTableName(s.dialect, s.genericRecordsTable()) +
		" SET " + quoteIdent(s.dialect, "pk_bytes") + " = ?, " + quoteIdent(s.dialect, "record_blob") + " = ?" +
		" WHERE " + quoteIdent(s.dialect, "store_name") + " = ? AND " + quoteIdent(s.dialect, "pk_hash") + " = ?"
	if existing != nil {
		if _, err := tx.ExecContext(ctx, s.q(updateStmt), primary.raw, payload, store, primary.hash); err != nil {
			return status.Errorf(codes.Internal, "update record: %v", err)
		}
		return nil
	}
	return s.insertGenericRecord(ctx, tx, store, primary, payload)
}

func (s *Store) reindexGenericStore(ctx context.Context, store string, schema *proto.ObjectStoreSchema) error {
	records, err := s.loadAllGenericRecords(ctx, store)
	if err != nil {
		return err
	}
	meta := newStoredSchema("", schema, storageVersionGeneric).toMeta(store)
	return s.withTx(ctx, func(txCtx context.Context, tx *sql.Tx) error {
		if err := s.clearGenericStoreTables(txCtx, tx, store); err != nil {
			return err
		}
		for _, row := range records {
			record, err := unmarshalRecordBlob(row.recordBlob)
			if err != nil {
				return err
			}
			primary, err := extractGenericPrimaryKey(record, meta)
			if err != nil {
				return err
			}
			uniqueRows, nonUniqueRows, err := buildGenericIndexRows(record, meta, primary)
			if err != nil {
				return err
			}
			if err := s.upsertGenericRecord(txCtx, tx, store, primary, row.recordBlob); err != nil {
				return err
			}
			for _, uniqueRow := range uniqueRows {
				if err := s.insertGenericUniqueIndexRow(txCtx, tx, store, uniqueRow); err != nil {
					return err
				}
			}
			if err := s.insertGenericIndexRows(txCtx, tx, s.genericIndexTable(), store, nonUniqueRows); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) addGeneric(ctx context.Context, store string, m *storeMeta, record *proto.Record) error {
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

func (s *Store) putGeneric(ctx context.Context, store string, m *storeMeta, record *proto.Record) error {
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
		existing, err := s.loadGenericRecordByHash(txCtx, tx, store, primary.hash)
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
		return s.upsertGenericRecord(txCtx, tx, store, primary, payload)
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
		existing, err := s.loadGenericRecordByHash(txCtx, tx, store, primary.hash)
		if err != nil {
			return err
		}
		if existing == nil {
			return nil
		}
		if !bytes.Equal(existing.pkBytes, primary.raw) {
			return status.Error(codes.Internal, "primary key hash collision")
		}
		if err := s.deleteGenericIndexRowsByPrimaryKey(txCtx, tx, store, primary.hash); err != nil {
			return err
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

			existing, err := s.loadGenericRecordByHash(txCtx, tx, store, primary.hash)
			if err != nil {
				return err
			}
			if existing == nil {
				continue
			}
			if !bytes.Equal(existing.pkBytes, primary.raw) {
				return status.Error(codes.Internal, "primary key hash collision")
			}
			if err := s.deleteGenericIndexRowsByPrimaryKey(txCtx, tx, store, primary.hash); err != nil {
				return err
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

func (s *Store) genericGet(ctx context.Context, store string, m *storeMeta, rawKey string) (*proto.Record, error) {
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

func (s *Store) genericObjectStoreEntries(ctx context.Context, store string, m *storeMeta, keyRange *proto.KeyRange, keysOnly bool) ([]cursorutil.Entry, error) {
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
		var record *proto.Record
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
	entries, err = applyKeyRangeToEntries(entries, keyRange, false)
	if err != nil {
		return nil, err
	}
	sortCursorEntries(entries)
	return entries, nil
}

func (s *Store) genericIndexEntries(ctx context.Context, store string, m *storeMeta, idx *proto.IndexSchema, values []*proto.TypedValue, keyRange *proto.KeyRange, keysOnly bool) ([]cursorutil.Entry, error) {
	var nonUniqueRows []genericIndexRow
	var uniqueRows []genericIndexRow
	var err error
	if len(values) == len(idx.GetKeyPath()) && len(values) > 0 {
		keyParts, err := gestalt.AnyFromTypedValues(values)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "index values: %v", err)
		}
		var lookupValue any = keyParts
		if len(keyParts) == 1 {
			lookupValue = keyParts[0]
		}
		encoded, err := encodeKeyValue(lookupValue)
		if err != nil {
			return nil, err
		}
		nonUniqueRows, err = s.loadGenericIndexRowsByHash(ctx, s.genericIndexTable(), store, idx.GetName(), encoded.hash)
		if err != nil {
			return nil, err
		}
		uniqueRows, err = s.loadGenericIndexRowsByHash(ctx, s.genericUniqueIndexTable(), store, idx.GetName(), encoded.hash)
		if err != nil {
			return nil, err
		}
	} else {
		nonUniqueRows, err = s.loadGenericIndexRows(ctx, s.genericIndexTable(), store, idx.GetName())
		if err != nil {
			return nil, err
		}
		uniqueRows, err = s.loadGenericIndexRows(ctx, s.genericUniqueIndexTable(), store, idx.GetName())
		if err != nil {
			return nil, err
		}
	}
	allRows := append(nonUniqueRows, uniqueRows...)

	recordByPrimary := map[string]*proto.Record{}
	if !keysOnly {
		records, err := s.loadAllGenericRecords(ctx, store)
		if err != nil {
			return nil, err
		}
		for _, row := range records {
			record, err := unmarshalRecordBlob(row.recordBlob)
			if err != nil {
				return nil, err
			}
			recordByPrimary[string(row.pkHash)+":"+string(row.pkBytes)] = record
		}
	}

	entries := make([]cursorutil.Entry, 0, len(allRows))
	for _, row := range allRows {
		indexKeyValue, err := decodeKeyValue(row.indexKeyBytes)
		if err != nil {
			return nil, err
		}
		primaryKeyValue, err := decodeKeyValue(row.pkBytes)
		if err != nil {
			return nil, err
		}
		entry := cursorutil.Entry{
			Key:             normalizeDocumentBound(indexKeyValue),
			PrimaryKey:      fmt.Sprint(primaryKeyValue),
			PrimaryKeyValue: primaryKeyValue,
		}
		if !keysOnly {
			record, ok := recordByPrimary[string(row.pkHash)+":"+string(row.pkBytes)]
			if !ok {
				return nil, status.Error(codes.Internal, "index row points to missing record")
			}
			entry.Record = record
		}
		entries = append(entries, entry)
	}

	entries, err = filterEntriesByPrefix(entries, values)
	if err != nil {
		return nil, err
	}
	entries, err = applyKeyRangeToEntries(entries, keyRange, true)
	if err != nil {
		return nil, err
	}
	sortCursorEntries(entries)
	return entries, nil
}
