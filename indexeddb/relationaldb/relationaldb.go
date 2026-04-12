package relationaldb

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/microsoft/go-mssqldb"
	_ "modernc.org/sqlite"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type storeMeta struct {
	table   string
	pkCol   string
	columns []*proto.ColumnDef
	indexes []*proto.IndexSchema
}

const (
	metadataTableName  = "_gestalt_stores"
	defaultTablePrefix = "_gestalt_store_"
)

type Store struct {
	proto.UnimplementedIndexedDBServer
	db          *sql.DB
	bind        bindStyle
	dialect     dialect
	tablePrefix string
	mu          sync.RWMutex
	meta        map[string]*storeMeta
}

func NewStore(dsn string) (*Store, error) {
	return newStoreWithTablePrefix(dsn, defaultTablePrefix)
}

func newStoreWithTablePrefix(dsn, tablePrefix string) (*Store, error) {
	driver, connStr, style, d := parseDSN(dsn)
	db, err := sql.Open(driver, connStr)
	if err != nil {
		return nil, fmt.Errorf("relationaldb: open: %w", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("relationaldb: ping: %w", err)
	}

	s := &Store{
		db:          db,
		bind:        style,
		dialect:     d,
		tablePrefix: tablePrefix,
		meta:        make(map[string]*storeMeta),
	}

	if _, err := db.Exec(s.q(metadataTableSQL(d))); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("relationaldb: create metadata table: %w", err)
	}

	rows, err := db.Query("SELECT " + quoteIdent(d, "name") + ", " + quoteIdent(d, "schema_json") +
		" FROM " + quoteIdent(d, metadataTableName))
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("relationaldb: load metadata: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, schemaJSON string
		if err := rows.Scan(&name, &schemaJSON); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("relationaldb: scan metadata: %w", err)
		}
		var stored storedSchema
		if err := json.Unmarshal([]byte(schemaJSON), &stored); err != nil {
			continue
		}
		s.meta[name] = stored.toMeta(name)
	}
	return s, rows.Err()
}

func (s *Store) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *Store) HealthCheck(_ context.Context) error {
	return s.db.Ping()
}

// q rebinds a query with ? placeholders to the driver's style.
func (s *Store) q(query string) string {
	return rebind(s.bind, query)
}

func (s *Store) getMeta(name string) (*storeMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.meta[name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "object store not found: %s", name)
	}
	return m, nil
}

func (s *Store) physicalTableName(store string) string {
	if s.tablePrefix == "" {
		return store
	}
	return s.tablePrefix + store
}

func (s *Store) ensureTable(ctx context.Context, table string, schema *proto.ObjectStoreSchema) error {
	ddl := createTableSQL(s.dialect, table, schema)
	if _, err := s.db.ExecContext(ctx, s.q(ddl)); err != nil {
		return status.Errorf(codes.Internal, "create table: %v", err)
	}
	for _, idx := range schema.GetIndexes() {
		if _, err := s.db.ExecContext(ctx, s.q(createIndexSQL(s.dialect, table, idx, schema))); err != nil && !isDuplicateErr(err) {
			return status.Errorf(codes.Internal, "create index: %v", err)
		}
	}
	return nil
}

func (s *Store) tableColumns(ctx context.Context, table string) (map[string]struct{}, error) {
	rows, err := s.db.QueryContext(ctx, s.q("SELECT * FROM "+quoteIdent(s.dialect, table)+" WHERE 1 = 0"))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	names, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	cols := make(map[string]struct{}, len(names))
	for _, name := range names {
		cols[name] = struct{}{}
	}
	return cols, nil
}

func schemaColumnsExist(existing map[string]struct{}, schema *proto.ObjectStoreSchema) bool {
	for _, col := range schema.GetColumns() {
		if _, ok := existing[col.Name]; !ok {
			return false
		}
	}
	return true
}

func (s *Store) tableCount(ctx context.Context, table string) (int64, error) {
	var count int64
	err := s.db.QueryRowContext(ctx, s.q("SELECT COUNT(*) FROM "+quoteIdent(s.dialect, table))).Scan(&count)
	return count, err
}

func (s *Store) resetOrphanedProviderTable(ctx context.Context, table string) error {
	if s.tablePrefix == "" || !strings.HasPrefix(table, s.tablePrefix) {
		return nil
	}
	count, err := s.tableCount(ctx, table)
	if err != nil {
		return nil
	}
	if count != 0 {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, s.q(dropTableSQL(s.dialect, table))); err != nil {
		return status.Errorf(codes.Internal, "drop orphaned table: %v", err)
	}
	return nil
}

func (s *Store) copyStoreRows(ctx context.Context, sourceTable, destTable string, schema *proto.ObjectStoreSchema) error {
	if sourceTable == "" || sourceTable == destTable {
		return nil
	}

	sourceCols, err := s.tableColumns(ctx, sourceTable)
	if err != nil || !schemaColumnsExist(sourceCols, schema) {
		return nil
	}

	destCount, err := s.tableCount(ctx, destTable)
	if err != nil {
		return status.Errorf(codes.Internal, "count destination rows: %v", err)
	}
	if destCount != 0 {
		return nil
	}

	cols := make([]string, len(schema.GetColumns()))
	for i, col := range schema.GetColumns() {
		cols[i] = quoteIdent(s.dialect, col.Name)
	}
	colList := strings.Join(cols, ", ")
	query := "INSERT INTO " + quoteIdent(s.dialect, destTable) +
		" (" + colList + ") SELECT " + colList + " FROM " + quoteIdent(s.dialect, sourceTable)
	if _, err := s.db.ExecContext(ctx, s.q(query)); err != nil {
		return status.Errorf(codes.Internal, "copy existing rows: %v", err)
	}
	return nil
}

func (s *Store) persistStoreMetadata(ctx context.Context, storeName, tableName string, schema *proto.ObjectStoreSchema) error {
	schemaJSON, err := json.Marshal(newStoredSchema(tableName, schema))
	if err != nil {
		return status.Errorf(codes.Internal, "marshal schema: %v", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return status.Errorf(codes.Internal, "begin metadata tx: %v", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx,
		s.q("DELETE FROM "+quoteIdent(s.dialect, metadataTableName)+" WHERE "+quoteIdent(s.dialect, "name")+" = ?"),
		storeName,
	); err != nil {
		return status.Errorf(codes.Internal, "delete metadata: %v", err)
	}
	if _, err := tx.ExecContext(ctx,
		s.q("INSERT INTO "+quoteIdent(s.dialect, metadataTableName)+" ("+quoteIdent(s.dialect, "name")+", "+quoteIdent(s.dialect, "schema_json")+") VALUES (?, ?)"),
		storeName, string(schemaJSON),
	); err != nil {
		return status.Errorf(codes.Internal, "insert metadata: %v", err)
	}
	if err := tx.Commit(); err != nil {
		return status.Errorf(codes.Internal, "commit metadata tx: %v", err)
	}

	s.meta[storeName] = newStoredSchema(tableName, schema).toMeta(storeName)
	return nil
}

// ---- Lifecycle ----

func (s *Store) CreateObjectStore(ctx context.Context, req *proto.CreateObjectStoreRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	schema := req.GetSchema()
	tableName := s.physicalTableName(req.Name)

	if existing, ok := s.meta[req.Name]; ok {
		if err := s.ensureTable(ctx, tableName, schema); err != nil {
			return nil, err
		}
		if existing.table != tableName {
			if err := s.copyStoreRows(ctx, existing.table, tableName, schema); err != nil {
				return nil, err
			}
		}
		if err := s.persistStoreMetadata(ctx, req.Name, tableName, schema); err != nil {
			return nil, err
		}
		return &emptypb.Empty{}, nil
	}

	if err := s.resetOrphanedProviderTable(ctx, tableName); err != nil {
		return nil, err
	}
	if err := s.ensureTable(ctx, tableName, schema); err != nil {
		return nil, err
	}
	if err := s.persistStoreMetadata(ctx, req.Name, tableName, schema); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *Store) DeleteObjectStore(ctx context.Context, req *proto.DeleteObjectStoreRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tableName := s.physicalTableName(req.Name)
	if meta, ok := s.meta[req.Name]; ok && meta.table != "" {
		tableName = meta.table
	}

	if _, err := s.db.ExecContext(ctx, s.q(dropTableSQL(s.dialect, tableName))); err != nil {
		return nil, status.Errorf(codes.Internal, "drop table: %v", err)
	}
	_, _ = s.db.ExecContext(ctx,
		s.q("DELETE FROM "+quoteIdent(s.dialect, metadataTableName)+" WHERE "+quoteIdent(s.dialect, "name")+" = ?"), req.Name)
	delete(s.meta, req.Name)
	return &emptypb.Empty{}, nil
}

// ---- Primary key CRUD ----

func (s *Store) Get(ctx context.Context, req *proto.ObjectStoreRequest) (*proto.RecordResponse, error) {
	m, err := s.getMeta(req.Store)
	if err != nil {
		return nil, err
	}
	row := s.db.QueryRowContext(ctx, s.q(selectByPK(s.dialect, m.table, m.pkCol, m.columns)), req.Id)
	record, err := scanRow(row, m.columns)
	if err != nil {
		return nil, mapSQLErr("get", err)
	}
	return &proto.RecordResponse{Record: record}, nil
}

func (s *Store) GetKey(ctx context.Context, req *proto.ObjectStoreRequest) (*proto.KeyResponse, error) {
	m, err := s.getMeta(req.Store)
	if err != nil {
		return nil, err
	}
	var key string
	err = s.db.QueryRowContext(ctx, s.q(selectKeyByPK(s.dialect, m.table, m.pkCol)), req.Id).Scan(&key)
	if err != nil {
		return nil, mapSQLErr("get_key", err)
	}
	return &proto.KeyResponse{Key: key}, nil
}

func (s *Store) Add(ctx context.Context, req *proto.RecordRequest) (*emptypb.Empty, error) {
	m, err := s.getMeta(req.Store)
	if err != nil {
		return nil, err
	}
	args, err := recordToArgs(req.GetRecord(), m.columns)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "marshal record: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, s.q(insertSQL(s.dialect, m.table, m.columns)), args...); err != nil {
		return nil, mapSQLErr("add", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Store) Put(ctx context.Context, req *proto.RecordRequest) (*emptypb.Empty, error) {
	m, err := s.getMeta(req.Store)
	if err != nil {
		return nil, err
	}
	record := req.GetRecord()
	id, err := extractID(record, m.pkCol)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "record id: %v", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "begin tx: %v", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, s.q(deleteByPK(s.dialect, m.table, m.pkCol)), id); err != nil {
		return nil, status.Errorf(codes.Internal, "put delete: %v", err)
	}
	args, err := recordToArgs(record, m.columns)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "marshal record: %v", err)
	}
	if _, err := tx.ExecContext(ctx, s.q(insertSQL(s.dialect, m.table, m.columns)), args...); err != nil {
		return nil, mapSQLErr("put insert", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, status.Errorf(codes.Internal, "commit: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Store) Delete(ctx context.Context, req *proto.ObjectStoreRequest) (*emptypb.Empty, error) {
	m, err := s.getMeta(req.Store)
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(ctx, s.q(deleteByPK(s.dialect, m.table, m.pkCol)), req.Id); err != nil {
		return nil, mapSQLErr("delete", err)
	}
	return &emptypb.Empty{}, nil
}

// ---- Bulk operations ----

func (s *Store) Clear(ctx context.Context, req *proto.ObjectStoreNameRequest) (*emptypb.Empty, error) {
	m, err := s.getMeta(req.Store)
	if err != nil {
		return nil, err
	}
	if _, err := s.db.ExecContext(ctx, s.q(deleteAll(s.dialect, m.table))); err != nil {
		return nil, status.Errorf(codes.Internal, "clear: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Store) GetAll(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.RecordsResponse, error) {
	m, err := s.getMeta(req.Store)
	if err != nil {
		return nil, err
	}
	query, args, err := selectAllWithRange(s.dialect, m, req.Range)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, s.q(query), args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get_all: %v", err)
	}
	defer rows.Close()
	records, err := scanRows(rows, m.columns)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get_all scan: %v", err)
	}
	return &proto.RecordsResponse{Records: records}, nil
}

func (s *Store) GetAllKeys(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.KeysResponse, error) {
	m, err := s.getMeta(req.Store)
	if err != nil {
		return nil, err
	}
	query, args, err := selectKeysWithRange(s.dialect, m, req.Range)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, s.q(query), args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get_all_keys: %v", err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, status.Errorf(codes.Internal, "get_all_keys scan: %v", err)
		}
		keys = append(keys, k)
	}
	return &proto.KeysResponse{Keys: keys}, rows.Err()
}

func (s *Store) Count(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.CountResponse, error) {
	m, err := s.getMeta(req.Store)
	if err != nil {
		return nil, err
	}
	query, args, err := countWithRange(s.dialect, m, req.Range)
	if err != nil {
		return nil, err
	}
	var count int64
	if err := s.db.QueryRowContext(ctx, s.q(query), args...).Scan(&count); err != nil {
		return nil, status.Errorf(codes.Internal, "count: %v", err)
	}
	return &proto.CountResponse{Count: count}, nil
}

func (s *Store) DeleteRange(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.DeleteResponse, error) {
	m, err := s.getMeta(req.Store)
	if err != nil {
		return nil, err
	}
	query, args, err := deleteWithRange(s.dialect, m, req.Range)
	if err != nil {
		return nil, err
	}
	result, err := s.db.ExecContext(ctx, s.q(query), args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "delete_range: %v", err)
	}
	n, _ := result.RowsAffected()
	return &proto.DeleteResponse{Deleted: n}, nil
}

// ---- Index queries ----

func (s *Store) IndexGet(ctx context.Context, req *proto.IndexQueryRequest) (*proto.RecordResponse, error) {
	m, err := s.getMeta(req.Store)
	if err != nil {
		return nil, err
	}
	query, args, err := indexSelectQuery(s.dialect, m, req, true, colList(s.dialect, m.columns))
	if err != nil {
		return nil, err
	}
	row := s.db.QueryRowContext(ctx, s.q(query), args...)
	record, err := scanRow(row, m.columns)
	if err != nil {
		return nil, mapSQLErr("index_get", err)
	}
	return &proto.RecordResponse{Record: record}, nil
}

func (s *Store) IndexGetKey(ctx context.Context, req *proto.IndexQueryRequest) (*proto.KeyResponse, error) {
	m, err := s.getMeta(req.Store)
	if err != nil {
		return nil, err
	}
	query, args, err := indexSelectQuery(s.dialect, m, req, true, quoteIdent(s.dialect, m.pkCol))
	if err != nil {
		return nil, err
	}
	var key string
	if err := s.db.QueryRowContext(ctx, s.q(query), args...).Scan(&key); err != nil {
		return nil, mapSQLErr("index_get_key", err)
	}
	return &proto.KeyResponse{Key: key}, nil
}

func (s *Store) IndexGetAll(ctx context.Context, req *proto.IndexQueryRequest) (*proto.RecordsResponse, error) {
	m, err := s.getMeta(req.Store)
	if err != nil {
		return nil, err
	}
	query, args, err := indexSelectQuery(s.dialect, m, req, false, colList(s.dialect, m.columns))
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, s.q(query), args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "index_get_all: %v", err)
	}
	defer rows.Close()
	records, err := scanRows(rows, m.columns)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "index_get_all scan: %v", err)
	}
	return &proto.RecordsResponse{Records: records}, nil
}

func (s *Store) IndexGetAllKeys(ctx context.Context, req *proto.IndexQueryRequest) (*proto.KeysResponse, error) {
	m, err := s.getMeta(req.Store)
	if err != nil {
		return nil, err
	}
	query, args, err := indexSelectQuery(s.dialect, m, req, false, quoteIdent(s.dialect, m.pkCol))
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, s.q(query), args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "index_get_all_keys: %v", err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, status.Errorf(codes.Internal, "index_get_all_keys scan: %v", err)
		}
		keys = append(keys, k)
	}
	return &proto.KeysResponse{Keys: keys}, rows.Err()
}

func (s *Store) IndexCount(ctx context.Context, req *proto.IndexQueryRequest) (*proto.CountResponse, error) {
	m, err := s.getMeta(req.Store)
	if err != nil {
		return nil, err
	}
	query, args, err := indexCountQuery(s.dialect, m, req)
	if err != nil {
		return nil, err
	}
	var count int64
	if err := s.db.QueryRowContext(ctx, s.q(query), args...).Scan(&count); err != nil {
		return nil, status.Errorf(codes.Internal, "index_count: %v", err)
	}
	return &proto.CountResponse{Count: count}, nil
}

func (s *Store) IndexDelete(ctx context.Context, req *proto.IndexQueryRequest) (*proto.DeleteResponse, error) {
	m, err := s.getMeta(req.Store)
	if err != nil {
		return nil, err
	}
	query, args, err := indexDeleteQuery(s.dialect, m, req)
	if err != nil {
		return nil, err
	}
	result, err := s.db.ExecContext(ctx, s.q(query), args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "index_delete: %v", err)
	}
	n, _ := result.RowsAffected()
	return &proto.DeleteResponse{Deleted: n}, nil
}

// ---- Query builders for range and index operations ----

func selectAllWithRange(d dialect, m *storeMeta, kr *proto.KeyRange) (string, []any, error) {
	where, args, err := keyRangeWhere(d, m, kr)
	if err != nil {
		return "", nil, err
	}
	if where == "" {
		return fmt.Sprintf("SELECT %s FROM %s", colList(d, m.columns), quoteIdent(d, m.table)), args, nil
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s", colList(d, m.columns), quoteIdent(d, m.table), where), args, nil
}

func selectKeysWithRange(d dialect, m *storeMeta, kr *proto.KeyRange) (string, []any, error) {
	pk := quoteIdent(d, m.pkCol)
	where, args, err := keyRangeWhere(d, m, kr)
	if err != nil {
		return "", nil, err
	}
	if where == "" {
		return fmt.Sprintf("SELECT %s FROM %s", pk, quoteIdent(d, m.table)), args, nil
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s", pk, quoteIdent(d, m.table), where), args, nil
}

func countWithRange(d dialect, m *storeMeta, kr *proto.KeyRange) (string, []any, error) {
	where, args, err := keyRangeWhere(d, m, kr)
	if err != nil {
		return "", nil, err
	}
	if where == "" {
		return fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdent(d, m.table)), args, nil
	}
	return fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", quoteIdent(d, m.table), where), args, nil
}

func deleteWithRange(d dialect, m *storeMeta, kr *proto.KeyRange) (string, []any, error) {
	where, args, err := keyRangeWhere(d, m, kr)
	if err != nil {
		return "", nil, err
	}
	if where == "" {
		return fmt.Sprintf("DELETE FROM %s", quoteIdent(d, m.table)), args, nil
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s", quoteIdent(d, m.table), where), args, nil
}

func indexSelectQuery(d dialect, m *storeMeta, req *proto.IndexQueryRequest, limitOne bool, selectExpr string) (string, []any, error) {
	idx := findIndex(m, req.Index)
	if idx == nil {
		return "", nil, status.Errorf(codes.NotFound, "index not found: %s", req.Index)
	}

	var clauses []string
	var args []any
	for i, col := range idx.KeyPath {
		if i >= len(req.Values) {
			break
		}
		clauses = append(clauses, quoteIdent(d, col)+" = ?")
		arg, err := protoValueToArg(req.Values[i], columnType(m, col))
		if err != nil {
			return "", nil, status.Errorf(codes.InvalidArgument, "index value %d: %v", i, err)
		}
		args = append(args, arg)
	}

	rangeWhere, rangeArgs, err := keyRangeWhere(d, m, req.Range)
	if err != nil {
		return "", nil, err
	}
	if rangeWhere != "" {
		clauses = append(clauses, rangeWhere)
		args = append(args, rangeArgs...)
	}

	where := ""
	if len(clauses) > 0 {
		where = " WHERE " + strings.Join(clauses, " AND ")
	}
	limit := ""
	if limitOne {
		limit = " LIMIT 1"
	}
	return fmt.Sprintf("SELECT %s FROM %s%s%s", selectExpr, quoteIdent(d, m.table), where, limit), args, nil
}

func indexCountQuery(d dialect, m *storeMeta, req *proto.IndexQueryRequest) (string, []any, error) {
	idx := findIndex(m, req.Index)
	if idx == nil {
		return "", nil, status.Errorf(codes.NotFound, "index not found: %s", req.Index)
	}

	var clauses []string
	var args []any
	for i, col := range idx.KeyPath {
		if i >= len(req.Values) {
			break
		}
		clauses = append(clauses, quoteIdent(d, col)+" = ?")
		arg, err := protoValueToArg(req.Values[i], columnType(m, col))
		if err != nil {
			return "", nil, status.Errorf(codes.InvalidArgument, "index value %d: %v", i, err)
		}
		args = append(args, arg)
	}

	rangeWhere, rangeArgs, err := keyRangeWhere(d, m, req.Range)
	if err != nil {
		return "", nil, err
	}
	if rangeWhere != "" {
		clauses = append(clauses, rangeWhere)
		args = append(args, rangeArgs...)
	}

	where := ""
	if len(clauses) > 0 {
		where = " WHERE " + strings.Join(clauses, " AND ")
	}
	return fmt.Sprintf("SELECT COUNT(*) FROM %s%s", quoteIdent(d, m.table), where), args, nil
}

func indexDeleteQuery(d dialect, m *storeMeta, req *proto.IndexQueryRequest) (string, []any, error) {
	idx := findIndex(m, req.Index)
	if idx == nil {
		return "", nil, status.Errorf(codes.NotFound, "index not found: %s", req.Index)
	}

	var clauses []string
	var args []any
	for i, col := range idx.KeyPath {
		if i >= len(req.Values) {
			break
		}
		clauses = append(clauses, quoteIdent(d, col)+" = ?")
		arg, err := protoValueToArg(req.Values[i], columnType(m, col))
		if err != nil {
			return "", nil, status.Errorf(codes.InvalidArgument, "index value %d: %v", i, err)
		}
		args = append(args, arg)
	}
	if len(clauses) == 0 {
		return "", nil, status.Errorf(codes.InvalidArgument, "no index values provided")
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s", quoteIdent(d, m.table), strings.Join(clauses, " AND ")), args, nil
}

func findIndex(m *storeMeta, name string) *proto.IndexSchema {
	for _, idx := range m.indexes {
		if idx.Name == name {
			return idx
		}
	}
	return nil
}

func keyRangeWhere(d dialect, m *storeMeta, kr *proto.KeyRange) (string, []any, error) {
	if kr == nil {
		return "", nil, nil
	}
	var clauses []string
	var args []any
	pk := quoteIdent(d, m.pkCol)
	pkType := columnType(m, m.pkCol)

	if kr.Lower != nil && kr.Lower.GetKind() != nil {
		if _, ok := kr.Lower.GetKind().(*proto.TypedValue_NullValue); !ok {
			op := ">="
			if kr.LowerOpen {
				op = ">"
			}
			clauses = append(clauses, fmt.Sprintf("%s %s ?", pk, op))
			arg, err := protoValueToArg(kr.Lower, pkType)
			if err != nil {
				return "", nil, status.Errorf(codes.InvalidArgument, "key range lower: %v", err)
			}
			args = append(args, arg)
		}
	}
	if kr.Upper != nil && kr.Upper.GetKind() != nil {
		if _, ok := kr.Upper.GetKind().(*proto.TypedValue_NullValue); !ok {
			op := "<="
			if kr.UpperOpen {
				op = "<"
			}
			clauses = append(clauses, fmt.Sprintf("%s %s ?", pk, op))
			arg, err := protoValueToArg(kr.Upper, pkType)
			if err != nil {
				return "", nil, status.Errorf(codes.InvalidArgument, "key range upper: %v", err)
			}
			args = append(args, arg)
		}
	}
	return strings.Join(clauses, " AND "), args, nil
}

// ---- Value conversion ----

func recordToArgs(record *proto.Record, cols []*proto.ColumnDef) ([]any, error) {
	if record == nil {
		return make([]any, len(cols)), nil
	}
	args := make([]any, len(cols))
	for i, col := range cols {
		v, ok := record.Fields[col.Name]
		if !ok || v == nil {
			args[i] = nil
			continue
		}
		arg, err := protoValueToSQLArg(v, col.Type)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", col.Name, err)
		}
		args[i] = arg
	}
	return args, nil
}

func protoValueToSQLArg(v *proto.TypedValue, colType int32) (any, error) {
	if v == nil {
		return nil, nil
	}
	goValue, err := gestalt.AnyFromTypedValue(v)
	if err != nil {
		return nil, err
	}
	return anyToSQLArg(goValue, colType)
}

func anyToSQLArg(value any, colType int32) (any, error) {
	if value == nil {
		return nil, nil
	}
	switch colType {
	case 1: // TypeInt
		n, err := toInt64(value)
		if err != nil {
			return nil, err
		}
		return n, nil
	case 2: // TypeFloat
		f, err := toFloat64(value)
		if err != nil {
			return nil, err
		}
		return f, nil
	case 3: // TypeBool
		b, err := toBool(value)
		if err != nil {
			return nil, err
		}
		if b {
			return int16(1), nil
		}
		return int16(0), nil
	case 4: // TypeTime
		return coerceSQLTime(value)
	case 5: // TypeBytes
		switch v := value.(type) {
		case []byte:
			return base64.StdEncoding.EncodeToString(v), nil
		case string:
			return v, nil
		default:
			return nil, fmt.Errorf("expected []byte or string, got %T", value)
		}
	case 6: // TypeJSON
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return string(raw), nil
	default:
		return fmt.Sprint(value), nil
	}
}

func protoValueToArg(v *proto.TypedValue, colType int32) (any, error) {
	return protoValueToSQLArg(v, colType)
}

func extractID(record *proto.Record, pkCol string) (string, error) {
	if record == nil {
		return "", status.Error(codes.InvalidArgument, "record is required")
	}
	v, ok := record.Fields[pkCol]
	if !ok || v == nil {
		return "", status.Errorf(codes.InvalidArgument, "record missing primary key %q", pkCol)
	}
	value, err := gestalt.AnyFromTypedValue(v)
	if err != nil {
		return "", err
	}
	return fmt.Sprint(value), nil
}

func scanRow(row *sql.Row, cols []*proto.ColumnDef) (*proto.Record, error) {
	dest := scanDestinations(len(cols))
	if err := row.Scan(dest...); err != nil {
		return nil, err
	}
	return destToStruct(dest, cols)
}

func scanRows(rows *sql.Rows, cols []*proto.ColumnDef) ([]*proto.Record, error) {
	var out []*proto.Record
	for rows.Next() {
		dest := scanDestinations(len(cols))
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		s, err := destToStruct(dest, cols)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func destToStruct(dest []any, cols []*proto.ColumnDef) (*proto.Record, error) {
	record := make(map[string]any, len(cols))
	for i, col := range cols {
		value, err := sqlValueToRecordValue(*(dest[i].(*any)), col.Type)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", col.Name, err)
		}
		record[col.Name] = value
	}
	return gestalt.RecordToProto(record)
}

func columnType(m *storeMeta, name string) int32 {
	for _, col := range m.columns {
		if col.Name == name {
			return col.Type
		}
	}
	return 0
}

func toInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int32:
		return int64(v), nil
	case int:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	default:
		return 0, fmt.Errorf("expected integer-compatible value, got %T", value)
	}
}

func toFloat64(value any) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	case []byte:
		return strconv.ParseFloat(string(v), 64)
	default:
		return 0, fmt.Errorf("expected float-compatible value, got %T", value)
	}
}

func toBool(value any) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case int64:
		return v != 0, nil
	case int:
		return v != 0, nil
	case string:
		switch strings.ToLower(v) {
		case "1", "true":
			return true, nil
		case "0", "false":
			return false, nil
		}
	case []byte:
		return toBool(string(v))
	}
	return false, fmt.Errorf("expected bool-compatible value, got %T", value)
}

func coerceSQLTime(value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v.UTC(), nil
	case string:
		return parseTimeString(v)
	case []byte:
		return parseTimeString(string(v))
	default:
		return time.Time{}, fmt.Errorf("expected time-compatible value, got %T", value)
	}
}

func parseTimeString(raw string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05 -0700 MST",
	}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid time value %q", raw)
}

func parseStoredTime(raw string) any {
	if parsed, err := parseTimeString(raw); err == nil {
		return parsed
	}
	return raw
}

func parseStoredBytes(raw string) []byte {
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return []byte(raw)
	}
	return decoded
}

func parseStoredJSON(raw string) any {
	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return raw
	}
	return value
}

func scanDestinations(n int) []any {
	dest := make([]any, n)
	for i := range dest {
		dest[i] = new(any)
	}
	return dest
}

func sqlValueToRecordValue(raw any, colType int32) (any, error) {
	if raw == nil {
		return nil, nil
	}
	switch colType {
	case 1: // TypeInt
		return toInt64(raw)
	case 2: // TypeFloat
		return toFloat64(raw)
	case 3: // TypeBool
		return toBool(raw)
	case 4: // TypeTime
		switch v := raw.(type) {
		case time.Time:
			return v.UTC(), nil
		case string:
			return parseStoredTime(v), nil
		case []byte:
			return parseStoredTime(string(v)), nil
		default:
			return parseStoredTime(fmt.Sprint(v)), nil
		}
	case 5: // TypeBytes
		switch v := raw.(type) {
		case []byte:
			return append([]byte(nil), v...), nil
		case string:
			return parseStoredBytes(v), nil
		default:
			return []byte(fmt.Sprint(v)), nil
		}
	case 6: // TypeJSON
		switch v := raw.(type) {
		case []byte:
			return parseStoredJSON(string(v)), nil
		case string:
			return parseStoredJSON(v), nil
		default:
			return raw, nil
		}
	default:
		switch v := raw.(type) {
		case []byte:
			return string(v), nil
		case string:
			return v, nil
		default:
			return fmt.Sprint(v), nil
		}
	}
}

// ---- DSN parsing ----

func parseDSN(dsn string) (driver, connStr string, style bindStyle, d dialect) {
	switch {
	case strings.HasPrefix(dsn, "postgres://"), strings.HasPrefix(dsn, "postgresql://"):
		return "pgx", dsn, bindDollar, dialectPostgres
	case strings.HasPrefix(dsn, "mysql://"):
		return "mysql", strings.TrimPrefix(dsn, "mysql://"), bindQuestion, dialectMySQL
	case strings.Contains(dsn, "@tcp("), strings.Contains(dsn, "@unix("):
		return "mysql", dsn, bindQuestion, dialectMySQL
	case strings.HasPrefix(dsn, "sqlserver://"):
		return "sqlserver", dsn, bindAtP, dialectSQLServer
	case strings.HasPrefix(dsn, "sqlite://"):
		return "sqlite", strings.TrimPrefix(dsn, "sqlite://"), bindQuestion, dialectSQLite
	case strings.HasPrefix(dsn, "sqlite:"):
		return "sqlite", strings.TrimPrefix(dsn, "sqlite:"), bindQuestion, dialectSQLite
	case strings.HasPrefix(dsn, "file:"):
		return "sqlite", dsn, bindQuestion, dialectSQLite
	default:
		return "sqlite", dsn, bindQuestion, dialectSQLite
	}
}

// ---- Error mapping ----

func mapSQLErr(op string, err error) error {
	if err == nil {
		return nil
	}
	if err == sql.ErrNoRows {
		return status.Errorf(codes.NotFound, "not found")
	}
	if isDuplicateErr(err) {
		return status.Errorf(codes.AlreadyExists, "already exists")
	}
	return status.Errorf(codes.Internal, "%s: %v", op, err)
}

func isDuplicateErr(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unique") || strings.Contains(msg, "duplicate") || strings.Contains(msg, "constraint")
}

// ---- Schema persistence ----

type storedSchema struct {
	Table   string         `json:"table,omitempty"`
	Columns []storedColumn `json:"columns"`
	Indexes []storedIndex  `json:"indexes"`
}

type storedColumn struct {
	Name       string `json:"name"`
	Type       int32  `json:"type"`
	PrimaryKey bool   `json:"primary_key"`
	NotNull    bool   `json:"not_null"`
	Unique     bool   `json:"unique"`
}

type storedIndex struct {
	Name    string   `json:"name"`
	KeyPath []string `json:"key_path"`
	Unique  bool     `json:"unique"`
}

func newStoredSchema(table string, schema *proto.ObjectStoreSchema) storedSchema {
	s := storedSchema{Table: table}
	for _, c := range schema.GetColumns() {
		s.Columns = append(s.Columns, storedColumn{
			Name: c.Name, Type: c.Type, PrimaryKey: c.PrimaryKey, NotNull: c.NotNull, Unique: c.Unique,
		})
	}
	for _, i := range schema.GetIndexes() {
		s.Indexes = append(s.Indexes, storedIndex{
			Name: i.Name, KeyPath: i.KeyPath, Unique: i.Unique,
		})
	}
	return s
}

func (s storedSchema) toMeta(name string) *storeMeta {
	table := s.Table
	if table == "" {
		table = name
	}
	m := &storeMeta{table: table, pkCol: "id"}
	for _, c := range s.Columns {
		col := &proto.ColumnDef{Name: c.Name, Type: c.Type, PrimaryKey: c.PrimaryKey, NotNull: c.NotNull, Unique: c.Unique}
		m.columns = append(m.columns, col)
		if c.PrimaryKey {
			m.pkCol = c.Name
		}
	}
	for _, i := range s.Indexes {
		m.indexes = append(m.indexes, &proto.IndexSchema{Name: i.Name, KeyPath: i.KeyPath, Unique: i.Unique})
	}
	return m
}
