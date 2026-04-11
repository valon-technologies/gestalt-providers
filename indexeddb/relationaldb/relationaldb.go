package relationaldb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/microsoft/go-mssqldb"
	_ "modernc.org/sqlite"

	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
)

type storeMeta struct {
	table   string
	pkCol   string
	columns []*proto.ColumnDef
	indexes []*proto.IndexSchema
}

type Store struct {
	proto.UnimplementedIndexedDBServer
	db      *sql.DB
	bind    bindStyle
	dialect dialect
	mu      sync.RWMutex
	meta    map[string]*storeMeta
}

func NewStore(dsn string) (*Store, error) {
	driver, connStr, style, d := parseDSN(dsn)
	db, err := sql.Open(driver, connStr)
	if err != nil {
		return nil, fmt.Errorf("relationaldb: open: %w", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("relationaldb: ping: %w", err)
	}

	s := &Store{db: db, bind: style, dialect: d, meta: make(map[string]*storeMeta)}

	if _, err := db.Exec(s.q(metadataTableSQL(d))); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("relationaldb: create metadata table: %w", err)
	}

	rows, err := db.Query("SELECT " + quoteIdent(d, "name") + ", " + quoteIdent(d, "schema_json") +
		" FROM " + quoteIdent(d, "_gestalt_stores"))
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

// ---- Lifecycle ----

func (s *Store) CreateObjectStore(ctx context.Context, req *proto.CreateObjectStoreRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.meta[req.Name]; ok {
		return &emptypb.Empty{}, nil
	}

	schema := req.GetSchema()
	ddl := createTableSQL(s.dialect, req.Name, schema)
	if _, err := s.db.ExecContext(ctx, s.q(ddl)); err != nil {
		return nil, status.Errorf(codes.Internal, "create table: %v", err)
	}

	for _, idx := range schema.GetIndexes() {
		if _, err := s.db.ExecContext(ctx, s.q(createIndexSQL(s.dialect, req.Name, idx))); err != nil {
			return nil, status.Errorf(codes.Internal, "create index: %v", err)
		}
	}

	schemaJSON, _ := json.Marshal(newStoredSchema(schema))
	_, err := s.db.ExecContext(ctx,
		s.q("INSERT INTO "+quoteIdent(s.dialect, "_gestalt_stores")+" ("+quoteIdent(s.dialect, "name")+", "+quoteIdent(s.dialect, "schema_json")+") VALUES (?, ?)"),
		req.Name, string(schemaJSON))
	if err != nil && !isDuplicateErr(err) {
		return nil, status.Errorf(codes.Internal, "insert metadata: %v", err)
	}

	s.meta[req.Name] = newStoredSchema(schema).toMeta(req.Name)
	return &emptypb.Empty{}, nil
}

func (s *Store) DeleteObjectStore(ctx context.Context, req *proto.DeleteObjectStoreRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.db.ExecContext(ctx, s.q(dropTableSQL(s.dialect, req.Name))); err != nil {
		return nil, status.Errorf(codes.Internal, "drop table: %v", err)
	}
	_, _ = s.db.ExecContext(ctx,
		s.q("DELETE FROM "+quoteIdent(s.dialect, "_gestalt_stores")+" WHERE "+quoteIdent(s.dialect, "name")+" = ?"), req.Name)
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
	args := structToArgs(req.GetRecord(), m.columns)
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
	id := extractID(record, m.pkCol)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "begin tx: %v", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, s.q(deleteByPK(s.dialect, m.table, m.pkCol)), id); err != nil {
		return nil, status.Errorf(codes.Internal, "put delete: %v", err)
	}
	args := structToArgs(record, m.columns)
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
	query, args := selectAllWithRange(s.dialect, m, req.Range)
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
	query, args := selectKeysWithRange(s.dialect, m, req.Range)
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
	query, args := countWithRange(s.dialect, m, req.Range)
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
	query, args := deleteWithRange(s.dialect, m, req.Range)
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

func selectAllWithRange(d dialect, m *storeMeta, kr *proto.KeyRange) (string, []any) {
	where, args := keyRangeWhere(d, m.pkCol, kr)
	if where == "" {
		return fmt.Sprintf("SELECT %s FROM %s", colList(d, m.columns), quoteIdent(d, m.table)), args
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s", colList(d, m.columns), quoteIdent(d, m.table), where), args
}

func selectKeysWithRange(d dialect, m *storeMeta, kr *proto.KeyRange) (string, []any) {
	pk := quoteIdent(d, m.pkCol)
	where, args := keyRangeWhere(d, m.pkCol, kr)
	if where == "" {
		return fmt.Sprintf("SELECT %s FROM %s", pk, quoteIdent(d, m.table)), args
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s", pk, quoteIdent(d, m.table), where), args
}

func countWithRange(d dialect, m *storeMeta, kr *proto.KeyRange) (string, []any) {
	where, args := keyRangeWhere(d, m.pkCol, kr)
	if where == "" {
		return fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdent(d, m.table)), args
	}
	return fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", quoteIdent(d, m.table), where), args
}

func deleteWithRange(d dialect, m *storeMeta, kr *proto.KeyRange) (string, []any) {
	where, args := keyRangeWhere(d, m.pkCol, kr)
	if where == "" {
		return fmt.Sprintf("DELETE FROM %s", quoteIdent(d, m.table)), args
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s", quoteIdent(d, m.table), where), args
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
		args = append(args, protoValueToArg(req.Values[i]))
	}

	rangeWhere, rangeArgs := keyRangeWhere(d, m.pkCol, req.Range)
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
		args = append(args, protoValueToArg(req.Values[i]))
	}

	rangeWhere, rangeArgs := keyRangeWhere(d, m.pkCol, req.Range)
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
		args = append(args, protoValueToArg(req.Values[i]))
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

func keyRangeWhere(d dialect, pkCol string, kr *proto.KeyRange) (string, []any) {
	if kr == nil {
		return "", nil
	}
	var clauses []string
	var args []any
	pk := quoteIdent(d, pkCol)

	if kr.Lower != nil && kr.Lower.GetKind() != nil {
		if _, ok := kr.Lower.GetKind().(*structpb.Value_NullValue); !ok {
			op := ">="
			if kr.LowerOpen {
				op = ">"
			}
			clauses = append(clauses, fmt.Sprintf("%s %s ?", pk, op))
			args = append(args, protoValueToArg(kr.Lower))
		}
	}
	if kr.Upper != nil && kr.Upper.GetKind() != nil {
		if _, ok := kr.Upper.GetKind().(*structpb.Value_NullValue); !ok {
			op := "<="
			if kr.UpperOpen {
				op = "<"
			}
			clauses = append(clauses, fmt.Sprintf("%s %s ?", pk, op))
			args = append(args, protoValueToArg(kr.Upper))
		}
	}
	return strings.Join(clauses, " AND "), args
}

// ---- Value conversion ----

func structToArgs(s *structpb.Struct, cols []*proto.ColumnDef) []any {
	if s == nil {
		return make([]any, len(cols))
	}
	args := make([]any, len(cols))
	for i, col := range cols {
		v, ok := s.Fields[col.Name]
		if !ok || v == nil {
			args[i] = nil
			continue
		}
		args[i] = protoValueToSQLArg(v, col.Type)
	}
	return args
}

func protoValueToSQLArg(v *structpb.Value, colType int32) any {
	switch k := v.GetKind().(type) {
	case *structpb.Value_NullValue:
		return nil
	case *structpb.Value_NumberValue:
		switch colType {
		case 1: // TypeInt
			return int64(k.NumberValue)
		case 2: // TypeFloat
			return k.NumberValue
		case 3: // TypeBool
			if k.NumberValue != 0 {
				return int16(1)
			}
			return int16(0)
		default:
			return fmt.Sprintf("%v", k.NumberValue)
		}
	case *structpb.Value_StringValue:
		return k.StringValue
	case *structpb.Value_BoolValue:
		if k.BoolValue {
			return int16(1)
		}
		return int16(0)
	case *structpb.Value_StructValue:
		b, _ := json.Marshal(k.StructValue.AsMap())
		return string(b)
	case *structpb.Value_ListValue:
		b, _ := json.Marshal(k.ListValue.AsSlice())
		return string(b)
	default:
		return nil
	}
}

func protoValueToArg(v *structpb.Value) any {
	if v == nil {
		return nil
	}
	switch k := v.GetKind().(type) {
	case *structpb.Value_StringValue:
		return k.StringValue
	case *structpb.Value_NumberValue:
		return fmt.Sprintf("%v", k.NumberValue)
	case *structpb.Value_BoolValue:
		return fmt.Sprintf("%v", k.BoolValue)
	default:
		return nil
	}
}

func extractID(s *structpb.Struct, pkCol string) string {
	if s == nil {
		return ""
	}
	v, ok := s.Fields[pkCol]
	if !ok || v == nil {
		return ""
	}
	switch k := v.GetKind().(type) {
	case *structpb.Value_StringValue:
		return k.StringValue
	case *structpb.Value_NumberValue:
		return fmt.Sprintf("%v", k.NumberValue)
	default:
		return ""
	}
}

func scanRow(row *sql.Row, cols []*proto.ColumnDef) (*structpb.Struct, error) {
	dest := make([]any, len(cols))
	for i := range cols {
		dest[i] = new(sql.NullString)
	}
	if err := row.Scan(dest...); err != nil {
		return nil, err
	}
	return destToStruct(dest, cols)
}

func scanRows(rows *sql.Rows, cols []*proto.ColumnDef) ([]*structpb.Struct, error) {
	var out []*structpb.Struct
	for rows.Next() {
		dest := make([]any, len(cols))
		for i := range cols {
			dest[i] = new(sql.NullString)
		}
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

func destToStruct(dest []any, cols []*proto.ColumnDef) (*structpb.Struct, error) {
	fields := make(map[string]*structpb.Value, len(cols))
	for i, col := range cols {
		ns := dest[i].(*sql.NullString)
		if !ns.Valid {
			fields[col.Name] = structpb.NewNullValue()
			continue
		}
		switch col.Type {
		case 1: // TypeInt
			var n int64
			fmt.Sscanf(ns.String, "%d", &n)
			fields[col.Name] = structpb.NewNumberValue(float64(n))
		case 2: // TypeFloat
			var f float64
			fmt.Sscanf(ns.String, "%g", &f)
			fields[col.Name] = structpb.NewNumberValue(f)
		case 3: // TypeBool
			fields[col.Name] = structpb.NewBoolValue(ns.String != "0" && ns.String != "false")
		default:
			fields[col.Name] = structpb.NewStringValue(ns.String)
		}
	}
	return &structpb.Struct{Fields: fields}, nil
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

func newStoredSchema(schema *proto.ObjectStoreSchema) storedSchema {
	s := storedSchema{}
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
	m := &storeMeta{table: name, pkCol: "id"}
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
