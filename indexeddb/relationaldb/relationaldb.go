package relationaldb

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	mssql "github.com/microsoft/go-mssqldb"
	_ "modernc.org/sqlite"

	cursorutil "github.com/valon-technologies/gestalt-providers/indexeddb/internal/cursorutil"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type storeMeta struct {
	name    string
	pkCol   string
	columns []*proto.ColumnDef
	indexes []*proto.IndexSchema
}

const (
	metadataTableName           = "_gestalt_stores"
	genericRecordsTableName     = "_gestalt_records"
	genericIndexTableName       = "_gestalt_index_entries"
	genericUniqueIndexTableName = "_gestalt_unique_index_entries"
	defaultTablePrefix          = ""
)

type storeOptions struct {
	TablePrefix string
	Schema      string
	Connection  connectionOptions
}

type Store struct {
	proto.UnimplementedIndexedDBServer
	db          *sql.DB
	bind        bindStyle
	dialect     dialect
	schemaName  string
	tablePrefix string
	conn        connectionOptions
	mu          sync.RWMutex
	meta        map[string]*storeMeta
}

func NewStore(dsn string) (*Store, error) {
	return newStoreWithOptions(dsn, storeOptions{TablePrefix: defaultTablePrefix})
}

func newStoreWithOptions(dsn string, options storeOptions) (*Store, error) {
	driver, connStr, style, d := parseDSN(dsn)
	if d == dialectSQLite && options.Schema != "" {
		return nil, fmt.Errorf("relationaldb: schema is not supported for sqlite")
	}
	if err := ensureRelationalTargetExists(dsn, options); err != nil {
		return nil, err
	}
	db, err := openConfiguredDB(driver, connStr, options.Connection)
	if err != nil {
		return nil, fmt.Errorf("relationaldb: open: %w", err)
	}
	if err := pingDatabase(context.Background(), db, options.Connection); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("relationaldb: ping: %w", err)
	}

	s := &Store{
		db:          db,
		bind:        style,
		dialect:     d,
		schemaName:  options.Schema,
		tablePrefix: options.TablePrefix,
		conn:        options.Connection,
		meta:        make(map[string]*storeMeta),
	}

	if _, err := execWithRetry(context.Background(), db, options.Connection, s.q(metadataTableSQL(d, s.metadataTable()))); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("relationaldb: create metadata table: %w", err)
	}
	if err := s.ensureGenericTables(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}

	if err := s.loadMetadata(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *Store) metadataTable() string {
	return qualifyTableName(s.schemaName, metadataTableName)
}

func (s *Store) genericRecordsTable() string {
	return qualifyTableName(s.schemaName, s.tablePrefix+genericRecordsTableName)
}

func (s *Store) genericIndexTable() string {
	return qualifyTableName(s.schemaName, s.tablePrefix+genericIndexTableName)
}

func (s *Store) genericUniqueIndexTable() string {
	return qualifyTableName(s.schemaName, s.tablePrefix+genericUniqueIndexTableName)
}

func (s *Store) usesNamespacedMetadata() bool {
	return s.dialect == dialectSQLite && s.schemaName == "" && s.tablePrefix != ""
}

func (s *Store) metadataStoreKey(storeName string) string {
	if !s.usesNamespacedMetadata() {
		return storeName
	}
	return s.tablePrefix + storeName
}

func (s *Store) loadMetadata(ctx context.Context) error {
	rows, err := s.query(ctx, "SELECT "+quoteIdent(s.dialect, "name")+", "+quoteIdent(s.dialect, "schema_json")+
		" FROM "+quoteTableName(s.dialect, s.metadataTable()))
	if err != nil {
		return fmt.Errorf("relationaldb: load metadata: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name, schemaJSON string
		if err := rows.Scan(&name, &schemaJSON); err != nil {
			return fmt.Errorf("relationaldb: scan metadata: %w", err)
		}
		var stored storedSchema
		if err := decodeStoredSchema([]byte(schemaJSON), &stored); err != nil {
			continue
		}
		if logicalName, ok := s.currentMetadataStoreName(name); ok {
			s.meta[logicalName] = stored.toMeta(logicalName)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("relationaldb: load metadata rows: %w", err)
	}
	return nil
}

func (s *Store) refreshStoreMetadataLocked(ctx context.Context, storeName string) error {
	var schemaJSON string
	err := s.scanOne(ctx,
		"SELECT "+quoteIdent(s.dialect, "schema_json")+
			" FROM "+quoteTableName(s.dialect, s.metadataTable())+
			" WHERE "+quoteIdent(s.dialect, "name")+" = ?",
		[]any{s.metadataStoreKey(storeName)},
		&schemaJSON,
	)
	if err == sql.ErrNoRows {
		delete(s.meta, storeName)
		return nil
	}
	if err != nil {
		return fmt.Errorf("relationaldb: refresh metadata for %q: %w", storeName, err)
	}

	var stored storedSchema
	if err := decodeStoredSchema([]byte(schemaJSON), &stored); err != nil {
		delete(s.meta, storeName)
		return nil
	}
	s.meta[storeName] = stored.toMeta(storeName)
	return nil
}

func (s *Store) refreshedStoreMetadataMatchesLocked(ctx context.Context, storeName string, schema *proto.ObjectStoreSchema) bool {
	if err := s.refreshStoreMetadataLocked(ctx, storeName); err != nil {
		return false
	}
	existing, ok := s.meta[storeName]
	return ok && genericStoreSchemaMatches(existing, schema)
}

func (s *Store) currentMetadataStoreName(key string) (string, bool) {
	if !s.usesNamespacedMetadata() {
		return key, true
	}
	if !strings.HasPrefix(key, s.tablePrefix) {
		return "", false
	}
	return strings.TrimPrefix(key, s.tablePrefix), true
}

func (s *Store) HealthCheck(ctx context.Context) error {
	return pingDatabase(ctx, s.db, s.conn)
}

// q rebinds a query with ? placeholders to the driver's style.
func (s *Store) q(query string) string {
	return rebind(s.bind, query)
}

type txContextKey struct{}

type txContextState struct {
	tx   *sql.Tx
	meta map[string]*storeMeta
}

func contextWithTx(ctx context.Context, tx *sql.Tx, meta map[string]*storeMeta) context.Context {
	return context.WithValue(ctx, txContextKey{}, txContextState{tx: tx, meta: meta})
}

func txFromContext(ctx context.Context) (*sql.Tx, bool) {
	state, ok := ctx.Value(txContextKey{}).(txContextState)
	return state.tx, ok && state.tx != nil
}

func (s *Store) exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if tx, ok := txFromContext(ctx); ok {
		return tx.ExecContext(ctx, s.q(query), args...)
	}
	return s.db.ExecContext(ctx, s.q(query), args...)
}

func (s *Store) query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if tx, ok := txFromContext(ctx); ok {
		return tx.QueryContext(ctx, s.q(query), args...)
	}
	return queryWithRetry(ctx, s.db, s.conn, s.q(query), args...)
}

func (s *Store) scanOne(ctx context.Context, query string, args []any, scanDest ...any) error {
	if tx, ok := txFromContext(ctx); ok {
		return tx.QueryRowContext(ctx, s.q(query), args...).Scan(scanDest...)
	}
	return queryRowScanWithRetry(ctx, s.db, s.conn, s.q(query), args, scanDest...)
}

func (s *Store) withTx(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	if tx, ok := txFromContext(ctx); ok {
		return fn(ctx, tx)
	}
	return withRetry(ctx, s.conn, func(attemptCtx context.Context) error {
		tx, err := s.db.BeginTx(attemptCtx, nil)
		if err != nil {
			return err
		}
		committed := false
		defer func() {
			if !committed {
				_ = tx.Rollback()
			}
		}()

		if err := fn(attemptCtx, tx); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		committed = true
		return nil
	})
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

func (s *Store) getMetaForContext(ctx context.Context, name string) (*storeMeta, error) {
	if state, ok := ctx.Value(txContextKey{}).(txContextState); ok && state.meta != nil {
		m, ok := state.meta[name]
		if !ok {
			return nil, status.Errorf(codes.NotFound, "object store not found: %s", name)
		}
		return m, nil
	}
	return s.getMeta(name)
}

func (s *Store) ensureGenericTables(ctx context.Context) error {
	statements := []string{
		createGenericRecordsTableSQL(s.dialect, s.genericRecordsTable()),
		createGenericIndexEntriesTableSQL(s.dialect, s.genericIndexTable()),
		createGenericIndexEntriesTableSQL(s.dialect, s.genericUniqueIndexTable()),
	}
	for _, stmt := range statements {
		if _, err := s.exec(ctx, stmt); err != nil {
			return fmt.Errorf("relationaldb: create generic storage table: %w", err)
		}
	}

	indexStatements := []string{
		createGenericRecordsLookupIndexSQL(s.dialect, s.genericRecordsTable()),
		createGenericRecordsStoreIndexSQL(s.dialect, s.genericRecordsTable()),
		createGenericIndexLookupIndexSQL(s.dialect, s.genericIndexTable(), false),
		createGenericIndexRecordIndexSQL(s.dialect, s.genericIndexTable()),
		createGenericIndexScanIndexSQL(s.dialect, s.genericIndexTable()),
		createGenericIndexLookupIndexSQL(s.dialect, s.genericUniqueIndexTable(), true),
		createGenericIndexRecordIndexSQL(s.dialect, s.genericUniqueIndexTable()),
		createGenericIndexScanIndexSQL(s.dialect, s.genericUniqueIndexTable()),
	}
	for _, stmt := range indexStatements {
		if _, err := s.exec(ctx, stmt); err != nil && !isDuplicateErr(err) {
			return fmt.Errorf("relationaldb: create generic storage index: %w", err)
		}
	}
	if err := s.ensureGenericMySQLLongBlobColumns(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Store) ensureGenericMySQLLongBlobColumns(ctx context.Context) error {
	if s.dialect != dialectMySQL {
		return nil
	}
	schema := strings.TrimSpace(s.schemaName)
	if schema == "" {
		if err := s.scanOne(ctx, "SELECT DATABASE()", nil, &schema); err != nil {
			return fmt.Errorf("relationaldb: inspect mysql database: %w", err)
		}
	}
	tables := []struct {
		name      string
		qualified string
		columns   []string
	}{
		{
			name:      s.tablePrefix + genericRecordsTableName,
			qualified: s.genericRecordsTable(),
			columns:   []string{"pk_bytes", "record_blob"},
		},
		{
			name:      s.tablePrefix + genericIndexTableName,
			qualified: s.genericIndexTable(),
			columns:   []string{"index_key_bytes", "pk_bytes"},
		},
		{
			name:      s.tablePrefix + genericUniqueIndexTableName,
			qualified: s.genericUniqueIndexTable(),
			columns:   []string{"index_key_bytes", "pk_bytes"},
		},
	}
	for _, table := range tables {
		if err := s.ensureMySQLLongBlobColumns(ctx, schema, table.name, table.qualified, table.columns); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ensureMySQLLongBlobColumns(ctx context.Context, schema, table, qualified string, columns []string) error {
	if len(columns) == 0 {
		return nil
	}
	placeholders := make([]string, 0, len(columns))
	args := []any{schema, table}
	for _, col := range columns {
		placeholders = append(placeholders, "?")
		args = append(args, col)
	}
	rows, err := s.query(ctx,
		"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME IN ("+strings.Join(placeholders, ", ")+")",
		args...,
	)
	if err != nil {
		return fmt.Errorf("relationaldb: inspect mysql generic storage columns: %w", err)
	}
	defer rows.Close()

	columnTypes := map[string]string{}
	for rows.Next() {
		var name, dataType string
		if err := rows.Scan(&name, &dataType); err != nil {
			return fmt.Errorf("relationaldb: scan mysql generic storage column: %w", err)
		}
		columnTypes[name] = strings.ToLower(strings.TrimSpace(dataType))
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("relationaldb: inspect mysql generic storage columns: %w", err)
	}

	clauses := make([]string, 0, len(columns))
	for _, col := range columns {
		dataType, ok := columnTypes[col]
		if !ok {
			return fmt.Errorf("relationaldb: mysql generic storage column missing: %s.%s", qualified, col)
		}
		if dataType == "longblob" {
			continue
		}
		clauses = append(clauses, "MODIFY COLUMN "+quoteIdent(s.dialect, col)+" LONGBLOB NOT NULL")
	}
	if len(clauses) == 0 {
		return nil
	}
	if _, err := s.exec(ctx, "ALTER TABLE "+quoteTableName(s.dialect, qualified)+" "+strings.Join(clauses, ", ")); err != nil {
		return fmt.Errorf("relationaldb: widen mysql generic storage columns: %w", err)
	}
	return nil
}

func (s *Store) tableColumns(ctx context.Context, table string) (map[string]struct{}, error) {
	rows, err := s.query(ctx, "SELECT * FROM "+quoteTableName(s.dialect, table)+" WHERE 1 = 0")
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

func (s *Store) persistStoreMetadata(ctx context.Context, storeName string, schema *proto.ObjectStoreSchema) error {
	schemaJSON, err := json.Marshal(newStoredSchema(schema))
	if err != nil {
		return status.Errorf(codes.Internal, "marshal schema: %v", err)
	}

	if err := s.withTx(ctx, func(txCtx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(txCtx,
			s.q("DELETE FROM "+quoteTableName(s.dialect, s.metadataTable())+" WHERE "+quoteIdent(s.dialect, "name")+" = ?"),
			s.metadataStoreKey(storeName),
		); err != nil {
			return err
		}
		if _, err := tx.ExecContext(txCtx,
			s.q("INSERT INTO "+quoteTableName(s.dialect, s.metadataTable())+" ("+quoteIdent(s.dialect, "name")+", "+quoteIdent(s.dialect, "schema_json")+") VALUES (?, ?)"),
			s.metadataStoreKey(storeName), string(schemaJSON),
		); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return status.Errorf(codes.Internal, "persist metadata: %v", err)
	}

	s.meta[storeName] = newStoredSchema(schema).toMeta(storeName)
	return nil
}

// ---- Lifecycle ----

func (s *Store) CreateObjectStore(ctx context.Context, req *proto.CreateObjectStoreRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	schema := req.GetSchema()
	if err := s.ensureGenericTables(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "create generic tables: %v", err)
	}
	if err := s.refreshStoreMetadataLocked(ctx, req.Name); err != nil {
		return nil, status.Errorf(codes.Internal, "refresh metadata: %v", err)
	}

	if existing, ok := s.meta[req.Name]; ok {
		if genericStoreSchemaMatches(existing, schema) {
			return &emptypb.Empty{}, nil
		}
		if err := s.reindexGenericStore(ctx, req.Name, schema); err != nil {
			if isRetryableConnectionError(err) && s.refreshedStoreMetadataMatchesLocked(ctx, req.Name, schema) {
				return &emptypb.Empty{}, nil
			}
			return nil, err
		}
		if err := s.persistStoreMetadata(ctx, req.Name, schema); err != nil {
			if isRetryableConnectionError(err) && s.refreshedStoreMetadataMatchesLocked(ctx, req.Name, schema) {
				return &emptypb.Empty{}, nil
			}
			return nil, err
		}
		return &emptypb.Empty{}, nil
	}

	if err := s.persistStoreMetadata(ctx, req.Name, schema); err != nil {
		if isRetryableConnectionError(err) && s.refreshedStoreMetadataMatchesLocked(ctx, req.Name, schema) {
			return &emptypb.Empty{}, nil
		}
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func genericStoreSchemaMatches(existing *storeMeta, schema *proto.ObjectStoreSchema) bool {
	if existing == nil {
		return false
	}
	next := newStoredSchema(schema).toMeta(existing.name)
	return columnsMatch(existing.columns, next.columns) && indexesMatch(existing.indexes, next.indexes)
}

func columnsMatch(left, right []*proto.ColumnDef) bool {
	if len(left) != len(right) {
		return false
	}
	byName := make(map[string]*proto.ColumnDef, len(left))
	for _, col := range left {
		if col == nil {
			return false
		}
		byName[col.Name] = col
	}
	for _, col := range right {
		if col == nil {
			return false
		}
		existing, ok := byName[col.Name]
		if !ok {
			return false
		}
		if existing.Type != col.Type ||
			existing.PrimaryKey != col.PrimaryKey ||
			existing.NotNull != col.NotNull ||
			existing.Unique != col.Unique {
			return false
		}
	}
	return true
}

func indexesMatch(left, right []*proto.IndexSchema) bool {
	if len(left) != len(right) {
		return false
	}
	byName := make(map[string]*proto.IndexSchema, len(left))
	for _, idx := range left {
		if idx == nil {
			return false
		}
		byName[idx.Name] = idx
	}
	for _, idx := range right {
		if idx == nil {
			return false
		}
		existing, ok := byName[idx.Name]
		if !ok || existing.Unique != idx.Unique || len(existing.KeyPath) != len(idx.KeyPath) {
			return false
		}
		for i := range idx.KeyPath {
			if existing.KeyPath[i] != idx.KeyPath[i] {
				return false
			}
		}
	}
	return true
}

func (s *Store) DeleteObjectStore(ctx context.Context, req *proto.DeleteObjectStoreRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.meta[req.Name]; ok {
		if err := s.clearGeneric(ctx, req.Name); err != nil {
			return nil, err
		}
	}
	_, _ = s.exec(ctx,
		"DELETE FROM "+quoteTableName(s.dialect, s.metadataTable())+" WHERE "+quoteIdent(s.dialect, "name")+" = ?",
		s.metadataStoreKey(req.Name),
	)
	delete(s.meta, req.Name)
	return &emptypb.Empty{}, nil
}

// ---- Primary key CRUD ----

func (s *Store) Get(ctx context.Context, req *proto.ObjectStoreRequest) (*proto.RecordResponse, error) {
	m, err := s.getMetaForContext(ctx, req.Store)
	if err != nil {
		return nil, err
	}
	record, err := s.genericGet(ctx, req.Store, m, req.Id)
	if err != nil {
		return nil, err
	}
	return &proto.RecordResponse{Record: record}, nil
}

func (s *Store) GetKey(ctx context.Context, req *proto.ObjectStoreRequest) (*proto.KeyResponse, error) {
	m, err := s.getMetaForContext(ctx, req.Store)
	if err != nil {
		return nil, err
	}
	record, err := s.genericGet(ctx, req.Store, m, req.Id)
	if err != nil {
		return nil, err
	}
	value, err := extractGenericPrimaryKey(record, m)
	if err != nil {
		return nil, err
	}
	return &proto.KeyResponse{Key: fmt.Sprint(value.value)}, nil
}

func (s *Store) Add(ctx context.Context, req *proto.RecordRequest) (*emptypb.Empty, error) {
	m, err := s.getMetaForContext(ctx, req.Store)
	if err != nil {
		return nil, err
	}
	if err := s.addGeneric(ctx, req.Store, m, req.GetRecord()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *Store) Put(ctx context.Context, req *proto.RecordRequest) (*emptypb.Empty, error) {
	m, err := s.getMetaForContext(ctx, req.Store)
	if err != nil {
		return nil, err
	}
	if err := s.putGeneric(ctx, req.Store, m, req.GetRecord()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *Store) Delete(ctx context.Context, req *proto.ObjectStoreRequest) (*emptypb.Empty, error) {
	m, err := s.getMetaForContext(ctx, req.Store)
	if err != nil {
		return nil, err
	}
	if err := s.deleteGeneric(ctx, req.Store, m, req.Id); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// ---- Bulk operations ----

func (s *Store) Clear(ctx context.Context, req *proto.ObjectStoreNameRequest) (*emptypb.Empty, error) {
	if _, err := s.getMetaForContext(ctx, req.Store); err != nil {
		return nil, err
	}
	if err := s.clearGeneric(ctx, req.Store); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *Store) GetAll(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.RecordsResponse, error) {
	m, err := s.getMetaForContext(ctx, req.Store)
	if err != nil {
		return nil, err
	}
	entries, err := s.genericObjectStoreEntries(ctx, req.Store, m, req.Range, false)
	if err != nil {
		return nil, err
	}
	records := make([]*proto.Record, 0, len(entries))
	for _, entry := range entries {
		records = append(records, entry.Record)
	}
	return &proto.RecordsResponse{Records: records}, nil
}

func (s *Store) GetAllKeys(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.KeysResponse, error) {
	m, err := s.getMetaForContext(ctx, req.Store)
	if err != nil {
		return nil, err
	}
	entries, err := s.genericObjectStoreEntries(ctx, req.Store, m, req.Range, true)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		keys = append(keys, entry.PrimaryKey)
	}
	return &proto.KeysResponse{Keys: keys}, nil
}

func (s *Store) Count(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.CountResponse, error) {
	m, err := s.getMetaForContext(ctx, req.Store)
	if err != nil {
		return nil, err
	}
	entries, err := s.genericObjectStoreEntries(ctx, req.Store, m, req.Range, true)
	if err != nil {
		return nil, err
	}
	return &proto.CountResponse{Count: int64(len(entries))}, nil
}

func (s *Store) DeleteRange(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.DeleteResponse, error) {
	m, err := s.getMetaForContext(ctx, req.Store)
	if err != nil {
		return nil, err
	}
	entries, err := s.genericObjectStoreEntries(ctx, req.Store, m, req.Range, true)
	if err != nil {
		return nil, err
	}
	deleted, err := s.deleteGenericEntries(ctx, req.Store, entries)
	if err != nil {
		return nil, err
	}
	return &proto.DeleteResponse{Deleted: deleted}, nil
}

// ---- Index queries ----

func (s *Store) IndexGet(ctx context.Context, req *proto.IndexQueryRequest) (*proto.RecordResponse, error) {
	_, entries, err := s.queryIndexEntries(ctx, req, false)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, status.Error(codes.NotFound, "record not found")
	}
	return &proto.RecordResponse{Record: entries[0].Record}, nil
}

func (s *Store) IndexGetKey(ctx context.Context, req *proto.IndexQueryRequest) (*proto.KeyResponse, error) {
	_, entries, err := s.queryIndexEntries(ctx, req, true)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, status.Error(codes.NotFound, "key not found")
	}
	return &proto.KeyResponse{Key: entries[0].PrimaryKey}, nil
}

func (s *Store) IndexGetAll(ctx context.Context, req *proto.IndexQueryRequest) (*proto.RecordsResponse, error) {
	_, entries, err := s.queryIndexEntries(ctx, req, false)
	if err != nil {
		return nil, err
	}
	records := make([]*proto.Record, 0, len(entries))
	for _, entry := range entries {
		records = append(records, entry.Record)
	}
	return &proto.RecordsResponse{Records: records}, nil
}

func (s *Store) IndexGetAllKeys(ctx context.Context, req *proto.IndexQueryRequest) (*proto.KeysResponse, error) {
	_, entries, err := s.queryIndexEntries(ctx, req, true)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		keys = append(keys, entry.PrimaryKey)
	}
	return &proto.KeysResponse{Keys: keys}, nil
}

func (s *Store) IndexCount(ctx context.Context, req *proto.IndexQueryRequest) (*proto.CountResponse, error) {
	_, entries, err := s.queryIndexEntries(ctx, req, true)
	if err != nil {
		return nil, err
	}
	return &proto.CountResponse{Count: int64(len(entries))}, nil
}

func (s *Store) IndexDelete(ctx context.Context, req *proto.IndexQueryRequest) (*proto.DeleteResponse, error) {
	_, entries, err := s.queryIndexEntries(ctx, req, true)
	if err != nil {
		return nil, err
	}
	deleted, err := s.deleteGenericEntries(ctx, req.Store, entries)
	if err != nil {
		return nil, err
	}
	return &proto.DeleteResponse{Deleted: deleted}, nil
}

// ---- Query builders for range and index operations ----

func (s *Store) queryIndexEntries(ctx context.Context, req *proto.IndexQueryRequest, keyOnly bool) (*storeMeta, []cursorutil.Entry, error) {
	m, err := s.getMetaForContext(ctx, req.Store)
	if err != nil {
		return nil, nil, err
	}
	idx := findIndex(m, req.Index)
	if idx == nil {
		return nil, nil, status.Errorf(codes.NotFound, "index not found: %s", req.Index)
	}
	entries, err := s.genericIndexEntries(ctx, req.Store, m, idx, req.GetValues(), req.GetRange(), keyOnly)
	if err != nil {
		return nil, nil, err
	}
	return m, entries, nil
}

func sortCursorEntries(entries []cursorutil.Entry) {
	sort.Slice(entries, func(i, j int) bool {
		if cmp := cursorutil.CompareValues(entries[i].Key, entries[j].Key); cmp != 0 {
			return cmp < 0
		}
		return cursorutil.CompareValues(entries[i].PrimaryKeyValue, entries[j].PrimaryKeyValue) < 0
	})
}

func findIndex(m *storeMeta, name string) *proto.IndexSchema {
	for _, idx := range m.indexes {
		if idx.Name == name {
			return idx
		}
	}
	return nil
}

func extractPrimaryKeyValue(record *proto.Record, pkCol string) (any, error) {
	if record == nil {
		return nil, status.Error(codes.InvalidArgument, "record is required")
	}
	v, ok := record.Fields[pkCol]
	if !ok || v == nil {
		return nil, status.Errorf(codes.InvalidArgument, "record missing primary key %q", pkCol)
	}
	value, err := gestalt.AnyFromTypedValue(v)
	if err != nil {
		return nil, err
	}
	return value, nil
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

func parseStoredBytes(raw string) []byte {
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return []byte(raw)
	}
	return decoded
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
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}

	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) {
		switch myErr.Number {
		case 1022, 1062, 1169:
			return true
		}
	}

	var msErr mssql.Error
	if errors.As(err, &msErr) {
		switch msErr.Number {
		case 2601, 2627, 2714:
			return true
		}
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unique") || strings.Contains(msg, "duplicate") || strings.Contains(msg, "constraint")
}

// ---- Schema persistence ----

type storedSchema struct {
	Columns []storedColumn `json:"columns"`
	Indexes []storedIndex  `json:"indexes"`
}

func decodeStoredSchema(data []byte, out *storedSchema) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("unexpected trailing JSON")
		}
		return err
	}
	return nil
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
	m := &storeMeta{name: name, pkCol: "id"}
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
