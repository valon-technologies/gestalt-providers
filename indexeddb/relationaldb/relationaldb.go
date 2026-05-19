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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type storeMeta struct {
	name    string
	pkCol   string
	columns []gestalt.ColumnDef
	indexes []gestalt.IndexSchema
}

const (
	metadataTableName           = "_gestalt_stores"
	genericRecordsTableName     = "_gestalt_records"
	genericIndexTableName       = "_gestalt_index_entries"
	genericUniqueIndexTableName = "_gestalt_unique_index_entries"
	defaultTablePrefix          = ""
)

type storeOptions struct {
	TablePrefix       string
	Schema            string
	Connection        connectionOptions
	MetadataKeyPrefix bool
}

type Store struct {
	db                *sql.DB
	ownsDB            bool
	bind              bindStyle
	dialect           dialect
	schemaName        string
	tablePrefix       string
	metadataKeyPrefix bool
	conn              connectionOptions
	lifecycle         *storeLifecycle
	mu                sync.RWMutex
}

type storeLifecycle struct {
	check func(context.Context) error
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

	s, err := newStoreWithDB(db, style, d, options, true)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return s, nil
}

func newStoreWithDB(db *sql.DB, style bindStyle, d dialect, options storeOptions, ownsDB bool) (*Store, error) {
	s := &Store{
		db:                db,
		ownsDB:            ownsDB,
		bind:              style,
		dialect:           d,
		schemaName:        options.Schema,
		tablePrefix:       options.TablePrefix,
		metadataKeyPrefix: options.MetadataKeyPrefix,
		conn:              options.Connection,
	}
	if _, err := execWithRetry(context.Background(), db, options.Connection, s.q(metadataTableSQL(d, s.metadataTable()))); err != nil {
		return nil, fmt.Errorf("relationaldb: create metadata table: %w", err)
	}
	if err := s.ensureGenericTables(context.Background()); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Store) Close() error {
	if s.db != nil && s.ownsDB {
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
	return s.metadataKeyPrefix || (s.dialect == dialectSQLite && s.schemaName == "" && s.tablePrefix != "")
}

func (s *Store) metadataStoreKey(storeName string) string {
	if !s.usesNamespacedMetadata() {
		return storeName
	}
	return s.tablePrefix + storeName
}

func (s *Store) loadStoreMetadata(ctx context.Context, storeName string) (*storeMeta, bool, error) {
	var schemaJSON string
	err := s.scanOne(ctx,
		"SELECT "+quoteIdent(s.dialect, "schema_json")+
			" FROM "+quoteTableName(s.dialect, s.metadataTable())+
			" WHERE "+quoteIdent(s.dialect, "name")+" = ?",
		[]any{s.metadataStoreKey(storeName)},
		&schemaJSON,
	)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("relationaldb: load metadata for %q: %w", storeName, err)
	}

	var stored storedSchema
	if err := decodeStoredSchema([]byte(schemaJSON), &stored); err != nil {
		return nil, false, nil
	}
	return stored.toMeta(storeName), true, nil
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

type lifecycleBypassContextKey struct{}

func contextWithLifecycleBypass(ctx context.Context) context.Context {
	return context.WithValue(ctx, lifecycleBypassContextKey{}, true)
}

func lifecycleBypassFromContext(ctx context.Context) bool {
	ok, _ := ctx.Value(lifecycleBypassContextKey{}).(bool)
	return ok
}

func (s *Store) checkLifecycle(ctx context.Context) error {
	if s.lifecycle == nil || lifecycleBypassFromContext(ctx) {
		return nil
	}
	if _, ok := txFromContext(ctx); ok {
		return nil
	}
	return s.lifecycle.check(ctx)
}

func (s *Store) exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if err := s.checkLifecycle(ctx); err != nil {
		return nil, err
	}
	if tx, ok := txFromContext(ctx); ok {
		return tx.ExecContext(ctx, s.q(query), args...)
	}
	return s.db.ExecContext(ctx, s.q(query), args...)
}

func (s *Store) query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if err := s.checkLifecycle(ctx); err != nil {
		return nil, err
	}
	if tx, ok := txFromContext(ctx); ok {
		return tx.QueryContext(ctx, s.q(query), args...)
	}
	return queryWithRetry(ctx, s.db, s.conn, s.q(query), args...)
}

func (s *Store) scanOne(ctx context.Context, query string, args []any, scanDest ...any) error {
	if err := s.checkLifecycle(ctx); err != nil {
		return err
	}
	if tx, ok := txFromContext(ctx); ok {
		return tx.QueryRowContext(ctx, s.q(query), args...).Scan(scanDest...)
	}
	return queryRowScanWithRetry(ctx, s.db, s.conn, s.q(query), args, scanDest...)
}

func (s *Store) withTx(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	if err := s.checkLifecycle(ctx); err != nil {
		return err
	}
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

func (s *Store) getMeta(ctx context.Context, name string) (*storeMeta, error) {
	m, ok, err := s.loadStoreMetadata(ctx, name)
	if err != nil {
		return nil, preserveStatusOrInternal("load metadata for %q: %v", name, err)
	}
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
	return s.getMeta(ctx, name)
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

func (s *Store) persistStoreMetadata(ctx context.Context, storeName string, schema gestalt.ObjectStoreSchema) error {
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

	return nil
}

// ---- Lifecycle ----

func (s *Store) CreateObjectStore(ctx context.Context, name string, schema gestalt.ObjectStoreSchema) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok, err := s.loadStoreMetadata(ctx, name)
	if err != nil {
		return preserveStatusOrInternal("load metadata: %v", err)
	}
	if ok {
		if genericStoreSchemaMatches(existing, schema) {
			return nil
		}
		return status.Errorf(codes.FailedPrecondition, "object store %q schema does not match; automatic schema upgrades are disabled, run an explicit migration before deploying this provider version", name)
	}

	if err := s.ensureGenericTables(ctx); err != nil {
		return status.Errorf(codes.Internal, "create generic tables: %v", err)
	}
	if err := s.persistStoreMetadata(ctx, name, schema); err != nil {
		return err
	}
	return nil
}

func genericStoreSchemaMatches(existing *storeMeta, schema gestalt.ObjectStoreSchema) bool {
	if existing == nil {
		return false
	}
	next := newStoredSchema(schema).toMeta(existing.name)
	return columnsMatch(existing.columns, next.columns) && indexesMatch(existing.indexes, next.indexes)
}

func columnsMatch(left, right []gestalt.ColumnDef) bool {
	if len(left) != len(right) {
		return false
	}
	byName := make(map[string]gestalt.ColumnDef, len(left))
	for _, col := range left {
		byName[col.Name] = col
	}
	for _, col := range right {
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

func indexesMatch(left, right []gestalt.IndexSchema) bool {
	if len(left) != len(right) {
		return false
	}
	byName := make(map[string]gestalt.IndexSchema, len(left))
	for _, idx := range left {
		byName[idx.Name] = idx
	}
	for _, idx := range right {
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

func (s *Store) DeleteObjectStore(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok, err := s.loadStoreMetadata(ctx, name); err != nil {
		return preserveStatusOrInternal("load metadata: %v", err)
	} else if ok {
		if err := s.clearGeneric(ctx, name); err != nil {
			return err
		}
	}
	_, _ = s.exec(ctx,
		"DELETE FROM "+quoteTableName(s.dialect, s.metadataTable())+" WHERE "+quoteIdent(s.dialect, "name")+" = ?",
		s.metadataStoreKey(name),
	)
	return nil
}

// ---- Primary key CRUD ----

func (s *Store) Get(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) (gestalt.Record, error) {
	m, err := s.getMetaForContext(ctx, req.Store)
	if err != nil {
		return nil, err
	}
	return s.genericGet(ctx, req.Store, m, req.ID)
}

func (s *Store) GetKey(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) (string, error) {
	m, err := s.getMetaForContext(ctx, req.Store)
	if err != nil {
		return "", err
	}
	record, err := s.genericGet(ctx, req.Store, m, req.ID)
	if err != nil {
		return "", err
	}
	value, err := extractGenericPrimaryKey(record, m)
	if err != nil {
		return "", err
	}
	return fmt.Sprint(value.value), nil
}

func (s *Store) Add(ctx context.Context, req gestalt.IndexedDBRecordRequest) error {
	m, err := s.getMetaForContext(ctx, req.Store)
	if err != nil {
		return err
	}
	return s.addGeneric(ctx, req.Store, m, req.Record)
}

func (s *Store) Put(ctx context.Context, req gestalt.IndexedDBRecordRequest) error {
	m, err := s.getMetaForContext(ctx, req.Store)
	if err != nil {
		return err
	}
	return s.putGeneric(ctx, req.Store, m, req.Record)
}

func (s *Store) Delete(ctx context.Context, req gestalt.IndexedDBObjectStoreRequest) error {
	m, err := s.getMetaForContext(ctx, req.Store)
	if err != nil {
		return err
	}
	return s.deleteGeneric(ctx, req.Store, m, req.ID)
}

// ---- Bulk operations ----

func (s *Store) Clear(ctx context.Context, store string) error {
	if _, err := s.getMetaForContext(ctx, store); err != nil {
		return err
	}
	return s.clearGeneric(ctx, store)
}

func (s *Store) GetAll(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]gestalt.Record, error) {
	m, err := s.getMetaForContext(ctx, req.Store)
	if err != nil {
		return nil, err
	}
	entries, err := s.genericObjectStoreEntries(ctx, req.Store, m, req.Range, false)
	if err != nil {
		return nil, err
	}
	records := make([]gestalt.Record, 0, len(entries))
	for _, entry := range entries {
		records = append(records, entry.Record)
	}
	return records, nil
}

func (s *Store) GetAllKeys(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) ([]string, error) {
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
	return keys, nil
}

func (s *Store) Query(ctx context.Context, req gestalt.IndexedDBObjectStoreQueryRequest) (*gestalt.IndexedDBQueryResponse, error) {
	if _, err := s.getMetaForContext(ctx, req.Store); err != nil {
		return nil, err
	}
	if len(req.Filters) > 0 {
		return nil, status.Error(codes.InvalidArgument, "query filters are not supported by relationaldb object-store scans")
	}
	for _, order := range req.OrderBy {
		if strings.TrimSpace(order.Column) != "" && strings.TrimSpace(order.Column) != "id" {
			return nil, status.Error(codes.InvalidArgument, "query order_by only supports id")
		}
		if order.Descending {
			return nil, status.Error(codes.InvalidArgument, "query order_by only supports ascending order")
		}
	}
	return s.queryGenericRecordsPage(ctx, req.Store, req.PageSize, req.PageToken, req.KeysOnly)
}

func (s *Store) Count(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) (int64, error) {
	m, err := s.getMetaForContext(ctx, req.Store)
	if err != nil {
		return 0, err
	}
	if req.Range == nil {
		count, err := s.countGenericRecords(ctx, req.Store)
		if err != nil {
			return 0, err
		}
		return count, nil
	}
	entries, err := s.genericObjectStoreEntries(ctx, req.Store, m, req.Range, true)
	if err != nil {
		return 0, err
	}
	return int64(len(entries)), nil
}

func (s *Store) DeleteRange(ctx context.Context, req gestalt.IndexedDBObjectStoreRangeRequest) (int64, error) {
	m, err := s.getMetaForContext(ctx, req.Store)
	if err != nil {
		return 0, err
	}
	entries, err := s.genericObjectStoreEntries(ctx, req.Store, m, req.Range, true)
	if err != nil {
		return 0, err
	}
	return s.deleteGenericEntries(ctx, req.Store, entries)
}

// ---- Index queries ----

func (s *Store) IndexGet(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (gestalt.Record, error) {
	_, entries, err := s.queryIndexEntries(ctx, req, false)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, status.Error(codes.NotFound, "record not found")
	}
	return entries[0].Record, nil
}

func (s *Store) IndexGetKey(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (string, error) {
	_, entries, err := s.queryIndexEntries(ctx, req, true)
	if err != nil {
		return "", err
	}
	if len(entries) == 0 {
		return "", status.Error(codes.NotFound, "key not found")
	}
	return entries[0].PrimaryKey, nil
}

func (s *Store) IndexGetAll(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]gestalt.Record, error) {
	_, entries, err := s.queryIndexEntries(ctx, req, false)
	if err != nil {
		return nil, err
	}
	records := make([]gestalt.Record, 0, len(entries))
	for _, entry := range entries {
		records = append(records, entry.Record)
	}
	return records, nil
}

func (s *Store) IndexGetAllKeys(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) ([]string, error) {
	_, entries, err := s.queryIndexEntries(ctx, req, true)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		keys = append(keys, entry.PrimaryKey)
	}
	return keys, nil
}

func (s *Store) IndexCount(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (int64, error) {
	_, entries, err := s.queryIndexEntries(ctx, req, true)
	if err != nil {
		return 0, err
	}
	return int64(len(entries)), nil
}

func (s *Store) IndexDelete(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest) (int64, error) {
	_, entries, err := s.queryIndexEntries(ctx, req, true)
	if err != nil {
		return 0, err
	}
	return s.deleteGenericEntries(ctx, req.Store, entries)
}

// ---- Query builders for range and index operations ----

func (s *Store) queryIndexEntries(ctx context.Context, req gestalt.IndexedDBIndexQueryRequest, keyOnly bool) (*storeMeta, []cursorutil.Entry, error) {
	m, err := s.getMetaForContext(ctx, req.Store)
	if err != nil {
		return nil, nil, err
	}
	idx := findIndex(m, req.Index)
	if idx == nil {
		return nil, nil, status.Errorf(codes.NotFound, "index not found: %s", req.Index)
	}
	entries, err := s.genericIndexEntries(ctx, req.Store, m, idx, req.Values, req.Range, keyOnly)
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

func findIndex(m *storeMeta, name string) *gestalt.IndexSchema {
	for i := range m.indexes {
		idx := &m.indexes[i]
		if idx.Name == name {
			return idx
		}
	}
	return nil
}

func extractPrimaryKeyValue(record gestalt.Record, pkCol string) (any, error) {
	if record == nil {
		return nil, status.Error(codes.InvalidArgument, "record is required")
	}
	value, ok := record[pkCol]
	if !ok || value == nil {
		return nil, status.Errorf(codes.InvalidArgument, "record missing primary key %q", pkCol)
	}
	return value, nil
}

func columnType(m *storeMeta, name string) int32 {
	for _, col := range m.columns {
		if col.Name == name {
			return int32(col.Type)
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

func preserveStatusOrInternal(format string, args ...any) error {
	if len(args) > 0 {
		if err, ok := args[len(args)-1].(error); ok && status.Code(err) != codes.Unknown {
			return err
		}
	}
	return status.Errorf(codes.Internal, format, args...)
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

func newStoredSchema(schema gestalt.ObjectStoreSchema) storedSchema {
	s := storedSchema{}
	for _, c := range schema.Columns {
		s.Columns = append(s.Columns, storedColumn{
			Name: c.Name, Type: int32(c.Type), PrimaryKey: c.PrimaryKey, NotNull: c.NotNull, Unique: c.Unique,
		})
	}
	for _, i := range schema.Indexes {
		s.Indexes = append(s.Indexes, storedIndex{
			Name: i.Name, KeyPath: i.KeyPath, Unique: i.Unique,
		})
	}
	return s
}

func (s storedSchema) toMeta(name string) *storeMeta {
	m := &storeMeta{name: name, pkCol: "id"}
	for _, c := range s.Columns {
		col := gestalt.ColumnDef{Name: c.Name, Type: gestalt.ColumnType(c.Type), PrimaryKey: c.PrimaryKey, NotNull: c.NotNull, Unique: c.Unique}
		m.columns = append(m.columns, col)
		if c.PrimaryKey {
			m.pkCol = c.Name
		}
	}
	for _, i := range s.Indexes {
		m.indexes = append(m.indexes, gestalt.IndexSchema{Name: i.Name, KeyPath: i.KeyPath, Unique: i.Unique})
	}
	return m
}

var _ gestalt.IndexedDBProvider = (*Provider)(nil)
var _ gestalt.HealthChecker = (*Provider)(nil)
var _ gestalt.Closer = (*Provider)(nil)
