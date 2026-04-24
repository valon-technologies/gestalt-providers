package relationaldb

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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
	name           string
	table          string
	pkCol          string
	columns        []*proto.ColumnDef
	indexes        []*proto.IndexSchema
	storageVersion int
}

const (
	metadataTableName           = "_gestalt_stores"
	genericRecordsTableName     = "_gestalt_records_v2"
	genericIndexTableName       = "_gestalt_index_entries_v2"
	genericUniqueIndexTableName = "_gestalt_unique_index_entries_v2"
	defaultTablePrefix          = ""
	storageVersionLegacy        = 1
	storageVersionGeneric       = 2
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

func newStoreWithTablePrefix(dsn, tablePrefix string) (*Store, error) {
	return newStoreWithOptions(dsn, storeOptions{TablePrefix: tablePrefix})
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
		if err := json.Unmarshal([]byte(schemaJSON), &stored); err != nil {
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

func (s *Store) exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.db.ExecContext(ctx, s.q(query), args...)
}

func (s *Store) query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return queryWithRetry(ctx, s.db, s.conn, s.q(query), args...)
}

func (s *Store) scanOne(ctx context.Context, query string, args []any, scanDest ...any) error {
	return queryRowScanWithRetry(ctx, s.db, s.conn, s.q(query), args, scanDest...)
}

func (s *Store) withTx(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
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

func (s *Store) physicalTableName(store string) string {
	tableName := store
	if s.tablePrefix != "" {
		tableName = s.tablePrefix + tableName
	}
	return qualifyTableName(s.schemaName, tableName)
}

func (s *Store) ensureTable(ctx context.Context, table string, schema *proto.ObjectStoreSchema) error {
	storage := storageSchema(schema)
	ddl := createTableSQL(s.dialect, table, storage)
	if _, err := s.exec(ctx, ddl); err != nil {
		return status.Errorf(codes.Internal, "create table: %v", err)
	}
	for _, idx := range storage.GetIndexes() {
		if _, err := s.exec(ctx, createIndexSQL(s.dialect, table, idx, storage)); err != nil && !isDuplicateErr(err) {
			return status.Errorf(codes.Internal, "create index: %v", err)
		}
	}
	return nil
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

func schemaColumnsExist(existing map[string]struct{}, schema *proto.ObjectStoreSchema) bool {
	for _, col := range storageSchema(schema).GetColumns() {
		if _, ok := existing[col.Name]; !ok {
			return false
		}
	}
	return true
}

func (s *Store) tableCount(ctx context.Context, table string) (int64, error) {
	var count int64
	err := s.scanOne(ctx, "SELECT COUNT(*) FROM "+quoteTableName(s.dialect, table), nil, &count)
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
	if _, err := s.exec(ctx, dropTableSQL(s.dialect, table)); err != nil {
		return status.Errorf(codes.Internal, "drop orphaned table: %v", err)
	}
	return nil
}

func (s *Store) copyStoreRows(ctx context.Context, sourceTable, destTable string, schema *proto.ObjectStoreSchema) error {
	if sourceTable == "" || sourceTable == destTable {
		return nil
	}

	storage := storageSchema(schema)
	sourceCols, err := s.tableColumns(ctx, sourceTable)
	if err != nil || !schemaColumnsExist(sourceCols, storage) {
		return nil
	}

	destCount, err := s.tableCount(ctx, destTable)
	if err != nil {
		return status.Errorf(codes.Internal, "count destination rows: %v", err)
	}
	if destCount != 0 {
		return nil
	}

	cols := make([]string, len(storage.GetColumns()))
	for i, col := range storage.GetColumns() {
		cols[i] = quoteIdent(s.dialect, col.Name)
	}
	colList := strings.Join(cols, ", ")
	query := "INSERT INTO " + quoteTableName(s.dialect, destTable) +
		" (" + colList + ") SELECT " + colList + " FROM " + quoteTableName(s.dialect, sourceTable)
	if _, err := s.exec(ctx, query); err != nil {
		return status.Errorf(codes.Internal, "copy existing rows: %v", err)
	}
	return nil
}

func (s *Store) loadLegacyStoreRecords(ctx context.Context, m *storeMeta) ([]*proto.Record, error) {
	if isDocumentStore(m) {
		return loadDocumentStoreRecords(ctx, &sqlStoreView{
			db: s.db, conn: s.conn, dialect: s.dialect, bind: s.bind,
		}, m, nil)
	}
	query, args, err := selectAllWithRange(s.dialect, m, nil)
	if err != nil {
		return nil, err
	}
	rows, err := s.query(ctx, query, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load legacy rows: %v", err)
	}
	defer rows.Close()
	records, err := scanRows(rows, m.columns)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load legacy rows: %v", err)
	}
	return records, nil
}

func (s *Store) migrateLegacyStoreToGeneric(ctx context.Context, store string, existing *storeMeta, tableName string, schema *proto.ObjectStoreSchema) error {
	if err := s.ensureGenericTables(ctx); err != nil {
		return status.Errorf(codes.Internal, "create generic tables: %v", err)
	}

	records, err := s.loadLegacyStoreRecords(ctx, existing)
	if err != nil {
		return err
	}

	meta := newStoredSchema(tableName, schema, storageVersionGeneric).toMeta(store)
	if err := s.withTx(ctx, func(txCtx context.Context, tx *sql.Tx) error {
		if err := s.clearGenericStoreTables(txCtx, tx, store); err != nil {
			return err
		}
		for _, record := range records {
			primary, err := extractGenericPrimaryKey(record, meta)
			if err != nil {
				return err
			}
			payload, err := marshalRecordBlob(record)
			if err != nil {
				return err
			}
			uniqueRows, nonUniqueRows, err := buildGenericIndexRows(record, meta, primary)
			if err != nil {
				return err
			}
			if err := s.upsertGenericRecord(txCtx, tx, store, primary, payload); err != nil {
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
	}); err != nil {
		return err
	}

	if err := s.persistStoreMetadata(ctx, store, tableName, schema, storageVersionGeneric); err != nil {
		return err
	}
	return nil
}

func (s *Store) persistStoreMetadata(ctx context.Context, storeName, tableName string, schema *proto.ObjectStoreSchema, storageVersion int) error {
	schemaJSON, err := json.Marshal(newStoredSchema(tableName, schema, storageVersion))
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

	s.meta[storeName] = newStoredSchema(tableName, schema, storageVersion).toMeta(storeName)
	return nil
}

// ---- Lifecycle ----

func (s *Store) CreateObjectStore(ctx context.Context, req *proto.CreateObjectStoreRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	schema := req.GetSchema()
	tableName := s.physicalTableName(req.Name)

	if existing, ok := s.meta[req.Name]; ok {
		if usesGenericStorage(existing) {
			if err := s.ensureGenericTables(ctx); err != nil {
				return nil, status.Errorf(codes.Internal, "create generic tables: %v", err)
			}
			if err := s.reindexGenericStore(ctx, req.Name, schema); err != nil {
				return nil, err
			}
			if err := s.persistStoreMetadata(ctx, req.Name, tableName, schema, storageVersionGeneric); err != nil {
				return nil, err
			}
			return &emptypb.Empty{}, nil
		}
		sourceCols, err := s.tableColumns(ctx, existing.table)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "inspect legacy table columns: %v", err)
		}
		if !schemaColumnsExist(sourceCols, storageSchema(schema)) {
			if err := s.migrateLegacyStoreToGeneric(ctx, req.Name, existing, tableName, schema); err != nil {
				return nil, err
			}
			return &emptypb.Empty{}, nil
		}
		if err := s.ensureTable(ctx, tableName, schema); err != nil {
			return nil, err
		}
		if existing.table != tableName {
			if err := s.copyStoreRows(ctx, existing.table, tableName, schema); err != nil {
				return nil, err
			}
		}
		if err := s.persistStoreMetadata(ctx, req.Name, tableName, schema, storageVersionLegacy); err != nil {
			return nil, err
		}
		return &emptypb.Empty{}, nil
	}

	if err := s.ensureGenericTables(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "create generic tables: %v", err)
	}
	if err := s.persistStoreMetadata(ctx, req.Name, tableName, schema, storageVersionGeneric); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *Store) DeleteObjectStore(ctx context.Context, req *proto.DeleteObjectStoreRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if meta, ok := s.meta[req.Name]; ok && usesGenericStorage(meta) {
		if err := s.clearGeneric(ctx, req.Name); err != nil {
			return nil, err
		}
		_, _ = s.exec(ctx,
			"DELETE FROM "+quoteTableName(s.dialect, s.metadataTable())+" WHERE "+quoteIdent(s.dialect, "name")+" = ?",
			s.metadataStoreKey(req.Name),
		)
		delete(s.meta, req.Name)
		return &emptypb.Empty{}, nil
	}

	tableName := s.physicalTableName(req.Name)
	if meta, ok := s.meta[req.Name]; ok && meta.table != "" {
		tableName = meta.table
	}

	if _, err := s.exec(ctx, dropTableSQL(s.dialect, tableName)); err != nil {
		return nil, status.Errorf(codes.Internal, "drop table: %v", err)
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
	m, err := s.getMeta(req.Store)
	if err != nil {
		return nil, err
	}
	if usesGenericStorage(m) {
		record, err := s.genericGet(ctx, req.Store, m, req.Id)
		if err != nil {
			return nil, err
		}
		return &proto.RecordResponse{Record: record}, nil
	}
	if isDocumentStore(m) {
		query := selectDocumentPayloadByPK(s.dialect, m.table, m.pkCol)
		var payload []byte
		if err := s.scanOne(ctx, query, []any{req.Id}, &payload); err != nil {
			return nil, mapSQLErr("get", err)
		}
		record, err := unmarshalRecordBlob(payload)
		if err != nil {
			return nil, err
		}
		return &proto.RecordResponse{Record: record}, nil
	}
	lookupArg, err := primaryKeyLookupArg(req.Id, m)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "lookup key: %v", err)
	}
	rows, err := s.query(ctx, selectByPK(s.dialect, m.table, m.pkCol, m.columns), lookupArg)
	if err != nil {
		return nil, mapSQLErr("get", err)
	}
	defer rows.Close()
	records, err := scanRows(rows, m.columns)
	if err != nil {
		return nil, mapSQLErr("get", err)
	}
	if len(records) == 0 {
		return nil, mapSQLErr("get", sql.ErrNoRows)
	}
	return &proto.RecordResponse{Record: records[0]}, nil
}

func (s *Store) GetKey(ctx context.Context, req *proto.ObjectStoreRequest) (*proto.KeyResponse, error) {
	m, err := s.getMeta(req.Store)
	if err != nil {
		return nil, err
	}
	if usesGenericStorage(m) {
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
	var key string
	lookupArg, err := primaryKeyLookupArg(req.Id, m)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "lookup key: %v", err)
	}
	err = s.scanOne(ctx, selectKeyByPK(s.dialect, m.table, m.pkCol), []any{lookupArg}, &key)
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
	if usesGenericStorage(m) {
		if err := s.addGeneric(ctx, req.Store, m, req.GetRecord()); err != nil {
			return nil, err
		}
		return &emptypb.Empty{}, nil
	}
	if isDocumentStore(m) {
		record := req.GetRecord()
		id, err := extractStringID(record)
		if err != nil {
			return nil, err
		}
		if err := checkDocumentUniqueIndexConflicts(ctx, s, m, record, ""); err != nil {
			return nil, err
		}
		payload, err := marshalRecordBlob(record)
		if err != nil {
			return nil, err
		}
		if _, err := s.exec(ctx, insertSQL(s.dialect, m.table, documentStorageColumns), id, payload); err != nil {
			return nil, mapSQLErr("add", err)
		}
		return &emptypb.Empty{}, nil
	}
	args, err := recordToArgs(req.GetRecord(), m.columns)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "marshal record: %v", err)
	}
	if _, err := s.exec(ctx, insertSQL(s.dialect, m.table, m.columns), args...); err != nil {
		return nil, mapSQLErr("add", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Store) Put(ctx context.Context, req *proto.RecordRequest) (*emptypb.Empty, error) {
	m, err := s.getMeta(req.Store)
	if err != nil {
		return nil, err
	}
	if usesGenericStorage(m) {
		if err := s.putGeneric(ctx, req.Store, m, req.GetRecord()); err != nil {
			return nil, err
		}
		return &emptypb.Empty{}, nil
	}
	if isDocumentStore(m) {
		record := req.GetRecord()
		id, err := extractStringID(record)
		if err != nil {
			return nil, err
		}
		if err := checkDocumentUniqueIndexConflicts(ctx, s, m, record, id); err != nil {
			return nil, err
		}
		payload, err := marshalRecordBlob(record)
		if err != nil {
			return nil, err
		}

		if err := s.withTx(ctx, func(txCtx context.Context, tx *sql.Tx) error {
			if _, err := tx.ExecContext(txCtx, s.q(deleteByPK(s.dialect, m.table, m.pkCol)), id); err != nil {
				return err
			}
			if _, err := tx.ExecContext(txCtx, s.q(insertSQL(s.dialect, m.table, documentStorageColumns)), id, payload); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return nil, mapSQLErr("put", err)
		}
		return &emptypb.Empty{}, nil
	}
	record := req.GetRecord()
	id, err := extractPrimaryKeyValue(record, m.pkCol)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "record id: %v", err)
	}
	idArg, err := anyToSQLArg(id, columnType(m, m.pkCol))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "marshal record id: %v", err)
	}

	args, err := recordToArgs(record, m.columns)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "marshal record: %v", err)
	}
	if err := s.withTx(ctx, func(txCtx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(txCtx, s.q(deleteByPK(s.dialect, m.table, m.pkCol)), idArg); err != nil {
			return err
		}
		if _, err := tx.ExecContext(txCtx, s.q(insertSQL(s.dialect, m.table, m.columns)), args...); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, mapSQLErr("put", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Store) Delete(ctx context.Context, req *proto.ObjectStoreRequest) (*emptypb.Empty, error) {
	m, err := s.getMeta(req.Store)
	if err != nil {
		return nil, err
	}
	if usesGenericStorage(m) {
		if err := s.deleteGeneric(ctx, req.Store, m, req.Id); err != nil {
			return nil, err
		}
		return &emptypb.Empty{}, nil
	}
	if err := s.deleteByPrimaryKeyValue(ctx, m, req.Id); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *Store) deleteByPrimaryKeyValue(ctx context.Context, m *storeMeta, value any) error {
	if usesGenericStorage(m) {
		return s.deleteGenericByValue(ctx, m.name, value)
	}
	arg, err := anyToSQLArg(value, columnType(m, m.pkCol))
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "marshal primary key: %v", err)
	}
	if _, err := s.exec(ctx, deleteByPK(s.dialect, m.table, m.pkCol), arg); err != nil {
		return mapSQLErr("delete", err)
	}
	return nil
}

// ---- Bulk operations ----

func (s *Store) Clear(ctx context.Context, req *proto.ObjectStoreNameRequest) (*emptypb.Empty, error) {
	m, err := s.getMeta(req.Store)
	if err != nil {
		return nil, err
	}
	if usesGenericStorage(m) {
		if err := s.clearGeneric(ctx, req.Store); err != nil {
			return nil, err
		}
		return &emptypb.Empty{}, nil
	}
	if _, err := s.exec(ctx, deleteAll(s.dialect, m.table)); err != nil {
		return nil, status.Errorf(codes.Internal, "clear: %v", err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Store) GetAll(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.RecordsResponse, error) {
	m, err := s.getMeta(req.Store)
	if err != nil {
		return nil, err
	}
	if usesGenericStorage(m) {
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
	if isDocumentStore(m) {
		records, err := loadDocumentStoreRecords(ctx, &sqlStoreView{db: s.db, conn: s.conn, dialect: s.dialect, bind: s.bind}, m, req.Range)
		if err != nil {
			return nil, err
		}
		return &proto.RecordsResponse{Records: records}, nil
	}
	query, args, err := selectAllWithRange(s.dialect, m, req.Range)
	if err != nil {
		return nil, err
	}
	rows, err := s.query(ctx, query, args...)
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
	if usesGenericStorage(m) {
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
	query, args, err := selectKeysWithRange(s.dialect, m, req.Range)
	if err != nil {
		return nil, err
	}
	rows, err := s.query(ctx, query, args...)
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
	if usesGenericStorage(m) {
		entries, err := s.genericObjectStoreEntries(ctx, req.Store, m, req.Range, true)
		if err != nil {
			return nil, err
		}
		return &proto.CountResponse{Count: int64(len(entries))}, nil
	}
	query, args, err := countWithRange(s.dialect, m, req.Range)
	if err != nil {
		return nil, err
	}
	var count int64
	if err := s.scanOne(ctx, query, args, &count); err != nil {
		return nil, status.Errorf(codes.Internal, "count: %v", err)
	}
	return &proto.CountResponse{Count: count}, nil
}

func (s *Store) DeleteRange(ctx context.Context, req *proto.ObjectStoreRangeRequest) (*proto.DeleteResponse, error) {
	m, err := s.getMeta(req.Store)
	if err != nil {
		return nil, err
	}
	if usesGenericStorage(m) {
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
	query, args, err := deleteWithRange(s.dialect, m, req.Range)
	if err != nil {
		return nil, err
	}
	result, err := s.exec(ctx, query, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "delete_range: %v", err)
	}
	n, _ := result.RowsAffected()
	return &proto.DeleteResponse{Deleted: n}, nil
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
	m, entries, err := s.queryIndexEntries(ctx, req, true)
	if err != nil {
		return nil, err
	}
	if usesGenericStorage(m) {
		deleted, err := s.deleteGenericEntries(ctx, req.Store, entries)
		if err != nil {
			return nil, err
		}
		return &proto.DeleteResponse{Deleted: deleted}, nil
	}
	var deleted int64
	for _, entry := range entries {
		if err := s.deleteByPrimaryKeyValue(ctx, m, entry.PrimaryKeyValue); err != nil {
			return nil, status.Errorf(codes.Internal, "index_delete: %v", err)
		}
		deleted++
	}
	return &proto.DeleteResponse{Deleted: deleted}, nil
}

// ---- Query builders for range and index operations ----

func (s *Store) queryIndexEntries(ctx context.Context, req *proto.IndexQueryRequest, keyOnly bool) (*storeMeta, []cursorutil.Entry, error) {
	m, err := s.getMeta(req.Store)
	if err != nil {
		return nil, nil, err
	}
	idx := findIndex(m, req.Index)
	if idx == nil {
		return nil, nil, status.Errorf(codes.NotFound, "index not found: %s", req.Index)
	}
	if usesGenericStorage(m) {
		entries, err := s.genericIndexEntries(ctx, req.Store, m, idx, req.GetValues(), req.GetRange(), keyOnly)
		if err != nil {
			return nil, nil, err
		}
		return m, entries, nil
	}
	if isDocumentStore(m) {
		records, err := loadDocumentStoreRecords(ctx, &sqlStoreView{db: s.db, conn: s.conn, dialect: s.dialect, bind: s.bind}, m, nil)
		if err != nil {
			return nil, nil, err
		}
		entries := make([]cursorutil.Entry, 0, len(records))
		for _, record := range records {
			key, ok, err := indexKeyFromRecord(record, idx)
			if err != nil {
				return nil, nil, err
			}
			if !ok {
				continue
			}
			primaryKeyValue, err := recordFieldAny(record, m.pkCol)
			if err != nil {
				return nil, nil, status.Errorf(codes.InvalidArgument, "record primary key: %v", err)
			}
			entries = append(entries, cursorutil.Entry{
				Key:             key,
				PrimaryKey:      fmt.Sprint(primaryKeyValue),
				PrimaryKeyValue: primaryKeyValue,
				Record:          record,
			})
		}
		entries, err = filterEntriesByPrefix(entries, req.GetValues())
		if err != nil {
			return nil, nil, err
		}
		entries, err = applyKeyRangeToEntries(entries, req.GetRange(), true)
		if err != nil {
			return nil, nil, err
		}
		sortCursorEntries(entries)
		if keyOnly {
			for i := range entries {
				entries[i].Record = nil
			}
		}
		return m, entries, nil
	}
	cols := m.columns
	if keyOnly {
		cols = indexQueryColumns(m, idx)
	}

	query, args, err := indexSelectQuery(s.dialect, m, req, false, colList(s.dialect, cols))
	if err != nil {
		return nil, nil, err
	}
	rows, err := s.query(ctx, query, args...)
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "index_query: %v", err)
	}
	defer rows.Close()

	records, err := scanRows(rows, cols)
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "index_query scan: %v", err)
	}

	rangeCursor := &relationalCursor{
		Snapshot: cursorutil.Snapshot{IndexCursor: true},
		meta:     m,
		index:    idx,
	}
	entries := make([]cursorutil.Entry, 0, len(records))
	for _, record := range records {
		entry, err := rangeCursor.entryFromRecord(record)
		if err != nil {
			return nil, nil, status.Errorf(codes.Internal, "index_query decode: %v", err)
		}
		entries = append(entries, entry)
	}

	entries, err = rangeCursor.ApplyRange(entries, req.GetRange())
	if err != nil {
		return nil, nil, err
	}
	sortCursorEntries(entries)
	return m, entries, nil
}

func indexQueryColumns(m *storeMeta, idx *proto.IndexSchema) []*proto.ColumnDef {
	if m == nil || idx == nil {
		return []*proto.ColumnDef{}
	}

	seen := map[string]struct{}{m.pkCol: {}}
	cols := []*proto.ColumnDef{findColumn(m, m.pkCol)}
	for _, field := range idx.KeyPath {
		if _, ok := seen[field]; ok {
			continue
		}
		col := findColumn(m, field)
		if col == nil {
			continue
		}
		seen[field] = struct{}{}
		cols = append(cols, col)
	}
	return cols
}

func findColumn(m *storeMeta, name string) *proto.ColumnDef {
	for _, col := range m.columns {
		if col.Name == name {
			return col
		}
	}
	return nil
}

func sortCursorEntries(entries []cursorutil.Entry) {
	sort.Slice(entries, func(i, j int) bool {
		if cmp := cursorutil.CompareValues(entries[i].Key, entries[j].Key); cmp != 0 {
			return cmp < 0
		}
		return cursorutil.CompareValues(entries[i].PrimaryKeyValue, entries[j].PrimaryKeyValue) < 0
	})
}

func selectAllWithRange(d dialect, m *storeMeta, kr *proto.KeyRange) (string, []any, error) {
	where, args, err := keyRangeWhere(d, m, kr)
	if err != nil {
		return "", nil, err
	}
	if where == "" {
		return fmt.Sprintf("SELECT %s FROM %s", colList(d, m.columns), quoteTableName(d, m.table)), args, nil
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s", colList(d, m.columns), quoteTableName(d, m.table), where), args, nil
}

func selectKeysWithRange(d dialect, m *storeMeta, kr *proto.KeyRange) (string, []any, error) {
	pk := quoteIdent(d, m.pkCol)
	where, args, err := keyRangeWhere(d, m, kr)
	if err != nil {
		return "", nil, err
	}
	if where == "" {
		return fmt.Sprintf("SELECT %s FROM %s", pk, quoteTableName(d, m.table)), args, nil
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s", pk, quoteTableName(d, m.table), where), args, nil
}

func countWithRange(d dialect, m *storeMeta, kr *proto.KeyRange) (string, []any, error) {
	where, args, err := keyRangeWhere(d, m, kr)
	if err != nil {
		return "", nil, err
	}
	if where == "" {
		return fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteTableName(d, m.table)), args, nil
	}
	return fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", quoteTableName(d, m.table), where), args, nil
}

func deleteWithRange(d dialect, m *storeMeta, kr *proto.KeyRange) (string, []any, error) {
	where, args, err := keyRangeWhere(d, m, kr)
	if err != nil {
		return "", nil, err
	}
	if where == "" {
		return fmt.Sprintf("DELETE FROM %s", quoteTableName(d, m.table)), args, nil
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s", quoteTableName(d, m.table), where), args, nil
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

	where := ""
	if len(clauses) > 0 {
		where = " WHERE " + strings.Join(clauses, " AND ")
	}
	limit := ""
	if limitOne {
		limit = " LIMIT 1"
	}
	return fmt.Sprintf("SELECT %s FROM %s%s%s", selectExpr, quoteTableName(d, m.table), where, limit), args, nil
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
			return append([]byte(nil), v...), nil
		case string:
			return parseStoredBytes(v), nil
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
	Table          string         `json:"table,omitempty"`
	StorageVersion int            `json:"storage_version,omitempty"`
	Columns        []storedColumn `json:"columns"`
	Indexes        []storedIndex  `json:"indexes"`
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

func newStoredSchema(table string, schema *proto.ObjectStoreSchema, storageVersion int) storedSchema {
	s := storedSchema{Table: table, StorageVersion: storageVersion}
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
	storageVersion := s.StorageVersion
	if storageVersion == 0 {
		storageVersion = storageVersionLegacy
	}
	if table == "" && storageVersion == storageVersionLegacy {
		table = name
	}
	m := &storeMeta{name: name, table: table, pkCol: "id", storageVersion: storageVersion}
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
