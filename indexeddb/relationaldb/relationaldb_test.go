package relationaldb

import (
	"context"
	"database/sql"
	"fmt"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	return testStoreWithDSN(t, "file:"+filepath.Join(t.TempDir(), "relationaldb.sqlite"))
}

func testStoreWithDSN(t *testing.T, dsn string) *Store {
	t.Helper()
	store, err := NewStore(dsn)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func testStoreWithOptions(t *testing.T, dsn string, options storeOptions) *Store {
	t.Helper()
	store, err := newStoreWithOptions(dsn, options)
	if err != nil {
		t.Fatalf("newStoreWithOptions: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func openSQLiteDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", strings.TrimPrefix(dsn, "sqlite://"))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestConfigStoreOptionsSupportsSchemaAndPrefixes(t *testing.T) {
	options, err := (config{
		Prefix: "tenant_",
		Schema: "analytics",
	}).storeOptions()
	if err != nil {
		t.Fatalf("storeOptions: %v", err)
	}
	if options.TablePrefix != "tenant_" {
		t.Fatalf("options.TablePrefix = %q, want %q", options.TablePrefix, "tenant_")
	}
	if options.Schema != "analytics" {
		t.Fatalf("options.Schema = %q, want %q", options.Schema, "analytics")
	}
}

func TestConfigStoreOptionsRejectsConflictingPrefixAliases(t *testing.T) {
	_, err := (config{
		TablePrefix: "tenant_",
		Prefix:      "other_",
	}).storeOptions()
	if err == nil {
		t.Fatal("expected conflicting prefix aliases to fail")
	}
}

func TestProviderConfigureAppliesConnectionSettings(t *testing.T) {
	p := New()
	err := p.Configure(context.Background(), "", map[string]any{
		"dsn": "file:" + filepath.Join(t.TempDir(), "connection-options.sqlite"),
		"connection": map[string]any{
			"max_open_conns":     7,
			"max_idle_conns":     3,
			"conn_max_lifetime":  "41m",
			"conn_max_idle_time": "2m",
			"ping_timeout":       "4s",
			"retry_attempts":     6,
			"retry_backoff":      "125ms",
		},
	})
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}

	stats := p.Store.db.Stats()
	if stats.MaxOpenConnections != 7 {
		t.Fatalf("db.Stats().MaxOpenConnections = %d, want 7", stats.MaxOpenConnections)
	}
	if p.Store.conn.MaxIdleConns == nil || *p.Store.conn.MaxIdleConns != 3 {
		t.Fatalf("store.conn.MaxIdleConns = %v, want 3", p.Store.conn.MaxIdleConns)
	}
	if p.Store.conn.ConnMaxLifetime == nil || *p.Store.conn.ConnMaxLifetime != 41*time.Minute {
		t.Fatalf("store.conn.ConnMaxLifetime = %v, want %v", p.Store.conn.ConnMaxLifetime, 41*time.Minute)
	}
	if p.Store.conn.ConnMaxIdleTime == nil || *p.Store.conn.ConnMaxIdleTime != 2*time.Minute {
		t.Fatalf("store.conn.ConnMaxIdleTime = %v, want %v", p.Store.conn.ConnMaxIdleTime, 2*time.Minute)
	}
	if p.Store.conn.PingTimeout == nil || *p.Store.conn.PingTimeout != 4*time.Second {
		t.Fatalf("store.conn.PingTimeout = %v, want %v", p.Store.conn.PingTimeout, 4*time.Second)
	}
	if p.Store.conn.RetryAttempts == nil || *p.Store.conn.RetryAttempts != 6 {
		t.Fatalf("store.conn.RetryAttempts = %v, want 6", p.Store.conn.RetryAttempts)
	}
	if p.Store.conn.RetryBackoff == nil || *p.Store.conn.RetryBackoff != 125*time.Millisecond {
		t.Fatalf("store.conn.RetryBackoff = %v, want %v", p.Store.conn.RetryBackoff, 125*time.Millisecond)
	}
	if err := p.Store.HealthCheck(context.Background()); err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
}

func TestProviderConfigureRejectsUnknownFields(t *testing.T) {
	p := New()
	err := p.Configure(context.Background(), "", map[string]any{
		"dsn":     "file:" + filepath.Join(t.TempDir(), "unknown-field.sqlite"),
		"unknown": true,
	})
	if err == nil {
		t.Fatal("expected unknown field to fail")
	}
	if !strings.Contains(err.Error(), "field unknown not found") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestProviderConfigureRejectsInvalidConnectionSettings(t *testing.T) {
	tests := []struct {
		name       string
		connection map[string]any
		want       string
	}{
		{
			name: "negative_max_open_conns",
			connection: map[string]any{
				"max_open_conns": -1,
			},
			want: "connection.max_open_conns must be >= 0",
		},
		{
			name: "max_idle_exceeds_max_open",
			connection: map[string]any{
				"max_open_conns": 4,
				"max_idle_conns": 5,
			},
			want: "connection.max_idle_conns must be <= connection.max_open_conns",
		},
		{
			name: "negative_ping_timeout",
			connection: map[string]any{
				"ping_timeout": "-1s",
			},
			want: "connection.ping_timeout must be >= 0",
		},
		{
			name: "negative_retry_attempts",
			connection: map[string]any{
				"retry_attempts": -1,
			},
			want: "connection.retry_attempts must be >= 0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := New()
			err := p.Configure(context.Background(), "", map[string]any{
				"dsn":        "file:" + filepath.Join(t.TempDir(), tc.name+".sqlite"),
				"connection": tc.connection,
			})
			if err == nil {
				t.Fatal("expected invalid connection settings to fail")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestStoreNamesUseConfiguredSchemaAndPrefix(t *testing.T) {
	s := &Store{
		schemaName:  "analytics",
		tablePrefix: "tenant_",
	}
	if got := s.metadataTable(); got != "analytics._gestalt_stores" {
		t.Fatalf("metadataTable() = %q, want %q", got, "analytics._gestalt_stores")
	}
	if got := s.genericRecordsTable(); got != "analytics.tenant__gestalt_records" {
		t.Fatalf("genericRecordsTable() = %q, want %q", got, "analytics.tenant__gestalt_records")
	}
}

func TestNewStoreWithSchemaRejectsSQLite(t *testing.T) {
	_, err := newStoreWithOptions("file:"+filepath.Join(t.TempDir(), "relationaldb.sqlite"), storeOptions{
		Schema: "analytics",
	})
	if err == nil {
		t.Fatal("expected sqlite schema config to fail")
	}
	if !strings.Contains(err.Error(), "schema is not supported for sqlite") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func widgetsSchema() *proto.ObjectStoreSchema {
	return &proto.ObjectStoreSchema{
		Indexes: []*proto.IndexSchema{
			{Name: "by_code", KeyPath: []string{"code"}, Unique: true},
		},
		Columns: []*proto.ColumnDef{
			{Name: "id", Type: 0, PrimaryKey: true, NotNull: true},
			{Name: "code", Type: 0, NotNull: true, Unique: true},
			{Name: "title", Type: 0},
			{Name: "created_at", Type: 4},
			{Name: "updated_at", Type: 4},
		},
	}
}

func sampleRecordsSchema() *proto.ObjectStoreSchema {
	return &proto.ObjectStoreSchema{
		Indexes: []*proto.IndexSchema{
			{Name: "by_owner", KeyPath: []string{"owner_id"}},
			{Name: "by_lookup", KeyPath: []string{"owner_id", "category", "region", "variant"}},
		},
		Columns: []*proto.ColumnDef{
			{Name: "id", Type: 0, PrimaryKey: true, NotNull: true},
			{Name: "owner_id", Type: 0, NotNull: true},
			{Name: "category", Type: 0, NotNull: true},
			{Name: "region", Type: 0, NotNull: true},
			{Name: "variant", Type: 0},
			{Name: "payload", Type: 0},
			{Name: "backup_payload", Type: 0},
			{Name: "last_seen_at", Type: 4},
			{Name: "created_at", Type: 4},
			{Name: "updated_at", Type: 4},
		},
	}
}

func intPrimaryKeySchema() *proto.ObjectStoreSchema {
	return &proto.ObjectStoreSchema{
		Columns: []*proto.ColumnDef{
			{Name: "id", Type: 1, PrimaryKey: true, NotNull: true},
			{Name: "title", Type: 0},
		},
	}
}

func makeWidget(id, code, title string) *proto.Record {
	record, _ := RecordToProto(map[string]any{
		"id":         id,
		"code":       code,
		"title":      title,
		"created_at": time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC),
		"updated_at": time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC),
	})
	return record
}

func makeSampleRecord(id string) *proto.Record {
	record, _ := RecordToProto(map[string]any{
		"id":             id,
		"owner_id":       "owner-1",
		"category":       "alpha",
		"region":         "east",
		"variant":        "v1",
		"payload":        "payload-a",
		"backup_payload": "payload-b",
		"last_seen_at":   time.Date(2026, time.April, 12, 2, 29, 44, 0, time.UTC),
		"created_at":     time.Date(2026, time.April, 12, 2, 29, 44, 0, time.UTC),
		"updated_at":     time.Date(2026, time.April, 12, 2, 29, 44, 0, time.UTC),
	})
	return record
}

func makeIntPrimaryKeyRecord(id int64, title string) *proto.Record {
	record, _ := RecordToProto(map[string]any{
		"id":    id,
		"title": title,
	})
	return record
}

func TestFullLifecycle(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	// Create object store.
	_, err := s.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "widgets", Schema: widgetsSchema(),
	})
	if err != nil {
		t.Fatalf("CreateObjectStore: %v", err)
	}

	// Idempotent create.
	_, err = s.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "widgets", Schema: widgetsSchema(),
	})
	if err != nil {
		t.Fatalf("CreateObjectStore idempotent: %v", err)
	}

	// Add a row.
	_, err = s.Add(ctx, &proto.RecordRequest{
		Store: "widgets", Record: makeWidget("w1", "W-001", "Alpha Widget"),
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	// Get by primary key.
	resp, err := s.Get(ctx, &proto.ObjectStoreRequest{Store: "widgets", Id: "w1"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := resp.Record.Fields["code"].GetStringValue(); got != "W-001" {
		t.Fatalf("Get code: got %q, want W-001", got)
	}
	createdAt, err := AnyFromTypedValue(resp.Record.Fields["created_at"])
	if err != nil {
		t.Fatalf("AnyFromTypedValue(created_at): %v", err)
	}
	if _, ok := createdAt.(time.Time); !ok {
		t.Fatalf("created_at type = %T, want time.Time", createdAt)
	}

	// Count.
	countResp, err := s.Count(ctx, &proto.ObjectStoreRangeRequest{Store: "widgets"})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if countResp.Count != 1 {
		t.Fatalf("Count: got %d, want 1", countResp.Count)
	}

	// Put (upsert) — update the title.
	_, err = s.Put(ctx, &proto.RecordRequest{
		Store: "widgets", Record: makeWidget("w1", "W-001", "Updated Widget"),
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	resp, _ = s.Get(ctx, &proto.ObjectStoreRequest{Store: "widgets", Id: "w1"})
	if got := resp.Record.Fields["title"].GetStringValue(); got != "Updated Widget" {
		t.Fatalf("Put title: got %q, want 'Updated Widget'", got)
	}

	// Index query.
	vals, _ := TypedValuesFromAny([]any{"W-001"})
	idxResp, err := s.IndexGet(ctx, &proto.IndexQueryRequest{
		Store: "widgets", Index: "by_code", Values: vals,
	})
	if err != nil {
		t.Fatalf("IndexGet: %v", err)
	}
	if got := idxResp.Record.Fields["id"].GetStringValue(); got != "w1" {
		t.Fatalf("IndexGet id: got %q, want w1", got)
	}

	// Delete.
	_, err = s.Delete(ctx, &proto.ObjectStoreRequest{Store: "widgets", Id: "w1"})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	countResp, _ = s.Count(ctx, &proto.ObjectStoreRangeRequest{Store: "widgets"})
	if countResp.Count != 0 {
		t.Fatalf("Count after delete: got %d, want 0", countResp.Count)
	}

	// Delete object store.
	_, err = s.DeleteObjectStore(ctx, &proto.DeleteObjectStoreRequest{Name: "widgets"})
	if err != nil {
		t.Fatalf("DeleteObjectStore: %v", err)
	}
	_, err = s.getMeta(ctx, "widgets")
	if err == nil {
		t.Fatal("expected error after DeleteObjectStore, got nil")
	}
}

func TestCreateObjectStoreUsesGenericTables(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	if _, err := s.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "widgets", Schema: widgetsSchema(),
	}); err != nil {
		t.Fatalf("CreateObjectStore: %v", err)
	}

	meta, err := s.getMeta(ctx, "widgets")
	if err != nil {
		t.Fatalf("getMeta: %v", err)
	}
	if got := meta.name; got != "widgets" {
		t.Fatalf("meta.name = %q, want widgets", got)
	}
	if _, err := s.tableColumns(ctx, s.genericRecordsTable()); err != nil {
		t.Fatalf("tableColumns(records): %v", err)
	}
	if _, err := s.tableColumns(ctx, s.genericIndexTable()); err != nil {
		t.Fatalf("tableColumns(index_entries): %v", err)
	}
}

func TestCreateObjectStorePersistsGenericRows(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	if _, err := s.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "sample_records", Schema: sampleRecordsSchema(),
	}); err != nil {
		t.Fatalf("CreateObjectStore: %v", err)
	}
	if _, err := s.Add(ctx, &proto.RecordRequest{
		Store: "sample_records", Record: makeSampleRecord("row-1"),
	}); err != nil {
		t.Fatalf("Add record: %v", err)
	}

	resp, err := s.Get(ctx, &proto.ObjectStoreRequest{Store: "sample_records", Id: "row-1"})
	if err != nil {
		t.Fatalf("Get record: %v", err)
	}
	if got := resp.Record.Fields["payload"].GetStringValue(); got != "payload-a" {
		t.Fatalf("payload = %q, want payload-a", got)
	}
}

func TestGenericStoreSupportsTypedPrimaryKeyLookup(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)

	if _, err := s.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "int_keys", Schema: intPrimaryKeySchema(),
	}); err != nil {
		t.Fatalf("CreateObjectStore: %v", err)
	}
	if _, err := s.Add(ctx, &proto.RecordRequest{
		Store: "int_keys", Record: makeIntPrimaryKeyRecord(42, "The Answer"),
	}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	resp, err := s.Get(ctx, &proto.ObjectStoreRequest{Store: "int_keys", Id: "42"})
	if err != nil {
		t.Fatalf("Get typed primary key: %v", err)
	}
	id, err := AnyFromTypedValue(resp.Record.Fields["id"])
	if err != nil {
		t.Fatalf("AnyFromTypedValue(id): %v", err)
	}
	if got, ok := id.(int64); !ok || got != 42 {
		t.Fatalf("id = %#v (%T), want int64(42)", id, id)
	}

	if _, err := s.Delete(ctx, &proto.ObjectStoreRequest{Store: "int_keys", Id: "42"}); err != nil {
		t.Fatalf("Delete typed primary key: %v", err)
	}
	count, err := s.Count(ctx, &proto.ObjectStoreRangeRequest{Store: "int_keys"})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if got := count.GetCount(); got != 0 {
		t.Fatalf("Count after delete = %d, want 0", got)
	}
}

func TestCreateObjectStoreKeepsGenericRowsWhenSchemaUnchanged(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	if _, err := s.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "widgets", Schema: widgetsSchema(),
	}); err != nil {
		t.Fatalf("CreateObjectStore: %v", err)
	}
	if _, err := s.Add(ctx, &proto.RecordRequest{
		Store: "widgets", Record: makeWidget("w1", "W-001", "Alpha Widget"),
	}); err != nil {
		t.Fatalf("Add record: %v", err)
	}

	indexKey, err := encodeKeyValue("sentinel-index")
	if err != nil {
		t.Fatalf("encode sentinel index key: %v", err)
	}
	primary, err := encodeKeyValue("sentinel-pk")
	if err != nil {
		t.Fatalf("encode sentinel primary key: %v", err)
	}
	if err := s.withTx(ctx, func(txCtx context.Context, tx *sql.Tx) error {
		return s.insertGenericIndexRows(txCtx, tx, s.genericIndexTable(), "widgets", []genericIndexRow{{
			indexName:     "__sentinel",
			indexKeyHash:  indexKey.hash,
			indexKeyBytes: indexKey.raw,
			pkHash:        primary.hash,
			pkBytes:       primary.raw,
		}})
	}); err != nil {
		t.Fatalf("insert sentinel index row: %v", err)
	}

	if _, err := s.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "widgets", Schema: widgetsSchema(),
	}); err != nil {
		t.Fatalf("CreateObjectStore unchanged: %v", err)
	}

	rows, err := s.loadGenericIndexRows(ctx, s.genericIndexTable(), "widgets", "__sentinel")
	if err != nil {
		t.Fatalf("load sentinel index rows: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("sentinel index rows = %d, want 1", len(rows))
	}
}

func TestCountWithoutRangeDoesNotMaterializeRows(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)
	if _, err := s.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "widgets", Schema: widgetsSchema(),
	}); err != nil {
		t.Fatalf("CreateObjectStore: %v", err)
	}
	if _, err := s.Add(ctx, &proto.RecordRequest{
		Store: "widgets", Record: makeWidget("w1", "W-001", "Alpha Widget"),
	}); err != nil {
		t.Fatalf("Add record: %v", err)
	}
	if err := s.withTx(ctx, func(txCtx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(txCtx,
			s.q("UPDATE "+quoteTableName(s.dialect, s.genericRecordsTable())+
				" SET "+quoteIdent(s.dialect, "pk_bytes")+" = ?"+
				" WHERE "+quoteIdent(s.dialect, "store_name")+" = ?"),
			[]byte("not-a-proto-key"),
			"widgets",
		)
		return err
	}); err != nil {
		t.Fatalf("corrupt primary key bytes: %v", err)
	}

	count, err := s.Count(ctx, &proto.ObjectStoreRangeRequest{Store: "widgets"})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if got := count.GetCount(); got != 1 {
		t.Fatalf("Count = %d, want 1", got)
	}
}

func TestCreateObjectStoreRejectsSchemaChanges(t *testing.T) {
	ctx := context.Background()
	s := testStore(t)

	initialSchema := widgetsSchema()
	initialSchema.Indexes = nil
	if _, err := s.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "widgets", Schema: initialSchema,
	}); err != nil {
		t.Fatalf("CreateObjectStore: %v", err)
	}
	if _, err := s.Add(ctx, &proto.RecordRequest{
		Store: "widgets", Record: makeWidget("w1", "W-001", "Alpha Widget"),
	}); err != nil {
		t.Fatalf("Add record: %v", err)
	}

	trigger := fmt.Sprintf(
		"CREATE TRIGGER prevent_widget_record_delete BEFORE DELETE ON %s WHEN OLD.%s = 'widgets' BEGIN SELECT RAISE(ABORT, 'record rows must not be cleared'); END",
		quoteTableName(s.dialect, s.genericRecordsTable()),
		quoteIdent(s.dialect, "store_name"),
	)
	if _, err := s.exec(ctx, trigger); err != nil {
		t.Fatalf("create delete guard trigger: %v", err)
	}

	if _, err := s.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "widgets", Schema: widgetsSchema(),
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("CreateObjectStore schema change error = %v, want FailedPrecondition", err)
	}

	count, err := s.Count(ctx, &proto.ObjectStoreRangeRequest{Store: "widgets"})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if got := count.GetCount(); got != 1 {
		t.Fatalf("Count after rejected schema change = %d, want 1", got)
	}
	vals, _ := TypedValuesFromAny([]any{"W-001"})
	if _, err := s.IndexGet(ctx, &proto.IndexQueryRequest{
		Store: "widgets", Index: "by_code", Values: vals,
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("IndexGet after rejected schema change error = %v, want NotFound", err)
	}
}

func TestCreateObjectStoreRefreshesExternalMetadataBeforeSchemaCheck(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "metadata-refresh.sqlite")
	first := testStoreWithDSN(t, dsn)

	initialSchema := widgetsSchema()
	initialSchema.Indexes = nil
	if _, err := first.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "widgets", Schema: initialSchema,
	}); err != nil {
		t.Fatalf("CreateObjectStore initial: %v", err)
	}
	if _, err := first.Add(ctx, &proto.RecordRequest{
		Store: "widgets", Record: makeWidget("w1", "W-001", "Alpha Widget"),
	}); err != nil {
		t.Fatalf("Add record: %v", err)
	}

	second := testStoreWithDSN(t, dsn)
	if _, err := first.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "widgets", Schema: widgetsSchema(),
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("CreateObjectStore schema change error = %v, want FailedPrecondition", err)
	}

	for _, tc := range []struct {
		name  string
		table string
	}{
		{name: "prevent_widget_index_delete", table: first.genericIndexTable()},
		{name: "prevent_widget_unique_index_delete", table: first.genericUniqueIndexTable()},
	} {
		trigger := fmt.Sprintf(
			"CREATE TRIGGER %s BEFORE DELETE ON %s WHEN OLD.%s = 'widgets' BEGIN SELECT RAISE(ABORT, 'index rows must not be cleared'); END",
			tc.name,
			quoteTableName(first.dialect, tc.table),
			quoteIdent(first.dialect, "store_name"),
		)
		if _, err := first.exec(ctx, trigger); err != nil {
			t.Fatalf("create index delete guard trigger %s: %v", tc.name, err)
		}
	}

	if _, err := second.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "widgets", Schema: widgetsSchema(),
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("CreateObjectStore refreshed external metadata error = %v, want FailedPrecondition", err)
	}

	count, err := second.Count(ctx, &proto.ObjectStoreRangeRequest{Store: "widgets"})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if got := count.GetCount(); got != 1 {
		t.Fatalf("Count after rejected external schema change = %d, want 1", got)
	}
}

func TestWithRetryRetriesSchemaContentionErrors(t *testing.T) {
	attempts := 0
	retryAttempts := 1
	retryBackoff := time.Duration(0)
	err := withRetry(context.Background(), connectionOptions{
		RetryAttempts: &retryAttempts,
		RetryBackoff:  &retryBackoff,
	}, func(context.Context) error {
		attempts++
		if attempts == 1 {
			return status.Errorf(codes.Internal, "clear index rows: Error 1205 (HY000): Lock wait timeout exceeded; try restarting transaction")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("withRetry: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}

func TestSQLiteTablePrefixNamespacesMetadataAndTables(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "namespaced-metadata.sqlite")

	alpha := testStoreWithOptions(t, dsn, storeOptions{TablePrefix: "alpha_"})
	beta := testStoreWithOptions(t, dsn, storeOptions{TablePrefix: "beta_"})

	for _, tc := range []struct {
		store  *Store
		id     string
		code   string
		title  string
		prefix string
	}{
		{store: alpha, id: "a1", code: "A-001", title: "Alpha Task", prefix: "alpha_"},
		{store: beta, id: "b1", code: "B-001", title: "Beta Task", prefix: "beta_"},
	} {
		if _, err := tc.store.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
			Name: "tasks", Schema: widgetsSchema(),
		}); err != nil {
			t.Fatalf("CreateObjectStore(%s): %v", tc.prefix, err)
		}
		if _, err := tc.store.Add(ctx, &proto.RecordRequest{
			Store: "tasks", Record: makeWidget(tc.id, tc.code, tc.title),
		}); err != nil {
			t.Fatalf("Add(%s): %v", tc.prefix, err)
		}
		meta, err := tc.store.getMeta(ctx, "tasks")
		if err != nil {
			t.Fatalf("getMeta(%s): %v", tc.prefix, err)
		}
		if meta.name != "tasks" {
			t.Fatalf("meta.name(%s) = %q, want tasks", tc.prefix, meta.name)
		}
	}

	db := openSQLiteDB(t, dsn)
	rows, err := db.Query(`SELECT "name" FROM "_gestalt_stores" ORDER BY "name"`)
	if err != nil {
		t.Fatalf("query metadata rows: %v", err)
	}
	defer rows.Close()

	var metadataNames []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan metadata row: %v", err)
		}
		metadataNames = append(metadataNames, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("metadata rows err: %v", err)
	}
	if got := strings.Join(metadataNames, ","); got != "alpha_tasks,beta_tasks" {
		t.Fatalf("metadata names = %q, want %q", got, "alpha_tasks,beta_tasks")
	}

	alphaReloaded := testStoreWithOptions(t, dsn, storeOptions{TablePrefix: "alpha_"})
	betaReloaded := testStoreWithOptions(t, dsn, storeOptions{TablePrefix: "beta_"})

	alphaResp, err := alphaReloaded.Get(ctx, &proto.ObjectStoreRequest{Store: "tasks", Id: "a1"})
	if err != nil {
		t.Fatalf("Get(alpha reload): %v", err)
	}
	if got := alphaResp.Record.Fields["title"].GetStringValue(); got != "Alpha Task" {
		t.Fatalf("alpha reloaded title = %q, want %q", got, "Alpha Task")
	}
	if _, err := alphaReloaded.Get(ctx, &proto.ObjectStoreRequest{Store: "beta_tasks", Id: "b1"}); status.Code(err) != codes.NotFound {
		t.Fatalf("Get(alpha reload foreign store) error = %v, want NotFound", err)
	}

	betaResp, err := betaReloaded.Get(ctx, &proto.ObjectStoreRequest{Store: "tasks", Id: "b1"})
	if err != nil {
		t.Fatalf("Get(beta reload): %v", err)
	}
	if got := betaResp.Record.Fields["title"].GetStringValue(); got != "Beta Task" {
		t.Fatalf("beta reloaded title = %q, want %q", got, "Beta Task")
	}
	if _, err := betaReloaded.Get(ctx, &proto.ObjectStoreRequest{Store: "alpha_tasks", Id: "a1"}); status.Code(err) != codes.NotFound {
		t.Fatalf("Get(beta reload foreign store) error = %v, want NotFound", err)
	}
}

func TestAddDuplicateReturnsAlreadyExists(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	s.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "widgets", Schema: widgetsSchema(),
	})
	s.Add(ctx, &proto.RecordRequest{
		Store: "widgets", Record: makeWidget("w1", "W-001", "Alpha Widget"),
	})

	_, err := s.Add(ctx, &proto.RecordRequest{
		Store: "widgets", Record: makeWidget("w1", "W-002", "Beta Widget"),
	})
	if err == nil {
		t.Fatal("expected AlreadyExists error, got nil")
	}
}

func TestClassifyGenericRecordInsertConflictTreatsInvisibleDuplicateAsAlreadyExists(t *testing.T) {
	primary := mustEncodedKey(t, "workflow_key:abc")

	err := classifyGenericRecordInsertConflict(nil, primary)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("error = %v, want AlreadyExists", err)
	}
}

func TestClassifyGenericRecordInsertConflictTreatsMatchingDuplicateAsAlreadyExists(t *testing.T) {
	primary := mustEncodedKey(t, "workflow_key:abc")

	err := classifyGenericRecordInsertConflict(&genericRecordRow{pkBytes: primary.raw}, primary)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("error = %v, want AlreadyExists", err)
	}
}

func TestClassifyGenericRecordInsertConflictDetectsHashCollision(t *testing.T) {
	primary := mustEncodedKey(t, "workflow_key:abc")
	other := mustEncodedKey(t, "workflow_key:def")

	err := classifyGenericRecordInsertConflict(&genericRecordRow{pkBytes: other.raw}, primary)
	if status.Code(err) != codes.Internal {
		t.Fatalf("error = %v, want Internal", err)
	}
	if !strings.Contains(err.Error(), "primary key hash collision") {
		t.Fatalf("error = %v, want primary key hash collision", err)
	}
}

func TestGetAllWithRange(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	s.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "widgets", Schema: widgetsSchema(),
	})
	s.Add(ctx, &proto.RecordRequest{Store: "widgets", Record: makeWidget("a", "W-001", "Widget A")})
	s.Add(ctx, &proto.RecordRequest{Store: "widgets", Record: makeWidget("b", "W-002", "Widget B")})
	s.Add(ctx, &proto.RecordRequest{Store: "widgets", Record: makeWidget("c", "W-003", "Widget C")})

	resp, err := s.GetAll(ctx, &proto.ObjectStoreRangeRequest{
		Store: "widgets",
		Range: &proto.KeyRange{
			Lower:     mustTypedValue(t, "a"),
			Upper:     mustTypedValue(t, "c"),
			UpperOpen: true,
		},
	})
	if err != nil {
		t.Fatalf("GetAll: %v", err)
	}
	if len(resp.Records) != 2 {
		t.Fatalf("GetAll: got %d records, want 2", len(resp.Records))
	}
}

func TestRebind(t *testing.T) {
	tests := []struct {
		style bindStyle
		input string
		want  string
	}{
		{bindQuestion, "SELECT ? FROM t WHERE id = ?", "SELECT ? FROM t WHERE id = ?"},
		{bindDollar, "SELECT ? FROM t WHERE id = ?", "SELECT $1 FROM t WHERE id = $2"},
		{bindAtP, "INSERT INTO t VALUES (?, ?)", "INSERT INTO t VALUES (@p1, @p2)"},
	}
	for _, tt := range tests {
		got := rebind(tt.style, tt.input)
		if got != tt.want {
			t.Errorf("rebind(%d, %q) = %q, want %q", tt.style, tt.input, got, tt.want)
		}
	}
}

func TestMetadataTableSQLMySQLUsesVarcharPrimaryKey(t *testing.T) {
	got := metadataTableSQL(dialectMySQL, metadataTableName)
	if strings.Contains(got, `"`) {
		t.Fatalf("metadataTableSQL(mysql) used double quotes: %s", got)
	}
	if !strings.Contains(got, "`name` VARCHAR(255) NOT NULL PRIMARY KEY") {
		t.Fatalf("metadataTableSQL(mysql) missing varchar primary key: %s", got)
	}
	if !strings.Contains(got, "`schema_json` LONGTEXT NOT NULL") {
		t.Fatalf("metadataTableSQL(mysql) missing longtext schema column: %s", got)
	}
}

func TestCreateGenericRecordsTableSQLMySQLUsesLongBlobPayloads(t *testing.T) {
	got := createGenericRecordsTableSQL(dialectMySQL, "_gestalt_records")
	if !strings.Contains(got, "`pk_bytes` LONGBLOB NOT NULL") {
		t.Fatalf("createGenericRecordsTableSQL(mysql) missing longblob primary key bytes: %s", got)
	}
	if !strings.Contains(got, "`record_blob` LONGBLOB NOT NULL") {
		t.Fatalf("createGenericRecordsTableSQL(mysql) missing longblob record payload: %s", got)
	}
}

func TestCreateGenericIndexEntriesTableSQLMySQLUsesLongBlobPayloads(t *testing.T) {
	got := createGenericIndexEntriesTableSQL(dialectMySQL, "_gestalt_index_entries")
	if !strings.Contains(got, "`index_key_bytes` LONGBLOB NOT NULL") {
		t.Fatalf("createGenericIndexEntriesTableSQL(mysql) missing longblob index key bytes: %s", got)
	}
	if !strings.Contains(got, "`pk_bytes` LONGBLOB NOT NULL") {
		t.Fatalf("createGenericIndexEntriesTableSQL(mysql) missing longblob primary key bytes: %s", got)
	}
}

func mustTypedValue(t *testing.T, value any) *proto.TypedValue {
	t.Helper()
	pbValue, err := TypedValueFromAny(value)
	if err != nil {
		t.Fatalf("TypedValueFromAny(%#v): %v", value, err)
	}
	return pbValue
}

func mustEncodedKey(t *testing.T, value any) encodedKey {
	t.Helper()
	key, err := encodeKeyValue(value)
	if err != nil {
		t.Fatalf("encodeKeyValue(%#v): %v", value, err)
	}
	return key
}
