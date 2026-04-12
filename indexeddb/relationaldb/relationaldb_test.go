package relationaldb

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
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

func openSQLiteDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", strings.TrimPrefix(dsn, "sqlite://"))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestConfigStoreOptionsSupportsAliases(t *testing.T) {
	options, err := (config{
		Prefix:    "tenant_",
		Namespace: "analytics",
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

func TestConfigStoreOptionsRejectsConflictingAliases(t *testing.T) {
	_, err := (config{
		TablePrefix: "tenant_",
		Prefix:      "other_",
	}).storeOptions()
	if err == nil {
		t.Fatal("expected conflicting prefix aliases to fail")
	}

	_, err = (config{
		Schema:    "analytics",
		Namespace: "reporting",
	}).storeOptions()
	if err == nil {
		t.Fatal("expected conflicting schema aliases to fail")
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
	if got := s.physicalTableName("widgets"); got != "analytics.tenant_widgets" {
		t.Fatalf("physicalTableName() = %q, want %q", got, "analytics.tenant_widgets")
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

func makeWidget(id, code, title string) *proto.Record {
	record, _ := gestalt.RecordToProto(map[string]any{
		"id":         id,
		"code":       code,
		"title":      title,
		"created_at": time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC),
		"updated_at": time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC),
	})
	return record
}

func makeSampleRecord(id string) *proto.Record {
	record, _ := gestalt.RecordToProto(map[string]any{
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
	createdAt, err := gestalt.AnyFromTypedValue(resp.Record.Fields["created_at"])
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
	vals, _ := gestalt.TypedValuesFromAny([]any{"W-001"})
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
	_, err = s.getMeta("widgets")
	if err == nil {
		t.Fatal("expected error after DeleteObjectStore, got nil")
	}
}

func TestCreateObjectStoreUsesRequestedTableName(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	if _, err := s.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "widgets", Schema: widgetsSchema(),
	}); err != nil {
		t.Fatalf("CreateObjectStore: %v", err)
	}

	meta, err := s.getMeta("widgets")
	if err != nil {
		t.Fatalf("getMeta: %v", err)
	}
	if meta.table != defaultTablePrefix+"widgets" {
		t.Fatalf("meta.table = %q, want %q", meta.table, defaultTablePrefix+"widgets")
	}
	if _, err := s.tableColumns(ctx, meta.table); err != nil {
		t.Fatalf("tableColumns(requested): %v", err)
	}
	if meta.table != "widgets" {
		t.Fatalf("meta.table = %q, want %q", meta.table, "widgets")
	}
}

func TestCreateObjectStoreMigratesLegacyBareStoreTable(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "legacy-provider.sqlite")
	db := openSQLiteDB(t, dsn)

	if _, err := db.Exec(metadataTableSQL(dialectSQLite, metadataTableName)); err != nil {
		t.Fatalf("create metadata table: %v", err)
	}
	if _, err := db.Exec(createTableSQL(dialectSQLite, "widgets", widgetsSchema())); err != nil {
		t.Fatalf("create legacy widgets table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO "widgets" ("id", "code", "title", "created_at", "updated_at") VALUES (?, ?, ?, ?, ?)`,
		"w1", "W-001", "Alpha Widget", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z",
	); err != nil {
		t.Fatalf("insert legacy widget: %v", err)
	}

	legacySchemaJSON, err := json.Marshal(newStoredSchema("", widgetsSchema()))
	if err != nil {
		t.Fatalf("marshal legacy schema: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO "_gestalt_stores" ("name", "schema_json") VALUES (?, ?)`, "widgets", string(legacySchemaJSON)); err != nil {
		t.Fatalf("insert legacy metadata: %v", err)
	}

	s := testStoreWithDSN(t, dsn)
	if _, err := s.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "widgets", Schema: widgetsSchema(),
	}); err != nil {
		t.Fatalf("CreateObjectStore migrate: %v", err)
	}

	meta, err := s.getMeta("widgets")
	if err != nil {
		t.Fatalf("getMeta: %v", err)
	}
	if meta.table != defaultTablePrefix+"widgets" {
		t.Fatalf("meta.table = %q, want %q", meta.table, defaultTablePrefix+"widgets")
	}

	resp, err := s.Get(ctx, &proto.ObjectStoreRequest{Store: "widgets", Id: "w1"})
	if err != nil {
		t.Fatalf("Get migrated widget: %v", err)
	}
	if got := resp.Record.Fields["code"].GetStringValue(); got != "W-001" {
		t.Fatalf("Get migrated code = %q, want W-001", got)
	}
}

func TestCreateObjectStoreMigratesLegacyPrefixedStoreTable(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "legacy-prefixed-provider.sqlite")
	db := openSQLiteDB(t, dsn)

	if _, err := db.Exec(metadataTableSQL(dialectSQLite, metadataTableName)); err != nil {
		t.Fatalf("create metadata table: %v", err)
	}
	if _, err := db.Exec(createTableSQL(dialectSQLite, "_gestalt_store_widgets", widgetsSchema())); err != nil {
		t.Fatalf("create legacy prefixed widgets table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO "_gestalt_store_widgets" ("id", "code", "title", "created_at", "updated_at") VALUES (?, ?, ?, ?, ?)`,
		"w1", "W-001", "Alpha Widget", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z",
	); err != nil {
		t.Fatalf("insert legacy prefixed widget: %v", err)
	}

	legacySchemaJSON, err := json.Marshal(newStoredSchema("_gestalt_store_widgets", widgetsSchema()))
	if err != nil {
		t.Fatalf("marshal legacy prefixed schema: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO "_gestalt_stores" ("name", "schema_json") VALUES (?, ?)`, "widgets", string(legacySchemaJSON)); err != nil {
		t.Fatalf("insert legacy prefixed metadata: %v", err)
	}

	s := testStoreWithDSN(t, dsn)
	if _, err := s.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "widgets", Schema: widgetsSchema(),
	}); err != nil {
		t.Fatalf("CreateObjectStore migrate legacy prefixed: %v", err)
	}

	meta, err := s.getMeta("widgets")
	if err != nil {
		t.Fatalf("getMeta: %v", err)
	}
	if meta.table != defaultTablePrefix+"widgets" {
		t.Fatalf("meta.table = %q, want %q", meta.table, defaultTablePrefix+"widgets")
	}

	resp, err := s.Get(ctx, &proto.ObjectStoreRequest{Store: "widgets", Id: "w1"})
	if err != nil {
		t.Fatalf("Get migrated prefixed widget: %v", err)
	}
	if got := resp.Record.Fields["code"].GetStringValue(); got != "W-001" {
		t.Fatalf("Get migrated prefixed code = %q, want W-001", got)
	}
}

func TestCreateObjectStoreUsesRequestedGenericTableName(t *testing.T) {
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

	meta, err := s.getMeta("sample_records")
	if err != nil {
		t.Fatalf("getMeta: %v", err)
	}
	if meta.table != "sample_records" {
		t.Fatalf("meta.table = %q, want %q", meta.table, "sample_records")
	}

	resp, err := s.Get(ctx, &proto.ObjectStoreRequest{Store: "sample_records", Id: "row-1"})
	if err != nil {
		t.Fatalf("Get record: %v", err)
	}
	if got := resp.Record.Fields["payload"].GetStringValue(); got != "payload-a" {
		t.Fatalf("payload = %q, want payload-a", got)
	}
}

func TestCreateObjectStoreIgnoresOrphanedLegacyPrefixedTable(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "orphaned-prefixed.sqlite")
	db := openSQLiteDB(t, dsn)

	if _, err := db.Exec(`CREATE TABLE "_gestalt_sample_records" (
		"id" TEXT NOT NULL PRIMARY KEY
	)`); err != nil {
		t.Fatalf("create orphaned prefixed table: %v", err)
	}

	s := testStoreWithDSN(t, dsn)
	if _, err := s.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "sample_records", Schema: sampleRecordsSchema(),
	}); err != nil {
		t.Fatalf("CreateObjectStore: %v", err)
	}
	if _, err := s.Add(ctx, &proto.RecordRequest{
		Store: "sample_records", Record: makeSampleRecord("row-2"),
	}); err != nil {
		t.Fatalf("Add record: %v", err)
	}

	meta, err := s.getMeta("sample_records")
	if err != nil {
		t.Fatalf("getMeta: %v", err)
	}
	if meta.table != "sample_records" {
		t.Fatalf("meta.table = %q, want %q", meta.table, "sample_records")
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

func TestCreateTableSQLMySQLUsesMySQLSafeTypes(t *testing.T) {
	got := createTableSQL(dialectMySQL, "widgets", widgetsSchema())
	if strings.Contains(got, `"`) {
		t.Fatalf("createTableSQL(mysql) used double quotes: %s", got)
	}
	if !strings.Contains(got, "`id` VARCHAR(255) NOT NULL PRIMARY KEY") {
		t.Fatalf("createTableSQL(mysql) missing varchar primary key: %s", got)
	}
	if !strings.Contains(got, "`code` VARCHAR(255) NOT NULL UNIQUE") {
		t.Fatalf("createTableSQL(mysql) missing varchar unique column: %s", got)
	}
	if !strings.Contains(got, "`title` LONGTEXT") {
		t.Fatalf("createTableSQL(mysql) should keep non-indexed strings as LONGTEXT: %s", got)
	}
}

func TestCreateIndexSQLMySQLOmitsIfNotExists(t *testing.T) {
	got := createIndexSQL(dialectMySQL, "widgets", &proto.IndexSchema{
		Name: "by_code", KeyPath: []string{"code"}, Unique: true,
	}, widgetsSchema())
	if strings.Contains(got, "IF NOT EXISTS") {
		t.Fatalf("createIndexSQL(mysql) should omit IF NOT EXISTS: %s", got)
	}
	if !strings.Contains(got, "CREATE UNIQUE INDEX `idx_widgets_by_code` ON `widgets` (`code`)") {
		t.Fatalf("createIndexSQL(mysql) unexpected SQL: %s", got)
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

func TestCreateTableSQLMySQLUsesNativeTimeType(t *testing.T) {
	got := createTableSQL(dialectMySQL, "widgets", widgetsSchema())
	if !strings.Contains(got, "`created_at` DATETIME(6)") {
		t.Fatalf("createTableSQL(mysql) missing native datetime type: %s", got)
	}
	if !strings.Contains(got, "`updated_at` DATETIME(6)") {
		t.Fatalf("createTableSQL(mysql) missing native datetime type: %s", got)
	}
}

func TestCreateTableSQLSupportsQualifiedNames(t *testing.T) {
	got := createTableSQL(dialectPostgres, "analytics.widgets", widgetsSchema())
	if !strings.Contains(got, `CREATE TABLE IF NOT EXISTS "analytics"."widgets"`) {
		t.Fatalf("createTableSQL(postgres) should quote qualified table names: %s", got)
	}
}

func TestCreateIndexSQLUsesBaseTableNameForQualifiedTables(t *testing.T) {
	got := createIndexSQL(dialectPostgres, "analytics.widgets", &proto.IndexSchema{
		Name: "by_code", KeyPath: []string{"code"}, Unique: true,
	}, widgetsSchema())
	if !strings.Contains(got, `CREATE UNIQUE INDEX IF NOT EXISTS "idx_widgets_by_code"`) {
		t.Fatalf("createIndexSQL(postgres) should derive index name from base table name: %s", got)
	}
	if !strings.Contains(got, `ON "analytics"."widgets" ("code")`) {
		t.Fatalf("createIndexSQL(postgres) should target the qualified table name: %s", got)
	}
}

func TestCreateIndexSQLMySQLUsesPrefixLengthsForCompositeStringIndexes(t *testing.T) {
	got := createIndexSQL(dialectMySQL, "sample_records", &proto.IndexSchema{
		Name: "by_lookup", KeyPath: []string{"owner_id", "category", "region", "variant"},
	}, sampleRecordsSchema())
	for _, col := range []string{"owner_id", "category", "region", "variant"} {
		if !strings.Contains(got, "`"+col+"`(128)") {
			t.Fatalf("createIndexSQL(mysql) missing prefix length for %s: %s", col, got)
		}
	}
}

func TestAnyToSQLArgTypeTimeUsesNativeTime(t *testing.T) {
	timestamp := time.Date(2026, time.April, 12, 1, 27, 45, 123456000, time.FixedZone("test", -5*60*60))
	arg, err := anyToSQLArg(timestamp, 4)
	if err != nil {
		t.Fatalf("anyToSQLArg(time.Time): %v", err)
	}
	got, ok := arg.(time.Time)
	if !ok {
		t.Fatalf("anyToSQLArg(time.Time) type = %T, want time.Time", arg)
	}
	if !got.Equal(timestamp.UTC()) {
		t.Fatalf("anyToSQLArg(time.Time) = %s, want %s", got.Format(time.RFC3339Nano), timestamp.UTC().Format(time.RFC3339Nano))
	}

	arg, err = anyToSQLArg("2026-04-12T01:27:45Z", 4)
	if err != nil {
		t.Fatalf("anyToSQLArg(string): %v", err)
	}
	got, ok = arg.(time.Time)
	if !ok {
		t.Fatalf("anyToSQLArg(string) type = %T, want time.Time", arg)
	}
	if got.Format(time.RFC3339Nano) != "2026-04-12T01:27:45Z" {
		t.Fatalf("anyToSQLArg(string) = %s, want 2026-04-12T01:27:45Z", got.Format(time.RFC3339Nano))
	}
}

func TestAnyToSQLArgTypeTimeRejectsInvalidString(t *testing.T) {
	if _, err := anyToSQLArg("definitely-not-a-time", 4); err == nil {
		t.Fatal("expected invalid time error, got nil")
	}
}

func mustTypedValue(t *testing.T, value any) *proto.TypedValue {
	t.Helper()
	pbValue, err := gestalt.TypedValueFromAny(value)
	if err != nil {
		t.Fatalf("TypedValueFromAny(%#v): %v", value, err)
	}
	return pbValue
}
