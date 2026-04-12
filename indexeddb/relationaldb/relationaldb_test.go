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

func usersSchema() *proto.ObjectStoreSchema {
	return &proto.ObjectStoreSchema{
		Indexes: []*proto.IndexSchema{
			{Name: "by_email", KeyPath: []string{"email"}, Unique: true},
		},
		Columns: []*proto.ColumnDef{
			{Name: "id", Type: 0, PrimaryKey: true, NotNull: true},
			{Name: "email", Type: 0, NotNull: true, Unique: true},
			{Name: "display_name", Type: 0},
			{Name: "created_at", Type: 4},
			{Name: "updated_at", Type: 4},
		},
	}
}

func integrationTokensSchema() *proto.ObjectStoreSchema {
	return &proto.ObjectStoreSchema{
		Indexes: []*proto.IndexSchema{
			{Name: "by_user", KeyPath: []string{"user_id"}},
			{Name: "by_lookup", KeyPath: []string{"user_id", "integration", "connection", "instance"}},
		},
		Columns: []*proto.ColumnDef{
			{Name: "id", Type: 0, PrimaryKey: true, NotNull: true},
			{Name: "user_id", Type: 0, NotNull: true},
			{Name: "integration", Type: 0, NotNull: true},
			{Name: "connection", Type: 0, NotNull: true},
			{Name: "instance", Type: 0},
			{Name: "access_token_sealed", Type: 0},
			{Name: "refresh_token_sealed", Type: 0},
			{Name: "last_refreshed_at", Type: 4},
			{Name: "created_at", Type: 4},
			{Name: "updated_at", Type: 4},
		},
	}
}

func makeUser(id, email, name string) *proto.Record {
	record, _ := gestalt.RecordToProto(map[string]any{
		"id":           id,
		"email":        email,
		"display_name": name,
		"created_at":   time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC),
		"updated_at":   time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC),
	})
	return record
}

func makeIntegrationToken(id string) *proto.Record {
	record, _ := gestalt.RecordToProto(map[string]any{
		"id":                   id,
		"user_id":              "user-1",
		"integration":          "slack",
		"connection":           "default",
		"instance":             "default",
		"access_token_sealed":  "sealed-access",
		"refresh_token_sealed": "sealed-refresh",
		"last_refreshed_at":    time.Date(2026, time.April, 12, 2, 29, 44, 0, time.UTC),
		"created_at":           time.Date(2026, time.April, 12, 2, 29, 44, 0, time.UTC),
		"updated_at":           time.Date(2026, time.April, 12, 2, 29, 44, 0, time.UTC),
	})
	return record
}

func TestFullLifecycle(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	// Create object store.
	_, err := s.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "users", Schema: usersSchema(),
	})
	if err != nil {
		t.Fatalf("CreateObjectStore: %v", err)
	}

	// Idempotent create.
	_, err = s.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "users", Schema: usersSchema(),
	})
	if err != nil {
		t.Fatalf("CreateObjectStore idempotent: %v", err)
	}

	// Add a user.
	_, err = s.Add(ctx, &proto.RecordRequest{
		Store: "users", Record: makeUser("u1", "alice@example.com", "Alice"),
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	// Get by primary key.
	resp, err := s.Get(ctx, &proto.ObjectStoreRequest{Store: "users", Id: "u1"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := resp.Record.Fields["email"].GetStringValue(); got != "alice@example.com" {
		t.Fatalf("Get email: got %q, want alice@example.com", got)
	}
	createdAt, err := gestalt.AnyFromTypedValue(resp.Record.Fields["created_at"])
	if err != nil {
		t.Fatalf("AnyFromTypedValue(created_at): %v", err)
	}
	if _, ok := createdAt.(time.Time); !ok {
		t.Fatalf("created_at type = %T, want time.Time", createdAt)
	}

	// Count.
	countResp, err := s.Count(ctx, &proto.ObjectStoreRangeRequest{Store: "users"})
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if countResp.Count != 1 {
		t.Fatalf("Count: got %d, want 1", countResp.Count)
	}

	// Put (upsert) — update the display name.
	_, err = s.Put(ctx, &proto.RecordRequest{
		Store: "users", Record: makeUser("u1", "alice@example.com", "Alice Updated"),
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	resp, _ = s.Get(ctx, &proto.ObjectStoreRequest{Store: "users", Id: "u1"})
	if got := resp.Record.Fields["display_name"].GetStringValue(); got != "Alice Updated" {
		t.Fatalf("Put display_name: got %q, want 'Alice Updated'", got)
	}

	// Index query.
	vals, _ := gestalt.TypedValuesFromAny([]any{"alice@example.com"})
	idxResp, err := s.IndexGet(ctx, &proto.IndexQueryRequest{
		Store: "users", Index: "by_email", Values: vals,
	})
	if err != nil {
		t.Fatalf("IndexGet: %v", err)
	}
	if got := idxResp.Record.Fields["id"].GetStringValue(); got != "u1" {
		t.Fatalf("IndexGet id: got %q, want u1", got)
	}

	// Delete.
	_, err = s.Delete(ctx, &proto.ObjectStoreRequest{Store: "users", Id: "u1"})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	countResp, _ = s.Count(ctx, &proto.ObjectStoreRangeRequest{Store: "users"})
	if countResp.Count != 0 {
		t.Fatalf("Count after delete: got %d, want 0", countResp.Count)
	}

	// Delete object store.
	_, err = s.DeleteObjectStore(ctx, &proto.DeleteObjectStoreRequest{Name: "users"})
	if err != nil {
		t.Fatalf("DeleteObjectStore: %v", err)
	}
	_, err = s.getMeta("users")
	if err == nil {
		t.Fatal("expected error after DeleteObjectStore, got nil")
	}
}

func TestCreateObjectStoreUsesNamespacedTables(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	if _, err := s.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "users", Schema: usersSchema(),
	}); err != nil {
		t.Fatalf("CreateObjectStore: %v", err)
	}

	meta, err := s.getMeta("users")
	if err != nil {
		t.Fatalf("getMeta: %v", err)
	}
	if meta.table != defaultTablePrefix+"users" {
		t.Fatalf("meta.table = %q, want %q", meta.table, defaultTablePrefix+"users")
	}
	if _, err := s.tableColumns(ctx, meta.table); err != nil {
		t.Fatalf("tableColumns(prefixed): %v", err)
	}
	if _, err := s.tableColumns(ctx, "users"); err == nil {
		t.Fatal("expected bare users table to be absent")
	}
}

func TestCreateObjectStoreMigratesLegacyBareStoreTable(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "legacy-provider.sqlite")
	db := openSQLiteDB(t, dsn)

	if _, err := db.Exec(metadataTableSQL(dialectSQLite)); err != nil {
		t.Fatalf("create metadata table: %v", err)
	}
	if _, err := db.Exec(createTableSQL(dialectSQLite, "users", usersSchema())); err != nil {
		t.Fatalf("create legacy users table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO "users" ("id", "email", "display_name", "created_at", "updated_at") VALUES (?, ?, ?, ?, ?)`,
		"u1", "alice@example.com", "Alice", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z",
	); err != nil {
		t.Fatalf("insert legacy user: %v", err)
	}

	legacySchemaJSON, err := json.Marshal(newStoredSchema("", usersSchema()))
	if err != nil {
		t.Fatalf("marshal legacy schema: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO "_gestalt_stores" ("name", "schema_json") VALUES (?, ?)`, "users", string(legacySchemaJSON)); err != nil {
		t.Fatalf("insert legacy metadata: %v", err)
	}

	s := testStoreWithDSN(t, dsn)
	if _, err := s.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "users", Schema: usersSchema(),
	}); err != nil {
		t.Fatalf("CreateObjectStore migrate: %v", err)
	}

	meta, err := s.getMeta("users")
	if err != nil {
		t.Fatalf("getMeta: %v", err)
	}
	if meta.table != defaultTablePrefix+"users" {
		t.Fatalf("meta.table = %q, want %q", meta.table, defaultTablePrefix+"users")
	}

	resp, err := s.Get(ctx, &proto.ObjectStoreRequest{Store: "users", Id: "u1"})
	if err != nil {
		t.Fatalf("Get migrated user: %v", err)
	}
	if got := resp.Record.Fields["email"].GetStringValue(); got != "alice@example.com" {
		t.Fatalf("Get migrated email = %q, want alice@example.com", got)
	}
}

func TestCreateObjectStoreAvoidsLegacyApplicationTableCollision(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "legacy-app.sqlite")
	db := openSQLiteDB(t, dsn)

	if _, err := db.Exec(`CREATE TABLE "integration_tokens" (
		"id" TEXT NOT NULL PRIMARY KEY,
		"user_id" TEXT NOT NULL,
		"integration" TEXT NOT NULL,
		"connection" TEXT NOT NULL,
		"instance" TEXT,
		"access_token_encrypted" TEXT NOT NULL,
		"refresh_token_encrypted" TEXT,
		"created_at" TEXT,
		"updated_at" TEXT
	)`); err != nil {
		t.Fatalf("create legacy integration_tokens table: %v", err)
	}

	s := testStoreWithDSN(t, dsn)
	if _, err := s.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "integration_tokens", Schema: integrationTokensSchema(),
	}); err != nil {
		t.Fatalf("CreateObjectStore: %v", err)
	}
	if _, err := s.Add(ctx, &proto.RecordRequest{
		Store: "integration_tokens", Record: makeIntegrationToken("tok-1"),
	}); err != nil {
		t.Fatalf("Add token: %v", err)
	}

	meta, err := s.getMeta("integration_tokens")
	if err != nil {
		t.Fatalf("getMeta: %v", err)
	}
	if meta.table != defaultTablePrefix+"integration_tokens" {
		t.Fatalf("meta.table = %q, want %q", meta.table, defaultTablePrefix+"integration_tokens")
	}

	resp, err := s.Get(ctx, &proto.ObjectStoreRequest{Store: "integration_tokens", Id: "tok-1"})
	if err != nil {
		t.Fatalf("Get token: %v", err)
	}
	if got := resp.Record.Fields["access_token_sealed"].GetStringValue(); got != "sealed-access" {
		t.Fatalf("access_token_sealed = %q, want sealed-access", got)
	}
}

func TestCreateObjectStoreRebuildsOrphanedPrefixedTable(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "orphaned-prefixed.sqlite")
	db := openSQLiteDB(t, dsn)

	if _, err := db.Exec(`CREATE TABLE "_gestalt_store_integration_tokens" (
		"id" TEXT NOT NULL PRIMARY KEY
	)`); err != nil {
		t.Fatalf("create orphaned prefixed table: %v", err)
	}

	s := testStoreWithDSN(t, dsn)
	if _, err := s.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "integration_tokens", Schema: integrationTokensSchema(),
	}); err != nil {
		t.Fatalf("CreateObjectStore: %v", err)
	}
	if _, err := s.Add(ctx, &proto.RecordRequest{
		Store: "integration_tokens", Record: makeIntegrationToken("tok-2"),
	}); err != nil {
		t.Fatalf("Add token: %v", err)
	}
}

func TestAddDuplicateReturnsAlreadyExists(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	s.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "users", Schema: usersSchema(),
	})
	s.Add(ctx, &proto.RecordRequest{
		Store: "users", Record: makeUser("u1", "alice@example.com", "Alice"),
	})

	_, err := s.Add(ctx, &proto.RecordRequest{
		Store: "users", Record: makeUser("u1", "bob@example.com", "Bob"),
	})
	if err == nil {
		t.Fatal("expected AlreadyExists error, got nil")
	}
}

func TestGetAllWithRange(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	s.CreateObjectStore(ctx, &proto.CreateObjectStoreRequest{
		Name: "users", Schema: usersSchema(),
	})
	s.Add(ctx, &proto.RecordRequest{Store: "users", Record: makeUser("a", "a@x.com", "A")})
	s.Add(ctx, &proto.RecordRequest{Store: "users", Record: makeUser("b", "b@x.com", "B")})
	s.Add(ctx, &proto.RecordRequest{Store: "users", Record: makeUser("c", "c@x.com", "C")})

	resp, err := s.GetAll(ctx, &proto.ObjectStoreRangeRequest{
		Store: "users",
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
	got := createTableSQL(dialectMySQL, "users", usersSchema())
	if strings.Contains(got, `"`) {
		t.Fatalf("createTableSQL(mysql) used double quotes: %s", got)
	}
	if !strings.Contains(got, "`id` VARCHAR(255) NOT NULL PRIMARY KEY") {
		t.Fatalf("createTableSQL(mysql) missing varchar primary key: %s", got)
	}
	if !strings.Contains(got, "`email` VARCHAR(255) NOT NULL UNIQUE") {
		t.Fatalf("createTableSQL(mysql) missing varchar unique column: %s", got)
	}
	if !strings.Contains(got, "`display_name` LONGTEXT") {
		t.Fatalf("createTableSQL(mysql) should keep non-indexed strings as LONGTEXT: %s", got)
	}
}

func TestCreateIndexSQLMySQLOmitsIfNotExists(t *testing.T) {
	got := createIndexSQL(dialectMySQL, "users", &proto.IndexSchema{
		Name: "by_email", KeyPath: []string{"email"}, Unique: true,
	}, usersSchema())
	if strings.Contains(got, "IF NOT EXISTS") {
		t.Fatalf("createIndexSQL(mysql) should omit IF NOT EXISTS: %s", got)
	}
	if !strings.Contains(got, "CREATE UNIQUE INDEX `idx_users_by_email` ON `users` (`email`)") {
		t.Fatalf("createIndexSQL(mysql) unexpected SQL: %s", got)
	}
}

func TestMetadataTableSQLMySQLUsesVarcharPrimaryKey(t *testing.T) {
	got := metadataTableSQL(dialectMySQL)
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
	got := createTableSQL(dialectMySQL, "users", usersSchema())
	if !strings.Contains(got, "`created_at` DATETIME(6)") {
		t.Fatalf("createTableSQL(mysql) missing native datetime type: %s", got)
	}
	if !strings.Contains(got, "`updated_at` DATETIME(6)") {
		t.Fatalf("createTableSQL(mysql) missing native datetime type: %s", got)
	}
}

func TestCreateIndexSQLMySQLUsesPrefixLengthsForCompositeStringIndexes(t *testing.T) {
	got := createIndexSQL(dialectMySQL, "integration_tokens", &proto.IndexSchema{
		Name: "by_lookup", KeyPath: []string{"user_id", "integration", "connection", "instance"},
	}, integrationTokensSchema())
	for _, col := range []string{"user_id", "integration", "connection", "instance"} {
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
