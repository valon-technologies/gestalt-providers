package relationaldb

import (
	"context"
	"strings"
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	store, err := NewStore("file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
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
	})
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

func mustTypedValue(t *testing.T, value any) *proto.TypedValue {
	t.Helper()
	pbValue, err := gestalt.TypedValueFromAny(value)
	if err != nil {
		t.Fatalf("TypedValueFromAny(%#v): %v", value, err)
	}
	return pbValue
}
