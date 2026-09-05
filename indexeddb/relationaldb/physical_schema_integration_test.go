package relationaldb_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func TestProviderRequiresMigratedPhysicalSchema(t *testing.T) {
	dsn := "file:" + filepath.Join(t.TempDir(), "relationaldb.sqlite")
	provider := relationaldb.New()
	if err := provider.Configure(context.Background(), "", map[string]any{"dsn": dsn}); err == nil || !strings.Contains(err.Error(), "go run ./cmd/migrate") {
		t.Fatalf("Configure unmigrated database error = %v, want migration instruction", err)
	}
	if err := relationaldb.Migrate(context.Background(), dsn, relationaldb.Options{}); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := provider.Configure(context.Background(), "", map[string]any{"dsn": dsn + "?mode=ro"}); err != nil {
		t.Fatalf("Configure migrated database: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
}

func TestNewStoreSupportsInMemorySQLite(t *testing.T) {
	store, err := relationaldb.NewStore(":memory:")
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ctx := context.Background()
	if err := store.CreateObjectStore(ctx, "widgets", gestalt.ObjectStoreOptions{Columns: []gestalt.ColumnDef{
		{Name: "id", Type: gestalt.TypeString, PrimaryKey: true, NotNull: true},
	}}); err != nil {
		t.Fatalf("CreateObjectStore: %v", err)
	}
	if err := store.Add(ctx, gestalt.IndexedDBRecordRequest{
		Store: "widgets", Record: gestalt.Record{"id": "widget-1"},
	}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := store.Get(ctx, gestalt.IndexedDBObjectStoreRequest{Store: "widgets", ID: "widget-1"}); err != nil {
		t.Fatalf("Get: %v", err)
	}
}
