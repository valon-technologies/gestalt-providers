package relationaldb

import (
	"context"
	"path/filepath"
	"testing"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	idb "github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

func TestObjectStoreGetAllAnyOfContract(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "object-store-any-of.sqlite")
	if err := Migrate(ctx, dsn, Options{}); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	provider := New()
	if err := provider.Configure(ctx, "", map[string]any{"dsn": dsn}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	const store = "records"
	if err := provider.CreateObjectStore(ctx, store, gestalt.ObjectStoreOptions{}); err != nil {
		t.Fatalf("CreateObjectStore: %v", err)
	}
	for _, id := range []string{"a", "b", "c", "d"} {
		if err := provider.Put(ctx, gestalt.IndexedDBRecordRequest{
			Store:  store,
			Record: gestalt.Record{"id": id, "value": id},
		}); err != nil {
			t.Fatalf("Put(%s): %v", id, err)
		}
	}

	queries := idb.AnyOf("a", "c", "a", "missing").Queries()
	records, err := provider.GetAll(ctx, gestalt.IndexedDBObjectStoreRangeRequest{
		Store:   store,
		Query:   queries[0],
		Queries: queries,
	})
	if err != nil {
		t.Fatalf("GetAll AnyOf: %v", err)
	}
	if len(records) != 2 || records[0]["id"] != "a" || records[1]["id"] != "c" {
		t.Fatalf("GetAll AnyOf = %#v, want rows a and c", records)
	}
}
