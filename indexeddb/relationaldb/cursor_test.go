package relationaldb

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	cursorutil "github.com/valon-technologies/gestalt-providers/indexeddb/internal/cursorutil"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

type cursorHarness struct {
	store *Store
}

func newCursorHarness(t *testing.T) *cursorHarness {
	t.Helper()
	return &cursorHarness{store: testStore(t)}
}

func cursorItemsSchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Indexes: []gestalt.IndexSchema{
			{Name: "by_status", KeyPath: []string{"status"}},
			{Name: "by_email", KeyPath: []string{"email"}, Unique: true},
		},
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeString, PrimaryKey: true, NotNull: true},
			{Name: "name", Type: gestalt.TypeString},
			{Name: "status", Type: gestalt.TypeString},
			{Name: "email", Type: gestalt.TypeString},
		},
	}
}

func makeCursorItem(id, name, status, email string) gestalt.Record {
	return gestalt.Record{
		"id":     id,
		"name":   name,
		"status": status,
		"email":  email,
	}
}

func cursorNumberSchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeInt, PrimaryKey: true, NotNull: true},
			{Name: "name", Type: gestalt.TypeString},
		},
	}
}

func makeCursorNumberItem(id int64, name string) gestalt.Record {
	return gestalt.Record{"id": id, "name": name}
}

func cursorBytesSchema() gestalt.ObjectStoreSchema {
	return gestalt.ObjectStoreSchema{
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: gestalt.TypeBytes, PrimaryKey: true, NotNull: true},
			{Name: "name", Type: gestalt.TypeString},
		},
	}
}

func makeCursorBytesItem(id []byte, name string) gestalt.Record {
	return gestalt.Record{"id": id, "name": name}
}

func seedCursorNumbers(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()

	if err := store.CreateObjectStore(ctx, "numbers", cursorNumberSchema()); err != nil {
		t.Fatalf("CreateObjectStore(numbers): %v", err)
	}
	for _, record := range []gestalt.Record{
		makeCursorNumberItem(2, "two"),
		makeCursorNumberItem(10, "ten"),
		makeCursorNumberItem(1, "one"),
	} {
		if err := store.Add(ctx, gestalt.IndexedDBRecordRequest{Store: "numbers", Record: record}); err != nil {
			t.Fatalf("Add(numbers): %v", err)
		}
	}
}

func seedCursorBytes(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()

	if err := store.CreateObjectStore(ctx, "blobs", cursorBytesSchema()); err != nil {
		t.Fatalf("CreateObjectStore(blobs): %v", err)
	}
	if err := store.Add(ctx, gestalt.IndexedDBRecordRequest{
		Store:  "blobs",
		Record: makeCursorBytesItem([]byte("blob-1"), "Blob One"),
	}); err != nil {
		t.Fatalf("Add(blobs): %v", err)
	}
}

func seedCursorItems(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()

	if err := store.CreateObjectStore(ctx, "items", cursorItemsSchema()); err != nil {
		t.Fatalf("CreateObjectStore: %v", err)
	}
	for _, record := range []gestalt.Record{
		makeCursorItem("a", "Alice", "active", "alice@test.com"),
		makeCursorItem("b", "Bob", "active", "bob@test.com"),
		makeCursorItem("c", "Carol", "inactive", "carol@test.com"),
		makeCursorItem("d", "Dave", "active", "dave@test.com"),
	} {
		if err := store.Add(ctx, gestalt.IndexedDBRecordRequest{Store: "items", Record: record}); err != nil {
			t.Fatalf("Add(%q): %v", record["id"], err)
		}
	}
}

func openTestCursor(t *testing.T, store *Store, req gestalt.IndexedDBOpenCursorRequest) gestalt.IndexedDBCursor {
	t.Helper()
	cursor, err := store.OpenCursor(context.Background(), req)
	if err != nil {
		t.Fatalf("OpenCursor: %v", err)
	}
	t.Cleanup(func() { _ = cursor.Close() })
	return cursor
}

func collectCursor(t *testing.T, cursor gestalt.IndexedDBCursor) []*gestalt.IndexedDBCursorEntry {
	t.Helper()
	var entries []*gestalt.IndexedDBCursorEntry
	for {
		entry, err := cursor.Next(context.Background())
		if err != nil {
			t.Fatalf("cursor.Next: %v", err)
		}
		if entry == nil {
			return entries
		}
		entries = append(entries, entry)
	}
}

func TestOpenCursorAdvanceSkipsRequestedCount(t *testing.T) {
	h := newCursorHarness(t)
	seedCursorItems(t, h.store)

	cursor := openTestCursor(t, h.store, gestalt.IndexedDBOpenCursorRequest{
		Store:     "items",
		Direction: gestalt.CursorNext,
	})

	entry, err := cursor.Advance(context.Background(), 2)
	if err != nil {
		t.Fatalf("Advance: %v", err)
	}
	if got := entry.PrimaryKey; got != "b" {
		t.Fatalf("Advance(2) primary key = %q, want %q", got, "b")
	}
	next, err := cursor.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if got := next.PrimaryKey; got != "c" {
		t.Fatalf("Next after Advance(2) primary key = %q, want %q", got, "c")
	}
}

func TestOpenCursorKeysOnlyOmitsRecord(t *testing.T) {
	h := newCursorHarness(t)
	seedCursorItems(t, h.store)

	cursor := openTestCursor(t, h.store, gestalt.IndexedDBOpenCursorRequest{
		Store:     "items",
		Direction: gestalt.CursorNext,
		KeysOnly:  true,
	})

	entry, err := cursor.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if got := entry.PrimaryKey; got != "a" {
		t.Fatalf("first primary key = %q, want %q", got, "a")
	}
	if entry.Record != nil {
		t.Fatalf("keys-only cursor returned record: %+v", entry.Record)
	}
}

func TestOpenCursorOrdersStringPrimaryKeysByNativeValue(t *testing.T) {
	h := newCursorHarness(t)
	ctx := context.Background()
	if err := h.store.CreateObjectStore(ctx, "strings", cursorItemsSchema()); err != nil {
		t.Fatalf("CreateObjectStore(strings): %v", err)
	}
	for _, record := range []gestalt.Record{
		makeCursorItem("b", "Bob", "active", "bob@test.com"),
		makeCursorItem("aa", "Alice", "active", "alice@test.com"),
		makeCursorItem("c", "Carol", "active", "carol@test.com"),
	} {
		if err := h.store.Add(ctx, gestalt.IndexedDBRecordRequest{Store: "strings", Record: record}); err != nil {
			t.Fatalf("Add(strings): %v", err)
		}
	}

	cursor := openTestCursor(t, h.store, gestalt.IndexedDBOpenCursorRequest{
		Store:     "strings",
		Direction: gestalt.CursorNext,
	})
	entries := collectCursor(t, cursor)
	got := []string{entries[0].PrimaryKey, entries[1].PrimaryKey, entries[2].PrimaryKey}
	if want := []string{"aa", "b", "c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("string cursor primary keys = %#v, want %#v", got, want)
	}
}

func TestOpenCursorStreamsPastRelationalPageSize(t *testing.T) {
	h := newCursorHarness(t)
	ctx := context.Background()
	if err := h.store.CreateObjectStore(ctx, "many", cursorItemsSchema()); err != nil {
		t.Fatalf("CreateObjectStore(many): %v", err)
	}
	count := relationalCursorPageSize + 5
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("%03d", i)
		record := makeCursorItem(id, "User "+id, "active", id+"@test.com")
		if err := h.store.Add(ctx, gestalt.IndexedDBRecordRequest{Store: "many", Record: record}); err != nil {
			t.Fatalf("Add(many, %q): %v", id, err)
		}
	}

	cursor := openTestCursor(t, h.store, gestalt.IndexedDBOpenCursorRequest{
		Store:     "many",
		Direction: gestalt.CursorNext,
	})
	entries := collectCursor(t, cursor)
	if len(entries) != count {
		t.Fatalf("cursor count = %d, want %d", len(entries), count)
	}
	if got, want := entries[len(entries)-1].PrimaryKey, fmt.Sprintf("%03d", count-1); got != want {
		t.Fatalf("last primary key = %q, want %q", got, want)
	}
}

func TestOpenCursorIndexRangeUsesIndexKeys(t *testing.T) {
	h := newCursorHarness(t)
	seedCursorItems(t, h.store)

	cursor := openTestCursor(t, h.store, gestalt.IndexedDBOpenCursorRequest{
		Store:     "items",
		Index:     "by_status",
		Direction: gestalt.CursorNext,
		Range:     &gestalt.KeyRange{Lower: "active", Upper: "active"},
	})

	entries := collectCursor(t, cursor)
	if len(entries) != 3 {
		t.Fatalf("active cursor count = %d, want 3", len(entries))
	}
	for _, entry := range entries {
		if key := normalizeDocumentBound(entry.Key); len(key) != 1 || key[0] != "active" {
			t.Fatalf("index key = %#v, want [\"active\"]", entry.Key)
		}
	}
}

func TestOpenCursorOrdersNumericPrimaryKeysByNativeType(t *testing.T) {
	h := newCursorHarness(t)
	seedCursorNumbers(t, h.store)

	cursor := openTestCursor(t, h.store, gestalt.IndexedDBOpenCursorRequest{
		Store:     "numbers",
		Direction: gestalt.CursorNext,
	})
	entries := collectCursor(t, cursor)
	gotKeys := []any{entries[0].Key, entries[1].Key, entries[2].Key}
	gotPrimaryKeys := []string{entries[0].PrimaryKey, entries[1].PrimaryKey, entries[2].PrimaryKey}
	if want := []any{int64(1), int64(2), int64(10)}; cursorutil.CompareValues(gotKeys[0], want[0]) != 0 || cursorutil.CompareValues(gotKeys[1], want[1]) != 0 || cursorutil.CompareValues(gotKeys[2], want[2]) != 0 {
		t.Fatalf("numeric cursor keys = %#v, want %#v", gotKeys, want)
	}
	if want := []string{"1", "2", "10"}; gotPrimaryKeys[0] != want[0] || gotPrimaryKeys[1] != want[1] || gotPrimaryKeys[2] != want[2] {
		t.Fatalf("numeric cursor primary keys = %#v, want %#v", gotPrimaryKeys, want)
	}
}

func TestOpenCursorRangeUsesNativeNumericPrimaryKeys(t *testing.T) {
	h := newCursorHarness(t)
	seedCursorNumbers(t, h.store)

	cursor := openTestCursor(t, h.store, gestalt.IndexedDBOpenCursorRequest{
		Store:     "numbers",
		Direction: gestalt.CursorNext,
		Range:     &gestalt.KeyRange{Lower: int64(2), Upper: int64(10)},
	})

	entries := collectCursor(t, cursor)
	got := []string{entries[0].PrimaryKey, entries[1].PrimaryKey}
	if want := []string{"2", "10"}; got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("numeric range cursor primary keys = %#v, want %#v", got, want)
	}
}

func TestOpenCursorDeleteAcknowledgesAndRemovesRecord(t *testing.T) {
	h := newCursorHarness(t)
	seedCursorItems(t, h.store)

	cursor := openTestCursor(t, h.store, gestalt.IndexedDBOpenCursorRequest{
		Store:     "items",
		Direction: gestalt.CursorNext,
	})

	entry, err := cursor.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if entry.PrimaryKey != "a" {
		t.Fatalf("first primary key = %q, want %q", entry.PrimaryKey, "a")
	}
	if err := cursor.Delete(context.Background()); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := h.store.Get(context.Background(), gestalt.IndexedDBObjectStoreRequest{Store: "items", ID: "a"}); err == nil {
		t.Fatal("deleted record still readable")
	}

	next, err := cursor.Next(context.Background())
	if err != nil {
		t.Fatalf("Next after delete: %v", err)
	}
	if got := next.PrimaryKey; got != "b" {
		t.Fatalf("primary key after delete = %q, want %q", got, "b")
	}
}

func TestOpenCursorDeleteUsesNativePrimaryKeyValue(t *testing.T) {
	h := newCursorHarness(t)
	seedCursorBytes(t, h.store)

	cursor := openTestCursor(t, h.store, gestalt.IndexedDBOpenCursorRequest{
		Store:     "blobs",
		Direction: gestalt.CursorNext,
	})
	if _, err := cursor.Next(context.Background()); err != nil {
		t.Fatalf("Next: %v", err)
	}
	if err := cursor.Delete(context.Background()); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	count, err := h.store.Count(context.Background(), gestalt.IndexedDBObjectStoreRangeRequest{Store: "blobs"})
	if err != nil {
		t.Fatalf("Count(blobs): %v", err)
	}
	if count != 0 {
		t.Fatalf("blob count after delete = %d, want 0", count)
	}
}

func TestOpenCursorUpdatePersistsRecordWithCurrentPrimaryKey(t *testing.T) {
	h := newCursorHarness(t)
	seedCursorItems(t, h.store)

	cursor := openTestCursor(t, h.store, gestalt.IndexedDBOpenCursorRequest{
		Store:     "items",
		Direction: gestalt.CursorNext,
	})

	entry, err := cursor.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if entry.PrimaryKey != "a" {
		t.Fatalf("first primary key = %q, want %q", entry.PrimaryKey, "a")
	}

	updated, err := cursor.Update(context.Background(), gestalt.Record{
		"name":   "Updated",
		"status": "inactive",
		"email":  "updated@test.com",
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if got := updated.PrimaryKey; got != "a" {
		t.Fatalf("updated primary key = %q, want %q", got, "a")
	}
	if got := updated.Record["name"]; got != "Updated" {
		t.Fatalf("updated record name = %#v, want %q", got, "Updated")
	}

	stored, err := h.store.Get(context.Background(), gestalt.IndexedDBObjectStoreRequest{Store: "items", ID: "a"})
	if err != nil {
		t.Fatalf("Get(updated): %v", err)
	}
	if got := stored["email"]; got != "updated@test.com" {
		t.Fatalf("stored email = %#v, want %q", got, "updated@test.com")
	}

	next, err := cursor.Next(context.Background())
	if err != nil {
		t.Fatalf("Next after update: %v", err)
	}
	if got := next.PrimaryKey; got != "b" {
		t.Fatalf("next primary key after update = %q, want %q", got, "b")
	}
}

func TestOpenCursorIndexUpdatePreservesSnapshotKeyOrder(t *testing.T) {
	h := newCursorHarness(t)
	seedCursorItems(t, h.store)

	cursor := openTestCursor(t, h.store, gestalt.IndexedDBOpenCursorRequest{
		Store:     "items",
		Index:     "by_status",
		Direction: gestalt.CursorNext,
	})

	first, err := cursor.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if got := first.PrimaryKey; got != "a" {
		t.Fatalf("first primary key = %q, want %q", got, "a")
	}
	if key := normalizeDocumentBound(first.Key); len(key) != 1 || key[0] != "active" {
		t.Fatalf("first key = %#v, want [\"active\"]", first.Key)
	}

	updated, err := cursor.Update(context.Background(), gestalt.Record{
		"name":   "Alice Updated",
		"status": "inactive",
		"email":  "alice-updated@test.com",
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if key := normalizeDocumentBound(updated.Key); len(key) != 1 || key[0] != "active" {
		t.Fatalf("updated key = %#v, want snapshot key [\"active\"]", updated.Key)
	}
	if got := updated.Record["status"]; got != "inactive" {
		t.Fatalf("updated record status = %#v, want %q", got, "inactive")
	}

	second, err := cursor.Next(context.Background())
	if err != nil {
		t.Fatalf("Next after update: %v", err)
	}
	if got := second.PrimaryKey; got != "b" {
		t.Fatalf("second primary key = %q, want %q", got, "b")
	}
	if key := normalizeDocumentBound(second.Key); len(key) != 1 || key[0] != "active" {
		t.Fatalf("second key = %#v, want [\"active\"]", second.Key)
	}
}

func TestOpenCursorIndexUpdateAllowsClearingIndexedField(t *testing.T) {
	h := newCursorHarness(t)
	seedCursorItems(t, h.store)

	cursor := openTestCursor(t, h.store, gestalt.IndexedDBOpenCursorRequest{
		Store:     "items",
		Index:     "by_status",
		Direction: gestalt.CursorNext,
	})

	first, err := cursor.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if got := first.PrimaryKey; got != "a" {
		t.Fatalf("first primary key = %q, want %q", got, "a")
	}

	updated, err := cursor.Update(context.Background(), gestalt.Record{
		"name":  "Alice Missing Status",
		"email": "alice-missing-status@test.com",
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if key := normalizeDocumentBound(updated.Key); len(key) != 1 || key[0] != "active" {
		t.Fatalf("updated key = %#v, want snapshot key [\"active\"]", updated.Key)
	}

	stored, err := h.store.Get(context.Background(), gestalt.IndexedDBObjectStoreRequest{Store: "items", ID: "a"})
	if err != nil {
		t.Fatalf("Get(a): %v", err)
	}
	if got, ok := stored["status"]; ok && got != nil {
		t.Fatalf("stored status = %#v, want nil", got)
	}
	if got := stored["email"]; got != "alice-missing-status@test.com" {
		t.Fatalf("stored email = %#v, want %q", got, "alice-missing-status@test.com")
	}

	active, err := h.store.IndexGetAll(context.Background(), gestalt.IndexedDBIndexQueryRequest{
		Store:  "items",
		Index:  "by_status",
		Values: []any{"active"},
	})
	if err != nil {
		t.Fatalf("IndexGetAll(active): %v", err)
	}
	if got := len(active); got != 2 {
		t.Fatalf("active record count = %d, want 2", got)
	}

	next, err := cursor.Next(context.Background())
	if err != nil {
		t.Fatalf("Next after update: %v", err)
	}
	if got := next.PrimaryKey; got != "b" {
		t.Fatalf("next primary key = %q, want %q", got, "b")
	}
}

func TestRelationalCursorCompoundIndexRangeUsesDecodedArrayKey(t *testing.T) {
	cursor := &relationalCursor{
		LazyCursor: cursorutil.LazyCursor{Snapshot: cursorutil.Snapshot{IndexedDBCursorSnapshot: gestalt.IndexedDBCursorSnapshot{IndexCursor: true}}},
		index:      &gestalt.IndexSchema{KeyPath: []string{"status", "rank"}},
	}

	entries := []cursorutil.Entry{
		{PrimaryKey: "a", PrimaryKeyValue: "a", Key: []any{"active", int64(1)}, Record: makeCursorItem("a", "Alice", "active", "alice@test.com")},
		{PrimaryKey: "b", PrimaryKeyValue: "b", Key: []any{"active", int64(2)}, Record: makeCursorItem("b", "Bob", "active", "bob@test.com")},
		{PrimaryKey: "c", PrimaryKeyValue: "c", Key: []any{"inactive", int64(1)}, Record: makeCursorItem("c", "Carol", "inactive", "carol@test.com")},
	}

	filtered, err := cursor.ApplyRange(entries, &gestalt.KeyRange{
		Lower: []any{"active", int64(2)},
		Upper: []any{"active", int64(2)},
	})
	if err != nil {
		t.Fatalf("applyRange: %v", err)
	}
	if len(filtered) != 1 {
		t.Fatalf("filtered count = %d, want 1", len(filtered))
	}
	if got := filtered[0].PrimaryKey; got != "b" {
		t.Fatalf("filtered primary key = %q, want %q", got, "b")
	}
}

func TestCursorUpdatePreservesTimePrimaryKey(t *testing.T) {
	updateRecord := gestalt.Record{"name": "Updated"}
	record, err := cursorutil.CloneRecordWithField(updateRecord, "id", time.Unix(1700000000, 0).UTC())
	if err != nil {
		t.Fatalf("CloneRecordWithField: %v", err)
	}
	if _, ok := record["id"].(time.Time); !ok {
		t.Fatalf("id type = %T, want time.Time", record["id"])
	}
}
