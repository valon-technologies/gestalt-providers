package mongodb

import (
	"context"
	"testing"
	"time"

	cursorutil "github.com/valon-technologies/gestalt-providers/indexeddb/internal/cursorutil"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func mongoTestRecord(id any, fields map[string]any) gestalt.Record {
	record := gestalt.Record{"id": id}
	for key, value := range fields {
		record[key] = value
	}
	return record
}

func TestMongoCursorAdvanceSkipsRequestedCount(t *testing.T) {
	cursor := &mongoCursor{
		Snapshot: cursorutil.Snapshot{IndexedDBCursorSnapshot: gestalt.IndexedDBCursorSnapshot{
			Entries: []cursorutil.Entry{
				{PrimaryKey: "a", PrimaryKeyValue: "a", Key: "a", Record: mongoTestRecord("a", nil)},
				{PrimaryKey: "b", PrimaryKeyValue: "b", Key: "b", Record: mongoTestRecord("b", nil)},
				{PrimaryKey: "c", PrimaryKeyValue: "c", Key: "c", Record: mongoTestRecord("c", nil)},
				{PrimaryKey: "d", PrimaryKeyValue: "d", Key: "d", Record: mongoTestRecord("d", nil)},
			},
			Pos: 0,
		}},
	}

	entry, err := cursor.Advance(context.Background(), 2)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if entry == nil {
		t.Fatal("advance returned exhausted cursor")
	}
	if got := entry.PrimaryKey; got != "c" {
		t.Fatalf("Advance(2) primary key = %q, want %q", got, "c")
	}
}

func TestMongoCursorKeysOnlyEntryOmitsRecord(t *testing.T) {
	cursor := &mongoCursor{
		Snapshot: cursorutil.Snapshot{IndexedDBCursorSnapshot: gestalt.IndexedDBCursorSnapshot{
			KeysOnly: true,
			Entries: []cursorutil.Entry{
				{
					PrimaryKey:      "a",
					PrimaryKeyValue: "a",
					Key:             "a",
				},
			},
			Pos: 0,
		}},
	}

	entry, err := cursor.CurrentEntry()
	if err != nil {
		t.Fatalf("currentEntry: %v", err)
	}
	if entry.Record != nil {
		t.Fatalf("keys-only cursor returned record: %+v", entry.Record)
	}
}

func TestMongoEntryFromRecordPreservesNativeObjectStorePrimaryKey(t *testing.T) {
	record := gestalt.Record{
		"id":   int64(10),
		"name": "ten",
	}

	cursor := &mongoCursor{}
	entry, err := cursor.entryFromRecord(record)
	if err != nil {
		t.Fatalf("entryFromRecord: %v", err)
	}
	if got := entry.PrimaryKey; got != "10" {
		t.Fatalf("primaryKey = %q, want %q", got, "10")
	}
	if got, ok := entry.PrimaryKeyValue.(int64); !ok || got != 10 {
		t.Fatalf("primaryKeyValue = %#v, want int64(10)", entry.PrimaryKeyValue)
	}
	if got, ok := entry.Key.(int64); !ok || got != 10 {
		t.Fatalf("key = %#v, want int64(10)", entry.Key)
	}
}

func TestMongoCursorIndexRangeUsesIndexKeys(t *testing.T) {
	cursor := &mongoCursor{
		Snapshot: cursorutil.Snapshot{IndexedDBCursorSnapshot: gestalt.IndexedDBCursorSnapshot{IndexCursor: true}},
		index:    &indexMeta{keyPath: []string{"status"}},
	}
	entries := []cursorutil.Entry{
		{PrimaryKey: "a", PrimaryKeyValue: "a", Key: []any{"active"}, Record: mongoTestRecord("a", map[string]any{"status": "active"})},
		{PrimaryKey: "b", PrimaryKeyValue: "b", Key: []any{"inactive"}, Record: mongoTestRecord("b", map[string]any{"status": "inactive"})},
		{PrimaryKey: "c", PrimaryKeyValue: "c", Key: []any{"active"}, Record: mongoTestRecord("c", map[string]any{"status": "active"})},
	}

	filtered, err := cursor.ApplyRange(entries, &gestalt.KeyRange{
		Lower: "active",
		Upper: "active",
	})
	if err != nil {
		t.Fatalf("applyRange: %v", err)
	}
	if len(filtered) != 2 {
		t.Fatalf("filtered count = %d, want 2", len(filtered))
	}
	for _, entry := range filtered {
		key, ok := entry.Key.([]any)
		if !ok || len(key) != 1 || key[0] != "active" {
			t.Fatalf("entry key = %#v, want []any{\"active\"}", entry.Key)
		}
	}
}

func TestMongoCursorReverseContinueToKey(t *testing.T) {
	cursor := &mongoCursor{
		Snapshot: cursorutil.Snapshot{IndexedDBCursorSnapshot: gestalt.IndexedDBCursorSnapshot{
			Reverse: true,
			Entries: []cursorutil.Entry{
				{PrimaryKey: "d", PrimaryKeyValue: "d", Key: "d", Record: mongoTestRecord("d", nil)},
				{PrimaryKey: "c", PrimaryKeyValue: "c", Key: "c", Record: mongoTestRecord("c", nil)},
				{PrimaryKey: "b", PrimaryKeyValue: "b", Key: "b", Record: mongoTestRecord("b", nil)},
				{PrimaryKey: "a", PrimaryKeyValue: "a", Key: "a", Record: mongoTestRecord("a", nil)},
			},
			Pos: -1,
		}},
	}

	entry, err := cursor.ContinueToKey(context.Background(), "c")
	if err != nil {
		t.Fatalf("continueToKey: %v", err)
	}
	if entry == nil {
		t.Fatal("continueToKey returned exhausted cursor")
	}
	if got := entry.PrimaryKey; got != "c" {
		t.Fatalf("ContinueToKey(\"c\") primary key = %q, want %q", got, "c")
	}
}

func TestMongoCursorObjectStoreRangeUsesNativePrimaryKeys(t *testing.T) {
	cursor := &mongoCursor{}
	entries := []cursorutil.Entry{
		{PrimaryKey: "1", PrimaryKeyValue: int64(1), Key: int64(1), Record: mongoTestRecord("1", nil)},
		{PrimaryKey: "2", PrimaryKeyValue: int64(2), Key: int64(2), Record: mongoTestRecord("2", nil)},
		{PrimaryKey: "10", PrimaryKeyValue: int64(10), Key: int64(10), Record: mongoTestRecord("10", nil)},
	}

	filtered, err := cursor.ApplyRange(entries, &gestalt.KeyRange{
		Lower: int64(2),
		Upper: int64(10),
	})
	if err != nil {
		t.Fatalf("applyRange: %v", err)
	}
	if len(filtered) != 2 {
		t.Fatalf("filtered count = %d, want 2", len(filtered))
	}
	if got := filtered[0].PrimaryKey; got != "2" {
		t.Fatalf("first filtered primary key = %q, want %q", got, "2")
	}
	if got := filtered[1].PrimaryKey; got != "10" {
		t.Fatalf("second filtered primary key = %q, want %q", got, "10")
	}
}

func TestMongoCursorCompoundIndexRangeUsesDecodedArrayKey(t *testing.T) {
	cursor := &mongoCursor{
		Snapshot: cursorutil.Snapshot{IndexedDBCursorSnapshot: gestalt.IndexedDBCursorSnapshot{IndexCursor: true}},
		index:    &indexMeta{keyPath: []string{"status", "rank"}},
	}
	entries := []cursorutil.Entry{
		{PrimaryKey: "a", PrimaryKeyValue: "a", Key: []any{"active", int64(1)}, Record: mongoTestRecord("a", map[string]any{"status": "active", "rank": int64(1)})},
		{PrimaryKey: "b", PrimaryKeyValue: "b", Key: []any{"active", int64(2)}, Record: mongoTestRecord("b", map[string]any{"status": "active", "rank": int64(2)})},
		{PrimaryKey: "c", PrimaryKeyValue: "c", Key: []any{"inactive", int64(1)}, Record: mongoTestRecord("c", map[string]any{"status": "inactive", "rank": int64(1)})},
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

func TestMongoPrepareUpdatedRecordAllowsClearingIndexedField(t *testing.T) {
	record, err := cursorutil.CloneRecordWithField(gestalt.Record{
		"name": "Alice Missing Status",
	}, "id", "a")
	if err != nil {
		t.Fatalf("CloneRecordWithField: %v", err)
	}

	if got := record["id"]; got != "a" {
		t.Fatalf("decoded id = %#v, want %q", got, "a")
	}
	if got, ok := record["status"]; ok && got != nil {
		t.Fatalf("decoded status = %#v, want nil", got)
	}
}

func TestMongoPrepareUpdatedRecordPreservesNativePrimaryKeyType(t *testing.T) {
	want := time.Unix(1700000000, 0).UTC()
	record, err := cursorutil.CloneRecordWithField(gestalt.Record{
		"name": "updated",
	}, "id", want)
	if err != nil {
		t.Fatalf("CloneRecordWithField: %v", err)
	}

	got, ok := record["id"].(time.Time)
	if !ok {
		t.Fatalf("decoded id type = %T, want time.Time", record["id"])
	}
	if !got.Equal(want) {
		t.Fatalf("decoded id = %s, want %s", got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}
}

func TestMongoCursorProjectionForKeyOnlyObjectStore(t *testing.T) {
	projection := mongoCursorProjection(&mongoCursor{
		Snapshot: cursorutil.Snapshot{IndexedDBCursorSnapshot: gestalt.IndexedDBCursorSnapshot{KeysOnly: true}},
	})
	if len(projection) != 1 {
		t.Fatalf("projection = %#v, want only _id", projection)
	}
	if got, ok := projection["_id"]; !ok || got != 1 {
		t.Fatalf("projection[_id] = %#v, want 1", got)
	}
}

func TestMongoCursorProjectionForKeyOnlyIndexCursor(t *testing.T) {
	projection := mongoCursorProjection(&mongoCursor{
		Snapshot: cursorutil.Snapshot{IndexedDBCursorSnapshot: gestalt.IndexedDBCursorSnapshot{
			KeysOnly:    true,
			IndexCursor: true,
		}},
		index: &indexMeta{keyPath: []string{"status", "rank"}},
	})
	if got, ok := projection["_id"]; !ok || got != 1 {
		t.Fatalf("projection[_id] = %#v, want 1", got)
	}
	if got, ok := projection["status"]; !ok || got != 1 {
		t.Fatalf("projection[status] = %#v, want 1", got)
	}
	if got, ok := projection["rank"]; !ok || got != 1 {
		t.Fatalf("projection[rank] = %#v, want 1", got)
	}
}

func TestMongoCursorDocToRecordPreservesNativeIDType(t *testing.T) {
	record, err := docToRecord(bson.M{
		"_id":  int64(10),
		"name": "ten",
	})
	if err != nil {
		t.Fatalf("docToRecord: %v", err)
	}

	if got, ok := record["id"].(int64); !ok || got != 10 {
		t.Fatalf("decoded id = %#v, want int64(10)", record["id"])
	}
}
