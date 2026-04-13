package mongodb

import (
	"testing"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
)

func mongoTestRecord(t *testing.T, id string, fields map[string]any) *proto.Record {
	t.Helper()
	record := map[string]any{"id": id}
	for key, value := range fields {
		record[key] = value
	}
	pb, err := gestalt.RecordToProto(record)
	if err != nil {
		t.Fatalf("RecordToProto: %v", err)
	}
	return pb
}

func mongoMustTypedValue(t *testing.T, value any) *proto.TypedValue {
	t.Helper()
	pb, err := gestalt.TypedValueFromAny(value)
	if err != nil {
		t.Fatalf("TypedValueFromAny(%#v): %v", value, err)
	}
	return pb
}

func TestMongoCursorAdvanceSkipsRequestedCount(t *testing.T) {
	cursor := &mongoCursor{
		entries: []mongoCursorEntry{
			{primaryKey: "a", key: "a", record: mongoTestRecord(t, "a", nil)},
			{primaryKey: "b", key: "b", record: mongoTestRecord(t, "b", nil)},
			{primaryKey: "c", key: "c", record: mongoTestRecord(t, "c", nil)},
			{primaryKey: "d", key: "d", record: mongoTestRecord(t, "d", nil)},
		},
		pos: -1,
	}

	entry, ok, err := cursor.advance(2)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if !ok {
		t.Fatal("advance returned exhausted cursor")
	}
	if got := entry.GetPrimaryKey(); got != "c" {
		t.Fatalf("Advance(2) primary key = %q, want %q", got, "c")
	}
}

func TestMongoCursorKeysOnlyEntryOmitsRecord(t *testing.T) {
	cursor := &mongoCursor{
		keysOnly: true,
		entries: []mongoCursorEntry{
			{
				primaryKey: "a",
				key:        "a",
				record:     mongoTestRecord(t, "a", map[string]any{"status": "active"}),
			},
		},
		pos: 0,
	}

	entry, err := cursor.currentEntry()
	if err != nil {
		t.Fatalf("currentEntry: %v", err)
	}
	if entry.GetRecord() != nil {
		t.Fatalf("keys-only cursor returned record: %+v", entry.GetRecord())
	}
}

func TestMongoCursorIndexRangeUsesIndexKeys(t *testing.T) {
	cursor := &mongoCursor{
		indexCursor: true,
		index:       &indexMeta{keyPath: []string{"status"}},
	}
	entries := []mongoCursorEntry{
		{primaryKey: "a", key: []any{"active"}, record: mongoTestRecord(t, "a", map[string]any{"status": "active"})},
		{primaryKey: "b", key: []any{"inactive"}, record: mongoTestRecord(t, "b", map[string]any{"status": "inactive"})},
		{primaryKey: "c", key: []any{"active"}, record: mongoTestRecord(t, "c", map[string]any{"status": "active"})},
	}

	filtered, err := cursor.applyRange(entries, &proto.KeyRange{
		Lower: mongoMustTypedValue(t, "active"),
		Upper: mongoMustTypedValue(t, "active"),
	})
	if err != nil {
		t.Fatalf("applyRange: %v", err)
	}
	if len(filtered) != 2 {
		t.Fatalf("filtered count = %d, want 2", len(filtered))
	}
	for _, entry := range filtered {
		key, ok := entry.key.([]any)
		if !ok || len(key) != 1 || key[0] != "active" {
			t.Fatalf("entry key = %#v, want []any{\"active\"}", entry.key)
		}
	}
}

func TestMongoCursorReverseContinueToKey(t *testing.T) {
	cursor := &mongoCursor{
		reverse: true,
		entries: []mongoCursorEntry{
			{primaryKey: "d", key: "d", record: mongoTestRecord(t, "d", nil)},
			{primaryKey: "c", key: "c", record: mongoTestRecord(t, "c", nil)},
			{primaryKey: "b", key: "b", record: mongoTestRecord(t, "b", nil)},
			{primaryKey: "a", key: "a", record: mongoTestRecord(t, "a", nil)},
		},
		pos: -1,
	}

	entry, ok, err := cursor.continueToKey("c")
	if err != nil {
		t.Fatalf("continueToKey: %v", err)
	}
	if !ok {
		t.Fatal("continueToKey returned exhausted cursor")
	}
	if got := entry.GetPrimaryKey(); got != "c" {
		t.Fatalf("ContinueToKey(\"c\") primary key = %q, want %q", got, "c")
	}
}

func TestMongoCursorCompoundIndexRangeUsesDecodedArrayKey(t *testing.T) {
	cursor := &mongoCursor{
		indexCursor: true,
		index:       &indexMeta{keyPath: []string{"status", "rank"}},
	}
	entries := []mongoCursorEntry{
		{primaryKey: "a", key: []any{"active", int64(1)}, record: mongoTestRecord(t, "a", map[string]any{"status": "active", "rank": int64(1)})},
		{primaryKey: "b", key: []any{"active", int64(2)}, record: mongoTestRecord(t, "b", map[string]any{"status": "active", "rank": int64(2)})},
		{primaryKey: "c", key: []any{"inactive", int64(1)}, record: mongoTestRecord(t, "c", map[string]any{"status": "inactive", "rank": int64(1)})},
	}

	filtered, err := cursor.applyRange(entries, &proto.KeyRange{
		Lower: mongoMustTypedValue(t, []any{"active", int64(2)}),
		Upper: mongoMustTypedValue(t, []any{"active", int64(2)}),
	})
	if err != nil {
		t.Fatalf("applyRange: %v", err)
	}
	if len(filtered) != 1 {
		t.Fatalf("filtered count = %d, want 1", len(filtered))
	}
	if got := filtered[0].primaryKey; got != "b" {
		t.Fatalf("filtered primary key = %q, want %q", got, "b")
	}
}

func TestMongoPrepareUpdatedRecordAllowsClearingIndexedField(t *testing.T) {
	cursor := &mongoCursor{
		indexCursor: true,
		index:       &indexMeta{keyPath: []string{"status"}},
	}

	updateRecord, err := gestalt.RecordToProto(map[string]any{
		"name": "Alice Missing Status",
	})
	if err != nil {
		t.Fatalf("RecordToProto: %v", err)
	}

	record, err := cursor.prepareUpdatedRecord(updateRecord, "a")
	if err != nil {
		t.Fatalf("prepareUpdatedRecord: %v", err)
	}

	decoded, err := gestalt.RecordFromProto(record)
	if err != nil {
		t.Fatalf("RecordFromProto: %v", err)
	}
	if got := decoded["id"]; got != "a" {
		t.Fatalf("decoded id = %#v, want %q", got, "a")
	}
	if got, ok := decoded["status"]; ok && got != nil {
		t.Fatalf("decoded status = %#v, want nil", got)
	}
}
