package dynamodb

import (
	"testing"

	cursorutil "github.com/valon-technologies/gestalt-providers/indexeddb/internal/cursorutil"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
)

func dynamoTestRecord(t *testing.T, id string, fields map[string]any) *proto.Record {
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

func dynamoMustTypedValue(t *testing.T, value any) *proto.TypedValue {
	t.Helper()
	pb, err := gestalt.TypedValueFromAny(value)
	if err != nil {
		t.Fatalf("TypedValueFromAny(%#v): %v", value, err)
	}
	return pb
}

func TestDynamoCursorAdvanceSkipsRequestedCount(t *testing.T) {
	cursor := &dynamoCursor{
		Snapshot: cursorutil.Snapshot{
			Entries: []cursorutil.Entry{
				{PrimaryKey: "a", PrimaryKeyValue: "a", Key: "a", Record: dynamoTestRecord(t, "a", nil)},
				{PrimaryKey: "b", PrimaryKeyValue: "b", Key: "b", Record: dynamoTestRecord(t, "b", nil)},
				{PrimaryKey: "c", PrimaryKeyValue: "c", Key: "c", Record: dynamoTestRecord(t, "c", nil)},
				{PrimaryKey: "d", PrimaryKeyValue: "d", Key: "d", Record: dynamoTestRecord(t, "d", nil)},
			},
			Pos: -1,
		},
	}

	entry, ok, err := cursor.Advance(2)
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

func TestDynamoCursorKeysOnlyEntryOmitsRecord(t *testing.T) {
	cursor := &dynamoCursor{
		Snapshot: cursorutil.Snapshot{
			KeysOnly: true,
			Entries: []cursorutil.Entry{
				{
					PrimaryKey:      "a",
					PrimaryKeyValue: "a",
					Key:             "a",
					Record:          dynamoTestRecord(t, "a", map[string]any{"status": "active"}),
				},
			},
			Pos: 0,
		},
	}

	entry, err := cursor.CurrentEntry()
	if err != nil {
		t.Fatalf("currentEntry: %v", err)
	}
	if entry.GetRecord() != nil {
		t.Fatalf("keys-only cursor returned record: %+v", entry.GetRecord())
	}
}

func TestDynamoCursorIndexRangeUsesIndexKeys(t *testing.T) {
	cursor := &dynamoCursor{
		Snapshot: cursorutil.Snapshot{IndexCursor: true},
		index:    &indexDef{KeyPath: []string{"status"}},
	}
	entries := []cursorutil.Entry{
		{PrimaryKey: "a", PrimaryKeyValue: "a", Key: []any{"active"}, Record: dynamoTestRecord(t, "a", map[string]any{"status": "active"})},
		{PrimaryKey: "b", PrimaryKeyValue: "b", Key: []any{"inactive"}, Record: dynamoTestRecord(t, "b", map[string]any{"status": "inactive"})},
		{PrimaryKey: "c", PrimaryKeyValue: "c", Key: []any{"active"}, Record: dynamoTestRecord(t, "c", map[string]any{"status": "active"})},
	}

	filtered, err := cursor.ApplyRange(entries, &proto.KeyRange{
		Lower: dynamoMustTypedValue(t, "active"),
		Upper: dynamoMustTypedValue(t, "active"),
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

func TestDynamoCursorReverseContinueToKey(t *testing.T) {
	cursor := &dynamoCursor{
		Snapshot: cursorutil.Snapshot{
			Reverse: true,
			Entries: []cursorutil.Entry{
				{PrimaryKey: "d", PrimaryKeyValue: "d", Key: "d", Record: dynamoTestRecord(t, "d", nil)},
				{PrimaryKey: "c", PrimaryKeyValue: "c", Key: "c", Record: dynamoTestRecord(t, "c", nil)},
				{PrimaryKey: "b", PrimaryKeyValue: "b", Key: "b", Record: dynamoTestRecord(t, "b", nil)},
				{PrimaryKey: "a", PrimaryKeyValue: "a", Key: "a", Record: dynamoTestRecord(t, "a", nil)},
			},
			Pos: -1,
		},
	}

	entry, ok, err := cursor.ContinueToKey("c")
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

func TestBuildIndexConditionWithoutValuesScansWholeIndexPartition(t *testing.T) {
	cond, expr := buildIndexCondition("items", "by_status", nil)
	if cond != "PK = :pk" {
		t.Fatalf("condition = %q, want %q", cond, "PK = :pk")
	}
	if _, ok := expr[":pk"]; !ok {
		t.Fatal("missing :pk expression value")
	}
	if _, ok := expr[":skp"]; ok {
		t.Fatal("unexpected :skp expression value for empty index query")
	}
}

func TestDynamoCursorCompoundIndexRangeUsesDecodedArrayKey(t *testing.T) {
	cursor := &dynamoCursor{
		Snapshot: cursorutil.Snapshot{IndexCursor: true},
		index:    &indexDef{KeyPath: []string{"status", "rank"}},
	}
	entries := []cursorutil.Entry{
		{PrimaryKey: "a", PrimaryKeyValue: "a", Key: []any{"active", int64(1)}, Record: dynamoTestRecord(t, "a", map[string]any{"status": "active", "rank": int64(1)})},
		{PrimaryKey: "b", PrimaryKeyValue: "b", Key: []any{"active", int64(2)}, Record: dynamoTestRecord(t, "b", map[string]any{"status": "active", "rank": int64(2)})},
		{PrimaryKey: "c", PrimaryKeyValue: "c", Key: []any{"inactive", int64(1)}, Record: dynamoTestRecord(t, "c", map[string]any{"status": "inactive", "rank": int64(1)})},
	}

	filtered, err := cursor.ApplyRange(entries, &proto.KeyRange{
		Lower: dynamoMustTypedValue(t, []any{"active", int64(2)}),
		Upper: dynamoMustTypedValue(t, []any{"active", int64(2)}),
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

func TestDynamoPrepareUpdatedRecordAllowsClearingIndexedField(t *testing.T) {
	updateRecord, err := gestalt.RecordToProto(map[string]any{
		"name": "Alice Missing Status",
	})
	if err != nil {
		t.Fatalf("RecordToProto: %v", err)
	}

	record, err := cursorutil.CloneRecordWithField(updateRecord, "id", "a")
	if err != nil {
		t.Fatalf("CloneRecordWithField: %v", err)
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
