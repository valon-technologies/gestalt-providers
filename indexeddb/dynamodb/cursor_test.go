package dynamodb

import (
	"context"
	"testing"

	cursorutil "github.com/valon-technologies/gestalt-providers/indexeddb/internal/cursorutil"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
)

func dynamoTestRecord(id string, fields map[string]any) gestalt.Record {
	record := gestalt.Record{"id": id}
	for key, value := range fields {
		record[key] = value
	}
	return record
}

func TestDynamoCursorAdvanceSkipsRequestedCount(t *testing.T) {
	cursor := &dynamoCursor{
		Snapshot: cursorutil.Snapshot{IndexedDBCursorSnapshot: gestalt.IndexedDBCursorSnapshot{
			Entries: []cursorutil.Entry{
				{PrimaryKey: "a", PrimaryKeyValue: "a", Key: "a", Record: dynamoTestRecord("a", nil)},
				{PrimaryKey: "b", PrimaryKeyValue: "b", Key: "b", Record: dynamoTestRecord("b", nil)},
				{PrimaryKey: "c", PrimaryKeyValue: "c", Key: "c", Record: dynamoTestRecord("c", nil)},
				{PrimaryKey: "d", PrimaryKeyValue: "d", Key: "d", Record: dynamoTestRecord("d", nil)},
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

func TestDynamoCursorKeysOnlyEntryOmitsRecord(t *testing.T) {
	cursor := &dynamoCursor{
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

func TestDynamoCursorIndexRangeUsesIndexKeys(t *testing.T) {
	cursor := &dynamoCursor{
		Snapshot: cursorutil.Snapshot{IndexedDBCursorSnapshot: gestalt.IndexedDBCursorSnapshot{IndexCursor: true}},
		index:    &indexDef{KeyPath: []string{"status"}},
	}
	entries := []cursorutil.Entry{
		{PrimaryKey: "a", PrimaryKeyValue: "a", Key: []any{"active"}, Record: dynamoTestRecord("a", map[string]any{"status": "active"})},
		{PrimaryKey: "b", PrimaryKeyValue: "b", Key: []any{"inactive"}, Record: dynamoTestRecord("b", map[string]any{"status": "inactive"})},
		{PrimaryKey: "c", PrimaryKeyValue: "c", Key: []any{"active"}, Record: dynamoTestRecord("c", map[string]any{"status": "active"})},
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

func TestDynamoCursorReverseContinueToKey(t *testing.T) {
	cursor := &dynamoCursor{
		Snapshot: cursorutil.Snapshot{IndexedDBCursorSnapshot: gestalt.IndexedDBCursorSnapshot{
			Reverse: true,
			Entries: []cursorutil.Entry{
				{PrimaryKey: "d", PrimaryKeyValue: "d", Key: "d", Record: dynamoTestRecord("d", nil)},
				{PrimaryKey: "c", PrimaryKeyValue: "c", Key: "c", Record: dynamoTestRecord("c", nil)},
				{PrimaryKey: "b", PrimaryKeyValue: "b", Key: "b", Record: dynamoTestRecord("b", nil)},
				{PrimaryKey: "a", PrimaryKeyValue: "a", Key: "a", Record: dynamoTestRecord("a", nil)},
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

func TestMergeDynamoCursorSeekRangeDoesNotMoveBehindCurrentKey(t *testing.T) {
	rangeAfterCurrent := mergeDynamoCursorSeekRange(nil, "b", "a")
	if rangeAfterCurrent.Lower != "b" || !rangeAfterCurrent.LowerOpen {
		t.Fatalf("range after current = %#v, want lower b open", rangeAfterCurrent)
	}

	rangeAtSeek := mergeDynamoCursorSeekRange(nil, "b", "c")
	if rangeAtSeek.Lower != "c" || rangeAtSeek.LowerOpen {
		t.Fatalf("range at seek = %#v, want lower c closed", rangeAtSeek)
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
		Snapshot: cursorutil.Snapshot{IndexedDBCursorSnapshot: gestalt.IndexedDBCursorSnapshot{IndexCursor: true}},
		index:    &indexDef{KeyPath: []string{"status", "rank"}},
	}
	entries := []cursorutil.Entry{
		{PrimaryKey: "a", PrimaryKeyValue: "a", Key: []any{"active", int64(1)}, Record: dynamoTestRecord("a", map[string]any{"status": "active", "rank": int64(1)})},
		{PrimaryKey: "b", PrimaryKeyValue: "b", Key: []any{"active", int64(2)}, Record: dynamoTestRecord("b", map[string]any{"status": "active", "rank": int64(2)})},
		{PrimaryKey: "c", PrimaryKeyValue: "c", Key: []any{"inactive", int64(1)}, Record: dynamoTestRecord("c", map[string]any{"status": "inactive", "rank": int64(1)})},
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

func TestDynamoPrepareUpdatedRecordAllowsClearingIndexedField(t *testing.T) {
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
