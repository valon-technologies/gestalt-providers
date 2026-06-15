package contracttest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

const (
	typeString gestalt.ColumnType = gestalt.TypeString
	typeInt    gestalt.ColumnType = gestalt.TypeInt
	typeTime   gestalt.ColumnType = gestalt.TypeTime
	typeBytes  gestalt.ColumnType = gestalt.TypeBytes
)

type Capabilities struct {
	TypedPrimaryKeys     bool
	NestedIndexPaths     bool
	UnreadablePayloadRow bool
}

type Harness interface {
	Name() string
	Capabilities() Capabilities
	NewProvider(t *testing.T) (gestalt.IndexedDBProvider, func())
	InsertUnreadablePayloadRow(t *testing.T, store, id, status string)
}

type session struct {
	harness Harness
	client  indexeddb.Database
	close   func()
}

type cursorEntry struct {
	Key        any
	PrimaryKey string
	Record     gestalt.Record
}

func Run(t *testing.T, harness Harness) {
	t.Helper()

	t.Run("TypedPrimaryKeyFidelity", func(t *testing.T) {
		if !harness.Capabilities().TypedPrimaryKeys {
			t.Skip("backend does not support typed primary keys")
		}
		runTypedPrimaryKeyFidelity(t, harness)
	})

	t.Run("TypedIndexRangeFidelity", func(t *testing.T) {
		runTypedIndexRangeFidelity(t, harness)
	})

	t.Run("CompoundIndexPrefixRange", func(t *testing.T) {
		runCompoundIndexPrefixRange(t, harness)
	})

	t.Run("CompoundIndexIncompletePrefixRangeRejected", func(t *testing.T) {
		runCompoundIndexIncompletePrefixRangeRejected(t, harness)
	})

	t.Run("KeyOnlyCursorSkipsUnreadableValues", func(t *testing.T) {
		if !harness.Capabilities().UnreadablePayloadRow {
			t.Skip("backend cannot inject unreadable payload rows under its native type constraints")
		}
		runKeyOnlyCursorSkipsUnreadableValues(t, harness)
	})

	t.Run("CursorMutationWithTypedKeys", func(t *testing.T) {
		if !harness.Capabilities().TypedPrimaryKeys {
			t.Skip("backend does not support typed primary keys")
		}
		runCursorMutationWithTypedKeys(t, harness)
	})

	t.Run("BulkConsistency", func(t *testing.T) {
		runBulkConsistency(t, harness)
	})

	t.Run("EmptyStoreCursors", func(t *testing.T) {
		runEmptyStoreCursors(t, harness)
	})

	t.Run("ExplicitTransactionSDKContract", func(t *testing.T) {
		runExplicitTransactionSDKContract(t, harness)
	})

	t.Run("TypedDeleteRangeFidelity", func(t *testing.T) {
		if !harness.Capabilities().TypedPrimaryKeys {
			t.Skip("backend does not support typed primary keys")
		}
		runTypedDeleteRangeFidelity(t, harness)
	})

	t.Run("RestartReconfigurePersistsIndexes", func(t *testing.T) {
		runRestartReconfigurePersistsIndexes(t, harness)
	})

	t.Run("MissingIndexFieldExclusion", func(t *testing.T) {
		runMissingIndexFieldExclusion(t, harness)
	})

	t.Run("UniqueIndexConflictOnCursorUpdate", func(t *testing.T) {
		runUniqueIndexConflictOnCursorUpdate(t, harness)
	})

	t.Run("NestedIndexPaths", func(t *testing.T) {
		if !harness.Capabilities().NestedIndexPaths {
			t.Skip("backend does not support nested index paths")
		}
		runNestedIndexPaths(t, harness)
	})
}

func runTypedPrimaryKeyFidelity(t *testing.T, harness Harness) {
	t.Helper()

	sess := newSession(t, harness)
	t.Cleanup(sess.Close)

	timeA := time.Date(2024, time.January, 1, 8, 0, 0, 0, time.UTC)
	timeB := time.Date(2024, time.January, 2, 8, 0, 0, 0, time.UTC)
	timeC := time.Date(2024, time.January, 10, 8, 0, 0, 0, time.UTC)

	cases := []struct {
		name       string
		store      string
		columnType gestalt.ColumnType
		ids        []any
		rangeWant  []any
		lower      any
		upper      any
	}{
		{
			name:       "String",
			store:      "typed_string_ids",
			columnType: typeString,
			ids:        []any{"a", "b", "c"},
			rangeWant:  []any{"b", "c"},
			lower:      "b",
			upper:      "c",
		},
		{
			name:       "Int64",
			store:      "typed_int_ids",
			columnType: typeInt,
			ids: []any{
				int64(9007199254740993),
				int64(9007199254741001),
				int64(9007199254740991),
			},
			rangeWant: []any{
				int64(9007199254740993),
				int64(9007199254741001),
			},
			lower: int64(9007199254740993),
			upper: int64(9007199254741001),
		},
		{
			name:       "Time",
			store:      "typed_time_ids",
			columnType: typeTime,
			ids:        []any{timeB, timeC, timeA},
			rangeWant:  []any{timeB, timeC},
			lower:      timeB,
			upper:      timeC,
		},
		{
			name:       "Bytes",
			store:      "typed_bytes_ids",
			columnType: typeBytes,
			ids:        []any{[]byte{0x02}, []byte{0x0A}, []byte{0x01}},
			rangeWant:  []any{[]byte{0x02}, []byte{0x0A}},
			lower:      []byte{0x02},
			upper:      []byte{0x0A},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mustCreateObjectStore(t, sess.client, tc.store, typedPrimaryKeySchema(tc.columnType))
			for i, id := range tc.ids {
				mustAddRecord(t, sess.client, tc.store, map[string]any{
					"id":   id,
					"name": fmt.Sprintf("%s-%d", tc.name, i),
				})
			}

			records := mustGetAll(t, sess.client, tc.store, nil)
			gotIDs := sortedRecordIDs(t, records)
			wantIDs := sortedValues(append([]any(nil), tc.ids...))
			assertValueSliceEqual(t, gotIDs, wantIDs)

			ranged := mustGetAll(t, sess.client, tc.store, &gestalt.KeyRange{Lower: tc.lower, Upper: tc.upper})
			gotRangeIDs := sortedRecordIDs(t, ranged)
			wantRangeIDs := sortedValues(append([]any(nil), tc.rangeWant...))
			assertValueSliceEqual(t, gotRangeIDs, wantRangeIDs)

			entries := collectCursorEntries(t, sess.client, &cursorRequest{
				Store:     tc.store,
				Direction: gestalt.CursorNext,
			})
			gotCursorIDs := make([]any, 0, len(entries))
			gotCursorKeys := make([]any, 0, len(entries))
			for _, entry := range entries {
				gotCursorKeys = append(gotCursorKeys, cursorScalarKey(t, entry))
				gotCursorIDs = append(gotCursorIDs, entry.Record["id"])
			}
			assertValueSliceEqual(t, gotCursorIDs, wantIDs)
			assertValueSliceEqual(t, gotCursorKeys, wantIDs)
		})
	}
}

func runTypedIndexRangeFidelity(t *testing.T, harness Harness) {
	t.Helper()

	sess := newSession(t, harness)
	t.Cleanup(sess.Close)

	store := "typed_index_range_fidelity"
	mustSeedNumericIndexItems(t, sess.client, store)

	rangeReq := &gestalt.KeyRange{
		Lower: int64(9007199254740993),
		Upper: int64(9007199254741001),
	}

	records := mustIndexGetAllWithRange(t, sess.client, store, "by_rank", rangeReq)
	gotIDs := recordPrimaryKeys(t, records)
	if !stringSlicesEqual(gotIDs, []string{"b", "c"}) {
		t.Fatalf("IndexGetAll by_rank ids = %#v, want %#v", gotIDs, []string{"b", "c"})
	}

	keys := mustIndexGetAllKeysWithRange(t, sess.client, store, "by_rank", rangeReq)
	if !stringSlicesEqual(keys, []string{"b", "c"}) {
		t.Fatalf("IndexGetAllKeys by_rank ids = %#v, want %#v", keys, []string{"b", "c"})
	}

	count := mustIndexCountWithRange(t, sess.client, store, "by_rank", rangeReq)
	if count != 2 {
		t.Fatalf("IndexCount by_rank = %d, want 2", count)
	}

	entries := collectCursorEntries(t, sess.client, &cursorRequest{
		Store:     store,
		Index:     "by_rank",
		Direction: gestalt.CursorNext,
		Range:     rangeReq,
	})
	if got := cursorPrimaryKeys(entries); !stringSlicesEqual(got, []string{"b", "c"}) {
		t.Fatalf("index cursor ids = %#v, want %#v", got, []string{"b", "c"})
	}
	gotKeys := []any{
		cursorScalarKey(t, entries[0]),
		cursorScalarKey(t, entries[1]),
	}
	assertValueSliceEqual(t, gotKeys, []any{int64(9007199254740993), int64(9007199254741001)})
}

func runCompoundIndexPrefixRange(t *testing.T, harness Harness) {
	t.Helper()

	t.Run("ClosedRange", func(t *testing.T) {
		sess := newSession(t, harness)
		t.Cleanup(sess.Close)

		store := "compound_vendor_date_closed"
		mustSeedCompoundVendorDateItems(t, sess.client, store)

		rangeReq := &gestalt.KeyRange{
			Lower: "2026-04-01",
			Upper: "2026-04-30",
		}
		values := []any{"claude_code"}
		want := []string{"claude-apr-09", "claude-apr-30"}

		assertCompoundVendorDateIndexQuery(t, sess.client, store, "by_vendor_date", rangeReq, values, want, [][]any{
			{"claude_code", "2026-04-09"},
			{"claude_code", "2026-04-30"},
		})
	})

	t.Run("LowerOnly", func(t *testing.T) {
		sess := newSession(t, harness)
		t.Cleanup(sess.Close)

		store := "compound_vendor_date_lower"
		mustSeedCompoundVendorDateItems(t, sess.client, store)

		rangeReq := &gestalt.KeyRange{Lower: "2026-04-15"}
		values := []any{"claude_code"}
		want := []string{"claude-apr-30", "claude-may-01"}

		assertCompoundVendorDateIndexQuery(t, sess.client, store, "by_vendor_date", rangeReq, values, want, nil)
	})

	t.Run("UpperOnly", func(t *testing.T) {
		sess := newSession(t, harness)
		t.Cleanup(sess.Close)

		store := "compound_vendor_date_upper"
		mustSeedCompoundVendorDateItems(t, sess.client, store)

		rangeReq := &gestalt.KeyRange{Upper: "2026-04-15"}
		values := []any{"claude_code"}
		want := []string{"claude-apr-09"}

		assertCompoundVendorDateIndexQuery(t, sess.client, store, "by_vendor_date", rangeReq, values, want, nil)
	})

	t.Run("DeleteRange", func(t *testing.T) {
		sess := newSession(t, harness)
		t.Cleanup(sess.Close)

		store := "compound_vendor_date_delete"
		mustSeedCompoundVendorDateItems(t, sess.client, store)

		rangeReq := &gestalt.KeyRange{
			Lower: "2026-04-01",
			Upper: "2026-04-30",
		}
		values := []any{"claude_code"}

		deleted := mustIndexDeleteWithRange(t, sess.client, store, "by_vendor_date", rangeReq, values...)
		if deleted != 2 {
			t.Fatalf("IndexDelete range deleted = %d, want 2", deleted)
		}
		remaining := sortedStrings(recordPrimaryKeys(t, mustGetAll(t, sess.client, store, nil)))
		if !stringSlicesEqual(remaining, []string{"beta-apr-01", "claude-may-01", "codex-apr-09", "cursor-apr-10"}) {
			t.Fatalf("remaining ids after IndexDelete(range) = %#v, want %#v", remaining, []string{"beta-apr-01", "claude-may-01", "codex-apr-09", "cursor-apr-10"})
		}
	})
}

func assertCompoundVendorDateIndexQuery(
	t *testing.T,
	client indexeddb.Database,
	store, index string,
	rangeReq *gestalt.KeyRange,
	values []any,
	want []string,
	wantCursorKeys [][]any,
) {
	t.Helper()

	records := mustIndexGetAllWithRange(t, client, store, index, rangeReq, values...)
	if got := recordPrimaryKeys(t, records); !stringSlicesEqual(got, want) {
		t.Fatalf("IndexGetAll %s ids = %#v, want %#v", index, got, want)
	}
	keys := mustIndexGetAllKeysWithRange(t, client, store, index, rangeReq, values...)
	if !stringSlicesEqual(keys, want) {
		t.Fatalf("IndexGetAllKeys %s ids = %#v, want %#v", index, keys, want)
	}
	count := mustIndexCountWithRange(t, client, store, index, rangeReq, values...)
	if count != int64(len(want)) {
		t.Fatalf("IndexCount %s = %d, want %d", index, count, len(want))
	}
	if wantCursorKeys == nil {
		return
	}
	entries := collectCursorEntries(t, client, &cursorRequest{
		Store:     store,
		Index:     index,
		Direction: gestalt.CursorNext,
		Range:     rangeReq,
		Values:    values,
	})
	if got := cursorPrimaryKeys(entries); !stringSlicesEqual(got, want) {
		t.Fatalf("index cursor ids = %#v, want %#v", got, want)
	}
	for i, entry := range entries {
		assertValueSliceEqual(t, cursorKeyValues(t, entry), wantCursorKeys[i])
	}
}

func runCompoundIndexIncompletePrefixRangeRejected(t *testing.T, harness Harness) {
	t.Helper()

	sess := newSession(t, harness)
	t.Cleanup(sess.Close)

	store := "compound_vendor_date_model"
	mustCreateObjectStore(t, sess.client, store, gestalt.ObjectStoreOptions{
		Indexes: []gestalt.IndexSchema{
			{Name: "by_vendor_date_model", KeyPath: []string{"vendor", "date", "model"}},
		},
	})
	mustAddRecord(t, sess.client, store, gestalt.Record{
		"id": "claude-apr-09-gpt", "vendor": "claude_code", "date": "2026-04-09", "model": "gpt-4",
	})

	rangeReq := &gestalt.KeyRange{Lower: "2026-04-01", Upper: "2026-04-30"}
	values := []any{"claude_code"}
	index := sess.client.ObjectStore(store).Index("by_vendor_date_model")

	assertIncompletePrefixRangeRejected(t, "GetAll", func() error {
		_, err := index.GetAll(context.Background(), rangeReq, values...)
		return err
	})
	assertIncompletePrefixRangeRejected(t, "GetAllKeys", func() error {
		_, err := index.GetAllKeys(context.Background(), rangeReq, values...)
		return err
	})
	assertIncompletePrefixRangeRejected(t, "Count", func() error {
		_, err := index.Count(context.Background(), rangeReq, values...)
		return err
	})
	assertIncompletePrefixRangeRejected(t, "DeleteRange", func() error {
		_, err := index.DeleteRange(context.Background(), rangeReq, values...)
		return err
	})
	assertIncompletePrefixRangeRejected(t, "OpenCursor", func() error {
		_, err := index.OpenCursor(context.Background(), rangeReq, gestalt.CursorNext, values...)
		return err
	})
	assertIncompletePrefixRangeRejected(t, "OpenKeyCursor", func() error {
		_, err := index.OpenKeyCursor(context.Background(), rangeReq, gestalt.CursorNext, values...)
		return err
	})
}

func assertIncompletePrefixRangeRejected(t *testing.T, surface string, call func() error) {
	t.Helper()
	err := call()
	if err == nil {
		t.Fatalf("%s with incomplete prefix + scalar range: want error, got nil", surface)
	}
	if got := err.Error(); !strings.Contains(got, "unpinned") {
		t.Fatalf("%s error = %q, want message about unpinned index key components", surface, got)
	}
}

func runKeyOnlyCursorSkipsUnreadableValues(t *testing.T, harness Harness) {
	t.Helper()

	sess := newSession(t, harness)
	t.Cleanup(sess.Close)

	store := "key_only_unreadable_rows"
	mustCreateObjectStore(t, sess.client, store, unreadablePayloadSchema())
	harness.InsertUnreadablePayloadRow(t, store, "broken", "active")

	objectStoreEntries := collectCursorEntries(t, sess.client, &cursorRequest{
		Store:     store,
		Direction: gestalt.CursorNext,
		KeysOnly:  true,
	})
	if len(objectStoreEntries) != 1 {
		t.Fatalf("object-store key cursor entry count = %d, want 1", len(objectStoreEntries))
	}
	if got := objectStoreEntries[0].PrimaryKey; got != "broken" {
		t.Fatalf("object-store key cursor primary key = %q, want %q", got, "broken")
	}
	if objectStoreEntries[0].Record != nil {
		t.Fatalf("object-store key cursor returned record: %+v", objectStoreEntries[0].Record)
	}

	indexEntries := collectCursorEntries(t, sess.client, &cursorRequest{
		Store:     store,
		Index:     "by_status",
		Direction: gestalt.CursorNext,
		Range:     &gestalt.KeyRange{Lower: "active", Upper: "active"},
		KeysOnly:  true,
	})
	if len(indexEntries) != 1 {
		t.Fatalf("index key cursor entry count = %d, want 1", len(indexEntries))
	}
	if got := indexEntries[0].PrimaryKey; got != "broken" {
		t.Fatalf("index key cursor primary key = %q, want %q", got, "broken")
	}
	if indexEntries[0].Record != nil {
		t.Fatalf("index key cursor returned record: %+v", indexEntries[0].Record)
	}
	if got := cursorKeyValues(t, indexEntries[0]); len(got) != 1 || got[0] != "active" {
		t.Fatalf("index key cursor key = %#v, want [\"active\"]", got)
	}

	rangeReq := &gestalt.KeyRange{Lower: "active", Upper: "active"}
	keys := mustIndexGetAllKeysWithRange(t, sess.client, store, "by_status", rangeReq)
	if !stringSlicesEqual(keys, []string{"broken"}) {
		t.Fatalf("IndexGetAllKeys unreadable ids = %#v, want %#v", keys, []string{"broken"})
	}
	count := mustIndexCountWithRange(t, sess.client, store, "by_status", rangeReq)
	if count != 1 {
		t.Fatalf("IndexCount unreadable = %d, want 1", count)
	}
	deleted := mustIndexDeleteWithRange(t, sess.client, store, "by_status", rangeReq)
	if deleted != 1 {
		t.Fatalf("IndexDelete unreadable deleted = %d, want 1", deleted)
	}
	if remaining := mustCount(t, sess.client, store, nil); remaining != 0 {
		t.Fatalf("remaining unreadable rows after IndexDelete = %d, want 0", remaining)
	}
}

func runCursorMutationWithTypedKeys(t *testing.T, harness Harness) {
	t.Helper()

	cases := []struct {
		name       string
		columnType gestalt.ColumnType
		id         any
	}{
		{name: "Numeric", columnType: typeInt, id: int64(42)},
		{name: "Binary", columnType: typeBytes, id: []byte{0x42}},
	}

	for _, tc := range cases {
		t.Run(tc.name+"Delete", func(t *testing.T) {
			sess := newSession(t, harness)
			t.Cleanup(sess.Close)

			store := fmt.Sprintf("cursor_delete_%s", tc.name)
			mustCreateObjectStore(t, sess.client, store, typedPrimaryKeySchema(tc.columnType))
			mustAddRecord(t, sess.client, store, map[string]any{"id": tc.id, "name": "before"})

			cursor := mustOpenCursor(t, sess.client, &cursorRequest{
				Store:     store,
				Direction: gestalt.CursorNext,
			})
			t.Cleanup(func() { _ = cursor.Close() })

			if !cursor.Continue() {
				t.Fatalf("cursor exhausted before delete: %v", cursor.Err())
			}
			if err := cursor.Delete(); err != nil {
				t.Fatalf("cursor Delete: %v", err)
			}

			records := mustGetAll(t, sess.client, store, nil)
			if len(records) != 0 {
				t.Fatalf("record count after delete = %d, want 0", len(records))
			}
		})

		t.Run(tc.name+"Update", func(t *testing.T) {
			sess := newSession(t, harness)
			t.Cleanup(sess.Close)

			store := fmt.Sprintf("cursor_update_%s", tc.name)
			mustCreateObjectStore(t, sess.client, store, typedPrimaryKeySchema(tc.columnType))
			mustAddRecord(t, sess.client, store, map[string]any{"id": tc.id, "name": "before"})

			cursor := mustOpenCursor(t, sess.client, &cursorRequest{
				Store:     store,
				Direction: gestalt.CursorNext,
			})
			t.Cleanup(func() { _ = cursor.Close() })

			if !cursor.Continue() {
				t.Fatalf("cursor exhausted before update: %v", cursor.Err())
			}

			if err := cursor.Update(map[string]any{"name": "after"}); err != nil {
				t.Fatalf("cursor Update: %v", err)
			}
			updated := cursorEntryFromCursor(t, cursor, false).Record
			assertValueEqual(t, updated["id"], tc.id)
			if got := updated["name"]; got != "after" {
				t.Fatalf("updated name = %#v, want %q", got, "after")
			}

			records := mustGetAll(t, sess.client, store, nil)
			if len(records) != 1 {
				t.Fatalf("record count after update = %d, want 1", len(records))
			}
			assertValueEqual(t, records[0]["id"], tc.id)
			if got := records[0]["name"]; got != "after" {
				t.Fatalf("persisted name = %#v, want %q", got, "after")
			}
		})
	}
}

func runEmptyStoreCursors(t *testing.T, harness Harness) {
	t.Helper()

	sess := newSession(t, harness)
	t.Cleanup(sess.Close)

	store := "empty_store_cursors"
	mustCreateObjectStore(t, sess.client, store, bulkItemsSchema())

	for _, req := range []cursorRequest{
		{Store: store, Direction: gestalt.CursorNext},
		{Store: store, Direction: gestalt.CursorNext, KeysOnly: true},
		{Store: store, Index: "by_status", Direction: gestalt.CursorNext},
		{Store: store, Index: "by_status", Direction: gestalt.CursorNext, KeysOnly: true},
	} {
		entries := collectCursorEntries(t, sess.client, &req)
		if len(entries) != 0 {
			t.Fatalf("cursor on empty store returned %d entries, want 0", len(entries))
		}
	}
}

func runBulkConsistency(t *testing.T, harness Harness) {
	t.Helper()

	t.Run("ObjectStoreRange", func(t *testing.T) {
		sess := newSession(t, harness)
		t.Cleanup(sess.Close)

		store := "bulk_object_store_range"
		mustSeedBulkItems(t, sess.client, store)

		rangeReq := &gestalt.KeyRange{
			Lower: "b",
			Upper: "c",
		}
		records := mustGetAll(t, sess.client, store, rangeReq)
		keys := mustGetAllKeys(t, sess.client, store, rangeReq)
		count := mustCount(t, sess.client, store, rangeReq)

		gotIDs := sortedStrings(recordPrimaryKeys(t, records))
		want := []string{"b", "c"}
		if !stringSlicesEqual(gotIDs, want) {
			t.Fatalf("GetAll range ids = %#v, want %#v", gotIDs, want)
		}
		if !stringSlicesEqual(sortedStrings(keys), want) {
			t.Fatalf("GetAllKeys range ids = %#v, want %#v", sortedStrings(keys), want)
		}
		if count != int64(len(want)) {
			t.Fatalf("Count range = %d, want %d", count, len(want))
		}

		deleted := mustDeleteRange(t, sess.client, store, rangeReq)
		if deleted != int64(len(want)) {
			t.Fatalf("DeleteRange deleted = %d, want %d", deleted, len(want))
		}
		remaining := sortedStrings(recordPrimaryKeys(t, mustGetAll(t, sess.client, store, nil)))
		if !stringSlicesEqual(remaining, []string{"a", "d"}) {
			t.Fatalf("remaining ids after DeleteRange = %#v, want %#v", remaining, []string{"a", "d"})
		}
	})

	t.Run("IndexQuery", func(t *testing.T) {
		sess := newSession(t, harness)
		t.Cleanup(sess.Close)

		store := "bulk_index_query"
		mustSeedBulkItems(t, sess.client, store)

		values := []any{"active"}
		records := mustIndexGetAll(t, sess.client, store, "by_status", values)
		keys := mustIndexGetAllKeys(t, sess.client, store, "by_status", values)
		count := mustIndexCount(t, sess.client, store, "by_status", values)

		want := []string{"a", "b", "d"}
		gotIDs := recordPrimaryKeys(t, records)
		if !stringSlicesEqual(gotIDs, want) {
			t.Fatalf("IndexGetAll ids = %#v, want %#v", gotIDs, want)
		}
		if !stringSlicesEqual(keys, want) {
			t.Fatalf("IndexGetAllKeys ids = %#v, want %#v", keys, want)
		}
		if count != int64(len(want)) {
			t.Fatalf("IndexCount = %d, want %d", count, len(want))
		}

		deleted := mustIndexDelete(t, sess.client, store, "by_status", values)
		if deleted != int64(len(want)) {
			t.Fatalf("IndexDelete deleted = %d, want %d", deleted, len(want))
		}
		remaining := sortedStrings(recordPrimaryKeys(t, mustGetAll(t, sess.client, store, nil)))
		if !stringSlicesEqual(remaining, []string{"c"}) {
			t.Fatalf("remaining ids after IndexDelete = %#v, want %#v", remaining, []string{"c"})
		}
	})

	t.Run("IndexRange", func(t *testing.T) {
		sess := newSession(t, harness)
		t.Cleanup(sess.Close)

		store := "bulk_index_range"
		mustSeedBulkItems(t, sess.client, store)

		rangeReq := &gestalt.KeyRange{Lower: "active", Upper: "active"}
		records := mustIndexGetAllWithRange(t, sess.client, store, "by_status", rangeReq)
		keys := mustIndexGetAllKeysWithRange(t, sess.client, store, "by_status", rangeReq)
		count := mustIndexCountWithRange(t, sess.client, store, "by_status", rangeReq)

		want := []string{"a", "b", "d"}
		gotIDs := recordPrimaryKeys(t, records)
		if !stringSlicesEqual(gotIDs, want) {
			t.Fatalf("IndexGetAll range ids = %#v, want %#v", gotIDs, want)
		}
		if !stringSlicesEqual(keys, want) {
			t.Fatalf("IndexGetAllKeys range ids = %#v, want %#v", keys, want)
		}
		if count != int64(len(want)) {
			t.Fatalf("IndexCount range = %d, want %d", count, len(want))
		}

		deleted := mustIndexDeleteWithRange(t, sess.client, store, "by_status", rangeReq)
		if deleted != int64(len(want)) {
			t.Fatalf("IndexDelete range deleted = %d, want %d", deleted, len(want))
		}
		remaining := sortedStrings(recordPrimaryKeys(t, mustGetAll(t, sess.client, store, nil)))
		if !stringSlicesEqual(remaining, []string{"c"}) {
			t.Fatalf("remaining ids after IndexDelete(range) = %#v, want %#v", remaining, []string{"c"})
		}
	})
}

func runExplicitTransactionSDKContract(t *testing.T, harness Harness) {
	t.Helper()

	sess := newSession(t, harness)
	t.Cleanup(sess.Close)

	store := "explicit_transaction_sdk_contract"
	mustCreateObjectStore(t, sess.client, store, gestalt.ObjectStoreOptions{
		Indexes: []gestalt.IndexSchema{
			{Name: "by_status", KeyPath: []string{"status"}},
		},
	})
	mustAddRecord(t, sess.client, store, map[string]any{
		"id":     "a",
		"name":   "Alpha",
		"status": "active",
	})

	tx := mustBeginTransaction(t, sess.client, []string{store}, gestalt.TransactionReadwrite)
	if err := txPut(t, tx, store, map[string]any{"id": "b", "name": "Beta", "status": "active"}); err != nil {
		t.Fatalf("transaction Put(b): %v", err)
	}
	gotB := mustTxGet(t, tx, store, "b")
	if gotB["name"] != "Beta" {
		t.Fatalf("transaction Get(b).name = %#v, want Beta", gotB["name"])
	}
	if got := mustTxIndexCount(t, tx, store, "by_status", "active"); got != 2 {
		t.Fatalf("transaction IndexCount(active) = %d, want 2", got)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("transaction Commit: %v", err)
	}
	mustGet(t, sess.client, store, "b")

	tx = mustBeginTransaction(t, sess.client, []string{store}, gestalt.TransactionReadwrite)
	if err := txPut(t, tx, store, map[string]any{"id": "c", "name": "Gamma", "status": "pending"}); err != nil {
		t.Fatalf("transaction Put(c): %v", err)
	}
	if err := txDelete(t, tx, store, "b"); err != nil {
		t.Fatalf("transaction Delete(b): %v", err)
	}
	if err := tx.Abort(context.Background()); err != nil {
		t.Fatalf("transaction Abort: %v", err)
	}
	mustGet(t, sess.client, store, "b")
	mustGetNotFound(t, sess.client, store, "c")

	tx = mustBeginTransaction(t, sess.client, []string{store}, gestalt.TransactionReadonly)
	err := txPut(t, tx, store, map[string]any{"id": "readonly", "name": "Read Only", "status": "active"})
	if !errors.Is(err, gestalt.ErrReadOnly) {
		t.Fatalf("readonly transaction Put error = %v, want ErrReadOnly", err)
	}
	mustGetNotFound(t, sess.client, store, "readonly")

	tx = mustBeginTransaction(t, sess.client, []string{store}, gestalt.TransactionReadwrite)
	if err := txPut(t, tx, store, map[string]any{"id": "d", "name": "Delta", "status": "active"}); err != nil {
		t.Fatalf("transaction Put(d): %v", err)
	}
	err = txAdd(t, tx, store, map[string]any{"id": "a", "name": "Duplicate", "status": "active"})
	if !errors.Is(err, gestalt.ErrAlreadyExists) {
		t.Fatalf("duplicate Add error = %v, want ErrAlreadyExists", err)
	}
	mustGetNotFound(t, sess.client, store, "d")
}

func runTypedDeleteRangeFidelity(t *testing.T, harness Harness) {
	t.Helper()

	sess := newSession(t, harness)
	t.Cleanup(sess.Close)

	timeA := time.Date(2024, time.January, 1, 8, 0, 0, 0, time.UTC)
	timeB := time.Date(2024, time.January, 2, 8, 0, 0, 0, time.UTC)
	timeC := time.Date(2024, time.January, 3, 8, 0, 0, 0, time.UTC)

	cases := []struct {
		name       string
		store      string
		columnType gestalt.ColumnType
		ids        []any
		lower      any
		upper      any
		remaining  []any
	}{
		{
			name:       "Int64",
			store:      "typed_delete_range_int",
			columnType: typeInt,
			ids:        []any{int64(10), int64(20), int64(30)},
			lower:      int64(20),
			upper:      int64(30),
			remaining:  []any{int64(10)},
		},
		{
			name:       "Time",
			store:      "typed_delete_range_time",
			columnType: typeTime,
			ids:        []any{timeA, timeB, timeC},
			lower:      timeB,
			upper:      timeC,
			remaining:  []any{timeA},
		},
		{
			name:       "Bytes",
			store:      "typed_delete_range_bytes",
			columnType: typeBytes,
			ids:        []any{[]byte{0x01}, []byte{0x02}, []byte{0x03}},
			lower:      []byte{0x02},
			upper:      []byte{0x03},
			remaining:  []any{[]byte{0x01}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mustCreateObjectStore(t, sess.client, tc.store, typedPrimaryKeySchema(tc.columnType))
			for i, id := range tc.ids {
				mustAddRecord(t, sess.client, tc.store, map[string]any{
					"id":   id,
					"name": fmt.Sprintf("%s-%d", tc.name, i),
				})
			}

			deleted := mustDeleteRange(t, sess.client, tc.store, &gestalt.KeyRange{Lower: tc.lower, Upper: tc.upper})
			if deleted != 2 {
				t.Fatalf("DeleteRange deleted = %d, want 2", deleted)
			}

			remaining := sortedRecordIDs(t, mustGetAll(t, sess.client, tc.store, nil))
			assertValueSliceEqual(t, remaining, sortedValues(append([]any(nil), tc.remaining...)))
		})
	}
}

func runRestartReconfigurePersistsIndexes(t *testing.T, harness Harness) {
	t.Helper()

	sess := newSession(t, harness)
	t.Cleanup(sess.Close)

	store := "restart_persists_indexes"
	mustSeedBulkItems(t, sess.client, store)
	sess.Restart(t)

	values := []any{"active"}
	records := mustIndexGetAll(t, sess.client, store, "by_status", values)
	gotIDs := sortedStrings(recordPrimaryKeys(t, records))
	want := []string{"a", "b", "d"}
	if !stringSlicesEqual(gotIDs, want) {
		t.Fatalf("IndexGetAll ids after restart = %#v, want %#v", gotIDs, want)
	}

	entries := collectCursorEntries(t, sess.client, &cursorRequest{
		Store:     store,
		Index:     "by_status",
		Direction: gestalt.CursorNext,
		Range:     &gestalt.KeyRange{Lower: "active", Upper: "active"},
	})
	if got := sortedStrings(cursorPrimaryKeys(entries)); !stringSlicesEqual(got, want) {
		t.Fatalf("index cursor ids after restart = %#v, want %#v", got, want)
	}
}

func runMissingIndexFieldExclusion(t *testing.T, harness Harness) {
	t.Helper()

	sess := newSession(t, harness)
	t.Cleanup(sess.Close)

	store := "missing_index_field_exclusion"
	mustCreateObjectStore(t, sess.client, store, bulkItemsSchema())
	mustAddRecord(t, sess.client, store, map[string]any{
		"id":     "a",
		"name":   "Alice",
		"status": "active",
	})
	mustAddRecord(t, sess.client, store, map[string]any{
		"id":     "b",
		"name":   "Bob",
		"status": "active",
	})

	cursor := mustOpenCursor(t, sess.client, &cursorRequest{
		Store:     store,
		Index:     "by_status",
		Direction: gestalt.CursorNext,
		Range:     &gestalt.KeyRange{Lower: "active", Upper: "active"},
	})
	t.Cleanup(func() { _ = cursor.Close() })

	if !cursor.Continue() {
		t.Fatalf("cursor exhausted before update: %v", cursor.Err())
	}
	first := cursorEntryFromCursor(t, cursor, false)
	if got := first.PrimaryKey; got != "a" {
		t.Fatalf("first active cursor id = %q, want %q", got, "a")
	}

	if err := cursor.Update(map[string]any{"name": "Alice"}); err != nil {
		t.Fatalf("cursor Update(clear indexed field): %v", err)
	}

	values := []any{"active"}
	records := mustIndexGetAll(t, sess.client, store, "by_status", values)
	gotIDs := sortedStrings(recordPrimaryKeys(t, records))
	if !stringSlicesEqual(gotIDs, []string{"b"}) {
		t.Fatalf("active ids after clearing indexed field = %#v, want %#v", gotIDs, []string{"b"})
	}

	entries := collectCursorEntries(t, sess.client, &cursorRequest{
		Store:     store,
		Index:     "by_status",
		Direction: gestalt.CursorNext,
		Range:     &gestalt.KeyRange{Lower: "active", Upper: "active"},
	})
	if got := cursorPrimaryKeys(entries); !stringSlicesEqual(got, []string{"b"}) {
		t.Fatalf("active cursor ids after clearing indexed field = %#v, want %#v", got, []string{"b"})
	}
}

func runUniqueIndexConflictOnCursorUpdate(t *testing.T, harness Harness) {
	t.Helper()

	sess := newSession(t, harness)
	t.Cleanup(sess.Close)

	store := "unique_index_conflict_update"
	mustCreateObjectStore(t, sess.client, store, uniqueEmailSchema())
	mustAddRecord(t, sess.client, store, map[string]any{
		"id":    "a",
		"name":  "Alice",
		"email": "alice@test.com",
	})
	mustAddRecord(t, sess.client, store, map[string]any{
		"id":    "b",
		"name":  "Bob",
		"email": "bob@test.com",
	})

	cursor := mustOpenCursor(t, sess.client, &cursorRequest{
		Store:     store,
		Direction: gestalt.CursorNext,
	})
	t.Cleanup(func() { _ = cursor.Close() })

	if !cursor.Advance(2) {
		t.Fatalf("cursor exhausted before conflict update: %v", cursor.Err())
	}

	err := cursor.Update(map[string]any{
		"name":  "Bob",
		"email": "alice@test.com",
	})
	if !errors.Is(err, gestalt.ErrAlreadyExists) {
		t.Fatalf("cursor update error = %v, want ErrAlreadyExists", err)
	}
}

func runNestedIndexPaths(t *testing.T, harness Harness) {
	t.Helper()

	sess := newSession(t, harness)
	t.Cleanup(sess.Close)

	store := "nested_index_paths"
	mustCreateObjectStore(t, sess.client, store, gestalt.ObjectStoreOptions{
		Indexes: []gestalt.IndexSchema{
			{Name: "by_profile_name", KeyPath: []string{"profile.name"}},
		},
	})
	mustAddRecord(t, sess.client, store, map[string]any{
		"id":      "a",
		"profile": map[string]any{"name": "Alice"},
	})
	mustAddRecord(t, sess.client, store, map[string]any{
		"id":      "b",
		"profile": map[string]any{"name": "Bob"},
	})

	entries := collectCursorEntries(t, sess.client, &cursorRequest{
		Store:     store,
		Index:     "by_profile_name",
		Direction: gestalt.CursorNext,
		Range:     &gestalt.KeyRange{Lower: "Alice", Upper: "Alice"},
	})
	if len(entries) != 1 {
		t.Fatalf("nested index entry count = %d, want 1", len(entries))
	}
	if got := entries[0].PrimaryKey; got != "a" {
		t.Fatalf("nested index primary key = %q, want %q", got, "a")
	}
	if got := cursorKeyValues(t, entries[0]); len(got) != 1 || got[0] != "Alice" {
		t.Fatalf("nested index key = %#v, want [\"Alice\"]", got)
	}
}

func newSession(t *testing.T, harness Harness) *session {
	t.Helper()

	provider, providerClose := harness.NewProvider(t)
	startIndexedDBHost(t, provider)

	client, err := gestalt.IndexedDB(context.Background())
	if err != nil {
		providerClose()
		t.Fatalf("IndexedDB connect: %v", err)
	}

	return &session{
		harness: harness,
		client:  client,
		close: func() {
			_ = client.Close()
			providerClose()
		},
	}
}

func (s *session) Restart(t *testing.T) {
	t.Helper()
	s.Close()
	next := newSession(t, s.harness)
	s.client = next.client
	s.close = next.close
}

func (s *session) Close() {
	if s.close != nil {
		s.close()
		s.close = nil
	}
}

type cursorRequest struct {
	Store     string
	Range     *gestalt.KeyRange
	Direction gestalt.CursorDirection
	KeysOnly  bool
	Index     string
	Values    []any
}

func bulkItemsSchema() gestalt.ObjectStoreOptions {
	return gestalt.ObjectStoreOptions{
		Indexes: []gestalt.IndexSchema{
			{Name: "by_status", KeyPath: []string{"status"}},
		},
	}
}

func unreadablePayloadSchema() gestalt.ObjectStoreOptions {
	return gestalt.ObjectStoreOptions{
		Indexes: []gestalt.IndexSchema{
			{Name: "by_status", KeyPath: []string{"status"}},
		},
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: typeString, PrimaryKey: true, NotNull: true},
			{Name: "status", Type: typeString},
			{Name: "payload", Type: typeInt},
		},
	}
}

func uniqueEmailSchema() gestalt.ObjectStoreOptions {
	return gestalt.ObjectStoreOptions{
		Indexes: []gestalt.IndexSchema{
			{Name: "by_email", KeyPath: []string{"email"}, Unique: true},
		},
	}
}

func numericIndexSchema() gestalt.ObjectStoreOptions {
	return gestalt.ObjectStoreOptions{
		Indexes: []gestalt.IndexSchema{
			{Name: "by_rank", KeyPath: []string{"rank"}},
		},
	}
}

func typedPrimaryKeySchema(columnType gestalt.ColumnType) gestalt.ObjectStoreOptions {
	return gestalt.ObjectStoreOptions{
		Columns: []gestalt.ColumnDef{
			{Name: "id", Type: columnType, PrimaryKey: true, NotNull: true},
			{Name: "name", Type: typeString},
		},
	}
}

func mustSeedCompoundVendorDateItems(t *testing.T, client indexeddb.Database, store string) {
	t.Helper()

	mustCreateObjectStore(t, client, store, gestalt.ObjectStoreOptions{
		Indexes: []gestalt.IndexSchema{
			{Name: "by_vendor_date", KeyPath: []string{"vendor", "date"}},
		},
	})
	for _, record := range []gestalt.Record{
		{"id": "beta-apr-01", "vendor": "beta", "date": "2026-04-01"},
		{"id": "claude-apr-09", "vendor": "claude_code", "date": "2026-04-09"},
		{"id": "claude-apr-30", "vendor": "claude_code", "date": "2026-04-30"},
		{"id": "claude-may-01", "vendor": "claude_code", "date": "2026-05-01"},
		{"id": "codex-apr-09", "vendor": "codex", "date": "2026-04-09"},
		{"id": "cursor-apr-10", "vendor": "cursor", "date": "2026-04-10"},
	} {
		mustAddRecord(t, client, store, record)
	}
}

func mustSeedBulkItems(t *testing.T, client indexeddb.Database, store string) {
	t.Helper()

	mustCreateObjectStore(t, client, store, bulkItemsSchema())
	for _, record := range []gestalt.Record{
		{"id": "a", "name": "Alice", "status": "active"},
		{"id": "b", "name": "Bob", "status": "active"},
		{"id": "c", "name": "Carol", "status": "inactive"},
		{"id": "d", "name": "Dave", "status": "active"},
	} {
		mustAddRecord(t, client, store, record)
	}
}

func mustSeedNumericIndexItems(t *testing.T, client indexeddb.Database, store string) {
	t.Helper()

	mustCreateObjectStore(t, client, store, numericIndexSchema())
	for _, record := range []gestalt.Record{
		{"id": "a", "name": "Alpha", "rank": int64(9007199254740991)},
		{"id": "b", "name": "Beta", "rank": int64(9007199254740993)},
		{"id": "c", "name": "Gamma", "rank": int64(9007199254741001)},
		{"id": "d", "name": "Delta", "rank": int64(9007199254741013)},
	} {
		mustAddRecord(t, client, store, record)
	}
}

func mustCreateObjectStore(t *testing.T, client indexeddb.Database, store string, schema gestalt.ObjectStoreOptions) {
	t.Helper()
	if _, err := client.CreateObjectStore(context.Background(), store, schema); err != nil {
		t.Fatalf("CreateObjectStore(%s): %v", store, err)
	}
}

func mustAddRecord(t *testing.T, client indexeddb.Database, store string, record gestalt.Record) {
	t.Helper()
	if err := client.ObjectStore(store).Add(context.Background(), record); err != nil {
		t.Fatalf("Add(%s): %v", store, err)
	}
}

func mustGet(t *testing.T, client indexeddb.Database, store, id string) gestalt.Record {
	t.Helper()
	record, err := client.ObjectStore(store).Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get(%s/%s): %v", store, id, err)
	}
	return record
}

func mustGetNotFound(t *testing.T, client indexeddb.Database, store, id string) {
	t.Helper()
	_, err := client.ObjectStore(store).Get(context.Background(), id)
	if !errors.Is(err, gestalt.ErrNotFound) {
		t.Fatalf("Get(%s/%s) error = %v, want ErrNotFound", store, id, err)
	}
}

func mustBeginTransaction(t *testing.T, client indexeddb.Database, stores []string, mode gestalt.TransactionMode) indexeddb.Transaction {
	t.Helper()
	tx, err := client.Transaction(context.Background(), stores, mode, gestalt.TransactionOptions{})
	if err != nil {
		t.Fatalf("Transaction: %v", err)
	}
	return tx
}

func txAdd(t *testing.T, tx indexeddb.Transaction, store string, record gestalt.Record) error {
	t.Helper()
	return tx.ObjectStore(store).Add(context.Background(), record)
}

func txPut(t *testing.T, tx indexeddb.Transaction, store string, record gestalt.Record) error {
	t.Helper()
	return tx.ObjectStore(store).Put(context.Background(), record)
}

func txDelete(t *testing.T, tx indexeddb.Transaction, store, id string) error {
	t.Helper()
	return tx.ObjectStore(store).Delete(context.Background(), id)
}

func mustTxGet(t *testing.T, tx indexeddb.Transaction, store, id string) gestalt.Record {
	t.Helper()
	record, err := tx.ObjectStore(store).Get(context.Background(), id)
	if err != nil {
		t.Fatalf("transaction Get(%s/%s): %v", store, id, err)
	}
	return record
}

func mustTxIndexCount(t *testing.T, tx indexeddb.Transaction, store, index string, values ...any) int64 {
	t.Helper()
	count, err := tx.ObjectStore(store).Index(index).Count(context.Background(), nil, values...)
	if err != nil {
		t.Fatalf("transaction IndexCount(%s/%s): %v", store, index, err)
	}
	return count
}

func mustGetAll(t *testing.T, client indexeddb.Database, store string, keyRange *gestalt.KeyRange) []gestalt.Record {
	t.Helper()
	records, err := client.ObjectStore(store).GetAll(context.Background(), keyRange)
	if err != nil {
		t.Fatalf("GetAll(%s): %v", store, err)
	}
	return records
}

func mustGetAllKeys(t *testing.T, client indexeddb.Database, store string, keyRange *gestalt.KeyRange) []string {
	t.Helper()
	keys, err := client.ObjectStore(store).GetAllKeys(context.Background(), keyRange)
	if err != nil {
		t.Fatalf("GetAllKeys(%s): %v", store, err)
	}
	return keys
}

func mustCount(t *testing.T, client indexeddb.Database, store string, keyRange *gestalt.KeyRange) int64 {
	t.Helper()
	count, err := client.ObjectStore(store).Count(context.Background(), keyRange)
	if err != nil {
		t.Fatalf("Count(%s): %v", store, err)
	}
	return count
}

func mustDeleteRange(t *testing.T, client indexeddb.Database, store string, keyRange *gestalt.KeyRange) int64 {
	t.Helper()
	if keyRange == nil {
		t.Fatalf("DeleteRange(%s) requires a key range", store)
	}
	deleted, err := client.ObjectStore(store).DeleteRange(context.Background(), *keyRange)
	if err != nil {
		t.Fatalf("DeleteRange(%s): %v", store, err)
	}
	return deleted
}

func mustIndexGetAll(t *testing.T, client indexeddb.Database, store, index string, values []any) []gestalt.Record {
	t.Helper()
	return mustIndexGetAllWithRange(t, client, store, index, nil, values...)
}

func mustIndexGetAllWithRange(t *testing.T, client indexeddb.Database, store, index string, keyRange *gestalt.KeyRange, values ...any) []gestalt.Record {
	t.Helper()
	records, err := client.ObjectStore(store).Index(index).GetAll(context.Background(), keyRange, values...)
	if err != nil {
		t.Fatalf("IndexGetAll(%s/%s): %v", store, index, err)
	}
	return records
}

func mustIndexGetAllKeys(t *testing.T, client indexeddb.Database, store, index string, values []any) []string {
	t.Helper()
	return mustIndexGetAllKeysWithRange(t, client, store, index, nil, values...)
}

func mustIndexGetAllKeysWithRange(t *testing.T, client indexeddb.Database, store, index string, keyRange *gestalt.KeyRange, values ...any) []string {
	t.Helper()
	keys, err := client.ObjectStore(store).Index(index).GetAllKeys(context.Background(), keyRange, values...)
	if err != nil {
		t.Fatalf("IndexGetAllKeys(%s/%s): %v", store, index, err)
	}
	return keys
}

func mustIndexCount(t *testing.T, client indexeddb.Database, store, index string, values []any) int64 {
	t.Helper()
	return mustIndexCountWithRange(t, client, store, index, nil, values...)
}

func mustIndexCountWithRange(t *testing.T, client indexeddb.Database, store, index string, keyRange *gestalt.KeyRange, values ...any) int64 {
	t.Helper()
	count, err := client.ObjectStore(store).Index(index).Count(context.Background(), keyRange, values...)
	if err != nil {
		t.Fatalf("IndexCount(%s/%s): %v", store, index, err)
	}
	return count
}

func mustIndexDelete(t *testing.T, client indexeddb.Database, store, index string, values []any) int64 {
	t.Helper()
	return mustIndexDeleteWithRange(t, client, store, index, nil, values...)
}

func mustIndexDeleteWithRange(t *testing.T, client indexeddb.Database, store, index string, keyRange *gestalt.KeyRange, values ...any) int64 {
	t.Helper()
	deleted, err := client.ObjectStore(store).Index(index).DeleteRange(context.Background(), keyRange, values...)
	if err != nil {
		t.Fatalf("IndexDelete(%s/%s): %v", store, index, err)
	}
	return deleted
}

func mustOpenCursor(t *testing.T, client indexeddb.Database, req *cursorRequest) indexeddb.Cursor {
	t.Helper()
	direction := req.Direction
	if direction == "" {
		direction = gestalt.CursorNext
	}

	store := client.ObjectStore(req.Store)
	var (
		cursor indexeddb.Cursor
		err    error
	)
	if req.Index != "" {
		index := store.Index(req.Index)
		if req.KeysOnly {
			cursor, err = index.OpenKeyCursor(context.Background(), req.Range, direction, req.Values...)
		} else {
			cursor, err = index.OpenCursor(context.Background(), req.Range, direction, req.Values...)
		}
	} else if req.KeysOnly {
		cursor, err = store.OpenKeyCursor(context.Background(), req.Range, direction)
	} else {
		cursor, err = store.OpenCursor(context.Background(), req.Range, direction)
	}
	if err != nil {
		t.Fatalf("OpenCursor(%s/%s): %v", req.Store, req.Index, err)
	}
	return cursor
}

func collectCursorEntries(t *testing.T, client indexeddb.Database, req *cursorRequest) []cursorEntry {
	t.Helper()

	cursor := mustOpenCursor(t, client, req)
	defer func() { _ = cursor.Close() }()

	var entries []cursorEntry
	for cursor.Continue() {
		entries = append(entries, cursorEntryFromCursor(t, cursor, req.KeysOnly))
	}
	if err := cursor.Err(); err != nil {
		t.Fatalf("cursor iteration: %v", err)
	}
	return entries
}

func cursorEntryFromCursor(t *testing.T, cursor indexeddb.Cursor, keysOnly bool) cursorEntry {
	t.Helper()

	var record gestalt.Record
	if !keysOnly {
		var err error
		record, err = cursor.Value()
		if err != nil {
			t.Fatalf("cursor Value: %v", err)
		}
	}
	return cursorEntry{
		Key:        cursor.Key(),
		PrimaryKey: cursor.PrimaryKey(),
		Record:     record,
	}
}

func cursorKeyValues(t *testing.T, entry cursorEntry) []any {
	t.Helper()
	if values, ok := entry.Key.([]any); ok {
		return values
	}
	return []any{entry.Key}
}

func cursorScalarKey(t *testing.T, entry cursorEntry) any {
	t.Helper()
	values := cursorKeyValues(t, entry)
	if len(values) != 1 {
		t.Fatalf("cursor key length = %d, want 1", len(values))
	}
	return values[0]
}

func cursorPrimaryKeys(entries []cursorEntry) []string {
	keys := make([]string, len(entries))
	for i, entry := range entries {
		keys[i] = entry.PrimaryKey
	}
	return keys
}

func recordPrimaryKeys(t *testing.T, records []gestalt.Record) []string {
	t.Helper()
	keys := make([]string, len(records))
	for i, record := range records {
		keys[i] = fmt.Sprint(record["id"])
	}
	return keys
}

func sortedRecordIDs(t *testing.T, records []gestalt.Record) []any {
	t.Helper()
	ids := make([]any, len(records))
	for i, record := range records {
		ids[i] = record["id"]
	}
	return sortedValues(ids)
}

func sortedValues(values []any) []any {
	sort.Slice(values, func(i, j int) bool {
		return compareValues(values[i], values[j]) < 0
	})
	return values
}

func sortedStrings(values []string) []string {
	sort.Strings(values)
	return values
}

func stringSlicesEqual(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func assertValueSliceEqual(t *testing.T, got, want []any) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("value count = %d, want %d", len(got), len(want))
	}
	for i := range got {
		assertValueEqual(t, got[i], want[i])
	}
}

func assertValueEqual(t *testing.T, got, want any) {
	t.Helper()
	if compareValues(got, want) != 0 {
		t.Fatalf("value = %#v (%T), want %#v (%T)", got, got, want, want)
	}
}

func compareValues(a, b any) int {
	switch av := a.(type) {
	case string:
		bv := b.(string)
		switch {
		case av < bv:
			return -1
		case av > bv:
			return 1
		default:
			return 0
		}
	case int64:
		bv := b.(int64)
		switch {
		case av < bv:
			return -1
		case av > bv:
			return 1
		default:
			return 0
		}
	case time.Time:
		bv := b.(time.Time)
		switch {
		case av.Before(bv):
			return -1
		case av.After(bv):
			return 1
		default:
			return 0
		}
	case []byte:
		return bytes.Compare(av, b.([]byte))
	default:
		panic(fmt.Sprintf("unsupported comparison type %T", a))
	}
}
