package relationaldb

import (
	"sort"

	cursorutil "github.com/valon-technologies/gestalt-providers/indexeddb/internal/cursorutil"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"github.com/valon-technologies/gestalt/sdk/go/client"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

func queryExactKey(query *client.IndexedDBQuery) (any, bool) {
	if query == nil {
		return nil, false
	}
	kq, ok := query.Query.(*client.IndexedDBQueryQueryKey)
	if !ok || kq.Value == nil {
		return nil, false
	}
	key, err := indexeddb.KeyValueToAny(kq.Value)
	if err != nil {
		return nil, false
	}
	return key, true
}

func limitRecords[T any](items []T, count *uint32) []T {
	if count == nil || *count == 0 || int(*count) >= len(items) {
		return items
	}
	return items[:*count]
}

func recordsFrom(entries []cursorutil.Entry) []gestalt.Record {
	records := make([]gestalt.Record, 0, len(entries))
	for _, entry := range entries {
		records = append(records, entry.Record)
	}
	return records
}

func sortIndexEntries(entries []cursorutil.Entry) {
	sort.SliceStable(entries, func(i, j int) bool {
		if cmp := indexeddb.CompareKeys(entries[i].Key, entries[j].Key); cmp != 0 {
			return cmp < 0
		}
		return indexeddb.CompareKeys(entries[i].PrimaryKeyValue, entries[j].PrimaryKeyValue) < 0
	})
}

func sortObjectStoreEntries(entries []cursorutil.Entry) {
	sort.SliceStable(entries, func(i, j int) bool {
		return indexeddb.CompareKeys(entries[i].Key, entries[j].Key) < 0
	})
}

func filterEntriesByQuery(entries []cursorutil.Entry, query *client.IndexedDBQuery) ([]cursorutil.Entry, error) {
	if query == nil {
		return entries, nil
	}
	filtered := make([]cursorutil.Entry, 0, len(entries))
	for _, entry := range entries {
		ok, err := indexeddb.MatchQuery(entry.Key, query)
		if err != nil {
			return nil, err
		}
		if ok {
			filtered = append(filtered, entry)
		}
	}
	return filtered, nil
}
