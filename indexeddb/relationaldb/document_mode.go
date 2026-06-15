package relationaldb

import (
	"fmt"
	"strings"

	cursorutil "github.com/valon-technologies/gestalt-providers/indexeddb/internal/cursorutil"
	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func extractStringID(record gestalt.Record) (string, error) {
	value, err := recordFieldAny(record, "id")
	if err != nil {
		return "", status.Errorf(codes.InvalidArgument, "record id: %v", err)
	}
	id, ok := value.(string)
	if !ok || strings.TrimSpace(id) == "" {
		return "", status.Error(codes.InvalidArgument, `record id must be a non-empty string`)
	}
	return id, nil
}

func marshalRecordBlob(record gestalt.Record) ([]byte, error) {
	if record == nil {
		return nil, status.Error(codes.InvalidArgument, "record is required")
	}
	raw, err := gestalt.EncodeIndexedDBRecord(record)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "marshal record payload: %v", err)
	}
	return raw, nil
}

func unmarshalRecordBlob(raw []byte) (gestalt.Record, error) {
	if len(raw) == 0 {
		return nil, status.Error(codes.Internal, "record payload is empty")
	}
	record, err := gestalt.DecodeIndexedDBRecord(raw)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "unmarshal record payload: %v", err)
	}
	return record, nil
}

func recordFieldAny(record gestalt.Record, field string) (any, error) {
	if record == nil {
		return nil, fmt.Errorf("record is required")
	}
	parts := strings.Split(field, ".")
	current, ok := record[parts[0]]
	if !ok {
		return nil, fmt.Errorf("field %q not found", field)
	}
	for _, part := range parts[1:] {
		obj, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("field %q not found", field)
		}
		current, ok = obj[part]
		if !ok {
			return nil, fmt.Errorf("field %q not found", field)
		}
	}
	return current, nil
}

func indexKeyFromRecord(record gestalt.Record, idx *gestalt.IndexSchema) (any, bool, error) {
	if idx == nil {
		return nil, false, status.Error(codes.InvalidArgument, "index is required")
	}
	parts := make([]any, 0, len(idx.KeyPath))
	for _, field := range idx.KeyPath {
		value, err := recordFieldAny(record, field)
		if err != nil {
			return nil, false, nil
		}
		parts = append(parts, value)
	}
	if len(parts) == 1 {
		return parts[0], true, nil
	}
	return parts, true, nil
}

// normalizeIndexQuery lowers Gestalt prefix+scalar-range queries to W3C compound
// array-key ranges and separates prefix-only pins from range bounds.
func normalizeIndexQuery(idx *gestalt.IndexSchema, values []any, keyRange *gestalt.KeyRange) (pin []any, effectiveRange *gestalt.KeyRange, err error) {
	if idx == nil {
		return nil, nil, status.Error(codes.InvalidArgument, "index is required")
	}
	if len(values) > len(idx.KeyPath) {
		return nil, nil, status.Errorf(codes.InvalidArgument,
			"index query has %d values for %d-part index %q",
			len(values), len(idx.KeyPath), idx.Name)
	}
	if keyRange != nil {
		if len(values) >= len(idx.KeyPath) {
			return nil, nil, status.Errorf(codes.InvalidArgument,
				"range cannot be combined with a complete index key for index %q",
				idx.Name)
		}
		if len(values) > 0 && len(values)+1 < len(idx.KeyPath) {
			return nil, nil, status.Errorf(codes.InvalidArgument,
				"range with %d pinned values leaves unpinned index key components for %d-part index %q; use a full compound array range with no values",
				len(values), len(idx.KeyPath), idx.Name)
		}
	}
	if keyRange != nil && len(values) > 0 && len(values)+1 == len(idx.KeyPath) {
		return values, compoundKeyRange(values, keyRange), nil
	}
	if len(values) > 0 && keyRange == nil {
		return values, nil, nil
	}
	return nil, keyRange, nil
}

func compoundKeyRange(prefix []any, keyRange *gestalt.KeyRange) *gestalt.KeyRange {
	if keyRange == nil {
		return nil
	}
	out := &gestalt.KeyRange{
		LowerOpen: keyRange.LowerOpen,
		UpperOpen: keyRange.UpperOpen,
	}
	if keyRange.Lower != nil {
		out.Lower = append(append([]any(nil), prefix...), keyRange.Lower)
	}
	if keyRange.Upper != nil {
		out.Upper = append(append([]any(nil), prefix...), keyRange.Upper)
	}
	return out
}

func keyInRange(key any, keyRange *gestalt.KeyRange, indexCursor bool) (bool, error) {
	if keyRange == nil {
		return true, nil
	}
	lower, upper, err := cursorutil.RangeBounds(keyRange, indexCursor)
	if err != nil {
		return false, err
	}
	entryKey := key
	if indexCursor {
		entryKey = normalizeDocumentBound(key)
	}
	if lower != nil {
		cmp := cursorutil.CompareValues(entryKey, lower)
		if cmp < 0 || (cmp == 0 && keyRange.LowerOpen) {
			return false, nil
		}
	}
	if upper != nil {
		cmp := cursorutil.CompareValues(entryKey, upper)
		if cmp > 0 || (cmp == 0 && keyRange.UpperOpen) {
			return false, nil
		}
	}
	return true, nil
}

func entryMatchesPin(entry cursorutil.Entry, pin []any) bool {
	if len(pin) == 0 {
		return true
	}
	parts := normalizeDocumentBound(entry.Key)
	for i, want := range pin {
		if i >= len(parts) || cursorutil.CompareValues(parts[i], want) != 0 {
			return false
		}
	}
	return true
}

func filterEntriesByPin(entries []cursorutil.Entry, pin []any) []cursorutil.Entry {
	if len(pin) == 0 {
		return entries
	}
	filtered := make([]cursorutil.Entry, 0, len(entries))
	for _, entry := range entries {
		if entryMatchesPin(entry, pin) {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

func filterEntriesByIndexQuery(entries []cursorutil.Entry, pin []any, keyRange *gestalt.KeyRange) ([]cursorutil.Entry, error) {
	entries = filterEntriesByPin(entries, pin)
	return applyKeyRangeToEntries(entries, keyRange, true)
}

func applyKeyRangeToEntries(entries []cursorutil.Entry, keyRange *gestalt.KeyRange, indexCursor bool) ([]cursorutil.Entry, error) {
	if keyRange == nil {
		return entries, nil
	}
	filtered := make([]cursorutil.Entry, 0, len(entries))
	for _, entry := range entries {
		ok, err := keyInRange(entry.Key, keyRange, indexCursor)
		if err != nil {
			return nil, err
		}
		if ok {
			filtered = append(filtered, entry)
		}
	}
	return filtered, nil
}

func normalizeDocumentBound(value any) []any {
	if parts, ok := value.([]any); ok {
		return parts
	}
	return []any{value}
}
