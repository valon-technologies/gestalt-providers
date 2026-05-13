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

func filterEntriesByPrefix(entries []cursorutil.Entry, values []any) ([]cursorutil.Entry, error) {
	if len(values) == 0 {
		return entries, nil
	}
	filtered := make([]cursorutil.Entry, 0, len(entries))
	for _, entry := range entries {
		entryParts := normalizeDocumentBound(entry.Key)
		if len(entryParts) < len(values) {
			continue
		}
		match := true
		for i, want := range values {
			if cursorutil.CompareValues(entryParts[i], want) != 0 {
				match = false
				break
			}
		}
		if match {
			filtered = append(filtered, entry)
		}
	}
	return filtered, nil
}

func applyKeyRangeToEntries(entries []cursorutil.Entry, keyRange *gestalt.KeyRange, indexCursor bool) ([]cursorutil.Entry, error) {
	if keyRange == nil {
		return entries, nil
	}
	lower, upper, err := cursorutil.RangeBounds(keyRange, indexCursor)
	if err != nil {
		return nil, err
	}
	filtered := make([]cursorutil.Entry, 0, len(entries))
	for _, entry := range entries {
		entryKey := entry.Key
		if indexCursor {
			entryKey = normalizeDocumentBound(entry.Key)
		}
		if lower != nil {
			cmp := cursorutil.CompareValues(entryKey, lower)
			if cmp < 0 || (cmp == 0 && keyRange.LowerOpen) {
				continue
			}
		}
		if upper != nil {
			cmp := cursorutil.CompareValues(entryKey, upper)
			if cmp > 0 || (cmp == 0 && keyRange.UpperOpen) {
				continue
			}
		}
		filtered = append(filtered, entry)
	}
	return filtered, nil
}

func normalizeDocumentBound(value any) []any {
	if parts, ok := value.([]any); ok {
		return parts
	}
	return []any{value}
}
