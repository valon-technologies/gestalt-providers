package v1

import (
	"github.com/valon-technologies/gestalt/sdk/go/internal/indexeddbcodec"
)

func TypedValueFromAny(v any) (*TypedValue, error) {
	return indexeddbcodec.TypedValueFromAny(v)
}

func AnyFromTypedValue(v *TypedValue) (any, error) {
	return indexeddbcodec.AnyFromTypedValue(v)
}

func TypedValuesFromAny(values []any) ([]*TypedValue, error) {
	return indexeddbcodec.TypedValuesFromAny(values)
}

func AnyFromTypedValues(values []*TypedValue) ([]any, error) {
	return indexeddbcodec.AnyFromTypedValues(values)
}

func RecordToNative(record *Record) (map[string]any, error) {
	return indexeddbcodec.RecordFromProto(record)
}

func RecordFromNative(record map[string]any) (*Record, error) {
	return indexeddbcodec.RecordToProto(record)
}

func RecordsFromNative(records []map[string]any) ([]*Record, error) {
	return indexeddbcodec.RecordsToProto(records)
}

func KeyValuesToAny(kvs []*KeyValue) ([]any, error) {
	return indexeddbcodec.KeyValuesToAny(kvs)
}

func CursorKeyToProto(key any, indexCursor bool) ([]*KeyValue, error) {
	return indexeddbcodec.CursorKeyToProto(key, indexCursor)
}
