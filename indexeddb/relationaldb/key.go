package relationaldb

import (
	"bytes"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"time"
)

type indexedDBKeyKind int

const (
	indexedDBKeyNumber indexedDBKeyKind = iota
	indexedDBKeyDate
	indexedDBKeyString
	indexedDBKeyBinary
	indexedDBKeyArray
)

type indexedDBKey struct {
	kind   indexedDBKeyKind
	number *big.Rat
	date   time.Time
	str    string
	binary []byte
	array  []indexedDBKey
}

func parseIndexedDBKey(value any) (indexedDBKey, error) {
	return parseIndexedDBKeyDepth(value, 0)
}

func parseIndexedDBKeyDepth(value any, depth int) (indexedDBKey, error) {
	if depth > 64 {
		return indexedDBKey{}, fmt.Errorf("array key nesting is too deep")
	}
	switch v := value.(type) {
	case int:
		return indexedDBNumberKey(big.NewRat(int64(v), 1)), nil
	case int8:
		return indexedDBNumberKey(big.NewRat(int64(v), 1)), nil
	case int16:
		return indexedDBNumberKey(big.NewRat(int64(v), 1)), nil
	case int32:
		return indexedDBNumberKey(big.NewRat(int64(v), 1)), nil
	case int64:
		return indexedDBNumberKey(big.NewRat(v, 1)), nil
	case uint:
		return indexedDBUintKey(uint64(v)), nil
	case uint8:
		return indexedDBUintKey(uint64(v)), nil
	case uint16:
		return indexedDBUintKey(uint64(v)), nil
	case uint32:
		return indexedDBUintKey(uint64(v)), nil
	case uint64:
		return indexedDBUintKey(v), nil
	case float32:
		return indexedDBFloatKey(float64(v))
	case float64:
		return indexedDBFloatKey(v)
	case time.Time:
		return indexedDBKey{kind: indexedDBKeyDate, date: v}, nil
	case string:
		return indexedDBKey{kind: indexedDBKeyString, str: v}, nil
	case []byte:
		return indexedDBKey{kind: indexedDBKeyBinary, binary: append([]byte(nil), v...)}, nil
	case []any:
		return indexedDBArrayKey(v, depth)
	default:
		return indexedDBReflectArrayKey(value, depth)
	}
}

func indexedDBNumberKey(v *big.Rat) indexedDBKey {
	return indexedDBKey{kind: indexedDBKeyNumber, number: v}
}

func indexedDBUintKey(v uint64) indexedDBKey {
	return indexedDBNumberKey(new(big.Rat).SetInt(new(big.Int).SetUint64(v)))
}

func indexedDBFloatKey(v float64) (indexedDBKey, error) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return indexedDBKey{}, fmt.Errorf("number key must be finite")
	}
	rat := new(big.Rat).SetFloat64(v)
	if rat == nil {
		return indexedDBKey{}, fmt.Errorf("number key must be finite")
	}
	return indexedDBNumberKey(rat), nil
}

func indexedDBArrayKey(values []any, depth int) (indexedDBKey, error) {
	out := make([]indexedDBKey, len(values))
	for i, value := range values {
		key, err := parseIndexedDBKeyDepth(value, depth+1)
		if err != nil {
			return indexedDBKey{}, fmt.Errorf("array key item %d: %w", i, err)
		}
		out[i] = key
	}
	return indexedDBKey{kind: indexedDBKeyArray, array: out}, nil
}

func indexedDBReflectArrayKey(value any, depth int) (indexedDBKey, error) {
	if value == nil {
		return indexedDBKey{}, fmt.Errorf("nil is not a valid IndexedDB key")
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return indexedDBKey{}, fmt.Errorf("%T is not a valid IndexedDB key", value)
	}
	out := make([]indexedDBKey, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		key, err := parseIndexedDBKeyDepth(rv.Index(i).Interface(), depth+1)
		if err != nil {
			return indexedDBKey{}, fmt.Errorf("array key item %d: %w", i, err)
		}
		out[i] = key
	}
	return indexedDBKey{kind: indexedDBKeyArray, array: out}, nil
}

func compareIndexedDBKeys(a, b indexedDBKey) int {
	if a.kind != b.kind {
		return compareInts(int(a.kind), int(b.kind))
	}
	switch a.kind {
	case indexedDBKeyNumber:
		return a.number.Cmp(b.number)
	case indexedDBKeyDate:
		switch {
		case a.date.Before(b.date):
			return -1
		case a.date.After(b.date):
			return 1
		default:
			return 0
		}
	case indexedDBKeyString:
		switch {
		case a.str < b.str:
			return -1
		case a.str > b.str:
			return 1
		default:
			return 0
		}
	case indexedDBKeyBinary:
		return bytes.Compare(a.binary, b.binary)
	case indexedDBKeyArray:
		for i := range a.array {
			if i >= len(b.array) {
				return 1
			}
			if cmp := compareIndexedDBKeys(a.array[i], b.array[i]); cmp != 0 {
				return cmp
			}
		}
		return compareInts(len(a.array), len(b.array))
	default:
		return 0
	}
}

func compareInts(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
