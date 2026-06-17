package relationaldb

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/big"
	"reflect"
	"time"
	"unicode/utf16"

	"github.com/valon-technologies/gestalt/sdk/go/client"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	tagArrayTerm = 0x00
	tagNumber    = 0x10
	tagDate      = 0x20
	tagString    = 0x30
	tagBinary    = 0x40
	tagArray     = 0x50

	numberSubtagFloat = 0x00
	numberSubtagInt   = 0x01
)

// encodeOrderedKey serializes a native IndexedDB key into a memcomparable byte
// string: bytes.Compare(encodeOrderedKey(a), encodeOrderedKey(b)) has the same
// sign as indexeddb.CompareKeys(a, b) for all valid keys. It is the byte image
// of the W3C ordering implemented by indexeddb.CompareKeys and is used only for
// SQL range scans on index_key_ord. It is not round-trippable.
func encodeOrderedKey(value any) ([]byte, error) {
	if value == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid key: nil")
	}
	if parts, ok := orderedKeyArrayParts(value); ok {
		return encodeOrderedArray(parts)
	}
	switch v := value.(type) {
	case []byte:
		return encodeOrderedBinary(v)
	case time.Time:
		return encodeOrderedDate(v)
	case string:
		return encodeOrderedString(v)
	case bool:
		return encodeOrderedNumberValue(v)
	default:
		if _, ok := numberRat(v); ok {
			return encodeOrderedNumberValue(v)
		}
		return nil, status.Errorf(codes.InvalidArgument, "invalid key type: %T", value)
	}
}

func encodeOrderedNumberValue(value any) ([]byte, error) {
	rat, ok := numberRat(value)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "invalid number key")
	}
	if rat.Sign() == 0 {
		return encodeOrderedFloat64(0)
	}
	if rat.IsInt() {
		return encodeOrderedIntegerRat(rat)
	}
	if ratFitsExactFloat64(rat) {
		f, _ := rat.Float64()
		return encodeOrderedFloat64(f)
	}
	f, _ := rat.Float64()
	return encodeOrderedFloat64(f)
}

func encodeOrderedIntegerRat(rat *big.Rat) ([]byte, error) {
	f, _ := rat.Float64()
	if math.IsInf(f, 0) {
		return encodeOrderedBigInt(rat.Num())
	}
	base, err := encodeOrderedFloat64(f)
	if err != nil {
		return nil, err
	}
	fRat := new(big.Rat).SetFloat64(f)
	cmp := rat.Cmp(fRat)
	if cmp == 0 {
		return base, nil
	}
	diff := new(big.Rat).Sub(rat, fRat)
	diff.Abs(diff)
	suffix := encodeRatULPOffset(diff)
	if cmp > 0 {
		out := append([]byte(nil), base...)
		out = append(out, 0x02)
		out = append(out, suffix...)
		return out, nil
	}
	out := append([]byte(nil), base...)
	out = append(out, 0x01)
	out = append(out, suffix...)
	return out, nil
}

func encodeRatULPOffset(offset *big.Rat) []byte {
	num := new(big.Int).Abs(offset.Num())
	den := offset.Denom()
	numMag := encodeLengthPrefixedMag(num.Bytes(), false)
	denMag := encodeLengthPrefixedMag(den.Bytes(), false)
	out := make([]byte, 0, len(numMag)+len(denMag))
	out = append(out, numMag...)
	return append(out, denMag...)
}

func ratFitsExactFloat64(rat *big.Rat) bool {
	f, _ := rat.Float64()
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return false
	}
	back := new(big.Rat).SetFloat64(f)
	return rat.Cmp(back) == 0
}

func orderedKeyArrayParts(v any) ([]any, bool) {
	if arr, ok := v.([]any); ok {
		return append([]any(nil), arr...), true
	}
	if _, ok := v.([]byte); ok {
		return nil, false
	}
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return nil, false
	}
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
	default:
		return nil, false
	}
	if rv.Type().Elem().Kind() == reflect.Uint8 {
		return nil, false
	}
	parts := make([]any, rv.Len())
	for i := range parts {
		parts[i] = rv.Index(i).Interface()
	}
	return parts, true
}

func encodeOrderedArray(parts []any) ([]byte, error) {
	out := []byte{tagArray}
	for _, part := range parts {
		encoded, err := encodeOrderedKey(part)
		if err != nil {
			return nil, err
		}
		out = append(out, encoded...)
	}
	return append(out, tagArrayTerm), nil
}

func encodeOrderedFloat64(v float64) ([]byte, error) {
	if math.IsNaN(v) {
		return nil, status.Error(codes.InvalidArgument, "invalid key: NaN")
	}
	if v == 0 {
		v = 0 // canonicalize -0.0 to +0.0
	}
	bits := math.Float64bits(v)
	if bits&(1<<63) != 0 {
		bits = ^bits
	} else {
		bits |= 1 << 63
	}
	out := make([]byte, 1+1+8)
	out[0] = tagNumber
	out[1] = numberSubtagFloat
	binary.BigEndian.PutUint64(out[2:], bits)
	return out, nil
}

func encodeOrderedBigInt(n *big.Int) ([]byte, error) {
	if n.Sign() == 0 {
		return encodeOrderedFloat64(0)
	}
	sign := n.Sign()
	abs := new(big.Int).Abs(n)
	mag := abs.Bytes()
	out := make([]byte, 0, 2+4+len(mag))
	out = append(out, tagNumber, numberSubtagInt)
	if sign < 0 {
		out = append(out, 0x00)
		out = append(out, encodeLengthPrefixedMag(mag, true)...)
		return out, nil
	}
	out = append(out, 0x02)
	out = append(out, encodeLengthPrefixedMag(mag, false)...)
	return out, nil
}

func encodeLengthPrefixedMag(mag []byte, negative bool) []byte {
	if len(mag) == 0 {
		mag = []byte{0x00}
	}
	out := make([]byte, 4+len(mag))
	binary.BigEndian.PutUint32(out, uint32(len(mag)))
	if negative {
		for i, b := range mag {
			out[4+i] = ^b
		}
		return out
	}
	copy(out[4:], mag)
	return out
}

func encodeOrderedDate(t time.Time) ([]byte, error) {
	n := uint64(t.UnixNano()) ^ 0x8000000000000000
	out := make([]byte, 1+8)
	out[0] = tagDate
	binary.BigEndian.PutUint64(out[1:], n)
	return out, nil
}

func encodeOrderedString(s string) ([]byte, error) {
	units := utf16.Encode([]rune(s))
	out := make([]byte, 0, 1+len(units)*2+2)
	out = append(out, tagString)
	for _, unit := range units {
		var unitBuf [2]byte
		binary.BigEndian.PutUint16(unitBuf[:], unit)
		out = appendEscapedBytes(out, unitBuf[:])
	}
	return append(out, 0x00, 0x00), nil
}

func encodeOrderedBinary(raw []byte) ([]byte, error) {
	out := make([]byte, 0, 1+len(raw)*2+2)
	out = append(out, tagBinary)
	out = appendEscapedBytes(out, raw)
	return append(out, 0x00, 0x00), nil
}

func appendEscapedBytes(out, raw []byte) []byte {
	for _, b := range raw {
		out = append(out, b)
		if b == 0x00 {
			out = append(out, 0xFF)
		}
	}
	return out
}

func numberRat(v any) (*big.Rat, bool) {
	switch n := v.(type) {
	case bool:
		if n {
			return big.NewRat(1, 1), true
		}
		return big.NewRat(0, 1), true
	case int:
		return big.NewRat(int64(n), 1), true
	case int8:
		return big.NewRat(int64(n), 1), true
	case int16:
		return big.NewRat(int64(n), 1), true
	case int32:
		return big.NewRat(int64(n), 1), true
	case int64:
		return big.NewRat(n, 1), true
	case uint:
		return new(big.Rat).SetInt(new(big.Int).SetUint64(uint64(n))), true
	case uint8:
		return new(big.Rat).SetInt(new(big.Int).SetUint64(uint64(n))), true
	case uint16:
		return new(big.Rat).SetInt(new(big.Int).SetUint64(uint64(n))), true
	case uint32:
		return new(big.Rat).SetInt(new(big.Int).SetUint64(uint64(n))), true
	case uint64:
		return new(big.Rat).SetInt(new(big.Int).SetUint64(n)), true
	case float32:
		return floatRat(float64(n))
	case float64:
		return floatRat(n)
	default:
		return nil, false
	}
}

func floatRat(v float64) (*big.Rat, bool) {
	if math.IsNaN(v) {
		return nil, false
	}
	r := new(big.Rat).SetFloat64(v)
	if r == nil {
		return nil, false
	}
	return r, true
}

// orderedBounds encodes the lower and upper bounds of kr for SQL range scans.
func orderedBounds(kr *client.KeyRange) (lo, hi []byte, loOpen, hiOpen bool, err error) {
	if kr == nil {
		return nil, nil, false, false, nil
	}
	loOpen = kr.LowerOpen
	hiOpen = kr.UpperOpen
	if kr.Lower != nil {
		lower, err := indexeddb.KeyValueToAny(kr.Lower)
		if err != nil {
			return nil, nil, false, false, err
		}
		lo, err = encodeOrderedKey(lower)
		if err != nil {
			return nil, nil, false, false, err
		}
	}
	if kr.Upper != nil {
		upper, err := indexeddb.KeyValueToAny(kr.Upper)
		if err != nil {
			return nil, nil, false, false, err
		}
		hi, err = encodeOrderedKey(upper)
		if err != nil {
			return nil, nil, false, false, err
		}
	}
	return lo, hi, loOpen, hiOpen, nil
}

// sqlPredicateMatches reports whether encoded key ord satisfies the same
// bounds as indexeddb.KeyInRange would for kr.
func sqlPredicateMatches(ord []byte, kr *client.KeyRange) (bool, error) {
	if kr == nil {
		return true, nil
	}
	lo, hi, loOpen, hiOpen, err := orderedBounds(kr)
	if err != nil {
		return false, err
	}
	if lo != nil {
		cmp := bytes.Compare(ord, lo)
		if loOpen {
			if cmp <= 0 {
				return false, nil
			}
		} else if cmp < 0 {
			return false, nil
		}
	}
	if hi != nil {
		cmp := bytes.Compare(ord, hi)
		if hiOpen {
			if cmp >= 0 {
				return false, nil
			}
		} else if cmp > 0 {
			return false, nil
		}
	}
	return true, nil
}
