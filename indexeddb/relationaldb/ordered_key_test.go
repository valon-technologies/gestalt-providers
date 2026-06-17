package relationaldb

import (
	"bytes"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/valon-technologies/gestalt/sdk/go/client"
	"github.com/valon-technologies/gestalt/sdk/go/indexeddb"
)

func TestOrderedKeyCompareMatchesW3C(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(1))

	for i := 0; i < 5000; i++ {
		a := randomOrderedKey(rng)
		b := randomOrderedKey(rng)
		assertOrderedKeyCompare(t, a, b)
	}

	hardCases := []struct {
		a any
		b any
	}{
		{int64(9007199254740991), int64(9007199254740993)},
		{int64(9007199254740993), int64(9007199254741001)},
		{uint64(9007199254740993), uint64(9007199254741001)},
		{float64(0), float64(-0)},
		{true, int64(1)},
		{false, int64(0)},
		{"a", "ab"},
		{"\U0001F600", "\U0001F601"},
		{[]byte{0x00, 0x01}, []byte{0x00, 0x02}},
		{[]any{"acme", "2026-06-01"}, []any{"acme", "2026-06-17"}},
		{[]any{"acme", "2026-06-01"}, []any{"acme", "2026-06-01", "extra"}},
	}
	for _, tc := range hardCases {
		assertOrderedKeyCompare(t, tc.a, tc.b)
	}
}

func TestOrderedKeyRangeMatchesKeyInRange(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewSource(2))

	for i := 0; i < 2000; i++ {
		key := randomScalarOrderedKey(rng)
		kr := randomKeyRange(rng)
		assertOrderedKeyInRange(t, key, kr)
	}
}

func randomScalarOrderedKey(rng *rand.Rand) any {
	switch rng.Intn(5) {
	case 0:
		return randomNumberKey(rng)
	case 1:
		return time.Unix(0, rng.Int63()).UTC()
	case 2:
		return randomStringKey(rng)
	default:
		return randomBinaryKey(rng)
	}
}

func assertOrderedKeyCompare(t *testing.T, a, b any) {
	t.Helper()
	encA, err := encodeOrderedKey(a)
	if err != nil {
		t.Fatalf("encodeOrderedKey(%#v): %v", a, err)
	}
	encB, err := encodeOrderedKey(b)
	if err != nil {
		t.Fatalf("encodeOrderedKey(%#v): %v", b, err)
	}
	want := sign(indexeddb.CompareKeys(a, b))
	got := sign(bytes.Compare(encA, encB))
	if want != got {
		t.Fatalf("CompareKeys(%#v, %#v)=%d but bytes.Compare(enc)=%d\nencA=%x\nencB=%x", a, b, want, got, encA, encB)
	}
}

func assertOrderedKeyInRange(t *testing.T, key any, kr *client.KeyRange) {
	t.Helper()
	ord, err := encodeOrderedKey(key)
	if err != nil {
		t.Fatalf("encodeOrderedKey(%#v): %v", key, err)
	}
	want, err := indexeddb.KeyInRange(key, kr)
	if err != nil {
		t.Fatalf("KeyInRange(%#v): %v", key, err)
	}
	got, err := sqlPredicateMatches(ord, kr)
	if err != nil {
		t.Fatalf("sqlPredicateMatches: %v", err)
	}
	if want != got {
		t.Fatalf("KeyInRange(%#v, kr)=%v but sqlPredicateMatches=%v (ord=%x)", key, want, got, ord)
	}
}

func sign(v int) int {
	switch {
	case v < 0:
		return -1
	case v > 0:
		return 1
	default:
		return 0
	}
}

func randomOrderedKey(rng *rand.Rand) any {
	switch rng.Intn(6) {
	case 0:
		return randomNumberKey(rng)
	case 1:
		return time.Unix(0, rng.Int63()).UTC()
	case 2:
		return randomStringKey(rng)
	case 3:
		return randomBinaryKey(rng)
	default:
		return randomArrayKey(rng, rng.Intn(3)+1)
	}
}

func randomNumberKey(rng *rand.Rand) any {
	switch rng.Intn(8) {
	case 0:
		return rng.Intn(2) == 1
	case 1:
		return int64(rng.Int63n(1_000_000) - 500_000)
	case 2:
		return uint64(rng.Uint64() >> 1)
	case 3:
		return int64(9007199254740991 + rng.Int63n(100))
	case 4:
		return uint64(9007199254740991 + rng.Int63n(100))
	case 5:
		return rng.Float64()*200 - 100
	case 6:
		return float64(0)
	default:
		return math.Copysign(0, -1)
	}
}

func randomStringKey(rng *rand.Rand) string {
	n := rng.Intn(8) + 1
	runes := make([]rune, n)
	for i := range runes {
		switch rng.Intn(3) {
		case 0:
			runes[i] = rune('a' + rng.Intn(26))
		case 1:
			runes[i] = rune(0x4E00 + rng.Intn(100))
		default:
			runes[i] = rune(0x10000 + rng.Intn(1000))
		}
	}
	return string(runes)
}

func randomBinaryKey(rng *rand.Rand) []byte {
	raw := make([]byte, rng.Intn(8)+1)
	_, _ = rng.Read(raw)
	return raw
}

func randomArrayKey(rng *rand.Rand, depth int) []any {
	n := rng.Intn(3) + 1
	parts := make([]any, n)
	for i := range parts {
		if depth > 1 && rng.Intn(4) == 0 {
			parts[i] = randomArrayKey(rng, depth-1)
			continue
		}
		switch rng.Intn(4) {
		case 0:
			parts[i] = randomNumberKey(rng)
		case 1:
			parts[i] = randomStringKey(rng)
		case 2:
			parts[i] = randomBinaryKey(rng)
		default:
			parts[i] = time.Unix(0, rng.Int63()).UTC()
		}
	}
	return parts
}

func randomKeyRange(rng *rand.Rand) *client.KeyRange {
	lower := randomScalarOrderedKey(rng)
	upper := randomScalarOrderedKey(rng)
	if indexeddb.CompareKeys(lower, upper) > 0 {
		lower, upper = upper, lower
	}
	kvLower, err := indexeddb.AnyToKeyValue(lower)
	if err != nil {
		panic(err)
	}
	kvUpper, err := indexeddb.AnyToKeyValue(upper)
	if err != nil {
		panic(err)
	}
	return &client.KeyRange{
		Lower:     kvLower,
		Upper:     kvUpper,
		LowerOpen: rng.Intn(2) == 0,
		UpperOpen: rng.Intn(2) == 0,
	}
}
