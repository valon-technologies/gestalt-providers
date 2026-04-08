package sealcodec

import (
	"bytes"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	t.Parallel()

	original := []byte{0x00, 0x01, 0x02, 0xfe, 0xff}
	encoded := Encode(original)
	if encoded == "" {
		t.Fatal("Encode returned empty string")
	}

	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode returned error: %v", err)
	}
	if !bytes.Equal(decoded, original) {
		t.Fatalf("Decode(Encode(...)) = %v, want %v", decoded, original)
	}
}

func TestDecodeLegacyRawString(t *testing.T) {
	t.Parallel()

	decoded, err := Decode("legacy-token")
	if err != nil {
		t.Fatalf("Decode returned error: %v", err)
	}
	if !bytes.Equal(decoded, []byte("legacy-token")) {
		t.Fatalf("Decode legacy = %q, want %q", decoded, "legacy-token")
	}
}
