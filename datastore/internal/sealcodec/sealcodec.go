package sealcodec

import (
	"encoding/base64"
	"fmt"
	"strings"
)

const prefix = "b64:"

// Encode produces a stable UTF-8 representation for sealed bytes.
func Encode(sealed []byte) string {
	if len(sealed) == 0 {
		return ""
	}
	return prefix + base64.StdEncoding.EncodeToString(sealed)
}

// Decode accepts the current prefixed encoding and legacy raw-string storage.
func Decode(encoded string) ([]byte, error) {
	if encoded == "" {
		return nil, nil
	}
	if !strings.HasPrefix(encoded, prefix) {
		return []byte(encoded), nil
	}
	sealed, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(encoded, prefix))
	if err != nil {
		return nil, fmt.Errorf("decode sealed bytes: %w", err)
	}
	return sealed, nil
}
