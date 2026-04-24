package externalcredentials

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"

	"golang.org/x/crypto/argon2"
)

const (
	argonTime    = 3
	argonMemory  = 64 * 1024
	argonThreads = 4
	argonKeyLen  = 32
)

var argonSalt = []byte("gestalt-derivekey-v1")

type aesgcmEncryptor struct {
	gcm cipher.AEAD
}

func newAESGCMEncryptor(key []byte) (*aesgcmEncryptor, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("aes.NewCipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("cipher.NewGCM: %w", err)
	}
	return &aesgcmEncryptor{gcm: gcm}, nil
}

func deriveKey(value string) []byte {
	if value == "" {
		return nil
	}
	if decoded, err := hex.DecodeString(value); err == nil && len(decoded) == 32 {
		return decoded
	}
	return argon2.IDKey([]byte(value), argonSalt, argonTime, argonMemory, argonThreads, argonKeyLen)
}

func newEncryptorFromConfig(value string) (*aesgcmEncryptor, error) {
	key := deriveKey(value)
	if len(key) == 0 {
		return nil, fmt.Errorf("encryption key is required")
	}
	return newAESGCMEncryptor(key)
}

func (e *aesgcmEncryptor) Encrypt(plaintext string) (string, error) {
	return e.encrypt(plaintext, base64.StdEncoding)
}

func (e *aesgcmEncryptor) Decrypt(encoded string) (string, error) {
	return e.decrypt(encoded, base64.StdEncoding)
}

func (e *aesgcmEncryptor) EncryptTokenPair(accessToken, refreshToken string) (string, string, error) {
	accessEnc, err := e.Encrypt(accessToken)
	if err != nil {
		return "", "", err
	}
	refreshEnc, err := e.Encrypt(refreshToken)
	if err != nil {
		return "", "", err
	}
	return accessEnc, refreshEnc, nil
}

func (e *aesgcmEncryptor) DecryptTokenPair(accessEnc, refreshEnc string) (string, string, error) {
	accessToken, err := e.Decrypt(accessEnc)
	if err != nil {
		return "", "", err
	}
	refreshToken, err := e.Decrypt(refreshEnc)
	if err != nil {
		return "", "", err
	}
	return accessToken, refreshToken, nil
}

func (e *aesgcmEncryptor) encrypt(plaintext string, enc *base64.Encoding) (string, error) {
	if plaintext == "" {
		return "", nil
	}
	nonce := make([]byte, e.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generating nonce: %w", err)
	}
	ciphertext := e.gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return enc.EncodeToString(ciphertext), nil
}

func (e *aesgcmEncryptor) decrypt(encoded string, enc *base64.Encoding) (string, error) {
	if encoded == "" {
		return "", nil
	}
	data, err := enc.DecodeString(encoded)
	if err != nil {
		return "", fmt.Errorf("base64 decode: %w", err)
	}
	nonceSize := e.gcm.NonceSize()
	if len(data) < nonceSize {
		return "", fmt.Errorf("ciphertext too short")
	}
	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	plaintext, err := e.gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("decrypt: %w", err)
	}
	return string(plaintext), nil
}
