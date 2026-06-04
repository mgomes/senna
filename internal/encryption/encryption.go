package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// ErrCiphertextTooShort indicates the encrypted payload is smaller than the
// AEAD nonce, so it cannot be a valid ciphertext.
var ErrCiphertextTooShort = errors.New("ciphertext too short")

type Encryptor struct {
	gcm cipher.AEAD
}

func New(key []byte) (*Encryptor, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	return &Encryptor{gcm: gcm}, nil
}

func (e *Encryptor) Encrypt(args map[string]any) (map[string]any, error) {
	plaintext, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("marshal args: %w", err)
	}

	nonce := make([]byte, e.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("read nonce: %w", err)
	}

	ciphertext := e.gcm.Seal(nonce, nonce, plaintext, nil)
	encoded := base64.StdEncoding.EncodeToString(ciphertext)

	return map[string]any{
		"_encrypted": encoded,
	}, nil
}

func (e *Encryptor) Decrypt(args map[string]any) (map[string]any, error) {
	encoded, ok := args["_encrypted"].(string)
	if !ok {
		return args, nil
	}

	ciphertext, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode ciphertext: %w", err)
	}

	nonceSize := e.gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, ErrCiphertextTooShort
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := e.gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt args: %w", err)
	}

	var result map[string]any
	if err := json.Unmarshal(plaintext, &result); err != nil {
		return nil, fmt.Errorf("unmarshal args: %w", err)
	}

	return result, nil
}
