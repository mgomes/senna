package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
)

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
		return nil, err
	}

	nonce := make([]byte, e.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
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
		return nil, err
	}

	nonceSize := e.gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := e.gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	var result map[string]any
	if err := json.Unmarshal(plaintext, &result); err != nil {
		return nil, err
	}

	return result, nil
}
