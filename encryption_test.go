package senna

import (
	"context"
	"crypto/rand"
	"testing"
)

func generateTestKey() []byte {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	return key
}

func TestEncryptor_EncryptDecrypt(t *testing.T) {
	key := generateTestKey()
	enc, err := newEncryptor(key)
	if err != nil {
		t.Fatalf("failed to create encryptor: %v", err)
	}

	original := map[string]any{
		"user_id":     123,
		"email":       "test@example.com",
		"card_number": "4111111111111111",
	}

	encrypted, err := enc.encrypt(original)
	if err != nil {
		t.Fatalf("encrypt failed: %v", err)
	}

	if _, ok := encrypted["_encrypted"]; !ok {
		t.Error("encrypted data should have _encrypted key")
	}

	if encrypted["user_id"] != nil {
		t.Error("original keys should not exist in encrypted data")
	}

	decrypted, err := enc.decrypt(encrypted)
	if err != nil {
		t.Fatalf("decrypt failed: %v", err)
	}

	if decrypted["user_id"].(float64) != 123 {
		t.Errorf("expected user_id 123, got %v", decrypted["user_id"])
	}
	if decrypted["email"] != "test@example.com" {
		t.Errorf("expected email 'test@example.com', got %v", decrypted["email"])
	}
	if decrypted["card_number"] != "4111111111111111" {
		t.Errorf("expected card_number, got %v", decrypted["card_number"])
	}
}

func TestEncryptor_DecryptNonEncrypted(t *testing.T) {
	key := generateTestKey()
	enc, err := newEncryptor(key)
	if err != nil {
		t.Fatalf("failed to create encryptor: %v", err)
	}

	plain := map[string]any{
		"key": "value",
	}

	result, err := enc.decrypt(plain)
	if err != nil {
		t.Fatalf("decrypt non-encrypted should not fail: %v", err)
	}

	if result["key"] != "value" {
		t.Errorf("expected key 'value', got %v", result["key"])
	}
}

func TestEncryptor_DifferentKeys(t *testing.T) {
	key1 := generateTestKey()
	key2 := generateTestKey()

	enc1, _ := newEncryptor(key1)
	enc2, _ := newEncryptor(key2)

	original := map[string]any{"secret": "data"}

	encrypted, err := enc1.encrypt(original)
	if err != nil {
		t.Fatalf("encrypt failed: %v", err)
	}

	_, err = enc2.decrypt(encrypted)
	if err == nil {
		t.Error("decrypt with wrong key should fail")
	}
}

func TestEncryptor_UniqueNonces(t *testing.T) {
	key := generateTestKey()
	enc, _ := newEncryptor(key)

	data := map[string]any{"value": "same"}

	encrypted1, _ := enc.encrypt(data)
	encrypted2, _ := enc.encrypt(data)

	if encrypted1["_encrypted"] == encrypted2["_encrypted"] {
		t.Error("same plaintext should produce different ciphertext (unique nonces)")
	}
}

func TestEncryptor_EmptyArgs(t *testing.T) {
	key := generateTestKey()
	enc, _ := newEncryptor(key)

	empty := map[string]any{}

	encrypted, err := enc.encrypt(empty)
	if err != nil {
		t.Fatalf("encrypt empty failed: %v", err)
	}

	decrypted, err := enc.decrypt(encrypted)
	if err != nil {
		t.Fatalf("decrypt empty failed: %v", err)
	}

	if len(decrypted) != 0 {
		t.Errorf("expected empty map, got %v", decrypted)
	}
}

func TestEncryptor_ComplexData(t *testing.T) {
	key := generateTestKey()
	enc, _ := newEncryptor(key)

	complex := map[string]any{
		"string": "value",
		"number": 42,
		"float":  3.14159,
		"bool":   true,
		"null":   nil,
		"array":  []any{1, "two", 3.0},
		"nested": map[string]any{
			"deep": map[string]any{
				"value": "found",
			},
		},
	}

	encrypted, err := enc.encrypt(complex)
	if err != nil {
		t.Fatalf("encrypt complex failed: %v", err)
	}

	decrypted, err := enc.decrypt(encrypted)
	if err != nil {
		t.Fatalf("decrypt complex failed: %v", err)
	}

	if decrypted["string"] != "value" {
		t.Errorf("expected string 'value', got %v", decrypted["string"])
	}
	if decrypted["number"].(float64) != 42 {
		t.Errorf("expected number 42, got %v", decrypted["number"])
	}
	if decrypted["bool"] != true {
		t.Errorf("expected bool true, got %v", decrypted["bool"])
	}

	nested := decrypted["nested"].(map[string]any)
	deep := nested["deep"].(map[string]any)
	if deep["value"] != "found" {
		t.Errorf("expected deep value 'found', got %v", deep["value"])
	}
}

func TestEncryptor_InvalidKey(t *testing.T) {
	shortKey := []byte("tooshort")
	_, err := newEncryptor(shortKey)
	if err == nil {
		t.Error("should fail with invalid key length")
	}
}

func TestEncryptor_TamperedData(t *testing.T) {
	key := generateTestKey()
	enc, _ := newEncryptor(key)

	original := map[string]any{"secret": "data"}
	encrypted, _ := enc.encrypt(original)

	ciphertext := encrypted["_encrypted"].(string)
	tampered := ciphertext[:len(ciphertext)-5] + "XXXXX"
	encrypted["_encrypted"] = tampered

	_, err := enc.decrypt(encrypted)
	if err == nil {
		t.Error("decrypt tampered data should fail")
	}
}

func TestEncryptionMiddleware(t *testing.T) {
	key := generateTestKey()

	middleware, err := EncryptionMiddleware(key)
	if err != nil {
		t.Fatalf("failed to create middleware: %v", err)
	}

	enc, _ := newEncryptor(key)
	originalArgs := map[string]any{"secret": "value"}
	encryptedArgs, _ := enc.encrypt(originalArgs)

	job := NewJob("test", encryptedArgs)
	job.Encrypted = true

	var decryptedJob *Job
	handler := middleware(func(ctx context.Context, j *Job) error {
		decryptedJob = j
		return nil
	})

	err = handler(context.Background(), job)
	if err != nil {
		t.Fatalf("handler failed: %v", err)
	}

	if decryptedJob.Encrypted {
		t.Error("job should be marked as decrypted")
	}
	if decryptedJob.Args["secret"] != "value" {
		t.Errorf("expected secret 'value', got %v", decryptedJob.Args["secret"])
	}
}

func TestEncryptionMiddleware_NonEncrypted(t *testing.T) {
	key := generateTestKey()
	middleware, _ := EncryptionMiddleware(key)

	job := NewJob("test", map[string]any{"plain": "data"})
	job.Encrypted = false

	var passedJob *Job
	handler := middleware(func(ctx context.Context, j *Job) error {
		passedJob = j
		return nil
	})

	err := handler(context.Background(), job)
	if err != nil {
		t.Fatalf("handler failed: %v", err)
	}

	if passedJob.Args["plain"] != "data" {
		t.Errorf("expected plain 'data', got %v", passedJob.Args["plain"])
	}
}
