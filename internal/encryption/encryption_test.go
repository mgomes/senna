package encryption

import (
	"crypto/rand"
	"testing"
)

func generateTestKey() []byte {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	return key
}

func TestEncryptor_EncryptDecrypt(t *testing.T) {
	t.Parallel()
	key := generateTestKey()
	enc, err := New(key)
	if err != nil {
		t.Fatalf("failed to create encryptor: %v", err)
	}

	original := map[string]any{
		"user_id":     123,
		"email":       "test@example.com",
		"card_number": "4111111111111111",
	}

	encrypted, err := enc.Encrypt(original)
	if err != nil {
		t.Fatalf("encrypt failed: %v", err)
	}

	if _, ok := encrypted["_encrypted"]; !ok {
		t.Error("encrypted data should have _encrypted key")
	}

	if encrypted["user_id"] != nil {
		t.Error("original keys should not exist in encrypted data")
	}

	decrypted, err := enc.Decrypt(encrypted)
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
	t.Parallel()
	key := generateTestKey()
	enc, err := New(key)
	if err != nil {
		t.Fatalf("failed to create encryptor: %v", err)
	}

	plain := map[string]any{
		"key": "value",
	}

	result, err := enc.Decrypt(plain)
	if err != nil {
		t.Fatalf("decrypt non-encrypted should not fail: %v", err)
	}

	if result["key"] != "value" {
		t.Errorf("expected key 'value', got %v", result["key"])
	}
}

func TestEncryptor_DifferentKeys(t *testing.T) {
	t.Parallel()
	key1 := generateTestKey()
	key2 := generateTestKey()

	enc1, _ := New(key1)
	enc2, _ := New(key2)

	original := map[string]any{"secret": "data"}

	encrypted, err := enc1.Encrypt(original)
	if err != nil {
		t.Fatalf("encrypt failed: %v", err)
	}

	_, err = enc2.Decrypt(encrypted)
	if err == nil {
		t.Error("decrypt with wrong key should fail")
	}
}

func TestEncryptor_UniqueNonces(t *testing.T) {
	t.Parallel()
	key := generateTestKey()
	enc, _ := New(key)

	data := map[string]any{"value": "same"}

	encrypted1, _ := enc.Encrypt(data)
	encrypted2, _ := enc.Encrypt(data)

	if encrypted1["_encrypted"] == encrypted2["_encrypted"] {
		t.Error("same plaintext should produce different ciphertext (unique nonces)")
	}
}

func TestEncryptor_EmptyArgs(t *testing.T) {
	t.Parallel()
	key := generateTestKey()
	enc, _ := New(key)

	empty := map[string]any{}

	encrypted, err := enc.Encrypt(empty)
	if err != nil {
		t.Fatalf("encrypt empty failed: %v", err)
	}

	decrypted, err := enc.Decrypt(encrypted)
	if err != nil {
		t.Fatalf("decrypt empty failed: %v", err)
	}

	if len(decrypted) != 0 {
		t.Errorf("expected empty map, got %v", decrypted)
	}
}

func TestEncryptor_ComplexData(t *testing.T) {
	t.Parallel()
	key := generateTestKey()
	enc, _ := New(key)

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

	encrypted, err := enc.Encrypt(complex)
	if err != nil {
		t.Fatalf("encrypt complex failed: %v", err)
	}

	decrypted, err := enc.Decrypt(encrypted)
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
	t.Parallel()
	shortKey := []byte("tooshort")
	_, err := New(shortKey)
	if err == nil {
		t.Error("should fail with invalid key length")
	}
}

func TestEncryptor_TamperedData(t *testing.T) {
	t.Parallel()
	key := generateTestKey()
	enc, _ := New(key)

	original := map[string]any{"secret": "data"}
	encrypted, _ := enc.Encrypt(original)

	ciphertext := encrypted["_encrypted"].(string)
	tampered := ciphertext[:len(ciphertext)-5] + "XXXXX"
	encrypted["_encrypted"] = tampered

	_, err := enc.Decrypt(encrypted)
	if err == nil {
		t.Error("decrypt tampered data should fail")
	}
}
