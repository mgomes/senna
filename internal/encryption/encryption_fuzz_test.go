package encryption

import (
	"encoding/base64"
	"encoding/json"
	"testing"
)

func FuzzEncryptorEncryptDecrypt(f *testing.F) {
	f.Add("hello", int64(42), true)
	f.Add("", int64(0), false)

	key := []byte("0123456789abcdef0123456789abcdef")

	f.Fuzz(func(t *testing.T, text string, number int64, flag bool) {
		if len(text) > 1024 {
			t.Skip()
		}
		number %= 1_000_000_000

		enc, err := New(key)
		if err != nil {
			t.Fatalf("New(valid key) error = %v", err)
		}
		args := map[string]any{
			"text":   text,
			"number": number,
			"flag":   flag,
			"nested": map[string]any{"text": text},
		}

		encrypted, err := enc.Encrypt(args)
		if err != nil {
			t.Fatalf("Encrypt(%v) error = %v", args, err)
		}
		if _, ok := encrypted["_encrypted"].(string); !ok {
			t.Fatalf("Encrypt(%v)[_encrypted] = %T, want string", args, encrypted["_encrypted"])
		}

		decrypted, err := enc.Decrypt(encrypted)
		if err != nil {
			t.Fatalf("Decrypt(Encrypt(%v)) error = %v", args, err)
		}
		if got, want := normalizedJSON(t, decrypted), normalizedJSON(t, args); got != want {
			t.Errorf("Decrypt(Encrypt(%v)) = %s, want %s", args, got, want)
		}
	})
}

func FuzzEncryptorRejectsTampering(f *testing.F) {
	f.Add([]byte("secret"), 0)
	f.Add([]byte{}, 12)

	key := []byte("0123456789abcdef0123456789abcdef")

	f.Fuzz(func(t *testing.T, payload []byte, rawIndex int) {
		if len(payload) > 1024 {
			t.Skip()
		}

		enc, err := New(key)
		if err != nil {
			t.Fatalf("New(valid key) error = %v", err)
		}
		encrypted, err := enc.Encrypt(map[string]any{"payload": string(payload)})
		if err != nil {
			t.Fatalf("Encrypt(payload length %d) error = %v", len(payload), err)
		}

		ciphertext, err := base64.StdEncoding.DecodeString(encrypted["_encrypted"].(string))
		if err != nil {
			t.Fatalf("DecodeString(Encrypt(payload length %d)) error = %v", len(payload), err)
		}
		index := rawIndex % len(ciphertext)
		if index < 0 {
			index = -index
		}
		ciphertext[index] ^= 0x80
		encrypted["_encrypted"] = base64.StdEncoding.EncodeToString(ciphertext)

		if _, err := enc.Decrypt(encrypted); err == nil {
			t.Fatalf("Decrypt(tampered Encrypt(payload length %d)) error = nil, want error", len(payload))
		}
	})
}

func normalizedJSON(t *testing.T, value map[string]any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal(%v) error = %v", value, err)
	}
	var normalized any
	if err := json.Unmarshal(data, &normalized); err != nil {
		t.Fatalf("json.Unmarshal(json.Marshal(%v)) error = %v", value, err)
	}
	data, err = json.Marshal(normalized)
	if err != nil {
		t.Fatalf("json.Marshal(normalized %v) error = %v", normalized, err)
	}
	return string(data)
}
