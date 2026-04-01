package testredis

import "testing"

func TestConfigParsesRedisURL(t *testing.T) {
	t.Setenv("REDIS_ADDR", "")
	t.Setenv("REDIS_URL", "redis://:secret@example.com:6380/3")

	opts := Options()
	if opts.Addr != "example.com:6380" {
		t.Fatalf("expected addr example.com:6380, got %q", opts.Addr)
	}
	if opts.Password != "secret" {
		t.Fatalf("expected password secret, got %q", opts.Password)
	}
	if opts.DB != 3 {
		t.Fatalf("expected db 3, got %d", opts.DB)
	}
	if addr := Addr(); addr != "example.com:6380" {
		t.Fatalf("expected parsed addr example.com:6380, got %q", addr)
	}
}

func TestOptionsPrefersRedisAddr(t *testing.T) {
	t.Setenv("REDIS_ADDR", "localhost:6381")
	t.Setenv("REDIS_URL", "redis://:secret@example.com:6380/3")

	opts := Options()
	if opts.Addr != "localhost:6381" {
		t.Fatalf("expected addr localhost:6381, got %q", opts.Addr)
	}
	if opts.Password != "" {
		t.Fatalf("expected empty password, got %q", opts.Password)
	}
	if opts.DB != 0 {
		t.Fatalf("expected db 0, got %d", opts.DB)
	}
}
