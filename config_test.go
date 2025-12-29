package senna

import (
	"testing"
	"time"
)

func TestRedisConfig_Options_Defaults(t *testing.T) {
	cfg := RedisConfig{
		Addr: "localhost:6379",
	}

	opts := cfg.Options()

	if opts.Addr != "localhost:6379" {
		t.Errorf("expected Addr 'localhost:6379', got '%s'", opts.Addr)
	}
	if opts.Password != "" {
		t.Errorf("expected empty Password, got '%s'", opts.Password)
	}
	if opts.DB != 0 {
		t.Errorf("expected DB 0, got %d", opts.DB)
	}
}

func TestRedisConfig_Options_AllFields(t *testing.T) {
	cfg := RedisConfig{
		Addr:         "redis.example.com:6380",
		Password:     "secret123",
		DB:           5,
		PoolSize:     100,
		MinIdleConns: 10,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}

	opts := cfg.Options()

	if opts.Addr != "redis.example.com:6380" {
		t.Errorf("expected Addr 'redis.example.com:6380', got '%s'", opts.Addr)
	}
	if opts.Password != "secret123" {
		t.Errorf("expected Password 'secret123', got '%s'", opts.Password)
	}
	if opts.DB != 5 {
		t.Errorf("expected DB 5, got %d", opts.DB)
	}
	if opts.PoolSize != 100 {
		t.Errorf("expected PoolSize 100, got %d", opts.PoolSize)
	}
	if opts.MinIdleConns != 10 {
		t.Errorf("expected MinIdleConns 10, got %d", opts.MinIdleConns)
	}
	if opts.DialTimeout != 5*time.Second {
		t.Errorf("expected DialTimeout 5s, got %v", opts.DialTimeout)
	}
	if opts.ReadTimeout != 3*time.Second {
		t.Errorf("expected ReadTimeout 3s, got %v", opts.ReadTimeout)
	}
	if opts.WriteTimeout != 3*time.Second {
		t.Errorf("expected WriteTimeout 3s, got %v", opts.WriteTimeout)
	}
}

func TestRedisConfig_Options_PartialFields(t *testing.T) {
	cfg := RedisConfig{
		Addr:     "localhost:6379",
		PoolSize: 50,
	}

	opts := cfg.Options()

	if opts.PoolSize != 50 {
		t.Errorf("expected PoolSize 50, got %d", opts.PoolSize)
	}
	if opts.MinIdleConns != 0 {
		t.Errorf("expected MinIdleConns 0 (not set), got %d", opts.MinIdleConns)
	}
}

func TestDefaultWorkerSettings(t *testing.T) {
	settings := DefaultWorkerSettings()

	if settings.Concurrency != 10 {
		t.Errorf("expected Concurrency 10, got %d", settings.Concurrency)
	}
	if len(settings.Queues) != 1 {
		t.Fatalf("expected 1 queue, got %d", len(settings.Queues))
	}
	if settings.Queues[0].Name != "default" {
		t.Errorf("expected queue name 'default', got '%s'", settings.Queues[0].Name)
	}
	if settings.Queues[0].Priority != 1 {
		t.Errorf("expected queue priority 1, got %d", settings.Queues[0].Priority)
	}
	if settings.ShutdownTimeout != 30*time.Second {
		t.Errorf("expected ShutdownTimeout 30s, got %v", settings.ShutdownTimeout)
	}
	if settings.PollInterval != 100*time.Millisecond {
		t.Errorf("expected PollInterval 100ms, got %v", settings.PollInterval)
	}
	if settings.HeartbeatRate != 5*time.Second {
		t.Errorf("expected HeartbeatRate 5s, got %v", settings.HeartbeatRate)
	}
}

func TestDefaultClientSettings(t *testing.T) {
	settings := DefaultClientSettings()

	if settings.DefaultQueue != "default" {
		t.Errorf("expected DefaultQueue 'default', got '%s'", settings.DefaultQueue)
	}
	if settings.DefaultRetry != 25 {
		t.Errorf("expected DefaultRetry 25, got %d", settings.DefaultRetry)
	}
}

func TestOption_WithNamespace(t *testing.T) {
	cfg := &Config{}
	opt := WithNamespace("myapp")
	opt(cfg)

	if cfg.Namespace != "myapp" {
		t.Errorf("expected Namespace 'myapp', got '%s'", cfg.Namespace)
	}
}

func TestOption_WithConcurrency(t *testing.T) {
	cfg := &Config{}
	opt := WithConcurrency(20)
	opt(cfg)

	if cfg.Worker.Concurrency != 20 {
		t.Errorf("expected Concurrency 20, got %d", cfg.Worker.Concurrency)
	}
}

func TestOption_WithQueues(t *testing.T) {
	cfg := &Config{}
	queues := []QueueConfig{
		{Name: "critical", Priority: 10},
		{Name: "default", Priority: 5},
		{Name: "low", Priority: 1},
	}
	opt := WithQueues(queues...)
	opt(cfg)

	if len(cfg.Worker.Queues) != 3 {
		t.Fatalf("expected 3 queues, got %d", len(cfg.Worker.Queues))
	}
	if cfg.Worker.Queues[0].Name != "critical" {
		t.Errorf("expected first queue 'critical', got '%s'", cfg.Worker.Queues[0].Name)
	}
	if cfg.Worker.Queues[0].Priority != 10 {
		t.Errorf("expected first queue priority 10, got %d", cfg.Worker.Queues[0].Priority)
	}
}

func TestOption_WithShutdownTimeout(t *testing.T) {
	cfg := &Config{}
	opt := WithShutdownTimeout(time.Minute)
	opt(cfg)

	if cfg.Worker.ShutdownTimeout != time.Minute {
		t.Errorf("expected ShutdownTimeout 1m, got %v", cfg.Worker.ShutdownTimeout)
	}
}

func TestOption_WithEncryptionConfig(t *testing.T) {
	cfg := &Config{}
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	opt := WithEncryptionConfig(key)
	opt(cfg)

	if !cfg.Encryption.Enabled {
		t.Error("expected Encryption.Enabled true")
	}
	if len(cfg.Encryption.Key) != 32 {
		t.Errorf("expected 32-byte key, got %d bytes", len(cfg.Encryption.Key))
	}
}

func TestOption_WithMetrics(t *testing.T) {
	cfg := &Config{}
	opt := WithMetrics("localhost:8125", "senna")
	opt(cfg)

	if !cfg.Metrics.Enabled {
		t.Error("expected Metrics.Enabled true")
	}
	if cfg.Metrics.StatsdAddr != "localhost:8125" {
		t.Errorf("expected StatsdAddr 'localhost:8125', got '%s'", cfg.Metrics.StatsdAddr)
	}
	if cfg.Metrics.Prefix != "senna" {
		t.Errorf("expected Prefix 'senna', got '%s'", cfg.Metrics.Prefix)
	}
}

func TestQueueConfig_Defaults(t *testing.T) {
	q := QueueConfig{Name: "test"}

	if q.Priority != 0 {
		t.Errorf("expected default Priority 0, got %d", q.Priority)
	}
	if q.Paused {
		t.Error("expected default Paused false")
	}
}

func TestConfig_MultipleOptions(t *testing.T) {
	cfg := &Config{}
	options := []Option{
		WithNamespace("myapp"),
		WithConcurrency(50),
		WithShutdownTimeout(2 * time.Minute),
	}

	for _, opt := range options {
		opt(cfg)
	}

	if cfg.Namespace != "myapp" {
		t.Errorf("expected Namespace 'myapp', got '%s'", cfg.Namespace)
	}
	if cfg.Worker.Concurrency != 50 {
		t.Errorf("expected Concurrency 50, got %d", cfg.Worker.Concurrency)
	}
	if cfg.Worker.ShutdownTimeout != 2*time.Minute {
		t.Errorf("expected ShutdownTimeout 2m, got %v", cfg.Worker.ShutdownTimeout)
	}
}
