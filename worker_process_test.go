package senna

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorker_New_DefaultSettings(t *testing.T) {
	worker, err := NewWorker(&WorkerConfig{
		Redis:     RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-worker-default",
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer worker.redis.Close()

	if worker.config.Settings.Concurrency != 10 {
		t.Errorf("expected default Concurrency 10, got %d", worker.config.Settings.Concurrency)
	}
	if len(worker.config.Settings.Queues) != 1 {
		t.Fatalf("expected 1 default queue, got %d", len(worker.config.Settings.Queues))
	}
	if worker.config.Settings.Queues[0].Name != "default" {
		t.Errorf("expected default queue 'default', got '%s'", worker.config.Settings.Queues[0].Name)
	}
}

func TestWorker_New_WithSettings(t *testing.T) {
	worker, err := NewWorker(&WorkerConfig{
		Redis:     RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-worker-settings",
		Settings: WorkerSettings{
			Concurrency:     5,
			Queues:          []QueueConfig{{Name: "high", Priority: 10}, {Name: "low", Priority: 1}},
			ShutdownTimeout: time.Minute,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer worker.redis.Close()

	if worker.config.Settings.Concurrency != 5 {
		t.Errorf("expected Concurrency 5, got %d", worker.config.Settings.Concurrency)
	}
	if len(worker.config.Settings.Queues) != 2 {
		t.Fatalf("expected 2 queues, got %d", len(worker.config.Settings.Queues))
	}
}

func TestWorker_New_EncryptionEnabled(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	worker, err := NewWorker(&WorkerConfig{
		Redis:     RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-worker-enc",
		Settings:  DefaultWorkerSettings(),
		Encryption: &EncryptionSettings{
			Enabled: true,
			Key:     key,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer worker.redis.Close()

	if worker.encryptor == nil {
		t.Error("encryptor should be initialized")
	}
	if len(worker.middleware) < 2 {
		t.Error("middleware should include encryption and recovery")
	}
}

func TestWorker_New_InvalidEncryptionKey(t *testing.T) {
	_, err := NewWorker(&WorkerConfig{
		Redis:     RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-worker-invalid-enc",
		Settings:  DefaultWorkerSettings(),
		Encryption: &EncryptionSettings{
			Enabled: true,
			Key:     []byte("tooshort"),
		},
	})
	if err == nil {
		t.Error("expected error for invalid encryption key")
	}
}

func TestWorker_Register_WithOptions(t *testing.T) {
	worker, err := NewWorker(&WorkerConfig{
		Redis:     RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-worker-register",
		Settings:  DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer worker.redis.Close()

	called := false
	worker.Register("test_job", func(ctx context.Context, job *Job) error {
		called = true
		return nil
	}, WithMaxRetries(3), WithJobTimeout(time.Second))

	job := NewJob("test_job", nil)
	opts, _ := worker.pool.process(context.Background(), job)

	if !called {
		t.Error("handler should have been called")
	}
	if opts == nil {
		t.Fatal("options should be returned")
	}
	if opts.MaxRetries != 3 {
		t.Errorf("expected MaxRetries 3, got %d", opts.MaxRetries)
	}
}

func TestWorker_Use_AddsMiddleware(t *testing.T) {
	worker, err := NewWorker(&WorkerConfig{
		Redis:     RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-worker-mw",
		Settings:  DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer worker.redis.Close()

	middlewareCalled := false
	worker.Use(func(next Handler) Handler {
		return func(ctx context.Context, job *Job) error {
			middlewareCalled = true
			return next(ctx, job)
		}
	})

	worker.Register("test_job", func(ctx context.Context, job *Job) error {
		return nil
	})

	job := NewJob("test_job", nil)
	worker.pool.process(context.Background(), job)

	if !middlewareCalled {
		t.Error("middleware should have been called")
	}
}

func TestWorker_Redis_ReturnsClient(t *testing.T) {
	worker, err := NewWorker(&WorkerConfig{
		Redis:     RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-worker-redis",
		Settings:  DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer worker.redis.Close()

	client := worker.Redis()
	if client == nil {
		t.Error("Redis() should return client")
	}

	err = client.Ping(context.Background()).Err()
	if err != nil {
		t.Errorf("Redis client should be connected: %v", err)
	}
}

func TestWorker_JobOptions(t *testing.T) {
	tests := []struct {
		name     string
		option   JobOption
		validate func(*testing.T, *JobOptions)
	}{
		{
			name:   "WithMaxRetries",
			option: WithMaxRetries(5),
			validate: func(t *testing.T, opts *JobOptions) {
				if opts.MaxRetries != 5 {
					t.Errorf("expected MaxRetries 5, got %d", opts.MaxRetries)
				}
			},
		},
		{
			name:   "WithJobTimeout",
			option: WithJobTimeout(30 * time.Second),
			validate: func(t *testing.T, opts *JobOptions) {
				if opts.Timeout != 30*time.Second {
					t.Errorf("expected Timeout 30s, got %v", opts.Timeout)
				}
			},
		},
		{
			name:   "WithMaxConcurrency",
			option: WithMaxConcurrency(3),
			validate: func(t *testing.T, opts *JobOptions) {
				if opts.MaxConcurrency != 3 {
					t.Errorf("expected MaxConcurrency 3, got %d", opts.MaxConcurrency)
				}
			},
		},
		{
			name:   "WithUniqueJob",
			option: WithUniqueJob("user:123", time.Hour),
			validate: func(t *testing.T, opts *JobOptions) {
				if opts.Unique == nil {
					t.Fatal("expected Unique config")
				}
				if opts.Unique.Key != "user:123" {
					t.Errorf("expected Key 'user:123', got '%s'", opts.Unique.Key)
				}
				if opts.Unique.TTL != time.Hour {
					t.Errorf("expected TTL 1h, got %v", opts.Unique.TTL)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := &JobOptions{
				MaxRetries:   25,
				RetryBackoff: DefaultBackoff(),
			}
			tt.option(opts)
			tt.validate(t, opts)
		})
	}
}

func TestWorker_ProcessJob_HandlerError(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-process-error:*")

	worker, err := NewWorker(&WorkerConfig{
		Redis:     RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-process-error",
		Settings: WorkerSettings{
			Concurrency:     1,
			Queues:          []QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout: time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer worker.redis.Close()

	worker.Register("failing_job", func(ctx context.Context, job *Job) error {
		return errors.New("job failed")
	}, WithMaxRetries(1))

	job := NewJob("failing_job", nil)
	opts, err := worker.pool.process(context.Background(), job)

	if err == nil {
		t.Error("expected error from handler")
	}
	if opts == nil {
		t.Error("expected options to be returned")
	}
}

func TestWorker_ProcessJob_RetryableError(t *testing.T) {
	worker, err := NewWorker(&WorkerConfig{
		Redis:     RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-process-retry",
		Settings:  DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer worker.redis.Close()

	worker.Register("retryable_job", func(ctx context.Context, job *Job) error {
		return &RetryableError{
			Job:     job,
			Cause:   errors.New("temporary failure"),
			RetryIn: 5 * time.Second,
		}
	})

	job := NewJob("retryable_job", nil)
	_, err = worker.pool.process(context.Background(), job)

	var retryErr *RetryableError
	if !errors.As(err, &retryErr) {
		t.Fatalf("expected RetryableError, got %T: %v", err, err)
	}
	if retryErr.RetryIn != 5*time.Second {
		t.Errorf("expected RetryIn 5s, got %v", retryErr.RetryIn)
	}
}

func TestWorker_DefaultBackoff(t *testing.T) {
	backoff := DefaultBackoff()

	d0 := backoff(0)
	d1 := backoff(1)
	d5 := backoff(5)

	if d0 < 15*time.Second {
		t.Errorf("expected d0 >= 15s, got %v", d0)
	}
	if d1 <= d0 {
		t.Errorf("expected d1 > d0, got d0=%v, d1=%v", d0, d1)
	}
	if d5 <= d1 {
		t.Errorf("expected d5 > d1, got d1=%v, d5=%v", d1, d5)
	}
}

func TestWorker_MultipleHandlers(t *testing.T) {
	worker, err := NewWorker(&WorkerConfig{
		Redis:     RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-multi-handler",
		Settings:  DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer worker.redis.Close()

	var job1Called, job2Called atomic.Bool

	worker.Register("job_type_1", func(ctx context.Context, job *Job) error {
		job1Called.Store(true)
		return nil
	})

	worker.Register("job_type_2", func(ctx context.Context, job *Job) error {
		job2Called.Store(true)
		return nil
	})

	worker.pool.process(context.Background(), NewJob("job_type_1", nil))
	worker.pool.process(context.Background(), NewJob("job_type_2", nil))

	if !job1Called.Load() {
		t.Error("job_type_1 handler should have been called")
	}
	if !job2Called.Load() {
		t.Error("job_type_2 handler should have been called")
	}
}

func TestWorker_UnknownJobType(t *testing.T) {
	worker, err := NewWorker(&WorkerConfig{
		Redis:     RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-unknown-job",
		Settings:  DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer worker.redis.Close()

	worker.Register("known_job", func(ctx context.Context, job *Job) error {
		return nil
	})

	job := NewJob("unknown_job", nil)
	_, err = worker.pool.process(context.Background(), job)

	var notFoundErr *JobNotFoundError
	if !errors.As(err, &notFoundErr) {
		t.Fatalf("expected JobNotFoundError, got %T: %v", err, err)
	}
}

func TestWorker_MiddlewareOrder(t *testing.T) {
	worker, err := NewWorker(&WorkerConfig{
		Redis:     RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-mw-order",
		Settings:  DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer worker.redis.Close()

	var order []string

	worker.Use(func(next Handler) Handler {
		return func(ctx context.Context, job *Job) error {
			order = append(order, "mw1-before")
			err := next(ctx, job)
			order = append(order, "mw1-after")
			return err
		}
	})

	worker.Use(func(next Handler) Handler {
		return func(ctx context.Context, job *Job) error {
			order = append(order, "mw2-before")
			err := next(ctx, job)
			order = append(order, "mw2-after")
			return err
		}
	})

	worker.Register("test_job", func(ctx context.Context, job *Job) error {
		order = append(order, "handler")
		return nil
	})

	worker.pool.process(context.Background(), NewJob("test_job", nil))

	expected := []string{"mw1-before", "mw2-before", "handler", "mw2-after", "mw1-after"}
	start := len(order) - len(expected)
	if start < 0 {
		t.Fatalf("expected at least %d entries in order, got %d", len(expected), len(order))
	}
	for i, v := range expected {
		if order[start+i] != v {
			t.Errorf("expected order[%d]='%s', got '%s'", i, v, order[start+i])
		}
	}
}
