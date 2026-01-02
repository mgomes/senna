package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgomes/senna"
)

func TestWorker_New_DefaultSettings(t *testing.T) {
	w, err := New(&Config{
		Redis:     senna.RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-worker-default",
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	if w.config.Settings.Concurrency != 10 {
		t.Errorf("expected default Concurrency 10, got %d", w.config.Settings.Concurrency)
	}
	if len(w.config.Settings.Queues) != 1 {
		t.Fatalf("expected 1 default queue, got %d", len(w.config.Settings.Queues))
	}
	if w.config.Settings.Queues[0].Name != "default" {
		t.Errorf("expected default queue 'default', got '%s'", w.config.Settings.Queues[0].Name)
	}
}

func TestWorker_New_WithSettings(t *testing.T) {
	w, err := New(&Config{
		Redis:     senna.RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-worker-settings",
		Settings: senna.WorkerSettings{
			Concurrency:     5,
			Queues:          []senna.QueueConfig{{Name: "high", Priority: 10}, {Name: "low", Priority: 1}},
			ShutdownTimeout: time.Minute,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	if w.config.Settings.Concurrency != 5 {
		t.Errorf("expected Concurrency 5, got %d", w.config.Settings.Concurrency)
	}
	if len(w.config.Settings.Queues) != 2 {
		t.Fatalf("expected 2 queues, got %d", len(w.config.Settings.Queues))
	}
}

func TestWorker_New_EncryptionEnabled(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	w, err := New(&Config{
		Redis:     senna.RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-worker-enc",
		Settings:  senna.DefaultWorkerSettings(),
		Encryption: &senna.EncryptionSettings{
			Enabled: true,
			Key:     key,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	if w.encryptor == nil {
		t.Error("encryptor should be initialized")
	}
	if len(w.middleware) < 2 {
		t.Error("middleware should include encryption and recovery")
	}
}

func TestWorker_New_InvalidEncryptionKey(t *testing.T) {
	_, err := New(&Config{
		Redis:     senna.RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-worker-invalid-enc",
		Settings:  senna.DefaultWorkerSettings(),
		Encryption: &senna.EncryptionSettings{
			Enabled: true,
			Key:     []byte("tooshort"),
		},
	})
	if err == nil {
		t.Error("expected error for invalid encryption key")
	}
}

func TestWorker_Register_WithOptions(t *testing.T) {
	w, err := New(&Config{
		Redis:     senna.RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-worker-register",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	called := false
	w.Register("test_job", func(ctx context.Context, job *senna.Job) error {
		called = true
		return nil
	}, WithMaxRetries(3), WithJobTimeout(time.Second))

	job := senna.NewJob("test_job", nil)
	opts, _ := w.pool.process(context.Background(), job)

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
	w, err := New(&Config{
		Redis:     senna.RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-worker-mw",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	middlewareCalled := false
	w.Use(func(next senna.Handler) senna.Handler {
		return func(ctx context.Context, job *senna.Job) error {
			middlewareCalled = true
			return next(ctx, job)
		}
	})

	w.Register("test_job", func(ctx context.Context, job *senna.Job) error {
		return nil
	})

	job := senna.NewJob("test_job", nil)
	_, _ = w.pool.process(context.Background(), job)

	if !middlewareCalled {
		t.Error("middleware should have been called")
	}
}

func TestWorker_Redis_ReturnsClient(t *testing.T) {
	w, err := New(&Config{
		Redis:     senna.RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-worker-redis",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	client := w.Redis()
	if client == nil {
		t.Fatal("Redis() should return client")
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
				RetryBackoff: senna.DefaultBackoff(),
			}
			tt.option(opts)
			tt.validate(t, opts)
		})
	}
}

func TestWorker_ProcessJob_HandlerError(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-process-error:*")

	w, err := New(&Config{
		Redis:     senna.RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-process-error",
		Settings: senna.WorkerSettings{
			Concurrency:     1,
			Queues:          []senna.QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout: time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	w.Register("failing_job", func(ctx context.Context, job *senna.Job) error {
		return errors.New("job failed")
	}, WithMaxRetries(1))

	job := senna.NewJob("failing_job", nil)
	opts, err := w.pool.process(context.Background(), job)

	if err == nil {
		t.Error("expected error from handler")
	}
	if opts == nil {
		t.Error("expected options to be returned")
	}
}

func TestWorker_ProcessJob_RetryableError(t *testing.T) {
	w, err := New(&Config{
		Redis:     senna.RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-process-retry",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	w.Register("retryable_job", func(ctx context.Context, job *senna.Job) error {
		return &senna.RetryableError{
			Job:     job,
			Cause:   errors.New("temporary failure"),
			RetryIn: 5 * time.Second,
		}
	})

	job := senna.NewJob("retryable_job", nil)
	_, err = w.pool.process(context.Background(), job)

	var retryErr *senna.RetryableError
	if !errors.As(err, &retryErr) {
		t.Fatalf("expected RetryableError, got %T: %v", err, err)
	}
	if retryErr.RetryIn != 5*time.Second {
		t.Errorf("expected RetryIn 5s, got %v", retryErr.RetryIn)
	}
}

func TestWorker_DefaultBackoff(t *testing.T) {
	backoff := senna.DefaultBackoff()

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
	w, err := New(&Config{
		Redis:     senna.RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-multi-handler",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	var job1Called, job2Called atomic.Bool

	w.Register("job_type_1", func(ctx context.Context, job *senna.Job) error {
		job1Called.Store(true)
		return nil
	})

	w.Register("job_type_2", func(ctx context.Context, job *senna.Job) error {
		job2Called.Store(true)
		return nil
	})

	_, _ = w.pool.process(context.Background(), senna.NewJob("job_type_1", nil))
	_, _ = w.pool.process(context.Background(), senna.NewJob("job_type_2", nil))

	if !job1Called.Load() {
		t.Error("job_type_1 handler should have been called")
	}
	if !job2Called.Load() {
		t.Error("job_type_2 handler should have been called")
	}
}

func TestWorker_UnknownJobType(t *testing.T) {
	w, err := New(&Config{
		Redis:     senna.RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-unknown-job",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	w.Register("known_job", func(ctx context.Context, job *senna.Job) error {
		return nil
	})

	job := senna.NewJob("unknown_job", nil)
	_, err = w.pool.process(context.Background(), job)

	var notFoundErr *senna.JobNotFoundError
	if !errors.As(err, &notFoundErr) {
		t.Fatalf("expected JobNotFoundError, got %T: %v", err, err)
	}
}

func TestWorker_MiddlewareOrder(t *testing.T) {
	w, err := New(&Config{
		Redis:     senna.RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-mw-order",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	var order []string

	w.Use(func(next senna.Handler) senna.Handler {
		return func(ctx context.Context, job *senna.Job) error {
			order = append(order, "mw1-before")
			err := next(ctx, job)
			order = append(order, "mw1-after")
			return err
		}
	})

	w.Use(func(next senna.Handler) senna.Handler {
		return func(ctx context.Context, job *senna.Job) error {
			order = append(order, "mw2-before")
			err := next(ctx, job)
			order = append(order, "mw2-after")
			return err
		}
	})

	w.Register("test_job", func(ctx context.Context, job *senna.Job) error {
		order = append(order, "handler")
		return nil
	})

	_, _ = w.pool.process(context.Background(), senna.NewJob("test_job", nil))

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

func TestWorker_BatchFailuresCountOncePerJob(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-batch-failures:*")

	w, err := New(&Config{
		Redis:     senna.RedisConfig{Addr: getTestRedisAddr()},
		Namespace: "test-batch-failures",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	job := senna.NewJob("batch-job", nil)
	job.BatchID = "batch-1"

	state := senna.BatchState{
		ID:        job.BatchID,
		Total:     1,
		Pending:   1,
		Failures:  0,
		Successes: 0,
		CreatedAt: time.Now(),
	}
	data, _ := json.Marshal(state)
	redisClient.Set(context.Background(), w.keys.Batch(job.BatchID), string(data), 0)
	redisClient.SAdd(context.Background(), w.keys.BatchJobs(job.BatchID), job.ID)

	w.updateBatchProgress(context.Background(), job, batchResultFailure)

	stateJSON, _ := redisClient.Get(context.Background(), w.keys.Batch(job.BatchID)).Result()
	var updated senna.BatchState
	_ = json.Unmarshal([]byte(stateJSON), &updated)
	if updated.Failures != 1 || updated.Pending != 1 {
		t.Fatalf("expected failures=1 pending=1 after first failure, got failures=%d pending=%d", updated.Failures, updated.Pending)
	}

	w.updateBatchProgress(context.Background(), job, batchResultFailure)
	stateJSON, _ = redisClient.Get(context.Background(), w.keys.Batch(job.BatchID)).Result()
	_ = json.Unmarshal([]byte(stateJSON), &updated)
	if updated.Failures != 1 || updated.Pending != 1 {
		t.Fatalf("expected failures to remain 1, got failures=%d pending=%d", updated.Failures, updated.Pending)
	}

	w.updateBatchProgress(context.Background(), job, batchResultSuccess)
	stateJSON, _ = redisClient.Get(context.Background(), w.keys.Batch(job.BatchID)).Result()
	_ = json.Unmarshal([]byte(stateJSON), &updated)
	if updated.Failures != 1 || updated.Pending != 0 || updated.Successes != 1 {
		t.Fatalf("after success expected failures=1 pending=0 successes=1, got failures=%d pending=%d successes=%d", updated.Failures, updated.Pending, updated.Successes)
	}
}
