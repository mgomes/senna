package senna_test

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgomes/senna"
	"github.com/mgomes/senna/ratelimit"
	"github.com/redis/go-redis/v9"
)

func getRedisAddr() string {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	return addr
}

func flushKeys(t *testing.T, pattern string) {
	client := redis.NewClient(&redis.Options{Addr: getRedisAddr()})
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	keys, err := client.Keys(ctx, pattern).Result()
	if err != nil {
		t.Fatalf("failed to get keys: %v", err)
	}
	if len(keys) > 0 {
		client.Del(ctx, keys...)
	}
}

func TestIntegration_EnqueueAndProcess(t *testing.T) {
	flushKeys(t, "integration:*")

	client, err := senna.NewClient(&senna.ClientConfig{
		Redis:     senna.RedisConfig{Addr: getRedisAddr()},
		Namespace: "integration",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	worker, err := senna.NewWorker(&senna.WorkerConfig{
		Redis:     senna.RedisConfig{Addr: getRedisAddr()},
		Namespace: "integration",
		Settings: senna.WorkerSettings{
			Concurrency:     2,
			Queues:          []senna.QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout: 5 * time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	var processed atomic.Int32
	var mu sync.Mutex
	results := make(map[int]bool)

	worker.Register("test_job", func(ctx context.Context, job *senna.Job) error {
		id := int(job.Args["id"].(float64))
		mu.Lock()
		results[id] = true
		mu.Unlock()
		processed.Add(1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		_ = worker.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	for i := range 10 {
		_, err := client.Enqueue(context.Background(), "test_job", map[string]any{"id": i + 1})
		if err != nil {
			t.Fatalf("enqueue failed: %v", err)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for processed.Load() < 10 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	time.Sleep(100 * time.Millisecond)

	if processed.Load() != 10 {
		t.Fatalf("expected 10 processed, got %d", processed.Load())
	}

	for i := range 10 {
		if !results[i+1] {
			t.Errorf("job %d was not processed", i+1)
		}
	}
}

func TestIntegration_ScheduledJob(t *testing.T) {
	flushKeys(t, "integration-scheduled:*")

	client, err := senna.NewClient(&senna.ClientConfig{
		Redis:     senna.RedisConfig{Addr: getRedisAddr()},
		Namespace: "integration-scheduled",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	worker, err := senna.NewWorker(&senna.WorkerConfig{
		Redis:     senna.RedisConfig{Addr: getRedisAddr()},
		Namespace: "integration-scheduled",
		Settings: senna.WorkerSettings{
			Concurrency:     1,
			Queues:          []senna.QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout: 5 * time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	var processedAt atomic.Int64

	worker.Register("scheduled_job", func(ctx context.Context, job *senna.Job) error {
		processedAt.Store(time.Now().UnixNano())
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		_ = worker.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	enqueuedAt := time.Now()
	_, err = client.EnqueueIn(context.Background(), 500*time.Millisecond, "scheduled_job", nil)
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	time.Sleep(300 * time.Millisecond)
	if processedAt.Load() != 0 {
		t.Error("job should not be processed yet")
	}

	deadline := time.Now().Add(5 * time.Second)
	for processedAt.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}

	cancel()

	if processedAt.Load() == 0 {
		t.Fatal("job was not processed")
	}

	processedTime := time.Unix(0, processedAt.Load())
	delay := processedTime.Sub(enqueuedAt)
	if delay < 400*time.Millisecond {
		t.Errorf("job processed too early, delay was %v", delay)
	}
}

func TestIntegration_RateLimitedJob(t *testing.T) {
	flushKeys(t, "integration-ratelimit:*")
	flushKeys(t, "senna:ratelimit:*")

	client, err := senna.NewClient(&senna.ClientConfig{
		Redis:     senna.RedisConfig{Addr: getRedisAddr()},
		Namespace: "integration-ratelimit",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	worker, err := senna.NewWorker(&senna.WorkerConfig{
		Redis:     senna.RedisConfig{Addr: getRedisAddr()},
		Namespace: "integration-ratelimit",
		Settings: senna.WorkerSettings{
			Concurrency:     5,
			Queues:          []senna.QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout: 5 * time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	limiter := ratelimit.Bucket(worker.Redis(), ratelimit.BucketConfig{
		Name:        "test-integration-limiter",
		Limit:       5,
		Interval:    time.Second,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	var processed atomic.Int32

	worker.Register("rate_limited_job", func(ctx context.Context, job *senna.Job) error {
		processed.Add(1)
		return nil
	})

	worker.Use(senna.RateLimitMiddleware(limiter))

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		_ = worker.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	for range 20 {
		_, _ = client.Enqueue(context.Background(), "rate_limited_job", nil)
	}

	time.Sleep(500 * time.Millisecond)

	cancel()

	count := processed.Load()
	if count > 10 {
		t.Errorf("expected <= 10 processed (rate limited), got %d", count)
	}
}

func TestIntegration_ConcurrentProcessing(t *testing.T) {
	flushKeys(t, "integration-concurrent:*")

	client, err := senna.NewClient(&senna.ClientConfig{
		Redis:     senna.RedisConfig{Addr: getRedisAddr()},
		Namespace: "integration-concurrent",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	worker, err := senna.NewWorker(&senna.WorkerConfig{
		Redis:     senna.RedisConfig{Addr: getRedisAddr()},
		Namespace: "integration-concurrent",
		Settings: senna.WorkerSettings{
			Concurrency:     10,
			Queues:          []senna.QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout: 5 * time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	var maxConcurrent atomic.Int32
	var currentConcurrent atomic.Int32
	var processed atomic.Int32

	worker.Register("concurrent_job", func(ctx context.Context, job *senna.Job) error {
		current := currentConcurrent.Add(1)
		for {
			max := maxConcurrent.Load()
			if current <= max || maxConcurrent.CompareAndSwap(max, current) {
				break
			}
		}

		time.Sleep(100 * time.Millisecond)

		currentConcurrent.Add(-1)
		processed.Add(1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		_ = worker.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	for range 50 {
		_, _ = client.Enqueue(context.Background(), "concurrent_job", nil)
	}

	deadline := time.Now().Add(10 * time.Second)
	for processed.Load() < 50 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}

	cancel()

	if processed.Load() < 50 {
		t.Fatalf("expected 50 processed, got %d", processed.Load())
	}

	if maxConcurrent.Load() < 5 {
		t.Errorf("expected significant concurrency, max was %d", maxConcurrent.Load())
	}
	if maxConcurrent.Load() > 10 {
		t.Errorf("concurrency exceeded limit, max was %d", maxConcurrent.Load())
	}
}

func TestIntegration_GracefulShutdown(t *testing.T) {
	flushKeys(t, "integration-shutdown:*")

	client, err := senna.NewClient(&senna.ClientConfig{
		Redis:     senna.RedisConfig{Addr: getRedisAddr()},
		Namespace: "integration-shutdown",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	worker, err := senna.NewWorker(&senna.WorkerConfig{
		Redis:     senna.RedisConfig{Addr: getRedisAddr()},
		Namespace: "integration-shutdown",
		Settings: senna.WorkerSettings{
			Concurrency:     2,
			Queues:          []senna.QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout: 5 * time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	var processed atomic.Int32
	started := make(chan struct{}, 1)

	worker.Register("slow_job", func(ctx context.Context, job *senna.Job) error {
		select {
		case started <- struct{}{}:
		default:
		}
		time.Sleep(500 * time.Millisecond)
		processed.Add(1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		_ = worker.Run(ctx)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)

	_, _ = client.Enqueue(context.Background(), "slow_job", nil)

	<-started

	cancel()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not shut down in time")
	}

	if processed.Load() != 1 {
		t.Errorf("expected job to complete during graceful shutdown, got %d", processed.Load())
	}
}
