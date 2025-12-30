package senna_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgomes/senna"
	"github.com/mgomes/senna/client"
	"github.com/mgomes/senna/worker"
	"github.com/redis/go-redis/v9"
)

func getRedisAddrBatch() string {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	return addr
}

func flushKeysBatch(t *testing.T, pattern string) {
	c := redis.NewClient(&redis.Options{Addr: getRedisAddrBatch()})
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	keys, err := c.Keys(ctx, pattern).Result()
	if err != nil {
		t.Fatalf("failed to get keys: %v", err)
	}
	if len(keys) > 0 {
		c.Del(ctx, keys...)
	}
}

func TestBatch_CallbackOptions(t *testing.T) {
	batch := client.NewBatch().
		WithDescription("Test batch").
		Add("job1", map[string]any{"key": "value"}).
		OnCompleteCallback("complete_handler", map[string]any{"notify": "admin@example.com"}).
		OnSuccessCallback("success_handler", map[string]any{"send_report": true}).
		OnDeathCallback("death_handler", map[string]any{"alert": "pagerduty"}).
		WithCallbackQueue("critical")

	if batch.Description != "Test batch" {
		t.Errorf("expected description 'Test batch', got '%s'", batch.Description)
	}
	if batch.OnComplete == nil || batch.OnComplete.JobType != "complete_handler" {
		t.Error("OnComplete not set correctly")
	}
	if batch.OnComplete.Options["notify"] != "admin@example.com" {
		t.Error("OnComplete options not set correctly")
	}
	if batch.OnSuccess == nil || batch.OnSuccess.JobType != "success_handler" {
		t.Error("OnSuccess not set correctly")
	}
	if batch.OnDeath == nil || batch.OnDeath.JobType != "death_handler" {
		t.Error("OnDeath not set correctly")
	}
	if batch.CallbackQueue != "critical" {
		t.Errorf("expected callback queue 'critical', got '%s'", batch.CallbackQueue)
	}
}

func TestBatch_SuccessCallback(t *testing.T) {
	flushKeysBatch(t, "batch-success:*")
	flushKeysBatch(t, "senna:*")

	c, err := client.New(&client.Config{
		Redis:     senna.RedisConfig{Addr: getRedisAddrBatch()},
		Namespace: "batch-success",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = c.Close() }()

	w, err := worker.New(&worker.Config{
		Redis:     senna.RedisConfig{Addr: getRedisAddrBatch()},
		Namespace: "batch-success",
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

	var jobsProcessed atomic.Int32
	var completeCallbackCalled atomic.Bool
	var successCallbackCalled atomic.Bool
	var receivedBatchID string
	var receivedOptions map[string]any
	var mu sync.Mutex

	w.Register("batch_job", func(ctx context.Context, job *senna.Job) error {
		jobsProcessed.Add(1)
		return nil
	})

	w.Register("on_complete", func(ctx context.Context, job *senna.Job) error {
		completeCallbackCalled.Store(true)
		mu.Lock()
		receivedBatchID = job.Args["batch_id"].(string)
		mu.Unlock()
		return nil
	})

	w.Register("on_success", func(ctx context.Context, job *senna.Job) error {
		successCallbackCalled.Store(true)
		mu.Lock()
		receivedOptions = job.Args
		mu.Unlock()
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = w.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	batch := client.NewBatch().
		Add("batch_job", nil).
		Add("batch_job", nil).
		Add("batch_job", nil).
		OnCompleteCallback("on_complete").
		OnSuccessCallback("on_success", map[string]any{"user_id": float64(123)})

	if err := c.EnqueueBatch(ctx, batch); err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	// Wait for all jobs and callbacks to complete
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if jobsProcessed.Load() >= 3 && completeCallbackCalled.Load() && successCallbackCalled.Load() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	time.Sleep(100 * time.Millisecond)

	if jobsProcessed.Load() != 3 {
		t.Errorf("expected 3 jobs processed, got %d", jobsProcessed.Load())
	}
	if !completeCallbackCalled.Load() {
		t.Error("complete callback should have been called")
	}
	if !successCallbackCalled.Load() {
		t.Error("success callback should have been called")
	}

	mu.Lock()
	if receivedBatchID != batch.ID {
		t.Errorf("expected batch ID '%s' in callback, got '%s'", batch.ID, receivedBatchID)
	}
	if receivedOptions["user_id"] != float64(123) {
		t.Errorf("expected user_id 123 in callback options, got %v", receivedOptions["user_id"])
	}
	mu.Unlock()
}

func TestBatch_DeathCallback(t *testing.T) {
	flushKeysBatch(t, "batch-death:*")
	flushKeysBatch(t, "senna:*")

	c, err := client.New(&client.Config{
		Redis:     senna.RedisConfig{Addr: getRedisAddrBatch()},
		Namespace: "batch-death",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = c.Close() }()

	w, err := worker.New(&worker.Config{
		Redis:     senna.RedisConfig{Addr: getRedisAddrBatch()},
		Namespace: "batch-death",
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

	var deathCallbackCalled atomic.Int32
	var completeCallbackCalled atomic.Bool
	var successCallbackCalled atomic.Bool

	w.Register("failing_job", func(ctx context.Context, job *senna.Job) error {
		return errors.New("intentional failure")
	}, worker.WithMaxRetries(0)) // No retries - immediate death

	w.Register("on_complete", func(ctx context.Context, job *senna.Job) error {
		completeCallbackCalled.Store(true)
		return nil
	})

	w.Register("on_success", func(ctx context.Context, job *senna.Job) error {
		successCallbackCalled.Store(true)
		return nil
	})

	w.Register("on_death", func(ctx context.Context, job *senna.Job) error {
		deathCallbackCalled.Add(1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = w.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	batch := client.NewBatch().
		Add("failing_job", nil).
		Add("failing_job", nil). // Two failing jobs
		OnCompleteCallback("on_complete").
		OnSuccessCallback("on_success").
		OnDeathCallback("on_death")

	if err := c.EnqueueBatch(ctx, batch); err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	// Wait for callbacks
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if deathCallbackCalled.Load() > 0 && completeCallbackCalled.Load() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	time.Sleep(100 * time.Millisecond)

	// Death callback should fire only once (first death)
	if deathCallbackCalled.Load() != 1 {
		t.Errorf("death callback should be called exactly once, got %d", deathCallbackCalled.Load())
	}
	if !completeCallbackCalled.Load() {
		t.Error("complete callback should have been called")
	}
	if successCallbackCalled.Load() {
		t.Error("success callback should NOT have been called (batch has deaths)")
	}
}

func TestBatch_Status(t *testing.T) {
	flushKeysBatch(t, "batch-status:*")
	flushKeysBatch(t, "senna:*")

	c, err := client.New(&client.Config{
		Redis:     senna.RedisConfig{Addr: getRedisAddrBatch()},
		Namespace: "batch-status",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = c.Close() }()

	batch := client.NewBatch().
		WithDescription("Status test batch").
		Add("job1", nil).
		Add("job2", nil).
		Add("job3", nil)

	ctx := context.Background()
	if err := c.EnqueueBatch(ctx, batch); err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	// Get batch status
	status := c.BatchStatus(batch.ID)
	if err := status.Refresh(ctx); err != nil {
		t.Fatalf("failed to refresh status: %v", err)
	}

	if status.BID() != batch.ID {
		t.Errorf("expected BID '%s', got '%s'", batch.ID, status.BID())
	}
	if status.Total() != 3 {
		t.Errorf("expected total 3, got %d", status.Total())
	}
	if status.Pending() != 3 {
		t.Errorf("expected pending 3, got %d", status.Pending())
	}
	if status.Successes() != 0 {
		t.Errorf("expected successes 0, got %d", status.Successes())
	}
	if status.Complete() {
		t.Error("batch should not be complete yet")
	}
	if status.Description() != "Status test batch" {
		t.Errorf("expected description 'Status test batch', got '%s'", status.Description())
	}

	// Test Data() method
	data := status.Data()
	if data["bid"] != batch.ID {
		t.Error("Data() should include bid")
	}
	if data["total"] != 3 {
		t.Error("Data() should include total")
	}
}

func TestBatch_InvalidBatchStatus(t *testing.T) {
	flushKeysBatch(t, "batch-invalid:*")

	redisClient := redis.NewClient(&redis.Options{Addr: getRedisAddrBatch()})
	defer func() { _ = redisClient.Close() }()

	status := senna.NewBatchStatus(redisClient, "batch-invalid", "nonexistent")
	err := status.Refresh(context.Background())

	var notFound *senna.BatchNotFoundError
	if !errors.As(err, &notFound) {
		t.Errorf("expected BatchNotFoundError, got %T: %v", err, err)
	}
}

func TestBatch_DynamicJobAdding(t *testing.T) {
	flushKeysBatch(t, "batch-dynamic:*")
	flushKeysBatch(t, "senna:*")

	c, err := client.New(&client.Config{
		Redis:     senna.RedisConfig{Addr: getRedisAddrBatch()},
		Namespace: "batch-dynamic",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = c.Close() }()

	w, err := worker.New(&worker.Config{
		Redis:     senna.RedisConfig{Addr: getRedisAddrBatch()},
		Namespace: "batch-dynamic",
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

	var jobsProcessed atomic.Int32
	var completeCalled atomic.Bool

	// This job adds more jobs to its batch
	w.Register("parent_job", func(ctx context.Context, job *senna.Job) error {
		jobsProcessed.Add(1)

		// Get the batch handle from context
		bh := worker.BatchFromContext(ctx)
		if bh == nil {
			t.Error("batch handle should be available in context")
			return nil
		}

		// Verify BID matches
		if bh.BID() != job.BatchID {
			t.Errorf("BID mismatch: context=%s, job=%s", bh.BID(), job.BatchID)
		}

		// Add child jobs dynamically
		if err := bh.Add(ctx, "child_job", map[string]any{"parent": job.ID}); err != nil {
			return err
		}
		return nil
	})

	w.Register("child_job", func(ctx context.Context, job *senna.Job) error {
		jobsProcessed.Add(1)
		return nil
	})

	w.Register("on_complete", func(ctx context.Context, job *senna.Job) error {
		completeCalled.Store(true)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = w.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	batch := client.NewBatch().
		Add("parent_job", nil). // Will add 1 child job
		OnCompleteCallback("on_complete")

	if err := c.EnqueueBatch(ctx, batch); err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	// Wait for all jobs (parent + child) and callback
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if jobsProcessed.Load() >= 2 && completeCalled.Load() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	time.Sleep(100 * time.Millisecond)

	if jobsProcessed.Load() < 2 {
		t.Errorf("expected at least 2 jobs processed (parent + child), got %d", jobsProcessed.Load())
	}
	if !completeCalled.Load() {
		t.Error("complete callback should have been called")
	}
}

func TestBatch_ValidWithinBatch(t *testing.T) {
	// Test ValidWithinBatch when not in a batch
	valid, err := worker.ValidWithinBatch(context.Background())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !valid {
		t.Error("should be valid when not in a batch")
	}

	// BIDFromContext when not in batch should return empty
	bid := worker.BIDFromContext(context.Background())
	if bid != "" {
		t.Errorf("expected empty BID, got '%s'", bid)
	}
}

func TestBatch_CallbackQueue(t *testing.T) {
	flushKeysBatch(t, "batch-cbqueue:*")
	flushKeysBatch(t, "senna:*")

	c, err := client.New(&client.Config{
		Redis:     senna.RedisConfig{Addr: getRedisAddrBatch()},
		Namespace: "batch-cbqueue",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = c.Close() }()

	w, err := worker.New(&worker.Config{
		Redis:     senna.RedisConfig{Addr: getRedisAddrBatch()},
		Namespace: "batch-cbqueue",
		Settings: senna.WorkerSettings{
			Concurrency:     2,
			Queues:          []senna.QueueConfig{{Name: "default", Priority: 1}, {Name: "critical", Priority: 10}},
			ShutdownTimeout: 5 * time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	var callbackQueue string
	var mu sync.Mutex

	w.Register("batch_job", func(ctx context.Context, job *senna.Job) error {
		return nil
	})

	w.Register("on_complete", func(ctx context.Context, job *senna.Job) error {
		mu.Lock()
		callbackQueue = job.Queue
		mu.Unlock()
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = w.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	batch := client.NewBatch().
		Add("batch_job", nil).
		OnCompleteCallback("on_complete").
		WithCallbackQueue("critical")

	if err := c.EnqueueBatch(ctx, batch); err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		q := callbackQueue
		mu.Unlock()
		if q != "" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if callbackQueue != "critical" {
		t.Errorf("expected callback to run on 'critical' queue, got '%s'", callbackQueue)
	}
	mu.Unlock()
}

func TestBatch_ClientDefaultQueueForCallbacks(t *testing.T) {
	flushKeysBatch(t, "batch-defqueue:*")
	flushKeysBatch(t, "senna:*")

	// Client with custom default queue
	c, err := client.New(&client.Config{
		Redis:     senna.RedisConfig{Addr: getRedisAddrBatch()},
		Namespace: "batch-defqueue",
		Settings: client.Settings{
			DefaultQueue: "custom",
			DefaultRetry: 25,
		},
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = c.Close() }()

	// Worker only listens to "custom" queue (not "default")
	w, err := worker.New(&worker.Config{
		Redis:     senna.RedisConfig{Addr: getRedisAddrBatch()},
		Namespace: "batch-defqueue",
		Settings: senna.WorkerSettings{
			Concurrency:     2,
			Queues:          []senna.QueueConfig{{Name: "custom", Priority: 1}},
			ShutdownTimeout: 5 * time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	var completeCalled atomic.Bool
	var callbackQueue string
	var mu sync.Mutex

	w.Register("batch_job", func(ctx context.Context, job *senna.Job) error {
		return nil
	})

	w.Register("on_complete", func(ctx context.Context, job *senna.Job) error {
		mu.Lock()
		callbackQueue = job.Queue
		mu.Unlock()
		completeCalled.Store(true)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = w.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	// Create batch WITHOUT explicit callback queue - should use client's default
	batch := client.NewBatch().
		Add("batch_job", nil, client.WithQueue("custom")). // Job goes to custom queue
		OnCompleteCallback("on_complete")                  // Callback queue should inherit from client

	if err := c.EnqueueBatch(ctx, batch); err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	// Wait for callback to fire
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if completeCalled.Load() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	time.Sleep(100 * time.Millisecond)

	if !completeCalled.Load() {
		t.Error("complete callback should have been called (should use client's default queue)")
	}

	mu.Lock()
	if callbackQueue != "custom" {
		t.Errorf("expected callback to run on 'custom' queue (client default), got '%s'", callbackQueue)
	}
	mu.Unlock()
}

func TestBatch_EmptyBatchFiresCallbacks(t *testing.T) {
	flushKeysBatch(t, "batch-empty:*")
	flushKeysBatch(t, "senna:*")

	c, err := client.New(&client.Config{
		Redis:     senna.RedisConfig{Addr: getRedisAddrBatch()},
		Namespace: "batch-empty",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = c.Close() }()

	w, err := worker.New(&worker.Config{
		Redis:     senna.RedisConfig{Addr: getRedisAddrBatch()},
		Namespace: "batch-empty",
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

	var completeCalled atomic.Bool
	var successCalled atomic.Bool

	w.Register("on_complete", func(ctx context.Context, job *senna.Job) error {
		completeCalled.Store(true)
		return nil
	})

	w.Register("on_success", func(ctx context.Context, job *senna.Job) error {
		successCalled.Store(true)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = w.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	// Create an empty batch with callbacks
	batch := client.NewBatch().
		WithDescription("Empty batch test").
		OnCompleteCallback("on_complete").
		OnSuccessCallback("on_success")

	if err := c.EnqueueBatch(ctx, batch); err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	// Batch should be immediately complete
	status := c.BatchStatus(batch.ID)
	if err := status.Refresh(ctx); err != nil {
		t.Fatalf("failed to refresh status: %v", err)
	}
	if !status.Complete() {
		t.Error("empty batch should be immediately complete")
	}
	if status.Pending() != 0 {
		t.Errorf("expected 0 pending, got %d", status.Pending())
	}

	// Wait for callbacks to be processed
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if completeCalled.Load() && successCalled.Load() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	time.Sleep(100 * time.Millisecond)

	if !completeCalled.Load() {
		t.Error("complete callback should have been called for empty batch")
	}
	if !successCalled.Load() {
		t.Error("success callback should have been called for empty batch")
	}
}

func TestBatch_InvalidatedBatchCompletes(t *testing.T) {
	flushKeysBatch(t, "batch-invalidate:*")
	flushKeysBatch(t, "senna:*")

	c, err := client.New(&client.Config{
		Redis:     senna.RedisConfig{Addr: getRedisAddrBatch()},
		Namespace: "batch-invalidate",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = c.Close() }()

	w, err := worker.New(&worker.Config{
		Redis:     senna.RedisConfig{Addr: getRedisAddrBatch()},
		Namespace: "batch-invalidate",
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

	var jobsProcessed atomic.Int32
	var completeCalled atomic.Bool
	var successCalled atomic.Bool
	invalidateOnce := sync.Once{}

	w.Register("batch_job", func(ctx context.Context, job *senna.Job) error {
		// First job invalidates the batch
		invalidateOnce.Do(func() {
			batch := worker.BatchFromContext(ctx)
			if batch != nil {
				_ = batch.Invalidate(ctx)
			}
		})
		jobsProcessed.Add(1)
		return nil
	})

	w.Register("on_complete", func(ctx context.Context, job *senna.Job) error {
		completeCalled.Store(true)
		return nil
	})

	w.Register("on_success", func(ctx context.Context, job *senna.Job) error {
		successCalled.Store(true)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = w.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	batch := client.NewBatch().
		Add("batch_job", map[string]any{"id": 1}).
		Add("batch_job", map[string]any{"id": 2}).
		Add("batch_job", map[string]any{"id": 3}).
		OnCompleteCallback("on_complete").
		OnSuccessCallback("on_success")

	if err := c.EnqueueBatch(ctx, batch); err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	// Use Join to wait for batch completion - this should return even though invalidated
	status := c.BatchStatus(batch.ID)
	joinCtx, joinCancel := context.WithTimeout(ctx, 5*time.Second)
	defer joinCancel()

	err = status.Join(joinCtx)
	if err != nil {
		t.Errorf("Join() should return without error for invalidated batch, got: %v", err)
	}

	// Wait a bit for callbacks to fire
	time.Sleep(200 * time.Millisecond)

	cancel()
	time.Sleep(100 * time.Millisecond)

	// All jobs should have processed
	if jobsProcessed.Load() != 3 {
		t.Errorf("expected 3 jobs processed, got %d", jobsProcessed.Load())
	}

	// Complete callback should fire
	if !completeCalled.Load() {
		t.Error("complete callback should have been called for invalidated batch")
	}

	// Success callback should NOT fire (batch was invalidated)
	if successCalled.Load() {
		t.Error("success callback should NOT have been called for invalidated batch")
	}

	// Verify batch status shows complete
	if err := status.Refresh(context.Background()); err != nil {
		t.Fatalf("failed to refresh status: %v", err)
	}
	if !status.Complete() {
		t.Error("batch should be marked as complete")
	}
	if status.Pending() != 0 {
		t.Errorf("expected 0 pending, got %d", status.Pending())
	}
}
