package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgomes/senna"
	"github.com/redis/go-redis/v9"
)

func TestWorker_New_DefaultSettings(t *testing.T) {
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
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
		Redis:     getTestRedisConfig(),
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
		Redis:     getTestRedisConfig(),
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
		Redis:     getTestRedisConfig(),
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

func TestWorker_StopIsIdempotent(t *testing.T) {
	w := newLifecycleTestWorker(t, "test-worker-stop-idempotent")

	errCh := runWorker(t, w)
	waitForWorkerRunning(t, w)

	w.Stop()
	w.Stop()

	if err := waitForWorkerExit(t, errCh); err != nil {
		t.Fatalf("Worker.Run() error = %v, want nil", err)
	}
}

func TestWorker_RunRestartsAfterCleanShutdown(t *testing.T) {
	w := newLifecycleTestWorker(t, "test-worker-run-restart")

	errCh := runWorker(t, w)
	waitForWorkerRunning(t, w)
	w.Stop()
	if err := waitForWorkerExit(t, errCh); err != nil {
		t.Fatalf("first Worker.Run() error = %v, want nil", err)
	}

	errCh = runWorker(t, w)
	waitForWorkerRunning(t, w)
	w.Stop()
	if err := waitForWorkerExit(t, errCh); err != nil {
		t.Fatalf("second Worker.Run() error = %v, want nil", err)
	}
}

func TestWorker_RunRestartsAfterTimedOutShutdownCompletes(t *testing.T) {
	w := newLifecycleTestWorker(t, "test-worker-timeout-restart")
	w.config.Settings.ShutdownTimeout = 10 * time.Millisecond

	started := make(chan struct{})
	finished := make(chan struct{})
	w.Register("slow_job", func(ctx context.Context, job *senna.Job) error {
		close(started)
		time.Sleep(50 * time.Millisecond)
		close(finished)
		return nil
	})

	job := senna.NewJob("slow_job", nil)
	data, err := job.Marshal()
	if err != nil {
		t.Fatalf("Job.Marshal() error = %v", err)
	}
	if err := w.redis.LPush(context.Background(), w.keys.Queue("default"), string(data)).Err(); err != nil {
		t.Fatalf("LPush(slow_job) error = %v", err)
	}

	errCh := runWorker(t, w)
	waitForWorkerRunning(t, w)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("slow_job handler did not start")
	}

	w.Stop()
	if err := waitForWorkerExit(t, errCh); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Worker.Run() error = %v, want %v", err, context.DeadlineExceeded)
	}

	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("slow_job handler did not finish")
	}
	waitForWorkerStopped(t, w)

	errCh = runWorker(t, w)
	waitForWorkerRunning(t, w)
	w.Stop()
	if err := waitForWorkerExit(t, errCh); err != nil {
		t.Fatalf("second Worker.Run() error = %v, want nil", err)
	}
}

func newLifecycleTestWorker(t *testing.T, namespace string) *Worker {
	t.Helper()

	settings := senna.DefaultWorkerSettings()
	settings.Concurrency = 1
	settings.PollInterval = 10 * time.Millisecond
	settings.ShutdownTimeout = time.Second
	settings.HeartbeatRate = time.Hour
	settings.ScheduledPollInterval = time.Hour

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: namespace,
		Settings:  settings,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	flushTestKeys(t, w.redis, namespace+":*")
	t.Cleanup(func() {
		flushTestKeys(t, w.redis, namespace+":*")
		_ = w.redis.Close()
	})

	return w
}

func runWorker(t *testing.T, w *Worker) <-chan error {
	t.Helper()

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Run(context.Background())
	}()
	return errCh
}

func waitForWorkerRunning(t *testing.T, w *Worker) {
	t.Helper()

	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		w.mu.RLock()
		running := w.running
		w.mu.RUnlock()
		if running {
			return
		}

		select {
		case <-deadline:
			t.Fatal("Worker.Run() did not enter running state")
		case <-ticker.C:
		}
	}
}

func waitForWorkerExit(t *testing.T, errCh <-chan error) error {
	t.Helper()

	select {
	case err := <-errCh:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("Worker.Run() did not stop")
		return nil
	}
}

func waitForWorkerStopped(t *testing.T, w *Worker) {
	t.Helper()

	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		w.mu.RLock()
		running := w.running
		stopping := w.stopping
		w.mu.RUnlock()
		if !running && !stopping {
			return
		}

		select {
		case <-deadline:
			t.Fatal("Worker.Run() did not clear lifecycle state")
		case <-ticker.C:
		}
	}
}

func TestWorker_Register_WithOptions(t *testing.T) {
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
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
	opts, _ := w.handlers.process(context.Background(), job)

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
		Redis:     getTestRedisConfig(),
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
	_, _ = w.handlers.process(context.Background(), job)

	if !middlewareCalled {
		t.Error("middleware should have been called")
	}
}

func TestWorker_Redis_ReturnsClient(t *testing.T) {
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
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
		Redis:     getTestRedisConfig(),
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
	opts, err := w.handlers.process(context.Background(), job)

	if err == nil {
		t.Error("expected error from handler")
	}
	if opts == nil {
		t.Error("expected options to be returned")
	}
}

func TestWorker_ProcessJob_RetryableError(t *testing.T) {
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
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
	_, err = w.handlers.process(context.Background(), job)

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
		Redis:     getTestRedisConfig(),
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

	_, _ = w.handlers.process(context.Background(), senna.NewJob("job_type_1", nil))
	_, _ = w.handlers.process(context.Background(), senna.NewJob("job_type_2", nil))

	if !job1Called.Load() {
		t.Error("job_type_1 handler should have been called")
	}
	if !job2Called.Load() {
		t.Error("job_type_2 handler should have been called")
	}
}

func TestWorker_UnknownJobType(t *testing.T) {
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
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
	_, err = w.handlers.process(context.Background(), job)

	var notFoundErr *senna.JobNotFoundError
	if !errors.As(err, &notFoundErr) {
		t.Fatalf("expected JobNotFoundError, got %T: %v", err, err)
	}
}

func TestWorker_MiddlewareOrder(t *testing.T) {
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
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

	_, _ = w.handlers.process(context.Background(), senna.NewJob("test_job", nil))

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
		Redis:     getTestRedisConfig(),
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

func TestWorker_Periodic_NotEnabled(t *testing.T) {
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-periodic-disabled",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	err = w.Periodic("* * * * *", "test_job")
	if err == nil {
		t.Error("expected error when periodic is not enabled")
	}

	jobs := w.PeriodicJobs()
	if jobs != nil {
		t.Error("expected nil jobs when periodic is not enabled")
	}
}

func TestWorker_Periodic_Enabled(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-periodic-enabled:*")

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-periodic-enabled",
		Settings: senna.WorkerSettings{
			Concurrency:     1,
			Queues:          []senna.QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout: time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
			PeriodicEnabled: true,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	err = w.Periodic("0 * * * *", "hourly_job")
	if err != nil {
		t.Fatalf("failed to register periodic job: %v", err)
	}

	jobs := w.PeriodicJobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 periodic job, got %d", len(jobs))
	}
	if jobs[0].JobType != "hourly_job" {
		t.Errorf("expected job type 'hourly_job', got '%s'", jobs[0].JobType)
	}
}

func TestWorker_Periodic_InvalidCron(t *testing.T) {
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-periodic-invalid",
		Settings: senna.WorkerSettings{
			Concurrency:     1,
			Queues:          []senna.QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout: time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
			PeriodicEnabled: true,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	err = w.Periodic("invalid", "test_job")
	if err == nil {
		t.Error("expected error for invalid cron expression")
	}
}

func TestWorker_Scheduler_EnqueuesDueJobs(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-scheduler-due:*")

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-scheduler-due",
		Settings: senna.WorkerSettings{
			Concurrency:           1,
			Queues:                []senna.QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout:       time.Second,
			PollInterval:          50 * time.Millisecond,
			ScheduledPollInterval: 100 * time.Millisecond,
			HeartbeatRate:         time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	ctx := context.Background()

	// Add a job scheduled in the past (should be enqueued immediately)
	pastTime := time.Now().Add(-time.Minute)
	job := senna.NewJob("past_job", nil)
	jobData, _ := job.Marshal()
	redisClient.ZAdd(ctx, w.keys.Scheduled(), redis.Z{
		Score:  float64(pastTime.Unix()),
		Member: string(jobData),
	})

	// Call enqueueScheduled directly
	w.enqueueScheduled(ctx)

	// Job should now be in the queue
	queueLen, _ := redisClient.LLen(ctx, w.keys.Queue("default")).Result()
	if queueLen != 1 {
		t.Errorf("expected 1 job in queue, got %d", queueLen)
	}

	// Scheduled set should be empty
	scheduledLen, _ := redisClient.ZCard(ctx, w.keys.Scheduled()).Result()
	if scheduledLen != 0 {
		t.Errorf("expected 0 jobs in scheduled, got %d", scheduledLen)
	}
}

func TestWorker_Scheduler_KeepsFutureJobs(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-scheduler-future:*")

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-scheduler-future",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	ctx := context.Background()

	// Add a job scheduled in the future
	futureTime := time.Now().Add(time.Hour)
	job := senna.NewJob("future_job", nil)
	jobData, _ := job.Marshal()
	redisClient.ZAdd(ctx, w.keys.Scheduled(), redis.Z{
		Score:  float64(futureTime.Unix()),
		Member: string(jobData),
	})

	// Call enqueueScheduled
	w.enqueueScheduled(ctx)

	// Job should still be in scheduled set
	scheduledLen, _ := redisClient.ZCard(ctx, w.keys.Scheduled()).Result()
	if scheduledLen != 1 {
		t.Errorf("expected 1 job in scheduled, got %d", scheduledLen)
	}

	// Queue should be empty
	queueLen, _ := redisClient.LLen(ctx, w.keys.Queue("default")).Result()
	if queueLen != 0 {
		t.Errorf("expected 0 jobs in queue, got %d", queueLen)
	}
}

func TestWorker_Scheduler_ProcessesMixedJobs(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-scheduler-mixed:*")

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-scheduler-mixed",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	ctx := context.Background()

	// Add jobs: 2 due, 2 future
	now := time.Now()

	jobs := []struct {
		offset time.Duration
		name   string
	}{
		{-2 * time.Minute, "past1"},
		{-1 * time.Minute, "past2"},
		{1 * time.Hour, "future1"},
		{2 * time.Hour, "future2"},
	}

	for _, j := range jobs {
		job := senna.NewJob(j.name, nil)
		jobData, _ := job.Marshal()
		redisClient.ZAdd(ctx, w.keys.Scheduled(), redis.Z{
			Score:  float64(now.Add(j.offset).Unix()),
			Member: string(jobData),
		})
	}

	// Call enqueueScheduled
	w.enqueueScheduled(ctx)

	// 2 due jobs should be in queue
	queueLen, _ := redisClient.LLen(ctx, w.keys.Queue("default")).Result()
	if queueLen != 2 {
		t.Errorf("expected 2 jobs in queue, got %d", queueLen)
	}

	// 2 future jobs should remain in scheduled
	scheduledLen, _ := redisClient.ZCard(ctx, w.keys.Scheduled()).Result()
	if scheduledLen != 2 {
		t.Errorf("expected 2 jobs in scheduled, got %d", scheduledLen)
	}
}

func TestWorker_Scheduler_EnqueuesRetries(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-scheduler-retry:*")

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-scheduler-retry",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	ctx := context.Background()

	// Add a job to the retry set with past timestamp
	pastTime := time.Now().Add(-time.Minute)
	job := senna.NewJob("retry_job", nil)
	job.RetryCount = 1
	jobData, _ := job.Marshal()
	redisClient.ZAdd(ctx, w.keys.Retry(), redis.Z{
		Score:  float64(pastTime.Unix()),
		Member: string(jobData),
	})

	// Call enqueueRetries
	w.enqueueRetries(ctx)

	// Job should now be in the queue
	queueLen, _ := redisClient.LLen(ctx, w.keys.Queue("default")).Result()
	if queueLen != 1 {
		t.Errorf("expected 1 job in queue, got %d", queueLen)
	}

	// Retry set should be empty
	retryLen, _ := redisClient.ZCard(ctx, w.keys.Retry()).Result()
	if retryLen != 0 {
		t.Errorf("expected 0 jobs in retry, got %d", retryLen)
	}
}

func TestWorker_Scheduler_RoutesToCorrectQueue(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-scheduler-queue:*")

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-scheduler-queue",
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	ctx := context.Background()
	pastTime := time.Now().Add(-time.Minute)

	// Add jobs to different queues
	job1 := senna.NewJob("job1", nil)
	job1.Queue = "critical"
	data1, _ := job1.Marshal()
	redisClient.ZAdd(ctx, w.keys.Scheduled(), redis.Z{
		Score:  float64(pastTime.Unix()),
		Member: string(data1),
	})

	job2 := senna.NewJob("job2", nil)
	job2.Queue = "low"
	data2, _ := job2.Marshal()
	redisClient.ZAdd(ctx, w.keys.Scheduled(), redis.Z{
		Score:  float64(pastTime.Unix()),
		Member: string(data2),
	})

	// Call enqueueScheduled
	w.enqueueScheduled(ctx)

	// Check jobs are in correct queues
	criticalLen, _ := redisClient.LLen(ctx, w.keys.Queue("critical")).Result()
	lowLen, _ := redisClient.LLen(ctx, w.keys.Queue("low")).Result()

	if criticalLen != 1 {
		t.Errorf("expected 1 job in critical queue, got %d", criticalLen)
	}
	if lowLen != 1 {
		t.Errorf("expected 1 job in low queue, got %d", lowLen)
	}
}

func TestWorker_ScheduledPollInterval_Configurable(t *testing.T) {
	customInterval := 50 * time.Millisecond
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-scheduler-interval",
		Settings: senna.WorkerSettings{
			Concurrency:           1,
			Queues:                []senna.QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout:       time.Second,
			PollInterval:          50 * time.Millisecond,
			ScheduledPollInterval: customInterval,
			HeartbeatRate:         time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	if w.config.Settings.ScheduledPollInterval != customInterval {
		t.Errorf("expected ScheduledPollInterval %v, got %v",
			customInterval, w.config.Settings.ScheduledPollInterval)
	}
}

func TestWorker_ScheduledPollInterval_DefaultValue(t *testing.T) {
	settings := senna.DefaultWorkerSettings()

	if settings.ScheduledPollInterval != 5*time.Second {
		t.Errorf("expected default ScheduledPollInterval 5s, got %v", settings.ScheduledPollInterval)
	}
}

func TestWorker_Scheduler_ConcurrentWorkers_NoDuplicates(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-scheduler-concurrent:*")

	// Create multiple workers
	workers := make([]*Worker, 3)
	for i := range workers {
		w, err := New(&Config{
			Redis:     getTestRedisConfig(),
			Namespace: "test-scheduler-concurrent",
			Settings:  senna.DefaultWorkerSettings(),
		})
		if err != nil {
			t.Fatalf("failed to create worker %d: %v", i, err)
		}
		workers[i] = w
		defer func() { _ = w.redis.Close() }()
	}

	ctx := context.Background()
	pastTime := time.Now().Add(-time.Minute)

	// Add 10 jobs
	for i := 0; i < 10; i++ {
		job := senna.NewJob("concurrent_job", map[string]any{"index": i})
		jobData, _ := job.Marshal()
		redisClient.ZAdd(ctx, workers[0].keys.Scheduled(), redis.Z{
			Score:  float64(pastTime.Unix()),
			Member: string(jobData),
		})
	}

	// Run schedulers concurrently from all workers
	done := make(chan struct{})
	for _, w := range workers {
		go func(w *Worker) {
			w.enqueueScheduled(ctx)
			done <- struct{}{}
		}(w)
	}

	// Wait for all workers
	for range workers {
		<-done
	}

	// Verify exactly 10 jobs in queue (no duplicates)
	queueLen, _ := redisClient.LLen(ctx, workers[0].keys.Queue("default")).Result()
	if queueLen != 10 {
		t.Errorf("expected exactly 10 jobs in queue (no duplicates), got %d", queueLen)
	}

	// Verify scheduled set is empty
	scheduledLen, _ := redisClient.ZCard(ctx, workers[0].keys.Scheduled()).Result()
	if scheduledLen != 0 {
		t.Errorf("expected 0 jobs in scheduled, got %d", scheduledLen)
	}
}

func TestWorker_Retries_ConcurrentWorkers_NoDuplicates(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-retry-concurrent:*")

	// Create multiple workers
	workers := make([]*Worker, 3)
	for i := range workers {
		w, err := New(&Config{
			Redis:     getTestRedisConfig(),
			Namespace: "test-retry-concurrent",
			Settings:  senna.DefaultWorkerSettings(),
		})
		if err != nil {
			t.Fatalf("failed to create worker %d: %v", i, err)
		}
		workers[i] = w
		defer func() { _ = w.redis.Close() }()
	}

	ctx := context.Background()
	pastTime := time.Now().Add(-time.Minute)

	// Add 10 retry jobs
	for i := 0; i < 10; i++ {
		job := senna.NewJob("retry_job", map[string]any{"index": i})
		job.RetryCount = 1
		jobData, _ := job.Marshal()
		redisClient.ZAdd(ctx, workers[0].keys.Retry(), redis.Z{
			Score:  float64(pastTime.Unix()),
			Member: string(jobData),
		})
	}

	// Run retry processors concurrently from all workers
	done := make(chan struct{})
	for _, w := range workers {
		go func(w *Worker) {
			w.enqueueRetries(ctx)
			done <- struct{}{}
		}(w)
	}

	// Wait for all workers
	for range workers {
		<-done
	}

	// Verify exactly 10 jobs in queue (no duplicates)
	queueLen, _ := redisClient.LLen(ctx, workers[0].keys.Queue("default")).Result()
	if queueLen != 10 {
		t.Errorf("expected exactly 10 retry jobs in queue (no duplicates), got %d", queueLen)
	}

	// Verify retry set is empty
	retryLen, _ := redisClient.ZCard(ctx, workers[0].keys.Retry()).Result()
	if retryLen != 0 {
		t.Errorf("expected 0 jobs in retry set, got %d", retryLen)
	}
}

func TestWorker_RequeueOrphanedJobs_StaleHeartbeat(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-orphan:*")

	ctx := context.Background()
	ns := "test-orphan"

	// Create a worker to get access to keys helper
	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: ns,
		Settings: senna.WorkerSettings{
			Concurrency:   1,
			Queues:        []senna.QueueConfig{{Name: "default"}},
			HeartbeatRate: time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	// Simulate a crashed worker by:
	// 1. Adding a stale heartbeat entry (beat_at in the past)
	// 2. Adding jobs to its in-flight list
	crashedWorkerID := "crashed-worker-123"
	staleTime := time.Now().Add(-2 * workerHeartbeatTimeout).Unix()

	workerInfo := map[string]any{
		"hostname":   "crashed-host",
		"pid":        12345,
		"beat_at":    staleTime,
		"started_at": staleTime,
	}
	workerData, _ := json.Marshal(workerInfo)
	redisClient.HSet(ctx, w.keys.Workers(), crashedWorkerID, string(workerData))

	// Add orphaned jobs to the crashed worker's in-flight list
	job1 := senna.NewJob("orphan_job", map[string]any{"id": 1})
	job1.Queue = "default"
	data1, _ := job1.Marshal()

	job2 := senna.NewJob("orphan_job", map[string]any{"id": 2})
	job2.Queue = "default"
	data2, _ := job2.Marshal()

	inFlightKey := w.keys.InFlight(crashedWorkerID)
	redisClient.LPush(ctx, inFlightKey, string(data1), string(data2))

	// Verify setup
	inFlightLen, _ := redisClient.LLen(ctx, inFlightKey).Result()
	if inFlightLen != 2 {
		t.Fatalf("expected 2 jobs in crashed worker's in-flight, got %d", inFlightLen)
	}

	// Run orphan recovery
	w.requeueOrphanedJobs(ctx)

	// Verify orphaned jobs were requeued
	queueLen, _ := redisClient.LLen(ctx, w.keys.Queue("default")).Result()
	if queueLen != 2 {
		t.Errorf("expected 2 jobs requeued to default queue, got %d", queueLen)
	}

	// Verify in-flight list was cleaned up
	inFlightLen, _ = redisClient.LLen(ctx, inFlightKey).Result()
	if inFlightLen != 0 {
		t.Errorf("expected 0 jobs in crashed worker's in-flight after recovery, got %d", inFlightLen)
	}

	// Verify stale worker was removed from workers hash
	exists, _ := redisClient.HExists(ctx, w.keys.Workers(), crashedWorkerID).Result()
	if exists {
		t.Error("expected stale worker to be removed from workers hash")
	}
}

func TestWorker_RequeueOrphanedJobs_ActiveWorkerNotAffected(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-orphan-active:*")

	ctx := context.Background()
	ns := "test-orphan-active"

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: ns,
		Settings: senna.WorkerSettings{
			Concurrency:   1,
			Queues:        []senna.QueueConfig{{Name: "default"}},
			HeartbeatRate: time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}
	defer func() { _ = w.redis.Close() }()

	// Simulate an active worker with recent heartbeat
	activeWorkerID := "active-worker-456"
	recentTime := time.Now().Unix()

	workerInfo := map[string]any{
		"hostname":   "active-host",
		"pid":        67890,
		"beat_at":    recentTime,
		"started_at": recentTime,
	}
	workerData, _ := json.Marshal(workerInfo)
	redisClient.HSet(ctx, w.keys.Workers(), activeWorkerID, string(workerData))

	// Add jobs to active worker's in-flight list (simulating jobs being processed)
	job := senna.NewJob("active_job", map[string]any{"id": 1})
	job.Queue = "default"
	data, _ := job.Marshal()

	inFlightKey := w.keys.InFlight(activeWorkerID)
	redisClient.LPush(ctx, inFlightKey, string(data))

	// Run orphan recovery
	w.requeueOrphanedJobs(ctx)

	// Verify active worker's in-flight jobs were NOT requeued
	queueLen, _ := redisClient.LLen(ctx, w.keys.Queue("default")).Result()
	if queueLen != 0 {
		t.Errorf("expected 0 jobs requeued (active worker should not be affected), got %d", queueLen)
	}

	// Verify in-flight list is still intact
	inFlightLen, _ := redisClient.LLen(ctx, inFlightKey).Result()
	if inFlightLen != 1 {
		t.Errorf("expected 1 job still in active worker's in-flight, got %d", inFlightLen)
	}

	// Verify active worker still in workers hash
	exists, _ := redisClient.HExists(ctx, w.keys.Workers(), activeWorkerID).Result()
	if !exists {
		t.Error("expected active worker to remain in workers hash")
	}
}
