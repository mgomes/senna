package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/mgomes/senna"
	"github.com/mgomes/senna/internal/encryption"
	"github.com/mgomes/senna/internal/keys"
	"github.com/mgomes/senna/periodic"
	"github.com/redis/go-redis/v9"
)

type Worker struct {
	id         string
	redis      *redis.Client
	keys       *keys.Keys
	config     *Config
	handlers   *handlerRegistry
	fetcher    *fetcher
	encryptor  *encryption.Encryptor
	middleware []senna.Middleware
	periodic   *periodic.Scheduler
	running    bool
	mu         sync.RWMutex
	stopCh     chan struct{}
}

type Config struct {
	Redis      senna.RedisConfig
	Namespace  string
	Settings   senna.WorkerSettings
	Encryption *senna.EncryptionSettings
}

func New(cfg *Config) (*Worker, error) {
	if cfg.Settings.Concurrency == 0 {
		cfg.Settings = senna.DefaultWorkerSettings()
	}

	client := redis.NewClient(cfg.Redis.Options())

	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, err
	}

	k := keys.New(cfg.Namespace)
	id := fmt.Sprintf("%s:%d:%s", hostname(), os.Getpid(), uuid.New().String()[:8])

	w := &Worker{
		id:       id,
		redis:    client,
		keys:     k,
		config:   cfg,
		handlers: newHandlerRegistry(),
		fetcher:  newFetcher(client, k, cfg.Settings.Queues, cfg.Settings.PollInterval, cfg.Settings.StrictPriority),
		stopCh:   make(chan struct{}),
	}

	if cfg.Encryption != nil && cfg.Encryption.Enabled {
		enc, err := encryption.New(cfg.Encryption.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to init encryptor: %w", err)
		}
		w.encryptor = enc

		mw := encryptionMiddleware(enc)
		w.Use(mw)
	}

	w.Use(senna.RecoveryMiddleware())

	if cfg.Settings.PeriodicEnabled {
		w.periodic = periodic.NewScheduler(client, k)
	}

	return w, nil
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}

func (w *Worker) Redis() *redis.Client {
	return w.redis
}

func (w *Worker) Register(jobType string, handler senna.Handler, opts ...JobOption) {
	jobOpts := &JobOptions{
		MaxRetries:   25,
		RetryBackoff: senna.DefaultBackoff(),
	}
	for _, opt := range opts {
		opt(jobOpts)
	}
	w.handlers.Register(jobType, handler, jobOpts)
}

func (w *Worker) Use(mw ...senna.Middleware) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.middleware = append(w.middleware, mw...)
	w.handlers.Use(mw...)
}

// Periodic registers a periodic job that runs on the given cron schedule.
// The worker must have PeriodicEnabled set to true in its config.
// Returns an error if periodic jobs are not enabled or if the cron expression is invalid.
func (w *Worker) Periodic(cronExpr, jobType string, opts ...periodic.Option) error {
	if w.periodic == nil {
		return errors.New("periodic jobs not enabled; set PeriodicEnabled: true in worker config")
	}
	return w.periodic.Register(cronExpr, jobType, opts...)
}

// PeriodicJobs returns all registered periodic jobs.
func (w *Worker) PeriodicJobs() []*periodic.Job {
	if w.periodic == nil {
		return nil
	}
	return w.periodic.Jobs()
}

func (w *Worker) Run(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return errors.New("worker already running")
	}
	w.running = true
	w.mu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Channel to signal when all workers have finished processing
	workersDone := make(chan struct{})

	go w.heartbeat(ctx)
	go w.scheduler(ctx)
	go w.reaper(ctx)
	go w.sequentialLockRenewer(ctx, workersDone)

	if w.periodic != nil {
		w.periodic.Start(ctx)
	}

	// Start worker goroutines that block on Redis for jobs
	var wg sync.WaitGroup
	for i := 0; i < w.config.Settings.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.workerLoop(ctx)
		}()
	}

	select {
	case <-sigCh:
	case <-ctx.Done():
	case <-w.stopCh:
	}

	cancel()

	if w.periodic != nil {
		w.periodic.Stop()
	}

	// Wait for all workers to finish with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(workersDone) // Signal lock renewer to stop
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(w.config.Settings.ShutdownTimeout):
		return context.DeadlineExceeded
	}
}

func (w *Worker) Stop() {
	close(w.stopCh)
}

func (w *Worker) Close() error {
	return w.redis.Close()
}

// workerLoop blocks on Redis waiting for jobs, then processes them.
// Uses BLMOVE for efficient blocking without polling.
func (w *Worker) workerLoop(ctx context.Context) {
	// Block timeout controls how long we wait before cycling.
	// This allows checking context cancellation and rotating across queues.
	// Uses configured PollInterval to honor user's latency/load tuning preferences.
	blockTimeout := w.config.Settings.PollInterval
	if blockTimeout <= 0 {
		blockTimeout = 100 * time.Millisecond
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		job, err := w.fetcher.BlockingFetch(ctx, w.id, blockTimeout)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.ErrorContext(ctx, "fetch failed", "error", err)
			time.Sleep(time.Second)
			continue
		}

		if job == nil {
			// Timeout, loop to check context and try again
			continue
		}

		w.processJob(ctx, job)
	}
}

func (w *Worker) processJob(ctx context.Context, job *senna.Job) {
	// If job is part of a batch, attach batch handle to context
	if job.BatchID != "" {
		bh := newBatchHandle(job.BatchID, w.redis, w.keys)
		ctx = contextWithBatch(ctx, bh)
	}

	// Release sequential lock after processing to allow other workers to take over.
	// Use a non-canceled context so shutdown doesn't strand the lock.
	defer w.fetcher.ReleaseSequentialLock(context.WithoutCancel(ctx), w.id, job.Queue)

	// Check for iterable handler first
	if iterHandler, iterOpts, ok := w.handlers.GetIterable(job.Type); ok {
		w.processIterableJob(ctx, job, iterHandler, iterOpts)
		return
	}

	opts, err := w.handlers.process(ctx, job)

	if err == nil {
		_ = w.fetcher.Ack(ctx, w.id, job)
		w.updateBatchProgress(ctx, job, batchResultSuccess)
		return
	}

	var retryErr *senna.RetryableError
	if errors.As(err, &retryErr) {
		w.updateBatchProgress(ctx, job, batchResultFailure)
		_ = w.fetcher.Nack(ctx, w.id, job, retryErr.RetryIn)
		return
	}

	var maxRetriesErr *senna.MaxRetriesExceededError
	if errors.As(err, &maxRetriesErr) {
		job.Error = maxRetriesErr.Error()
		_ = w.fetcher.MoveToDead(ctx, w.id, job)
		w.updateBatchProgress(ctx, job, batchResultDeath)
		return
	}

	backoffFn := senna.DefaultBackoff()
	maxRetries := job.Retry
	if opts != nil {
		if opts.RetryBackoff != nil {
			backoffFn = opts.RetryBackoff
		}
		// Use handler's MaxRetries setting (which defaults to 25 if not set)
		maxRetries = opts.MaxRetries
	}
	backoff := backoffFn(job.RetryCount)
	if job.RetryCount < maxRetries {
		w.updateBatchProgress(ctx, job, batchResultFailure)
		_ = w.fetcher.Nack(ctx, w.id, job, backoff)
	} else {
		job.Error = err.Error()
		_ = w.fetcher.MoveToDead(ctx, w.id, job)
		w.updateBatchProgress(ctx, job, batchResultDeath)
	}
}

// processIterableJob handles iterable jobs with cursor tracking and interruption support.
func (w *Worker) processIterableJob(ctx context.Context, job *senna.Job, handler senna.IterableHandler, opts *IterableJobOptions) {
	now := time.Now()
	job.ProcessedAt = &now

	iterHandler := func(ctx context.Context, job *senna.Job) error {
		return w.processIterable(ctx, job, handler, opts)
	}

	if opts != nil && opts.Timeout > 0 {
		iterHandler = senna.TimeoutMiddleware(opts.Timeout)(iterHandler)
	}
	if opts != nil && opts.RateLimiter != nil {
		iterHandler = rateLimitMiddlewareWithReschedule(opts.RateLimiter)(iterHandler)
	}
	if middleware := w.handlers.middlewareChain(); len(middleware) > 0 {
		iterHandler = senna.Chain(middleware...)(iterHandler)
	}

	err := iterHandler(ctx, job)

	if err == nil {
		_ = w.fetcher.Ack(ctx, w.id, job)
		w.updateBatchProgress(ctx, job, batchResultSuccess)
		return
	}

	// Handle InterruptedError - requeue same job, no retry increment, no batch failure
	var interruptedErr *senna.InterruptedError
	if errors.As(err, &interruptedErr) {
		// Requeue with same job ID - don't treat as failure
		if requeueErr := w.requeue(context.WithoutCancel(ctx), job); requeueErr != nil {
			slog.ErrorContext(ctx, "failed to requeue interrupted job", "error", requeueErr, "job_id", job.ID)
		}
		// No batch progress update - this is not a failure
		return
	}

	var retryErr *senna.RetryableError
	if errors.As(err, &retryErr) {
		w.updateBatchProgress(ctx, job, batchResultFailure)
		_ = w.fetcher.Nack(ctx, w.id, job, retryErr.RetryIn)
		return
	}

	var maxRetriesErr *senna.MaxRetriesExceededError
	if errors.As(err, &maxRetriesErr) {
		job.Error = maxRetriesErr.Error()
		_ = w.fetcher.MoveToDead(ctx, w.id, job)
		w.updateBatchProgress(ctx, job, batchResultDeath)
		return
	}

	// Standard error - use backoff retry
	backoffFn := senna.DefaultBackoff()
	maxRetries := 25
	if opts != nil {
		if opts.RetryBackoff != nil {
			backoffFn = opts.RetryBackoff
		}
		if opts.MaxRetries > 0 {
			maxRetries = opts.MaxRetries
		}
	}
	backoff := backoffFn(job.RetryCount)
	if job.RetryCount < maxRetries {
		w.updateBatchProgress(ctx, job, batchResultFailure)
		_ = w.fetcher.Nack(ctx, w.id, job, backoff)
	} else {
		job.Error = err.Error()
		_ = w.fetcher.MoveToDead(ctx, w.id, job)
		w.updateBatchProgress(ctx, job, batchResultDeath)
	}
}

// batchResult represents the result of a job completion.
type batchResult string

const (
	batchResultSuccess batchResult = "success"
	batchResultFailure batchResult = "failure"
	batchResultDeath   batchResult = "death"
)

// batchCallbackResult is the response from the batch_complete Lua script.
type batchCallbackResult struct {
	Callbacks        []batchCallback `json:"callbacks"`
	Pending          int             `json:"pending"`
	Successes        int             `json:"successes"`
	Failures         int             `json:"failures"`
	Dead             bool            `json:"dead"`
	CallbackQueue    string          `json:"callback_queue"`
	Error            string          `json:"error,omitempty"`
	Invalidated      bool            `json:"invalidated,omitempty"`
	AlreadyProcessed bool            `json:"already_processed,omitempty"`
}

type batchCallback struct {
	CallbackType string         `json:"callback_type"`
	JobType      string         `json:"job_type"`
	Options      map[string]any `json:"options,omitempty"`
}

func (w *Worker) updateBatchProgress(ctx context.Context, job *senna.Job, result batchResult) {
	if job.BatchID == "" {
		return
	}

	keys := []string{
		w.keys.Batch(job.BatchID),
		w.keys.BatchJobs(job.BatchID),
		w.keys.BatchFailed(job.BatchID),
		w.keys.DeadBatches(),
	}

	resultJSON, err := batchCompleteScript.Run(ctx, w.redis, keys, job.ID, string(result))
	if err != nil {
		slog.ErrorContext(ctx, "batch script failed", "error", err, "batch_id", job.BatchID)
		return
	}

	var callbackResult batchCallbackResult
	if err := json.Unmarshal([]byte(resultJSON.(string)), &callbackResult); err != nil {
		slog.ErrorContext(ctx, "failed to parse batch result", "error", err)
		return
	}

	if callbackResult.Error != "" || callbackResult.AlreadyProcessed {
		return
	}

	// Enqueue any callbacks that need to fire
	queue := callbackResult.CallbackQueue
	if queue == "" {
		queue = "default"
	}

	for _, cb := range callbackResult.Callbacks {
		w.enqueueBatchCallback(ctx, cb.JobType, job.BatchID, cb.Options, queue)
	}
}

func (w *Worker) enqueueBatchCallback(ctx context.Context, jobType, batchID string, options map[string]any, queue string) {
	args := map[string]any{
		"batch_id": batchID,
	}
	// Merge user-provided options into args
	for k, v := range options {
		args[k] = v
	}

	job := senna.NewJob(jobType, args)
	job.Queue = queue
	data, _ := job.Marshal()
	w.redis.LPush(ctx, w.keys.Queue(queue), string(data))
}

func (w *Worker) heartbeat(ctx context.Context) {
	ticker := time.NewTicker(w.config.Settings.HeartbeatRate)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.redis.HDel(context.Background(), w.keys.Workers(), w.id)
			return
		case <-ticker.C:
			info := map[string]any{
				"hostname":    hostname(),
				"pid":         os.Getpid(),
				"queues":      w.config.Settings.Queues,
				"concurrency": w.config.Settings.Concurrency,
				"started_at":  time.Now().Unix(),
				"beat_at":     time.Now().Unix(),
			}
			data, _ := json.Marshal(info)
			w.redis.HSet(ctx, w.keys.Workers(), w.id, string(data))
		}
	}
}

// sequentialLockRenewer periodically renews locks for sequential queues
// to prevent expiry during long-running job processing.
// Keeps running until workersDone is closed to ensure locks are renewed
// during graceful shutdown while jobs are still being processed.
func (w *Worker) sequentialLockRenewer(ctx context.Context, workersDone <-chan struct{}) {
	// Renew at 1/3 of TTL to ensure locks don't expire
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// Use background context for renewals during shutdown
	// since the main ctx may be cancelled while jobs are still running
	renewCtx := context.Background()

	for {
		select {
		case <-workersDone:
			return
		case <-ticker.C:
			w.fetcher.RenewSequentialLocks(renewCtx, w.id)
		}
	}
}

func (w *Worker) scheduler(ctx context.Context) {
	interval := w.config.Settings.ScheduledPollInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.enqueueScheduled(ctx)
			w.enqueueRetries(ctx)
		}
	}
}

func (w *Worker) enqueueScheduled(ctx context.Context) {
	now := fmt.Sprintf("%d", time.Now().Unix())
	queuePrefix := w.keys.Queue("")

	for {
		// Atomically pop due jobs and push to their queues
		result, err := enqueueScheduledScript.Run(
			ctx, w.redis,
			[]string{w.keys.Scheduled(), w.keys.Queues()},
			now, 100, queuePrefix,
		)
		if err != nil {
			return
		}

		count, ok := result.(int64)
		if !ok || count == 0 {
			return
		}
	}
}

func (w *Worker) enqueueRetries(ctx context.Context) {
	now := fmt.Sprintf("%d", time.Now().Unix())
	queuePrefix := w.keys.Queue("")

	for {
		// Atomically pop due retries and push to their queues
		result, err := enqueueScheduledScript.Run(
			ctx, w.redis,
			[]string{w.keys.Retry(), w.keys.Queues()},
			now, 100, queuePrefix,
		)
		if err != nil {
			return
		}

		count, ok := result.(int64)
		if !ok || count == 0 {
			return
		}
	}
}

func (w *Worker) reaper(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.requeueOrphanedJobs(ctx)
		}
	}
}

// workerHeartbeatTimeout defines how long a worker can go without heartbeating
// before it's considered dead and its in-flight jobs are recovered.
const workerHeartbeatTimeout = 60 * time.Second

func (w *Worker) requeueOrphanedJobs(ctx context.Context) {
	workers, err := w.redis.HGetAll(ctx, w.keys.Workers()).Result()
	if err != nil {
		return
	}

	now := time.Now().Unix()
	activeWorkers := make(map[string]bool)
	staleWorkers := make([]string, 0)

	for id, data := range workers {
		var info map[string]any
		if err := json.Unmarshal([]byte(data), &info); err != nil {
			continue
		}

		beatAt, ok := info["beat_at"].(float64)
		if !ok {
			// Legacy entry without beat_at, treat as stale
			staleWorkers = append(staleWorkers, id)
			continue
		}

		if now-int64(beatAt) > int64(workerHeartbeatTimeout.Seconds()) {
			staleWorkers = append(staleWorkers, id)
		} else {
			activeWorkers[id] = true
		}
	}

	// Clean up stale worker entries from the hash
	for _, id := range staleWorkers {
		w.redis.HDel(ctx, w.keys.Workers(), id)
	}

	// Use SCAN instead of KEYS to avoid blocking Redis on large databases
	pattern := w.keys.InFlight("*")
	var cursor uint64
	for {
		keys, nextCursor, err := w.redis.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return
		}

		for _, key := range keys {
			workerID := key[len(w.keys.InFlight("")):]
			if activeWorkers[workerID] {
				continue
			}

			jobs, err := w.redis.LRange(ctx, key, 0, -1).Result()
			if err != nil {
				continue
			}

			for _, data := range jobs {
				var job senna.Job
				if err := json.Unmarshal([]byte(data), &job); err != nil {
					continue
				}
				w.redis.LPush(ctx, w.keys.Queue(job.Queue), data)
			}

			w.redis.Del(ctx, key)
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
}

func encryptionMiddleware(enc *encryption.Encryptor) senna.Middleware {
	return func(next senna.Handler) senna.Handler {
		return func(ctx context.Context, job *senna.Job) error {
			if job.Encrypted {
				decrypted, err := enc.Decrypt(job.Args)
				if err != nil {
					return fmt.Errorf("failed to decrypt job args: %w", err)
				}
				job.Args = decrypted
				job.Encrypted = false
			}
			return next(ctx, job)
		}
	}
}
