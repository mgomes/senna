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
	"github.com/mgomes/senna/internal/batch"
	"github.com/mgomes/senna/internal/encryption"
	"github.com/mgomes/senna/internal/keys"
	"github.com/mgomes/senna/periodic"
	"github.com/redis/go-redis/v9"
)

var errWorkerAlreadyRunning = errors.New("worker already running")

// Worker fetches and executes jobs from Redis queues.
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
	stopping   bool
	mu         sync.RWMutex
	stopCh     chan struct{}

	sequentialLockRenewEvery time.Duration
}

// Config configures a Worker.
type Config struct {
	Redis      senna.RedisConfig
	Namespace  string
	Settings   senna.WorkerSettings
	Encryption *senna.EncryptionSettings
}

// New creates a Worker and verifies the Redis connection.
func New(cfg *Config) (*Worker, error) {
	cfg.Settings = normalizeWorkerSettings(cfg.Settings)

	var enc *encryption.Encryptor
	if cfg.Encryption != nil && cfg.Encryption.Enabled {
		var err error
		enc, err = encryption.New(cfg.Encryption.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to init encryptor: %w", err)
		}
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
		fetcher: newFetcherWithSequentialLockTTL(
			client,
			k,
			cfg.Settings.Queues,
			cfg.Settings.PollInterval,
			cfg.Settings.StrictPriority,
			cfg.Settings.SequentialLockTTL,
		),
		stopCh: make(chan struct{}),

		sequentialLockRenewEvery: cfg.Settings.SequentialLockRenewInterval,
	}

	if enc != nil {
		w.encryptor = enc

		mw := encryptionMiddleware(enc)
		w.Use(mw)
	}

	w.Use(senna.RecoveryMiddleware())

	if cfg.Settings.PeriodicEnabled {
		w.periodic = periodic.NewScheduler(
			client,
			k,
			periodic.WithPollInterval(cfg.Settings.PeriodicPollInterval),
		)
	}

	return w, nil
}

func normalizeWorkerSettings(settings senna.WorkerSettings) senna.WorkerSettings {
	defaults := senna.DefaultWorkerSettings()

	if settings.Concurrency <= 0 {
		settings.Concurrency = defaults.Concurrency
	}
	if len(settings.Queues) == 0 {
		settings.Queues = append([]senna.QueueConfig(nil), defaults.Queues...)
	} else {
		settings.Queues = append([]senna.QueueConfig(nil), settings.Queues...)
	}
	if settings.ShutdownTimeout <= 0 {
		settings.ShutdownTimeout = defaults.ShutdownTimeout
	}
	if settings.PollInterval <= 0 {
		settings.PollInterval = defaults.PollInterval
	}
	if settings.BlockTimeout <= 0 {
		settings.BlockTimeout = defaults.BlockTimeout
	}
	if settings.ScheduledPollInterval <= 0 {
		settings.ScheduledPollInterval = defaults.ScheduledPollInterval
	}
	if settings.HeartbeatRate <= 0 {
		settings.HeartbeatRate = defaults.HeartbeatRate
	}
	if settings.ReaperOperationTimeout <= 0 {
		settings.ReaperOperationTimeout = defaults.ReaperOperationTimeout
	}
	if settings.ReaperInterval <= 0 {
		settings.ReaperInterval = defaults.ReaperInterval
	}
	if settings.SequentialLockTTL <= 0 {
		settings.SequentialLockTTL = defaults.SequentialLockTTL
	}
	if settings.SequentialLockRenewInterval <= 0 {
		settings.SequentialLockRenewInterval = defaults.SequentialLockRenewInterval
	}
	if settings.SequentialLockRenewInterval >= settings.SequentialLockTTL {
		settings.SequentialLockRenewInterval = settings.SequentialLockTTL / 3
		if settings.SequentialLockRenewInterval <= 0 {
			settings.SequentialLockRenewInterval = settings.SequentialLockTTL
		}
	}
	if settings.IterableMaxRuntime == 0 {
		settings.IterableMaxRuntime = defaults.IterableMaxRuntime
	}
	if settings.PeriodicPollInterval <= 0 {
		settings.PeriodicPollInterval = defaults.PeriodicPollInterval
	}

	return settings
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}

// Redis returns the underlying Redis client.
func (w *Worker) Redis() *redis.Client {
	return w.redis
}

// Register registers a handler for the given job type.
func (w *Worker) Register(jobType string, handler senna.Handler, opts ...JobOption) {
	jobOpts := &JobOptions{
		MaxRetries:   senna.DefaultRetryCount,
		RetryBackoff: senna.DefaultBackoff(),
	}
	for _, opt := range opts {
		opt(jobOpts)
	}
	w.handlers.Register(jobType, handler, jobOpts)
}

// Use appends middleware to the worker and its registered handlers.
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

// Run starts the worker loop and blocks until shutdown completes. If shutdown
// exceeds ShutdownTimeout, Run returns context.DeadlineExceeded and rejects
// another Run call until the still-active worker goroutines exit.
func (w *Worker) Run(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return errWorkerAlreadyRunning
	}
	w.running = true
	w.stopping = false
	w.stopCh = make(chan struct{})
	stopCh := w.stopCh
	w.mu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	renewCtx, stopRenewingSequentialLocks := context.WithCancel(context.Background())
	defer stopRenewingSequentialLocks()

	go w.heartbeat(ctx)
	go w.scheduler(ctx)
	go w.reaper(ctx)
	go w.sequentialLockRenewer(renewCtx)

	if w.periodic != nil {
		w.periodic.Start(ctx)
	}

	// Start worker goroutines that block on Redis for jobs
	var wg sync.WaitGroup
	for range w.config.Settings.Concurrency {
		wg.Go(func() {
			w.workerLoop(ctx)
		})
	}

	select {
	case <-sigCh:
	case <-ctx.Done():
	case <-stopCh:
	}

	cancel()

	if w.periodic != nil {
		w.periodic.Stop()
	}

	// Wait for all workers to finish with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		stopRenewingSequentialLocks()
		w.mu.Lock()
		w.running = false
		w.stopping = false
		w.mu.Unlock()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(w.config.Settings.ShutdownTimeout):
		stopRenewingSequentialLocks()
		return context.DeadlineExceeded
	}
}

// Stop requests a graceful worker shutdown.
func (w *Worker) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.running || w.stopping {
		return
	}
	close(w.stopCh)
	w.stopping = true
}

// Close closes the underlying Redis client.
func (w *Worker) Close() error {
	return w.redis.Close()
}

// workerLoop blocks on Redis waiting for jobs, then processes them.
// Uses BLMOVE for efficient blocking without polling.
func (w *Worker) workerLoop(ctx context.Context) {
	blockTimeout := workerBlockTimeout(w.config.Settings)

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

func workerBlockTimeout(settings senna.WorkerSettings) time.Duration {
	blockTimeout := settings.BlockTimeout
	if blockTimeout <= 0 {
		blockTimeout = senna.DefaultWorkerSettings().BlockTimeout
	}

	shutdownTimeout := settings.ShutdownTimeout
	if shutdownTimeout > 0 && blockTimeout >= shutdownTimeout {
		blockTimeout = shutdownTimeout / 2
		if blockTimeout <= 0 {
			blockTimeout = shutdownTimeout
		}
	}
	return blockTimeout
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

	if job.Finalization() != nil {
		w.resumeFinalizedJob(ctx, job)
		return
	}

	// Check for iterable handler first
	if iterHandler, iterOpts, ok := w.handlers.GetIterable(job.Type); ok {
		w.processIterableJob(ctx, job, iterHandler, iterOpts)
		return
	}

	opts, err := w.handlers.process(ctx, job)
	w.handleJobResult(ctx, job, err, opts, nil)
}

const (
	jobFinalizationRetryInterval = 250 * time.Millisecond
	jobFinalizationComplete      = "complete"
	jobFinalizationRetry         = "retry"
	jobFinalizationKill          = "kill"
)

// completeJob records batch progress and then removes the job from the
// in-flight list. The ordering is deliberate: the in-flight entry is the
// recovery anchor, so it is removed only once the outcome is durably recorded.
func (w *Worker) completeJob(ctx context.Context, job *senna.Job) {
	finalization := senna.JobFinalization{Operation: jobFinalizationComplete}
	w.finalizeJob(ctx, job, finalization, func(ctx context.Context) error {
		if err := w.updateBatchProgress(ctx, job, batch.ResultSuccess); err != nil {
			return err
		}
		if err := w.handleBatchCallbackComplete(ctx, job); err != nil {
			return err
		}
		if err := w.fetcher.Ack(ctx, w.id, job); err != nil {
			return err
		}
		return nil
	})
}

func (w *Worker) retryJob(ctx context.Context, job *senna.Job, retryIn time.Duration) {
	if retryIn > 0 {
		w.retryJobAt(ctx, job, time.Now().Add(retryIn))
		return
	}
	w.finalizeRetryJob(ctx, job, senna.JobFinalization{Operation: jobFinalizationRetry}, retryIn)
}

func (w *Worker) retryJobAt(ctx context.Context, job *senna.Job, retryAt time.Time) {
	w.finalizeRetryJob(ctx, job, senna.JobFinalization{
		Operation: jobFinalizationRetry,
		RetryAt:   retryAt,
	}, 0)
}

func (w *Worker) finalizeRetryJob(ctx context.Context, job *senna.Job, finalization senna.JobFinalization, retryIn time.Duration) {
	w.finalizeJob(ctx, job, finalization, func(ctx context.Context) error {
		if err := w.updateBatchProgress(ctx, job, batch.ResultFailure); err != nil {
			return err
		}
		if !finalization.RetryAt.IsZero() {
			if err := w.fetcher.NackAt(ctx, w.id, job, finalization.RetryAt); err != nil {
				return err
			}
			return nil
		}
		if err := w.fetcher.Nack(ctx, w.id, job, retryIn); err != nil {
			return err
		}
		return nil
	})
}

// killJob records batch progress and then moves the job to the dead set. As in
// completeJob, the in-flight entry is the recovery anchor and is removed only
// after the outcome is durably recorded.
func (w *Worker) killJob(ctx context.Context, job *senna.Job, err error) {
	job.Error = err.Error()
	finalization := senna.JobFinalization{
		Operation: jobFinalizationKill,
		Error:     job.Error,
	}
	w.finalizeJob(ctx, job, finalization, func(ctx context.Context) error {
		if err := w.updateBatchProgress(ctx, job, batch.ResultDeath); err != nil {
			return err
		}
		if err := w.handleBatchCallbackComplete(ctx, job); err != nil {
			return err
		}
		if err := w.fetcher.MoveToDead(ctx, w.id, job); err != nil {
			return err
		}
		return nil
	})
}

func (w *Worker) resumeFinalizedJob(ctx context.Context, job *senna.Job) {
	finalization := job.Finalization()
	if finalization == nil {
		return
	}

	switch finalization.Operation {
	case jobFinalizationComplete:
		w.completeJob(ctx, job)
	case jobFinalizationRetry:
		if !finalization.RetryAt.IsZero() {
			w.retryJobAt(ctx, job, finalization.RetryAt)
			return
		}
		w.retryJob(ctx, job, 0)
	case jobFinalizationKill:
		w.killJob(ctx, job, errors.New(finalization.Error))
	default:
		slog.ErrorContext(ctx,
			"unknown job finalization operation",
			"job_id", job.ID,
			"operation", finalization.Operation,
		)
		job.ClearFinalization()
		w.killJob(ctx, job, fmt.Errorf("unknown job finalization operation %q", finalization.Operation))
	}
}

func (w *Worker) finalizeJob(ctx context.Context, job *senna.Job, finalization senna.JobFinalization, fn func(context.Context) error) {
	for {
		if job.Finalization() == nil {
			if err := w.fetcher.MarkFinalization(ctx, w.id, job, finalization); err != nil {
				w.logFinalizationRetry(ctx, job, finalization.Operation, err)
				if w.waitToRetryFinalization(ctx, job, finalization.Operation, err) {
					continue
				}
				return
			}
		}

		err := fn(ctx)
		if err == nil {
			return
		}

		w.logFinalizationRetry(ctx, job, finalization.Operation, err)
		if !w.waitToRetryFinalization(ctx, job, finalization.Operation, err) {
			return
		}
	}
}

func (w *Worker) logFinalizationRetry(ctx context.Context, job *senna.Job, operation string, err error) {
	slog.ErrorContext(ctx,
		"job finalization failed; retrying while worker is active",
		"job_id", job.ID,
		"operation", operation,
		"error", err,
	)
}

func (w *Worker) waitToRetryFinalization(ctx context.Context, job *senna.Job, operation string, err error) bool {
	timer := time.NewTimer(jobFinalizationRetryInterval)
	select {
	case <-ctx.Done():
		timer.Stop()
		slog.ErrorContext(context.WithoutCancel(ctx),
			"leaving job in-flight after finalization failure during shutdown",
			"job_id", job.ID,
			"operation", operation,
			"error", err,
		)
		return false
	case <-timer.C:
		return true
	}
}

func (w *Worker) calculateRetryBackoff(job *senna.Job, opts *JobOptions) (time.Duration, bool) {
	backoffFn := senna.DefaultBackoff()
	maxRetries := job.Retry
	if opts != nil {
		if opts.RetryBackoff != nil {
			backoffFn = opts.RetryBackoff
		}
		if opts.MaxRetries < maxRetries {
			maxRetries = opts.MaxRetries
		}
	}
	return backoffFn(job.RetryCount), job.RetryCount < maxRetries
}

func iterableRetryOptions(job *senna.Job, opts *IterableJobOptions) *JobOptions {
	iterOpts := &JobOptions{
		MaxRetries:   job.Retry,
		RetryBackoff: senna.DefaultBackoff(),
	}
	if opts != nil {
		if opts.RetryBackoff != nil {
			iterOpts.RetryBackoff = opts.RetryBackoff
		}
		if opts.MaxRetries < iterOpts.MaxRetries {
			iterOpts.MaxRetries = opts.MaxRetries
		}
	}
	return iterOpts
}

func (w *Worker) handleJobResult(ctx context.Context, job *senna.Job, err error, opts *JobOptions, onInterrupted func(context.Context)) {
	if err == nil {
		w.completeJob(ctx, job)
		return
	}

	var interruptedErr *senna.InterruptedError
	if onInterrupted != nil && errors.As(err, &interruptedErr) {
		onInterrupted(context.WithoutCancel(ctx))
		return
	}

	var retryErr *senna.RetryableError
	if errors.As(err, &retryErr) {
		w.retryJob(ctx, job, retryErr.RetryIn)
		return
	}

	var maxRetriesErr *senna.MaxRetriesExceededError
	if errors.As(err, &maxRetriesErr) {
		w.killJob(ctx, job, maxRetriesErr)
		return
	}

	backoff, shouldRetry := w.calculateRetryBackoff(job, opts)
	if shouldRetry {
		w.retryJob(ctx, job, backoff)
	} else {
		w.killJob(ctx, job, err)
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
		iterHandler = senna.RateLimitMiddlewareWithReschedule(opts.RateLimiter)(iterHandler)
	}
	if middleware := w.handlers.middlewareChain(); len(middleware) > 0 {
		iterHandler = senna.Chain(middleware...)(iterHandler)
	}

	err := iterHandler(ctx, job)
	retryOpts := iterableRetryOptions(job, opts)
	w.handleJobResult(ctx, job, err, retryOpts, func(interruptCtx context.Context) {
		if requeueErr := w.requeue(interruptCtx, job); requeueErr != nil {
			slog.ErrorContext(interruptCtx, "failed to requeue interrupted job", "error", requeueErr, "job_id", job.ID)
		}
	})
}

// updateBatchProgress records the outcome of a batch member in its batch's
// completion counters. It returns an error only when the counter update for
// this job could not be recorded, signaling the caller to keep the job
// in-flight while retrying finalization. Propagation to a parent batch is
// best-effort: a failure there is logged but does not gate the caller, because
// re-running this job cannot re-drive propagation (the completion script is
// idempotent and short-circuits on the second delivery).
func (w *Worker) updateBatchProgress(ctx context.Context, job *senna.Job, result batch.Result) error {
	if job.BatchID == "" {
		return nil
	}

	scriptKeys := batch.CompletionKeys(w.keys, job.BatchID)
	scriptArgs := batch.CompletionArgs(w.keys, job.ID, result)

	var callbackResult batch.CompleteResult
	if err := batchCompleteScript.RunJSON(ctx, w.redis, &callbackResult, scriptKeys, scriptArgs...); err != nil {
		return fmt.Errorf("record batch progress for %s: %w", job.BatchID, err)
	}

	if callbackResult.Error != "" {
		slog.ErrorContext(ctx, "batch completion returned error", "error", callbackResult.Error, "batch_id", job.BatchID)
		return nil
	}
	if callbackResult.AlreadyProcessed {
		return nil
	}

	parentResultType, ok := batch.ParentResultType(&callbackResult)
	if ok {
		parentJob := &senna.Job{
			ID:      job.BatchID,
			BatchID: callbackResult.ParentID,
		}
		if err := w.updateBatchProgress(ctx, parentJob, parentResultType); err != nil {
			slog.ErrorContext(ctx, "failed to propagate batch completion to parent", "error", err, "batch_id", job.BatchID, "parent_id", callbackResult.ParentID)
		}
	}

	return nil
}

// batchCallbackCompleteResult is the response from the batch_callback_complete Lua script.
type batchCallbackCompleteResult struct {
	CallbacksPending int    `json:"callbacks_pending"`
	Pending          int    `json:"pending"`
	ShouldPropagate  bool   `json:"should_propagate"`
	ParentID         string `json:"parent_id,omitempty"`
	Dead             bool   `json:"dead"`
	Invalidated      bool   `json:"invalidated"`
	AlreadyProcessed bool   `json:"already_processed,omitempty"`
	Error            string `json:"error,omitempty"`
}

// handleBatchCallbackComplete is called after a callback job finishes.
// It decrements the callbacks_pending counter and propagates to parent if ready.
func (w *Worker) handleBatchCallbackComplete(ctx context.Context, job *senna.Job) error {
	if job.CallbackBatchID == "" {
		return nil
	}

	keys := []string{
		w.keys.Batch(job.CallbackBatchID),
		w.keys.BatchCallbacks(job.CallbackBatchID),
	}

	var result batchCallbackCompleteResult
	if err := batchCallbackCompleteScript.RunJSON(ctx, w.redis, &result, keys, job.ID); err != nil {
		return fmt.Errorf("record batch callback completion for %s: %w", job.CallbackBatchID, err)
	}

	if result.Error != "" {
		slog.ErrorContext(ctx, "batch callback complete error", "error", result.Error, "batch_id", job.CallbackBatchID)
		return nil
	}

	if result.AlreadyProcessed {
		return nil
	}

	// If all jobs AND all callbacks are done, propagate to parent. Propagation
	// is best-effort for the same reason as updateBatchProgress.
	if result.ShouldPropagate && result.ParentID != "" {
		parentResult := batch.ResultSuccess
		if result.Dead {
			parentResult = batch.ResultDeath
		} else if result.Invalidated {
			parentResult = batch.ResultInvalidated
		}
		parentJob := &senna.Job{
			ID:      job.CallbackBatchID,
			BatchID: result.ParentID,
		}
		if err := w.updateBatchProgress(ctx, parentJob, parentResult); err != nil {
			slog.ErrorContext(ctx, "failed to propagate batch callback completion to parent", "error", err, "batch_id", job.CallbackBatchID, "parent_id", result.ParentID)
		}
	}

	return nil
}

type workerHeartbeatInfo struct {
	Hostname    string              `json:"hostname"`
	PID         int                 `json:"pid"`
	Queues      []senna.QueueConfig `json:"queues"`
	Concurrency int                 `json:"concurrency"`
	StartedAt   int64               `json:"started_at"`
	BeatAt      int64               `json:"beat_at"`
}

func (w *Worker) heartbeat(ctx context.Context) {
	ticker := time.NewTicker(w.config.Settings.HeartbeatRate)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			cleanupCtx := context.WithoutCancel(ctx)
			if err := w.removeHeartbeat(cleanupCtx); err != nil {
				slog.WarnContext(cleanupCtx,
					"failed to remove worker heartbeat",
					"error", err,
					"worker_id", w.id,
				)
			}
			return
		case <-ticker.C:
			if err := w.recordHeartbeat(ctx, time.Now()); err != nil {
				slog.WarnContext(ctx, "failed to record worker heartbeat", "error", err, "worker_id", w.id)
			}
		}
	}
}

func (w *Worker) recordHeartbeat(ctx context.Context, now time.Time) error {
	unixNow := now.Unix()
	info := workerHeartbeatInfo{
		Hostname:    hostname(),
		PID:         os.Getpid(),
		Queues:      w.config.Settings.Queues,
		Concurrency: w.config.Settings.Concurrency,
		StartedAt:   unixNow,
		BeatAt:      unixNow,
	}
	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshal worker heartbeat: %w", err)
	}
	if err := w.redis.HSet(ctx, w.keys.Workers(), w.id, string(data)).Err(); err != nil {
		return fmt.Errorf("write worker heartbeat: %w", err)
	}
	return nil
}

func (w *Worker) removeHeartbeat(ctx context.Context) error {
	if err := w.redis.HDel(ctx, w.keys.Workers(), w.id).Err(); err != nil {
		return fmt.Errorf("remove worker heartbeat: %w", err)
	}
	return nil
}

// sequentialLockRenewer keeps sequential locks alive until its context is cancelled.
// Run gives it a context that survives graceful shutdown but is cancelled once
// workers finish or ShutdownTimeout expires.
func (w *Worker) sequentialLockRenewer(ctx context.Context) {
	interval := w.sequentialLockRenewEvery
	if interval <= 0 {
		interval = senna.DefaultWorkerSettings().SequentialLockRenewInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.fetcher.RenewSequentialLocks(ctx, w.id)
		}
	}
}

func (w *Worker) scheduler(ctx context.Context) {
	interval := w.config.Settings.ScheduledPollInterval
	if interval <= 0 {
		interval = senna.DefaultWorkerSettings().ScheduledPollInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.enqueueScheduled(ctx); err != nil {
				slog.WarnContext(ctx, "failed to enqueue scheduled jobs", "error", err)
			}
			if err := w.enqueueRetries(ctx); err != nil {
				slog.WarnContext(ctx, "failed to enqueue retry jobs", "error", err)
			}
		}
	}
}

func (w *Worker) enqueueFromZSet(ctx context.Context, sourceKey string) error {
	now := fmt.Sprintf("%d", time.Now().Unix())
	queuePrefix := w.keys.Queue("")

	for {
		result, err := enqueueScheduledScript.Run(
			ctx, w.redis,
			[]string{sourceKey, w.keys.Queues()},
			now, 100, queuePrefix,
		)
		if err != nil {
			return fmt.Errorf("promote jobs from %q: %w", sourceKey, err)
		}

		count, ok := result.(int64)
		if !ok {
			return fmt.Errorf("promote jobs from %q: unexpected script result %T", sourceKey, result)
		}
		if count == 0 {
			return nil
		}
	}
}

func (w *Worker) enqueueScheduled(ctx context.Context) error {
	return w.enqueueFromZSet(ctx, w.keys.Scheduled())
}

func (w *Worker) enqueueRetries(ctx context.Context) error {
	return w.enqueueFromZSet(ctx, w.keys.Retry())
}

func (w *Worker) reaper(ctx context.Context) {
	ticker := time.NewTicker(workerReaperInterval(w.config.Settings))
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

func workerReaperInterval(settings senna.WorkerSettings) time.Duration {
	interval := settings.ReaperInterval
	if interval <= 0 {
		return senna.DefaultWorkerSettings().ReaperInterval
	}
	return interval
}

// workerHeartbeatTimeout defines how long a worker can go without heartbeating
// before it's considered dead and its in-flight jobs are recovered.
const workerHeartbeatTimeout = 60 * time.Second

func (w *Worker) requeueOrphanedJobs(ctx context.Context) {
	opCtx, cancel := w.reaperOperationContext(ctx)
	workers, err := w.redis.HGetAll(opCtx, w.keys.Workers()).Result()
	cancel()
	if err != nil {
		if ctx.Err() == nil {
			slog.WarnContext(ctx, "failed to read worker heartbeats", "error", err)
		}
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
		opCtx, cancel := w.reaperOperationContext(ctx)
		err := w.redis.HDel(opCtx, w.keys.Workers(), id).Err()
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				slog.WarnContext(ctx, "failed to remove stale worker heartbeat", "error", err, "worker_id", id)
			}
		}
	}

	// Use SCAN instead of KEYS to avoid blocking Redis on large databases
	pattern := w.keys.InFlight("*")
	queuePrefix := w.keys.Queue("")
	var cursor uint64
	for {
		opCtx, cancel := w.reaperOperationContext(ctx)
		keys, nextCursor, err := w.redis.Scan(opCtx, cursor, pattern, 100).Result()
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				slog.WarnContext(ctx, "failed to scan in-flight jobs", "error", err)
			}
			return
		}

		for _, key := range keys {
			workerID := key[len(w.keys.InFlight("")):]
			if activeWorkers[workerID] {
				continue
			}

			opCtx, cancel := w.reaperOperationContext(ctx)
			_, err := requeueOrphanedScript.Run(opCtx, w.redis, []string{key, w.keys.Queues()}, queuePrefix)
			cancel()
			if err != nil {
				if ctx.Err() == nil {
					slog.WarnContext(ctx, "failed to requeue orphaned jobs", "error", err, "worker_id", workerID)
				}
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
}

func (w *Worker) reaperOperationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, w.config.Settings.ReaperOperationTimeout)
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
