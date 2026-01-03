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
	pool       *workerPool
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
		id:      id,
		redis:   client,
		keys:    k,
		config:  cfg,
		pool:    newWorkerPool(cfg.Settings.Concurrency),
		fetcher: newFetcher(client, k, cfg.Settings.Queues, cfg.Settings.PollInterval),
		stopCh:  make(chan struct{}),
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
	w.pool.Register(jobType, handler, jobOpts)
}

func (w *Worker) Use(mw ...senna.Middleware) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.middleware = append(w.middleware, mw...)
	w.pool.Use(mw...)
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

	w.pool.Start(ctx)

	go w.heartbeat(ctx)
	go w.scheduler(ctx)
	go w.reaper(ctx)

	if w.periodic != nil {
		w.periodic.Start(ctx)
	}

	var wg sync.WaitGroup
	for i := 0; i < w.config.Settings.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.fetchLoop(ctx)
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

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), w.config.Settings.ShutdownTimeout)
	defer shutdownCancel()

	w.pool.Stop()
	wg.Wait()

	return w.pool.Drain(shutdownCtx)
}

func (w *Worker) Stop() {
	close(w.stopCh)
}

func (w *Worker) Close() error {
	return w.redis.Close()
}

func (w *Worker) fetchLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		job, err := w.fetcher.Fetch(ctx, w.id)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.ErrorContext(ctx, "fetch failed", "error", err)
			time.Sleep(time.Second)
			continue
		}

		if job == nil {
			time.Sleep(w.config.Settings.PollInterval)
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

	opts, err := w.pool.process(ctx, job)

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
			}
			data, _ := json.Marshal(info)
			w.redis.HSet(ctx, w.keys.Workers(), w.id, string(data))
			w.redis.Expire(ctx, w.keys.Workers(), 60*time.Second)
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

func (w *Worker) requeueOrphanedJobs(ctx context.Context) {
	workers, err := w.redis.HGetAll(ctx, w.keys.Workers()).Result()
	if err != nil {
		return
	}

	activeWorkers := make(map[string]bool)
	for id := range workers {
		activeWorkers[id] = true
	}

	pattern := w.keys.InFlight("*")
	foundKeys, err := w.redis.Keys(ctx, pattern).Result()
	if err != nil {
		return
	}

	for _, key := range foundKeys {
		workerID := key[len(w.keys.InFlight(""))+1:]
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
