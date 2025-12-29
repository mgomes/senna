package senna

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
	"github.com/mgomes/senna/internal/keys"
	"github.com/redis/go-redis/v9"
)

type Worker struct {
	id         string
	redis      *redis.Client
	keys       *keys.Keys
	config     *WorkerConfig
	pool       *workerPool
	fetcher    *fetcher
	encryptor  *encryptor
	middleware []Middleware
	running    bool
	mu         sync.RWMutex
	stopCh     chan struct{}
}

type WorkerConfig struct {
	Redis      RedisConfig
	Namespace  string
	Settings   WorkerSettings
	Encryption *EncryptionSettings
}

func NewWorker(cfg *WorkerConfig) (*Worker, error) {
	if cfg.Settings.Concurrency == 0 {
		cfg.Settings = DefaultWorkerSettings()
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
		enc, err := newEncryptor(cfg.Encryption.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to init encryptor: %w", err)
		}
		w.encryptor = enc

		mw, err := EncryptionMiddleware(cfg.Encryption.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to init encryption middleware: %w", err)
		}
		w.Use(mw)
	}

	w.Use(RecoveryMiddleware())

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

func (w *Worker) Register(jobType string, handler Handler, opts ...JobOption) {
	jobOpts := &JobOptions{
		MaxRetries:   25,
		RetryBackoff: DefaultBackoff(),
	}
	for _, opt := range opts {
		opt(jobOpts)
	}
	w.pool.Register(jobType, handler, jobOpts)
}

type JobOption func(*JobOptions)

func WithMaxRetries(n int) JobOption {
	return func(o *JobOptions) {
		o.MaxRetries = n
	}
}

func WithJobTimeout(d time.Duration) JobOption {
	return func(o *JobOptions) {
		o.Timeout = d
	}
}

func WithMaxConcurrency(n int) JobOption {
	return func(o *JobOptions) {
		o.MaxConcurrency = n
	}
}

func WithUniqueJob(key string, ttl time.Duration) JobOption {
	return func(o *JobOptions) {
		o.Unique = &UniqueConfig{
			Key: key,
			TTL: ttl,
		}
	}
}

func (w *Worker) Use(mw ...Middleware) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.middleware = append(w.middleware, mw...)
	w.pool.Use(mw...)
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

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), w.config.Settings.ShutdownTimeout)
	defer shutdownCancel()

	w.pool.Stop()
	wg.Wait()

	return w.pool.Drain(shutdownCtx)
}

func (w *Worker) Stop() {
	close(w.stopCh)
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

func (w *Worker) processJob(ctx context.Context, job *Job) {
	opts, err := w.pool.process(ctx, job)

	if err == nil {
		w.fetcher.Ack(ctx, w.id, job)
		w.updateBatchProgress(ctx, job, true)
		return
	}

	var retryErr *RetryableError
	if errors.As(err, &retryErr) {
		w.fetcher.Nack(ctx, w.id, job, retryErr.RetryIn)
		return
	}

	var maxRetriesErr *MaxRetriesExceededError
	if errors.As(err, &maxRetriesErr) {
		job.Error = maxRetriesErr.Error()
		w.fetcher.MoveToDead(ctx, w.id, job)
		w.updateBatchProgress(ctx, job, false)
		return
	}

	backoffFn := DefaultBackoff()
	maxRetries := job.Retry
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
		w.fetcher.Nack(ctx, w.id, job, backoff)
	} else {
		job.Error = err.Error()
		w.fetcher.MoveToDead(ctx, w.id, job)
		w.updateBatchProgress(ctx, job, false)
	}
}

func (w *Worker) updateBatchProgress(ctx context.Context, job *Job, success bool) {
	if job.BatchID == "" {
		return
	}

	w.redis.SRem(ctx, w.keys.BatchJobs(job.BatchID), job.ID)

	remaining, _ := w.redis.SCard(ctx, w.keys.BatchJobs(job.BatchID)).Result()
	if remaining > 0 {
		return
	}

	batchData, err := w.redis.Get(ctx, w.keys.Batch(job.BatchID)).Result()
	if err != nil {
		return
	}

	var batch Batch
	if err := json.Unmarshal([]byte(batchData), &batch); err != nil {
		return
	}

	if batch.OnComplete != "" {
		w.enqueueBatchCallback(ctx, batch.OnComplete, job.BatchID)
	}
}

func (w *Worker) enqueueBatchCallback(ctx context.Context, jobType, batchID string) {
	job := NewJob(jobType, map[string]any{
		"batch_id": batchID,
	})
	data, _ := job.Marshal()
	w.redis.LPush(ctx, w.keys.Queue("default"), string(data))
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
	ticker := time.NewTicker(5 * time.Second)
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
	now := float64(time.Now().Unix())

	for {
		items, err := w.redis.ZPopMin(ctx, w.keys.Scheduled(), 100).Result()
		if err != nil {
			return
		}
		if len(items) == 0 {
			return
		}

		for _, z := range items {
			if z.Score > now {
				w.redis.ZAdd(ctx, w.keys.Scheduled(), z)
				continue
			}

			data, ok := z.Member.(string)
			if !ok {
				continue
			}

			var job Job
			if err := json.Unmarshal([]byte(data), &job); err != nil {
				continue
			}

			w.redis.LPush(ctx, w.keys.Queue(job.Queue), data)
		}
	}
}

func (w *Worker) enqueueRetries(ctx context.Context) {
	now := float64(time.Now().Unix())

	for {
		items, err := w.redis.ZPopMin(ctx, w.keys.Retry(), 100).Result()
		if err != nil {
			return
		}
		if len(items) == 0 {
			return
		}

		for _, z := range items {
			if z.Score > now {
				w.redis.ZAdd(ctx, w.keys.Retry(), z)
				continue
			}

			data, ok := z.Member.(string)
			if !ok {
				continue
			}

			var job Job
			if err := json.Unmarshal([]byte(data), &job); err != nil {
				continue
			}

			w.redis.LPush(ctx, w.keys.Queue(job.Queue), data)
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
	keys, err := w.redis.Keys(ctx, pattern).Result()
	if err != nil {
		return
	}

	for _, key := range keys {
		workerID := key[len(w.keys.InFlight(""))+1:]
		if activeWorkers[workerID] {
			continue
		}

		jobs, err := w.redis.LRange(ctx, key, 0, -1).Result()
		if err != nil {
			continue
		}

		for _, data := range jobs {
			var job Job
			if err := json.Unmarshal([]byte(data), &job); err != nil {
				continue
			}
			w.redis.LPush(ctx, w.keys.Queue(job.Queue), data)
		}

		w.redis.Del(ctx, key)
	}
}
