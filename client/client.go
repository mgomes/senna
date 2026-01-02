package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mgomes/senna"
	"github.com/mgomes/senna/internal/encryption"
	"github.com/mgomes/senna/internal/keys"
	"github.com/redis/go-redis/v9"
)

type Client struct {
	redis     *redis.Client
	keys      *keys.Keys
	settings  Settings
	encryptor *encryption.Encryptor
}

type Config struct {
	Redis      senna.RedisConfig
	Namespace  string
	Settings   Settings
	Encryption *senna.EncryptionSettings
}

type Settings struct {
	DefaultQueue string
	DefaultRetry int
}

func DefaultSettings() Settings {
	return Settings{
		DefaultQueue: "default",
		DefaultRetry: 25,
	}
}

func New(cfg *Config) (*Client, error) {
	if cfg.Settings.DefaultQueue == "" {
		cfg.Settings = DefaultSettings()
	}

	client := redis.NewClient(cfg.Redis.Options())

	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, err
	}

	c := &Client{
		redis:    client,
		keys:     keys.New(cfg.Namespace),
		settings: cfg.Settings,
	}

	if cfg.Encryption != nil && cfg.Encryption.Enabled {
		enc, err := encryption.New(cfg.Encryption.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to init encryptor: %w", err)
		}
		c.encryptor = enc
	}

	return c, nil
}

func (c *Client) Close() error {
	return c.redis.Close()
}

func (c *Client) Redis() *redis.Client {
	return c.redis
}

type EnqueueOption func(*enqueueConfig)

type enqueueConfig struct {
	queue     string
	retry     int
	uniqueKey string
	uniqueTTL time.Duration
	batchID   string
	encrypt   bool
	delay     time.Duration
	at        time.Time
}

func WithQueue(q string) EnqueueOption {
	return func(c *enqueueConfig) {
		c.queue = q
	}
}

func WithRetry(n int) EnqueueOption {
	return func(c *enqueueConfig) {
		c.retry = n
	}
}

func WithUniqueKey(key string, ttl time.Duration) EnqueueOption {
	return func(c *enqueueConfig) {
		c.uniqueKey = key
		c.uniqueTTL = ttl
	}
}

func WithBatch(batchID string) EnqueueOption {
	return func(c *enqueueConfig) {
		c.batchID = batchID
	}
}

func WithEncryption() EnqueueOption {
	return func(c *enqueueConfig) {
		c.encrypt = true
	}
}

func WithDelay(d time.Duration) EnqueueOption {
	return func(c *enqueueConfig) {
		c.delay = d
	}
}

func WithScheduleAt(t time.Time) EnqueueOption {
	return func(c *enqueueConfig) {
		c.at = t
	}
}

func (c *Client) Enqueue(ctx context.Context, jobType string, args map[string]any, opts ...EnqueueOption) (*senna.Job, error) {
	cfg := &enqueueConfig{
		queue: c.settings.DefaultQueue,
		retry: c.settings.DefaultRetry,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	job := senna.NewJob(jobType, args)
	job.Queue = cfg.queue
	job.Retry = cfg.retry
	job.BatchID = cfg.batchID
	job.UniqueKey = cfg.uniqueKey
	job.UniqueTTL = cfg.uniqueTTL

	if cfg.encrypt && c.encryptor != nil {
		encryptedArgs, err := c.encryptor.Encrypt(args)
		if err != nil {
			return nil, err
		}
		job.Args = encryptedArgs
		job.Encrypted = true
	}

	if cfg.uniqueKey != "" {
		if cfg.uniqueTTL <= 0 {
			return nil, fmt.Errorf("unique TTL must be > 0 when using a unique key")
		}
		ok, err := c.redis.SetNX(ctx, c.keys.Unique(cfg.uniqueKey), job.ID, cfg.uniqueTTL).Result()
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, &senna.DuplicateJobError{UniqueKey: cfg.uniqueKey}
		}
	}

	if !cfg.at.IsZero() {
		return c.enqueueAt(ctx, cfg.at, job)
	}
	if cfg.delay > 0 {
		return c.enqueueAt(ctx, time.Now().Add(cfg.delay), job)
	}

	return c.enqueueNow(ctx, job)
}

func (c *Client) EnqueueIn(ctx context.Context, d time.Duration, jobType string, args map[string]any, opts ...EnqueueOption) (*senna.Job, error) {
	opts = append(opts, WithDelay(d))
	return c.Enqueue(ctx, jobType, args, opts...)
}

func (c *Client) EnqueueAt(ctx context.Context, t time.Time, jobType string, args map[string]any, opts ...EnqueueOption) (*senna.Job, error) {
	opts = append(opts, WithScheduleAt(t))
	return c.Enqueue(ctx, jobType, args, opts...)
}

func (c *Client) enqueueNow(ctx context.Context, job *senna.Job) (*senna.Job, error) {
	data, err := job.Marshal()
	if err != nil {
		return nil, err
	}

	if err := c.redis.SAdd(ctx, c.keys.Queues(), job.Queue).Err(); err != nil {
		return nil, err
	}

	if err := c.redis.LPush(ctx, c.keys.Queue(job.Queue), string(data)).Err(); err != nil {
		return nil, err
	}

	return job, nil
}

func (c *Client) enqueueAt(ctx context.Context, t time.Time, job *senna.Job) (*senna.Job, error) {
	data, err := job.Marshal()
	if err != nil {
		return nil, err
	}

	if err := c.redis.ZAdd(ctx, c.keys.Scheduled(), redis.Z{
		Score:  float64(t.Unix()),
		Member: string(data),
	}).Err(); err != nil {
		return nil, err
	}

	return job, nil
}

func (c *Client) EnqueueBatch(ctx context.Context, batch *Batch) error {
	if batch.err != nil {
		return batch.err
	}

	const batchTTL = 30 * 24 * time.Hour

	pipe := c.redis.Pipeline()

	// For empty batches, mark callbacks as already fired since we'll enqueue them immediately
	emptyBatch := len(batch.Jobs) == 0

	// Use client's default queue if batch doesn't specify a callback queue
	callbackQueue := batch.CallbackQueue
	if callbackQueue == "" {
		callbackQueue = c.settings.DefaultQueue
	}

	// Build batch state for tracking
	state := &senna.BatchState{
		ID:            batch.ID,
		Description:   batch.Description,
		Total:         len(batch.Jobs),
		Pending:       len(batch.Jobs),
		Failures:      0,
		Successes:     0,
		Dead:          false,
		DeathFired:    false,
		CompleteFired: emptyBatch,
		SuccessFired:  emptyBatch,
		CreatedAt:     batch.CreatedAt,
		CallbackQueue: callbackQueue,
	}

	if batch.OnComplete != nil {
		state.OnComplete = &senna.CallbackInfo{
			JobType: batch.OnComplete.JobType,
			Options: batch.OnComplete.Options,
		}
	}
	if batch.OnSuccess != nil {
		state.OnSuccess = &senna.CallbackInfo{
			JobType: batch.OnSuccess.JobType,
			Options: batch.OnSuccess.Options,
		}
	}
	if batch.OnDeath != nil {
		state.OnDeath = &senna.CallbackInfo{
			JobType: batch.OnDeath.JobType,
			Options: batch.OnDeath.Options,
		}
	}

	batchData, err := json.Marshal(state)
	if err != nil {
		return err
	}

	// Store batch state with 30 day expiration (like Sidekiq)
	pipe.Set(ctx, c.keys.Batch(batch.ID), string(batchData), batchTTL)

	// Add to batches set for iteration
	pipe.SAdd(ctx, c.keys.Batches(), batch.ID)

	for _, job := range batch.Jobs {
		job.BatchID = batch.ID

		data, err := job.Marshal()
		if err != nil {
			return err
		}

		pipe.LPush(ctx, c.keys.Queue(job.Queue), string(data))
		pipe.SAdd(ctx, c.keys.BatchJobs(batch.ID), job.ID)
	}

	// Ensure batch job/failed sets expire alongside the batch state
	pipe.Expire(ctx, c.keys.BatchJobs(batch.ID), batchTTL)
	pipe.Expire(ctx, c.keys.BatchFailed(batch.ID), batchTTL)

	_, err = pipe.Exec(ctx)
	if err != nil {
		return err
	}

	// For empty batches, immediately enqueue callbacks
	if emptyBatch {
		c.enqueueEmptyBatchCallbacks(ctx, batch, callbackQueue)
	}

	return nil
}

// enqueueEmptyBatchCallbacks enqueues callbacks for empty batches immediately.
func (c *Client) enqueueEmptyBatchCallbacks(ctx context.Context, batch *Batch, queue string) {
	// OnComplete always fires for empty batches
	if batch.OnComplete != nil {
		c.enqueueBatchCallback(ctx, batch.OnComplete.JobType, batch.ID, batch.OnComplete.Options, queue)
	}

	// OnSuccess fires for empty batches (no jobs = no failures)
	if batch.OnSuccess != nil {
		c.enqueueBatchCallback(ctx, batch.OnSuccess.JobType, batch.ID, batch.OnSuccess.Options, queue)
	}
}

func (c *Client) enqueueBatchCallback(ctx context.Context, jobType, batchID string, options map[string]any, queue string) {
	args := map[string]any{
		"batch_id": batchID,
	}
	for k, v := range options {
		args[k] = v
	}

	job := senna.NewJob(jobType, args)
	job.Queue = queue
	data, _ := job.Marshal()
	c.redis.LPush(ctx, c.keys.Queue(queue), string(data))
}

// BatchStatus returns the status of a batch.
func (c *Client) BatchStatus(bid string) *senna.BatchStatus {
	return senna.NewBatchStatus(c.redis, c.keys.Namespace(), bid)
}
