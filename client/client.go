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

// enqueuedJob pairs a job with its marshaled data for bulk operations.
type enqueuedJob struct {
	job  *senna.Job
	data []byte
}

// EnqueueBulk enqueues multiple jobs of the same type in a single Redis round trip.
// All jobs share the same options (queue, retry, etc.) but have different arguments.
// Returns only the jobs that were successfully enqueued. Jobs that fail to marshal
// or encrypt are silently skipped.
func (c *Client) EnqueueBulk(ctx context.Context, jobType string, argsList []map[string]any, opts ...EnqueueOption) ([]*senna.Job, error) {
	if len(argsList) == 0 {
		return nil, nil
	}

	cfg := &enqueueConfig{
		queue: c.settings.DefaultQueue,
		retry: c.settings.DefaultRetry,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	// Unique keys are not supported in bulk operations
	if cfg.uniqueKey != "" {
		return nil, fmt.Errorf("unique keys are not supported in bulk enqueue")
	}

	// Build and marshal all jobs upfront, only keeping those that succeed
	enqueued := make([]enqueuedJob, 0, len(argsList))
	for _, args := range argsList {
		job := senna.NewJob(jobType, args)
		job.Queue = cfg.queue
		job.Retry = cfg.retry
		job.BatchID = cfg.batchID

		if cfg.encrypt && c.encryptor != nil {
			encryptedArgs, err := c.encryptor.Encrypt(args)
			if err != nil {
				continue
			}
			job.Args = encryptedArgs
			job.Encrypted = true
		}

		data, err := job.Marshal()
		if err != nil {
			continue
		}

		enqueued = append(enqueued, enqueuedJob{job: job, data: data})
	}

	if len(enqueued) == 0 {
		return nil, nil
	}

	// Determine if we're scheduling or enqueueing immediately
	var scheduleAt time.Time
	if !cfg.at.IsZero() {
		scheduleAt = cfg.at
	} else if cfg.delay > 0 {
		scheduleAt = time.Now().Add(cfg.delay)
	}

	// Use pipeline for efficiency
	pipe := c.redis.Pipeline()

	if scheduleAt.IsZero() {
		// Immediate enqueue - group jobs by queue
		jobsByQueue := make(map[string][]string)
		for _, ej := range enqueued {
			jobsByQueue[ej.job.Queue] = append(jobsByQueue[ej.job.Queue], string(ej.data))
		}

		// Add all queues to the queues set
		for queue := range jobsByQueue {
			pipe.SAdd(ctx, c.keys.Queues(), queue)
		}

		// Push jobs to their respective queues
		for queue, jobDataList := range jobsByQueue {
			args := make([]any, len(jobDataList))
			for i, d := range jobDataList {
				args[i] = d
			}
			pipe.LPush(ctx, c.keys.Queue(queue), args...)
		}
	} else {
		// Scheduled enqueue
		score := float64(scheduleAt.Unix())
		members := make([]redis.Z, len(enqueued))
		for i, ej := range enqueued {
			members[i] = redis.Z{
				Score:  score,
				Member: string(ej.data),
			}
		}
		pipe.ZAdd(ctx, c.keys.Scheduled(), members...)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}

	// Extract only the successfully enqueued jobs
	jobs := make([]*senna.Job, len(enqueued))
	for i, ej := range enqueued {
		jobs[i] = ej.job
	}

	return jobs, nil
}

// EnqueueBulkIn schedules multiple jobs to run after a delay.
func (c *Client) EnqueueBulkIn(ctx context.Context, d time.Duration, jobType string, argsList []map[string]any, opts ...EnqueueOption) ([]*senna.Job, error) {
	opts = append(opts, WithDelay(d))
	return c.EnqueueBulk(ctx, jobType, argsList, opts...)
}

// EnqueueBulkAt schedules multiple jobs to run at a specific time.
func (c *Client) EnqueueBulkAt(ctx context.Context, t time.Time, jobType string, argsList []map[string]any, opts ...EnqueueOption) ([]*senna.Job, error) {
	opts = append(opts, WithScheduleAt(t))
	return c.EnqueueBulk(ctx, jobType, argsList, opts...)
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

	if batch.ParentID != "" && batch.ParentID == batch.ID {
		return fmt.Errorf("batch cannot be its own parent")
	}

	const batchTTL = 30 * 24 * time.Hour

	if batch.ParentID != "" {
		if _, err := c.redis.Get(ctx, c.keys.Batch(batch.ParentID)).Result(); err != nil {
			if err == redis.Nil {
				return &senna.BatchNotFoundError{BatchID: batch.ParentID}
			}
			return err
		}
	}

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
		ParentID:      batch.ParentID,
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

	if batch.ParentID != "" {
		keys := []string{
			c.keys.Batch(batch.ParentID),
			c.keys.BatchJobs(batch.ParentID),
		}

		result, err := batchAddChildScript.Run(ctx, c.redis, keys, batch.ID)
		if err != nil {
			return fmt.Errorf("failed to add child batch to parent: %w", err)
		}

		var addResult struct {
			Error   string `json:"error,omitempty"`
			Success bool   `json:"success"`
		}
		if err := json.Unmarshal([]byte(result.(string)), &addResult); err != nil {
			return fmt.Errorf("failed to parse batch add child result: %w", err)
		}

		if addResult.Error != "" {
			switch addResult.Error {
			case "batch_not_found":
				return &senna.BatchNotFoundError{BatchID: batch.ParentID}
			case "batch_invalidated":
				return fmt.Errorf("parent batch %s has been invalidated", batch.ParentID)
			case "batch_complete":
				return fmt.Errorf("parent batch %s has already completed", batch.ParentID)
			default:
				return fmt.Errorf("parent batch error: %s", addResult.Error)
			}
		}
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
		c.enqueueBatchCallback(ctx, batch.OnComplete.JobType, batch.ID, batch.ParentID, batch.OnComplete.Options, queue)
	}

	// OnSuccess fires for empty batches (no jobs = no failures)
	if batch.OnSuccess != nil {
		c.enqueueBatchCallback(ctx, batch.OnSuccess.JobType, batch.ID, batch.ParentID, batch.OnSuccess.Options, queue)
	}
}

func (c *Client) enqueueBatchCallback(ctx context.Context, jobType, batchID, parentID string, options map[string]any, queue string) {
	args := map[string]any{
		"batch_id": batchID,
	}
	if parentID != "" {
		args["parent_id"] = parentID
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

// CancelIteration marks an iterable job for cancellation.
// The job will stop after its current item and complete without calling OnComplete.
// If the iteration state doesn't exist yet (job hasn't started or hasn't saved),
// a minimal cancelled state is created so the cancellation is honored when the job runs.
func (c *Client) CancelIteration(ctx context.Context, jobID string) error {
	key := c.keys.IterationState(jobID)
	ttl := 30 * 24 * time.Hour // Default 30 days

	// Load current state
	data, err := c.redis.Get(ctx, key).Result()
	if err != nil {
		if err.Error() == "redis: nil" {
			// State doesn't exist yet - create minimal cancelled state
			state := senna.IterationState{
				JobID:     jobID,
				Cancelled: true,
			}
			newData, err := json.Marshal(state)
			if err != nil {
				return err
			}
			return c.redis.Set(ctx, key, string(newData), ttl).Err()
		}
		return err
	}

	var state senna.IterationState
	if err := json.Unmarshal([]byte(data), &state); err != nil {
		return err
	}

	// Mark as cancelled
	state.Cancelled = true

	// Save back with same TTL
	newData, err := json.Marshal(state)
	if err != nil {
		return err
	}

	// Get remaining TTL and use it
	existingTTL, err := c.redis.TTL(ctx, key).Result()
	if err == nil && existingTTL > 0 {
		ttl = existingTTL
	}

	return c.redis.Set(ctx, key, string(newData), ttl).Err()
}

// IterationStatus returns the current state of an iterable job.
// Returns nil if no iteration state exists for the job.
func (c *Client) IterationStatus(ctx context.Context, jobID string) (*senna.IterationState, error) {
	key := c.keys.IterationState(jobID)

	data, err := c.redis.Get(ctx, key).Result()
	if err != nil {
		if err.Error() == "redis: nil" {
			return nil, nil
		}
		return nil, err
	}

	var state senna.IterationState
	if err := json.Unmarshal([]byte(data), &state); err != nil {
		return nil, err
	}

	return &state, nil
}
