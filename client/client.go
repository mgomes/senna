package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/mgomes/senna"
	"github.com/mgomes/senna/internal/batch"
	"github.com/mgomes/senna/internal/encryption"
	"github.com/mgomes/senna/internal/iteration"
	"github.com/mgomes/senna/internal/keys"
	"github.com/redis/go-redis/v9"
)

// Validation errors returned by enqueue and batch operations. They are exported
// so callers can match them with errors.Is rather than comparing message text.
var (
	// ErrUniqueTTLRequired indicates a unique key was supplied without a positive TTL.
	ErrUniqueTTLRequired = errors.New("unique TTL must be > 0 when using a unique key")
	// ErrUniqueKeyInBulk indicates a unique key was supplied to a bulk enqueue, which is unsupported.
	ErrUniqueKeyInBulk = errors.New("unique keys are not supported in bulk enqueue")
	// ErrBatchSelfParent indicates a batch was configured as its own parent.
	ErrBatchSelfParent = errors.New("batch cannot be its own parent")
)

// Client enqueues jobs and exposes batch and iteration helpers.
type Client struct {
	redis     *redis.Client
	keys      *keys.Keys
	settings  Settings
	encryptor *encryption.Encryptor
}

// Config configures a Client.
type Config struct {
	Redis      senna.RedisConfig
	Namespace  string
	Settings   Settings
	Encryption *senna.EncryptionSettings
}

// Settings configures enqueue defaults for a Client. It aliases
// senna.ClientSettings so the two stay in lockstep.
type Settings = senna.ClientSettings

// DefaultSettings returns the default client settings.
func DefaultSettings() Settings {
	return senna.DefaultClientSettings()
}

func normalizeSettings(settings Settings) Settings {
	if settings == (Settings{}) {
		return DefaultSettings()
	}

	if settings.DefaultQueue == "" {
		settings.DefaultQueue = senna.DefaultQueueName
	}

	return settings
}

// New creates a Client and verifies the Redis connection.
func New(cfg *Config) (*Client, error) {
	cfg.Settings = normalizeSettings(cfg.Settings)

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

	c := &Client{
		redis:     client,
		keys:      keys.New(cfg.Namespace),
		settings:  cfg.Settings,
		encryptor: enc,
	}

	return c, nil
}

// Close closes the underlying Redis client.
func (c *Client) Close() error {
	return c.redis.Close()
}

// Redis returns the underlying Redis client.
func (c *Client) Redis() *redis.Client {
	return c.redis
}

// EnqueueOption configures how a job is enqueued.
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

// WithQueue enqueues the job onto the provided queue.
func WithQueue(q string) EnqueueOption {
	return func(c *enqueueConfig) {
		c.queue = q
	}
}

// WithRetry sets the retry count for the enqueued job.
func WithRetry(n int) EnqueueOption {
	return func(c *enqueueConfig) {
		c.retry = n
	}
}

// WithUniqueKey deduplicates jobs by key for the provided TTL.
func WithUniqueKey(key string, ttl time.Duration) EnqueueOption {
	return func(c *enqueueConfig) {
		c.uniqueKey = key
		c.uniqueTTL = ttl
	}
}

// WithBatch associates the enqueued job with a batch ID.
func WithBatch(batchID string) EnqueueOption {
	return func(c *enqueueConfig) {
		c.batchID = batchID
	}
}

// WithEncryption encrypts job arguments when client encryption is enabled.
func WithEncryption() EnqueueOption {
	return func(c *enqueueConfig) {
		c.encrypt = true
	}
}

// WithDelay schedules the job to run after the provided delay.
func WithDelay(d time.Duration) EnqueueOption {
	return func(c *enqueueConfig) {
		c.delay = d
	}
}

// WithScheduleAt schedules the job to run at the provided time.
func WithScheduleAt(t time.Time) EnqueueOption {
	return func(c *enqueueConfig) {
		c.at = t
	}
}

// Enqueue creates a job and enqueues it immediately or at a scheduled time.
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
			return nil, ErrUniqueTTLRequired
		}
		status, err := c.redis.SetArgs(ctx, c.keys.Unique(cfg.uniqueKey), job.ID, redis.SetArgs{
			Mode: "NX",
			TTL:  cfg.uniqueTTL,
		}).Result()
		if errors.Is(err, redis.Nil) {
			return nil, &senna.DuplicateJobError{UniqueKey: cfg.uniqueKey}
		}
		if err != nil {
			return nil, err
		}
		if status != "OK" {
			return nil, fmt.Errorf("unexpected unique job lock status: %q", status)
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

// EnqueueIn schedules a job to run after the provided delay.
func (c *Client) EnqueueIn(ctx context.Context, d time.Duration, jobType string, args map[string]any, opts ...EnqueueOption) (*senna.Job, error) {
	opts = append(opts, WithDelay(d))
	return c.Enqueue(ctx, jobType, args, opts...)
}

// EnqueueAt schedules a job to run at the provided time.
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
		return nil, ErrUniqueKeyInBulk
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
		return nil, fmt.Errorf("enqueue %d bulk jobs of type %s: %w", len(enqueued), jobType, err)
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
		return nil, fmt.Errorf("marshal job %s: %w", job.ID, err)
	}

	if err := c.redis.SAdd(ctx, c.keys.Queues(), job.Queue).Err(); err != nil {
		return nil, fmt.Errorf("register queue %s: %w", job.Queue, err)
	}

	if err := c.redis.LPush(ctx, c.keys.Queue(job.Queue), string(data)).Err(); err != nil {
		return nil, fmt.Errorf("enqueue job %s to queue %s: %w", job.ID, job.Queue, err)
	}

	return job, nil
}

func (c *Client) enqueueAt(ctx context.Context, t time.Time, job *senna.Job) (*senna.Job, error) {
	data, err := job.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshal job %s: %w", job.ID, err)
	}

	if err := c.redis.ZAdd(ctx, c.keys.Scheduled(), redis.Z{
		Score:  float64(t.Unix()),
		Member: string(data),
	}).Err(); err != nil {
		return nil, fmt.Errorf("schedule job %s: %w", job.ID, err)
	}

	return job, nil
}

// EnqueueBatch stores batch state and enqueues the batch's jobs.
func (c *Client) EnqueueBatch(ctx context.Context, b *Batch) error {
	// Validation
	if b.err != nil {
		return b.err
	}
	if b.ParentID != "" && b.ParentID == b.ID {
		return ErrBatchSelfParent
	}
	if b.ParentID != "" {
		if _, err := c.redis.Get(ctx, c.keys.Batch(b.ParentID)).Result(); err != nil {
			if errors.Is(err, redis.Nil) {
				return &senna.BatchNotFoundError{BatchID: b.ParentID}
			}
			return err
		}
	}

	emptyBatch := len(b.Jobs) == 0
	callbackQueue := b.CallbackQueue
	if callbackQueue == "" {
		callbackQueue = c.settings.DefaultQueue
	}

	// Pre-marshal jobs to fail fast before any Redis changes
	marshaledJobs, err := c.marshalBatchJobs(b)
	if err != nil {
		return err
	}

	// Build and store batch state
	state := c.buildBatchState(b, callbackQueue)

	// Step 1: Store batch state
	if err := c.storeBatchState(ctx, b.ID, state); err != nil {
		return err
	}

	// Step 2: Link to parent (if applicable)
	if b.ParentID != "" {
		if err := c.linkBatchToParent(ctx, b); err != nil {
			return err
		}
	}

	// Step 3: Enqueue jobs
	if err := c.enqueueBatchJobs(ctx, b, marshaledJobs); err != nil {
		return err
	}

	// Handle empty batches
	if emptyBatch {
		if err := c.handleEmptyBatch(ctx, b); err != nil {
			if b.ParentID != "" {
				c.rollbackParentLink(ctx, b.ParentID, b.ID)
			}
			c.cleanupOrphanedBatch(ctx, b.ID)
			return err
		}
	}

	return nil
}

// propagateBatchCompletion notifies a parent batch that a child has completed.
func (c *Client) propagateBatchCompletion(ctx context.Context, childBatchID, parentID string, resultType batch.Result) error {
	scriptKeys := batch.CompletionKeys(c.keys, parentID)
	scriptArgs := batch.CompletionArgs(c.keys, childBatchID, resultType)

	var result batch.CompleteResult
	if err := batchCompleteScript.RunJSON(ctx, c.redis, &result, scriptKeys, scriptArgs...); err != nil {
		return fmt.Errorf("failed to propagate batch completion to parent %s: %w", parentID, err)
	}

	if result.Error != "" || result.AlreadyProcessed {
		if result.Error == "" {
			return nil
		}
		if result.Error == batch.ErrCodeNotFound {
			return &senna.BatchNotFoundError{BatchID: parentID}
		}
		return fmt.Errorf("parent batch %s completion failed: %s", parentID, result.Error)
	}

	grandparentResult, ok := batch.ParentResultType(&result)
	if ok {
		if err := c.propagateBatchCompletion(ctx, parentID, result.ParentID, grandparentResult); err != nil {
			slog.WarnContext(ctx, "failed to propagate batch completion to ancestor",
				"batch_id", parentID,
				"parent_id", result.ParentID,
				"error", err,
			)
		}
	}

	return nil
}

// cleanupOrphanedBatch removes a batch that failed to link to its parent.
// This prevents orphaned batches from accumulating when parent-linking fails.
// Jobs already enqueued will still run but their batch updates will be no-ops
// since the batch state is deleted.
func (c *Client) cleanupOrphanedBatch(ctx context.Context, batchID string) {
	pipe := c.redis.Pipeline()
	pipe.Del(ctx, c.keys.Batch(batchID))
	pipe.SRem(ctx, c.keys.Batches(), batchID)
	pipe.Del(ctx, c.keys.BatchJobs(batchID))
	pipe.Del(ctx, c.keys.BatchFailed(batchID))
	pipe.Del(ctx, c.keys.BatchCallbacks(batchID))
	if _, err := pipe.Exec(ctx); err != nil {
		slog.WarnContext(ctx, "failed to cleanup orphaned batch", "batch_id", batchID, "error", err)
	}
}

// rollbackParentLink undoes the effect of batch_add_child, decrementing
// the parent's pending count and removing the child from its jobs set.
func (c *Client) rollbackParentLink(ctx context.Context, parentID, childID string) {
	keys := []string{
		c.keys.Batch(parentID),
		c.keys.BatchJobs(parentID),
	}
	if _, err := batchRemoveChildScript.Run(ctx, c.redis, keys, childID); err != nil {
		slog.WarnContext(ctx, "failed to rollback parent link", "parent_id", parentID, "child_id", childID, "error", err)
	}
}

// marshaledJob holds a job and its serialized form.
type marshaledJob struct {
	job  *senna.Job
	data []byte
}

// marshalBatchJobs pre-marshals all jobs to fail fast before any Redis changes.
func (c *Client) marshalBatchJobs(b *Batch) ([]marshaledJob, error) {
	jobs := make([]marshaledJob, 0, len(b.Jobs))
	for _, job := range b.Jobs {
		job.BatchID = b.ID
		data, err := job.Marshal()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal job %s: %w", job.ID, err)
		}
		jobs = append(jobs, marshaledJob{job: job, data: data})
	}
	return jobs, nil
}

// buildBatchState constructs the BatchState from a Batch.
func (c *Client) buildBatchState(b *Batch, callbackQueue string) *senna.BatchState {
	state := &senna.BatchState{
		ID:            b.ID,
		Description:   b.Description,
		ParentID:      b.ParentID,
		Total:         len(b.Jobs),
		Pending:       len(b.Jobs),
		Failures:      0,
		Successes:     0,
		Dead:          false,
		DeathFired:    false,
		CompleteFired: false,
		SuccessFired:  false,
		CreatedAt:     b.CreatedAt,
		CallbackQueue: callbackQueue,
	}

	if b.OnComplete != nil {
		state.OnComplete = &senna.CallbackInfo{
			JobType: b.OnComplete.JobType,
			Options: b.OnComplete.Options,
		}
	}
	if b.OnSuccess != nil {
		state.OnSuccess = &senna.CallbackInfo{
			JobType: b.OnSuccess.JobType,
			Options: b.OnSuccess.Options,
		}
	}
	if b.OnDeath != nil {
		state.OnDeath = &senna.CallbackInfo{
			JobType: b.OnDeath.JobType,
			Options: b.OnDeath.Options,
		}
	}

	return state
}

// storeBatchState saves the batch state to Redis (Step 1).
func (c *Client) storeBatchState(ctx context.Context, batchID string, state *senna.BatchState) error {
	batchData, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal batch state %s: %w", batchID, err)
	}

	pipe := c.redis.Pipeline()
	pipe.Set(ctx, c.keys.Batch(batchID), string(batchData), batch.BatchTTL)
	pipe.SAdd(ctx, c.keys.Batches(), batchID)

	if _, err = pipe.Exec(ctx); err != nil {
		return fmt.Errorf("store batch state %s: %w", batchID, err)
	}
	return nil
}

// linkBatchToParent links a child batch to its parent (Step 2).
func (c *Client) linkBatchToParent(ctx context.Context, b *Batch) error {
	scriptKeys := []string{
		c.keys.Batch(b.ParentID),
		c.keys.BatchJobs(b.ParentID),
	}

	var addResult struct {
		Error   string `json:"error,omitempty"`
		Success bool   `json:"success"`
	}
	if err := batchAddChildScript.RunJSON(ctx, c.redis, &addResult, scriptKeys, b.ID); err != nil {
		c.cleanupOrphanedBatch(ctx, b.ID)
		return fmt.Errorf("failed to add child batch to parent: %w", err)
	}

	if addResult.Error != "" {
		c.cleanupOrphanedBatch(ctx, b.ID)

		switch addResult.Error {
		case batch.ErrCodeNotFound:
			return &senna.BatchNotFoundError{BatchID: b.ParentID}
		case batch.ErrCodeInvalidated:
			return fmt.Errorf("parent batch %s has been invalidated", b.ParentID)
		case batch.ErrCodeComplete:
			return fmt.Errorf("parent batch %s has already completed", b.ParentID)
		default:
			return fmt.Errorf("parent batch error: %s", addResult.Error)
		}
	}

	return nil
}

// enqueueBatchJobs enqueues all jobs for a batch (Step 3).
func (c *Client) enqueueBatchJobs(ctx context.Context, b *Batch, jobs []marshaledJob) error {
	if len(jobs) == 0 {
		return nil
	}

	pipe := c.redis.Pipeline()
	for _, mj := range jobs {
		pipe.LPush(ctx, c.keys.Queue(mj.job.Queue), string(mj.data))
		pipe.SAdd(ctx, c.keys.BatchJobs(b.ID), mj.job.ID)
	}
	pipe.Expire(ctx, c.keys.BatchJobs(b.ID), batch.BatchTTL)

	if _, err := pipe.Exec(ctx); err != nil {
		// Rollback: undo parent link and clean up batch state
		if b.ParentID != "" {
			c.rollbackParentLink(ctx, b.ParentID, b.ID)
		}
		c.cleanupOrphanedBatch(ctx, b.ID)
		return err
	}
	return nil
}

// handleEmptyBatch marks an empty batch complete and enqueues callbacks atomically.
func (c *Client) handleEmptyBatch(ctx context.Context, b *Batch) error {
	scriptKeys := batch.CompletionKeys(c.keys, b.ID)
	scriptArgs := batch.CompletionArgs(c.keys, b.ID, batch.ResultEmptySuccess)

	var result batch.CompleteResult
	if err := batchCompleteScript.RunJSON(ctx, c.redis, &result, scriptKeys, scriptArgs...); err != nil {
		return fmt.Errorf("failed to complete empty batch %s: %w", b.ID, err)
	}

	if result.Error != "" {
		return fmt.Errorf("empty batch %s completion failed: %s", b.ID, result.Error)
	}
	if result.AlreadyProcessed {
		return nil
	}

	parentResultType, ok := batch.ParentResultType(&result)
	if !ok {
		return nil
	}

	if err := c.propagateBatchCompletion(ctx, b.ID, result.ParentID, parentResultType); err != nil {
		return err
	}
	return nil
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
	return iteration.Cancel(ctx, c.redis, key, jobID, iteration.StateTTL)
}

// IterationStatus returns the current state of an iterable job.
// Returns nil if no iteration state exists for the job.
func (c *Client) IterationStatus(ctx context.Context, jobID string) (*senna.IterationState, error) {
	key := c.keys.IterationState(jobID)
	return iteration.Load(ctx, c.redis, key)
}
