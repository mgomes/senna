package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mgomes/senna"
	"github.com/mgomes/senna/internal/batch"
	"github.com/mgomes/senna/internal/encryption"
	"github.com/mgomes/senna/internal/keys"
	"github.com/redis/go-redis/v9"
)

const batchTTL = 30 * 24 * time.Hour

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

func (c *Client) EnqueueBatch(ctx context.Context, b *Batch) error {
	// Validation
	if b.err != nil {
		return b.err
	}
	if b.ParentID != "" && b.ParentID == b.ID {
		return fmt.Errorf("batch cannot be its own parent")
	}
	if b.ParentID != "" {
		if _, err := c.redis.Get(ctx, c.keys.Batch(b.ParentID)).Result(); err != nil {
			if err == redis.Nil {
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
	state := c.buildBatchState(b, callbackQueue, emptyBatch)

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
		c.handleEmptyBatch(ctx, b, callbackQueue)
	}

	return nil
}

// enqueueEmptyBatchCallbacks enqueues callbacks for empty batches immediately.
func (c *Client) enqueueEmptyBatchCallbacks(ctx context.Context, b *Batch, queue string) {
	// OnComplete always fires for empty batches
	if b.OnComplete != nil {
		batch.EnqueueCallback(ctx, c.redis, c.keys, b.OnComplete.JobType, b.ID, b.ParentID, b.OnComplete.Options, queue, batchTTL)
	}

	// OnSuccess fires for empty batches (no jobs = no failures)
	if b.OnSuccess != nil {
		batch.EnqueueCallback(ctx, c.redis, c.keys, b.OnSuccess.JobType, b.ID, b.ParentID, b.OnSuccess.Options, queue, batchTTL)
	}
}

// propagateEmptyChildBatch notifies the parent batch that an empty child batch
// with no callbacks has completed. This is necessary because empty batches with
// no callbacks have no jobs to process and no callbacks to trigger completion.
func (c *Client) propagateEmptyChildBatch(ctx context.Context, childBatchID, parentID, queue string) {
	c.propagateBatchCompletion(ctx, childBatchID, parentID, "success", queue)
}

// propagateBatchCompletion notifies a parent batch that a child has completed.
// The resultType should be "success", "death", or "invalidated".
func (c *Client) propagateBatchCompletion(ctx context.Context, childBatchID, parentID, resultType, queue string) {
	scriptKeys := []string{
		c.keys.Batch(parentID),
		c.keys.BatchJobs(parentID),
		c.keys.BatchFailed(parentID),
		c.keys.DeadBatches(),
	}

	resultJSON, err := batchCompleteScript.Run(ctx, c.redis, scriptKeys, childBatchID, resultType)
	if err != nil {
		return
	}

	var result batch.CompleteResult
	if err := json.Unmarshal([]byte(resultJSON.(string)), &result); err != nil {
		return
	}

	if result.Error != "" || result.AlreadyProcessed {
		return
	}

	callbackQueue := result.CallbackQueue
	if callbackQueue == "" {
		callbackQueue = queue
	}

	for _, cb := range result.Callbacks {
		batch.EnqueueCallback(ctx, c.redis, c.keys, cb.JobType, parentID, result.ParentID, cb.Options, callbackQueue, batchTTL)
	}

	if result.CompletedNow && result.ParentID != "" {
		// Determine the result to propagate to grandparent
		grandparentResult := "success"
		if result.Dead {
			grandparentResult = "death"
		} else if result.Invalidated {
			grandparentResult = "invalidated"
		}
		c.propagateBatchCompletion(ctx, parentID, result.ParentID, grandparentResult, callbackQueue)
	}
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
	_, _ = pipe.Exec(ctx) // Best-effort cleanup, ignore errors
}

// rollbackParentLink undoes the effect of batch_add_child, decrementing
// the parent's pending count and removing the child from its jobs set.
func (c *Client) rollbackParentLink(ctx context.Context, parentID, childID string) {
	keys := []string{
		c.keys.Batch(parentID),
		c.keys.BatchJobs(parentID),
	}
	_, _ = batchRemoveChildScript.Run(ctx, c.redis, keys, childID) // Best-effort rollback
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
func (c *Client) buildBatchState(b *Batch, callbackQueue string, emptyBatch bool) *senna.BatchState {
	callbackCount := 0
	if emptyBatch {
		if b.OnComplete != nil {
			callbackCount++
		}
		if b.OnSuccess != nil {
			callbackCount++
		}
	}

	state := &senna.BatchState{
		ID:               b.ID,
		Description:      b.Description,
		ParentID:         b.ParentID,
		Total:            len(b.Jobs),
		Pending:          len(b.Jobs),
		Failures:         0,
		Successes:        0,
		CallbacksPending: callbackCount,
		Dead:             false,
		DeathFired:       false,
		CompleteFired:    emptyBatch,
		SuccessFired:     emptyBatch,
		CreatedAt:        b.CreatedAt,
		CallbackQueue:    callbackQueue,
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
		return err
	}

	pipe := c.redis.Pipeline()
	pipe.Set(ctx, c.keys.Batch(batchID), string(batchData), batchTTL)
	pipe.SAdd(ctx, c.keys.Batches(), batchID)

	_, err = pipe.Exec(ctx)
	return err
}

// linkBatchToParent links a child batch to its parent (Step 2).
func (c *Client) linkBatchToParent(ctx context.Context, b *Batch) error {
	scriptKeys := []string{
		c.keys.Batch(b.ParentID),
		c.keys.BatchJobs(b.ParentID),
	}

	result, err := batchAddChildScript.Run(ctx, c.redis, scriptKeys, b.ID)
	if err != nil {
		c.cleanupOrphanedBatch(ctx, b.ID)
		return fmt.Errorf("failed to add child batch to parent: %w", err)
	}

	var addResult struct {
		Error   string `json:"error,omitempty"`
		Success bool   `json:"success"`
	}
	if err := json.Unmarshal([]byte(result.(string)), &addResult); err != nil {
		c.cleanupOrphanedBatch(ctx, b.ID)
		return fmt.Errorf("failed to parse batch add child result: %w", err)
	}

	if addResult.Error != "" {
		c.cleanupOrphanedBatch(ctx, b.ID)

		switch addResult.Error {
		case "batch_not_found":
			return &senna.BatchNotFoundError{BatchID: b.ParentID}
		case "batch_invalidated":
			return fmt.Errorf("parent batch %s has been invalidated", b.ParentID)
		case "batch_complete":
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
	pipe.Expire(ctx, c.keys.BatchJobs(b.ID), batchTTL)

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

// handleEmptyBatch handles callbacks and propagation for empty batches.
func (c *Client) handleEmptyBatch(ctx context.Context, b *Batch, callbackQueue string) {
	c.enqueueEmptyBatchCallbacks(ctx, b, callbackQueue)

	// If this empty batch has a parent but no callbacks, propagate completion immediately
	if b.ParentID != "" && b.OnComplete == nil && b.OnSuccess == nil {
		c.propagateEmptyChildBatch(ctx, b.ID, b.ParentID, callbackQueue)
	}
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
