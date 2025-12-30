package worker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mgomes/senna"
	"github.com/mgomes/senna/internal/keys"
	"github.com/redis/go-redis/v9"
)

// batchContextKey is the context key for the batch handle.
type batchContextKey struct{}

// BatchHandle provides access to the current batch from within a job handler.
// It allows dynamically adding more jobs to the batch.
type BatchHandle struct {
	bid   string
	redis redis.Cmdable
	keys  *keys.Keys
}

// newBatchHandle creates a new batch handle.
func newBatchHandle(bid string, redis redis.Cmdable, keys *keys.Keys) *BatchHandle {
	return &BatchHandle{
		bid:   bid,
		redis: redis,
		keys:  keys,
	}
}

// BID returns the batch ID.
func (bh *BatchHandle) BID() string {
	return bh.bid
}

// AddJobs atomically adds jobs to the batch.
// This is safe to call from within a job that is part of this batch.
// All jobs are added atomically - either all are added or none are.
func (bh *BatchHandle) AddJobs(ctx context.Context, jobs []*senna.Job) error {
	if len(jobs) == 0 {
		return nil
	}

	// First, atomically update the batch state
	keys := []string{
		bh.keys.Batch(bh.bid),
		bh.keys.BatchJobs(bh.bid),
	}

	args := make([]any, 0, len(jobs)+1)
	args = append(args, len(jobs))
	for _, job := range jobs {
		job.BatchID = bh.bid
		args = append(args, job.ID)
	}

	result, err := batchAddJobsScript.Run(ctx, bh.redis, keys, args...)
	if err != nil {
		return fmt.Errorf("failed to add jobs to batch: %w", err)
	}

	var addResult struct {
		Error   string `json:"error,omitempty"`
		Success bool   `json:"success"`
	}
	if err := json.Unmarshal([]byte(result.(string)), &addResult); err != nil {
		return fmt.Errorf("failed to parse batch add result: %w", err)
	}

	if addResult.Error != "" {
		switch addResult.Error {
		case "batch_not_found":
			return &senna.BatchNotFoundError{BatchID: bh.bid}
		case "batch_invalidated":
			return fmt.Errorf("batch %s has been invalidated", bh.bid)
		case "batch_complete":
			return fmt.Errorf("batch %s has already completed", bh.bid)
		default:
			return fmt.Errorf("batch error: %s", addResult.Error)
		}
	}

	// Now push the jobs to their queues
	pipe := bh.redis.Pipeline()
	for _, job := range jobs {
		data, err := job.Marshal()
		if err != nil {
			return err
		}
		pipe.LPush(ctx, bh.keys.Queue(job.Queue), string(data))
	}

	_, err = pipe.Exec(ctx)
	return err
}

// Add is a convenience method to add a single job to the batch.
func (bh *BatchHandle) Add(ctx context.Context, jobType string, args map[string]any, queue ...string) error {
	job := senna.NewJob(jobType, args)
	if len(queue) > 0 {
		job.Queue = queue[0]
	}
	return bh.AddJobs(ctx, []*senna.Job{job})
}

// Invalidate marks the batch as invalidated.
// Jobs that check for validity will skip execution.
func (bh *BatchHandle) Invalidate(ctx context.Context) error {
	keys := []string{bh.keys.Batch(bh.bid)}

	result, err := batchInvalidateScript.Run(ctx, bh.redis, keys)
	if err != nil {
		return fmt.Errorf("failed to invalidate batch: %w", err)
	}

	var invalidateResult struct {
		Error   string `json:"error,omitempty"`
		Success bool   `json:"success"`
	}
	if err := json.Unmarshal([]byte(result.(string)), &invalidateResult); err != nil {
		return fmt.Errorf("failed to parse invalidate result: %w", err)
	}

	if invalidateResult.Error != "" {
		if invalidateResult.Error == "batch_not_found" {
			return &senna.BatchNotFoundError{BatchID: bh.bid}
		}
		return fmt.Errorf("batch error: %s", invalidateResult.Error)
	}

	return nil
}

// Valid checks if the batch is still valid (not invalidated).
func (bh *BatchHandle) Valid(ctx context.Context) (bool, error) {
	data, err := bh.redis.Get(ctx, bh.keys.Batch(bh.bid)).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}

	var state senna.BatchState
	if err := json.Unmarshal([]byte(data), &state); err != nil {
		return false, err
	}

	return !state.Invalidated, nil
}

// contextWithBatch returns a context with the batch handle attached.
func contextWithBatch(ctx context.Context, bh *BatchHandle) context.Context {
	return context.WithValue(ctx, batchContextKey{}, bh)
}

// BatchFromContext returns the BatchHandle from the context, if present.
// Returns nil if the job is not part of a batch.
func BatchFromContext(ctx context.Context) *BatchHandle {
	bh, _ := ctx.Value(batchContextKey{}).(*BatchHandle)
	return bh
}

// BIDFromContext returns the batch ID from the context, if present.
// Returns empty string if the job is not part of a batch.
func BIDFromContext(ctx context.Context) string {
	if bh := BatchFromContext(ctx); bh != nil {
		return bh.BID()
	}
	return ""
}

// ValidWithinBatch checks if the current job is still valid within its batch.
// Returns true if:
// - The job is not part of a batch, OR
// - The job is part of a batch that has not been invalidated
//
// Use this at the start of a job handler to skip work if the batch was cancelled:
//
//	func myHandler(ctx context.Context, job *senna.Job) error {
//	    if valid, _ := worker.ValidWithinBatch(ctx); !valid {
//	        return nil // Skip execution
//	    }
//	    // ... do work ...
//	}
func ValidWithinBatch(ctx context.Context) (bool, error) {
	bh := BatchFromContext(ctx)
	if bh == nil {
		// Not in a batch, always valid
		return true, nil
	}
	return bh.Valid(ctx)
}
