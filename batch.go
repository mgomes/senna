package senna

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// BatchState represents the current state of a batch stored in Redis.
type BatchState struct {
	ID               string        `json:"id"`
	Description      string        `json:"description,omitempty"`
	ParentID         string        `json:"parent_id,omitempty"`
	Total            int           `json:"total"`
	Pending          int           `json:"pending"`
	Failures         int           `json:"failures"`
	Successes        int           `json:"successes"`
	CallbacksPending int           `json:"callbacks_pending,omitempty"`
	Dead             bool          `json:"dead"`
	DeathFired       bool          `json:"death_fired"`
	CompleteFired    bool          `json:"complete_fired"`
	SuccessFired     bool          `json:"success_fired"`
	CreatedAt        time.Time     `json:"created_at"`
	OnComplete       *CallbackInfo `json:"on_complete,omitempty"`
	OnSuccess        *CallbackInfo `json:"on_success,omitempty"`
	OnDeath          *CallbackInfo `json:"on_death,omitempty"`
	CallbackQueue    string        `json:"callback_queue,omitempty"`
	FailedJIDs       []string      `json:"failed_jids,omitempty"`
	Invalidated      bool          `json:"invalidated,omitempty"`
}

// CallbackInfo holds callback configuration.
type CallbackInfo struct {
	JobType string         `json:"job_type"`
	Options map[string]any `json:"options,omitempty"`
}

// BatchStatus provides methods to query batch state.
type BatchStatus struct {
	redis     redis.Cmdable
	namespace string
	bid       string
	state     *BatchState
}

// NewBatchStatus creates a BatchStatus for querying a batch's state.
func NewBatchStatus(redis redis.Cmdable, namespace, bid string) *BatchStatus {
	if namespace == "" {
		namespace = "senna"
	}
	return &BatchStatus{
		redis:     redis,
		namespace: namespace,
		bid:       bid,
	}
}

func (bs *BatchStatus) batchKey() string {
	return bs.namespace + ":batch:" + bs.bid
}

func (bs *BatchStatus) batchJobsKey() string {
	return bs.namespace + ":batch:" + bs.bid + ":jobs"
}

func (bs *BatchStatus) batchFailedKey() string {
	return bs.namespace + ":batch:" + bs.bid + ":failed"
}

// Refresh reloads the batch state from Redis.
func (bs *BatchStatus) Refresh(ctx context.Context) error {
	data, err := bs.redis.Get(ctx, bs.batchKey()).Result()
	if err != nil {
		if err == redis.Nil {
			return &BatchNotFoundError{BatchID: bs.bid}
		}
		return err
	}

	var state BatchState
	if err := json.Unmarshal([]byte(data), &state); err != nil {
		return err
	}
	bs.state = &state
	return nil
}

// BID returns the batch ID.
func (bs *BatchStatus) BID() string {
	return bs.bid
}

// Total returns the total number of jobs in the batch.
func (bs *BatchStatus) Total() int {
	if bs.state == nil {
		return 0
	}
	return bs.state.Total
}

// Pending returns the number of jobs that haven't completed yet.
func (bs *BatchStatus) Pending() int {
	if bs.state == nil {
		return 0
	}
	return bs.state.Pending
}

// Failures returns the number of jobs that have failed (but may retry).
func (bs *BatchStatus) Failures() int {
	if bs.state == nil {
		return 0
	}
	return bs.state.Failures
}

// Successes returns the number of jobs that completed successfully.
func (bs *BatchStatus) Successes() int {
	if bs.state == nil {
		return 0
	}
	return bs.state.Successes
}

// Complete returns true if all jobs have executed at least once.
func (bs *BatchStatus) Complete() bool {
	if bs.state == nil {
		return false
	}
	return bs.state.Pending == 0
}

// Dead returns true if any job in the batch has died (exhausted retries).
func (bs *BatchStatus) Dead() bool {
	if bs.state == nil {
		return false
	}
	return bs.state.Dead
}

// Description returns the batch description.
func (bs *BatchStatus) Description() string {
	if bs.state == nil {
		return ""
	}
	return bs.state.Description
}

// CreatedAt returns when the batch was created.
func (bs *BatchStatus) CreatedAt() time.Time {
	if bs.state == nil {
		return time.Time{}
	}
	return bs.state.CreatedAt
}

// FailedJIDs returns the job IDs of failed jobs.
func (bs *BatchStatus) FailedJIDs(ctx context.Context) ([]string, error) {
	return bs.redis.SMembers(ctx, bs.batchFailedKey()).Result()
}

// Data returns a map of batch data suitable for JSON serialization.
func (bs *BatchStatus) Data() map[string]any {
	if bs.state == nil {
		return nil
	}
	return map[string]any{
		"bid":         bs.bid,
		"description": bs.state.Description,
		"total":       bs.state.Total,
		"pending":     bs.state.Pending,
		"failures":    bs.state.Failures,
		"successes":   bs.state.Successes,
		"complete":    bs.state.Pending == 0,
		"dead":        bs.state.Dead,
		"created_at":  bs.state.CreatedAt.Unix(),
	}
}

// Join blocks until the batch is complete or the context is cancelled.
func (bs *BatchStatus) Join(ctx context.Context) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := bs.Refresh(ctx); err != nil {
				return err
			}
			if bs.Complete() {
				return nil
			}
		}
	}
}

// Delete removes the batch data from Redis.
// Warning: This will break any in-progress jobs associated with the batch.
func (bs *BatchStatus) Delete(ctx context.Context) error {
	keys := []string{
		bs.batchKey(),
		bs.batchJobsKey(),
		bs.batchFailedKey(),
	}
	// Also remove from the batches set
	bs.redis.SRem(ctx, bs.namespace+":batches", bs.bid)
	bs.redis.SRem(ctx, bs.namespace+":batches:dead", bs.bid)
	return bs.redis.Del(ctx, keys...).Err()
}

// BatchSet provides iteration over all known batches.
type BatchSet struct {
	redis     redis.Cmdable
	namespace string
}

// NewBatchSet creates a new BatchSet for iterating batches.
func NewBatchSet(redis redis.Cmdable, namespace string) *BatchSet {
	if namespace == "" {
		namespace = "senna"
	}
	return &BatchSet{
		redis:     redis,
		namespace: namespace,
	}
}

// Each iterates over all batches, calling fn for each.
// Only returns batches that still have pending jobs.
func (bs *BatchSet) Each(ctx context.Context, fn func(*BatchStatus) error) error {
	bids, err := bs.redis.SMembers(ctx, bs.namespace+":batches").Result()
	if err != nil {
		return err
	}

	for _, bid := range bids {
		status := NewBatchStatus(bs.redis, bs.namespace, bid)
		if err := status.Refresh(ctx); err != nil {
			// Batch may have expired or been deleted
			var notFound *BatchNotFoundError
			if errors.As(err, &notFound) {
				// Remove stale entry
				bs.redis.SRem(ctx, bs.namespace+":batches", bid)
				continue
			}
			return err
		}

		// Skip completed batches (they linger for status queries)
		if status.Complete() {
			continue
		}

		if err := fn(status); err != nil {
			return err
		}
	}

	return nil
}

// Size returns the number of batches (may include completed ones).
func (bs *BatchSet) Size(ctx context.Context) (int64, error) {
	return bs.redis.SCard(ctx, bs.namespace+":batches").Result()
}

// DeadBatchSet provides iteration over batches that have dead jobs.
type DeadBatchSet struct {
	redis     redis.Cmdable
	namespace string
}

// NewDeadBatchSet creates a new DeadBatchSet for iterating dead batches.
func NewDeadBatchSet(redis redis.Cmdable, namespace string) *DeadBatchSet {
	if namespace == "" {
		namespace = "senna"
	}
	return &DeadBatchSet{
		redis:     redis,
		namespace: namespace,
	}
}

// Each iterates over all dead batches, calling fn for each.
func (dbs *DeadBatchSet) Each(ctx context.Context, fn func(*BatchStatus) error) error {
	bids, err := dbs.redis.SMembers(ctx, dbs.namespace+":batches:dead").Result()
	if err != nil {
		return err
	}

	for _, bid := range bids {
		status := NewBatchStatus(dbs.redis, dbs.namespace, bid)
		if err := status.Refresh(ctx); err != nil {
			var notFound *BatchNotFoundError
			if errors.As(err, &notFound) {
				dbs.redis.SRem(ctx, dbs.namespace+":batches:dead", bid)
				continue
			}
			return err
		}

		if err := fn(status); err != nil {
			return err
		}
	}

	return nil
}

// Size returns the number of dead batches.
func (dbs *DeadBatchSet) Size(ctx context.Context) (int64, error) {
	return dbs.redis.SCard(ctx, dbs.namespace+":batches:dead").Result()
}
