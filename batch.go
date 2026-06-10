package senna

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/mgomes/senna/internal/keys"
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
	CallbackSequence int           `json:"callback_seq,omitempty"`
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
	redis redis.Cmdable
	keys  *keys.Keys
	bid   string
	state *BatchState
}

const defaultBatchJoinInterval = 500 * time.Millisecond

// NewBatchStatus creates a BatchStatus for querying a batch's state.
func NewBatchStatus(redis redis.Cmdable, namespace, bid string) *BatchStatus {
	k := keys.New(namespace)
	return &BatchStatus{
		redis: redis,
		keys:  k,
		bid:   bid,
	}
}

func (bs *BatchStatus) batchKey() string {
	return bs.keys.Batch(bs.bid)
}

func (bs *BatchStatus) batchJobsKey() string {
	return bs.keys.BatchJobs(bs.bid)
}

func (bs *BatchStatus) batchFailedKey() string {
	return bs.keys.BatchFailed(bs.bid)
}

// Refresh reloads the batch state from Redis.
func (bs *BatchStatus) Refresh(ctx context.Context) error {
	data, err := bs.redis.Get(ctx, bs.batchKey()).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return &BatchNotFoundError{BatchID: bs.bid}
		}
		return err
	}

	state, err := decodeBatchState(data)
	if err != nil {
		return err
	}
	bs.state = state
	return nil
}

// RefreshFull reloads the batch state and failed job IDs from Redis.
func (bs *BatchStatus) RefreshFull(ctx context.Context) error {
	pipe := bs.redis.Pipeline()
	stateCmd := pipe.Get(ctx, bs.batchKey())
	failedCmd := pipe.SMembers(ctx, bs.batchFailedKey())

	_, execErr := pipe.Exec(ctx)

	data, err := stateCmd.Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return &BatchNotFoundError{BatchID: bs.bid}
		}
		return err
	}
	failedJIDs, err := failedCmd.Result()
	if err != nil {
		return err
	}
	if execErr != nil {
		return execErr
	}

	state, err := decodeBatchState(data)
	if err != nil {
		return err
	}
	if failedJIDs == nil {
		failedJIDs = []string{}
	}
	state.FailedJIDs = failedJIDs
	bs.state = state
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
	if bs.state != nil && bs.state.FailedJIDs != nil {
		return copyStrings(bs.state.FailedJIDs), nil
	}
	return bs.redis.SMembers(ctx, bs.batchFailedKey()).Result()
}

// Data returns a map of batch data suitable for JSON serialization.
func (bs *BatchStatus) Data() map[string]any {
	if bs.state == nil {
		return nil
	}
	data := map[string]any{
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
	if bs.state.FailedJIDs != nil {
		data["failed_jids"] = copyStrings(bs.state.FailedJIDs)
	}
	return data
}

// Join blocks until the batch is complete or the context is cancelled.
func (bs *BatchStatus) Join(ctx context.Context) error {
	return bs.join(ctx, defaultBatchJoinInterval)
}

// JoinWithInterval blocks until the batch is complete, refreshing at interval.
func (bs *BatchStatus) JoinWithInterval(ctx context.Context, interval time.Duration) error {
	return bs.join(ctx, interval)
}

func (bs *BatchStatus) join(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = defaultBatchJoinInterval
	}

	ticker := time.NewTicker(interval)
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

	pipe := bs.redis.Pipeline()
	removeFromBatches := pipe.SRem(ctx, bs.keys.Batches(), bs.bid)
	removeFromDeadBatches := pipe.SRem(ctx, bs.keys.DeadBatches(), bs.bid)
	deleteBatchData := pipe.Del(ctx, keys...)

	_, execErr := pipe.Exec(ctx)

	var commandErrs []error
	if err := removeFromBatches.Err(); err != nil {
		commandErrs = append(commandErrs, fmt.Errorf("remove batch %s from batches set: %w", bs.bid, err))
	}
	if err := removeFromDeadBatches.Err(); err != nil {
		commandErrs = append(commandErrs, fmt.Errorf("remove batch %s from dead batches set: %w", bs.bid, err))
	}
	if err := deleteBatchData.Err(); err != nil {
		commandErrs = append(commandErrs, fmt.Errorf("delete batch %s data: %w", bs.bid, err))
	}
	if err := errors.Join(commandErrs...); err != nil {
		return err
	}
	return execErr
}

func loadBatchStatus(ctx context.Context, client redis.Cmdable, k *keys.Keys, setKey, bid string) (*BatchStatus, error) {
	status := NewBatchStatus(client, k.Namespace(), bid)
	if err := status.Refresh(ctx); err != nil {
		var notFound *BatchNotFoundError
		if errors.As(err, &notFound) {
			client.SRem(ctx, setKey, bid)
			return nil, nil
		}
		return nil, err
	}
	return status, nil
}

func copyStrings(values []string) []string {
	if values == nil {
		return nil
	}
	copied := make([]string, len(values))
	copy(copied, values)
	return copied
}

func decodeBatchState(data string) (*BatchState, error) {
	var state BatchState
	if err := json.Unmarshal([]byte(data), &state); err != nil {
		return nil, err
	}
	return &state, nil
}

// BatchSet provides iteration over all known batches.
type BatchSet struct {
	redis redis.Cmdable
	keys  *keys.Keys
}

// NewBatchSet creates a new BatchSet for iterating batches.
func NewBatchSet(redis redis.Cmdable, namespace string) *BatchSet {
	k := keys.New(namespace)
	return &BatchSet{
		redis: redis,
		keys:  k,
	}
}

// Each iterates over all batches, calling fn for each.
// Only returns batches that still have pending jobs.
func (bs *BatchSet) Each(ctx context.Context, fn func(*BatchStatus) error) error {
	bids, err := bs.redis.SMembers(ctx, bs.keys.Batches()).Result()
	if err != nil {
		return err
	}

	for _, bid := range bids {
		status, err := loadBatchStatus(ctx, bs.redis, bs.keys, bs.keys.Batches(), bid)
		if err != nil {
			return err
		}
		if status == nil {
			continue
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
	return bs.redis.SCard(ctx, bs.keys.Batches()).Result()
}

// DeadBatchSet provides iteration over batches that have dead jobs.
type DeadBatchSet struct {
	redis redis.Cmdable
	keys  *keys.Keys
}

// NewDeadBatchSet creates a new DeadBatchSet for iterating dead batches.
func NewDeadBatchSet(redis redis.Cmdable, namespace string) *DeadBatchSet {
	k := keys.New(namespace)
	return &DeadBatchSet{
		redis: redis,
		keys:  k,
	}
}

// Each iterates over all dead batches, calling fn for each.
func (dbs *DeadBatchSet) Each(ctx context.Context, fn func(*BatchStatus) error) error {
	bids, err := dbs.redis.SMembers(ctx, dbs.keys.DeadBatches()).Result()
	if err != nil {
		return err
	}

	for _, bid := range bids {
		status, err := loadBatchStatus(ctx, dbs.redis, dbs.keys, dbs.keys.DeadBatches(), bid)
		if err != nil {
			return err
		}
		if status == nil {
			continue
		}

		if err := fn(status); err != nil {
			return err
		}
	}

	return nil
}

// Size returns the number of dead batches.
func (dbs *DeadBatchSet) Size(ctx context.Context) (int64, error) {
	return dbs.redis.SCard(ctx, dbs.keys.DeadBatches()).Result()
}
