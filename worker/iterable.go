package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/mgomes/senna"
	"github.com/mgomes/senna/ratelimit"
)

const (
	defaultCursorSaveInterval = 5 * time.Second
	iterationStateTTL         = 30 * 24 * time.Hour // 30 days
)

// IterableJobOptions configures iterable job behavior.
type IterableJobOptions struct {
	// CursorSaveInterval controls how often the cursor is saved to Redis.
	// Default: 5 seconds
	CursorSaveInterval time.Duration

	// MaxItemsPerRun limits items processed before re-enqueueing the job.
	// 0 means no limit (process until completion or interruption).
	MaxItemsPerRun int

	// Callbacks for lifecycle events.
	Callbacks *senna.IterableCallbacks

	// Standard job options
	MaxRetries   int
	RetryBackoff senna.BackoffFunc
	Timeout      time.Duration
	RateLimiter  ratelimit.Limiter
}

// IterableJobOption configures iterable job behavior.
type IterableJobOption func(*IterableJobOptions)

// WithCursorSaveInterval sets how often the cursor is persisted to Redis.
func WithCursorSaveInterval(d time.Duration) IterableJobOption {
	return func(o *IterableJobOptions) {
		o.CursorSaveInterval = d
	}
}

// WithMaxItemsPerRun limits items processed before re-enqueueing.
// Useful for very long iterations to allow other jobs to run.
func WithMaxItemsPerRun(n int) IterableJobOption {
	return func(o *IterableJobOptions) {
		o.MaxItemsPerRun = n
	}
}

// WithIterableCallbacks sets lifecycle callbacks for the iterable job.
func WithIterableCallbacks(cb *senna.IterableCallbacks) IterableJobOption {
	return func(o *IterableJobOptions) {
		o.Callbacks = cb
	}
}

// WithIterableMaxRetries sets the maximum number of retries for the iterable job.
func WithIterableMaxRetries(n int) IterableJobOption {
	return func(o *IterableJobOptions) {
		o.MaxRetries = n
	}
}

// WithIterableTimeout sets the timeout for the entire iterable job execution.
func WithIterableTimeout(d time.Duration) IterableJobOption {
	return func(o *IterableJobOptions) {
		o.Timeout = d
	}
}

// WithIterableRateLimiter sets a rate limiter for the iterable job.
func WithIterableRateLimiter(limiter ratelimit.Limiter) IterableJobOption {
	return func(o *IterableJobOptions) {
		o.RateLimiter = limiter
	}
}

// RegisterIterable registers an iterable job handler.
func (w *Worker) RegisterIterable(jobType string, handler senna.IterableHandler, opts ...IterableJobOption) {
	options := &IterableJobOptions{
		CursorSaveInterval: defaultCursorSaveInterval,
		MaxRetries:         25,
		RetryBackoff:       senna.DefaultBackoff(),
	}
	for _, opt := range opts {
		opt(options)
	}

	w.handlers.RegisterIterable(jobType, handler, options)
}

// processIterable handles the iteration loop for an iterable job.
func (w *Worker) processIterable(ctx context.Context, job *senna.Job, handler senna.IterableHandler, opts *IterableJobOptions) error {
	stateKey := w.keys.IterationState(job.ID)

	// Load or create state
	state, err := w.loadIterationState(ctx, stateKey)
	if err != nil {
		return err
	}

	isResume := state != nil
	if state == nil {
		state = &senna.IterationState{
			JobID:     job.ID,
			StartedAt: time.Now(),
		}
	} else if state.StartedAt.IsZero() {
		// State was created by CancelIteration before job started
		state.StartedAt = time.Now()
	}

	state.Executions++
	state.LastRunAt = time.Now()

	// Fire callbacks
	if opts.Callbacks != nil {
		if state.Executions == 1 && opts.Callbacks.OnStart != nil {
			if err := opts.Callbacks.OnStart(ctx, job); err != nil {
				return err
			}
		} else if isResume && opts.Callbacks.OnResume != nil {
			if err := opts.Callbacks.OnResume(ctx, job, state); err != nil {
				return err
			}
		}
	}

	// Build iterator
	iter, err := handler.BuildIterator(ctx, job, state.Cursor)
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	// Cursor save ticker
	saveInterval := opts.CursorSaveInterval
	if saveInterval <= 0 {
		saveInterval = defaultCursorSaveInterval
	}
	saveTicker := time.NewTicker(saveInterval)
	defer saveTicker.Stop()

	itemsThisRun := 0
	runStart := time.Now()
	needsSave := false

	for iter.Next(ctx) {
		// Check for cancellation (marked in Redis)
		if state.Cancelled || w.checkIterationCancelled(ctx, stateKey) {
			state.Cancelled = true
			state.TotalTime += time.Since(runStart)
			// Don't update cursor - current item wasn't processed yet
			_ = w.saveIterationState(ctx, stateKey, state)

			if opts.Callbacks != nil {
				if opts.Callbacks.OnCancel != nil {
					_ = opts.Callbacks.OnCancel(ctx, job, state)
				}
				if opts.Callbacks.OnStop != nil {
					_ = opts.Callbacks.OnStop(ctx, job, state)
				}
			}
			return nil // Ack job (success), no on_complete
		}

		// Check for shutdown
		select {
		case <-ctx.Done():
			state.TotalTime += time.Since(runStart)
			// Don't update cursor - current item wasn't processed yet
			// Use non-cancelled context for saving state during shutdown
			saveCtx := context.WithoutCancel(ctx)
			_ = w.saveIterationState(saveCtx, stateKey, state)

			if opts.Callbacks != nil && opts.Callbacks.OnStop != nil {
				_ = opts.Callbacks.OnStop(saveCtx, job, state)
			}
			return &senna.InterruptedError{} // Requeue same job
		default:
		}

		// Process item
		item := iter.Item()
		err := handler.ProcessItem(ctx, job, item)

		if err != nil {
			var skipErr *senna.SkipItemError
			var stopErr *senna.StopIterationError

			if errors.As(err, &skipErr) {
				slog.WarnContext(ctx, "skipping item", "reason", skipErr.Reason, "job_id", job.ID)
				// Continue to next item
			} else if errors.As(err, &stopErr) {
				slog.InfoContext(ctx, "stopping iteration early", "reason", stopErr.Reason, "job_id", job.ID)
				break // Complete successfully
			} else {
				// Real error - save cursor and return for retry
				// Don't update cursor - failed item will be retried
				state.TotalTime += time.Since(runStart)
				_ = w.saveIterationState(ctx, stateKey, state)
				return err
			}
		}

		state.TotalItems++
		state.Cursor = iter.Cursor()
		itemsThisRun++
		needsSave = true

		// Check max items per run
		if opts.MaxItemsPerRun > 0 && itemsThisRun >= opts.MaxItemsPerRun {
			// Preserve cancellation flag set by external client
			if w.checkIterationCancelled(ctx, stateKey) {
				state.Cancelled = true
			}
			state.TotalTime += time.Since(runStart)
			_ = w.saveIterationState(ctx, stateKey, state)

			if opts.Callbacks != nil && opts.Callbacks.OnStop != nil {
				_ = opts.Callbacks.OnStop(ctx, job, state)
			}
			return &senna.InterruptedError{} // Requeue same job
		}

		// Periodic cursor save
		select {
		case <-saveTicker.C:
			if needsSave {
				// Preserve cancellation flag set by external client
				if w.checkIterationCancelled(ctx, stateKey) {
					state.Cancelled = true
				}
				state.TotalTime += time.Since(runStart)
				_ = w.saveIterationState(ctx, stateKey, state)
				runStart = time.Now()
			}
		default:
		}
	}

	// Check iterator error
	if err := iter.Err(); err != nil {
		// Don't update cursor - Next() failed, so no new item was fetched
		if ctx.Err() != nil || errors.Is(err, context.Canceled) {
			state.TotalTime += time.Since(runStart)
			saveCtx := context.WithoutCancel(ctx)
			_ = w.saveIterationState(saveCtx, stateKey, state)

			if opts.Callbacks != nil && opts.Callbacks.OnStop != nil {
				_ = opts.Callbacks.OnStop(saveCtx, job, state)
			}
			return &senna.InterruptedError{}
		}

		state.TotalTime += time.Since(runStart)
		_ = w.saveIterationState(ctx, stateKey, state)
		return err
	}

	// Check for late cancellation before completing
	if w.checkIterationCancelled(ctx, stateKey) {
		state.Cancelled = true
		state.TotalTime += time.Since(runStart)
		_ = w.saveIterationState(ctx, stateKey, state)

		if opts.Callbacks != nil {
			if opts.Callbacks.OnCancel != nil {
				_ = opts.Callbacks.OnCancel(ctx, job, state)
			}
			if opts.Callbacks.OnStop != nil {
				_ = opts.Callbacks.OnStop(ctx, job, state)
			}
		}
		return nil // Ack job (success), no OnComplete
	}

	// Complete - fire OnComplete and DELETE state from Redis
	state.TotalTime += time.Since(runStart)

	if opts.Callbacks != nil && opts.Callbacks.OnComplete != nil {
		if err := opts.Callbacks.OnComplete(ctx, job, state); err != nil {
			return err
		}
	}

	// Delete state on successful completion
	w.redis.Del(ctx, stateKey)

	return nil
}

// loadIterationState loads the iteration state from Redis.
// Returns nil if no state exists.
func (w *Worker) loadIterationState(ctx context.Context, key string) (*senna.IterationState, error) {
	data, err := w.redis.Get(ctx, key).Result()
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

// saveIterationState saves the iteration state to Redis with TTL.
func (w *Worker) saveIterationState(ctx context.Context, key string, state *senna.IterationState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	return w.redis.Set(ctx, key, string(data), iterationStateTTL).Err()
}

// checkIterationCancelled checks if the iteration has been cancelled.
func (w *Worker) checkIterationCancelled(ctx context.Context, key string) bool {
	state, err := w.loadIterationState(ctx, key)
	if err != nil || state == nil {
		return false
	}
	return state.Cancelled
}

// requeue puts the job back on its queue without creating a new job.
// Used for interrupted iterable jobs to preserve job ID.
func (w *Worker) requeue(ctx context.Context, job *senna.Job) error {
	// Remove from in-flight first
	if err := w.fetcher.Ack(ctx, w.id, job); err != nil {
		slog.ErrorContext(ctx, "failed to ack job before requeue", "error", err, "job_id", job.ID)
	}

	// Re-serialize with same ID and push to back of queue (RPUSH)
	data, err := job.Marshal()
	if err != nil {
		return err
	}

	return w.redis.RPush(ctx, w.keys.Queue(job.Queue), string(data)).Err()
}
