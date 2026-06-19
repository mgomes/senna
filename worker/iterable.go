package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/mgomes/senna"
	"github.com/mgomes/senna/internal/iteration"
	"github.com/mgomes/senna/ratelimit"
)

const defaultCursorSaveInterval = 5 * time.Second

// IterableJobOptions configures iterable job behavior.
type IterableJobOptions struct {
	// CursorSaveInterval controls how often the cursor is saved to Redis.
	// Default: 5 seconds
	CursorSaveInterval time.Duration

	// MaxItemsPerRun limits items processed before re-enqueueing the job.
	// 0 means no limit (process until completion or interruption).
	MaxItemsPerRun int

	// MaxRuntime limits elapsed processing time before re-enqueueing the job.
	// 0 uses the worker default. A negative value disables runtime-based yielding.
	MaxRuntime time.Duration

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

// WithIterableMaxRuntime limits elapsed processing time before re-enqueueing.
func WithIterableMaxRuntime(d time.Duration) IterableJobOption {
	return func(o *IterableJobOptions) {
		o.MaxRuntime = d
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

// WithIterableTimeout sets the context deadline for the entire iterable job execution.
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
		MaxRuntime:         w.defaultIterableMaxRuntime(),
		MaxRetries:         senna.DefaultRetryCount,
		RetryBackoff:       senna.DefaultBackoff(),
	}
	for _, opt := range opts {
		opt(options)
	}

	w.handlers.RegisterIterable(jobType, handler, options)
}

func (w *Worker) defaultIterableMaxRuntime() time.Duration {
	if w != nil && w.config != nil {
		return w.config.Settings.IterableMaxRuntime
	}
	return senna.DefaultWorkerSettings().IterableMaxRuntime
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
	iterClosed := false
	defer func() {
		if iterClosed {
			return
		}
		if err := iter.Close(); err != nil {
			slog.WarnContext(ctx, "failed to close iterable iterator", "job_id", job.ID, "error", err)
		}
	}()

	// Cursor save ticker
	saveInterval := opts.CursorSaveInterval
	if saveInterval <= 0 {
		saveInterval = defaultCursorSaveInterval
	}
	saveTicker := time.NewTicker(saveInterval)
	defer saveTicker.Stop()

	itemsThisRun := 0
	runStart := time.Now()
	maxRuntime := w.iterableMaxRuntime(opts)
	runtimeDeadline := time.Time{}
	if maxRuntime > 0 {
		runtimeDeadline = runStart.Add(maxRuntime)
	}
	needsSave := false

	for iter.Next(ctx) {
		// Check for cancellation (marked in Redis)
		if state.Cancelled || w.iterationCancelled(ctx, stateKey) {
			return w.handleIterationCancelled(ctx, state, stateKey, runStart, opts, job)
		}

		// Check for shutdown
		select {
		case <-ctx.Done():
			saveCtx := context.WithoutCancel(ctx)
			return w.handleIterationInterrupt(saveCtx, state, stateKey, runStart, opts, job)
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
				// Real error - save state and return for retry
				w.preserveCancellation(ctx, state, stateKey)
				w.updateIterationTiming(state, runStart)
				if saveErr := w.saveIterationStateFor(ctx, stateKey, state, "on item error"); saveErr != nil {
					return errors.Join(err, saveErr)
				}
				return err
			}
		}

		state.TotalItems++
		state.Cursor = iter.Cursor()
		itemsThisRun++
		needsSave = true

		// Check max items per run
		if opts.MaxItemsPerRun > 0 && itemsThisRun >= opts.MaxItemsPerRun {
			interruptCtx := ctx
			if ctx.Err() != nil {
				interruptCtx = context.WithoutCancel(ctx)
			}
			w.preserveCancellation(interruptCtx, state, stateKey)
			return w.handleIterationInterrupt(interruptCtx, state, stateKey, runStart, opts, job)
		}

		if !runtimeDeadline.IsZero() && !time.Now().Before(runtimeDeadline) {
			interruptCtx := ctx
			if ctx.Err() != nil {
				interruptCtx = context.WithoutCancel(ctx)
			}
			w.preserveCancellation(interruptCtx, state, stateKey)
			return w.handleIterationInterrupt(interruptCtx, state, stateKey, runStart, opts, job)
		}

		// Periodic cursor save
		select {
		case <-saveTicker.C:
			if needsSave {
				if ctx.Err() != nil {
					saveCtx := context.WithoutCancel(ctx)
					return w.handleIterationInterrupt(saveCtx, state, stateKey, runStart, opts, job)
				}
				w.preserveCancellation(ctx, state, stateKey)
				runStart = w.updateIterationTiming(state, runStart)
				if saveErr := w.saveIterationStateFor(ctx, stateKey, state, "during cursor checkpoint"); saveErr != nil {
					if ctx.Err() != nil {
						saveCtx := context.WithoutCancel(ctx)
						return w.handleIterationInterrupt(saveCtx, state, stateKey, runStart, opts, job)
					}
					return saveErr
				}
			}
		default:
		}
	}

	// Check iterator error
	if err := iter.Err(); err != nil {
		// Don't update cursor - Next() failed, so no new item was fetched
		if ctx.Err() != nil || errors.Is(err, context.Canceled) {
			saveCtx := context.WithoutCancel(ctx)
			return w.handleIterationInterrupt(saveCtx, state, stateKey, runStart, opts, job)
		}

		w.updateIterationTiming(state, runStart)
		if saveErr := w.saveIterationStateFor(ctx, stateKey, state, "on iterator error"); saveErr != nil {
			return errors.Join(err, saveErr)
		}
		return err
	}

	terminalCtx := ctx
	if ctx.Err() != nil {
		terminalCtx = context.WithoutCancel(ctx)
	}

	// Check for late cancellation before completing
	if w.iterationCancelled(terminalCtx, stateKey) {
		return w.handleIterationCancelled(terminalCtx, state, stateKey, runStart, opts, job)
	}

	if err := iter.Close(); err != nil {
		iterClosed = true
		w.preserveCancellation(terminalCtx, state, stateKey)
		w.updateIterationTiming(state, runStart)
		closeErr := fmt.Errorf("close iterable iterator: %w", err)
		if saveErr := w.saveIterationStateFor(terminalCtx, stateKey, state, "on iterator close error"); saveErr != nil {
			return errors.Join(closeErr, saveErr)
		}
		return closeErr
	}
	iterClosed = true

	// Complete - fire OnComplete and DELETE state from Redis
	w.preserveCancellation(terminalCtx, state, stateKey)
	w.updateIterationTiming(state, runStart)
	if saveErr := w.saveIterationStateFor(terminalCtx, stateKey, state, "before completion"); saveErr != nil {
		return saveErr
	}

	if opts.Callbacks != nil && opts.Callbacks.OnComplete != nil {
		if err := opts.Callbacks.OnComplete(terminalCtx, job, state); err != nil {
			return err
		}
	}

	// Delete state on successful completion
	if err := w.redis.Del(terminalCtx, stateKey).Err(); err != nil {
		slog.WarnContext(terminalCtx, "failed to delete completed iteration state", "job_id", job.ID, "error", err)
	}

	return nil
}

func (w *Worker) iterableMaxRuntime(opts *IterableJobOptions) time.Duration {
	if opts != nil && opts.MaxRuntime != 0 {
		return opts.MaxRuntime
	}
	return w.defaultIterableMaxRuntime()
}

// loadIterationState loads the iteration state from Redis.
// Returns nil if no state exists.
func (w *Worker) loadIterationState(ctx context.Context, key string) (*senna.IterationState, error) {
	return iteration.Load(ctx, w.redis, key)
}

func (w *Worker) saveIterationState(ctx context.Context, key string, state *senna.IterationState) error {
	return iteration.Save(ctx, w.redis, key, state, iteration.StateTTL)
}

func (w *Worker) saveIterationStateFor(ctx context.Context, key string, state *senna.IterationState, reason string) error {
	if err := w.saveIterationState(ctx, key, state); err != nil {
		return fmt.Errorf("save iteration state %s: %w", reason, err)
	}
	return nil
}

func (w *Worker) iterationCancelled(ctx context.Context, key string) bool {
	cancelled, err := iteration.IsCancelled(ctx, w.redis, key)
	if err != nil {
		return false
	}
	return cancelled
}

// updateIterationTiming updates the state's TotalTime from runStart and returns the new runStart.
func (w *Worker) updateIterationTiming(state *senna.IterationState, runStart time.Time) time.Time {
	state.TotalTime += time.Since(runStart)
	return time.Now()
}

// preserveCancellation checks if iteration was cancelled and sets the flag.
func (w *Worker) preserveCancellation(ctx context.Context, state *senna.IterationState, stateKey string) {
	if w.iterationCancelled(ctx, stateKey) {
		state.Cancelled = true
	}
}

// handleIterationCancelled handles cancellation - saves state, fires callbacks.
func (w *Worker) handleIterationCancelled(ctx context.Context, state *senna.IterationState, stateKey string, runStart time.Time, opts *IterableJobOptions, job *senna.Job) error {
	state.Cancelled = true
	w.updateIterationTiming(state, runStart)
	if err := w.saveIterationStateFor(ctx, stateKey, state, "on cancel"); err != nil {
		return err
	}

	if opts.Callbacks != nil {
		if opts.Callbacks.OnCancel != nil {
			if err := opts.Callbacks.OnCancel(ctx, job, state); err != nil {
				slog.WarnContext(ctx, "OnCancel callback failed", "job_id", job.ID, "error", err)
			}
		}
		if opts.Callbacks.OnStop != nil {
			if err := opts.Callbacks.OnStop(ctx, job, state); err != nil {
				slog.WarnContext(ctx, "OnStop callback failed", "job_id", job.ID, "error", err)
			}
		}
	}
	return nil
}

// handleIterationInterrupt handles interrupt/requeue - saves state, fires OnStop, returns InterruptedError.
func (w *Worker) handleIterationInterrupt(ctx context.Context, state *senna.IterationState, stateKey string, runStart time.Time, opts *IterableJobOptions, job *senna.Job) error {
	w.preserveCancellation(ctx, state, stateKey)
	w.updateIterationTiming(state, runStart)
	if err := w.saveIterationStateFor(ctx, stateKey, state, "on interrupt"); err != nil {
		return err
	}

	if opts.Callbacks != nil && opts.Callbacks.OnStop != nil {
		if err := opts.Callbacks.OnStop(ctx, job, state); err != nil {
			slog.WarnContext(ctx, "OnStop callback failed", "job_id", job.ID, "error", err)
		}
	}
	return &senna.InterruptedError{}
}

// requeue puts the job back on its queue without creating a new job.
// Used for interrupted iterable jobs to preserve job ID.
// Unlike Ack, this preserves the unique key so uniqueness is maintained.
func (w *Worker) requeue(ctx context.Context, job *senna.Job) error {
	return w.fetcher.requeue(ctx, w.id, job)
}
