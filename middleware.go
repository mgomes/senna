package senna

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"
)

// Middleware decorates a job handler.
type Middleware func(Handler) Handler

// Chain composes middlewares around a handler in declaration order.
func Chain(middlewares ...Middleware) Middleware {
	return func(final Handler) Handler {
		for i := len(middlewares) - 1; i >= 0; i-- {
			final = middlewares[i](final)
		}
		return final
	}
}

// LoggingMiddleware logs job completion and failures with durations.
func LoggingMiddleware(logger *slog.Logger) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, job *Job) error {
			start := time.Now()
			err := next(ctx, job)
			duration := time.Since(start)

			if err != nil {
				logger.ErrorContext(ctx, "job failed",
					"job_id", job.ID,
					"type", job.Type,
					"queue", job.Queue,
					"duration", duration,
					"error", err,
				)
			} else {
				logger.InfoContext(ctx, "job completed",
					"job_id", job.ID,
					"type", job.Type,
					"queue", job.Queue,
					"duration", duration,
				)
			}
			return err
		}
	}
}

// RecoveryMiddleware converts panics into returned errors.
func RecoveryMiddleware() Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, job *Job) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
				}
			}()
			return next(ctx, job)
		}
	}
}

// TimeoutMiddleware runs the job handler with a context deadline.
// Handlers must observe ctx cancellation to stop work when the deadline expires.
func TimeoutMiddleware(d time.Duration) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, job *Job) error {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()

			return next(ctx, job)
		}
	}
}

// RetryMiddleware wraps handler failures in retry-oriented Senna errors.
func RetryMiddleware(maxRetries int, backoff BackoffFunc) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, job *Job) error {
			err := next(ctx, job)
			if err == nil {
				return nil
			}

			if job.RetryCount >= maxRetries {
				return &MaxRetriesExceededError{
					Job:   job,
					Cause: err,
				}
			}

			return &RetryableError{
				Job:     job,
				Cause:   err,
				RetryIn: backoff(job.RetryCount),
			}
		}
	}
}

// BackoffFunc returns the retry delay for the given attempt.
type BackoffFunc func(attempt int) time.Duration

// ExponentialBackoff returns an exponential retry backoff capped at maxBackoff.
func ExponentialBackoff(base time.Duration, maxBackoff time.Duration) BackoffFunc {
	return func(attempt int) time.Duration {
		if base <= 0 || attempt <= 0 {
			if base > maxBackoff {
				return maxBackoff
			}
			return base
		}
		if maxBackoff <= base {
			return maxBackoff
		}

		backoff := base
		for range attempt {
			if backoff > maxBackoff/2 {
				return maxBackoff
			}
			backoff *= 2
		}
		return backoff
	}
}

// DefaultBackoff returns Senna's default retry backoff function.
func DefaultBackoff() BackoffFunc {
	return func(attempt int) time.Duration {
		if attempt < 0 {
			attempt = 0
		}
		secs, ok := defaultBackoffSeconds(int64(attempt))
		if !ok {
			return maxDuration
		}
		return time.Duration(secs) * time.Second
	}
}

const (
	maxDuration        = time.Duration(1<<63 - 1)
	maxDurationSeconds = int64(maxDuration / time.Second)
)

func defaultBackoffSeconds(attempt int64) (int64, bool) {
	squared, ok := checkedMulDurationSeconds(attempt, attempt)
	if !ok {
		return 0, false
	}
	fourth, ok := checkedMulDurationSeconds(squared, squared)
	if !ok {
		return 0, false
	}
	linear, ok := checkedMulDurationSeconds(attempt, 10)
	if !ok {
		return 0, false
	}
	secs, ok := checkedAddDurationSeconds(fourth, linear)
	if !ok {
		return 0, false
	}
	return checkedAddDurationSeconds(secs, 15)
}

func checkedMulDurationSeconds(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}
	if a > maxDurationSeconds/b {
		return 0, false
	}
	return a * b, true
}

func checkedAddDurationSeconds(a, b int64) (int64, bool) {
	if a > maxDurationSeconds-b {
		return 0, false
	}
	return a + b, true
}
