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

// TimeoutMiddleware cancels job execution after the provided duration.
func TimeoutMiddleware(d time.Duration) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, job *Job) error {
			ctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()

			done := make(chan error, 1)
			go func() {
				done <- next(ctx, job)
			}()

			select {
			case err := <-done:
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
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
		backoff := base * (1 << uint(attempt))
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
		return backoff
	}
}

// DefaultBackoff returns Senna's default retry backoff function.
func DefaultBackoff() BackoffFunc {
	return func(attempt int) time.Duration {
		secs := (attempt * attempt * attempt * attempt) + 15 + (attempt * 10)
		return time.Duration(secs) * time.Second
	}
}
