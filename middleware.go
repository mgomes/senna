package senna

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"
)

type Middleware func(Handler) Handler

func Chain(middlewares ...Middleware) Middleware {
	return func(final Handler) Handler {
		for i := len(middlewares) - 1; i >= 0; i-- {
			final = middlewares[i](final)
		}
		return final
	}
}

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

type BackoffFunc func(attempt int) time.Duration

func ExponentialBackoff(base time.Duration, maxBackoff time.Duration) BackoffFunc {
	return func(attempt int) time.Duration {
		backoff := base * (1 << uint(attempt))
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
		return backoff
	}
}

func DefaultBackoff() BackoffFunc {
	return func(attempt int) time.Duration {
		secs := (attempt * attempt * attempt * attempt) + 15 + (attempt * 10)
		return time.Duration(secs) * time.Second
	}
}
