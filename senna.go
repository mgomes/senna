package senna

import (
	"context"
	"errors"

	"github.com/mgomes/senna/ratelimit"
)

// WithinLimit runs fn through the limiter using a background context.
func WithinLimit(limiter ratelimit.Limiter, fn func() error) error {
	return limiter.WithinLimit(context.Background(), fn)
}

// WithinLimitCtx runs fn through the limiter using the provided context.
func WithinLimitCtx(ctx context.Context, limiter ratelimit.Limiter, fn func() error) error {
	return limiter.WithinLimit(ctx, fn)
}

// RateLimitMiddleware rejects work when the limiter reports the job is over limit.
func RateLimitMiddleware(limiter ratelimit.Limiter) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, job *Job) error {
			return limiter.WithinLimit(ctx, func() error {
				return next(ctx, job)
			})
		}
	}
}

// RateLimitMiddlewareWithReschedule retries over-limit jobs after the limiter's wait time.
func RateLimitMiddlewareWithReschedule(limiter ratelimit.Limiter) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, job *Job) (err error) {
			waitTime, err := limiter.Acquire(ctx)
			if err != nil {
				return err
			}
			if waitTime > 0 {
				return &RetryableError{
					Job:     job,
					Cause:   &ratelimit.OverLimitError{LimiterName: limiter.Name(), LimiterType: "unknown", RetryIn: waitTime},
					RetryIn: waitTime,
				}
			}
			defer func() {
				err = errors.Join(err, limiter.Release(ctx))
			}()

			return next(ctx, job)
		}
	}
}
