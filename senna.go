package senna

import (
	"context"

	"github.com/mgomes/senna/ratelimit"
)

func WithinLimit(limiter ratelimit.Limiter, fn func() error) error {
	return limiter.WithinLimit(context.Background(), fn)
}

func WithinLimitCtx(ctx context.Context, limiter ratelimit.Limiter, fn func() error) error {
	return limiter.WithinLimit(ctx, fn)
}

func RateLimitMiddleware(limiter ratelimit.Limiter) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, job *Job) error {
			return limiter.WithinLimit(ctx, func() error {
				return next(ctx, job)
			})
		}
	}
}

func RateLimitMiddlewareWithReschedule(limiter ratelimit.Limiter) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, job *Job) error {
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
			defer func() { _ = limiter.Release(ctx) }()
			return next(ctx, job)
		}
	}
}
