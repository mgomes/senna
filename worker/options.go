package worker

import (
	"time"

	"github.com/mgomes/senna/ratelimit"
)

type JobOption func(*JobOptions)

func WithMaxRetries(n int) JobOption {
	return func(o *JobOptions) {
		o.MaxRetries = n
	}
}

func WithJobTimeout(d time.Duration) JobOption {
	return func(o *JobOptions) {
		o.Timeout = d
	}
}

func WithMaxConcurrency(n int) JobOption {
	return func(o *JobOptions) {
		o.MaxConcurrency = n
	}
}

func WithUniqueJob(key string, ttl time.Duration) JobOption {
	return func(o *JobOptions) {
		o.Unique = &UniqueConfig{
			Key: key,
			TTL: ttl,
		}
	}
}

func WithRateLimiter(limiter ratelimit.Limiter) JobOption {
	return func(o *JobOptions) {
		o.RateLimiter = limiter
	}
}
