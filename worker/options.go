package worker

import (
	"time"

	"github.com/mgomes/senna/ratelimit"
)

// JobOption configures how a worker executes a registered job type.
type JobOption func(*JobOptions)

// WithMaxRetries sets the maximum retry count for a registered job type.
func WithMaxRetries(n int) JobOption {
	return func(o *JobOptions) {
		o.MaxRetries = n
	}
}

// WithJobTimeout sets the handler context deadline for a registered job type.
func WithJobTimeout(d time.Duration) JobOption {
	return func(o *JobOptions) {
		o.Timeout = d
	}
}

// WithMaxConcurrency limits concurrent executions of a registered job type.
func WithMaxConcurrency(n int) JobOption {
	return func(o *JobOptions) {
		o.MaxConcurrency = n
	}
}

// WithUniqueJob stores uniqueness settings on the registered job type.
func WithUniqueJob(key string, ttl time.Duration) JobOption {
	return func(o *JobOptions) {
		o.Unique = &UniqueConfig{
			Key: key,
			TTL: ttl,
		}
	}
}

// WithRateLimiter applies a rate limiter to a registered job type.
func WithRateLimiter(limiter ratelimit.Limiter) JobOption {
	return func(o *JobOptions) {
		o.RateLimiter = limiter
	}
}
