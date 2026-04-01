package ratelimit

import (
	"context"
	"time"
)

// UnlimitedLimiter is a limiter that never blocks work.
type UnlimitedLimiter struct {
	name string
}

// Unlimited constructs an UnlimitedLimiter.
func Unlimited(name string) *UnlimitedLimiter {
	return &UnlimitedLimiter{name: name}
}

// Name returns the limiter name.
func (l *UnlimitedLimiter) Name() string {
	return l.name
}

// WithinLimit runs fn immediately.
func (l *UnlimitedLimiter) WithinLimit(ctx context.Context, fn func() error) error {
	return fn()
}

// Acquire immediately succeeds for unlimited limiters.
func (l *UnlimitedLimiter) Acquire(ctx context.Context) (time.Duration, error) {
	return 0, nil
}

// Release is a no-op for unlimited limiters.
func (l *UnlimitedLimiter) Release(ctx context.Context) error {
	return nil
}
