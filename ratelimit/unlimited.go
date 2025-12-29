package ratelimit

import (
	"context"
	"time"
)

type UnlimitedLimiter struct {
	name string
}

func Unlimited(name string) *UnlimitedLimiter {
	return &UnlimitedLimiter{name: name}
}

func (l *UnlimitedLimiter) Name() string {
	return l.name
}

func (l *UnlimitedLimiter) WithinLimit(ctx context.Context, fn func() error) error {
	return fn()
}

func (l *UnlimitedLimiter) Acquire(ctx context.Context) (time.Duration, error) {
	return 0, nil
}

func (l *UnlimitedLimiter) Release(ctx context.Context) error {
	return nil
}
