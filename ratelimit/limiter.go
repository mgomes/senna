package ratelimit

import (
	"context"
	"fmt"
	"time"
)

type Limiter interface {
	WithinLimit(ctx context.Context, fn func() error) error
	Acquire(ctx context.Context) (waitTime time.Duration, err error)
	Release(ctx context.Context) error
	Name() string
}

type Result struct {
	Allowed   bool
	Remaining int
	ResetAt   time.Time
	RetryIn   time.Duration
}

type OverLimitError struct {
	LimiterName string
	LimiterType string
	Limit       int
	Current     int
	RetryIn     time.Duration
}

func (e *OverLimitError) Error() string {
	if e.RetryIn > 0 {
		return fmt.Sprintf("rate limit exceeded for %s (%s): %d/%d, retry in %v",
			e.LimiterName, e.LimiterType, e.Current, e.Limit, e.RetryIn)
	}
	return fmt.Sprintf("rate limit exceeded for %s (%s): %d/%d",
		e.LimiterName, e.LimiterType, e.Current, e.Limit)
}

type Policy int

const (
	PolicyRaise Policy = iota
	PolicySkip
)

const (
	Second = time.Second
	Minute = time.Minute
	Hour   = time.Hour
	Day    = 24 * time.Hour
)
