package ratelimit

import (
	"context"
	"fmt"
	"time"
)

// Limiter defines the interface implemented by Senna rate limiters.
type Limiter interface {
	WithinLimit(ctx context.Context, fn func() error) error
	// Acquire returns a non-nil lease when capacity is acquired with waitTime == 0.
	Acquire(ctx context.Context) (lease Lease, waitTime time.Duration, err error)
	Name() string
}

// Lease releases capacity acquired from a limiter.
type Lease interface {
	Release(ctx context.Context) error
}

type noopLease struct{}

func (noopLease) Release(ctx context.Context) error {
	return nil
}

// Result describes a rate-limit decision.
type Result struct {
	Allowed   bool
	Remaining int
	ResetAt   time.Time
	RetryIn   time.Duration
}

// LimiterType identifies which limiter algorithm produced a result.
type LimiterType string

const (
	// TypeBucket is the fixed-window bucket limiter.
	TypeBucket LimiterType = "bucket"
	// TypeWindow is the sliding-window limiter.
	TypeWindow LimiterType = "window"
	// TypeLeaky is the leaky-bucket limiter.
	TypeLeaky LimiterType = "leaky"
	// TypePoints is the points (variable-cost token bucket) limiter.
	TypePoints LimiterType = "points"
	// TypeConcurrent is the concurrency limiter.
	TypeConcurrent LimiterType = "concurrent"
)

// OverLimitError reports that a limiter denied work.
type OverLimitError struct {
	LimiterName string
	LimiterType LimiterType
	Limit       int
	Current     int
	RetryIn     time.Duration
}

// Error implements error.
func (e *OverLimitError) Error() string {
	if e.RetryIn > 0 {
		return fmt.Sprintf("rate limit exceeded for %s (%s): %d/%d, retry in %v",
			e.LimiterName, e.LimiterType, e.Current, e.Limit, e.RetryIn)
	}
	return fmt.Sprintf("rate limit exceeded for %s (%s): %d/%d",
		e.LimiterName, e.LimiterType, e.Current, e.Limit)
}

// Policy controls how a limiter behaves when work would exceed its budget.
type Policy int

const (
	// PolicyRaise returns an OverLimitError when the limiter cannot acquire capacity.
	PolicyRaise Policy = iota
	// PolicySkip returns a positive wait time instead of blocking or raising.
	PolicySkip
)

const (
	// Second is a convenience alias for time.Second.
	Second = time.Second
	// Minute is a convenience alias for time.Minute.
	Minute = time.Minute
	// Hour is a convenience alias for time.Hour.
	Hour = time.Hour
	// Day is a convenience alias for twenty-four hours.
	Day = 24 * time.Hour
)
