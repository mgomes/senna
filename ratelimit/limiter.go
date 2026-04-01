package ratelimit

import (
	"context"
	"fmt"
	"time"
)

// Limiter defines the interface implemented by Senna rate limiters.
type Limiter interface {
	WithinLimit(ctx context.Context, fn func() error) error
	Acquire(ctx context.Context) (waitTime time.Duration, err error)
	Release(ctx context.Context) error
	Name() string
}

// Result describes a rate-limit decision.
type Result struct {
	Allowed   bool
	Remaining int
	ResetAt   time.Time
	RetryIn   time.Duration
}

// OverLimitError reports that a limiter denied work.
type OverLimitError struct {
	LimiterName string
	LimiterType string
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
