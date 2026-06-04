package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Acquirer acquires capacity for a single unit of work. It is the minimal
// interface needed to gate work, used by callers that schedule their own retry
// (e.g. the reschedule middleware) rather than blocking inside WithinLimit.
type Acquirer interface {
	// Acquire returns a non-nil lease when capacity is acquired with waitTime == 0.
	Acquire(ctx context.Context) (lease Lease, waitTime time.Duration, err error)
	Name() string
}

// Limiter is an Acquirer that can also run a function within its limit.
type Limiter interface {
	Acquirer
	WithinLimit(ctx context.Context, fn func() error) error
}

// Lease releases capacity acquired from a limiter.
type Lease interface {
	Release(ctx context.Context) error
}

// runWithinLimit acquires capacity from a, runs fn when capacity is available,
// and releases the lease afterward. It returns an OverLimitError when the
// limiter is over budget under PolicySkip; under PolicyRaise the over-limit
// error is surfaced by Acquire itself. This centralizes the logic every
// limiter's WithinLimit would otherwise duplicate.
func runWithinLimit(ctx context.Context, a Acquirer, typ LimiterType, limit int, fn func() error) (err error) {
	lease, waitTime, err := a.Acquire(ctx)
	if err != nil {
		return err
	}
	if waitTime > 0 {
		return &OverLimitError{
			LimiterName: a.Name(),
			LimiterType: typ,
			Limit:       limit,
			RetryIn:     waitTime,
		}
	}
	if lease != nil {
		defer func() {
			releaseErr := lease.Release(ctx)
			if releaseErr == nil {
				return
			}
			if err == nil {
				err = releaseErr
				return
			}
			err = errors.Join(err, releaseErr)
		}()
	}
	return fn()
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
