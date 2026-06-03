package ratelimit_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgomes/senna/ratelimit"
)

func waitForRateLimitWindow(interval time.Duration) {
	if interval <= 0 {
		return
	}

	guard := 200 * time.Millisecond
	if interval < guard*2 {
		guard = interval / 2
	}
	if guard <= 0 || guard >= interval {
		return
	}

	for {
		offset := time.Duration(time.Now().UnixNano()) % interval
		if interval-offset >= guard {
			return
		}
		time.Sleep(interval - offset)
	}
}

func TestBucketLimiter_Basic(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:bucket:test-basic*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:        "test-basic",
		Limit:       5,
		Interval:    time.Second,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	waitForRateLimitWindow(time.Second)
	for i := range 5 {
		_, waitTime, err := limiter.Acquire(ctx)
		if err != nil {
			t.Fatalf("acquire %d failed: %v", i, err)
		}
		if waitTime != 0 {
			t.Fatalf("acquire %d should not have wait time, got %v", i, waitTime)
		}
	}

	_, waitTime, err := limiter.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire 6 failed: %v", err)
	}
	if waitTime == 0 {
		t.Fatal("acquire 6 should have wait time")
	}
}

func TestBucketLimiter_WithinLimit(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:bucket:test-within*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:        "test-within",
		Limit:       3,
		Interval:    time.Second,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	executed := 0
	waitForRateLimitWindow(time.Second)
	for i := range 5 {
		err := limiter.WithinLimit(ctx, func() error {
			executed++
			return nil
		})
		if i < 3 && err != nil {
			t.Fatalf("iteration %d should succeed: %v", i, err)
		}
	}

	if executed != 3 {
		t.Fatalf("expected 3 executions, got %d", executed)
	}
}

func TestBucketLimiter_Concurrent(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:bucket:test-concurrent*")

	interval := 2 * time.Second
	waitForRateLimitWindow(interval)

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:        "test-concurrent",
		Limit:       100,
		Interval:    interval,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	var wg sync.WaitGroup
	var successCount atomic.Int32

	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := limiter.WithinLimit(ctx, func() error {
				successCount.Add(1)
				return nil
			})
			if err != nil {
				var overLimit *ratelimit.OverLimitError
				if _, ok := err.(*ratelimit.OverLimitError); !ok {
					_ = overLimit
				}
			}
		}()
	}

	wg.Wait()

	count := successCount.Load()
	if count != 100 {
		t.Fatalf("expected exactly 100 successes, got %d", count)
	}
}

func TestBucketLimiter_WindowReset(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:bucket:test-reset*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:        "test-reset",
		Limit:       3,
		Interval:    500 * time.Millisecond,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	waitForRateLimitWindow(500 * time.Millisecond)
	for i := range 3 {
		_, _, err := limiter.Acquire(ctx)
		if err != nil {
			t.Fatalf("first batch acquire %d failed: %v", i, err)
		}
	}

	_, waitTime, err := limiter.Acquire(ctx)
	if err != nil {
		t.Fatalf("over-limit acquire failed: %v", err)
	}
	if waitTime == 0 {
		t.Fatal("should be rate limited")
	}

	time.Sleep(waitTime + 50*time.Millisecond)

	for i := range 3 {
		_, waitTime, err := limiter.Acquire(ctx)
		if err != nil {
			t.Fatalf("second batch acquire %d failed: %v", i, err)
		}
		if waitTime != 0 {
			t.Fatalf("second batch acquire %d should not wait", i)
		}
	}
}

func TestBucketLimiter_PolicyRaise(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:bucket:test-raise*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:        "test-raise",
		Limit:       2,
		Interval:    time.Minute, // Long interval to avoid window boundary issues
		WaitTimeout: 50 * time.Millisecond,
		Policy:      ratelimit.PolicyRaise,
	})

	for i := range 2 {
		_, _, err := limiter.Acquire(ctx)
		if err != nil {
			t.Fatalf("acquire %d failed: %v", i, err)
		}
	}

	_, _, err := limiter.Acquire(ctx)
	if err == nil {
		t.Fatal("expected error when rate limited with PolicyRaise")
	}

	var overLimit *ratelimit.OverLimitError
	if _, ok := err.(*ratelimit.OverLimitError); !ok {
		_ = overLimit
		t.Fatalf("expected OverLimitError, got %T", err)
	}
}

func TestBucketLimiter_ContextCancellation(t *testing.T) {
	client := newTestClient(t)
	flushKeys(t, client, "senna:ratelimit:bucket:test-cancel*")

	// Use a long interval so retryIn will be long, and long waitTimeout so it waits
	// instead of returning OverLimitError immediately. Context will expire first.
	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:        "test-cancel",
		Limit:       1,
		Interval:    time.Minute,     // Long interval = long retryIn
		WaitTimeout: 2 * time.Minute, // Longer than interval so it waits
		Policy:      ratelimit.PolicyRaise,
	})

	ctx := context.Background()
	_, _, err := limiter.Acquire(ctx)
	if err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}

	// Context expires before the wait completes
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, _, err = limiter.Acquire(ctx)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestBucketLimiter_DifferentIntervals(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()

	tests := []struct {
		name     string
		interval time.Duration
	}{
		{"second", time.Second},
		{"minute", time.Minute},
		{"hour", time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flushKeys(t, client, "senna:ratelimit:bucket:test-interval-"+tt.name+"*")

			limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
				Name:        "test-interval-" + tt.name,
				Limit:       5,
				Interval:    tt.interval,
				WaitTimeout: 10 * time.Millisecond,
				Policy:      ratelimit.PolicySkip,
			})

			waitForRateLimitWindow(tt.interval)
			for i := range 5 {
				_, waitTime, err := limiter.Acquire(ctx)
				if err != nil {
					t.Fatalf("acquire %d failed: %v", i, err)
				}
				if waitTime != 0 {
					t.Fatalf("acquire %d should not wait", i)
				}
			}

			_, waitTime, _ := limiter.Acquire(ctx)
			if waitTime == 0 {
				t.Fatal("should be rate limited")
			}
		})
	}
}
