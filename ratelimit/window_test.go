package ratelimit_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgomes/senna/ratelimit"
)

func TestWindowLimiter_Basic(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:window:test-basic*")

	limiter := ratelimit.Window(client, ratelimit.WindowConfig{
		Name:        "test-basic",
		Limit:       5,
		Interval:    time.Second,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	waitForRateLimitWindow(time.Second)
	for i := range 5 {
		waitTime, err := limiter.Acquire(ctx)
		if err != nil {
			t.Fatalf("acquire %d failed: %v", i, err)
		}
		if waitTime != 0 {
			t.Fatalf("acquire %d should not have wait time, got %v", i, waitTime)
		}
	}

	waitTime, err := limiter.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire 6 failed: %v", err)
	}
	if waitTime == 0 {
		t.Fatal("acquire 6 should have wait time")
	}
}

func TestWindowLimiter_SlidingBehavior(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:window:test-sliding*")

	limiter := ratelimit.Window(client, ratelimit.WindowConfig{
		Name:        "test-sliding",
		Limit:       3,
		Interval:    500 * time.Millisecond,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	waitForRateLimitWindow(500 * time.Millisecond)
	for i := range 3 {
		_, err := limiter.Acquire(ctx)
		if err != nil {
			t.Fatalf("acquire %d failed: %v", i, err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(250 * time.Millisecond)

	waitTime, err := limiter.Acquire(ctx)
	if err != nil {
		t.Fatalf("sliding acquire failed: %v", err)
	}
	if waitTime != 0 {
		t.Fatalf("sliding window should allow new request, got wait time %v", waitTime)
	}
}

func TestWindowLimiter_Concurrent(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:window:test-concurrent*")

	limiter := ratelimit.Window(client, ratelimit.WindowConfig{
		Name:        "test-concurrent",
		Limit:       50,
		Interval:    time.Second,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	waitForRateLimitWindow(time.Second)
	var wg sync.WaitGroup
	var successCount atomic.Int32

	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := limiter.WithinLimit(ctx, func() error {
				successCount.Add(1)
				return nil
			})
			_ = err
		}()
	}

	wg.Wait()

	count := successCount.Load()
	if count != 50 {
		t.Fatalf("expected exactly 50 successes, got %d", count)
	}
}

func TestWindowLimiter_WithinLimit(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:window:test-within*")

	limiter := ratelimit.Window(client, ratelimit.WindowConfig{
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

func TestWindowLimiter_UniqueMembers(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:window:test-unique*")

	limiter := ratelimit.Window(client, ratelimit.WindowConfig{
		Name:        "test-unique",
		Limit:       100,
		Interval:    time.Second,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	waitForRateLimitWindow(time.Second)
	for i := range 100 {
		waitTime, err := limiter.Acquire(ctx)
		if err != nil {
			t.Fatalf("acquire %d failed: %v", i, err)
		}
		if waitTime != 0 {
			t.Fatalf("acquire %d should not wait, got %v", i, waitTime)
		}
	}

	waitTime, _ := limiter.Acquire(ctx)
	if waitTime == 0 {
		t.Fatal("should be rate limited after 100 requests")
	}
}

func TestWindowLimiter_ContextCancellation(t *testing.T) {
	client := newTestClient(t)
	flushKeys(t, client, "senna:ratelimit:window:test-cancel*")

	limiter := ratelimit.Window(client, ratelimit.WindowConfig{
		Name:        "test-cancel",
		Limit:       1,
		Interval:    time.Second,
		WaitTimeout: 5 * time.Second,
		Policy:      ratelimit.PolicyRaise,
	})

	ctx := context.Background()
	waitForRateLimitWindow(time.Second)
	_, err := limiter.Acquire(ctx)
	if err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = limiter.Acquire(ctx)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}
