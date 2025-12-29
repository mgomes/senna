package ratelimit_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgomes/senna/ratelimit"
)

func TestLeakyLimiter_Basic(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:leaky:test-basic*")

	limiter := ratelimit.Leaky(client, ratelimit.LeakyConfig{
		Name:        "test-basic",
		Capacity:    5,
		DrainTime:   time.Second,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	for i := 0; i < 5; i++ {
		waitTime, err := limiter.Acquire(ctx)
		if err != nil {
			t.Fatalf("acquire %d failed: %v", i, err)
		}
		if waitTime != 0 {
			t.Fatalf("acquire %d should not wait, got %v", i, waitTime)
		}
	}

	waitTime, err := limiter.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire 6 failed: %v", err)
	}
	if waitTime == 0 {
		t.Fatal("acquire 6 should have wait time when bucket is full")
	}
}

func TestLeakyLimiter_Draining(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:leaky:test-drain*")

	limiter := ratelimit.Leaky(client, ratelimit.LeakyConfig{
		Name:        "test-drain",
		Capacity:    5,
		DrainTime:   500 * time.Millisecond,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	for i := 0; i < 5; i++ {
		_, err := limiter.Acquire(ctx)
		if err != nil {
			t.Fatalf("acquire %d failed: %v", i, err)
		}
	}

	waitTime, _ := limiter.Acquire(ctx)
	if waitTime == 0 {
		t.Fatal("should be at capacity")
	}

	time.Sleep(150 * time.Millisecond)

	waitTime, err := limiter.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire after drain failed: %v", err)
	}
	if waitTime != 0 {
		t.Fatalf("should allow request after draining, got wait %v", waitTime)
	}
}

func TestLeakyLimiter_BurstThenSteady(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:leaky:test-burst*")

	limiter := ratelimit.Leaky(client, ratelimit.LeakyConfig{
		Name:        "test-burst",
		Capacity:    10,
		DrainTime:   time.Second,
		WaitTimeout: 50 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	burstSuccess := 0
	for i := 0; i < 15; i++ {
		err := limiter.WithinLimit(ctx, func() error {
			burstSuccess++
			return nil
		})
		_ = err
	}

	if burstSuccess != 10 {
		t.Fatalf("expected 10 burst successes, got %d", burstSuccess)
	}
}

func TestLeakyLimiter_Level(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:leaky:test-level*")

	limiter := ratelimit.Leaky(client, ratelimit.LeakyConfig{
		Name:        "test-level",
		Capacity:    10,
		DrainTime:   time.Second,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	level, err := limiter.Level(ctx)
	if err != nil {
		t.Fatalf("level check failed: %v", err)
	}
	if level != 0 {
		t.Fatalf("expected level 0, got %f", level)
	}

	for i := 0; i < 5; i++ {
		limiter.Acquire(ctx)
	}

	level, err = limiter.Level(ctx)
	if err != nil {
		t.Fatalf("level check failed: %v", err)
	}
	if level < 4 || level > 5 {
		t.Fatalf("expected level ~5, got %f", level)
	}
}

func TestLeakyLimiter_Concurrent(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:leaky:test-concurrent*")

	limiter := ratelimit.Leaky(client, ratelimit.LeakyConfig{
		Name:        "test-concurrent",
		Capacity:    50,
		DrainTime:   time.Second,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	var wg sync.WaitGroup
	var successCount atomic.Int32

	for i := 0; i < 100; i++ {
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

func TestLeakyLimiter_WithinLimit(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:leaky:test-within*")

	limiter := ratelimit.Leaky(client, ratelimit.LeakyConfig{
		Name:        "test-within",
		Capacity:    3,
		DrainTime:   time.Second,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	executed := 0
	for i := 0; i < 5; i++ {
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
