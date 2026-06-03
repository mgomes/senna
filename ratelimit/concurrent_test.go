package ratelimit_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgomes/senna/ratelimit"
)

func TestConcurrentLimiter_Basic(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:concurrent:test-basic*")

	limiter := ratelimit.Concurrent(client, ratelimit.ConcurrentConfig{
		Name:        "test-basic",
		Limit:       3,
		LockTimeout: 5 * time.Second,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	var acquired []func()

	for i := range 3 {
		waitTime, err := limiter.Acquire(ctx)
		if err != nil {
			t.Fatalf("acquire %d failed: %v", i, err)
		}
		if waitTime != 0 {
			t.Fatalf("acquire %d should not wait", i)
		}
		acquired = append(acquired, func() { _ = limiter.Release(ctx) })
	}

	waitTime, _ := limiter.Acquire(ctx)
	if waitTime == 0 {
		t.Fatal("fourth acquire should be blocked")
	}

	acquired[0]()

	waitTime, err := limiter.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire after release failed: %v", err)
	}
	if waitTime != 0 {
		t.Fatal("acquire after release should not wait")
	}
}

func TestConcurrentLimiter_WithinLimit(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:concurrent:test-within*")

	limiter := ratelimit.Concurrent(client, ratelimit.ConcurrentConfig{
		Name:        "test-within",
		Limit:       2,
		LockTimeout: 5 * time.Second,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	var wg sync.WaitGroup
	var running atomic.Int32
	var maxRunning atomic.Int32

	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := limiter.WithinLimit(ctx, func() error {
				current := running.Add(1)
				for {
					max := maxRunning.Load()
					if current <= max || maxRunning.CompareAndSwap(max, current) {
						break
					}
				}
				time.Sleep(50 * time.Millisecond)
				running.Add(-1)
				return nil
			})
			_ = err
		}()
	}

	wg.Wait()

	if maxRunning.Load() > 2 {
		t.Fatalf("max concurrent should be <= 2, got %d", maxRunning.Load())
	}
}

func TestConcurrentLimiter_AutoRelease(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:concurrent:test-autorelease*")

	limiter := ratelimit.Concurrent(client, ratelimit.ConcurrentConfig{
		Name:        "test-autorelease",
		Limit:       2,
		LockTimeout: 5 * time.Second,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	executed := 0
	for i := range 5 {
		err := limiter.WithinLimit(ctx, func() error {
			executed++
			return nil
		})
		if err != nil {
			t.Fatalf("iteration %d failed: %v", i, err)
		}
	}

	if executed != 5 {
		t.Fatalf("expected 5 executions, got %d", executed)
	}
}

func TestConcurrentLimiter_ReleaseFreesSlot(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:concurrent:test-release*")

	limiter := ratelimit.Concurrent(client, ratelimit.ConcurrentConfig{
		Name:        "test-release",
		Limit:       1,
		LockTimeout: time.Minute,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	_, err := limiter.Acquire(ctx)
	if err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}

	// Release should free the slot for reuse
	if err := limiter.Release(ctx); err != nil {
		t.Fatalf("release failed: %v", err)
	}

	waitTime, err := limiter.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire after release failed: %v", err)
	}
	if waitTime != 0 {
		t.Fatal("should acquire immediately after release")
	}
}

func TestConcurrentLimiter_ReleasesMultipleSlotsForSameContext(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:concurrent:test-release-same-context*")

	limiter := ratelimit.Concurrent(client, ratelimit.ConcurrentConfig{
		Name:        "test-release-same-context",
		Limit:       3,
		LockTimeout: time.Minute,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	for i := range 3 {
		waitTime, err := limiter.Acquire(ctx)
		if err != nil {
			t.Fatalf("ConcurrentLimiter.Acquire(%d) error = %v", i, err)
		}
		if waitTime != 0 {
			t.Fatalf("ConcurrentLimiter.Acquire(%d) wait = %v, want 0", i, waitTime)
		}
	}

	held, err := limiter.Held(ctx)
	if err != nil {
		t.Fatalf("ConcurrentLimiter.Held() after acquire error = %v", err)
	}
	if held != 3 {
		t.Fatalf("ConcurrentLimiter.Held() after acquire = %d, want 3", held)
	}

	for i := range 3 {
		if err := limiter.Release(ctx); err != nil {
			t.Fatalf("ConcurrentLimiter.Release(%d) error = %v", i, err)
		}
	}

	held, err = limiter.Held(ctx)
	if err != nil {
		t.Fatalf("ConcurrentLimiter.Held() after release error = %v", err)
	}
	if held != 0 {
		t.Fatalf("ConcurrentLimiter.Held() after release = %d, want 0", held)
	}
}

func TestConcurrentLimiter_WithinLimitReturnsReleaseError(t *testing.T) {
	client := newTestClient(t)
	cleanupClient := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, cleanupClient, "senna:ratelimit:concurrent:test-release-error*")
	t.Cleanup(func() { flushKeys(t, cleanupClient, "senna:ratelimit:concurrent:test-release-error*") })

	limiter := ratelimit.Concurrent(client, ratelimit.ConcurrentConfig{
		Name:        "test-release-error",
		Limit:       1,
		LockTimeout: time.Minute,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	err := limiter.WithinLimit(ctx, func() error {
		return client.Close()
	})
	if err == nil {
		t.Fatal("ConcurrentLimiter.WithinLimit() error = nil, want release error")
	}
}

func TestConcurrentLimiter_WithinLimitPreservesHandlerError(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:concurrent:test-handler-error*")

	limiter := ratelimit.Concurrent(client, ratelimit.ConcurrentConfig{
		Name:        "test-handler-error",
		Limit:       1,
		LockTimeout: time.Minute,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	handlerErr := errors.New("handler failed")
	err := limiter.WithinLimit(ctx, func() error {
		return handlerErr
	})
	if err != handlerErr {
		t.Fatalf("ConcurrentLimiter.WithinLimit() error = %v, want original handler error", err)
	}
}

func TestConcurrentLimiter_WithinLimitReleasesAfterPanic(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:concurrent:test-panic-release*")

	limiter := ratelimit.Concurrent(client, ratelimit.ConcurrentConfig{
		Name:        "test-panic-release",
		Limit:       1,
		LockTimeout: time.Minute,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	didPanic := false
	func() {
		defer func() {
			didPanic = recover() != nil
		}()
		_ = limiter.WithinLimit(ctx, func() error {
			panic("job failed")
		})
	}()

	if !didPanic {
		t.Fatal("ConcurrentLimiter.WithinLimit() did not propagate panic")
	}

	waitTime, err := limiter.Acquire(ctx)
	if err != nil {
		t.Fatalf("ConcurrentLimiter.Acquire() after panic release error = %v", err)
	}
	if waitTime != 0 {
		t.Fatalf("ConcurrentLimiter.Acquire() after panic release wait = %v, want 0", waitTime)
	}
	if err := limiter.Release(ctx); err != nil {
		t.Fatalf("ConcurrentLimiter.Release() after panic reacquire error = %v", err)
	}
}

func TestConcurrentLimiter_AcquireReturnsReclaimError(t *testing.T) {
	client := newTestClient(t)
	cleanupClient := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, cleanupClient, "senna:ratelimit:concurrent:test-reclaim-error*")
	t.Cleanup(func() { flushKeys(t, cleanupClient, "senna:ratelimit:concurrent:test-reclaim-error*") })

	limiter := ratelimit.Concurrent(client, ratelimit.ConcurrentConfig{
		Name:        "test-reclaim-error",
		Limit:       1,
		LockTimeout: time.Minute,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	if waitTime, err := limiter.Acquire(ctx); err != nil || waitTime != 0 {
		t.Fatalf("ConcurrentLimiter.Acquire() before close = (%v, %v), want (0, nil)", waitTime, err)
	}
	if err := limiter.Release(ctx); err != nil {
		t.Fatalf("ConcurrentLimiter.Release() before close error = %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("redis Close() error = %v", err)
	}

	if _, err := limiter.Acquire(ctx); err == nil {
		t.Fatal("ConcurrentLimiter.Acquire() after client close error = nil, want reclaim error")
	}
}

func TestConcurrentLimiter_Concurrent(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:concurrent:test-concurrent*")

	limiter := ratelimit.Concurrent(client, ratelimit.ConcurrentConfig{
		Name:        "test-concurrent",
		Limit:       10,
		LockTimeout: 5 * time.Second,
		WaitTimeout: 2 * time.Second,
		Policy:      ratelimit.PolicyRaise,
	})

	var wg sync.WaitGroup
	var running atomic.Int32
	var maxRunning atomic.Int32
	var errors atomic.Int32

	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := limiter.WithinLimit(ctx, func() error {
				current := running.Add(1)
				for {
					max := maxRunning.Load()
					if current <= max || maxRunning.CompareAndSwap(max, current) {
						break
					}
				}
				time.Sleep(100 * time.Millisecond)
				running.Add(-1)
				return nil
			})
			if err != nil {
				errors.Add(1)
			}
		}()
	}

	wg.Wait()

	if maxRunning.Load() > 10 {
		t.Fatalf("max concurrent should be <= 10, got %d", maxRunning.Load())
	}
}

func TestConcurrentLimiter_Held(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:concurrent:test-held*")

	limiter := ratelimit.Concurrent(client, ratelimit.ConcurrentConfig{
		Name:        "test-held",
		Limit:       5,
		LockTimeout: 5 * time.Second,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	held, err := limiter.Held(ctx)
	if err != nil {
		t.Fatalf("held check failed: %v", err)
	}
	if held != 0 {
		t.Fatalf("expected 0 held, got %d", held)
	}

	for range 3 {
		_, _ = limiter.Acquire(ctx)
	}

	held, err = limiter.Held(ctx)
	if err != nil {
		t.Fatalf("held check failed: %v", err)
	}
	if held != 3 {
		t.Fatalf("expected 3 held, got %d", held)
	}
}
