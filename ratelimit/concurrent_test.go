package ratelimit_test

import (
	"context"
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

	for i := 0; i < 3; i++ {
		waitTime, err := limiter.Acquire(ctx)
		if err != nil {
			t.Fatalf("acquire %d failed: %v", i, err)
		}
		if waitTime != 0 {
			t.Fatalf("acquire %d should not wait", i)
		}
		acquired = append(acquired, func() { limiter.Release(ctx) })
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

	for i := 0; i < 10; i++ {
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
	for i := 0; i < 5; i++ {
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

func TestConcurrentLimiter_LockReclaim(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:concurrent:test-reclaim*")

	limiter := ratelimit.Concurrent(client, ratelimit.ConcurrentConfig{
		Name:        "test-reclaim",
		Limit:       1,
		LockTimeout: 200 * time.Millisecond,
		WaitTimeout: 500 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	_, err := limiter.Acquire(ctx)
	if err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	waitTime, err := limiter.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire after reclaim failed: %v", err)
	}
	if waitTime != 0 {
		t.Fatal("should acquire immediately after lock timeout")
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

	for i := 0; i < 50; i++ {
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

	for i := 0; i < 3; i++ {
		limiter.Acquire(ctx)
	}

	held, err = limiter.Held(ctx)
	if err != nil {
		t.Fatalf("held check failed: %v", err)
	}
	if held != 3 {
		t.Fatalf("expected 3 held, got %d", held)
	}
}
