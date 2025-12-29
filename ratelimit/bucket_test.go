package ratelimit_test

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgomes/senna/ratelimit"
	"github.com/redis/go-redis/v9"
)

func getRedisAddr() string {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	return addr
}

func newTestClient(t *testing.T) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr: getRedisAddr(),
	})
	t.Cleanup(func() {
		_ = client.Close()
	})
	return client
}

func flushKeys(t *testing.T, client *redis.Client, pattern string) {
	ctx := context.Background()
	keys, err := client.Keys(ctx, pattern).Result()
	if err != nil {
		t.Fatalf("failed to get keys: %v", err)
	}
	if len(keys) > 0 {
		client.Del(ctx, keys...)
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

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:        "test-concurrent",
		Limit:       100,
		Interval:    time.Second,
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

	for i := range 3 {
		_, err := limiter.Acquire(ctx)
		if err != nil {
			t.Fatalf("first batch acquire %d failed: %v", i, err)
		}
	}

	waitTime, _ := limiter.Acquire(ctx)
	if waitTime == 0 {
		t.Fatal("should be rate limited")
	}

	time.Sleep(600 * time.Millisecond)

	for i := range 3 {
		waitTime, err := limiter.Acquire(ctx)
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
		Interval:    time.Second,
		WaitTimeout: 50 * time.Millisecond,
		Policy:      ratelimit.PolicyRaise,
	})

	for i := range 2 {
		_, err := limiter.Acquire(ctx)
		if err != nil {
			t.Fatalf("acquire %d failed: %v", i, err)
		}
	}

	_, err := limiter.Acquire(ctx)
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

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:        "test-cancel",
		Limit:       1,
		Interval:    time.Second,
		WaitTimeout: 5 * time.Second,
		Policy:      ratelimit.PolicyRaise,
	})

	ctx := context.Background()
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

			for i := range 5 {
				waitTime, err := limiter.Acquire(ctx)
				if err != nil {
					t.Fatalf("acquire %d failed: %v", i, err)
				}
				if waitTime != 0 {
					t.Fatalf("acquire %d should not wait", i)
				}
			}

			waitTime, _ := limiter.Acquire(ctx)
			if waitTime == 0 {
				t.Fatal("should be rate limited")
			}
		})
	}
}
