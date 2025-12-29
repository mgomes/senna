package ratelimit_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mgomes/senna/ratelimit"
)

func TestUnlimitedLimiter_AlwaysAllows(t *testing.T) {
	limiter := ratelimit.Unlimited("test")
	ctx := context.Background()

	for range 1000 {
		waitTime, err := limiter.Acquire(ctx)
		if err != nil {
			t.Fatalf("acquire failed: %v", err)
		}
		if waitTime != 0 {
			t.Fatal("acquire should never wait")
		}
	}
}

func TestUnlimitedLimiter_WithinLimit(t *testing.T) {
	limiter := ratelimit.Unlimited("test")
	ctx := context.Background()

	executed := 0
	for range 100 {
		err := limiter.WithinLimit(ctx, func() error {
			executed++
			return nil
		})
		if err != nil {
			t.Fatalf("WithinLimit failed: %v", err)
		}
	}

	if executed != 100 {
		t.Fatalf("expected 100 executions, got %d", executed)
	}
}

func TestUnlimitedLimiter_PropagatesErrors(t *testing.T) {
	limiter := ratelimit.Unlimited("test")
	ctx := context.Background()

	expectedErr := errors.New("test error")
	err := limiter.WithinLimit(ctx, func() error {
		return expectedErr
	})

	if err != expectedErr {
		t.Fatalf("expected error to propagate, got %v", err)
	}
}

func TestUnlimitedLimiter_Concurrent(t *testing.T) {
	limiter := ratelimit.Unlimited("test")
	ctx := context.Background()

	var wg sync.WaitGroup
	var successCount atomic.Int32

	for range 1000 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := limiter.WithinLimit(ctx, func() error {
				successCount.Add(1)
				return nil
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}

	wg.Wait()

	if successCount.Load() != 1000 {
		t.Fatalf("expected 1000 successes, got %d", successCount.Load())
	}
}

func TestUnlimitedLimiter_Name(t *testing.T) {
	limiter := ratelimit.Unlimited("my-limiter")

	if limiter.Name() != "my-limiter" {
		t.Fatalf("expected name 'my-limiter', got '%s'", limiter.Name())
	}
}

func TestUnlimitedLimiter_Release(t *testing.T) {
	limiter := ratelimit.Unlimited("test")
	ctx := context.Background()

	err := limiter.Release(ctx)
	if err != nil {
		t.Fatalf("release should never fail: %v", err)
	}
}
