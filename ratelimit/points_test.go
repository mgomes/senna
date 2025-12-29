package ratelimit_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgomes/senna/ratelimit"
)

func TestPointsLimiter_Basic(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:points:test-basic*")

	limiter := ratelimit.Points(client, ratelimit.PointsConfig{
		Name:        "test-basic",
		Capacity:    100,
		RefillTime:  time.Second,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	for i := 0; i < 100; i++ {
		waitTime, err := limiter.Acquire(ctx)
		if err != nil {
			t.Fatalf("acquire %d failed: %v", i, err)
		}
		if waitTime != 0 {
			t.Fatalf("acquire %d should not wait", i)
		}
	}

	waitTime, err := limiter.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire 101 failed: %v", err)
	}
	if waitTime == 0 {
		t.Fatal("acquire 101 should have wait time")
	}
}

func TestPointsLimiter_VariableCost(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:points:test-variable*")

	limiter := ratelimit.Points(client, ratelimit.PointsConfig{
		Name:        "test-variable",
		Capacity:    100,
		RefillTime:  time.Second,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	waitTime, err := limiter.AcquirePoints(ctx, 50)
	if err != nil {
		t.Fatalf("acquire 50 failed: %v", err)
	}
	if waitTime != 0 {
		t.Fatal("acquire 50 should not wait")
	}

	waitTime, err = limiter.AcquirePoints(ctx, 50)
	if err != nil {
		t.Fatalf("acquire another 50 failed: %v", err)
	}
	if waitTime != 0 {
		t.Fatal("acquire another 50 should not wait")
	}

	waitTime, _ = limiter.AcquirePoints(ctx, 1)
	if waitTime == 0 {
		t.Fatal("should be out of points")
	}
}

func TestPointsLimiter_WithinLimitCost(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:points:test-cost*")

	limiter := ratelimit.Points(client, ratelimit.PointsConfig{
		Name:        "test-cost",
		Capacity:    100,
		RefillTime:  time.Second,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	executed := 0
	for i := 0; i < 5; i++ {
		err := limiter.WithinLimitCost(ctx, 30, func() error {
			executed++
			return nil
		})
		if i < 3 && err != nil {
			t.Fatalf("iteration %d should succeed: %v", i, err)
		}
	}

	if executed != 3 {
		t.Fatalf("expected 3 executions (90 points used), got %d", executed)
	}
}

func TestPointsLimiter_EstimateAndAdjust(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:points:test-estimate*")

	limiter := ratelimit.Points(client, ratelimit.PointsConfig{
		Name:        "test-estimate",
		Capacity:    100,
		RefillTime:  time.Second,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	err := limiter.WithinLimitEstimate(ctx, 50, func(h *ratelimit.PointsHandle) error {
		return h.PointsUsed(20)
	})
	if err != nil {
		t.Fatalf("estimate operation failed: %v", err)
	}

	available, err := limiter.AvailablePoints(ctx)
	if err != nil {
		t.Fatalf("available points check failed: %v", err)
	}
	if available < 75 || available > 85 {
		t.Fatalf("expected ~80 available points (100-50+30 refund), got %f", available)
	}
}

func TestPointsLimiter_Refill(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:points:test-refill*")

	limiter := ratelimit.Points(client, ratelimit.PointsConfig{
		Name:        "test-refill",
		Capacity:    100,
		RefillTime:  500 * time.Millisecond,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	for i := 0; i < 100; i++ {
		limiter.Acquire(ctx)
	}

	waitTime, _ := limiter.Acquire(ctx)
	if waitTime == 0 {
		t.Fatal("should be out of points")
	}

	time.Sleep(300 * time.Millisecond)

	waitTime, err := limiter.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire after refill failed: %v", err)
	}
	if waitTime != 0 {
		t.Fatal("should have refilled some points")
	}
}

func TestPointsLimiter_AvailablePoints(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:points:test-available*")

	limiter := ratelimit.Points(client, ratelimit.PointsConfig{
		Name:        "test-available",
		Capacity:    100,
		RefillTime:  time.Second,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	available, err := limiter.AvailablePoints(ctx)
	if err != nil {
		t.Fatalf("available check failed: %v", err)
	}
	if available != 100 {
		t.Fatalf("expected 100 available, got %f", available)
	}

	limiter.AcquirePoints(ctx, 30)

	available, err = limiter.AvailablePoints(ctx)
	if err != nil {
		t.Fatalf("available check failed: %v", err)
	}
	if available < 68 || available > 72 {
		t.Fatalf("expected ~70 available, got %f", available)
	}
}

func TestPointsLimiter_Concurrent(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:points:test-concurrent*")

	limiter := ratelimit.Points(client, ratelimit.PointsConfig{
		Name:        "test-concurrent",
		Capacity:    100,
		RefillTime:  time.Second,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	var wg sync.WaitGroup
	var totalPoints atomic.Int32

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := limiter.WithinLimitCost(ctx, 10, func() error {
				totalPoints.Add(10)
				return nil
			})
			_ = err
		}()
	}

	wg.Wait()

	total := totalPoints.Load()
	if total != 100 {
		t.Fatalf("expected exactly 100 points used, got %d", total)
	}
}
