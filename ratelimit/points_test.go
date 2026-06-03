package ratelimit_test

import (
	"context"
	"errors"
	"strconv"
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
		Capacity:    10,
		RefillTime:  time.Second,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	// Exhaust all points (with extra to account for refilling during acquires)
	waitForRateLimitWindow(time.Second)
	for i := range 12 {
		waitTime, err := limiter.Acquire(ctx)
		if err != nil {
			t.Fatalf("acquire %d failed: %v", i, err)
		}
		if i < 10 && waitTime != 0 {
			t.Fatalf("acquire %d should not wait", i)
		}
	}

	waitTime, err := limiter.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire 13 failed: %v", err)
	}
	if waitTime == 0 {
		t.Fatal("acquire 13 should have wait time")
	}
}

func TestPointsLimiter_SubMicroIntervalFloors(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:points:test-submicro*")

	limiter := ratelimit.Points(client, ratelimit.PointsConfig{
		Name:       "test-submicro",
		Capacity:   2,
		RefillTime: 500 * time.Nanosecond, // Sub-micro; should floor internally
		Policy:     ratelimit.PolicySkip,
	})

	// Consume available points.
	for range 2 {
		if wait, err := limiter.Acquire(ctx); err != nil || wait != 0 {
			t.Fatalf("unexpected wait/err: wait=%v err=%v", wait, err)
		}
	}

	// Third call should not panic/divide by zero; wait may be tiny due to fast refill.
	if _, err := limiter.Acquire(ctx); err != nil && err != context.DeadlineExceeded {
		t.Fatalf("unexpected error: %v", err)
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

	waitForRateLimitWindow(time.Second)
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
	waitForRateLimitWindow(time.Second)
	for i := range 5 {
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

	waitForRateLimitWindow(time.Second)
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
		Capacity:    10,
		RefillTime:  200 * time.Millisecond,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	// Exhaust all points (with a few extra to account for refilling during acquires)
	waitForRateLimitWindow(200 * time.Millisecond)
	for range 12 {
		_, _ = limiter.Acquire(ctx)
	}

	waitTime, _ := limiter.Acquire(ctx)
	if waitTime == 0 {
		t.Fatal("should be out of points")
	}

	// Wait for refill (refills 10 points over 200ms, so 100ms = ~5 points)
	time.Sleep(150 * time.Millisecond)

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

	waitForRateLimitWindow(time.Second)
	available, err := limiter.AvailablePoints(ctx)
	if err != nil {
		t.Fatalf("available check failed: %v", err)
	}
	if available != 100 {
		t.Fatalf("expected 100 available, got %f", available)
	}

	_, _ = limiter.AcquirePoints(ctx, 30)

	available, err = limiter.AvailablePoints(ctx)
	if err != nil {
		t.Fatalf("available check failed: %v", err)
	}
	if available < 68 || available > 72 {
		t.Fatalf("expected ~70 available, got %f", available)
	}
}

func TestPointsLimiter_AvailablePointsRejectsMalformedState(t *testing.T) {
	client := newTestClient(t)
	ctx := context.Background()
	flushKeys(t, client, "senna:ratelimit:points:test-available-malformed*")

	limiter := ratelimit.Points(client, ratelimit.PointsConfig{
		Name:       "test-available-malformed",
		Capacity:   100,
		RefillTime: time.Second,
	})

	tests := []struct {
		name       string
		field      string
		value      string
		wantNumErr bool
	}{
		{name: "points text", field: "points", value: "not-a-number", wantNumErr: true},
		{name: "points NaN", field: "points", value: "NaN"},
		{name: "points positive infinity", field: "points", value: "+Inf"},
		{name: "points negative infinity", field: "points", value: "-Inf"},
		{name: "last refill text", field: "last_refill", value: "not-a-number", wantNumErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := client.Del(ctx, "senna:ratelimit:points:test-available-malformed").Err(); err != nil {
				t.Fatalf("Del malformed points limiter state error = %v", err)
			}
			if err := client.HSet(ctx, "senna:ratelimit:points:test-available-malformed", tt.field, tt.value).Err(); err != nil {
				t.Fatalf("HSet malformed points limiter %s=%q error = %v", tt.field, tt.value, err)
			}

			_, err := limiter.AvailablePoints(ctx)
			if err == nil {
				t.Fatalf("PointsLimiter.AvailablePoints() with malformed %s=%q error = nil, want malformed state error", tt.field, tt.value)
			}
			if tt.wantNumErr {
				var numberErr *strconv.NumError
				if !errors.As(err, &numberErr) {
					t.Fatalf("PointsLimiter.AvailablePoints() with malformed %s=%q error = %v, want strconv.NumError", tt.field, tt.value, err)
				}
			}
		})
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

	waitForRateLimitWindow(time.Second)
	var wg sync.WaitGroup
	var totalPoints atomic.Int32

	for range 50 {
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
