package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
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

func TestWindowLimiterHonorsLimit(t *testing.T) {
	client := newRedisClient(t)
	prefix := "senna:rl:test:window:" + uuid.NewString()
	t.Cleanup(func() { cleanupKeys(t, client, prefix+"*") })

	l := Window(client, WindowConfig{
		Name:        "w",
		Limit:       2,
		Interval:    time.Second,
		KeyPrefix:   prefix,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      PolicySkip,
	})

	ctx := context.Background()
	waitForRateLimitWindow(time.Second)
	if wait, err := l.Acquire(ctx); err != nil || wait != 0 {
		t.Fatalf("first acquire unexpected: wait=%v err=%v", wait, err)
	}
	if wait, err := l.Acquire(ctx); err != nil || wait != 0 {
		t.Fatalf("second acquire unexpected: wait=%v err=%v", wait, err)
	}
	if wait, err := l.Acquire(ctx); err != nil || wait <= 0 {
		t.Fatalf("expected over limit wait>0 got wait=%v err=%v", wait, err)
	}
}

func TestBucketLimiterOverLimitReturnsRetry(t *testing.T) {
	client := newRedisClient(t)
	prefix := "senna:rl:test:bucket:" + uuid.NewString()
	t.Cleanup(func() { cleanupKeys(t, client, prefix+"*") })

	l := Bucket(client, BucketConfig{
		Name:        "b",
		Limit:       1,
		Interval:    time.Second,
		KeyPrefix:   prefix,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      PolicySkip,
	})

	ctx := context.Background()
	waitForRateLimitWindow(time.Second)
	if wait, err := l.Acquire(ctx); err != nil || wait != 0 {
		t.Fatalf("first acquire unexpected: wait=%v err=%v", wait, err)
	}
	if wait, err := l.Acquire(ctx); err != nil || wait <= 0 {
		t.Fatalf("expected retry wait>0, got wait=%v err=%v", wait, err)
	}
}

func TestPointsLimiterRefills(t *testing.T) {
	client := newRedisClient(t)
	prefix := "senna:rl:test:points:" + uuid.NewString()
	t.Cleanup(func() { cleanupKeys(t, client, prefix+"*") })

	l := Points(client, PointsConfig{
		Name:        "p",
		Capacity:    2,
		RefillTime:  200 * time.Millisecond,
		KeyPrefix:   prefix,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      PolicySkip,
	})

	ctx := context.Background()
	waitForRateLimitWindow(200 * time.Millisecond)
	if wait, err := l.AcquirePoints(ctx, 2); err != nil || wait != 0 {
		t.Fatalf("acquire cost2 unexpected: wait=%v err=%v", wait, err)
	}
	if wait, err := l.Acquire(ctx); err != nil || wait <= 0 {
		t.Fatalf("expected over limit wait>0, got wait=%v err=%v", wait, err)
	}

	time.Sleep(250 * time.Millisecond)
	if wait, err := l.Acquire(ctx); err != nil || wait != 0 {
		t.Fatalf("expected refill and wait=0, got wait=%v err=%v", wait, err)
	}
}

func TestLeakyLimiterDrains(t *testing.T) {
	client := newRedisClient(t)
	prefix := "senna:rl:test:leaky:" + uuid.NewString()
	t.Cleanup(func() { cleanupKeys(t, client, prefix+"*") })

	l := Leaky(client, LeakyConfig{
		Name:        "l",
		Capacity:    2,
		DrainTime:   200 * time.Millisecond,
		KeyPrefix:   prefix,
		WaitTimeout: 100 * time.Millisecond,
		Policy:      PolicySkip,
	})

	ctx := context.Background()
	waitForRateLimitWindow(200 * time.Millisecond)
	if wait, err := l.Acquire(ctx); err != nil || wait != 0 {
		t.Fatalf("first acquire unexpected: wait=%v err=%v", wait, err)
	}
	if wait, err := l.Acquire(ctx); err != nil || wait != 0 {
		t.Fatalf("second acquire unexpected: wait=%v err=%v", wait, err)
	}
	if wait, err := l.Acquire(ctx); err != nil || wait <= 0 {
		t.Fatalf("expected over limit wait>0, got wait=%v err=%v", wait, err)
	}

	time.Sleep(250 * time.Millisecond)
	if wait, err := l.Acquire(ctx); err != nil || wait != 0 {
		t.Fatalf("expected drained and wait=0, got wait=%v err=%v", wait, err)
	}
}

func TestConcurrentLimiterReturnsSlotAfterRelease(t *testing.T) {
	client := newRedisClient(t)
	prefix := "senna:rl:test:concurrent:" + uuid.NewString()
	t.Cleanup(func() { cleanupKeys(t, client, prefix+"*") })

	l := Concurrent(client, ConcurrentConfig{
		Name:        "c",
		Limit:       1,
		WaitTimeout: 200 * time.Millisecond,
		KeyPrefix:   prefix,
		Policy:      PolicySkip,
	})

	ctx := context.Background()
	if wait, err := l.Acquire(ctx); err != nil || wait != 0 {
		t.Fatalf("first acquire unexpected: wait=%v err=%v", wait, err)
	}

	if wait, err := l.Acquire(ctx); err == nil && wait == 0 {
		t.Fatalf("expected over limit error/wait, got wait=%v err=%v", wait, err)
	}

	if err := l.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}

	if wait, err := l.Acquire(ctx); err != nil || wait != 0 {
		t.Fatalf("expected slot after release, got wait=%v err=%v", wait, err)
	}
	_ = l.Release(ctx)
}
