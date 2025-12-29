package ratelimit

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func redisAddr() string {
	if url := os.Getenv("REDIS_URL"); url != "" {
		return url
	}
	return "127.0.0.1:6379"
}

func newRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: redisAddr()})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping failed: %v", err)
	}

	return client
}

func cleanupKeys(t *testing.T, client *redis.Client, prefix string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var cursor uint64
	pattern := prefix + "*"
	for {
		keys, next, err := client.Scan(ctx, cursor, pattern, 200).Result()
		if err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		if len(keys) > 0 {
			if err := client.Del(ctx, keys...).Err(); err != nil {
				t.Fatalf("del failed: %v", err)
			}
		}
		if next == 0 {
			break
		}
		cursor = next
	}
}

func TestWindowLimiterHonorsLimit(t *testing.T) {
	client := newRedisClient(t)
	prefix := "senna:rl:test:window:" + uuid.NewString()
	t.Cleanup(func() { cleanupKeys(t, client, prefix) })

	l := Window(client, WindowConfig{
		Name:      "w",
		Limit:     2,
		Interval:  time.Second,
		KeyPrefix: prefix,
	})

	ctx := context.Background()
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
	t.Cleanup(func() { cleanupKeys(t, client, prefix) })

	l := Bucket(client, BucketConfig{
		Name:      "b",
		Limit:     1,
		Interval:  time.Second,
		KeyPrefix: prefix,
	})

	ctx := context.Background()
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
	t.Cleanup(func() { cleanupKeys(t, client, prefix) })

	l := Points(client, PointsConfig{
		Name:       "p",
		Capacity:   2,
		RefillTime: 200 * time.Millisecond,
		KeyPrefix:  prefix,
	})

	ctx := context.Background()
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
	t.Cleanup(func() { cleanupKeys(t, client, prefix) })

	l := Leaky(client, LeakyConfig{
		Name:        "l",
		Capacity:    2,
		DrainTime:   200 * time.Millisecond,
		KeyPrefix:   prefix,
		WaitTimeout: 500 * time.Millisecond,
	})

	ctx := context.Background()
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
	t.Cleanup(func() { cleanupKeys(t, client, prefix) })

	l := Concurrent(client, ConcurrentConfig{
		Name:        "c",
		Limit:       1,
		WaitTimeout: 200 * time.Millisecond,
		KeyPrefix:   prefix,
	})

	ctx := context.Background()
	if wait, err := l.Acquire(ctx); err != nil || wait != 0 {
		t.Fatalf("first acquire unexpected: wait=%v err=%v", wait, err)
	}

	if wait, err := l.Acquire(ctx); err == nil || wait == 0 {
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
