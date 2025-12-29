package senna

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mgomes/senna/internal/keys"
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
	client := redis.NewClient(RedisConfig{Addr: redisAddr()}.Options())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping failed: %v", err)
	}
	return client
}

func cleanupNamespace(t *testing.T, client *redis.Client, namespace string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cursor := uint64(0)
	pattern := namespace + ":*"

	for {
		keys, next, err := client.Scan(ctx, cursor, pattern, 200).Result()
		if err != nil {
			t.Fatalf("failed to scan keys: %v", err)
		}
		if len(keys) > 0 {
			if err := client.Del(ctx, keys...).Err(); err != nil {
				t.Fatalf("failed to delete keys: %v", err)
			}
		}
		if next == 0 {
			break
		}
		cursor = next
	}
}

func TestEnqueueFetchAckRemovesInFlight(t *testing.T) {
	client := newRedisClient(t)
	namespace := "senna-test-" + uuid.NewString()
	t.Cleanup(func() { cleanupNamespace(t, client, namespace) })

	c, err := NewClient(&ClientConfig{
		Redis:     RedisConfig{Addr: redisAddr()},
		Namespace: namespace,
		Settings:  DefaultClientSettings(),
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job, err := c.Enqueue(ctx, "demo", map[string]any{"x": 1})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	k := keys.New(namespace)
	f := newFetcher(c.Redis(), k, []QueueConfig{{Name: "default", Priority: 1}}, 100*time.Millisecond)

	fetchCtx, fetchCancel := context.WithTimeout(ctx, 2*time.Second)
	defer fetchCancel()
	fetched, err := f.Fetch(fetchCtx, "worker-1")
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if fetched == nil {
		t.Fatalf("expected job, got nil")
	}
	if fetched.ID != job.ID {
		t.Fatalf("expected job id %s, got %s", job.ID, fetched.ID)
	}
	if fetched.raw == "" {
		t.Fatalf("expected raw payload to be populated")
	}

	if err := f.Ack(ctx, "worker-1", fetched); err != nil {
		t.Fatalf("ack: %v", err)
	}

	if n, _ := c.Redis().LLen(ctx, k.InFlight("worker-1")).Result(); n != 0 {
		t.Fatalf("expected inflight empty, got %d", n)
	}
	if n, _ := c.Redis().LLen(ctx, k.Queue("default")).Result(); n != 0 {
		t.Fatalf("expected queue empty, got %d", n)
	}
}

func TestNackMovesToRetryAndBumpsRetryCount(t *testing.T) {
	client := newRedisClient(t)
	namespace := "senna-test-" + uuid.NewString()
	t.Cleanup(func() { cleanupNamespace(t, client, namespace) })

	c, err := NewClient(&ClientConfig{
		Redis:     RedisConfig{Addr: redisAddr()},
		Namespace: namespace,
		Settings:  DefaultClientSettings(),
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := c.Enqueue(ctx, "demo", map[string]any{"x": 1}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	k := keys.New(namespace)
	f := newFetcher(c.Redis(), k, []QueueConfig{{Name: "default", Priority: 1}}, 100*time.Millisecond)

	fetchCtx, fetchCancel := context.WithTimeout(ctx, 2*time.Second)
	defer fetchCancel()
	fetched, err := f.Fetch(fetchCtx, "worker-1")
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}

	if fetched.RetryCount != 0 {
		t.Fatalf("expected retry_count 0, got %d", fetched.RetryCount)
	}

	retryIn := 2 * time.Second
	if err := f.Nack(ctx, "worker-1", fetched, retryIn); err != nil {
		t.Fatalf("nack: %v", err)
	}

	if fetched.RetryCount != 1 {
		t.Fatalf("expected retry_count 1 after nack, got %d", fetched.RetryCount)
	}

	retries, err := c.Redis().ZRangeWithScores(ctx, k.Retry(), 0, -1).Result()
	if err != nil {
		t.Fatalf("read retry set: %v", err)
	}
	if len(retries) != 1 {
		t.Fatalf("expected 1 job in retry set, got %d", len(retries))
	}
	if retries[0].Score < float64(time.Now().Unix()) {
		t.Fatalf("expected retry score to be in the future")
	}
}

func TestSchedulerMovesDueJobs(t *testing.T) {
	client := newRedisClient(t)
	namespace := "senna-test-" + uuid.NewString()
	t.Cleanup(func() { cleanupNamespace(t, client, namespace) })

	w, err := NewWorker(&WorkerConfig{
		Redis:     RedisConfig{Addr: redisAddr()},
		Namespace: namespace,
		Settings:  DefaultWorkerSettings(),
	})
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}
	t.Cleanup(func() { w.Redis().Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	job := NewJob("demo", map[string]any{"x": 1})
	data, _ := job.Marshal()
	if err := w.redis.ZAdd(ctx, w.keys.Scheduled(), redis.Z{
		Score:  float64(time.Now().Add(-time.Second).Unix()),
		Member: string(data),
	}).Err(); err != nil {
		t.Fatalf("seed scheduled: %v", err)
	}

	w.enqueueScheduled(ctx)

	if n, _ := w.redis.ZCard(ctx, w.keys.Scheduled()).Result(); n != 0 {
		t.Fatalf("expected scheduled empty, got %d", n)
	}
	if n, _ := w.redis.LLen(ctx, w.keys.Queue("default")).Result(); n != 1 {
		t.Fatalf("expected queue to have job, got %d", n)
	}
}

func TestUniqueRequiresTTL(t *testing.T) {
	client := newRedisClient(t)
	namespace := "senna-test-" + uuid.NewString()
	t.Cleanup(func() { cleanupNamespace(t, client, namespace) })

	c, err := NewClient(&ClientConfig{
		Redis:     RedisConfig{Addr: redisAddr()},
		Namespace: namespace,
		Settings:  DefaultClientSettings(),
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := c.Enqueue(ctx, "demo", map[string]any{"x": 1}, WithUniqueKey("uniq", 0)); err == nil {
		t.Fatalf("expected error when TTL is zero for unique key")
	}
}
