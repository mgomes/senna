package worker

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/mgomes/senna"
	"github.com/mgomes/senna/internal/keys"
	"github.com/redis/go-redis/v9"
)

func getTestRedisAddr() string {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	return addr
}

func newTestRedisClient(t *testing.T) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr: getTestRedisAddr(),
	})
	t.Cleanup(func() {
		_ = client.Close()
	})
	return client
}

func flushTestKeys(t *testing.T, client *redis.Client, pattern string) {
	ctx := context.Background()
	keys, err := client.Keys(ctx, pattern).Result()
	if err != nil {
		t.Fatalf("failed to get keys: %v", err)
	}
	if len(keys) > 0 {
		client.Del(ctx, keys...)
	}
}

func TestFetcher_SelectQueue_SingleQueue(t *testing.T) {
	client := newTestRedisClient(t)
	k := keys.New("test-fetcher")

	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "default", Priority: 1},
	}, 100*time.Millisecond, false)

	for range 10 {
		queue := f.selectQueueWeighted()
		if queue != "default" {
			t.Errorf("expected 'default', got '%s'", queue)
		}
	}
}

func TestFetcher_SelectQueue_MultipleQueues(t *testing.T) {
	client := newTestRedisClient(t)
	k := keys.New("test-fetcher")

	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "critical", Priority: 10},
		{Name: "default", Priority: 5},
		{Name: "low", Priority: 1},
	}, 100*time.Millisecond, false)

	counts := make(map[string]int)
	iterations := 10000

	for i := 0; i < iterations; i++ {
		queue := f.selectQueueWeighted()
		counts[queue]++
	}

	criticalRatio := float64(counts["critical"]) / float64(iterations)
	defaultRatio := float64(counts["default"]) / float64(iterations)
	lowRatio := float64(counts["low"]) / float64(iterations)

	if criticalRatio < 0.5 || criticalRatio > 0.7 {
		t.Errorf("critical queue ratio should be ~0.625, got %f", criticalRatio)
	}
	if defaultRatio < 0.2 || defaultRatio > 0.4 {
		t.Errorf("default queue ratio should be ~0.3125, got %f", defaultRatio)
	}
	if lowRatio < 0.03 || lowRatio > 0.1 {
		t.Errorf("low queue ratio should be ~0.0625, got %f", lowRatio)
	}
}

func TestFetcher_SelectQueue_PausedQueues(t *testing.T) {
	client := newTestRedisClient(t)
	k := keys.New("test-fetcher")

	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "critical", Priority: 10, Paused: true},
		{Name: "default", Priority: 5},
	}, 100*time.Millisecond, false)

	for range 100 {
		queue := f.selectQueueWeighted()
		if queue != "default" {
			t.Errorf("expected 'default' (critical is paused), got '%s'", queue)
		}
	}
}

func TestFetcher_SelectQueue_AllPaused(t *testing.T) {
	client := newTestRedisClient(t)
	k := keys.New("test-fetcher")

	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "critical", Priority: 10, Paused: true},
		{Name: "default", Priority: 5, Paused: true},
	}, 100*time.Millisecond, false)

	queue := f.selectQueueWeighted()
	if queue != "" {
		t.Errorf("expected empty string when all paused, got '%s'", queue)
	}
}

func TestFetcher_SelectQueue_ZeroPriority(t *testing.T) {
	client := newTestRedisClient(t)
	k := keys.New("test-fetcher")

	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "default", Priority: 0},
	}, 100*time.Millisecond, false)

	queue := f.selectQueueWeighted()
	if queue != "default" {
		t.Errorf("expected 'default' (priority normalized to 1), got '%s'", queue)
	}
}

func TestFetcher_Fetch_Success(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-fetch:*")

	k := keys.New("test-fetch")
	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "default", Priority: 1},
	}, 100*time.Millisecond, false)

	ctx := context.Background()
	job := senna.NewJob("test_job", map[string]any{"id": 123})
	data, _ := job.Marshal()
	client.LPush(ctx, k.Queue("default"), string(data))

	fetched, err := f.Fetch(ctx, "worker-1")
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if fetched == nil {
		t.Fatal("expected job, got nil")
	}
	if fetched.ID != job.ID {
		t.Errorf("expected job ID '%s', got '%s'", job.ID, fetched.ID)
	}
	if fetched.Type != "test_job" {
		t.Errorf("expected job type 'test_job', got '%s'", fetched.Type)
	}

	inFlightLen, _ := client.LLen(ctx, k.InFlight("worker-1")).Result()
	if inFlightLen != 1 {
		t.Errorf("expected 1 job in-flight, got %d", inFlightLen)
	}
}

func TestFetcher_Fetch_EmptyQueue(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-fetch-empty:*")

	k := keys.New("test-fetch-empty")
	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "default", Priority: 1},
	}, 50*time.Millisecond, false)

	ctx := context.Background()
	fetched, err := f.Fetch(ctx, "worker-1")
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if fetched != nil {
		t.Errorf("expected nil for empty queue, got %v", fetched)
	}
}

func TestFetcher_Fetch_ContextCanceled(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-fetch-cancel:*")

	k := keys.New("test-fetch-cancel")
	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "default", Priority: 1},
	}, 5*time.Second, false)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := f.Fetch(ctx, "worker-1")
	elapsed := time.Since(start)

	if elapsed > time.Second {
		t.Errorf("fetch should have been canceled quickly, took %v", elapsed)
	}
	_ = err
}

func TestFetcher_Ack_RemovesFromInFlight(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-ack:*")

	k := keys.New("test-ack")
	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "default", Priority: 1},
	}, 100*time.Millisecond, false)

	ctx := context.Background()

	job := senna.NewJob("test_job", nil)
	data, _ := job.Marshal()
	client.LPush(ctx, k.Queue("default"), string(data))

	fetched, _ := f.Fetch(ctx, "worker-1")

	err := f.Ack(ctx, "worker-1", fetched)
	if err != nil {
		t.Fatalf("ack failed: %v", err)
	}

	inFlightLen, _ := client.LLen(ctx, k.InFlight("worker-1")).Result()
	if inFlightLen != 0 {
		t.Errorf("expected 0 jobs in-flight after ack, got %d", inFlightLen)
	}
}

func TestFetcher_Ack_CleansUniqueKey(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-ack-unique:*")

	k := keys.New("test-ack-unique")
	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "default", Priority: 1},
	}, 100*time.Millisecond, false)

	ctx := context.Background()

	job := senna.NewJob("test_job", nil)
	job.UniqueKey = "user:123:sync"
	data, _ := job.Marshal()

	client.Set(ctx, k.Unique(job.UniqueKey), job.ID, time.Hour)
	client.LPush(ctx, k.Queue("default"), string(data))

	fetched, _ := f.Fetch(ctx, "worker-1")

	err := f.Ack(ctx, "worker-1", fetched)
	if err != nil {
		t.Fatalf("ack failed: %v", err)
	}

	exists, _ := client.Exists(ctx, k.Unique(job.UniqueKey)).Result()
	if exists != 0 {
		t.Error("unique key should be deleted after ack")
	}
}

func TestFetcher_Nack_SchedulesRetry(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-nack:*")

	k := keys.New("test-nack")
	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "default", Priority: 1},
	}, 100*time.Millisecond, false)

	ctx := context.Background()

	job := senna.NewJob("test_job", nil)
	data, _ := job.Marshal()
	client.LPush(ctx, k.Queue("default"), string(data))

	fetched, _ := f.Fetch(ctx, "worker-1")

	err := f.Nack(ctx, "worker-1", fetched, 5*time.Second)
	if err != nil {
		t.Fatalf("nack failed: %v", err)
	}

	inFlightLen, _ := client.LLen(ctx, k.InFlight("worker-1")).Result()
	if inFlightLen != 0 {
		t.Errorf("expected 0 jobs in-flight after nack, got %d", inFlightLen)
	}

	retryLen, _ := client.ZCard(ctx, k.Retry()).Result()
	if retryLen != 1 {
		t.Errorf("expected 1 job in retry set, got %d", retryLen)
	}
}

func TestFetcher_Nack_IncrementsRetryCount(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-nack-count:*")

	k := keys.New("test-nack-count")
	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "default", Priority: 1},
	}, 100*time.Millisecond, false)

	ctx := context.Background()

	job := senna.NewJob("test_job", nil)
	job.RetryCount = 2
	data, _ := job.Marshal()
	client.LPush(ctx, k.Queue("default"), string(data))

	fetched, _ := f.Fetch(ctx, "worker-1")

	_ = f.Nack(ctx, "worker-1", fetched, time.Second)

	items, _ := client.ZRange(ctx, k.Retry(), 0, -1).Result()
	if len(items) != 1 {
		t.Fatalf("expected 1 item in retry, got %d", len(items))
	}

	retried, _ := senna.UnmarshalJob([]byte(items[0]))
	if retried.RetryCount != 3 {
		t.Errorf("expected RetryCount 3, got %d", retried.RetryCount)
	}
}

func TestFetcher_Nack_NoRetry(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-nack-no-retry:*")

	k := keys.New("test-nack-no-retry")
	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "default", Priority: 1},
	}, 100*time.Millisecond, false)

	ctx := context.Background()

	job := senna.NewJob("test_job", nil)
	data, _ := job.Marshal()
	client.LPush(ctx, k.Queue("default"), string(data))

	fetched, _ := f.Fetch(ctx, "worker-1")

	err := f.Nack(ctx, "worker-1", fetched, 0)
	if err != nil {
		t.Fatalf("nack failed: %v", err)
	}

	retryLen, _ := client.ZCard(ctx, k.Retry()).Result()
	if retryLen != 0 {
		t.Errorf("expected 0 jobs in retry set (retryIn=0), got %d", retryLen)
	}
}

func TestFetcher_MoveToDead_AddsToDeadSet(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-dead:*")

	k := keys.New("test-dead")
	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "default", Priority: 1},
	}, 100*time.Millisecond, false)

	ctx := context.Background()

	job := senna.NewJob("test_job", nil)
	data, _ := job.Marshal()
	client.LPush(ctx, k.Queue("default"), string(data))

	fetched, _ := f.Fetch(ctx, "worker-1")

	err := f.MoveToDead(ctx, "worker-1", fetched)
	if err != nil {
		t.Fatalf("move to dead failed: %v", err)
	}

	inFlightLen, _ := client.LLen(ctx, k.InFlight("worker-1")).Result()
	if inFlightLen != 0 {
		t.Errorf("expected 0 jobs in-flight, got %d", inFlightLen)
	}

	deadLen, _ := client.ZCard(ctx, k.Dead()).Result()
	if deadLen != 1 {
		t.Errorf("expected 1 job in dead set, got %d", deadLen)
	}
}

func TestFetcher_MoveToDead_SetsFailedAt(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-dead-time:*")

	k := keys.New("test-dead-time")
	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "default", Priority: 1},
	}, 100*time.Millisecond, false)

	ctx := context.Background()

	job := senna.NewJob("test_job", nil)
	data, _ := job.Marshal()
	client.LPush(ctx, k.Queue("default"), string(data))

	fetched, _ := f.Fetch(ctx, "worker-1")

	before := time.Now()
	_ = f.MoveToDead(ctx, "worker-1", fetched)
	after := time.Now()

	items, _ := client.ZRange(ctx, k.Dead(), 0, -1).Result()
	if len(items) != 1 {
		t.Fatalf("expected 1 item in dead, got %d", len(items))
	}

	dead, _ := senna.UnmarshalJob([]byte(items[0]))
	if dead.FailedAt == nil {
		t.Fatal("expected FailedAt to be set")
	}
	if dead.FailedAt.Before(before) || dead.FailedAt.After(after) {
		t.Errorf("FailedAt should be between %v and %v, got %v", before, after, dead.FailedAt)
	}
}

func TestFetcher_MoveToDead_CleansUniqueKey(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-dead-unique:*")

	k := keys.New("test-dead-unique")
	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "default", Priority: 1},
	}, 100*time.Millisecond, false)

	ctx := context.Background()

	job := senna.NewJob("test_job", nil)
	job.UniqueKey = "order:456:process"
	data, _ := job.Marshal()

	client.Set(ctx, k.Unique(job.UniqueKey), job.ID, time.Hour)
	client.LPush(ctx, k.Queue("default"), string(data))

	fetched, _ := f.Fetch(ctx, "worker-1")

	_ = f.MoveToDead(ctx, "worker-1", fetched)

	exists, _ := client.Exists(ctx, k.Unique(job.UniqueKey)).Result()
	if exists != 0 {
		t.Error("unique key should be deleted after move to dead")
	}
}

func TestFetcher_AckWithoutRaw(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-ack-noraw:*")

	k := keys.New("test-ack-noraw")
	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "default", Priority: 1},
	}, 100*time.Millisecond, false)

	ctx := context.Background()

	job := senna.NewJob("test_job", nil)
	data, _ := job.Marshal()
	client.LPush(ctx, k.InFlight("worker-1"), string(data))

	job.SetRaw("")

	err := f.Ack(ctx, "worker-1", job)
	if err != nil {
		t.Fatalf("ack failed: %v", err)
	}

	inFlightLen, _ := client.LLen(ctx, k.InFlight("worker-1")).Result()
	if inFlightLen != 0 {
		t.Errorf("expected 0 jobs in-flight, got %d", inFlightLen)
	}
}

func TestFetcher_StrictPriority_ProcessesHighPriorityFirst(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-strict:*")

	k := keys.New("test-strict")

	// Create fetcher with strict priority enabled
	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "low", Priority: 1},
		{Name: "critical", Priority: 10},
		{Name: "default", Priority: 5},
	}, 100*time.Millisecond, true) // strict = true

	ctx := context.Background()

	// Add jobs to all queues
	lowJob := senna.NewJob("low_job", nil)
	lowJob.Queue = "low"
	lowData, _ := lowJob.Marshal()
	client.LPush(ctx, k.Queue("low"), string(lowData))

	defaultJob := senna.NewJob("default_job", nil)
	defaultJob.Queue = "default"
	defaultData, _ := defaultJob.Marshal()
	client.LPush(ctx, k.Queue("default"), string(defaultData))

	criticalJob := senna.NewJob("critical_job", nil)
	criticalJob.Queue = "critical"
	criticalData, _ := criticalJob.Marshal()
	client.LPush(ctx, k.Queue("critical"), string(criticalData))

	// With strict priority, should always get critical first
	fetched, err := f.Fetch(ctx, "worker-1")
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if fetched.Type != "critical_job" {
		t.Errorf("expected critical_job first, got %s", fetched.Type)
	}

	// Then default
	fetched, err = f.Fetch(ctx, "worker-1")
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if fetched.Type != "default_job" {
		t.Errorf("expected default_job second, got %s", fetched.Type)
	}

	// Then low
	fetched, err = f.Fetch(ctx, "worker-1")
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if fetched.Type != "low_job" {
		t.Errorf("expected low_job third, got %s", fetched.Type)
	}
}

func TestFetcher_StrictPriority_SkipsPausedQueues(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-strict-paused:*")

	k := keys.New("test-strict-paused")

	// Create fetcher with critical queue paused
	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "critical", Priority: 10, Paused: true},
		{Name: "default", Priority: 5},
	}, 100*time.Millisecond, true)

	ctx := context.Background()

	// Add jobs to both queues
	criticalJob := senna.NewJob("critical_job", nil)
	criticalData, _ := criticalJob.Marshal()
	client.LPush(ctx, k.Queue("critical"), string(criticalData))

	defaultJob := senna.NewJob("default_job", nil)
	defaultData, _ := defaultJob.Marshal()
	client.LPush(ctx, k.Queue("default"), string(defaultData))

	// Should skip critical (paused) and get default
	fetched, err := f.Fetch(ctx, "worker-1")
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if fetched.Type != "default_job" {
		t.Errorf("expected default_job (critical is paused), got %s", fetched.Type)
	}
}

func TestFetcher_StrictPriority_TriesNextQueueWhenEmpty(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-strict-empty:*")

	k := keys.New("test-strict-empty")

	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "critical", Priority: 10},
		{Name: "default", Priority: 5},
	}, 100*time.Millisecond, true)

	ctx := context.Background()

	// Only add job to default queue (critical is empty)
	defaultJob := senna.NewJob("default_job", nil)
	defaultData, _ := defaultJob.Marshal()
	client.LPush(ctx, k.Queue("default"), string(defaultData))

	// Should try critical first (empty), then get default
	fetched, err := f.Fetch(ctx, "worker-1")
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if fetched.Type != "default_job" {
		t.Errorf("expected default_job, got %s", fetched.Type)
	}
}
