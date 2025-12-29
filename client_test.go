package senna

import (
	"context"
	"os"
	"testing"
	"time"

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

func TestClient_New(t *testing.T) {
	client, err := NewClient(&ClientConfig{
		Redis: RedisConfig{
			Addr: getTestRedisAddr(),
		},
		Namespace: "test",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	if client.Redis() == nil {
		t.Error("Redis client should not be nil")
	}
}

func TestClient_Enqueue(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test:*")

	client, err := NewClient(&ClientConfig{
		Redis: RedisConfig{
			Addr: getTestRedisAddr(),
		},
		Namespace: "test",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	job, err := client.Enqueue(ctx, "send_email", map[string]any{
		"to":      "user@example.com",
		"subject": "Test",
	})
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	if job.ID == "" {
		t.Error("job ID should not be empty")
	}
	if job.Type != "send_email" {
		t.Errorf("expected type 'send_email', got '%s'", job.Type)
	}
	if job.Queue != "default" {
		t.Errorf("expected queue 'default', got '%s'", job.Queue)
	}

	length, err := redisClient.LLen(ctx, "test:queue:default").Result()
	if err != nil {
		t.Fatalf("failed to get queue length: %v", err)
	}
	if length != 1 {
		t.Errorf("expected 1 job in queue, got %d", length)
	}
}

func TestClient_EnqueueWithQueue(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test:*")

	client, err := NewClient(&ClientConfig{
		Redis: RedisConfig{
			Addr: getTestRedisAddr(),
		},
		Namespace: "test",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	job, err := client.Enqueue(ctx, "process", nil, WithQueue("critical"))
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	if job.Queue != "critical" {
		t.Errorf("expected queue 'critical', got '%s'", job.Queue)
	}

	length, err := redisClient.LLen(ctx, "test:queue:critical").Result()
	if err != nil {
		t.Fatalf("failed to get queue length: %v", err)
	}
	if length != 1 {
		t.Errorf("expected 1 job in critical queue, got %d", length)
	}
}

func TestClient_EnqueueWithRetry(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test:*")

	client, err := NewClient(&ClientConfig{
		Redis: RedisConfig{
			Addr: getTestRedisAddr(),
		},
		Namespace: "test",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	job, err := client.Enqueue(ctx, "process", nil, WithRetry(5))
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	if job.Retry != 5 {
		t.Errorf("expected retry 5, got %d", job.Retry)
	}
}

func TestClient_EnqueueIn(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test:*")

	client, err := NewClient(&ClientConfig{
		Redis: RedisConfig{
			Addr: getTestRedisAddr(),
		},
		Namespace: "test",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	_, err = client.EnqueueIn(ctx, 5*time.Minute, "delayed_job", nil)
	if err != nil {
		t.Fatalf("enqueue in failed: %v", err)
	}

	length, err := redisClient.ZCard(ctx, "test:scheduled").Result()
	if err != nil {
		t.Fatalf("failed to get scheduled length: %v", err)
	}
	if length != 1 {
		t.Errorf("expected 1 scheduled job, got %d", length)
	}

	queueLength, _ := redisClient.LLen(ctx, "test:queue:default").Result()
	if queueLength != 0 {
		t.Errorf("expected 0 jobs in queue, got %d", queueLength)
	}
}

func TestClient_EnqueueAt(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test:*")

	client, err := NewClient(&ClientConfig{
		Redis: RedisConfig{
			Addr: getTestRedisAddr(),
		},
		Namespace: "test",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	futureTime := time.Now().Add(time.Hour)
	_, err = client.EnqueueAt(ctx, futureTime, "scheduled_job", nil)
	if err != nil {
		t.Fatalf("enqueue at failed: %v", err)
	}

	length, err := redisClient.ZCard(ctx, "test:scheduled").Result()
	if err != nil {
		t.Fatalf("failed to get scheduled length: %v", err)
	}
	if length != 1 {
		t.Errorf("expected 1 scheduled job, got %d", length)
	}
}

func TestClient_UniqueJob(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test:*")

	client, err := NewClient(&ClientConfig{
		Redis: RedisConfig{
			Addr: getTestRedisAddr(),
		},
		Namespace: "test",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()

	_, err = client.Enqueue(ctx, "unique_job", nil,
		WithUniqueKey("user:123:sync", time.Hour))
	if err != nil {
		t.Fatalf("first enqueue failed: %v", err)
	}

	_, err = client.Enqueue(ctx, "unique_job", nil,
		WithUniqueKey("user:123:sync", time.Hour))

	var dupErr *DuplicateJobError
	if err == nil {
		t.Fatal("expected DuplicateJobError for second enqueue")
	}
	if _, ok := err.(*DuplicateJobError); !ok {
		_ = dupErr
		t.Fatalf("expected DuplicateJobError, got %T: %v", err, err)
	}
}

func TestClient_UniqueJobDifferentKeys(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test:*")

	client, err := NewClient(&ClientConfig{
		Redis: RedisConfig{
			Addr: getTestRedisAddr(),
		},
		Namespace: "test",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()

	_, err = client.Enqueue(ctx, "unique_job", nil,
		WithUniqueKey("user:123:sync", time.Hour))
	if err != nil {
		t.Fatalf("first enqueue failed: %v", err)
	}

	_, err = client.Enqueue(ctx, "unique_job", nil,
		WithUniqueKey("user:456:sync", time.Hour))
	if err != nil {
		t.Fatalf("second enqueue with different key should succeed: %v", err)
	}
}

func TestClient_Batch(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test:*")

	client, err := NewClient(&ClientConfig{
		Redis: RedisConfig{
			Addr: getTestRedisAddr(),
		},
		Namespace: "test",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()

	batch := NewBatch()
	batch.Add("job1", map[string]any{"id": 1})
	batch.Add("job2", map[string]any{"id": 2})
	batch.Add("job3", map[string]any{"id": 3})
	batch.OnCompleteCallback("batch_complete")

	err = client.EnqueueBatch(ctx, batch)
	if err != nil {
		t.Fatalf("enqueue batch failed: %v", err)
	}

	queueLength, err := redisClient.LLen(ctx, "test:queue:default").Result()
	if err != nil {
		t.Fatalf("failed to get queue length: %v", err)
	}
	if queueLength != 3 {
		t.Errorf("expected 3 jobs in queue, got %d", queueLength)
	}

	batchJobsCount, err := redisClient.SCard(ctx, "test:batch:"+batch.ID+":jobs").Result()
	if err != nil {
		t.Fatalf("failed to get batch jobs count: %v", err)
	}
	if batchJobsCount != 3 {
		t.Errorf("expected 3 jobs in batch tracking, got %d", batchJobsCount)
	}
}

func TestBatch_Builder(t *testing.T) {
	batch := NewBatch().
		Add("job1", nil).
		Add("job2", nil, WithQueue("critical")).
		OnCompleteCallback("on_complete").
		OnSuccessCallback("on_success").
		OnDeathCallback("on_death")

	if len(batch.Jobs) != 2 {
		t.Errorf("expected 2 jobs, got %d", len(batch.Jobs))
	}
	if batch.Jobs[1].Queue != "critical" {
		t.Errorf("expected queue 'critical', got '%s'", batch.Jobs[1].Queue)
	}
	if batch.OnComplete != "on_complete" {
		t.Errorf("expected OnComplete 'on_complete', got '%s'", batch.OnComplete)
	}
	if batch.OnSuccess != "on_success" {
		t.Errorf("expected OnSuccess 'on_success', got '%s'", batch.OnSuccess)
	}
	if batch.OnDeath != "on_death" {
		t.Errorf("expected OnDeath 'on_death', got '%s'", batch.OnDeath)
	}
}

func TestClient_EncryptedJob(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test:*")

	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	client, err := NewClient(&ClientConfig{
		Redis: RedisConfig{
			Addr: getTestRedisAddr(),
		},
		Namespace: "test",
		Encryption: &EncryptionSettings{
			Enabled: true,
			Key:     key,
		},
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	job, err := client.Enqueue(ctx, "sensitive_job", map[string]any{
		"card_number": "4111111111111111",
	}, WithEncryption())
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	if !job.Encrypted {
		t.Error("job should be marked as encrypted")
	}
	if _, ok := job.Args["_encrypted"]; !ok {
		t.Error("job args should be encrypted")
	}
	if job.Args["card_number"] != nil {
		t.Error("original args should not be present")
	}
}

func TestClient_MultipleQueues(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test:*")

	client, err := NewClient(&ClientConfig{
		Redis: RedisConfig{
			Addr: getTestRedisAddr(),
		},
		Namespace: "test",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()

	_, _ = client.Enqueue(ctx, "job1", nil, WithQueue("critical"))
	_, _ = client.Enqueue(ctx, "job2", nil, WithQueue("critical"))
	_, _ = client.Enqueue(ctx, "job3", nil, WithQueue("default"))
	_, _ = client.Enqueue(ctx, "job4", nil, WithQueue("low"))

	criticalLen, _ := redisClient.LLen(ctx, "test:queue:critical").Result()
	defaultLen, _ := redisClient.LLen(ctx, "test:queue:default").Result()
	lowLen, _ := redisClient.LLen(ctx, "test:queue:low").Result()

	if criticalLen != 2 {
		t.Errorf("expected 2 in critical, got %d", criticalLen)
	}
	if defaultLen != 1 {
		t.Errorf("expected 1 in default, got %d", defaultLen)
	}
	if lowLen != 1 {
		t.Errorf("expected 1 in low, got %d", lowLen)
	}
}
