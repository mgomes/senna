package client

import (
	"context"
	"crypto/aes"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mgomes/senna"
	"github.com/redis/go-redis/v9"
)

func TestClient_New(t *testing.T) {
	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
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

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
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

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
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

func TestClient_EnqueueDoesNotRegisterQueueWhenQueuePushFails(t *testing.T) {
	const namespace = "test-enqueue-wrong-queue-type"

	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, namespace+":*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: namespace,
	})
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	if err := redisClient.Set(ctx, namespace+":queue:default", "wrong-type", 0).Err(); err != nil {
		t.Fatalf("Set(queue wrong type) error = %v, want nil", err)
	}

	job, err := client.Enqueue(ctx, "send_email", nil)
	if err == nil {
		t.Fatal("Enqueue(with wrong queue type) error = nil, want non-nil")
	}
	if job != nil {
		t.Fatalf("Enqueue(with wrong queue type) job = %#v, want nil", job)
	}

	registered, err := redisClient.SIsMember(ctx, namespace+":queues", "default").Result()
	if err != nil {
		t.Fatalf("SIsMember(queues, default) error = %v, want nil", err)
	}
	if registered {
		t.Fatal("SIsMember(queues, default) = true, want false")
	}

	if err := redisClient.Del(ctx, namespace+":queue:default").Err(); err != nil {
		t.Fatalf("Del(queue wrong type) error = %v, want nil", err)
	}

	_, err = client.Enqueue(ctx, "send_email", nil)
	if err != nil {
		t.Fatalf("Enqueue(after repairing queue) error = %v, want nil", err)
	}
}

func TestClient_EnqueueDoesNotPushJobWhenQueuesKeyHasWrongType(t *testing.T) {
	const namespace = "test-enqueue-wrong-queues-type"

	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, namespace+":*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: namespace,
	})
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	if err := redisClient.Set(ctx, namespace+":queues", "wrong-type", 0).Err(); err != nil {
		t.Fatalf("Set(queues wrong type) error = %v, want nil", err)
	}

	job, err := client.Enqueue(ctx, "send_email", nil)
	if err == nil {
		t.Fatal("Enqueue(with wrong queues type) error = nil, want non-nil")
	}
	if job != nil {
		t.Fatalf("Enqueue(with wrong queues type) job = %#v, want nil", job)
	}

	queueExists, err := redisClient.Exists(ctx, namespace+":queue:default").Result()
	if err != nil {
		t.Fatalf("Exists(queue) error = %v, want nil", err)
	}
	if queueExists != 0 {
		t.Fatalf("Exists(queue) = %d, want 0", queueExists)
	}

	if err := redisClient.Del(ctx, namespace+":queues").Err(); err != nil {
		t.Fatalf("Del(queues wrong type) error = %v, want nil", err)
	}

	_, err = client.Enqueue(ctx, "send_email", nil)
	if err != nil {
		t.Fatalf("Enqueue(after repairing queues key) error = %v, want nil", err)
	}
}

func TestClient_EnqueueRejectsInvalidQueueName(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-invalid-queue:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-invalid-queue",
	})
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	defer func() { _ = client.Close() }()

	tests := []struct {
		name string
		opts []EnqueueOption
	}{
		{
			name: "empty option queue",
			opts: []EnqueueOption{WithQueue("")},
		},
		{
			name: "whitespace option queue",
			opts: []EnqueueOption{WithQueue(" \t\n")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job, err := client.Enqueue(context.Background(), "process", nil, tt.opts...)
			if !errors.Is(err, ErrInvalidQueueName) {
				t.Fatalf("Enqueue(%s) error = %v, want %v", tt.name, err, ErrInvalidQueueName)
			}
			if job != nil {
				t.Fatalf("Enqueue(%s) job = %#v, want nil", tt.name, job)
			}
		})
	}

	queueCount, err := redisClient.Exists(context.Background(), "test-invalid-queue:queues").Result()
	if err != nil {
		t.Fatalf("Exists(queues) error = %v, want nil", err)
	}
	if queueCount != 0 {
		t.Errorf("Exists(queues) = %d, want 0", queueCount)
	}
}

func TestClient_EnqueueRejectsInvalidDefaultQueue(t *testing.T) {
	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-invalid-default-queue",
		Settings: Settings{
			DefaultQueue: " ",
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	defer func() { _ = client.Close() }()

	job, err := client.Enqueue(context.Background(), "process", nil)
	if !errors.Is(err, ErrInvalidQueueName) {
		t.Fatalf("Enqueue(default whitespace queue) error = %v, want %v", err, ErrInvalidQueueName)
	}
	if job != nil {
		t.Fatalf("Enqueue(default whitespace queue) job = %#v, want nil", job)
	}
}

func TestClient_EnqueueWithRetry(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
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

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
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

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
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

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
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

	if err == nil {
		t.Fatal("expected DuplicateJobError for second enqueue")
	}
	if _, ok := err.(*senna.DuplicateJobError); !ok {
		t.Fatalf("expected DuplicateJobError, got %T: %v", err, err)
	}
}

func TestClient_UniqueJobDifferentKeys(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
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

func TestClient_UniqueJobDoesNotClaimLockWhenImmediateEnqueueFails(t *testing.T) {
	const namespace = "test-unique-immediate-failure"

	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, namespace+":*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: namespace,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	uniqueKey := "user:123:sync"
	if err := redisClient.Set(ctx, namespace+":queue:default", "wrong-type", 0).Err(); err != nil {
		t.Fatalf("failed to poison default queue: %v", err)
	}

	_, err = client.Enqueue(ctx, "unique_job", nil, WithUniqueKey(uniqueKey, time.Hour))
	if err == nil {
		t.Fatal("expected enqueue to fail")
	}

	exists, err := redisClient.Exists(ctx, namespace+":unique:"+uniqueKey).Result()
	if err != nil {
		t.Fatalf("failed to check unique key: %v", err)
	}
	if exists != 0 {
		t.Fatalf("unique lock exists = %d, want 0", exists)
	}

	if err := redisClient.Del(ctx, namespace+":queue:default").Err(); err != nil {
		t.Fatalf("failed to repair default queue: %v", err)
	}

	_, err = client.Enqueue(ctx, "unique_job", nil, WithUniqueKey(uniqueKey, time.Hour))
	if err != nil {
		t.Fatalf("enqueue after unique lock cleanup failed: %v", err)
	}
}

func TestClient_UniqueJobDoesNotClaimLockWhenScheduledEnqueueFails(t *testing.T) {
	const namespace = "test-unique-scheduled-failure"

	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, namespace+":*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: namespace,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	uniqueKey := "user:123:sync"
	if err := redisClient.Set(ctx, namespace+":scheduled", "wrong-type", 0).Err(); err != nil {
		t.Fatalf("failed to poison scheduled key: %v", err)
	}

	_, err = client.EnqueueAt(ctx, time.Now().Add(time.Hour), "unique_job", nil, WithUniqueKey(uniqueKey, time.Hour))
	if err == nil {
		t.Fatal("expected scheduled enqueue to fail")
	}

	exists, err := redisClient.Exists(ctx, namespace+":unique:"+uniqueKey).Result()
	if err != nil {
		t.Fatalf("failed to check unique key: %v", err)
	}
	if exists != 0 {
		t.Fatalf("unique lock exists = %d, want 0", exists)
	}

	if err := redisClient.Del(ctx, namespace+":scheduled").Err(); err != nil {
		t.Fatalf("failed to repair scheduled key: %v", err)
	}

	_, err = client.EnqueueAt(ctx, time.Now().Add(time.Hour), "unique_job", nil, WithUniqueKey(uniqueKey, time.Hour))
	if err != nil {
		t.Fatalf("scheduled enqueue after failed unique claim failed: %v", err)
	}

	scheduledCount, err := redisClient.ZCard(ctx, namespace+":scheduled").Result()
	if err != nil {
		t.Fatalf("failed to get scheduled count: %v", err)
	}
	if scheduledCount != 1 {
		t.Errorf("scheduled count = %d, want 1", scheduledCount)
	}
}

func TestClient_Batch(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
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

func TestClient_EnqueueBatchRejectsInvalidJobQueue(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-batch-invalid-job-queue:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-batch-invalid-job-queue",
	})
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	defer func() { _ = client.Close() }()

	batch := NewBatch().
		Add("job", nil, WithQueue(" \t\n"))

	err = client.EnqueueBatch(context.Background(), batch)
	if !errors.Is(err, ErrInvalidQueueName) {
		t.Fatalf("EnqueueBatch(invalid job queue) error = %v, want %v", err, ErrInvalidQueueName)
	}

	stateCount, err := redisClient.Exists(context.Background(), "test-batch-invalid-job-queue:batch:"+batch.ID).Result()
	if err != nil {
		t.Fatalf("Exists(batch state) error = %v, want nil", err)
	}
	if stateCount != 0 {
		t.Errorf("Exists(batch state) = %d, want 0", stateCount)
	}
}

func TestClient_EnqueueBatchRejectsInvalidCallbackQueue(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-batch-invalid-callback-queue:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-batch-invalid-callback-queue",
	})
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	defer func() { _ = client.Close() }()

	batch := NewBatch().
		Add("job", nil).
		OnCompleteCallback("complete").
		WithCallbackQueue(" \t\n")

	err = client.EnqueueBatch(context.Background(), batch)
	if !errors.Is(err, ErrInvalidQueueName) {
		t.Fatalf("EnqueueBatch(invalid callback queue) error = %v, want %v", err, ErrInvalidQueueName)
	}

	stateCount, err := redisClient.Exists(context.Background(), "test-batch-invalid-callback-queue:batch:"+batch.ID).Result()
	if err != nil {
		t.Fatalf("Exists(batch state) error = %v, want nil", err)
	}
	if stateCount != 0 {
		t.Errorf("Exists(batch state) = %d, want 0", stateCount)
	}
}

func TestClient_EnqueueBatch_RejectsWrongQueueTypeWithoutQueueingPartialJobs(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-batch-atomic:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-batch-atomic",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	batch := NewBatch()
	batch.Add("first_job", map[string]any{"id": 1})
	batch.Add("blocked_job", map[string]any{"id": 2}, WithQueue("blocked"))

	if err := redisClient.Set(ctx, client.keys.Queue("blocked"), "not a list", 0).Err(); err != nil {
		t.Fatalf("failed to poison blocked queue: %v", err)
	}

	err = client.EnqueueBatch(ctx, batch)
	if err == nil {
		t.Fatal("expected EnqueueBatch to fail")
	}

	queueLength, err := redisClient.LLen(ctx, client.keys.Queue("default")).Result()
	if err != nil {
		t.Fatalf("failed to get default queue length: %v", err)
	}
	if queueLength != 0 {
		t.Errorf("default queue length = %d, want 0", queueLength)
	}

	keysLeft, err := redisClient.Exists(
		ctx,
		client.keys.Batch(batch.ID),
		client.keys.BatchProgress(batch.ID),
		client.keys.BatchJobs(batch.ID),
	).Result()
	if err != nil {
		t.Fatalf("failed to check batch cleanup: %v", err)
	}
	if keysLeft != 0 {
		t.Errorf("batch keys left after failed enqueue = %d, want 0", keysLeft)
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
	if batch.OnComplete == nil || batch.OnComplete.JobType != "on_complete" {
		t.Errorf("expected OnComplete.JobType 'on_complete', got '%v'", batch.OnComplete)
	}
	if batch.OnSuccess == nil || batch.OnSuccess.JobType != "on_success" {
		t.Errorf("expected OnSuccess.JobType 'on_success', got '%v'", batch.OnSuccess)
	}
	if batch.OnDeath == nil || batch.OnDeath.JobType != "on_death" {
		t.Errorf("expected OnDeath.JobType 'on_death', got '%v'", batch.OnDeath)
	}
}

func TestClientCleanupOrphanedBatch_UsesUnlinkForBatchData(t *testing.T) {
	const namespace = "test-cleanup-orphaned-unlink"

	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, namespace+":*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: namespace,
	})
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	batchID := "batch-1"
	batchDataKeys := []string{
		client.keys.Batch(batchID),
		client.keys.BatchProgress(batchID),
		client.keys.BatchJobs(batchID),
		client.keys.BatchFailed(batchID),
		client.keys.BatchCallbacks(batchID),
	}

	if err := redisClient.Set(ctx, client.keys.Batch(batchID), "{}", 0).Err(); err != nil {
		t.Fatalf("Set(batch state) error = %v, want nil", err)
	}
	if err := redisClient.HSet(ctx, client.keys.BatchProgress(batchID), "pending", 1).Err(); err != nil {
		t.Fatalf("HSet(batch progress) error = %v, want nil", err)
	}
	if err := redisClient.SAdd(ctx, client.keys.BatchJobs(batchID), "jid-1").Err(); err != nil {
		t.Fatalf("SAdd(batch jobs) error = %v, want nil", err)
	}
	if err := redisClient.SAdd(ctx, client.keys.BatchFailed(batchID), "jid-1").Err(); err != nil {
		t.Fatalf("SAdd(batch failed) error = %v, want nil", err)
	}
	if err := redisClient.SAdd(ctx, client.keys.BatchCallbacks(batchID), "jid-1:callback:1").Err(); err != nil {
		t.Fatalf("SAdd(batch callbacks) error = %v, want nil", err)
	}
	if err := redisClient.SAdd(ctx, client.keys.Batches(), batchID).Err(); err != nil {
		t.Fatalf("SAdd(batches) error = %v, want nil", err)
	}

	hook := &clientCommandHook{}
	client.redis.AddHook(hook)

	client.cleanupOrphanedBatch(ctx, batchID)

	names := hook.pipelineCommandNames()
	if len(names) != 2 {
		t.Fatalf("cleanupOrphanedBatch pipeline command names = %v, want 2 commands", names)
	}
	if names[0] != "srem" || names[1] != "unlink" {
		t.Fatalf("cleanupOrphanedBatch pipeline command names = %v, want [srem unlink]", names)
	}

	exists, err := redisClient.Exists(ctx, batchDataKeys...).Result()
	if err != nil {
		t.Fatalf("Exists(batch data keys) error = %v, want nil", err)
	}
	if exists != 0 {
		t.Errorf("Exists(batch data keys) = %d, want 0", exists)
	}

	registered, err := redisClient.SIsMember(ctx, client.keys.Batches(), batchID).Result()
	if err != nil {
		t.Fatalf("SIsMember(batches, batchID) error = %v, want nil", err)
	}
	if registered {
		t.Fatal("SIsMember(batches, batchID) = true, want false")
	}
}

type clientCommandHook struct {
	mu            sync.Mutex
	pipelineNames []string
}

func (h *clientCommandHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *clientCommandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}

func (h *clientCommandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.mu.Lock()
		for _, cmd := range cmds {
			h.pipelineNames = append(h.pipelineNames, cmd.Name())
		}
		h.mu.Unlock()

		return next(ctx, cmds)
	}
}

func (h *clientCommandHook) pipelineCommandNames() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	names := make([]string, len(h.pipelineNames))
	copy(names, h.pipelineNames)
	return names
}

type commandSizeHook struct {
	mu       sync.Mutex
	eval     int
	maxEval  int
	lpush    int
	maxLPush int
	zadd     int
	maxZAdd  int
}

func (h *commandSizeHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *commandSizeHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.record(cmd)
		return next(ctx, cmd)
	}
}

func (h *commandSizeHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			h.record(cmd)
		}
		return next(ctx, cmds)
	}
}

func (h *commandSizeHook) record(cmd redis.Cmder) {
	argsLen := len(cmd.Args())
	h.mu.Lock()
	defer h.mu.Unlock()
	switch cmd.Name() {
	case "eval", "evalsha":
		h.eval++
		if argsLen > h.maxEval {
			h.maxEval = argsLen
		}
	case "lpush":
		h.lpush++
		if argsLen > h.maxLPush {
			h.maxLPush = argsLen
		}
	case "zadd":
		h.zadd++
		if argsLen > h.maxZAdd {
			h.maxZAdd = argsLen
		}
	}
}

func (h *commandSizeHook) evalCalls() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.eval
}

func (h *commandSizeHook) maxEvalArgs() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.maxEval
}

func (h *commandSizeHook) lpushCalls() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lpush
}

func (h *commandSizeHook) maxLPushArgs() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.maxLPush
}

func (h *commandSizeHook) zaddCalls() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.zadd
}

func (h *commandSizeHook) maxZAddArgs() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.maxZAdd
}

type failNthLPushHook struct {
	mu     sync.Mutex
	failOn int
	seen   int
	err    error
}

func (h *failNthLPushHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *failNthLPushHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return next
}

func (h *failNthLPushHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			if cmd.Name() != "lpush" {
				continue
			}
			h.mu.Lock()
			h.seen++
			shouldFail := h.seen == h.failOn
			h.mu.Unlock()
			if shouldFail {
				return h.err
			}
		}
		return next(ctx, cmds)
	}
}

func TestClient_EncryptedJob(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test:*")

	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test",
		Encryption: &senna.EncryptionSettings{
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

func TestClient_EnqueueWithEncryptionRequiresEncryptor(t *testing.T) {
	const namespace = "test-encryption-required"

	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, namespace+":*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: namespace,
	})
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	defer func() { _ = client.Close() }()

	job, err := client.Enqueue(context.Background(), "sensitive_job", map[string]any{
		"secret": "plaintext",
	}, WithEncryption())
	if !errors.Is(err, ErrEncryptionUnavailable) {
		t.Fatalf("Enqueue(WithEncryption without encryptor) error = %v, want ErrEncryptionUnavailable", err)
	}
	if job != nil {
		t.Fatalf("Enqueue(WithEncryption without encryptor) job = %#v, want nil", job)
	}

	queueLen, err := redisClient.LLen(context.Background(), namespace+":queue:default").Result()
	if err != nil {
		t.Fatalf("LLen(default after failed encrypted enqueue) error = %v, want nil", err)
	}
	if queueLen != 0 {
		t.Fatalf("LLen(default after failed encrypted enqueue) = %d, want 0", queueLen)
	}
}

func TestClient_EnqueueBulkWithEncryptionRequiresEncryptor(t *testing.T) {
	const namespace = "test-bulk-encryption-required"

	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, namespace+":*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: namespace,
	})
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	defer func() { _ = client.Close() }()

	jobs, err := client.EnqueueBulk(context.Background(), "sensitive_job", []map[string]any{
		{"secret": "one"},
		{"secret": "two"},
	}, WithEncryption())
	if !errors.Is(err, ErrEncryptionUnavailable) {
		t.Fatalf("EnqueueBulk(WithEncryption without encryptor) error = %v, want ErrEncryptionUnavailable", err)
	}
	if jobs != nil {
		t.Fatalf("EnqueueBulk(WithEncryption without encryptor) jobs = %#v, want nil", jobs)
	}

	queueLen, err := redisClient.LLen(context.Background(), namespace+":queue:default").Result()
	if err != nil {
		t.Fatalf("LLen(default after failed encrypted bulk enqueue) error = %v, want nil", err)
	}
	if queueLen != 0 {
		t.Fatalf("LLen(default after failed encrypted bulk enqueue) = %d, want 0", queueLen)
	}
}

func TestClient_New_InvalidEncryptionKeyBeforeRedis(t *testing.T) {
	_, err := New(&Config{
		Redis: senna.RedisConfig{
			Addr:        "127.0.0.1:0",
			DialTimeout: time.Millisecond,
		},
		Namespace: "test-invalid-encryption-before-redis",
		Encryption: &senna.EncryptionSettings{
			Enabled: true,
			Key:     []byte("tooshort"),
		},
	})

	var keySizeErr aes.KeySizeError
	if !errors.As(err, &keySizeErr) {
		t.Fatalf("New() error = %v, want aes.KeySizeError", err)
	}
}

func TestClient_MultipleQueues(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
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

func TestClient_EnqueueIn_CorrectTimestamp(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-schedule-ts:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-schedule-ts",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	delay := 10 * time.Minute
	beforeEnqueue, err := redisNow(ctx, redisClient)
	if err != nil {
		t.Fatalf("redisNow() before EnqueueIn error = %v, want nil", err)
	}

	_, err = client.EnqueueIn(ctx, delay, "delayed_job", nil)
	if err != nil {
		t.Fatalf("enqueue in failed: %v", err)
	}
	afterEnqueue, err := redisNow(ctx, redisClient)
	if err != nil {
		t.Fatalf("redisNow() after EnqueueIn error = %v, want nil", err)
	}

	items, err := redisClient.ZRangeWithScores(ctx, "test-schedule-ts:scheduled", 0, -1).Result()
	if err != nil {
		t.Fatalf("failed to get scheduled items: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 scheduled job, got %d", len(items))
	}

	score := items[0].Score
	expectedMin := float64(beforeEnqueue.Add(delay).Unix())
	expectedMax := float64(afterEnqueue.Add(delay).Unix())

	if score < expectedMin || score > expectedMax {
		t.Errorf("score %f not in expected range [%f, %f]", score, expectedMin, expectedMax)
	}
}

func TestClient_EnqueueIn_UniqueUsesRedisTime(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-schedule-unique-ts:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-schedule-unique-ts",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	delay := 10 * time.Minute
	beforeEnqueue, err := redisNow(ctx, redisClient)
	if err != nil {
		t.Fatalf("redisNow() before unique EnqueueIn error = %v, want nil", err)
	}

	_, err = client.EnqueueIn(ctx, delay, "delayed_unique_job", nil, WithUniqueKey("unique-delayed", time.Hour))
	if err != nil {
		t.Fatalf("unique enqueue in failed: %v", err)
	}
	afterEnqueue, err := redisNow(ctx, redisClient)
	if err != nil {
		t.Fatalf("redisNow() after unique EnqueueIn error = %v, want nil", err)
	}

	items, err := redisClient.ZRangeWithScores(ctx, "test-schedule-unique-ts:scheduled", 0, -1).Result()
	if err != nil {
		t.Fatalf("failed to get scheduled items: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 scheduled job, got %d", len(items))
	}

	score := items[0].Score
	expectedMin := float64(beforeEnqueue.Add(delay).Unix())
	expectedMax := float64(afterEnqueue.Add(delay).Unix())

	if score < expectedMin || score > expectedMax {
		t.Errorf("score %f not in expected range [%f, %f]", score, expectedMin, expectedMax)
	}
}

func TestClient_EnqueueAt_CorrectTimestamp(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-schedule-at:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-schedule-at",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	scheduledTime := time.Date(2030, 6, 15, 14, 30, 0, 0, time.UTC)

	_, err = client.EnqueueAt(ctx, scheduledTime, "future_job", nil)
	if err != nil {
		t.Fatalf("enqueue at failed: %v", err)
	}

	items, err := redisClient.ZRangeWithScores(ctx, "test-schedule-at:scheduled", 0, -1).Result()
	if err != nil {
		t.Fatalf("failed to get scheduled items: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 scheduled job, got %d", len(items))
	}

	score := items[0].Score
	expectedScore := float64(scheduledTime.Unix())

	if score != expectedScore {
		t.Errorf("expected score %f, got %f", expectedScore, score)
	}
}

func TestClient_EnqueueIn_WithOptions(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-schedule-opts:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-schedule-opts",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()

	job, err := client.EnqueueIn(ctx, time.Hour, "delayed_job",
		map[string]any{"user_id": 123},
		WithQueue("emails"),
		WithRetry(3),
	)
	if err != nil {
		t.Fatalf("enqueue in failed: %v", err)
	}

	if job.Queue != "emails" {
		t.Errorf("expected queue 'emails', got '%s'", job.Queue)
	}
	if job.Retry != 3 {
		t.Errorf("expected retry 3, got %d", job.Retry)
	}
	if job.Args["user_id"] != 123 {
		t.Errorf("expected user_id 123, got %v", job.Args["user_id"])
	}
}

func TestClient_EnqueueAt_InThePast(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-schedule-past:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-schedule-past",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	pastTime := time.Now().Add(-time.Hour)

	_, err = client.EnqueueAt(ctx, pastTime, "past_job", nil)
	if err != nil {
		t.Fatalf("enqueue at past time failed: %v", err)
	}

	// Job should still be in scheduled set (worker will move it)
	length, err := redisClient.ZCard(ctx, "test-schedule-past:scheduled").Result()
	if err != nil {
		t.Fatalf("failed to get scheduled length: %v", err)
	}
	if length != 1 {
		t.Errorf("expected 1 scheduled job, got %d", length)
	}

	// Check the score is in the past
	items, _ := redisClient.ZRangeWithScores(ctx, "test-schedule-past:scheduled", 0, -1).Result()
	if items[0].Score > float64(time.Now().Unix()) {
		t.Error("job scheduled in the past should have past timestamp")
	}
}

func TestClient_EnqueueIn_ZeroDuration(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-schedule-zero:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-schedule-zero",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()

	// Zero duration should schedule for now (goes to scheduled set with current timestamp)
	_, err = client.EnqueueIn(ctx, 0, "immediate_job", nil)
	if err != nil {
		t.Fatalf("enqueue in with zero duration failed: %v", err)
	}

	// Should be in the queue immediately (delay of 0 means enqueue now)
	queueLen, _ := redisClient.LLen(ctx, "test-schedule-zero:queue:default").Result()
	if queueLen != 1 {
		t.Errorf("expected 1 job in queue (zero delay), got %d", queueLen)
	}
}

func TestClient_MultipleScheduledJobs_OrderedByTime(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-schedule-order:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-schedule-order",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	now := time.Now()

	// Schedule in reverse order
	_, _ = client.EnqueueAt(ctx, now.Add(3*time.Hour), "job3", nil)
	_, _ = client.EnqueueAt(ctx, now.Add(1*time.Hour), "job1", nil)
	_, _ = client.EnqueueAt(ctx, now.Add(2*time.Hour), "job2", nil)

	items, err := redisClient.ZRangeWithScores(ctx, "test-schedule-order:scheduled", 0, -1).Result()
	if err != nil {
		t.Fatalf("failed to get scheduled items: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("expected 3 scheduled jobs, got %d", len(items))
	}

	// Verify they're ordered by score (earliest first)
	for offset := range len(items) - 1 {
		i := offset + 1
		if items[i].Score < items[i-1].Score {
			t.Error("scheduled jobs should be ordered by timestamp")
		}
	}
}

func TestClient_EnqueueBulk_Basic(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-bulk:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-bulk",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()

	argsList := []map[string]any{
		{"user_id": 1},
		{"user_id": 2},
		{"user_id": 3},
	}

	jobs, err := client.EnqueueBulk(ctx, "bulk_job", argsList)
	if err != nil {
		t.Fatalf("EnqueueBulk failed: %v", err)
	}

	if len(jobs) != 3 {
		t.Fatalf("expected 3 jobs, got %d", len(jobs))
	}

	// Verify all jobs have unique IDs
	ids := make(map[string]bool)
	for _, job := range jobs {
		if ids[job.ID] {
			t.Error("duplicate job ID found")
		}
		ids[job.ID] = true
	}

	// Verify jobs are in queue
	queueLen, _ := redisClient.LLen(ctx, "test-bulk:queue:default").Result()
	if queueLen != 3 {
		t.Errorf("expected 3 jobs in queue, got %d", queueLen)
	}
}

func TestClient_EnqueueBulk_EmptyList(t *testing.T) {
	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-bulk-empty",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	jobs, err := client.EnqueueBulk(context.Background(), "bulk_job", nil)
	if err != nil {
		t.Fatalf("EnqueueBulk with empty list should not error: %v", err)
	}
	if jobs != nil {
		t.Error("expected nil jobs for empty list")
	}
}

func TestClient_EnqueueBulk_WithQueue(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-bulk-queue:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-bulk-queue",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()

	argsList := []map[string]any{
		{"id": 1},
		{"id": 2},
	}

	jobs, err := client.EnqueueBulk(ctx, "bulk_job", argsList, WithQueue("critical"))
	if err != nil {
		t.Fatalf("EnqueueBulk failed: %v", err)
	}

	for _, job := range jobs {
		if job.Queue != "critical" {
			t.Errorf("expected queue 'critical', got '%s'", job.Queue)
		}
	}

	// Verify jobs are in critical queue
	criticalLen, _ := redisClient.LLen(ctx, "test-bulk-queue:queue:critical").Result()
	if criticalLen != 2 {
		t.Errorf("expected 2 jobs in critical queue, got %d", criticalLen)
	}

	// Verify queue was added to queues set
	isMember, _ := redisClient.SIsMember(ctx, "test-bulk-queue:queues", "critical").Result()
	if !isMember {
		t.Error("critical queue should be in queues set")
	}
}

func TestClient_EnqueueBulk_UsesChunkSizeAndPreservesOrder(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-bulk-chunk:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-bulk-chunk",
	})
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	defer func() { _ = client.Close() }()

	hook := &commandSizeHook{}
	client.redis.AddHook(hook)

	ctx := context.Background()
	argsList := []map[string]any{
		{"index": 0},
		{"index": 1},
		{"index": 2},
		{"index": 3},
		{"index": 4},
	}

	jobs, err := client.EnqueueBulk(ctx, "bulk_chunk_job", argsList, WithBulkChunkSize(2))
	if err != nil {
		t.Fatalf("EnqueueBulk(chunked) error = %v, want nil", err)
	}
	if len(jobs) != len(argsList) {
		t.Fatalf("EnqueueBulk(chunked) jobs = %d, want %d", len(jobs), len(argsList))
	}

	if got := hook.lpushCalls(); got != 3 {
		t.Fatalf("bulk enqueue LPush calls = %d, want 3", got)
	}
	if got, wantMax := hook.maxLPushArgs(), 4; got > wantMax {
		t.Fatalf("max LPush args = %d, want <= %d", got, wantMax)
	}

	for i, wantJob := range jobs {
		payload, err := redisClient.RPop(ctx, "test-bulk-chunk:queue:default").Result()
		if err != nil {
			t.Fatalf("RPop(default) at index %d error = %v, want nil", i, err)
		}
		gotJob, err := senna.UnmarshalJob([]byte(payload))
		if err != nil {
			t.Fatalf("UnmarshalJob(queue payload %d) error = %v, want nil", i, err)
		}
		if gotJob.ID != wantJob.ID {
			t.Fatalf("queued job %d ID = %q, want %q", i, gotJob.ID, wantJob.ID)
		}
	}
}

func TestClient_EnqueueBulkAt_UsesChunkSize(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-bulk-at-chunk:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-bulk-at-chunk",
	})
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	defer func() { _ = client.Close() }()

	hook := &commandSizeHook{}
	client.redis.AddHook(hook)

	argsList := []map[string]any{
		{"index": 0},
		{"index": 1},
		{"index": 2},
		{"index": 3},
		{"index": 4},
	}
	_, err = client.EnqueueBulkAt(context.Background(), time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), "bulk_at_chunk_job", argsList, WithBulkChunkSize(2))
	if err != nil {
		t.Fatalf("EnqueueBulkAt(chunked) error = %v, want nil", err)
	}

	if got := hook.zaddCalls(); got != 3 {
		t.Fatalf("ZAdd calls = %d, want 3", got)
	}
	if got, wantMax := hook.maxZAddArgs(), 6; got > wantMax {
		t.Fatalf("max ZAdd args = %d, want <= %d", got, wantMax)
	}
}

func TestClient_EnqueueBulkRejectsInvalidChunkSize(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-bulk-invalid-chunk:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-bulk-invalid-chunk",
	})
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	defer func() { _ = client.Close() }()

	jobs, err := client.EnqueueBulk(context.Background(), "bulk_job", []map[string]any{{"id": 1}}, WithBulkChunkSize(0))
	if !errors.Is(err, ErrInvalidChunkSize) {
		t.Fatalf("EnqueueBulk(invalid chunk) error = %v, want %v", err, ErrInvalidChunkSize)
	}
	if jobs != nil {
		t.Fatalf("EnqueueBulk(invalid chunk) jobs = %#v, want nil", jobs)
	}
	if exists, err := redisClient.Exists(context.Background(), "test-bulk-invalid-chunk:queues").Result(); err != nil {
		t.Fatalf("Exists(queues) error = %v, want nil", err)
	} else if exists != 0 {
		t.Fatalf("Exists(queues) = %d, want 0", exists)
	}
}

func TestClient_EnqueueBulkReturnsAcceptedJobsOnChunkFailure(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-bulk-partial:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-bulk-partial",
	})
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	defer func() { _ = client.Close() }()

	injectedErr := errors.New("injected lpush failure")
	client.redis.AddHook(&failNthLPushHook{
		failOn: 2,
		err:    injectedErr,
	})

	argsList := []map[string]any{
		{"index": 0},
		{"index": 1},
		{"index": 2},
		{"index": 3},
		{"index": 4},
	}
	jobs, err := client.EnqueueBulk(context.Background(), "bulk_partial_job", argsList, WithBulkChunkSize(2))
	if !errors.Is(err, injectedErr) {
		t.Fatalf("EnqueueBulk(partial failure) error = %v, want wrapped %v", err, injectedErr)
	}
	var partial *BulkPartialError
	if !errors.As(err, &partial) {
		t.Fatalf("EnqueueBulk(partial failure) error = %T, want *BulkPartialError", err)
	}
	if partial.Enqueued != 2 || partial.Total != len(argsList) {
		t.Fatalf("BulkPartialError = {Enqueued:%d Total:%d}, want {Enqueued:2 Total:%d}", partial.Enqueued, partial.Total, len(argsList))
	}
	if len(jobs) != 2 {
		t.Fatalf("EnqueueBulk(partial failure) jobs = %d, want 2", len(jobs))
	}

	ctx := context.Background()
	if queued, err := redisClient.LLen(ctx, "test-bulk-partial:queue:default").Result(); err != nil {
		t.Fatalf("LLen(default) error = %v, want nil", err)
	} else if queued != 2 {
		t.Fatalf("LLen(default) = %d, want 2", queued)
	}
	for i, wantJob := range jobs {
		payload, err := redisClient.RPop(ctx, "test-bulk-partial:queue:default").Result()
		if err != nil {
			t.Fatalf("RPop(default) at index %d error = %v, want nil", i, err)
		}
		gotJob, err := senna.UnmarshalJob([]byte(payload))
		if err != nil {
			t.Fatalf("UnmarshalJob(queue payload %d) error = %v, want nil", i, err)
		}
		if gotJob.ID != wantJob.ID {
			t.Fatalf("queued job %d ID = %q, want %q", i, gotJob.ID, wantJob.ID)
		}
	}
}

func TestClient_EnqueueBulkRejectsInvalidQueueName(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-bulk-invalid-queue:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-bulk-invalid-queue",
	})
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	defer func() { _ = client.Close() }()

	argsList := []map[string]any{{"id": 1}}
	jobs, err := client.EnqueueBulk(context.Background(), "bulk_job", argsList, WithQueue(" \t\n"))
	if !errors.Is(err, ErrInvalidQueueName) {
		t.Fatalf("EnqueueBulk(invalid queue) error = %v, want %v", err, ErrInvalidQueueName)
	}
	if jobs != nil {
		t.Fatalf("EnqueueBulk(invalid queue) jobs = %#v, want nil", jobs)
	}

	queueCount, err := redisClient.Exists(context.Background(), "test-bulk-invalid-queue:queues").Result()
	if err != nil {
		t.Fatalf("Exists(queues) error = %v, want nil", err)
	}
	if queueCount != 0 {
		t.Errorf("Exists(queues) = %d, want 0", queueCount)
	}
}

func TestClient_EnqueueBulk_WithRetry(t *testing.T) {
	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-bulk-retry",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	argsList := []map[string]any{
		{"id": 1},
	}

	jobs, err := client.EnqueueBulk(context.Background(), "bulk_job", argsList, WithRetry(5))
	if err != nil {
		t.Fatalf("EnqueueBulk failed: %v", err)
	}

	if jobs[0].Retry != 5 {
		t.Errorf("expected retry 5, got %d", jobs[0].Retry)
	}
}

func TestClient_EnqueueBulk_UniqueKeyNotSupported(t *testing.T) {
	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-bulk-unique",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	argsList := []map[string]any{
		{"id": 1},
	}

	_, err = client.EnqueueBulk(context.Background(), "bulk_job", argsList, WithUniqueKey("key", time.Hour))
	if err == nil {
		t.Error("expected error when using unique key with bulk enqueue")
	}
}

func TestClient_EnqueueBulkIn_Scheduled(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-bulk-in:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-bulk-in",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	delay := time.Hour
	beforeEnqueue, err := redisNow(ctx, redisClient)
	if err != nil {
		t.Fatalf("redisNow() before EnqueueBulkIn error = %v, want nil", err)
	}

	argsList := []map[string]any{
		{"id": 1},
		{"id": 2},
		{"id": 3},
	}

	jobs, err := client.EnqueueBulkIn(ctx, delay, "delayed_bulk_job", argsList)
	if err != nil {
		t.Fatalf("EnqueueBulkIn failed: %v", err)
	}
	afterEnqueue, err := redisNow(ctx, redisClient)
	if err != nil {
		t.Fatalf("redisNow() after EnqueueBulkIn error = %v, want nil", err)
	}

	if len(jobs) != 3 {
		t.Fatalf("expected 3 jobs, got %d", len(jobs))
	}

	// Verify jobs are in scheduled set, not queue
	scheduledLen, _ := redisClient.ZCard(ctx, "test-bulk-in:scheduled").Result()
	if scheduledLen != 3 {
		t.Errorf("expected 3 jobs in scheduled, got %d", scheduledLen)
	}
	items, err := redisClient.ZRangeWithScores(ctx, "test-bulk-in:scheduled", 0, -1).Result()
	if err != nil {
		t.Fatalf("ZRangeWithScores(%q) error = %v, want nil", "test-bulk-in:scheduled", err)
	}
	if len(items) != 3 {
		t.Fatalf("expected 3 scheduled jobs, got %d", len(items))
	}
	expectedMin := float64(beforeEnqueue.Add(delay).Unix())
	expectedMax := float64(afterEnqueue.Add(delay).Unix())
	for i, item := range items {
		if item.Score < expectedMin || item.Score > expectedMax {
			t.Errorf("scheduled item %d score = %f, want in range [%f, %f]", i, item.Score, expectedMin, expectedMax)
		}
	}

	queueLen, _ := redisClient.LLen(ctx, "test-bulk-in:queue:default").Result()
	if queueLen != 0 {
		t.Errorf("expected 0 jobs in queue, got %d", queueLen)
	}
}

func TestClient_EnqueueBulkAt_Scheduled(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-bulk-at:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-bulk-at",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	futureTime := time.Date(2030, 6, 15, 12, 0, 0, 0, time.UTC)

	argsList := []map[string]any{
		{"id": 1},
		{"id": 2},
	}

	jobs, err := client.EnqueueBulkAt(ctx, futureTime, "scheduled_bulk_job", argsList)
	if err != nil {
		t.Fatalf("EnqueueBulkAt failed: %v", err)
	}

	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(jobs))
	}

	// Verify all jobs have the same score (timestamp)
	items, _ := redisClient.ZRangeWithScores(ctx, "test-bulk-at:scheduled", 0, -1).Result()
	expectedScore := float64(futureTime.Unix())
	for _, item := range items {
		if item.Score != expectedScore {
			t.Errorf("expected score %f, got %f", expectedScore, item.Score)
		}
	}
}

func TestClient_EnqueueBulk_LargeVolume(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-bulk-large:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-bulk-large",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()

	// Create 1000 jobs
	argsList := make([]map[string]any, 1000)
	for i := range argsList {
		argsList[i] = map[string]any{"index": i}
	}

	jobs, err := client.EnqueueBulk(ctx, "large_bulk_job", argsList)
	if err != nil {
		t.Fatalf("EnqueueBulk failed: %v", err)
	}

	if len(jobs) != 1000 {
		t.Fatalf("expected 1000 jobs, got %d", len(jobs))
	}

	// Verify all jobs are in queue
	queueLen, _ := redisClient.LLen(ctx, "test-bulk-large:queue:default").Result()
	if queueLen != 1000 {
		t.Errorf("expected 1000 jobs in queue, got %d", queueLen)
	}
}

func TestClient_EnqueueBulk_WithBatch(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-bulk-batch:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-bulk-batch",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()

	argsList := []map[string]any{
		{"id": 1},
		{"id": 2},
	}

	jobs, err := client.EnqueueBulk(ctx, "batch_bulk_job", argsList, WithBatch("batch-123"))
	if err != nil {
		t.Fatalf("EnqueueBulk failed: %v", err)
	}

	for _, job := range jobs {
		if job.BatchID != "batch-123" {
			t.Errorf("expected batch ID 'batch-123', got '%s'", job.BatchID)
		}
	}
}

func TestClient_EnqueueBulk_ReturnsErrorForUnmarshalableJobs(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-bulk-filter:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-bulk-filter",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()

	unmarshalable := make(chan int)
	argsList := []map[string]any{
		{"id": 1},
		{"id": 2, "bad": unmarshalable},
		{"id": 3},
	}

	jobs, err := client.EnqueueBulk(ctx, "filter_job", argsList)
	if err == nil {
		t.Fatal("expected EnqueueBulk to return an error")
	}
	if jobs != nil {
		t.Errorf("EnqueueBulk returned jobs = %v, want nil", jobs)
	}

	queueLen, err := redisClient.LLen(ctx, "test-bulk-filter:queue:default").Result()
	if err != nil {
		t.Fatalf("failed to get queue length: %v", err)
	}
	if queueLen != 0 {
		t.Errorf("queue length = %d, want 0", queueLen)
	}
}

func TestClient_EnqueueBulkIn_ReturnsErrorForUnmarshalableJobs(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-bulk-filter-scheduled:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-bulk-filter-scheduled",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()

	argsList := []map[string]any{
		{"id": 1},
		{"id": 2, "bad": make(chan bool)},
		{"id": 3},
	}

	jobs, err := client.EnqueueBulkIn(ctx, time.Hour, "scheduled_filter_job", argsList)
	if err == nil {
		t.Fatal("expected EnqueueBulkIn to return an error")
	}
	if jobs != nil {
		t.Errorf("EnqueueBulkIn returned jobs = %v, want nil", jobs)
	}

	scheduledCount, err := redisClient.ZCard(ctx, "test-bulk-filter-scheduled:scheduled").Result()
	if err != nil {
		t.Fatalf("failed to get scheduled count: %v", err)
	}
	if scheduledCount != 0 {
		t.Errorf("scheduled count = %d, want 0", scheduledCount)
	}
}

func TestClient_EnqueueBatch_WithAutoflushUsesChunks(t *testing.T) {
	redisClient := newTestRedisClient(t)
	flushTestKeys(t, redisClient, "test-batch-autoflush:*")

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-batch-autoflush",
	})
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	defer func() { _ = client.Close() }()

	hook := &commandSizeHook{}
	client.redis.AddHook(hook)

	batch := NewBatch().WithAutoflush(2)
	for i := range 5 {
		batch.Add("batch_autoflush_job", map[string]any{"index": i})
	}

	ctx := context.Background()
	if err := client.EnqueueBatch(ctx, batch); err != nil {
		t.Fatalf("EnqueueBatch(autoflush) error = %v, want nil", err)
	}

	if got := hook.evalCalls(); got != 3 {
		t.Fatalf("batch enqueue script calls = %d, want 3", got)
	}
	if got, wantMax := hook.maxEvalArgs(), 12; got > wantMax {
		t.Fatalf("max eval args = %d, want <= %d", got, wantMax)
	}
	if jobs, err := redisClient.SCard(ctx, client.keys.BatchJobs(batch.ID)).Result(); err != nil {
		t.Fatalf("SCard(batch jobs) error = %v, want nil", err)
	} else if jobs != 5 {
		t.Fatalf("SCard(batch jobs) = %d, want 5", jobs)
	}
	if queued, err := redisClient.LLen(ctx, client.keys.Queue("default")).Result(); err != nil {
		t.Fatalf("LLen(default) error = %v, want nil", err)
	} else if queued != 5 {
		t.Fatalf("LLen(default) = %d, want 5", queued)
	}
}

func TestClient_EnqueueBatchRejectsInvalidAutoflushChunkSize(t *testing.T) {
	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: "test-batch-invalid-autoflush",
	})
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
	defer func() { _ = client.Close() }()

	batch := NewBatch().WithAutoflush(0).Add("job", nil)
	err = client.EnqueueBatch(context.Background(), batch)
	if !errors.Is(err, ErrInvalidChunkSize) {
		t.Fatalf("EnqueueBatch(invalid autoflush chunk) error = %v, want %v", err, ErrInvalidChunkSize)
	}
}
