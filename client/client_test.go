package client

import (
	"context"
	"crypto/aes"
	"errors"
	"testing"
	"time"

	"github.com/mgomes/senna"
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

	keysLeft, err := redisClient.Exists(ctx, client.keys.Batch(batch.ID), client.keys.BatchJobs(batch.ID)).Result()
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
	beforeEnqueue := time.Now()

	_, err = client.EnqueueIn(ctx, delay, "delayed_job", nil)
	if err != nil {
		t.Fatalf("enqueue in failed: %v", err)
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
	expectedMax := float64(beforeEnqueue.Add(delay).Unix() + 2)

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

	argsList := []map[string]any{
		{"id": 1},
		{"id": 2},
		{"id": 3},
	}

	jobs, err := client.EnqueueBulkIn(ctx, time.Hour, "delayed_bulk_job", argsList)
	if err != nil {
		t.Fatalf("EnqueueBulkIn failed: %v", err)
	}

	if len(jobs) != 3 {
		t.Fatalf("expected 3 jobs, got %d", len(jobs))
	}

	// Verify jobs are in scheduled set, not queue
	scheduledLen, _ := redisClient.ZCard(ctx, "test-bulk-in:scheduled").Result()
	if scheduledLen != 3 {
		t.Errorf("expected 3 jobs in scheduled, got %d", scheduledLen)
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
