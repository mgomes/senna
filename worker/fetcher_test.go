package worker

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mgomes/senna"
	"github.com/mgomes/senna/internal/encryption"
	"github.com/mgomes/senna/internal/keys"
	"github.com/redis/go-redis/v9"
)

func selectTestQueue(ctx context.Context, f *fetcher, workerID string) string {
	processable, totalWeight := f.processableQueues(ctx, workerID)
	return selectProcessableQueue(processable, totalWeight)
}

func TestFetcher_SelectQueue_SingleQueue(t *testing.T) {
	client := newTestRedisClient(t)
	k := keys.New("test-fetcher")

	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "default", Priority: 1},
	}, 100*time.Millisecond, false)

	ctx := context.Background()
	for range 10 {
		queue := selectTestQueue(ctx, f, "worker-1")
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
	ctx := context.Background()

	for range iterations {
		queue := selectTestQueue(ctx, f, "worker-1")
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

	ctx := context.Background()
	for range 100 {
		queue := selectTestQueue(ctx, f, "worker-1")
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

	queue := selectTestQueue(context.Background(), f, "worker-1")
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

	queue := selectTestQueue(context.Background(), f, "worker-1")
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

func TestFetcher_MarkFinalizationPreservesEncryptedPayload(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-mark-finalization-encrypted:*")

	k := keys.New("test-mark-finalization-encrypted")
	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "default", Priority: 1},
	}, 100*time.Millisecond, false)

	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	enc, err := encryption.New(key)
	if err != nil {
		t.Fatalf("encryption.New() error = %v, want nil", err)
	}

	plainArgs := map[string]any{"secret": "plaintext"}
	encryptedArgs, err := enc.Encrypt(plainArgs)
	if err != nil {
		t.Fatalf("Encrypt() error = %v, want nil", err)
	}

	ctx := context.Background()
	job := senna.NewJob("test_job", encryptedArgs)
	job.Encrypted = true
	data, err := job.Marshal()
	if err != nil {
		t.Fatalf("Job.Marshal() error = %v, want nil", err)
	}
	job.SetRaw(string(data))
	if err := client.LPush(ctx, k.InFlight("worker-1"), string(data)).Err(); err != nil {
		t.Fatalf("seed in-flight: %v", err)
	}

	job.Args = plainArgs
	job.Encrypted = false
	if err := f.MarkFinalization(ctx, "worker-1", job, senna.JobFinalization{Operation: jobFinalizationComplete}); err != nil {
		t.Fatalf("MarkFinalization() error = %v, want nil", err)
	}

	items, err := client.LRange(ctx, k.InFlight("worker-1"), 0, -1).Result()
	if err != nil {
		t.Fatalf("LRange() error = %v, want nil", err)
	}
	if len(items) != 1 {
		t.Fatalf("in-flight length = %d, want 1", len(items))
	}
	if strings.Contains(items[0], "plaintext") {
		t.Fatalf("finalized payload contains plaintext args: %s", items[0])
	}

	markedJob, err := senna.UnmarshalJob([]byte(items[0]))
	if err != nil {
		t.Fatalf("UnmarshalJob(finalized payload) error = %v, want nil", err)
	}
	if !markedJob.Encrypted {
		t.Fatal("finalized job Encrypted = false, want true")
	}
	decrypted, err := enc.Decrypt(markedJob.Args)
	if err != nil {
		t.Fatalf("Decrypt(finalized args) error = %v, want nil", err)
	}
	if decrypted["secret"] != "plaintext" {
		t.Errorf("Decrypt(finalized args)[secret] = %v, want plaintext", decrypted["secret"])
	}
	finalization := markedJob.Finalization()
	if finalization == nil {
		t.Fatal("finalized job finalization = nil, want complete marker")
	}
	if finalization.Operation != jobFinalizationComplete {
		t.Errorf("finalized job operation = %q, want %q", finalization.Operation, jobFinalizationComplete)
	}
}

func TestFetcher_MarkFinalizationIgnoresBadQueueTypeForInFlightHit(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-mark-finalization-queue-type:*")

	k := keys.New("test-mark-finalization-queue-type")
	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "default", Priority: 1},
	}, 100*time.Millisecond, false)

	ctx := context.Background()
	job := senna.NewJob("test_job", nil)
	data, err := job.Marshal()
	if err != nil {
		t.Fatalf("Job.Marshal() error = %v, want nil", err)
	}
	job.SetRaw(string(data))
	if err := client.LPush(ctx, k.InFlight("worker-1"), string(data)).Err(); err != nil {
		t.Fatalf("seed in-flight: %v", err)
	}
	if err := client.Set(ctx, k.Queue("default"), "wrong-type", 0).Err(); err != nil {
		t.Fatalf("set queue wrong type: %v", err)
	}

	err = f.MarkFinalization(ctx, "worker-1", job, senna.JobFinalization{Operation: jobFinalizationComplete})
	if err != nil {
		t.Fatalf("MarkFinalization() error = %v, want nil", err)
	}

	items, err := client.LRange(ctx, k.InFlight("worker-1"), 0, -1).Result()
	if err != nil {
		t.Fatalf("LRange() error = %v, want nil", err)
	}
	if len(items) != 1 {
		t.Fatalf("in-flight length = %d, want 1", len(items))
	}
	markedJob, err := senna.UnmarshalJob([]byte(items[0]))
	if err != nil {
		t.Fatalf("UnmarshalJob(finalized payload) error = %v, want nil", err)
	}
	finalization := markedJob.Finalization()
	if finalization == nil {
		t.Fatal("finalized job finalization = nil, want complete marker")
	}
	if finalization.Operation != jobFinalizationComplete {
		t.Errorf("finalized job operation = %q, want %q", finalization.Operation, jobFinalizationComplete)
	}
}

func TestFetcher_MarkFinalizationRecognizesExistingFinalizedPayload(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-mark-finalization-existing:*")

	k := keys.New("test-mark-finalization-existing")
	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "default", Priority: 1},
	}, 100*time.Millisecond, false)

	ctx := context.Background()
	job := senna.NewJob("test_job", nil)
	data, err := job.Marshal()
	if err != nil {
		t.Fatalf("Job.Marshal() error = %v, want nil", err)
	}
	job.SetRaw(string(data))

	finalization := senna.JobFinalization{Operation: jobFinalizationComplete}
	finalizedData, err := payloadWithFinalization(string(data), finalization)
	if err != nil {
		t.Fatalf("payloadWithFinalization() error = %v, want nil", err)
	}
	if err := client.LPush(ctx, k.InFlight("worker-1"), string(finalizedData)).Err(); err != nil {
		t.Fatalf("seed finalized in-flight: %v", err)
	}

	err = f.MarkFinalization(ctx, "worker-1", job, finalization)
	if err != nil {
		t.Fatalf("MarkFinalization() error = %v, want nil", err)
	}
	if job.Finalization() == nil {
		t.Fatal("job finalization = nil, want complete marker")
	}
	if job.Raw() != string(finalizedData) {
		t.Fatal("job raw payload was not advanced to existing finalized payload")
	}

	err = f.Ack(ctx, "worker-1", job)
	if err != nil {
		t.Fatalf("Ack() error = %v, want nil", err)
	}
	inFlightLen, err := client.LLen(ctx, k.InFlight("worker-1")).Result()
	if err != nil {
		t.Fatalf("LLen() error = %v, want nil", err)
	}
	if inFlightLen != 0 {
		t.Errorf("in-flight length after Ack() = %d, want 0", inFlightLen)
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

func TestFetcher_Nack_KeepsInFlightWhenRetryWriteFails(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-nack-fail:*")

	k := keys.New("test-nack-fail")
	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "default", Priority: 1},
	}, 100*time.Millisecond, false)

	ctx := context.Background()

	job := senna.NewJob("test_job", nil)
	data, _ := job.Marshal()
	client.LPush(ctx, k.Queue("default"), string(data))

	fetched, _ := f.Fetch(ctx, "worker-1")
	if err := client.Set(ctx, k.Retry(), "wrong-type", 0).Err(); err != nil {
		t.Fatalf("Set(%q) retry key failed: %v", k.Retry(), err)
	}

	err := f.Nack(ctx, "worker-1", fetched, 5*time.Second)
	if err == nil {
		t.Fatalf("fetcher.Nack() error = nil, want non-nil")
	}

	inFlightLen, err := client.LLen(ctx, k.InFlight("worker-1")).Result()
	if err != nil {
		t.Fatalf("LLen(%q) error = %v, want nil", k.InFlight("worker-1"), err)
	}
	if inFlightLen != 1 {
		t.Errorf("LLen(%q) = %d, want %d", k.InFlight("worker-1"), inFlightLen, 1)
	}
	if fetched.RetryCount != 0 {
		t.Errorf("RetryCount = %d after failed retry write, want 0", fetched.RetryCount)
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

func TestFetcher_MoveToDead_KeepsInFlightWhenDeadWriteFails(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-dead-fail:*")

	k := keys.New("test-dead-fail")
	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "default", Priority: 1},
	}, 100*time.Millisecond, false)

	ctx := context.Background()

	job := senna.NewJob("test_job", nil)
	data, _ := job.Marshal()
	client.LPush(ctx, k.Queue("default"), string(data))

	fetched, _ := f.Fetch(ctx, "worker-1")
	if err := client.Set(ctx, k.Dead(), "wrong-type", 0).Err(); err != nil {
		t.Fatalf("Set(%q) dead key failed: %v", k.Dead(), err)
	}

	err := f.MoveToDead(ctx, "worker-1", fetched)
	if err == nil {
		t.Fatalf("fetcher.MoveToDead() error = nil, want non-nil")
	}

	inFlightLen, err := client.LLen(ctx, k.InFlight("worker-1")).Result()
	if err != nil {
		t.Fatalf("LLen(%q) error = %v, want nil", k.InFlight("worker-1"), err)
	}
	if inFlightLen != 1 {
		t.Errorf("LLen(%q) = %d, want %d", k.InFlight("worker-1"), inFlightLen, 1)
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

func TestFetcher_Requeue_KeepsInFlightWhenQueueWriteFails(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-requeue-fail:*")

	k := keys.New("test-requeue-fail")
	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "default", Priority: 1},
	}, 100*time.Millisecond, false)

	ctx := context.Background()

	job := senna.NewJob("test_job", nil)
	data, _ := job.Marshal()
	client.LPush(ctx, k.InFlight("worker-1"), string(data))
	if err := client.Set(ctx, k.Queue("default"), "wrong-type", 0).Err(); err != nil {
		t.Fatalf("Set(%q) queue key failed: %v", k.Queue("default"), err)
	}

	err := f.requeue(ctx, "worker-1", job)
	if err == nil {
		t.Fatalf("fetcher.requeue() error = nil, want non-nil")
	}

	inFlightLen, err := client.LLen(ctx, k.InFlight("worker-1")).Result()
	if err != nil {
		t.Fatalf("LLen(%q) error = %v, want nil", k.InFlight("worker-1"), err)
	}
	if inFlightLen != 1 {
		t.Errorf("LLen(%q) = %d, want %d", k.InFlight("worker-1"), inFlightLen, 1)
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

func TestFetcher_Sequential_AcquiresLock(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-seq-lock:*")

	k := keys.New("test-seq-lock")

	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "transforms", Priority: 1, Sequential: true},
	}, 100*time.Millisecond, false)

	ctx := context.Background()

	// Add a job to the sequential queue
	job := senna.NewJob("transform_job", nil)
	data, _ := job.Marshal()
	client.LPush(ctx, k.Queue("transforms"), string(data))

	// Fetch should acquire the lock and get the job
	fetched, err := f.Fetch(ctx, "worker-1")
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if fetched == nil {
		t.Fatal("expected job, got nil")
	}

	// Verify lock was acquired
	holder, err := client.Get(ctx, k.SequentialLock("transforms")).Result()
	if err != nil {
		t.Fatalf("failed to get lock: %v", err)
	}
	if holder != "worker-1" {
		t.Errorf("expected lock holder 'worker-1', got '%s'", holder)
	}
}

func TestFetcher_Sequential_UnmarshalErrorDiscardsPayloadAndReleasesLock(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-seq-invalid:*")

	k := keys.New("test-seq-invalid")
	queue := senna.QueueConfig{Name: "transforms", Priority: 1, Sequential: true}
	f1 := newFetcher(client, k, []senna.QueueConfig{queue}, 100*time.Millisecond, false)
	f2 := newFetcher(client, k, []senna.QueueConfig{queue}, 100*time.Millisecond, false)

	ctx := context.Background()
	if err := client.LPush(ctx, k.Queue("transforms"), "not-json").Err(); err != nil {
		t.Fatalf("LPush(invalid sequential payload) error = %v, want nil", err)
	}

	fetched, err := f1.Fetch(ctx, "worker-1")
	if err == nil {
		t.Fatal("Fetch(invalid sequential payload) error = nil, want decode error")
	}
	if fetched != nil {
		t.Fatalf("Fetch(invalid sequential payload) job = %#v, want nil", fetched)
	}

	lockExists, err := client.Exists(ctx, k.SequentialLock("transforms")).Result()
	if err != nil {
		t.Fatalf("Exists(%q) error = %v, want nil", k.SequentialLock("transforms"), err)
	}
	if lockExists != 0 {
		t.Errorf("sequential lock exists = %d, want 0", lockExists)
	}
	if f1.hasSequentialLock("transforms") {
		t.Error("local sequential lock held after invalid payload, want released")
	}

	inFlightLen, err := client.LLen(ctx, k.InFlight("worker-1")).Result()
	if err != nil {
		t.Fatalf("LLen(%q) error = %v, want nil", k.InFlight("worker-1"), err)
	}
	if inFlightLen != 0 {
		t.Errorf("LLen(%q) = %d, want 0", k.InFlight("worker-1"), inFlightLen)
	}

	validJob := senna.NewJob("valid_job", nil)
	validData, err := validJob.Marshal()
	if err != nil {
		t.Fatalf("Job.Marshal() error = %v, want nil", err)
	}
	if err := client.LPush(ctx, k.Queue("transforms"), string(validData)).Err(); err != nil {
		t.Fatalf("LPush(valid sequential payload) error = %v, want nil", err)
	}

	fetched, err = f2.Fetch(ctx, "worker-2")
	if err != nil {
		t.Fatalf("second Fetch() error = %v, want nil", err)
	}
	if fetched == nil {
		t.Fatal("second Fetch() job = nil, want valid job")
	}
	if fetched.ID != validJob.ID {
		t.Errorf("second Fetch() job ID = %q, want %q", fetched.ID, validJob.ID)
	}
}

func TestFetcher_Sequential_DiscardClaimedPayloadIgnoresCancellation(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-seq-invalid-cancel:*")

	k := keys.New("test-seq-invalid-cancel")
	queue := senna.QueueConfig{Name: "transforms", Priority: 1, Sequential: true}
	f := newFetcher(client, k, []senna.QueueConfig{queue}, 100*time.Millisecond, false)

	ctx := context.Background()
	workerID := "worker-1"
	payload := "not-json"
	if err := client.LPush(ctx, k.InFlight(workerID), payload).Err(); err != nil {
		t.Fatalf("LPush(%q) error = %v, want nil", k.InFlight(workerID), err)
	}
	if err := client.Set(ctx, k.SequentialLock("transforms"), workerID, sequentialLockTTL).Err(); err != nil {
		t.Fatalf("Set(%q) error = %v, want nil", k.SequentialLock("transforms"), err)
	}

	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()

	if err := f.discardClaimedSequentialPayload(canceledCtx, workerID, "transforms", payload); err != nil {
		t.Fatalf("discardClaimedSequentialPayload(canceled context) error = %v, want nil", err)
	}

	inFlightLen, err := client.LLen(ctx, k.InFlight(workerID)).Result()
	if err != nil {
		t.Fatalf("LLen(%q) error = %v, want nil", k.InFlight(workerID), err)
	}
	if inFlightLen != 0 {
		t.Errorf("LLen(%q) = %d, want 0", k.InFlight(workerID), inFlightLen)
	}

	lockExists, err := client.Exists(ctx, k.SequentialLock("transforms")).Result()
	if err != nil {
		t.Fatalf("Exists(%q) error = %v, want nil", k.SequentialLock("transforms"), err)
	}
	if lockExists != 0 {
		t.Errorf("Exists(%q) = %d, want 0", k.SequentialLock("transforms"), lockExists)
	}
}

func TestFetcher_Sequential_OnlyOneWorkerProcesses(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-seq-exclusive:*")

	k := keys.New("test-seq-exclusive")

	// Two fetchers for different workers
	f1 := newFetcher(client, k, []senna.QueueConfig{
		{Name: "transforms", Priority: 1, Sequential: true},
	}, 100*time.Millisecond, false)

	f2 := newFetcher(client, k, []senna.QueueConfig{
		{Name: "transforms", Priority: 1, Sequential: true},
	}, 100*time.Millisecond, false)

	ctx := context.Background()

	// Add two jobs
	job1 := senna.NewJob("job1", nil)
	data1, _ := job1.Marshal()
	client.LPush(ctx, k.Queue("transforms"), string(data1))

	job2 := senna.NewJob("job2", nil)
	data2, _ := job2.Marshal()
	client.LPush(ctx, k.Queue("transforms"), string(data2))

	// Worker 1 fetches first - should get the job and lock
	fetched1, err := f1.Fetch(ctx, "worker-1")
	if err != nil {
		t.Fatalf("worker-1 fetch failed: %v", err)
	}
	if fetched1 == nil {
		t.Fatal("worker-1 should have gotten a job")
	}

	// Worker 2 tries to fetch - should get nil (can't acquire lock)
	fetched2, err := f2.Fetch(ctx, "worker-2")
	if err != nil {
		t.Fatalf("worker-2 fetch failed: %v", err)
	}
	if fetched2 != nil {
		t.Error("worker-2 should not have gotten a job (lock held by worker-1)")
	}

	// Verify worker-1 still holds the lock
	holder, _ := client.Get(ctx, k.SequentialLock("transforms")).Result()
	if holder != "worker-1" {
		t.Errorf("expected lock holder 'worker-1', got '%s'", holder)
	}
}

func TestFetcher_Sequential_LockRenewal(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-seq-renew:*")

	k := keys.New("test-seq-renew")

	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "transforms", Priority: 1, Sequential: true},
	}, 100*time.Millisecond, false)

	ctx := context.Background()

	// Add jobs
	for range 3 {
		job := senna.NewJob("job", nil)
		data, _ := job.Marshal()
		client.LPush(ctx, k.Queue("transforms"), string(data))
	}

	// Fetch and process jobs one at a time (sequential queue semantics)
	for i := range 3 {
		fetched, err := f.Fetch(ctx, "worker-1")
		if err != nil {
			t.Fatalf("fetch %d failed: %v", i, err)
		}
		if fetched == nil {
			t.Fatalf("fetch %d: expected job, got nil", i)
		}

		// Verify lock is held with full TTL
		ttl, err := client.TTL(ctx, k.SequentialLock("transforms")).Result()
		if err != nil {
			t.Fatalf("failed to get TTL: %v", err)
		}
		if ttl < 25*time.Second {
			t.Errorf("expected TTL > 25s, got %v", ttl)
		}

		// Simulate job processing and test lock renewal
		f.RenewSequentialLocks(ctx, "worker-1")

		// TTL should still be close to 30 seconds after renewal
		ttl, err = client.TTL(ctx, k.SequentialLock("transforms")).Result()
		if err != nil {
			t.Fatalf("failed to get TTL after renewal: %v", err)
		}
		if ttl < 25*time.Second {
			t.Errorf("expected TTL > 25s after renewal, got %v", ttl)
		}

		// Release lock to allow next fetch (simulates ack/nack completing)
		f.ReleaseSequentialLock(ctx, "worker-1", "transforms")
	}
}

func TestFetcher_Sequential_LockExpires(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-seq-expire:*")

	k := keys.New("test-seq-expire")

	f1 := newFetcher(client, k, []senna.QueueConfig{
		{Name: "transforms", Priority: 1, Sequential: true},
	}, 100*time.Millisecond, false)

	f2 := newFetcher(client, k, []senna.QueueConfig{
		{Name: "transforms", Priority: 1, Sequential: true},
	}, 100*time.Millisecond, false)

	ctx := context.Background()

	// Add a job
	job := senna.NewJob("job", nil)
	data, _ := job.Marshal()
	client.LPush(ctx, k.Queue("transforms"), string(data))

	// Worker 1 acquires lock
	_, _ = f1.Fetch(ctx, "worker-1")

	// Simulate lock expiry by deleting it
	client.Del(ctx, k.SequentialLock("transforms"))

	// Add another job
	job2 := senna.NewJob("job2", nil)
	data2, _ := job2.Marshal()
	client.LPush(ctx, k.Queue("transforms"), string(data2))

	// Worker 2 should now be able to acquire lock and fetch
	fetched, err := f2.Fetch(ctx, "worker-2")
	if err != nil {
		t.Fatalf("worker-2 fetch failed: %v", err)
	}
	if fetched == nil {
		t.Fatal("worker-2 should have gotten a job after lock expired")
	}

	// Verify worker-2 now holds the lock
	holder, _ := client.Get(ctx, k.SequentialLock("transforms")).Result()
	if holder != "worker-2" {
		t.Errorf("expected lock holder 'worker-2', got '%s'", holder)
	}
}

func TestFetcher_Sequential_MixedQueues(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-seq-mixed:*")

	k := keys.New("test-seq-mixed")

	// One sequential queue, one regular queue
	f2 := newFetcher(client, k, []senna.QueueConfig{
		{Name: "transforms", Priority: 1, Sequential: true},
		{Name: "default", Priority: 1},
	}, 100*time.Millisecond, false)

	ctx := context.Background()

	// Add jobs to both queues
	seqJob := senna.NewJob("seq_job", nil)
	seqData, _ := seqJob.Marshal()
	client.LPush(ctx, k.Queue("transforms"), string(seqData))

	defaultJob := senna.NewJob("default_job", nil)
	defaultData, _ := defaultJob.Marshal()
	client.LPush(ctx, k.Queue("default"), string(defaultData))

	// Simulate worker-1 holding the sequential lock
	if err := client.SetArgs(ctx, k.SequentialLock("transforms"), "worker-1", redis.SetArgs{
		Mode: "NX",
		TTL:  30 * time.Second,
	}).Err(); err != nil {
		t.Fatalf("failed to set sequential lock: %v", err)
	}

	// Worker 2 should only be able to fetch from default queue
	// since worker-1 holds the sequential lock
	fetched, err := f2.Fetch(ctx, "worker-2")
	if err != nil {
		t.Fatalf("worker-2 fetch failed: %v", err)
	}
	if fetched == nil {
		t.Fatal("worker-2 should have gotten a job from default queue")
	}
	if fetched.Type != "default_job" {
		t.Errorf("expected default_job, got %s", fetched.Type)
	}
}

func TestFetcher_Sequential_BlockingFetch(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-seq-blocking:*")

	k := keys.New("test-seq-blocking")

	f2 := newFetcher(client, k, []senna.QueueConfig{
		{Name: "transforms", Priority: 1, Sequential: true},
	}, 100*time.Millisecond, false)

	ctx := context.Background()

	// Simulate worker-1 holding the lock
	if err := client.SetArgs(ctx, k.SequentialLock("transforms"), "worker-1", redis.SetArgs{
		Mode: "NX",
		TTL:  30 * time.Second,
	}).Err(); err != nil {
		t.Fatalf("failed to set sequential lock: %v", err)
	}

	// Worker 2's blocking fetch should timeout (can't acquire lock)
	start := time.Now()
	fetched, err := f2.BlockingFetch(ctx, "worker-2", 200*time.Millisecond)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("blocking fetch failed: %v", err)
	}
	if fetched != nil {
		t.Error("worker-2 should not have gotten a job")
	}
	if elapsed < 150*time.Millisecond {
		t.Errorf("should have waited for timeout, only waited %v", elapsed)
	}
}

func TestFetcher_Sequential_NoLockOnEmptyQueue(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-seq-empty:*")

	k := keys.New("test-seq-empty")

	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "transforms", Priority: 1, Sequential: true},
	}, 100*time.Millisecond, false)

	ctx := context.Background()

	// Queue is empty - fetch should return nil and NOT acquire lock
	fetched, err := f.Fetch(ctx, "worker-1")
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if fetched != nil {
		t.Fatal("expected nil from empty queue")
	}

	// Verify NO lock was acquired
	exists, _ := client.Exists(ctx, k.SequentialLock("transforms")).Result()
	if exists != 0 {
		t.Error("lock should NOT be acquired when queue is empty")
	}

	// Now add a job and fetch - lock SHOULD be acquired
	job := senna.NewJob("test_job", nil)
	data, _ := job.Marshal()
	client.LPush(ctx, k.Queue("transforms"), string(data))

	fetched, err = f.Fetch(ctx, "worker-1")
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if fetched == nil {
		t.Fatal("expected job")
	}

	// Verify lock was acquired
	holder, err := client.Get(ctx, k.SequentialLock("transforms")).Result()
	if err != nil {
		t.Fatalf("failed to get lock: %v", err)
	}
	if holder != "worker-1" {
		t.Errorf("expected lock holder 'worker-1', got '%s'", holder)
	}
}

func TestFetcher_Sequential_ReleaseSequentialLock(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-seq-release:*")

	k := keys.New("test-seq-release")

	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "transforms", Priority: 1, Sequential: true},
	}, 100*time.Millisecond, false)

	ctx := context.Background()

	// Set up a lock held by worker-1
	client.Set(ctx, k.SequentialLock("transforms"), "worker-1", 30*time.Second)

	// Verify lock exists
	holder, _ := client.Get(ctx, k.SequentialLock("transforms")).Result()
	if holder != "worker-1" {
		t.Fatalf("expected lock holder 'worker-1', got '%s'", holder)
	}

	// Release should work for the holder
	f.ReleaseSequentialLock(ctx, "worker-1", "transforms")

	// Verify lock is released
	exists, _ := client.Exists(ctx, k.SequentialLock("transforms")).Result()
	if exists != 0 {
		t.Error("lock should be released after ReleaseSequentialLock")
	}

	// Set up lock again
	client.Set(ctx, k.SequentialLock("transforms"), "worker-1", 30*time.Second)

	// Release should NOT work for a different worker
	f.ReleaseSequentialLock(ctx, "worker-2", "transforms")

	// Verify lock still exists (not released by wrong worker)
	holder, _ = client.Get(ctx, k.SequentialLock("transforms")).Result()
	if holder != "worker-1" {
		t.Errorf("lock should not be released by wrong worker, got holder '%s'", holder)
	}
}

func TestFetcher_Sequential_RenewSequentialLocks(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-seq-renew-bg:*")

	k := keys.New("test-seq-renew-bg")

	f := newFetcher(client, k, []senna.QueueConfig{
		{Name: "transforms", Priority: 1, Sequential: true},
		{Name: "default", Priority: 1}, // Non-sequential queue
	}, 100*time.Millisecond, false)

	ctx := context.Background()

	// Acquire lock for worker-1
	if err := client.SetArgs(ctx, k.SequentialLock("transforms"), "worker-1", redis.SetArgs{
		Mode: "NX",
		TTL:  5 * time.Second,
	}).Err(); err != nil {
		t.Fatalf("failed to set sequential lock: %v", err)
	}
	f.holdSequentialLock("transforms")

	// Verify initial TTL
	ttl1, _ := client.TTL(ctx, k.SequentialLock("transforms")).Result()
	if ttl1 < 4*time.Second || ttl1 > 5*time.Second {
		t.Errorf("expected initial TTL ~5s, got %v", ttl1)
	}

	// Simulate time passing (reduce TTL manually)
	client.Expire(ctx, k.SequentialLock("transforms"), 2*time.Second)

	// Call RenewSequentialLocks - should extend TTL back to 30s
	f.RenewSequentialLocks(ctx, "worker-1")

	// Verify TTL was renewed to 30s
	ttl2, _ := client.TTL(ctx, k.SequentialLock("transforms")).Result()
	if ttl2 < 25*time.Second {
		t.Errorf("expected renewed TTL ~30s, got %v", ttl2)
	}

	// Verify RenewSequentialLocks doesn't renew locks held by other workers
	client.Set(ctx, k.SequentialLock("transforms"), "worker-2", 5*time.Second)
	f.RenewSequentialLocks(ctx, "worker-1")

	ttl3, _ := client.TTL(ctx, k.SequentialLock("transforms")).Result()
	if ttl3 > 5*time.Second {
		t.Errorf("should not have renewed lock held by worker-2, TTL: %v", ttl3)
	}
}
