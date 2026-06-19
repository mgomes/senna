package senna_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/mgomes/senna"
	"github.com/mgomes/senna/client"
	"github.com/mgomes/senna/worker"
	"github.com/redis/go-redis/v9"
)

func TestBatch_CallbackOptions(t *testing.T) {
	batch := client.NewBatch().
		WithDescription("Test batch").
		Add("job1", map[string]any{"key": "value"}).
		OnCompleteCallback("complete_handler", map[string]any{"notify": "admin@example.com"}).
		OnSuccessCallback("success_handler", map[string]any{"send_report": true}).
		OnDeathCallback("death_handler", map[string]any{"alert": "pagerduty"}).
		WithCallbackQueue("critical")

	if batch.Description != "Test batch" {
		t.Errorf("expected description 'Test batch', got '%s'", batch.Description)
	}
	if batch.OnComplete == nil || batch.OnComplete.JobType != "complete_handler" {
		t.Error("OnComplete not set correctly")
	}
	if batch.OnComplete.Options["notify"] != "admin@example.com" {
		t.Error("OnComplete options not set correctly")
	}
	if batch.OnSuccess == nil || batch.OnSuccess.JobType != "success_handler" {
		t.Error("OnSuccess not set correctly")
	}
	if batch.OnDeath == nil || batch.OnDeath.JobType != "death_handler" {
		t.Error("OnDeath not set correctly")
	}
	if batch.CallbackQueue != "critical" {
		t.Errorf("expected callback queue 'critical', got '%s'", batch.CallbackQueue)
	}
}

func TestBatch_TTLSetsExpire(t *testing.T) {
	flushKeysBatch(t, "batch-ttl:*")

	c, err := client.New(&client.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: "batch-ttl",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	batch := client.NewBatch().Add("job", nil)
	if err := c.EnqueueBatch(ctx, batch); err != nil {
		t.Fatalf("enqueue batch failed: %v", err)
	}

	stateTTL, err := c.Redis().TTL(ctx, "batch-ttl:batch:"+batch.ID).Result()
	if err != nil {
		t.Fatalf("failed to read state ttl: %v", err)
	}
	jobsTTL, err := c.Redis().TTL(ctx, "batch-ttl:batch:"+batch.ID+":jobs").Result()
	if err != nil {
		t.Fatalf("failed to read jobs ttl: %v", err)
	}

	// Note: :failed key is only created when jobs actually fail,
	// so we don't check its TTL here

	minTTL := 25 * 24 * time.Hour
	if stateTTL <= minTTL {
		t.Fatalf("expected state ttl > %v, got %v", minTTL, stateTTL)
	}
	if jobsTTL <= minTTL {
		t.Fatalf("expected jobs ttl > %v, got %v", minTTL, jobsTTL)
	}
}

func TestBatch_UnsupportedOptions(t *testing.T) {
	flushKeysBatch(t, "batch-unsupported:*")

	c, err := client.New(&client.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: "batch-unsupported",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	batch := client.NewBatch().
		Add("job", nil,
			client.WithUniqueKey("unique", time.Minute),
			client.WithEncryption(),
			client.WithDelay(time.Second),
			client.WithBulkChunkSize(10),
		)

	if len(batch.Jobs) != 0 {
		t.Fatalf("batch should not accept unsupported job options, got %d jobs", len(batch.Jobs))
	}

	err = c.EnqueueBatch(ctx, batch)
	if err == nil {
		t.Fatal("expected enqueue batch to fail for unsupported options")
	}
	if !strings.Contains(err.Error(), "does not support options") {
		t.Fatalf("unexpected error: %v", err)
	}

	exists, err := c.Redis().Exists(ctx, "batch-unsupported:batch:"+batch.ID).Result()
	if err != nil {
		t.Fatalf("failed to check batch state: %v", err)
	}
	if exists != 0 {
		t.Fatalf("batch state should not be created when options are unsupported")
	}
}

func TestBatch_SuccessCallback(t *testing.T) {
	flushKeysBatch(t, "batch-success:*")

	c, err := client.New(&client.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: "batch-success",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = c.Close() }()

	w, err := worker.New(&worker.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: "batch-success",
		Settings: senna.WorkerSettings{
			Concurrency:     2,
			Queues:          []senna.QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout: 5 * time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	var jobsProcessed atomic.Int32
	var completeCallbackCalled atomic.Bool
	var successCallbackCalled atomic.Bool
	var receivedBatchID string
	var receivedOptions map[string]any

	w.Register("batch_job", func(ctx context.Context, job *senna.Job) error {
		jobsProcessed.Add(1)
		return nil
	})

	w.Register("on_complete", func(ctx context.Context, job *senna.Job) error {
		receivedBatchID = job.Args["batch_id"].(string)
		completeCallbackCalled.Store(true)
		return nil
	})

	w.Register("on_success", func(ctx context.Context, job *senna.Job) error {
		receivedOptions = job.Args
		successCallbackCalled.Store(true)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = w.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	batch := client.NewBatch().
		Add("batch_job", nil).
		Add("batch_job", nil).
		Add("batch_job", nil).
		OnCompleteCallback("on_complete").
		OnSuccessCallback("on_success", map[string]any{"user_id": float64(123)})

	if err := c.EnqueueBatch(ctx, batch); err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	// Wait for all jobs and callbacks to complete
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if jobsProcessed.Load() >= 3 && completeCallbackCalled.Load() && successCallbackCalled.Load() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	time.Sleep(100 * time.Millisecond)

	if jobsProcessed.Load() != 3 {
		t.Errorf("expected 3 jobs processed, got %d", jobsProcessed.Load())
	}
	if !completeCallbackCalled.Load() {
		t.Error("complete callback should have been called")
	}
	if !successCallbackCalled.Load() {
		t.Error("success callback should have been called")
	}
	if receivedBatchID != batch.ID {
		t.Errorf("expected batch ID '%s' in callback, got '%s'", batch.ID, receivedBatchID)
	}
	if receivedOptions["user_id"] != float64(123) {
		t.Errorf("expected user_id 123 in callback options, got %v", receivedOptions["user_id"])
	}
}

func TestBatch_DeathCallback(t *testing.T) {
	flushKeysBatch(t, "batch-death:*")

	c, err := client.New(&client.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: "batch-death",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = c.Close() }()

	w, err := worker.New(&worker.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: "batch-death",
		Settings: senna.WorkerSettings{
			Concurrency:     2,
			Queues:          []senna.QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout: 5 * time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	var deathCallbackCalled atomic.Int32
	var completeCallbackCalled atomic.Bool
	var successCallbackCalled atomic.Bool

	w.Register("failing_job", func(ctx context.Context, job *senna.Job) error {
		return errors.New("intentional failure")
	}, worker.WithJobMaxRetries(0)) // No retries - immediate death

	w.Register("on_complete", func(ctx context.Context, job *senna.Job) error {
		completeCallbackCalled.Store(true)
		return nil
	})

	w.Register("on_success", func(ctx context.Context, job *senna.Job) error {
		successCallbackCalled.Store(true)
		return nil
	})

	w.Register("on_death", func(ctx context.Context, job *senna.Job) error {
		deathCallbackCalled.Add(1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = w.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	batch := client.NewBatch().
		Add("failing_job", nil).
		Add("failing_job", nil). // Two failing jobs
		OnCompleteCallback("on_complete").
		OnSuccessCallback("on_success").
		OnDeathCallback("on_death")

	if err := c.EnqueueBatch(ctx, batch); err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	// Wait for callbacks
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if deathCallbackCalled.Load() > 0 && completeCallbackCalled.Load() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	time.Sleep(100 * time.Millisecond)

	// Death callback should fire only once (first death)
	if deathCallbackCalled.Load() != 1 {
		t.Errorf("death callback should be called exactly once, got %d", deathCallbackCalled.Load())
	}
	if !completeCallbackCalled.Load() {
		t.Error("complete callback should have been called")
	}
	if successCallbackCalled.Load() {
		t.Error("success callback should NOT have been called (batch has deaths)")
	}
}

func TestBatch_Status(t *testing.T) {
	flushKeysBatch(t, "batch-status:*")

	c, err := client.New(&client.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: "batch-status",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = c.Close() }()

	batch := client.NewBatch().
		WithDescription("Status test batch").
		Add("job1", nil).
		Add("job2", nil).
		Add("job3", nil)

	ctx := context.Background()
	if err := c.EnqueueBatch(ctx, batch); err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	// Get batch status
	status := c.BatchStatus(batch.ID)
	if err := status.Refresh(ctx); err != nil {
		t.Fatalf("failed to refresh status: %v", err)
	}

	if status.BID() != batch.ID {
		t.Errorf("expected BID '%s', got '%s'", batch.ID, status.BID())
	}
	if status.Total() != 3 {
		t.Errorf("expected total 3, got %d", status.Total())
	}
	if status.Pending() != 3 {
		t.Errorf("expected pending 3, got %d", status.Pending())
	}
	if status.Successes() != 0 {
		t.Errorf("expected successes 0, got %d", status.Successes())
	}
	if status.Complete() {
		t.Error("batch should not be complete yet")
	}
	if status.Description() != "Status test batch" {
		t.Errorf("expected description 'Status test batch', got '%s'", status.Description())
	}

	// Test Data() method
	data := status.Data()
	if data["bid"] != batch.ID {
		t.Error("Data() should include bid")
	}
	if data["total"] != 3 {
		t.Error("Data() should include total")
	}
}

func TestBatchStatusJoinPreservesMethodSignature(t *testing.T) {
	var _ interface {
		Join(context.Context) error
	} = (*senna.BatchStatus)(nil)
}

func TestBatchStatusJoinWithInterval(t *testing.T) {
	const namespace = "batch-join-interval"
	const bid = "batch-join-interval-bid"
	flushKeysBatch(t, namespace+":*")

	redisClient := newTestRedisClient(t)
	ctx := context.Background()
	status := senna.NewBatchStatus(redisClient, namespace, bid)

	pendingState := senna.BatchState{
		ID:        bid,
		Total:     1,
		Pending:   1,
		CreatedAt: time.Now(),
	}
	pendingData, err := json.Marshal(pendingState)
	if err != nil {
		t.Fatalf("json.Marshal(pending BatchState) error = %v, want nil", err)
	}
	batchKey := namespace + ":batch:" + bid
	if err := redisClient.Set(ctx, batchKey, string(pendingData), 0).Err(); err != nil {
		t.Fatalf("Set(%q, pending state) error = %v, want nil", batchKey, err)
	}

	updateErr := make(chan error, 1)
	go func() {
		time.Sleep(25 * time.Millisecond)
		completeState := pendingState
		completeState.Pending = 0
		completeData, err := json.Marshal(completeState)
		if err != nil {
			updateErr <- err
			return
		}
		updateErr <- redisClient.Set(context.Background(), batchKey, string(completeData), 0).Err()
	}()

	joinCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	if err := status.JoinWithInterval(joinCtx, 10*time.Millisecond); err != nil {
		t.Fatalf("BatchStatus.JoinWithInterval(ctx, 10ms) error = %v, want nil", err)
	}

	if err := <-updateErr; err != nil {
		t.Fatalf("complete batch update error = %v, want nil", err)
	}
	if !status.Complete() {
		t.Fatal("BatchStatus.Complete() = false, want true")
	}
}

func TestBatchStatusRefreshDoesNotLoadFailedJIDs(t *testing.T) {
	const namespace = "batch-refresh-cheap"
	const bid = "batch-refresh-cheap-bid"
	flushKeysBatch(t, namespace+":*")

	redisClient := newTestRedisClient(t)
	ctx := context.Background()
	batchKey := namespace + ":batch:" + bid
	failedKey := batchKey + ":failed"
	state := senna.BatchState{
		ID:        bid,
		Total:     3,
		Pending:   0,
		Failures:  2,
		Successes: 1,
		CreatedAt: time.Now(),
	}
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("json.Marshal(BatchState) error = %v, want nil", err)
	}
	if err := redisClient.Set(ctx, batchKey, string(data), 0).Err(); err != nil {
		t.Fatalf("Set(%q, batch state) error = %v, want nil", batchKey, err)
	}
	if err := redisClient.SAdd(ctx, failedKey, "jid-1", "jid-2").Err(); err != nil {
		t.Fatalf("SAdd(%q, failed JIDs) error = %v, want nil", failedKey, err)
	}

	hook := &batchStatusCommandHook{}
	redisClient.AddHook(hook)
	status := senna.NewBatchStatus(redisClient, namespace, bid)
	if err := status.Refresh(ctx); err != nil {
		t.Fatalf("BatchStatus.Refresh(ctx) error = %v, want nil", err)
	}

	if got := hook.singleCommands.Load(); got != 0 {
		t.Errorf("single Redis commands after Refresh = %d, want 0", got)
	}
	if got := hook.pipelines.Load(); got != 1 {
		t.Errorf("Redis pipelines after Refresh = %d, want 1", got)
	}
	if got := hook.pipelineCommands.Load(); got != 2 {
		t.Errorf("Redis pipeline command count = %d, want 2", got)
	}
	wantCommandNames := []string{"get", "hgetall"}
	if diff := cmp.Diff(wantCommandNames, hook.pipelineCommandNames(), cmpopts.SortSlices(func(a, b string) bool {
		return a < b
	})); diff != "" {
		t.Errorf("Redis command names after Refresh mismatch (-want +got):\n%s", diff)
	}
	if _, ok := status.Data()["failed_jids"]; ok {
		t.Fatal("BatchStatus.Data()[\"failed_jids\"] exists after Refresh, want absent")
	}
}

func TestBatchStatusRefreshFullLoadsFailedJIDsInSinglePipeline(t *testing.T) {
	const namespace = "batch-full-status"
	const bid = "batch-full-status-bid"
	flushKeysBatch(t, namespace+":*")

	redisClient := newTestRedisClient(t)
	ctx := context.Background()
	batchKey := namespace + ":batch:" + bid
	failedKey := batchKey + ":failed"
	state := senna.BatchState{
		ID:        bid,
		Total:     3,
		Pending:   0,
		Failures:  2,
		Successes: 1,
		CreatedAt: time.Now(),
	}
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("json.Marshal(BatchState) error = %v, want nil", err)
	}
	if err := redisClient.Set(ctx, batchKey, string(data), 0).Err(); err != nil {
		t.Fatalf("Set(%q, batch state) error = %v, want nil", batchKey, err)
	}
	if err := redisClient.SAdd(ctx, failedKey, "jid-1", "jid-2").Err(); err != nil {
		t.Fatalf("SAdd(%q, failed JIDs) error = %v, want nil", failedKey, err)
	}

	hook := &batchStatusCommandHook{}
	redisClient.AddHook(hook)
	status := senna.NewBatchStatus(redisClient, namespace, bid)
	if err := status.RefreshFull(ctx); err != nil {
		t.Fatalf("BatchStatus.RefreshFull(ctx) error = %v, want nil", err)
	}
	failedJIDs, err := status.FailedJIDs(ctx)
	if err != nil {
		t.Fatalf("BatchStatus.FailedJIDs(ctx) error = %v, want nil", err)
	}

	wantFailedJIDs := []string{"jid-1", "jid-2"}
	if diff := cmp.Diff(wantFailedJIDs, failedJIDs, cmpopts.SortSlices(func(a, b string) bool {
		return a < b
	})); diff != "" {
		t.Errorf("BatchStatus.FailedJIDs(ctx) mismatch (-want +got):\n%s", diff)
	}
	dataView := status.Data()
	gotDataFailedJIDs, ok := dataView["failed_jids"].([]string)
	if !ok {
		t.Fatalf("BatchStatus.Data()[%q] = %T, want []string", "failed_jids", dataView["failed_jids"])
	}
	if diff := cmp.Diff(wantFailedJIDs, gotDataFailedJIDs, cmpopts.SortSlices(func(a, b string) bool {
		return a < b
	})); diff != "" {
		t.Errorf("BatchStatus.Data()[%q] mismatch (-want +got):\n%s", "failed_jids", diff)
	}

	if got := hook.singleCommands.Load(); got != 0 {
		t.Errorf("single Redis commands after hook install = %d, want 0", got)
	}
	if got := hook.pipelines.Load(); got != 1 {
		t.Errorf("Redis pipelines after hook install = %d, want 1", got)
	}
	if got := hook.pipelineCommands.Load(); got != 3 {
		t.Errorf("Redis pipeline command count = %d, want 3", got)
	}
	wantCommandNames := []string{"get", "hgetall", "smembers"}
	if diff := cmp.Diff(wantCommandNames, hook.pipelineCommandNames(), cmpopts.SortSlices(func(a, b string) bool {
		return a < b
	})); diff != "" {
		t.Errorf("Redis pipeline command names mismatch (-want +got):\n%s", diff)
	}
}

type batchStatusCommandHook struct {
	singleCommands   atomic.Int64
	pipelines        atomic.Int64
	pipelineCommands atomic.Int64
	mu               sync.Mutex
	pipelineNames    []string
}

func (h *batchStatusCommandHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *batchStatusCommandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.singleCommands.Add(1)
		return next(ctx, cmd)
	}
}

func (h *batchStatusCommandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.pipelines.Add(1)
		h.pipelineCommands.Add(int64(len(cmds)))

		h.mu.Lock()
		for _, cmd := range cmds {
			h.pipelineNames = append(h.pipelineNames, cmd.Name())
		}
		h.mu.Unlock()

		return next(ctx, cmds)
	}
}

func (h *batchStatusCommandHook) pipelineCommandNames() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	names := make([]string, len(h.pipelineNames))
	copy(names, h.pipelineNames)
	return names
}

func TestBatch_InvalidBatchStatus(t *testing.T) {
	flushKeysBatch(t, "batch-invalid:*")

	redisClient := newTestRedisClient(t)
	defer func() { _ = redisClient.Close() }()

	status := senna.NewBatchStatus(redisClient, "batch-invalid", "nonexistent")
	err := status.Refresh(context.Background())

	var notFound *senna.BatchNotFoundError
	if !errors.As(err, &notFound) {
		t.Errorf("expected BatchNotFoundError, got %T: %v", err, err)
	}
}

func TestBatchStatusDelete_ReturnsSetRemovalErrors(t *testing.T) {
	namespace := "batch-delete-errors"
	flushKeysBatch(t, namespace+":*")

	redisClient := newTestRedisClient(t)
	defer func() { _ = redisClient.Close() }()

	ctx := context.Background()
	bid := "batch-1"
	batchKey := namespace + ":batch:" + bid
	batchProgressKey := batchKey + ":progress"
	batchJobsKey := batchKey + ":jobs"
	batchFailedKey := batchKey + ":failed"
	batchCallbacksKey := batchKey + ":callbacks"

	if err := redisClient.Set(ctx, namespace+":batches", "not-a-set", 0).Err(); err != nil {
		t.Fatalf("failed to seed batches key: %v", err)
	}
	if err := redisClient.Set(ctx, namespace+":batches:dead", "not-a-set", 0).Err(); err != nil {
		t.Fatalf("failed to seed dead batches key: %v", err)
	}
	if err := redisClient.Set(ctx, batchKey, "{}", 0).Err(); err != nil {
		t.Fatalf("failed to seed batch key: %v", err)
	}
	if err := redisClient.HSet(ctx, batchProgressKey, "pending", 1).Err(); err != nil {
		t.Fatalf("failed to seed batch progress key: %v", err)
	}
	if err := redisClient.Set(ctx, batchJobsKey, "{}", 0).Err(); err != nil {
		t.Fatalf("failed to seed batch jobs key: %v", err)
	}
	if err := redisClient.Set(ctx, batchFailedKey, "{}", 0).Err(); err != nil {
		t.Fatalf("failed to seed batch failed key: %v", err)
	}
	if err := redisClient.Set(ctx, batchCallbacksKey, "{}", 0).Err(); err != nil {
		t.Fatalf("failed to seed batch callbacks key: %v", err)
	}

	status := senna.NewBatchStatus(redisClient, namespace, bid)
	err := status.Delete(ctx)
	if err == nil {
		t.Fatal("BatchStatus.Delete(ctx) error = nil, want set removal errors")
	}

	for _, want := range []string{"remove batch batch-1 from batches set", "remove batch batch-1 from dead batches set"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("BatchStatus.Delete(ctx) error = %q, want substring %q", err, want)
		}
	}

	exists, err := redisClient.Exists(ctx, batchKey, batchProgressKey, batchJobsKey, batchFailedKey, batchCallbacksKey).Result()
	if err != nil {
		t.Fatalf("failed to check deleted batch keys: %v", err)
	}
	if exists != 0 {
		t.Errorf("batch data keys still exist = %d, want 0", exists)
	}
}

func TestBatchStatusDelete_UsesUnlinkForBatchData(t *testing.T) {
	namespace := "batch-delete-unlink"
	flushKeysBatch(t, namespace+":*")

	redisClient := newTestRedisClient(t)
	defer func() { _ = redisClient.Close() }()

	ctx := context.Background()
	bid := "batch-1"
	batchKey := namespace + ":batch:" + bid
	batchProgressKey := batchKey + ":progress"
	batchJobsKey := batchKey + ":jobs"
	batchFailedKey := batchKey + ":failed"
	batchCallbacksKey := batchKey + ":callbacks"

	if err := redisClient.Set(ctx, batchKey, "{}", 0).Err(); err != nil {
		t.Fatalf("Set(%q) error = %v, want nil", batchKey, err)
	}
	if err := redisClient.HSet(ctx, batchProgressKey, "pending", 1).Err(); err != nil {
		t.Fatalf("HSet(%q) error = %v, want nil", batchProgressKey, err)
	}
	if err := redisClient.SAdd(ctx, batchJobsKey, "jid-1", "jid-2").Err(); err != nil {
		t.Fatalf("SAdd(%q) error = %v, want nil", batchJobsKey, err)
	}
	if err := redisClient.SAdd(ctx, batchFailedKey, "jid-2").Err(); err != nil {
		t.Fatalf("SAdd(%q) error = %v, want nil", batchFailedKey, err)
	}
	if err := redisClient.SAdd(ctx, batchCallbacksKey, "jid-2:callback:1").Err(); err != nil {
		t.Fatalf("SAdd(%q) error = %v, want nil", batchCallbacksKey, err)
	}

	hook := &batchStatusCommandHook{}
	redisClient.AddHook(hook)

	status := senna.NewBatchStatus(redisClient, namespace, bid)
	if err := status.Delete(ctx); err != nil {
		t.Fatalf("BatchStatus.Delete(ctx) error = %v, want nil", err)
	}

	if got := hook.singleCommands.Load(); got != 0 {
		t.Errorf("single Redis commands after hook install = %d, want 0", got)
	}
	if got := hook.pipelines.Load(); got != 1 {
		t.Errorf("Redis pipelines after hook install = %d, want 1", got)
	}

	wantCommandNames := []string{"srem", "srem", "unlink"}
	if diff := cmp.Diff(wantCommandNames, hook.pipelineCommandNames(), cmpopts.SortSlices(func(a, b string) bool {
		return a < b
	})); diff != "" {
		t.Errorf("Redis pipeline command names mismatch (-want +got):\n%s", diff)
	}

	exists, err := redisClient.Exists(ctx, batchKey, batchProgressKey, batchJobsKey, batchFailedKey, batchCallbacksKey).Result()
	if err != nil {
		t.Fatalf("Exists(batch data keys) error = %v, want nil", err)
	}
	if exists != 0 {
		t.Errorf("Exists(batch data keys) = %d, want 0", exists)
	}
}

func TestBatch_DynamicJobAdding(t *testing.T) {
	flushKeysBatch(t, "batch-dynamic:*")

	c, err := client.New(&client.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: "batch-dynamic",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = c.Close() }()

	w, err := worker.New(&worker.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: "batch-dynamic",
		Settings: senna.WorkerSettings{
			Concurrency:     2,
			Queues:          []senna.QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout: 5 * time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	var jobsProcessed atomic.Int32
	var completeCalled atomic.Bool

	// This job adds more jobs to its batch
	w.Register("parent_job", func(ctx context.Context, job *senna.Job) error {
		jobsProcessed.Add(1)

		// Get the batch handle from context
		bh := worker.BatchFromContext(ctx)
		if bh == nil {
			t.Error("batch handle should be available in context")
			return nil
		}

		// Verify BID matches
		if bh.BID() != job.BatchID {
			t.Errorf("BID mismatch: context=%s, job=%s", bh.BID(), job.BatchID)
		}

		// Add child jobs dynamically
		if err := bh.Add(ctx, "child_job", map[string]any{"parent": job.ID}); err != nil {
			return err
		}
		return nil
	})

	w.Register("child_job", func(ctx context.Context, job *senna.Job) error {
		jobsProcessed.Add(1)
		return nil
	})

	w.Register("on_complete", func(ctx context.Context, job *senna.Job) error {
		completeCalled.Store(true)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = w.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	batch := client.NewBatch().
		Add("parent_job", nil). // Will add 1 child job
		OnCompleteCallback("on_complete")

	if err := c.EnqueueBatch(ctx, batch); err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	// Wait for all jobs (parent + child) and callback
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if jobsProcessed.Load() >= 2 && completeCalled.Load() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	time.Sleep(100 * time.Millisecond)

	if jobsProcessed.Load() < 2 {
		t.Errorf("expected at least 2 jobs processed (parent + child), got %d", jobsProcessed.Load())
	}
	if !completeCalled.Load() {
		t.Error("complete callback should have been called")
	}
}

func TestBatch_DynamicAddRejectsWrongQueueTypeWithoutTrackingJob(t *testing.T) {
	const namespace = "batch-dynamic-add-atomic"
	flushKeysBatch(t, namespace+":*")

	c, err := client.New(&client.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: namespace,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = c.Close() }()

	w, err := worker.New(&worker.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: namespace,
		Settings: senna.WorkerSettings{
			Concurrency:     1,
			Queues:          []senna.QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout: 5 * time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := c.Redis().Set(ctx, namespace+":queue:blocked", "not-a-list", 0).Err(); err != nil {
		t.Fatalf("failed to poison dynamic add queue key: %v", err)
	}

	var parentProcessed atomic.Bool
	var addAttempted atomic.Bool
	var addErr atomic.Value
	var childID atomic.Value

	w.Register("parent_job", func(ctx context.Context, job *senna.Job) error {
		parentProcessed.Store(true)

		bh := worker.BatchFromContext(ctx)
		if bh == nil {
			addErr.Store("missing batch handle")
			return nil
		}

		child := senna.NewJob("child_job", nil)
		child.Queue = "blocked"
		childID.Store(child.ID)
		if err := bh.AddJobs(ctx, []*senna.Job{child}); err != nil {
			addErr.Store(err.Error())
		}
		addAttempted.Store(true)
		return nil
	})

	w.Register("child_job", func(ctx context.Context, job *senna.Job) error {
		t.Error("child job should not run when dynamic add queue has the wrong Redis type")
		return nil
	})

	go func() {
		_ = w.Run(ctx)
	}()
	time.Sleep(100 * time.Millisecond)

	batch := client.NewBatch().Add("parent_job", nil)
	if err := c.EnqueueBatch(ctx, batch); err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if parentProcessed.Load() && addAttempted.Load() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)

	cancel()
	time.Sleep(100 * time.Millisecond)

	if !parentProcessed.Load() {
		t.Fatal("parent job should have processed")
	}
	value := addErr.Load()
	if value == nil {
		t.Fatal("dynamic AddJobs should fail for wrong queue type")
	}
	if !strings.Contains(value.(string), "queue key has type string, want list") {
		t.Fatalf("dynamic AddJobs error = %q, want wrong queue type", value.(string))
	}

	childValue := childID.Load()
	if childValue == nil {
		t.Fatal("child job ID should be captured")
	}
	tracked, err := c.Redis().SIsMember(context.Background(), namespace+":batch:"+batch.ID+":jobs", childValue.(string)).Result()
	if err != nil {
		t.Fatalf("failed to check batch jobs set: %v", err)
	}
	if tracked {
		t.Fatalf("failed dynamic add should not leave child job %s in batch jobs set", childValue.(string))
	}

	status := senna.NewBatchStatus(c.Redis(), namespace, batch.ID)
	if err := status.Refresh(context.Background()); err != nil {
		t.Fatalf("BatchStatus.Refresh(ctx) error = %v, want nil", err)
	}
	if status.Total() != 1 {
		t.Fatalf("BatchStatus.Total() = %d, want 1", status.Total())
	}
	if status.Pending() != 0 {
		t.Fatalf("BatchStatus.Pending() = %d, want 0", status.Pending())
	}
}

func TestBatch_ValidWithinBatch(t *testing.T) {
	// Test ValidWithinBatch when not in a batch
	valid, err := worker.ValidWithinBatch(context.Background())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !valid {
		t.Error("should be valid when not in a batch")
	}

	// BIDFromContext when not in batch should return empty
	bid := worker.BIDFromContext(context.Background())
	if bid != "" {
		t.Errorf("expected empty BID, got '%s'", bid)
	}
}

func TestBatch_CallbackQueue(t *testing.T) {
	flushKeysBatch(t, "batch-cbqueue:*")

	c, err := client.New(&client.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: "batch-cbqueue",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = c.Close() }()

	w, err := worker.New(&worker.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: "batch-cbqueue",
		Settings: senna.WorkerSettings{
			Concurrency:     2,
			Queues:          []senna.QueueConfig{{Name: "default", Priority: 1}, {Name: "critical", Priority: 10}},
			ShutdownTimeout: 5 * time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	var callbackQueue string
	var completeCalled atomic.Bool

	w.Register("batch_job", func(ctx context.Context, job *senna.Job) error {
		return nil
	})

	w.Register("on_complete", func(ctx context.Context, job *senna.Job) error {
		callbackQueue = job.Queue
		completeCalled.Store(true)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = w.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	batch := client.NewBatch().
		Add("batch_job", nil).
		OnCompleteCallback("on_complete").
		WithCallbackQueue("critical")

	if err := c.EnqueueBatch(ctx, batch); err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if completeCalled.Load() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	time.Sleep(100 * time.Millisecond)

	if callbackQueue != "critical" {
		t.Errorf("expected callback to run on 'critical' queue, got '%s'", callbackQueue)
	}
}

func TestBatch_SpecialCharsInJobType(t *testing.T) {
	flushKeysBatch(t, "batch-special:*")

	c, err := client.New(&client.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: "batch-special",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = c.Close() }()

	w, err := worker.New(&worker.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: "batch-special",
		Settings: senna.WorkerSettings{
			Concurrency:     2,
			Queues:          []senna.QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout: 5 * time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	var completeCalled atomic.Bool
	var receivedJobType string

	w.Register("batch_job", func(ctx context.Context, job *senna.Job) error {
		return nil
	})

	// Job type with special JSON characters: quotes, backslashes, newlines
	specialJobType := "callback:with\"quotes\\and\nnewlines"

	w.Register(specialJobType, func(ctx context.Context, job *senna.Job) error {
		receivedJobType = job.Type
		completeCalled.Store(true)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = w.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	batch := client.NewBatch().
		Add("batch_job", nil).
		OnCompleteCallback(specialJobType)

	if err := c.EnqueueBatch(ctx, batch); err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if completeCalled.Load() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	time.Sleep(100 * time.Millisecond)

	if !completeCalled.Load() {
		t.Error("callback with special characters should have been called")
	}
	if receivedJobType != specialJobType {
		t.Errorf("expected job type %q, got %q", specialJobType, receivedJobType)
	}
}

func TestBatch_ClientDefaultQueueForCallbacks(t *testing.T) {
	flushKeysBatch(t, "batch-defqueue:*")

	// Client with custom default queue
	c, err := client.New(&client.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: "batch-defqueue",
		Settings: client.Settings{
			DefaultQueue: "custom",
			DefaultRetry: senna.DefaultRetryCount,
		},
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = c.Close() }()

	// Worker only listens to "custom" queue (not "default")
	w, err := worker.New(&worker.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: "batch-defqueue",
		Settings: senna.WorkerSettings{
			Concurrency:     2,
			Queues:          []senna.QueueConfig{{Name: "custom", Priority: 1}},
			ShutdownTimeout: 5 * time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	var completeCalled atomic.Bool
	var callbackQueue string

	w.Register("batch_job", func(ctx context.Context, job *senna.Job) error {
		return nil
	})

	w.Register("on_complete", func(ctx context.Context, job *senna.Job) error {
		callbackQueue = job.Queue
		completeCalled.Store(true)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = w.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	// Create batch WITHOUT explicit callback queue - should use client's default
	batch := client.NewBatch().
		Add("batch_job", nil, client.WithQueue("custom")). // Job goes to custom queue
		OnCompleteCallback("on_complete")                  // Callback queue should inherit from client

	if err := c.EnqueueBatch(ctx, batch); err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	// Wait for callback to fire
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if completeCalled.Load() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	time.Sleep(100 * time.Millisecond)

	if !completeCalled.Load() {
		t.Error("complete callback should have been called (should use client's default queue)")
	}
	if callbackQueue != "custom" {
		t.Errorf("expected callback to run on 'custom' queue (client default), got '%s'", callbackQueue)
	}
}

func TestBatch_CallbackEnqueueRejectsWrongQueueTypeWithoutStateMutation(t *testing.T) {
	const namespace = "batch-callback-atomic"
	flushKeysBatch(t, namespace+":*")

	defaultLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() {
		slog.SetDefault(defaultLogger)
	})

	c, err := client.New(&client.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: namespace,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = c.Close() }()

	w, err := worker.New(&worker.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: namespace,
		Settings: senna.WorkerSettings{
			Concurrency:     1,
			Queues:          []senna.QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout: 5 * time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := c.Redis().Set(ctx, namespace+":queue:callbacks", "not-a-list", 0).Err(); err != nil {
		t.Fatalf("failed to poison callback queue key: %v", err)
	}

	var jobsProcessed atomic.Int32
	w.Register("batch_job", func(ctx context.Context, job *senna.Job) error {
		jobsProcessed.Add(1)
		return nil
	})
	w.Register("on_complete", func(ctx context.Context, job *senna.Job) error {
		t.Error("callback should not run when callback queue has the wrong Redis type")
		return nil
	})

	go func() {
		_ = w.Run(ctx)
	}()
	time.Sleep(100 * time.Millisecond)

	batch := client.NewBatch().
		Add("batch_job", nil).
		OnCompleteCallback("on_complete").
		WithCallbackQueue("callbacks")

	if err := c.EnqueueBatch(ctx, batch); err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if jobsProcessed.Load() == 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)

	cancel()
	time.Sleep(100 * time.Millisecond)

	if jobsProcessed.Load() != 1 {
		t.Fatalf("batch job should have run once, got %d", jobsProcessed.Load())
	}

	status := senna.NewBatchStatus(c.Redis(), namespace, batch.ID)
	if err := status.Refresh(context.Background()); err != nil {
		t.Fatalf("BatchStatus.Refresh(ctx) error = %v, want nil", err)
	}
	if status.Pending() != 1 {
		t.Errorf("BatchStatus.Pending() = %d, want 1", status.Pending())
	}
	if status.Successes() != 0 {
		t.Errorf("BatchStatus.Successes() = %d, want 0", status.Successes())
	}
	callbacksPending, err := c.Redis().HGet(context.Background(), namespace+":batch:"+batch.ID+":progress", "callbacks_pending").Result()
	if err != nil {
		t.Fatalf("failed to read callbacks_pending progress: %v", err)
	}
	if callbacksPending != "0" {
		t.Errorf("BatchStatus.CallbacksPending = %s, want 0", callbacksPending)
	}

	callbacks, err := c.Redis().SCard(context.Background(), namespace+":batch:"+batch.ID+":callbacks").Result()
	if err != nil {
		t.Fatalf("failed to read callback set size: %v", err)
	}
	if callbacks != 0 {
		t.Errorf("callback set size = %d, want 0", callbacks)
	}
}

func TestBatch_EmptyBatchCallbackEnqueueErrorCleansState(t *testing.T) {
	const namespace = "batch-empty-callback-atomic"
	flushKeysBatch(t, namespace+":*")

	c, err := client.New(&client.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: namespace,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	if err := c.Redis().Set(ctx, namespace+":queue:callbacks", "not-a-list", 0).Err(); err != nil {
		t.Fatalf("failed to poison callback queue key: %v", err)
	}

	batch := client.NewBatch().
		OnCompleteCallback("on_complete").
		WithCallbackQueue("callbacks")

	err = c.EnqueueBatch(ctx, batch)
	if err == nil {
		t.Fatal("expected empty batch enqueue to fail for wrong callback queue type")
	}
	if !strings.Contains(err.Error(), "queue key has type string, want list") {
		t.Fatalf("unexpected empty batch enqueue error: %v", err)
	}

	exists, err := c.Redis().Exists(ctx, namespace+":batch:"+batch.ID).Result()
	if err != nil {
		t.Fatalf("failed to check batch state: %v", err)
	}
	if exists != 0 {
		t.Fatalf("batch state should be cleaned up after callback enqueue failure")
	}

	callbacks, err := c.Redis().Exists(ctx, namespace+":batch:"+batch.ID+":callbacks").Result()
	if err != nil {
		t.Fatalf("failed to check callback set: %v", err)
	}
	if callbacks != 0 {
		t.Fatalf("callback set should be cleaned up after callback enqueue failure")
	}
}

func TestBatch_EmptyChildDoesNotRollbackCompletedParentOnAncestorPropagationError(t *testing.T) {
	const namespace = "batch-empty-ancestor-propagation"
	flushKeysBatch(t, namespace+":*")

	defaultLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() {
		slog.SetDefault(defaultLogger)
	})

	c, err := client.New(&client.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: namespace,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	now := time.Now()
	grandparentID := "grandparent"
	parentID := "parent"

	grandparentState := senna.BatchState{
		ID:            grandparentID,
		Total:         1,
		Pending:       1,
		CreatedAt:     now,
		OnComplete:    &senna.CallbackInfo{JobType: "grandparent_complete"},
		CallbackQueue: "callbacks",
	}
	parentState := senna.BatchState{
		ID:        parentID,
		ParentID:  grandparentID,
		CreatedAt: now,
	}

	grandparentData, err := json.Marshal(grandparentState)
	if err != nil {
		t.Fatalf("failed to marshal grandparent state: %v", err)
	}
	parentData, err := json.Marshal(parentState)
	if err != nil {
		t.Fatalf("failed to marshal parent state: %v", err)
	}

	if err := c.Redis().Set(ctx, namespace+":batch:"+grandparentID, string(grandparentData), time.Hour).Err(); err != nil {
		t.Fatalf("failed to store grandparent state: %v", err)
	}
	if err := c.Redis().SAdd(ctx, namespace+":batch:"+grandparentID+":jobs", parentID).Err(); err != nil {
		t.Fatalf("failed to store grandparent jobs set: %v", err)
	}
	if err := c.Redis().Set(ctx, namespace+":batch:"+parentID, string(parentData), time.Hour).Err(); err != nil {
		t.Fatalf("failed to store parent state: %v", err)
	}
	if err := c.Redis().Set(ctx, namespace+":queue:callbacks", "not-a-list", 0).Err(); err != nil {
		t.Fatalf("failed to poison grandparent callback queue key: %v", err)
	}

	child := client.NewBatch().WithParent(parentID)
	if err := c.EnqueueBatch(ctx, child); err != nil {
		t.Fatalf("EnqueueBatch(empty child) error = %v, want nil", err)
	}

	parentStatus := senna.NewBatchStatus(c.Redis(), namespace, parentID)
	if err := parentStatus.Refresh(ctx); err != nil {
		t.Fatalf("parent BatchStatus.Refresh(ctx) error = %v, want nil", err)
	}
	if parentStatus.Pending() != 0 {
		t.Fatalf("parent Pending = %d, want 0", parentStatus.Pending())
	}
	parentCompleteFired, err := c.Redis().HGet(ctx, namespace+":batch:"+parentID+":progress", "complete_fired").Result()
	if err != nil {
		t.Fatalf("failed to read parent complete_fired progress: %v", err)
	}
	if parentCompleteFired != "1" {
		t.Fatal("parent CompleteFired = false, want true")
	}

	childExists, err := c.Redis().Exists(ctx, namespace+":batch:"+child.ID).Result()
	if err != nil {
		t.Fatalf("failed to check child state: %v", err)
	}
	if childExists != 1 {
		t.Fatalf("child batch state should remain after ancestor propagation failure")
	}

	grandparentStatus := senna.NewBatchStatus(c.Redis(), namespace, grandparentID)
	if err := grandparentStatus.Refresh(ctx); err != nil {
		t.Fatalf("grandparent BatchStatus.Refresh(ctx) error = %v, want nil", err)
	}
	if grandparentStatus.Pending() != 1 {
		t.Fatalf("grandparent Pending = %d, want 1", grandparentStatus.Pending())
	}
	grandparentCallbacksPending, err := c.Redis().HGet(ctx, namespace+":batch:"+grandparentID+":progress", "callbacks_pending").Result()
	if err != nil {
		t.Fatalf("failed to read grandparent callbacks_pending progress: %v", err)
	}
	if grandparentCallbacksPending != "0" {
		t.Fatalf("grandparent CallbacksPending = %s, want 0", grandparentCallbacksPending)
	}
}

func TestBatch_ReopenedBatchUsesUniqueCallbackIDs(t *testing.T) {
	const namespace = "batch-reopen-callback-ids"
	flushKeysBatch(t, namespace+":*")

	c, err := client.New(&client.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: namespace,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = c.Close() }()

	w, err := worker.New(&worker.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: namespace,
		Settings: senna.WorkerSettings{
			Concurrency:     2,
			Queues:          []senna.QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout: 5 * time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var callbackCount atomic.Int32
	var childProcessed atomic.Bool
	var firstCallbackTimedOut atomic.Bool
	var enqueueChildErr atomic.Value

	w.Register("batch_job", func(ctx context.Context, job *senna.Job) error {
		return nil
	})
	w.Register("child_job", func(ctx context.Context, job *senna.Job) error {
		childProcessed.Store(true)
		return nil
	})
	w.Register("on_complete", func(ctx context.Context, job *senna.Job) error {
		count := callbackCount.Add(1)
		if count != 1 {
			return nil
		}

		parentID, ok := job.Args["batch_id"].(string)
		if !ok || parentID == "" {
			enqueueChildErr.Store("missing batch_id in callback args")
			return nil
		}

		child := client.NewBatch().
			WithParent(parentID).
			Add("child_job", nil)
		if err := c.EnqueueBatch(ctx, child); err != nil {
			enqueueChildErr.Store(err.Error())
			return nil
		}

		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if callbackCount.Load() >= 2 {
				return nil
			}
			time.Sleep(25 * time.Millisecond)
		}
		firstCallbackTimedOut.Store(true)
		return nil
	})

	go func() {
		_ = w.Run(ctx)
	}()
	time.Sleep(100 * time.Millisecond)

	batch := client.NewBatch().
		Add("batch_job", nil).
		OnCompleteCallback("on_complete")

	if err := c.EnqueueBatch(ctx, batch); err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	progress := map[string]string{}
	for time.Now().Before(deadline) {
		gotProgress, err := c.Redis().HGetAll(ctx, namespace+":batch:"+batch.ID+":progress").Result()
		if err == nil {
			progress = gotProgress
			if callbackCount.Load() >= 2 && progress["callbacks_pending"] == "0" {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	time.Sleep(100 * time.Millisecond)

	if value := enqueueChildErr.Load(); value != nil {
		t.Fatalf("failed to enqueue child batch from callback: %v", value)
	}
	if firstCallbackTimedOut.Load() {
		t.Fatal("first callback timed out waiting for reopened batch callback")
	}
	if !childProcessed.Load() {
		t.Fatal("child job should have processed after callback reopened the parent batch")
	}
	if callbackCount.Load() != 2 {
		t.Fatalf("callback count = %d, want 2", callbackCount.Load())
	}
	if progress["callbacks_pending"] != "0" {
		t.Fatalf("BatchState.CallbacksPending = %s, want 0", progress["callbacks_pending"])
	}
	if progress["callback_seq"] != "2" {
		t.Fatalf("BatchState.CallbackSequence = %s, want 2", progress["callback_seq"])
	}
}

func TestBatch_EmptyBatchFiresCallbacks(t *testing.T) {
	flushKeysBatch(t, "batch-empty:*")

	c, err := client.New(&client.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: "batch-empty",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = c.Close() }()

	w, err := worker.New(&worker.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: "batch-empty",
		Settings: senna.WorkerSettings{
			Concurrency:     2,
			Queues:          []senna.QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout: 5 * time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	var completeCalled atomic.Bool
	var successCalled atomic.Bool

	w.Register("on_complete", func(ctx context.Context, job *senna.Job) error {
		completeCalled.Store(true)
		return nil
	})

	w.Register("on_success", func(ctx context.Context, job *senna.Job) error {
		successCalled.Store(true)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = w.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	// Create an empty batch with callbacks
	batch := client.NewBatch().
		WithDescription("Empty batch test").
		OnCompleteCallback("on_complete").
		OnSuccessCallback("on_success")

	if err := c.EnqueueBatch(ctx, batch); err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	// Batch should be immediately complete
	status := c.BatchStatus(batch.ID)
	if err := status.Refresh(ctx); err != nil {
		t.Fatalf("failed to refresh status: %v", err)
	}
	if !status.Complete() {
		t.Error("empty batch should be immediately complete")
	}
	if status.Pending() != 0 {
		t.Errorf("expected 0 pending, got %d", status.Pending())
	}

	// Wait for callbacks to be processed
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if completeCalled.Load() && successCalled.Load() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cancel()
	time.Sleep(100 * time.Millisecond)

	if !completeCalled.Load() {
		t.Error("complete callback should have been called for empty batch")
	}
	if !successCalled.Load() {
		t.Error("success callback should have been called for empty batch")
	}
}

func TestBatch_InvalidatedBatchCompletes(t *testing.T) {
	flushKeysBatch(t, "batch-invalidate:*")

	c, err := client.New(&client.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: "batch-invalidate",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer func() { _ = c.Close() }()

	w, err := worker.New(&worker.Config{
		Redis:     getRedisConfigBatch(),
		Namespace: "batch-invalidate",
		Settings: senna.WorkerSettings{
			Concurrency:     2,
			Queues:          []senna.QueueConfig{{Name: "default", Priority: 1}},
			ShutdownTimeout: 5 * time.Second,
			PollInterval:    50 * time.Millisecond,
			HeartbeatRate:   time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed to create worker: %v", err)
	}

	var jobsProcessed atomic.Int32
	var completeCalled atomic.Bool
	var successCalled atomic.Bool
	invalidateOnce := sync.Once{}

	w.Register("batch_job", func(ctx context.Context, job *senna.Job) error {
		// First job invalidates the batch
		invalidateOnce.Do(func() {
			batch := worker.BatchFromContext(ctx)
			if batch != nil {
				_ = batch.Invalidate(ctx)
			}
		})
		jobsProcessed.Add(1)
		return nil
	})

	w.Register("on_complete", func(ctx context.Context, job *senna.Job) error {
		completeCalled.Store(true)
		return nil
	})

	w.Register("on_success", func(ctx context.Context, job *senna.Job) error {
		successCalled.Store(true)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = w.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	batch := client.NewBatch().
		Add("batch_job", map[string]any{"id": 1}).
		Add("batch_job", map[string]any{"id": 2}).
		Add("batch_job", map[string]any{"id": 3}).
		OnCompleteCallback("on_complete").
		OnSuccessCallback("on_success")

	if err := c.EnqueueBatch(ctx, batch); err != nil {
		t.Fatalf("failed to enqueue batch: %v", err)
	}

	// Use Join to wait for batch completion - this should return even though invalidated
	status := c.BatchStatus(batch.ID)
	joinCtx, joinCancel := context.WithTimeout(ctx, 5*time.Second)
	defer joinCancel()

	err = status.Join(joinCtx)
	if err != nil {
		t.Errorf("Join() should return without error for invalidated batch, got: %v", err)
	}

	// Wait a bit for callbacks to fire
	time.Sleep(200 * time.Millisecond)

	cancel()
	time.Sleep(100 * time.Millisecond)

	// All jobs should have processed
	if jobsProcessed.Load() != 3 {
		t.Errorf("expected 3 jobs processed, got %d", jobsProcessed.Load())
	}

	// Complete callback should fire
	if !completeCalled.Load() {
		t.Error("complete callback should have been called for invalidated batch")
	}

	// Success callback should NOT fire (batch was invalidated)
	if successCalled.Load() {
		t.Error("success callback should NOT have been called for invalidated batch")
	}

	// Verify batch status shows complete
	if err := status.Refresh(context.Background()); err != nil {
		t.Fatalf("failed to refresh status: %v", err)
	}
	if !status.Complete() {
		t.Error("batch should be marked as complete")
	}
	if status.Pending() != 0 {
		t.Errorf("expected 0 pending, got %d", status.Pending())
	}
}

func BenchmarkBatchStatusDeleteLargeBatchData(b *testing.B) {
	const namespace = "bench-batch-delete-large"
	const memberCount = 1000

	redisClient := newTestRedisClient(b)
	flushKeysBatch(b, namespace+":*")
	b.Cleanup(func() {
		flushKeysBatch(b, namespace+":*")
	})

	ctx := context.Background()

	b.ReportAllocs()
	for i := range b.N {
		bid := fmt.Sprintf("batch-%d", i)
		batchKey := namespace + ":batch:" + bid
		batchJobsKey := batchKey + ":jobs"
		batchFailedKey := batchKey + ":failed"

		b.StopTimer()
		seedLargeBatchData(b, redisClient, batchKey, batchJobsKey, batchFailedKey, memberCount)
		b.StartTimer()

		status := senna.NewBatchStatus(redisClient, namespace, bid)
		if err := status.Delete(ctx); err != nil {
			b.Fatalf("BatchStatus.Delete(ctx) error = %v, want nil", err)
		}
	}
}

func seedLargeBatchData(b testing.TB, redisClient *redis.Client, batchKey, batchJobsKey, batchFailedKey string, memberCount int) {
	b.Helper()

	ctx := context.Background()
	pipe := redisClient.Pipeline()
	pipe.Set(ctx, batchKey, "{}", 0)

	jobMembers := make([]any, 0, memberCount)
	failedMembers := make([]any, 0, memberCount)
	for i := range memberCount {
		jid := fmt.Sprintf("jid-%d", i)
		jobMembers = append(jobMembers, jid)
		failedMembers = append(failedMembers, jid)
	}

	pipe.SAdd(ctx, batchJobsKey, jobMembers...)
	pipe.SAdd(ctx, batchFailedKey, failedMembers...)

	if _, err := pipe.Exec(ctx); err != nil {
		b.Fatalf("seed large batch data pipeline error = %v, want nil", err)
	}
}
