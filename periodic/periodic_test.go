package periodic

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mgomes/senna/internal/keys"
)

func waitForMinuteWindow(guard time.Duration) {
	if guard <= 0 {
		return
	}

	for {
		nextMinute := time.Now().Truncate(time.Minute).Add(time.Minute)
		if remaining := time.Until(nextMinute); remaining > guard {
			return
		} else {
			time.Sleep(remaining + 10*time.Millisecond)
		}
	}
}

func TestScheduler_Register_ValidCron(t *testing.T) {
	client := newRedisClient(t)
	ns := "test-periodic-" + uuid.NewString()[:8]
	k := keys.New(ns)
	t.Cleanup(func() { cleanupKeys(t, client, ns+":*") })

	s := NewScheduler(client, k)

	err := s.Register("* * * * *", "test_job")
	if err != nil {
		t.Fatalf("register with valid cron failed: %v", err)
	}

	jobs := s.Jobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}

	if jobs[0].JobType != "test_job" {
		t.Errorf("expected job type 'test_job', got '%s'", jobs[0].JobType)
	}
	if jobs[0].Queue != "default" {
		t.Errorf("expected queue 'default', got '%s'", jobs[0].Queue)
	}
	if jobs[0].Retry != 25 {
		t.Errorf("expected retry 25, got %d", jobs[0].Retry)
	}
}

func TestScheduler_Register_InvalidCron(t *testing.T) {
	client := newRedisClient(t)
	ns := "test-periodic-" + uuid.NewString()[:8]
	k := keys.New(ns)
	t.Cleanup(func() { cleanupKeys(t, client, ns+":*") })

	s := NewScheduler(client, k)

	err := s.Register("invalid cron", "test_job")
	if err == nil {
		t.Fatal("expected error for invalid cron expression")
	}
}

func TestScheduler_Register_WithOptions(t *testing.T) {
	client := newRedisClient(t)
	ns := "test-periodic-" + uuid.NewString()[:8]
	k := keys.New(ns)
	t.Cleanup(func() { cleanupKeys(t, client, ns+":*") })

	s := NewScheduler(client, k)

	err := s.Register(
		"0 * * * *",
		"hourly_report",
		WithQueue("reports"),
		WithRetry(3),
		WithArgs(map[string]any{"region": "us-east"}),
	)
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	jobs := s.Jobs()
	if len(jobs) != 1 {
		t.Fatalf("expected 1 job, got %d", len(jobs))
	}

	job := jobs[0]
	if job.Queue != "reports" {
		t.Errorf("expected queue 'reports', got '%s'", job.Queue)
	}
	if job.Retry != 3 {
		t.Errorf("expected retry 3, got %d", job.Retry)
	}
	if job.Args["region"] != "us-east" {
		t.Errorf("expected args['region']='us-east', got '%v'", job.Args["region"])
	}
}

func TestScheduler_ClaimSlot_AtomicDeduplication(t *testing.T) {
	client := newRedisClient(t)
	ns := "test-periodic-" + uuid.NewString()[:8]
	k := keys.New(ns)
	t.Cleanup(func() { cleanupKeys(t, client, ns+":*") })

	s1 := NewScheduler(client, k)
	s2 := NewScheduler(client, k)

	_ = s1.Register("* * * * *", "test_job")
	_ = s2.Register("* * * * *", "test_job")

	job := s1.Jobs()[0]
	now := time.Now()

	// First scheduler should claim the slot
	claimed1 := s1.claimSlot(context.Background(), job, now)
	if !claimed1 {
		t.Error("first scheduler should have claimed the slot")
	}

	// Second scheduler should NOT claim the slot (already taken)
	claimed2 := s2.claimSlot(context.Background(), job, now)
	if claimed2 {
		t.Error("second scheduler should NOT have claimed the slot")
	}
}

func TestScheduler_ClaimSlot_DifferentMinutes(t *testing.T) {
	client := newRedisClient(t)
	ns := "test-periodic-" + uuid.NewString()[:8]
	k := keys.New(ns)
	t.Cleanup(func() { cleanupKeys(t, client, ns+":*") })

	s := NewScheduler(client, k)
	_ = s.Register("* * * * *", "test_job")
	job := s.Jobs()[0]

	now := time.Now()
	nextMinute := now.Add(time.Minute)

	// Claim for current minute
	claimed1 := s.claimSlot(context.Background(), job, now)
	if !claimed1 {
		t.Error("should claim current minute")
	}

	// Should be able to claim for next minute (different slot)
	claimed2 := s.claimSlot(context.Background(), job, nextMinute)
	if !claimed2 {
		t.Error("should claim next minute (different slot)")
	}
}

func TestScheduler_ShouldEnqueue(t *testing.T) {
	client := newRedisClient(t)
	ns := "test-periodic-" + uuid.NewString()[:8]
	k := keys.New(ns)
	t.Cleanup(func() { cleanupKeys(t, client, ns+":*") })

	s := NewScheduler(client, k)

	// Job that runs at minute 30 of every hour
	_ = s.Register("30 * * * *", "test_job")
	job := s.Jobs()[0]

	// At 12:30:00, should enqueue
	t1 := time.Date(2024, 1, 15, 12, 30, 0, 0, time.UTC)
	if !s.shouldEnqueue(t1, job) {
		t.Error("should enqueue at 12:30:00")
	}

	// At 12:30:45, still minute 30, should enqueue
	t2 := time.Date(2024, 1, 15, 12, 30, 45, 0, time.UTC)
	if !s.shouldEnqueue(t2, job) {
		t.Error("should enqueue at 12:30:45")
	}

	// At 12:31:00, should NOT enqueue (wrong minute)
	t3 := time.Date(2024, 1, 15, 12, 31, 0, 0, time.UTC)
	if s.shouldEnqueue(t3, job) {
		t.Error("should NOT enqueue at 12:31:00")
	}
}

func TestScheduler_EnqueueJob(t *testing.T) {
	client := newRedisClient(t)
	ns := "test-periodic-" + uuid.NewString()[:8]
	k := keys.New(ns)
	t.Cleanup(func() { cleanupKeys(t, client, ns+":*") })

	s := NewScheduler(client, k)
	_ = s.Register("* * * * *", "test_enqueue_job", WithQueue("critical"))
	job := s.Jobs()[0]

	ctx := context.Background()
	if err := s.enqueueJob(ctx, job); err != nil {
		t.Fatalf("enqueueJob failed: %v", err)
	}

	// Check job was enqueued
	queueKey := k.Queue("critical")
	length, err := client.LLen(ctx, queueKey).Result()
	if err != nil {
		t.Fatalf("failed to check queue length: %v", err)
	}
	if length != 1 {
		t.Errorf("expected 1 job in queue, got %d", length)
	}

	// Verify queue was added to queues set
	isMember, err := client.SIsMember(ctx, k.Queues(), "critical").Result()
	if err != nil {
		t.Fatalf("failed to check queues set: %v", err)
	}
	if !isMember {
		t.Error("queue should be added to queues set")
	}
}

func TestScheduler_History(t *testing.T) {
	client := newRedisClient(t)
	ns := "test-periodic-" + uuid.NewString()[:8]
	k := keys.New(ns)
	t.Cleanup(func() { cleanupKeys(t, client, ns+":*") })

	s := NewScheduler(client, k)
	_ = s.Register("* * * * *", "test_history_job")
	job := s.Jobs()[0]

	ctx := context.Background()

	// Enqueue job (which records history)
	if err := s.enqueueJob(ctx, job); err != nil {
		t.Fatalf("first enqueueJob failed: %v", err)
	}
	if err := s.enqueueJob(ctx, job); err != nil {
		t.Fatalf("second enqueueJob failed: %v", err)
	}

	history, err := s.History(ctx, "test_history_job")
	if err != nil {
		t.Fatalf("failed to get history: %v", err)
	}

	if len(history) != 2 {
		t.Errorf("expected 2 history entries, got %d", len(history))
	}
}

func TestScheduler_StartStop(t *testing.T) {
	client := newRedisClient(t)
	ns := "test-periodic-" + uuid.NewString()[:8]
	k := keys.New(ns)
	t.Cleanup(func() { cleanupKeys(t, client, ns+":*") })

	s := NewScheduler(client, k)
	s.interval = 50 * time.Millisecond

	ctx := context.Background()
	s.Start(ctx)

	// Give it time to run
	time.Sleep(100 * time.Millisecond)

	// Stop should complete without hanging
	done := make(chan struct{})
	go func() {
		s.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(time.Second):
		t.Fatal("Stop() hung")
	}
}

func TestScheduler_StopBeforeStartReturns(t *testing.T) {
	client := newRedisClient(t)
	ns := "test-periodic-" + uuid.NewString()[:8]
	k := keys.New(ns)
	t.Cleanup(func() { cleanupKeys(t, client, ns+":*") })

	s := NewScheduler(client, k)
	done := make(chan struct{})
	go func() {
		s.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop() before Start() hung")
	}
}

func TestScheduler_StopIsIdempotent(t *testing.T) {
	client := newRedisClient(t)
	ns := "test-periodic-" + uuid.NewString()[:8]
	k := keys.New(ns)
	t.Cleanup(func() { cleanupKeys(t, client, ns+":*") })

	s := NewScheduler(client, k)
	s.interval = 50 * time.Millisecond
	s.Start(context.Background())
	time.Sleep(100 * time.Millisecond)

	s.Stop()
	s.Stop()
}

func TestScheduler_StartsAfterStop(t *testing.T) {
	client := newRedisClient(t)
	ns := "test-periodic-" + uuid.NewString()[:8]
	k := keys.New(ns)
	t.Cleanup(func() { cleanupKeys(t, client, ns+":*") })

	s := NewScheduler(client, k)
	s.interval = 50 * time.Millisecond

	s.Start(context.Background())
	time.Sleep(100 * time.Millisecond)
	s.Stop()

	s.Start(context.Background())
	time.Sleep(100 * time.Millisecond)
	s.Stop()
}

func TestScheduler_MultipleWorkers_OnlyOneEnqueues(t *testing.T) {
	client := newRedisClient(t)
	ns := "test-periodic-" + uuid.NewString()[:8]
	k := keys.New(ns)
	t.Cleanup(func() { cleanupKeys(t, client, ns+":*") })

	waitForMinuteWindow(2 * time.Second)

	// Create 3 schedulers simulating 3 workers
	schedulers := make([]*Scheduler, 3)
	for i := range schedulers {
		schedulers[i] = NewScheduler(client, k)
		schedulers[i].interval = 100 * time.Millisecond
		_ = schedulers[i].Register("* * * * *", "multi_worker_job")
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Start all schedulers
	for _, s := range schedulers {
		s.Start(ctx)
	}

	// Let them run for a bit - since the job runs every minute,
	// we just check that only one can claim the slot
	time.Sleep(300 * time.Millisecond)

	cancel()

	// Stop all schedulers
	for _, s := range schedulers {
		s.Stop()
	}

	// Check that exactly 1 job was enqueued (or 0 if we're between minutes)
	queueKey := k.Queue("default")
	length, err := client.LLen(context.Background(), queueKey).Result()
	if err != nil {
		t.Fatalf("failed to check queue length: %v", err)
	}

	if length > 1 {
		t.Errorf("expected at most 1 job in queue, got %d (deduplication failed)", length)
	}
}

func TestScheduler_ConcurrentClaim(t *testing.T) {
	client := newRedisClient(t)
	ns := "test-periodic-" + uuid.NewString()[:8]
	k := keys.New(ns)
	t.Cleanup(func() { cleanupKeys(t, client, ns+":*") })

	// Simulate 10 workers racing to claim the same slot
	numWorkers := 10
	var claimed atomic.Int32

	job := &Job{
		Name:    "race_job",
		JobType: "race_job",
		Queue:   "default",
		Retry:   25,
	}

	now := time.Now()
	done := make(chan struct{})

	for range numWorkers {
		go func() {
			defer func() { done <- struct{}{} }()
			s := NewScheduler(client, k)
			if s.claimSlot(context.Background(), job, now) {
				claimed.Add(1)
			}
		}()
	}

	// Wait for all goroutines
	for range numWorkers {
		<-done
	}

	if claimed.Load() != 1 {
		t.Errorf("expected exactly 1 claim, got %d", claimed.Load())
	}
}

func TestScheduler_ClaimAndEnqueue_DoesNotClaimSlotWhenQueueRejects(t *testing.T) {
	client := newRedisClient(t)
	ns := "test-periodic-" + uuid.NewString()[:8]
	k := keys.New(ns)
	t.Cleanup(func() { cleanupKeys(t, client, ns+":*") })

	s := NewScheduler(client, k)
	if err := s.Register("* * * * *", "retryable_periodic_job", WithQueue("blocked")); err != nil {
		t.Fatalf("register failed: %v", err)
	}
	job := s.Jobs()[0]
	ctx := context.Background()
	now := time.Date(2026, 6, 4, 12, 30, 0, 0, time.UTC)

	queueKey := k.Queue("blocked")
	if err := client.Set(ctx, queueKey, "not a list", 0).Err(); err != nil {
		t.Fatalf("failed to poison queue key: %v", err)
	}

	enqueued, err := s.claimAndEnqueue(ctx, job, now)
	if err == nil {
		t.Fatal("expected claimAndEnqueue to fail")
	}
	if enqueued {
		t.Error("claimAndEnqueue enqueued job = true, want false")
	}

	lockKey := s.periodicSlotKey(job, now)
	lockExists, err := client.Exists(ctx, lockKey).Result()
	if err != nil {
		t.Fatalf("failed to check periodic lock: %v", err)
	}
	if lockExists != 0 {
		t.Errorf("periodic lock exists = %d, want 0", lockExists)
	}

	if err := client.Del(ctx, queueKey).Err(); err != nil {
		t.Fatalf("failed to repair queue key: %v", err)
	}

	enqueued, err = s.claimAndEnqueue(ctx, job, now)
	if err != nil {
		t.Fatalf("claimAndEnqueue after queue repair failed: %v", err)
	}
	if !enqueued {
		t.Error("claimAndEnqueue after queue repair enqueued job = false, want true")
	}

	queueLength, err := client.LLen(ctx, queueKey).Result()
	if err != nil {
		t.Fatalf("failed to check queue length: %v", err)
	}
	if queueLength != 1 {
		t.Errorf("queue length = %d, want 1", queueLength)
	}
}

func TestScheduler_ContextCancellation(t *testing.T) {
	client := newRedisClient(t)
	ns := "test-periodic-" + uuid.NewString()[:8]
	k := keys.New(ns)
	t.Cleanup(func() { cleanupKeys(t, client, ns+":*") })

	s := NewScheduler(client, k)
	s.interval = 50 * time.Millisecond
	_ = s.Register("* * * * *", "context_cancel_job")

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)

	// Cancel context immediately
	cancel()

	// Scheduler should stop quickly
	done := make(chan struct{})
	go func() {
		s.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(time.Second):
		t.Fatal("scheduler didn't stop on context cancellation")
	}
}
