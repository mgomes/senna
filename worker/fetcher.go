package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/mgomes/senna"
	"github.com/mgomes/senna/internal/keys"
	"github.com/redis/go-redis/v9"
)

const sequentialLockTTL = 30 * time.Second

type fetcher struct {
	client         *redis.Client
	keys           *keys.Keys
	queues         []senna.QueueConfig
	pollInterval   time.Duration
	strictPriority bool
	sequentialSema map[string]chan struct{} // per-queue semaphore for local coordination
	sequentialMu   sync.RWMutex
	sequentialHeld map[string]struct{}
}

func newFetcher(client *redis.Client, k *keys.Keys, queues []senna.QueueConfig, pollInterval time.Duration, strictPriority bool) *fetcher {
	for i := range queues {
		if queues[i].Priority < 1 {
			queues[i].Priority = 1
		}
	}

	// For strict priority, sort queues by priority descending
	if strictPriority {
		sorted := make([]senna.QueueConfig, len(queues))
		copy(sorted, queues)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].Priority > sorted[j].Priority
		})
		queues = sorted
	}

	// Create semaphores for sequential queues to ensure only one goroutine
	// in this process can be processing each sequential queue at a time
	sema := make(map[string]chan struct{})
	for _, q := range queues {
		if q.Sequential {
			sema[q.Name] = make(chan struct{}, 1)
		}
	}

	return &fetcher{
		client:         client,
		keys:           k,
		queues:         queues,
		pollInterval:   pollInterval,
		strictPriority: strictPriority,
		sequentialSema: sema,
		sequentialHeld: make(map[string]struct{}),
	}
}

// RenewSequentialLocks renews all sequential queue locks held by this worker.
// Should be called periodically to prevent lock expiry during long-running jobs.
func (f *fetcher) RenewSequentialLocks(ctx context.Context, workerID string) {
	for _, q := range f.queues {
		if !q.Sequential {
			continue
		}
		if !f.hasSequentialLock(q.Name) {
			continue
		}
		lockKey := f.keys.SequentialLock(q.Name)
		holder, err := f.client.Get(ctx, lockKey).Result()
		if err == nil && holder == workerID {
			f.client.Expire(ctx, lockKey, sequentialLockTTL)
		}
	}
}

// ReleaseSequentialLock releases the lock for a sequential queue if held by this worker.
// Called after Ack/Nack to allow other workers to process the queue.
// Also releases the local semaphore to allow other goroutines in this process to fetch.
func (f *fetcher) ReleaseSequentialLock(ctx context.Context, workerID, queueName string) {
	if !f.isSequentialQueue(queueName) {
		return
	}

	f.clearSequentialLock(queueName)

	// Release Redis lock (only if we hold it) before freeing the local semaphore.
	// This prevents a new local goroutine from acquiring the semaphore and fetching
	// another job while the old lock is still present, which could then be deleted here.
	lockKey := f.keys.SequentialLock(queueName)
	_, _ = releaseSequentialLockScript.Run(ctx, f.client, []string{lockKey}, workerID)

	// Release local semaphore last
	sema := f.sequentialSema[queueName]
	select {
	case <-sema:
		// Released
	default:
		// Wasn't held (shouldn't happen in normal operation)
	}
}

// isSequentialQueue returns true if the named queue is configured as sequential.
func (f *fetcher) isSequentialQueue(name string) bool {
	for _, q := range f.queues {
		if q.Name == name {
			return q.Sequential
		}
	}
	return false
}

// canProcessQueue checks if this worker can process from the given queue.
// For sequential queues, checks if we hold the lock or if it's available.
// Does NOT acquire the lock - that happens atomically during fetch.
func (f *fetcher) canProcessQueue(ctx context.Context, workerID string, q senna.QueueConfig) bool {
	if q.Paused {
		return false
	}
	if !q.Sequential {
		return true
	}

	lockKey := f.keys.SequentialLock(q.Name)

	// Check who holds the lock (if anyone)
	holder, err := f.client.Get(ctx, lockKey).Result()
	if errors.Is(err, redis.Nil) {
		// No one holds the lock - we can try to fetch
		return true
	}
	if err != nil {
		return false
	}

	// We can process if we hold the lock
	return holder == workerID
}

func (f *fetcher) Fetch(ctx context.Context, workerID string) (*senna.Job, error) {
	if f.strictPriority {
		return f.fetchStrict(ctx, workerID)
	}
	return f.fetchWeighted(ctx, workerID)
}

func (f *fetcher) processableQueues(ctx context.Context, workerID string) ([]senna.QueueConfig, int) {
	processable := make([]senna.QueueConfig, 0, len(f.queues))
	var totalWeight int
	for _, q := range f.queues {
		if f.canProcessQueue(ctx, workerID, q) {
			processable = append(processable, q)
			totalWeight += q.Priority
		}
	}
	return processable, totalWeight
}

// fetchWeighted selects a queue using weighted random and tries to fetch from it
func (f *fetcher) fetchWeighted(ctx context.Context, workerID string) (*senna.Job, error) {
	// Build list of queues we can currently process
	processable, totalWeight := f.processableQueues(ctx, workerID)

	if len(processable) == 0 {
		return nil, nil
	}

	// Select queue using weighted random from processable queues
	queueName := selectProcessableQueue(processable, totalWeight)

	if queueName == "" {
		return nil, nil
	}

	return f.fetchFromQueue(ctx, workerID, queueName)
}

// fetchStrict tries each queue in priority order until a job is found
func (f *fetcher) fetchStrict(ctx context.Context, workerID string) (*senna.Job, error) {
	for _, q := range f.queues {
		if !f.canProcessQueue(ctx, workerID, q) {
			continue
		}

		job, err := f.fetchFromQueue(ctx, workerID, q.Name)
		if err != nil {
			return nil, err
		}
		if job != nil {
			return job, nil
		}
		// Queue was empty, try next queue in priority order
	}
	return nil, nil
}

// fetchFromQueue attempts to fetch a job from a specific queue (non-blocking)
func (f *fetcher) fetchFromQueue(ctx context.Context, workerID, queueName string) (*senna.Job, error) {
	// Use atomic fetch for sequential queues (acquires lock only if job claimed)
	if f.isSequentialQueue(queueName) {
		return f.fetchFromSequentialQueue(ctx, workerID, queueName)
	}

	queueKey := f.keys.Queue(queueName)
	inFlightKey := f.keys.InFlight(workerID)

	result, err := f.client.LMove(ctx, queueKey, inFlightKey, "RIGHT", "LEFT").Result()
	if errors.Is(err, redis.Nil) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return nil, nil
		}
	}
	if err != nil {
		return nil, err
	}

	var job senna.Job
	if err := json.Unmarshal([]byte(result), &job); err != nil {
		return nil, err
	}

	job.SetRaw(result)
	return &job, nil
}

func (f *fetcher) hasSequentialLock(queueName string) bool {
	f.sequentialMu.RLock()
	defer f.sequentialMu.RUnlock()
	_, ok := f.sequentialHeld[queueName]
	return ok
}

func (f *fetcher) holdSequentialLock(queueName string) {
	f.sequentialMu.Lock()
	f.sequentialHeld[queueName] = struct{}{}
	f.sequentialMu.Unlock()
}

func (f *fetcher) clearSequentialLock(queueName string) {
	f.sequentialMu.Lock()
	delete(f.sequentialHeld, queueName)
	f.sequentialMu.Unlock()
}

// fetchFromSequentialQueue atomically checks lock and fetches a job.
// Only acquires the lock if a job is actually claimed.
// Uses local semaphore to ensure only one goroutine in this process can
// process from this sequential queue at a time.
func (f *fetcher) fetchFromSequentialQueue(ctx context.Context, workerID, queueName string) (*senna.Job, error) {
	// Acquire local semaphore first (non-blocking)
	// This ensures only one goroutine in this process can process this queue
	sema := f.sequentialSema[queueName]
	select {
	case sema <- struct{}{}:
		// Got the local lock
	default:
		// Another goroutine in this process is already processing this queue
		return nil, nil
	}

	queueKey := f.keys.Queue(queueName)
	inFlightKey := f.keys.InFlight(workerID)
	lockKey := f.keys.SequentialLock(queueName)

	result, err := sequentialFetchScript.Run(
		ctx, f.client,
		[]string{queueKey, inFlightKey, lockKey},
		workerID, int(sequentialLockTTL.Seconds()),
	)
	if errors.Is(err, redis.Nil) {
		// Script returned nil - queue empty or lock held by another worker
		<-sema // Release local semaphore
		return nil, nil
	}
	if err != nil {
		<-sema // Release local semaphore
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}

	if result == nil {
		<-sema // Release local semaphore
		return nil, nil
	}

	jobData, ok := result.(string)
	if !ok {
		<-sema // Release local semaphore
		return nil, nil
	}

	var job senna.Job
	if err := json.Unmarshal([]byte(jobData), &job); err != nil {
		cleanupErr := f.discardClaimedSequentialPayload(ctx, workerID, queueName, jobData)
		<-sema // Release local semaphore
		if cleanupErr != nil {
			return nil, errors.Join(err, cleanupErr)
		}
		return nil, err
	}

	// Successfully fetched job - semaphore will be released in ReleaseSequentialLock
	f.holdSequentialLock(queueName)
	job.SetRaw(jobData)
	return &job, nil
}

func (f *fetcher) discardClaimedSequentialPayload(ctx context.Context, workerID, queueName, payload string) error {
	keys := []string{
		f.keys.InFlight(workerID),
		f.keys.SequentialLock(queueName),
	}
	if _, err := discardSequentialFetchScript.Run(ctx, f.client, keys, payload, workerID); err != nil {
		return fmt.Errorf("discard invalid sequential payload from queue %s: %w", queueName, err)
	}
	return nil
}

// BlockingFetch blocks until a job is available, then atomically moves it to in-flight.
// Uses BLMOVE (Redis 6.2+) for efficient blocking without polling.
func (f *fetcher) BlockingFetch(ctx context.Context, workerID string, timeout time.Duration) (*senna.Job, error) {
	if timeout < time.Second {
		job, err := f.Fetch(ctx, workerID)
		if err != nil || job != nil {
			return job, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(timeout):
			return nil, nil
		}
	}
	if f.strictPriority {
		return f.blockingFetchStrict(ctx, workerID, timeout)
	}
	return f.blockingFetchWeighted(ctx, workerID, timeout)
}

// blockingFetchWeighted uses weighted random selection to honor queue priorities,
// while still checking all queues to avoid unnecessary blocking
func (f *fetcher) blockingFetchWeighted(ctx context.Context, workerID string, timeout time.Duration) (*senna.Job, error) {
	// Build list of queues we can currently process
	// (non-paused, and for sequential queues, we hold the lock)
	processable, totalWeight := f.processableQueues(ctx, workerID)

	if len(processable) == 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(timeout):
			return nil, nil
		}
	}

	// Select primary queue using weighted random from processable queues
	primaryQueue := selectProcessableQueue(processable, totalWeight)

	// Try primary queue first
	if primaryQueue != "" {
		job, err := f.fetchFromQueue(ctx, workerID, primaryQueue)
		if err != nil {
			return nil, err
		}
		if job != nil {
			return job, nil
		}
	}

	// Primary queue empty - check remaining processable queues
	for _, q := range processable {
		if q.Name == primaryQueue {
			continue
		}
		job, err := f.fetchFromQueue(ctx, workerID, q.Name)
		if err != nil {
			return nil, err
		}
		if job != nil {
			return job, nil
		}
	}

	// All queues empty, block on a weighted random processable queue
	blockQueue := selectProcessableQueue(processable, totalWeight)

	if blockQueue == "" {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(timeout):
			return nil, nil
		}
	}
	return f.blockingFetchFromQueue(ctx, workerID, blockQueue, timeout)
}

func selectProcessableQueue(queues []senna.QueueConfig, totalWeight int) string {
	if len(queues) == 0 {
		return ""
	}
	if len(queues) == 1 {
		return queues[0].Name
	}
	if totalWeight <= 0 {
		return queues[rand.Intn(len(queues))].Name
	}
	r := rand.Intn(totalWeight)
	for _, q := range queues {
		r -= q.Priority
		if r < 0 {
			return q.Name
		}
	}
	return ""
}

// blockingFetchStrict tries all queues non-blocking in priority order,
// then blocks on the HIGHEST priority queue so high-priority jobs wake us immediately
func (f *fetcher) blockingFetchStrict(ctx context.Context, workerID string, timeout time.Duration) (*senna.Job, error) {
	// Build list of queues we can currently process
	// (non-paused, and for sequential queues, we hold the lock)
	// Note: f.queues is already sorted by priority descending for strict mode
	processable, _ := f.processableQueues(ctx, workerID)

	if len(processable) == 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(timeout):
			return nil, nil
		}
	}

	// Try ALL processable queues non-blocking in priority order (high to low)
	for _, q := range processable {
		job, err := f.fetchFromQueue(ctx, workerID, q.Name)
		if err != nil {
			return nil, err
		}
		if job != nil {
			return job, nil
		}
	}

	// Block on HIGHEST priority processable queue - ensures high-priority jobs wake us immediately
	// Low-priority jobs will be picked up on the next cycle after timeout
	return f.blockingFetchFromQueue(ctx, workerID, processable[0].Name, timeout)
}

// blockingFetchFromQueue uses BLMOVE to block until a job is available.
// For sequential queues, uses non-blocking atomic fetch to maintain exclusivity.
func (f *fetcher) blockingFetchFromQueue(ctx context.Context, workerID, queueName string, timeout time.Duration) (*senna.Job, error) {
	// Sequential queues can't use BLMOVE - multiple workers blocking would
	// break the exclusivity guarantee. Use non-blocking fetch with the atomic
	// lock script, then sleep for the timeout if no job.
	if f.isSequentialQueue(queueName) {
		job, err := f.fetchFromSequentialQueue(ctx, workerID, queueName)
		if err != nil || job != nil {
			return job, err
		}
		// No job available, wait for timeout before returning
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(timeout):
			return nil, nil
		}
	}

	queueKey := f.keys.Queue(queueName)
	inFlightKey := f.keys.InFlight(workerID)

	result, err := f.client.BLMove(ctx, queueKey, inFlightKey, "RIGHT", "LEFT", timeout).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		// Check if context was cancelled
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}

	var job senna.Job
	if err := json.Unmarshal([]byte(result), &job); err != nil {
		return nil, err
	}

	job.SetRaw(result)
	return &job, nil
}

func (f *fetcher) Ack(ctx context.Context, workerID string, job *senna.Job) error {
	payload, err := jobPayload(job)
	if err != nil {
		return err
	}
	keys := []string{f.keys.InFlight(workerID)}
	if job.UniqueKey != "" {
		keys = append(keys, f.keys.Unique(job.UniqueKey))
	}
	if _, err = ackJobScript.Run(ctx, f.client, keys, payload); err != nil {
		return fmt.Errorf("ack job %s: %w", job.ID, err)
	}
	job.ClearFinalization()
	return nil
}

func (f *fetcher) Nack(ctx context.Context, workerID string, job *senna.Job, retryIn time.Duration) error {
	if retryIn > 0 {
		return f.NackAt(ctx, workerID, job, time.Now().Add(retryIn))
	}

	payload, err := jobPayload(job)
	if err != nil {
		return err
	}

	if _, err = ackJobScript.Run(ctx, f.client, []string{f.keys.InFlight(workerID)}, payload); err != nil {
		return fmt.Errorf("nack job %s: %w", job.ID, err)
	}
	job.ClearFinalization()
	return nil
}

func (f *fetcher) NackAt(ctx context.Context, workerID string, job *senna.Job, retryAt time.Time) error {
	payload, err := jobPayload(job)
	if err != nil {
		return err
	}

	nextJob := *job
	nextJob.RetryCount++
	nextJob.ClearFinalization()
	newData, err := nextJob.Marshal()
	if err != nil {
		return err
	}
	if _, err = retryJobScript.Run(ctx, f.client,
		[]string{f.keys.InFlight(workerID), f.keys.Retry()},
		payload, retryAt.Unix(), string(newData),
	); err != nil {
		return fmt.Errorf("nack job %s for retry: %w", job.ID, err)
	}
	job.RetryCount = nextJob.RetryCount
	job.ClearFinalization()
	return nil
}

func (f *fetcher) MoveToDead(ctx context.Context, workerID string, job *senna.Job) error {
	payload, err := jobPayload(job)
	if err != nil {
		return err
	}

	now := time.Now()
	deadJob := *job
	deadJob.FailedAt = &now
	deadJob.ClearFinalization()
	newData, err := deadJob.Marshal()
	if err != nil {
		return err
	}

	keys := []string{f.keys.InFlight(workerID), f.keys.Dead()}
	if job.UniqueKey != "" {
		keys = append(keys, f.keys.Unique(job.UniqueKey))
	}
	if _, err = moveToDeadJobScript.Run(ctx, f.client, keys, payload, now.Unix(), string(newData)); err != nil {
		return fmt.Errorf("move job %s to dead queue: %w", job.ID, err)
	}
	job.FailedAt = &now
	job.ClearFinalization()
	return nil
}

func (f *fetcher) MarkFinalization(ctx context.Context, workerID string, job *senna.Job, finalization senna.JobFinalization) error {
	if job.Finalization() != nil {
		return nil
	}

	payload, err := jobPayload(job)
	if err != nil {
		return err
	}

	finalizedData, err := payloadWithFinalization(payload, finalization)
	if err != nil {
		return err
	}

	result, err := markJobFinalizationScript.Run(
		ctx, f.client,
		[]string{f.keys.InFlight(workerID), f.keys.Queue(job.Queue)},
		payload, string(finalizedData),
	)
	if err != nil {
		return fmt.Errorf("mark job %s finalization: %w", job.ID, err)
	}
	marked, ok := result.(int64)
	if !ok {
		return fmt.Errorf("mark job %s finalization: unexpected script result %T", job.ID, result)
	}
	if marked == 0 {
		return nil
	}
	if marked != 1 && marked != 2 {
		return fmt.Errorf("mark job %s finalization: unexpected script status %d", job.ID, marked)
	}

	job.SetFinalization(finalization)
	job.SetRaw(string(finalizedData))
	return nil
}

func (f *fetcher) requeue(ctx context.Context, workerID string, job *senna.Job) error {
	payload, err := jobPayload(job)
	if err != nil {
		return err
	}
	data, err := job.Marshal()
	if err != nil {
		return err
	}
	if _, err = requeueJobScript.Run(ctx, f.client,
		[]string{f.keys.InFlight(workerID), f.keys.Queue(job.Queue)},
		payload, string(data),
	); err != nil {
		return fmt.Errorf("requeue job %s to queue %s: %w", job.ID, job.Queue, err)
	}
	return nil
}

func jobPayload(job *senna.Job) (string, error) {
	payload := job.Raw()
	if payload != "" {
		return payload, nil
	}

	data, err := job.Marshal()
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func payloadWithFinalization(payload string, finalization senna.JobFinalization) ([]byte, error) {
	job, err := senna.UnmarshalJob([]byte(payload))
	if err != nil {
		return nil, err
	}
	job.SetFinalization(finalization)
	return job.Marshal()
}
