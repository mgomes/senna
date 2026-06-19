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

type fetcher struct {
	client            *redis.Client
	keys              *keys.Keys
	queues            []queueInfo
	activeQueues      []queueInfo
	blockableQueues   []queueInfo
	queueByName       map[string]queueInfo
	totalActiveWeight int
	pollInterval      time.Duration
	strictPriority    bool
	sequentialLockTTL time.Duration
	sequentialSema    map[string]chan struct{} // per-queue semaphore for local coordination
	sequentialMu      sync.RWMutex
	sequentialHeld    map[string]struct{}
}

type queueInfo struct {
	name              string
	priority          int
	paused            bool
	sequential        bool
	queueKey          string
	sequentialLockKey string
}

type queueFetchStatus int

const (
	queueFetchEmpty queueFetchStatus = iota
	queueFetchLocked
	queueFetchJob
)

func newFetcher(client *redis.Client, k *keys.Keys, queues []senna.QueueConfig, pollInterval time.Duration, strictPriority bool) *fetcher {
	return newFetcherWithSequentialLockTTL(
		client,
		k,
		queues,
		pollInterval,
		strictPriority,
		senna.DefaultWorkerSettings().SequentialLockTTL,
	)
}

func newFetcherWithSequentialLockTTL(
	client *redis.Client,
	k *keys.Keys,
	queues []senna.QueueConfig,
	pollInterval time.Duration,
	strictPriority bool,
	sequentialLockTTL time.Duration,
) *fetcher {
	if sequentialLockTTL <= 0 {
		sequentialLockTTL = senna.DefaultWorkerSettings().SequentialLockTTL
	}

	queueInfos := make([]queueInfo, len(queues))
	for i, q := range queues {
		priority := q.Priority
		if priority < 1 {
			priority = 1
		}
		queueInfos[i] = queueInfo{
			name:              q.Name,
			priority:          priority,
			paused:            q.Paused,
			sequential:        q.Sequential,
			queueKey:          k.Queue(q.Name),
			sequentialLockKey: k.SequentialLock(q.Name),
		}
	}

	// For strict priority, sort queues by priority descending
	if strictPriority {
		sorted := make([]queueInfo, len(queueInfos))
		copy(sorted, queueInfos)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].priority > sorted[j].priority
		})
		queueInfos = sorted
	}

	// Create semaphores for sequential queues to ensure only one goroutine
	// in this process can be processing each sequential queue at a time
	sema := make(map[string]chan struct{})
	queueByName := make(map[string]queueInfo, len(queueInfos))
	activeQueues := make([]queueInfo, 0, len(queueInfos))
	blockableQueues := make([]queueInfo, 0, len(queueInfos))
	var totalActiveWeight int
	for _, q := range queueInfos {
		queueByName[q.name] = q
		if q.sequential {
			sema[q.name] = make(chan struct{}, 1)
		}
		if q.paused {
			continue
		}
		activeQueues = append(activeQueues, q)
		totalActiveWeight += q.priority
		if !q.sequential {
			blockableQueues = append(blockableQueues, q)
		}
	}

	return &fetcher{
		client:            client,
		keys:              k,
		queues:            queueInfos,
		activeQueues:      activeQueues,
		blockableQueues:   blockableQueues,
		queueByName:       queueByName,
		totalActiveWeight: totalActiveWeight,
		pollInterval:      pollInterval,
		strictPriority:    strictPriority,
		sequentialLockTTL: sequentialLockTTL,
		sequentialSema:    sema,
		sequentialHeld:    make(map[string]struct{}),
	}
}

// RenewSequentialLocks renews all sequential queue locks held by this worker.
// Should be called periodically to prevent lock expiry during long-running jobs.
func (f *fetcher) RenewSequentialLocks(ctx context.Context, workerID string) {
	for _, q := range f.queues {
		if !q.sequential {
			continue
		}
		if !f.hasSequentialLock(q.name) {
			continue
		}
		holder, err := f.client.Get(ctx, q.sequentialLockKey).Result()
		if err == nil && holder == workerID {
			f.client.PExpire(ctx, q.sequentialLockKey, f.sequentialLockTTL)
		}
	}
}

// ReleaseSequentialLock releases the lock for a sequential queue if held by this worker.
// Called after Ack/Nack to allow other workers to process the queue.
// Also releases the local semaphore to allow other goroutines in this process to fetch.
func (f *fetcher) ReleaseSequentialLock(ctx context.Context, workerID, queueName string) {
	queue, ok := f.queueByName[queueName]
	if !ok || !queue.sequential {
		return
	}

	f.clearSequentialLock(queueName)

	// Release Redis lock (only if we hold it) before freeing the local semaphore.
	// This prevents a new local goroutine from acquiring the semaphore and fetching
	// another job while the old lock is still present, which could then be deleted here.
	_, _ = releaseSequentialLockScript.Run(ctx, f.client, []string{queue.sequentialLockKey}, workerID)

	// Release local semaphore last
	sema := f.sequentialSema[queueName]
	select {
	case <-sema:
		// Released
	default:
		// Wasn't held (shouldn't happen in normal operation)
	}
}

func (f *fetcher) Fetch(ctx context.Context, workerID string) (*senna.Job, error) {
	if f.strictPriority {
		return f.fetchStrict(ctx, workerID)
	}
	return f.fetchWeighted(ctx, workerID)
}

// fetchWeighted selects a queue using weighted random and tries to fetch from it
func (f *fetcher) fetchWeighted(ctx context.Context, workerID string) (*senna.Job, error) {
	var skippedStorage [8]string
	skipped := skippedStorage[:0]
	for {
		queue, ok := f.selectWeightedQueueSkipping(skipped)
		if !ok {
			return nil, nil
		}

		job, status, err := f.fetchFromQueueInfo(ctx, workerID, queue)
		if err != nil || job != nil {
			return job, err
		}
		if !queue.sequential || status != queueFetchLocked {
			return nil, nil
		}
		skipped = appendSkippedQueueName(skipped, queue.name)
	}
}

// fetchStrict tries each queue in priority order until a job is found
func (f *fetcher) fetchStrict(ctx context.Context, workerID string) (*senna.Job, error) {
	for _, q := range f.activeQueues {
		job, _, err := f.fetchFromQueueInfo(ctx, workerID, q)
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

func (f *fetcher) fetchFromQueueInfo(ctx context.Context, workerID string, queue queueInfo) (*senna.Job, queueFetchStatus, error) {
	// Use atomic fetch for sequential queues (acquires lock only if job claimed)
	if queue.sequential {
		return f.fetchFromSequentialQueue(ctx, workerID, queue)
	}

	inFlightKey := f.keys.InFlight(workerID)

	result, err := f.client.LMove(ctx, queue.queueKey, inFlightKey, "RIGHT", "LEFT").Result()
	if errors.Is(err, redis.Nil) {
		select {
		case <-ctx.Done():
			return nil, queueFetchEmpty, ctx.Err()
		default:
			return nil, queueFetchEmpty, nil
		}
	}
	if err != nil {
		return nil, queueFetchEmpty, err
	}

	var job senna.Job
	if err := json.Unmarshal([]byte(result), &job); err != nil {
		return nil, queueFetchEmpty, err
	}

	job.SetRaw(result)
	return &job, queueFetchJob, nil
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
func (f *fetcher) fetchFromSequentialQueue(ctx context.Context, workerID string, queue queueInfo) (*senna.Job, queueFetchStatus, error) {
	// Acquire local semaphore first (non-blocking)
	// This ensures only one goroutine in this process can process this queue
	sema := f.sequentialSema[queue.name]
	select {
	case sema <- struct{}{}:
		// Got the local lock
	default:
		// Another goroutine in this process is already processing this queue
		return nil, queueFetchLocked, nil
	}

	inFlightKey := f.keys.InFlight(workerID)

	result, err := sequentialFetchScript.Run(
		ctx, f.client,
		[]string{queue.queueKey, inFlightKey, queue.sequentialLockKey},
		workerID, durationMilliseconds(f.sequentialLockTTL),
	)
	if errors.Is(err, redis.Nil) {
		// Script returned nil - queue empty or lock held by another worker
		<-sema // Release local semaphore
		return nil, queueFetchEmpty, nil
	}
	if err != nil {
		<-sema // Release local semaphore
		if ctx.Err() != nil {
			return nil, queueFetchEmpty, ctx.Err()
		}
		return nil, queueFetchEmpty, err
	}

	if result == nil {
		<-sema // Release local semaphore
		return nil, queueFetchEmpty, nil
	}

	status, jobData := parseSequentialFetchResult(result)
	switch status {
	case queueFetchLocked, queueFetchEmpty:
		<-sema // Release local semaphore
		return nil, status, nil
	case queueFetchJob:
	default:
		<-sema // Release local semaphore
		return nil, queueFetchEmpty, nil
	}

	var job senna.Job
	if err := json.Unmarshal([]byte(jobData), &job); err != nil {
		cleanupErr := f.discardClaimedSequentialPayload(ctx, workerID, queue.name, jobData)
		<-sema // Release local semaphore
		if cleanupErr != nil {
			return nil, queueFetchEmpty, errors.Join(err, cleanupErr)
		}
		return nil, queueFetchEmpty, err
	}

	// Successfully fetched job - semaphore will be released in ReleaseSequentialLock
	f.holdSequentialLock(queue.name)
	job.SetRaw(jobData)
	return &job, queueFetchJob, nil
}

func parseSequentialFetchResult(result any) (queueFetchStatus, string) {
	values, ok := result.([]any)
	if !ok || len(values) < 1 {
		return queueFetchEmpty, ""
	}
	status, ok := values[0].(string)
	if !ok {
		return queueFetchEmpty, ""
	}
	switch status {
	case "locked":
		return queueFetchLocked, ""
	case "empty":
		return queueFetchEmpty, ""
	case "job":
		if len(values) < 2 {
			return queueFetchEmpty, ""
		}
		jobData, ok := values[1].(string)
		if !ok {
			return queueFetchEmpty, ""
		}
		return queueFetchJob, jobData
	default:
		return queueFetchEmpty, ""
	}
}

func (f *fetcher) discardClaimedSequentialPayload(ctx context.Context, workerID, queueName, payload string) error {
	keys := []string{
		f.keys.InFlight(workerID),
		f.keys.SequentialLock(queueName),
	}
	cleanupCtx := context.WithoutCancel(ctx)
	if _, err := discardSequentialFetchScript.Run(cleanupCtx, f.client, keys, payload, workerID); err != nil {
		return fmt.Errorf("discard invalid sequential payload from queue %s: %w", queueName, err)
	}
	return nil
}

func durationMilliseconds(d time.Duration) int64 {
	ms := d.Milliseconds()
	if ms <= 0 {
		return 1
	}
	return ms
}

// BlockingFetch blocks until a job is available, then atomically moves it to in-flight.
// Uses BLMOVE (Redis 6.2+) when blockTimeout is at least one second.
func (f *fetcher) BlockingFetch(ctx context.Context, workerID string, blockTimeout time.Duration) (*senna.Job, error) {
	if blockTimeout < time.Second {
		job, err := f.Fetch(ctx, workerID)
		if err != nil || job != nil {
			return job, err
		}
		if err := f.waitPollInterval(ctx, blockTimeout); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if f.strictPriority {
		return f.blockingFetchStrict(ctx, workerID, blockTimeout)
	}
	return f.blockingFetchWeighted(ctx, workerID, blockTimeout)
}

// blockingFetchWeighted uses weighted random selection to honor queue priorities,
// while still checking all queues to avoid unnecessary blocking
func (f *fetcher) blockingFetchWeighted(ctx context.Context, workerID string, blockTimeout time.Duration) (*senna.Job, error) {
	if len(f.activeQueues) == 0 {
		if err := f.waitPollInterval(ctx, blockTimeout); err != nil {
			return nil, err
		}
		return nil, nil
	}

	// Select primary queue using weighted random from processable queues
	var skippedStorage [8]string
	skipped := skippedStorage[:0]
	var selectedQueue queueInfo
	var selectedQueueOK bool
	sawSequentialEmpty := false
	for {
		queue, ok := f.selectWeightedQueueSkipping(skipped)
		if !ok {
			break
		}

		job, status, err := f.fetchFromQueueInfo(ctx, workerID, queue)
		if err != nil {
			return nil, err
		}
		if job != nil {
			return job, nil
		}
		if queue.sequential && status == queueFetchLocked {
			skipped = appendSkippedQueueName(skipped, queue.name)
			continue
		}
		selectedQueue = queue
		selectedQueueOK = true
		sawSequentialEmpty = queue.sequential && status == queueFetchEmpty
		break
	}

	// Primary queue empty - check remaining processable queues
	for _, q := range f.activeQueues {
		if selectedQueueOK && q.name == selectedQueue.name {
			continue
		}
		if queueNameSkipped(q.name, skipped) {
			continue
		}
		job, status, err := f.fetchFromQueueInfo(ctx, workerID, q)
		if err != nil {
			return nil, err
		}
		if job != nil {
			return job, nil
		}
		if q.sequential && status == queueFetchEmpty {
			sawSequentialEmpty = true
		}
		if q.sequential && status == queueFetchLocked {
			skipped = appendSkippedQueueName(skipped, q.name)
		}
	}

	// BLMOVE can only watch one queue. Keep multi-queue rotation bounded by the
	// poll interval so jobs arriving on another queue are not delayed by the full
	// block timeout.
	if len(f.blockableQueues) != 1 || sawSequentialEmpty {
		if err := f.waitPollInterval(ctx, blockTimeout); err != nil {
			return nil, err
		}
		return nil, nil
	}

	return f.blockingFetchFromQueueInfo(ctx, workerID, f.blockableQueues[0], blockTimeout)
}

func (f *fetcher) selectWeightedQueue() (queueInfo, bool) {
	return selectWeightedQueue(f.activeQueues, f.totalActiveWeight)
}

func (f *fetcher) selectWeightedQueueSkipping(skipped []string) (queueInfo, bool) {
	if len(skipped) == 0 {
		return f.selectWeightedQueue()
	}
	return selectWeightedQueueSkipping(f.activeQueues, skipped)
}

func selectWeightedQueue(queues []queueInfo, totalWeight int) (queueInfo, bool) {
	if len(queues) == 0 {
		return queueInfo{}, false
	}
	if len(queues) == 1 {
		return queues[0], true
	}
	if totalWeight <= 0 {
		return queues[rand.Intn(len(queues))], true
	}
	r := rand.Intn(totalWeight)
	for _, q := range queues {
		r -= q.priority
		if r < 0 {
			return q, true
		}
	}
	return queueInfo{}, false
}

func selectWeightedQueueSkipping(queues []queueInfo, skipped []string) (queueInfo, bool) {
	var only queueInfo
	var count int
	var totalWeight int
	for _, q := range queues {
		if queueNameSkipped(q.name, skipped) {
			continue
		}
		only = q
		count++
		totalWeight += q.priority
	}
	if count == 0 {
		return queueInfo{}, false
	}
	if count == 1 {
		return only, true
	}
	if totalWeight <= 0 {
		n := rand.Intn(count)
		for _, q := range queues {
			if queueNameSkipped(q.name, skipped) {
				continue
			}
			if n == 0 {
				return q, true
			}
			n--
		}
		return queueInfo{}, false
	}

	r := rand.Intn(totalWeight)
	for _, q := range queues {
		if queueNameSkipped(q.name, skipped) {
			continue
		}
		r -= q.priority
		if r < 0 {
			return q, true
		}
	}
	return queueInfo{}, false
}

func queueNameSkipped(name string, skipped []string) bool {
	for _, skippedName := range skipped {
		if name == skippedName {
			return true
		}
	}
	return false
}

func appendSkippedQueueName(skipped []string, name string) []string {
	if queueNameSkipped(name, skipped) {
		return skipped
	}
	return append(skipped, name)
}

// blockingFetchStrict tries all queues non-blocking in priority order,
// then blocks on the HIGHEST priority queue so high-priority jobs wake us immediately
func (f *fetcher) blockingFetchStrict(ctx context.Context, workerID string, blockTimeout time.Duration) (*senna.Job, error) {
	if len(f.activeQueues) == 0 {
		if err := f.waitPollInterval(ctx, blockTimeout); err != nil {
			return nil, err
		}
		return nil, nil
	}

	// Try ALL processable queues non-blocking in priority order (high to low)
	sawSequentialEmpty := false
	for _, q := range f.activeQueues {
		job, status, err := f.fetchFromQueueInfo(ctx, workerID, q)
		if err != nil {
			return nil, err
		}
		if job != nil {
			return job, nil
		}
		if q.sequential && status == queueFetchEmpty {
			sawSequentialEmpty = true
		}
	}

	// BLMOVE can only watch one queue. Keep multi-queue rotation bounded by the
	// poll interval so lower-priority queues are rechecked promptly when all
	// current queues are empty.
	if len(f.blockableQueues) != 1 || sawSequentialEmpty {
		if err := f.waitPollInterval(ctx, blockTimeout); err != nil {
			return nil, err
		}
		return nil, nil
	}

	return f.blockingFetchFromQueueInfo(ctx, workerID, f.blockableQueues[0], blockTimeout)
}

func (f *fetcher) blockingFetchFromQueueInfo(ctx context.Context, workerID string, queue queueInfo, blockTimeout time.Duration) (*senna.Job, error) {
	// Sequential queues can't use BLMOVE - multiple workers blocking would
	// break the exclusivity guarantee. Use non-blocking fetch with the atomic
	// lock script, then wait for the poll interval if no job.
	if queue.sequential {
		job, _, err := f.fetchFromSequentialQueue(ctx, workerID, queue)
		if err != nil || job != nil {
			return job, err
		}
		if err := f.waitPollInterval(ctx, blockTimeout); err != nil {
			return nil, err
		}
		return nil, nil
	}

	inFlightKey := f.keys.InFlight(workerID)

	result, err := f.client.BLMove(ctx, queue.queueKey, inFlightKey, "RIGHT", "LEFT", blockTimeout).Result()
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

func (f *fetcher) waitPollInterval(ctx context.Context, maxWait time.Duration) error {
	interval := f.pollInterval
	if interval <= 0 {
		interval = senna.DefaultWorkerSettings().PollInterval
	}
	if maxWait > 0 && interval > maxWait {
		interval = maxWait
	}

	timer := time.NewTimer(interval)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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
		now, err := redisNow(ctx, f.client)
		if err != nil {
			return fmt.Errorf("get Redis time for retry: %w", err)
		}
		return f.NackAt(ctx, workerID, job, now.Add(retryIn))
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
