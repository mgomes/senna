package worker

import (
	"context"
	"encoding/json"
	"math/rand"
	"sort"
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
	totalWeight    int
	strictPriority bool
}

func newFetcher(client *redis.Client, k *keys.Keys, queues []senna.QueueConfig, pollInterval time.Duration, strictPriority bool) *fetcher {
	var totalWeight int
	for i := range queues {
		if queues[i].Priority < 1 {
			queues[i].Priority = 1
		}
		if !queues[i].Paused {
			totalWeight += queues[i].Priority
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

	return &fetcher{
		client:         client,
		keys:           k,
		queues:         queues,
		pollInterval:   pollInterval,
		totalWeight:    totalWeight,
		strictPriority: strictPriority,
	}
}

// canProcessQueue checks if this worker can process from the given queue.
// For sequential queues, attempts to acquire or renew the exclusive lock.
func (f *fetcher) canProcessQueue(ctx context.Context, workerID string, q senna.QueueConfig) bool {
	if q.Paused {
		return false
	}
	if !q.Sequential {
		return true
	}

	lockKey := f.keys.SequentialLock(q.Name)

	// Try to acquire lock
	ok, err := f.client.SetNX(ctx, lockKey, workerID, sequentialLockTTL).Result()
	if err != nil {
		return false
	}
	if ok {
		return true
	}

	// Check if we already hold it
	holder, err := f.client.Get(ctx, lockKey).Result()
	if err != nil {
		return false
	}
	if holder == workerID {
		// Renew our lock
		f.client.Expire(ctx, lockKey, sequentialLockTTL)
		return true
	}

	return false
}

// selectQueueWeighted uses weighted random selection
func (f *fetcher) selectQueueWeighted() string {
	if f.totalWeight == 0 {
		return ""
	}
	if len(f.queues) == 1 && !f.queues[0].Paused {
		return f.queues[0].Name
	}

	r := rand.Intn(f.totalWeight)
	for _, q := range f.queues {
		if q.Paused {
			continue
		}
		r -= q.Priority
		if r < 0 {
			return q.Name
		}
	}
	return ""
}

func (f *fetcher) Fetch(ctx context.Context, workerID string) (*senna.Job, error) {
	if f.strictPriority {
		return f.fetchStrict(ctx, workerID)
	}
	return f.fetchWeighted(ctx, workerID)
}

// fetchWeighted selects a queue using weighted random and tries to fetch from it
func (f *fetcher) fetchWeighted(ctx context.Context, workerID string) (*senna.Job, error) {
	// Build list of queues we can currently process
	processable := make([]senna.QueueConfig, 0, len(f.queues))
	var totalWeight int
	for _, q := range f.queues {
		if f.canProcessQueue(ctx, workerID, q) {
			processable = append(processable, q)
			totalWeight += q.Priority
		}
	}

	if len(processable) == 0 {
		return nil, nil
	}

	// Select queue using weighted random from processable queues
	var queueName string
	if len(processable) == 1 {
		queueName = processable[0].Name
	} else {
		r := rand.Intn(totalWeight)
		for _, q := range processable {
			r -= q.Priority
			if r < 0 {
				queueName = q.Name
				break
			}
		}
	}

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
	queueKey := f.keys.Queue(queueName)
	inFlightKey := f.keys.InFlight(workerID)

	result, err := f.client.LMove(ctx, queueKey, inFlightKey, "RIGHT", "LEFT").Result()
	if err == redis.Nil {
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

// BlockingFetch blocks until a job is available, then atomically moves it to in-flight.
// Uses BLMOVE (Redis 6.2+) for efficient blocking without polling.
func (f *fetcher) BlockingFetch(ctx context.Context, workerID string, timeout time.Duration) (*senna.Job, error) {
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
	processable := make([]senna.QueueConfig, 0, len(f.queues))
	var totalWeight int
	for _, q := range f.queues {
		if f.canProcessQueue(ctx, workerID, q) {
			processable = append(processable, q)
			totalWeight += q.Priority
		}
	}

	if len(processable) == 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(timeout):
			return nil, nil
		}
	}

	// Select primary queue using weighted random from processable queues
	var primaryQueue string
	if len(processable) == 1 {
		primaryQueue = processable[0].Name
	} else {
		r := rand.Intn(totalWeight)
		for _, q := range processable {
			r -= q.Priority
			if r < 0 {
				primaryQueue = q.Name
				break
			}
		}
	}

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
	var blockQueue string
	if len(processable) == 1 {
		blockQueue = processable[0].Name
	} else {
		r := rand.Intn(totalWeight)
		for _, q := range processable {
			r -= q.Priority
			if r < 0 {
				blockQueue = q.Name
				break
			}
		}
	}

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

// blockingFetchStrict tries all queues non-blocking in priority order,
// then blocks on the HIGHEST priority queue so high-priority jobs wake us immediately
func (f *fetcher) blockingFetchStrict(ctx context.Context, workerID string, timeout time.Duration) (*senna.Job, error) {
	// Build list of queues we can currently process
	// (non-paused, and for sequential queues, we hold the lock)
	// Note: f.queues is already sorted by priority descending for strict mode
	processable := make([]senna.QueueConfig, 0, len(f.queues))
	for _, q := range f.queues {
		if f.canProcessQueue(ctx, workerID, q) {
			processable = append(processable, q)
		}
	}

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

// blockingFetchFromQueue uses BLMOVE to block until a job is available
func (f *fetcher) blockingFetchFromQueue(ctx context.Context, workerID, queueName string, timeout time.Duration) (*senna.Job, error) {
	queueKey := f.keys.Queue(queueName)
	inFlightKey := f.keys.InFlight(workerID)

	result, err := f.client.BLMove(ctx, queueKey, inFlightKey, "RIGHT", "LEFT", timeout).Result()
	if err == redis.Nil {
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
	inFlightKey := f.keys.InFlight(workerID)

	payload := job.Raw()
	if payload == "" {
		data, err := job.Marshal()
		if err != nil {
			return err
		}
		payload = string(data)
	}

	if err := f.client.LRem(ctx, inFlightKey, 1, payload).Err(); err != nil {
		return err
	}

	if job.UniqueKey != "" {
		f.client.Del(ctx, f.keys.Unique(job.UniqueKey))
	}

	return nil
}

func (f *fetcher) Nack(ctx context.Context, workerID string, job *senna.Job, retryIn time.Duration) error {
	inFlightKey := f.keys.InFlight(workerID)

	payload := job.Raw()
	if payload == "" {
		data, err := job.Marshal()
		if err != nil {
			return err
		}
		payload = string(data)
	}

	if err := f.client.LRem(ctx, inFlightKey, 1, payload).Err(); err != nil {
		return err
	}

	if retryIn > 0 {
		job.RetryCount++
		retryAt := time.Now().Add(retryIn)
		newData, err := job.Marshal()
		if err != nil {
			return err
		}
		return f.client.ZAdd(ctx, f.keys.Retry(), redis.Z{
			Score:  float64(retryAt.Unix()),
			Member: string(newData),
		}).Err()
	}

	return nil
}

func (f *fetcher) MoveToDead(ctx context.Context, workerID string, job *senna.Job) error {
	inFlightKey := f.keys.InFlight(workerID)

	payload := job.Raw()
	if payload == "" {
		data, err := job.Marshal()
		if err != nil {
			return err
		}
		payload = string(data)
	}

	if err := f.client.LRem(ctx, inFlightKey, 1, payload).Err(); err != nil {
		return err
	}

	now := time.Now()
	job.FailedAt = &now
	newData, err := job.Marshal()
	if err != nil {
		return err
	}

	if job.UniqueKey != "" {
		f.client.Del(ctx, f.keys.Unique(job.UniqueKey))
	}

	return f.client.ZAdd(ctx, f.keys.Dead(), redis.Z{
		Score:  float64(now.Unix()),
		Member: string(newData),
	}).Err()
}
