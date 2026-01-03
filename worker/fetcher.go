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
	queueName := f.selectQueueWeighted()
	if queueName == "" {
		return nil, nil
	}

	return f.fetchFromQueue(ctx, workerID, queueName)
}

// fetchStrict tries each queue in priority order until a job is found
func (f *fetcher) fetchStrict(ctx context.Context, workerID string) (*senna.Job, error) {
	for _, q := range f.queues {
		if q.Paused {
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

// fetchFromQueue attempts to fetch a job from a specific queue
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
