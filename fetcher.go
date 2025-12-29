package senna

import (
	"context"
	"encoding/json"
	"math/rand"
	"time"

	"github.com/mgomes/senna/internal/keys"
	"github.com/redis/go-redis/v9"
)

type fetcher struct {
	client       *redis.Client
	keys         *keys.Keys
	queues       []QueueConfig
	pollInterval time.Duration
	totalWeight  int
}

func newFetcher(client *redis.Client, k *keys.Keys, queues []QueueConfig, pollInterval time.Duration) *fetcher {
	var totalWeight int
	for _, q := range queues {
		if q.Priority < 1 {
			q.Priority = 1
		}
		if !q.Paused {
			totalWeight += q.Priority
		}
	}

	return &fetcher{
		client:       client,
		keys:         k,
		queues:       queues,
		pollInterval: pollInterval,
		totalWeight:  totalWeight,
	}
}

func (f *fetcher) selectQueue() string {
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

func (f *fetcher) Fetch(ctx context.Context, workerID string) (*Job, error) {
	queueName := f.selectQueue()
	if queueName == "" {
		return nil, nil
	}

	queueKey := f.keys.Queue(queueName)
	inFlightKey := f.keys.InFlight(workerID)

	result, err := f.client.BRPopLPush(ctx, queueKey, inFlightKey, f.pollInterval).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var job Job
	if err := json.Unmarshal([]byte(result), &job); err != nil {
		return nil, err
	}

	job.raw = result
	return &job, nil
}

func (f *fetcher) Ack(ctx context.Context, workerID string, job *Job) error {
	inFlightKey := f.keys.InFlight(workerID)

	payload := job.raw
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

func (f *fetcher) Nack(ctx context.Context, workerID string, job *Job, retryIn time.Duration) error {
	inFlightKey := f.keys.InFlight(workerID)

	payload := job.raw
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

func (f *fetcher) MoveToDead(ctx context.Context, workerID string, job *Job) error {
	inFlightKey := f.keys.InFlight(workerID)

	payload := job.raw
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
