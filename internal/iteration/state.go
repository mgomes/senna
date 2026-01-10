package iteration

import (
	"context"
	"encoding/json"
	"time"

	"github.com/mgomes/senna"
	"github.com/redis/go-redis/v9"
)

const StateTTL = 30 * 24 * time.Hour

func Load(ctx context.Context, client redis.Cmdable, key string) (*senna.IterationState, error) {
	data, err := client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	var state senna.IterationState
	if err := json.Unmarshal([]byte(data), &state); err != nil {
		return nil, err
	}

	return &state, nil
}

func Save(ctx context.Context, client redis.Cmdable, key string, state *senna.IterationState, ttl time.Duration) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	return client.Set(ctx, key, string(data), ttl).Err()
}

func IsCancelled(ctx context.Context, client redis.Cmdable, key string) (bool, error) {
	state, err := Load(ctx, client, key)
	if err != nil || state == nil {
		return false, err
	}
	return state.Cancelled, nil
}

func Cancel(ctx context.Context, client redis.Cmdable, key, jobID string, ttl time.Duration) error {
	state, err := Load(ctx, client, key)
	if err != nil {
		return err
	}

	if state == nil {
		state = &senna.IterationState{
			JobID:     jobID,
			Cancelled: true,
		}
		return Save(ctx, client, key, state, ttl)
	}

	state.Cancelled = true
	existingTTL, err := client.TTL(ctx, key).Result()
	if err == nil && existingTTL > 0 {
		ttl = existingTTL
	}

	return Save(ctx, client, key, state, ttl)
}
