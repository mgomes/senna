package client

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

func redisNow(ctx context.Context, client redis.Cmdable) (time.Time, error) {
	return client.Time(ctx).Result()
}
