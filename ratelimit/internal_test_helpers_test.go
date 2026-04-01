package ratelimit

import (
	"testing"

	"github.com/mgomes/senna/internal/testredis"
	"github.com/redis/go-redis/v9"
)

func newRedisClient(t testing.TB) *redis.Client {
	return testredis.NewClient(t)
}

func cleanupKeys(t testing.TB, client *redis.Client, pattern string) {
	testredis.FlushKeys(t, client, pattern)
}
