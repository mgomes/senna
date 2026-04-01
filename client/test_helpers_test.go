package client

import (
	"testing"

	"github.com/mgomes/senna"
	"github.com/mgomes/senna/internal/testredis"
	"github.com/redis/go-redis/v9"
)

func getTestRedisConfig() senna.RedisConfig {
	opts := testredis.Options()
	return senna.RedisConfig{
		Addr:     opts.Addr,
		Password: opts.Password,
		DB:       opts.DB,
	}
}

func newTestRedisClient(t testing.TB) *redis.Client {
	return testredis.NewClient(t)
}

func flushTestKeys(t testing.TB, client *redis.Client, pattern string) {
	testredis.FlushKeys(t, client, pattern)
}
