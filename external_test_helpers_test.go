package senna_test

import (
	"testing"

	"github.com/mgomes/senna"
	"github.com/mgomes/senna/internal/testredis"
	"github.com/redis/go-redis/v9"
)

func getRedisConfig() senna.RedisConfig {
	opts := testredis.Options()
	return senna.RedisConfig{
		Addr:     opts.Addr,
		Password: opts.Password,
		DB:       opts.DB,
	}
}

func flushKeys(t testing.TB, pattern string) {
	testredis.FlushPattern(t, pattern)
}

func getRedisConfigBatch() senna.RedisConfig {
	opts := testredis.Options()
	return senna.RedisConfig{
		Addr:     opts.Addr,
		Password: opts.Password,
		DB:       opts.DB,
	}
}

func flushKeysBatch(t testing.TB, pattern string) {
	testredis.FlushPattern(t, pattern)
}

func newTestRedisClient(t testing.TB) *redis.Client {
	return testredis.NewClient(t)
}
