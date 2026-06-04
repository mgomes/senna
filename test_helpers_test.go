package senna

import (
	"testing"

	"github.com/mgomes/senna/internal/testredis"
	"github.com/redis/go-redis/v9"
)

func newTestRedisClient(t testing.TB) *redis.Client {
	t.Helper()
	return testredis.NewClient(t)
}

func flushTestKeys(t testing.TB, client *redis.Client, pattern string) {
	t.Helper()
	testredis.FlushKeys(t, client, pattern)
}
