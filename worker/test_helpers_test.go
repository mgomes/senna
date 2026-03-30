package worker

import (
	"testing"

	"github.com/mgomes/senna/internal/testredis"
	"github.com/redis/go-redis/v9"
)

func getTestRedisAddr() string {
	return testredis.Addr()
}

func newTestRedisClient(t testing.TB) *redis.Client {
	return testredis.NewClient(t)
}

func flushTestKeys(t testing.TB, client *redis.Client, pattern string) {
	testredis.FlushKeys(t, client, pattern)
}
