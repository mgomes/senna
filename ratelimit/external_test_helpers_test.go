package ratelimit_test

import (
	"testing"

	"github.com/mgomes/senna/internal/testredis"
	"github.com/redis/go-redis/v9"
)

func newTestClient(t testing.TB) *redis.Client {
	t.Helper()
	return testredis.NewClient(t)
}

func flushKeys(t testing.TB, client *redis.Client, pattern string) {
	t.Helper()
	testredis.FlushKeys(t, client, pattern)
}
