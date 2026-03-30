package ratelimit_test

import (
	"testing"

	"github.com/mgomes/senna/internal/testredis"
	"github.com/redis/go-redis/v9"
)

func newTestClient(t testing.TB) *redis.Client {
	return testredis.NewClient(t)
}

func flushKeys(t testing.TB, client *redis.Client, pattern string) {
	testredis.FlushKeys(t, client, pattern)
}
