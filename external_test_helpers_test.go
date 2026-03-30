package senna_test

import (
	"testing"

	"github.com/mgomes/senna/internal/testredis"
)

func getRedisAddr() string {
	return testredis.Addr()
}

func flushKeys(t testing.TB, pattern string) {
	testredis.FlushPattern(t, pattern)
}

func getRedisAddrBatch() string {
	return testredis.Addr()
}

func flushKeysBatch(t testing.TB, pattern string) {
	testredis.FlushPattern(t, pattern)
}
