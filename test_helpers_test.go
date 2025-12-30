package senna

import (
	"context"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
)

func getTestRedisAddr() string {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	return addr
}

func newTestRedisClient(t *testing.T) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr: getTestRedisAddr(),
	})
	t.Cleanup(func() {
		_ = client.Close()
	})
	return client
}

func flushTestKeys(t *testing.T, client *redis.Client, pattern string) {
	ctx := context.Background()
	keys, err := client.Keys(ctx, pattern).Result()
	if err != nil {
		t.Fatalf("failed to get keys: %v", err)
	}
	if len(keys) > 0 {
		client.Del(ctx, keys...)
	}
}
