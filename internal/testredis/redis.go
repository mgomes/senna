package testredis

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

const defaultAddr = "localhost:6379"

func parseOptions() (*redis.Options, error) {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return &redis.Options{Addr: addr}, nil
	}
	if redisURL := os.Getenv("REDIS_URL"); redisURL != "" {
		return redis.ParseURL(redisURL)
	}
	return &redis.Options{Addr: defaultAddr}, nil
}

func Options() *redis.Options {
	opts, err := parseOptions()
	if err != nil {
		panic(fmt.Errorf("invalid REDIS_URL: %w", err))
	}
	return opts
}

func Addr() string {
	return Options().Addr
}

func NewClient(t testing.TB) *redis.Client {
	t.Helper()

	client := redis.NewClient(Options())
	t.Cleanup(func() {
		_ = client.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping failed: %v", err)
	}

	return client
}

func FlushKeys(t testing.TB, client *redis.Client, pattern string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, pattern, 200).Result()
		if err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		if len(keys) > 0 {
			if err := client.Del(ctx, keys...).Err(); err != nil {
				t.Fatalf("del failed: %v", err)
			}
		}
		if next == 0 {
			return
		}
		cursor = next
	}
}

func FlushPattern(t testing.TB, pattern string) {
	t.Helper()

	client := redis.NewClient(Options())
	defer func() {
		_ = client.Close()
	}()

	FlushKeys(t, client, pattern)
}
