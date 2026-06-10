package client

import (
	"context"
	"testing"
)

func BenchmarkClientEnqueue(b *testing.B) {
	redisClient := newTestRedisClient(b)
	const namespace = "bench-client-enqueue"
	flushTestKeys(b, redisClient, namespace+":*")
	b.Cleanup(func() {
		flushTestKeys(b, redisClient, namespace+":*")
	})

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: namespace,
	})
	if err != nil {
		b.Fatalf("New() error = %v, want nil", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	args := map[string]any{
		"user_id": 123,
		"source":  "benchmark",
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := client.Enqueue(ctx, "bench_job", args); err != nil {
			b.Fatalf("Client.Enqueue() error = %v, want nil", err)
		}
	}
}

func BenchmarkClientEnqueueBulk100(b *testing.B) {
	redisClient := newTestRedisClient(b)
	const namespace = "bench-client-enqueue-bulk"
	flushTestKeys(b, redisClient, namespace+":*")
	b.Cleanup(func() {
		flushTestKeys(b, redisClient, namespace+":*")
	})

	client, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: namespace,
	})
	if err != nil {
		b.Fatalf("New() error = %v, want nil", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	argsList := make([]map[string]any, 100)
	for i := range argsList {
		argsList[i] = map[string]any{
			"user_id": i,
			"source":  "benchmark",
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := client.EnqueueBulk(ctx, "bench_bulk_job", argsList); err != nil {
			b.Fatalf("Client.EnqueueBulk(100 jobs) error = %v, want nil", err)
		}
	}
}
