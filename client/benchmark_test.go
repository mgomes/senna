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
	benchmarkClientEnqueueBulk(b, 100)
}

func BenchmarkClientEnqueueBulk1000(b *testing.B) {
	benchmarkClientEnqueueBulk(b, 1000)
}

func BenchmarkClientEnqueueBulk10000(b *testing.B) {
	benchmarkClientEnqueueBulk(b, 10000)
}

func benchmarkClientEnqueueBulk(b *testing.B, count int) {
	b.Helper()

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
	argsList := make([]map[string]any, count)
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
			b.Fatalf("Client.EnqueueBulk(%d jobs) error = %v, want nil", count, err)
		}
	}
}

func BenchmarkClientEnqueueBatchAutoflush100(b *testing.B) {
	benchmarkClientEnqueueBatchAutoflush(b, 100)
}

func BenchmarkClientEnqueueBatchAutoflush1000(b *testing.B) {
	benchmarkClientEnqueueBatchAutoflush(b, 1000)
}

func BenchmarkClientEnqueueBatchAutoflush10000(b *testing.B) {
	benchmarkClientEnqueueBatchAutoflush(b, 10000)
}

func benchmarkClientEnqueueBatchAutoflush(b *testing.B, count int) {
	b.Helper()

	redisClient := newTestRedisClient(b)
	const namespace = "bench-client-enqueue-batch-autoflush"
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

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		batch := NewBatch().WithAutoflush(1000)
		for i := range count {
			batch.Add("bench_batch_job", map[string]any{
				"user_id": i,
				"source":  "benchmark",
			})
		}
		if err := client.EnqueueBatch(ctx, batch); err != nil {
			b.Fatalf("Client.EnqueueBatch(%d autoflush jobs) error = %v, want nil", count, err)
		}
	}
}
