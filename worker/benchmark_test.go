package worker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/mgomes/senna"
	"github.com/mgomes/senna/internal/batch"
	"github.com/mgomes/senna/internal/keys"
	"github.com/redis/go-redis/v9"
)

func BenchmarkFetcherFetchIdle(b *testing.B) {
	redisClient := newTestRedisClient(b)
	const namespace = "bench-fetch-idle"
	flushTestKeys(b, redisClient, namespace+":*")
	b.Cleanup(func() {
		flushTestKeys(b, redisClient, namespace+":*")
	})

	k := keys.New(namespace)
	f := newFetcher(redisClient, k, []senna.QueueConfig{{Name: "default", Priority: 1}}, 100*time.Millisecond, false)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		job, err := f.Fetch(ctx, "worker-1")
		if err != nil {
			b.Fatalf("fetcher.Fetch() error = %v, want nil", err)
		}
		if job != nil {
			b.Fatalf("fetcher.Fetch() job = %v, want nil", job)
		}
	}
}

func BenchmarkFetcherFetchWeightedMultiQueueIdle(b *testing.B) {
	redisClient := newTestRedisClient(b)
	const namespace = "bench-fetch-weighted-multi-idle"
	flushTestKeys(b, redisClient, namespace+":*")
	b.Cleanup(func() {
		flushTestKeys(b, redisClient, namespace+":*")
	})

	k := keys.New(namespace)
	f := newFetcher(redisClient, k, []senna.QueueConfig{
		{Name: "critical", Priority: 10},
		{Name: "mailers", Priority: 6},
		{Name: "default", Priority: 4},
		{Name: "reports", Priority: 2},
		{Name: "low", Priority: 1},
	}, 100*time.Millisecond, false)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		job, err := f.Fetch(ctx, "worker-1")
		if err != nil {
			b.Fatalf("fetcher.Fetch() error = %v, want nil", err)
		}
		if job != nil {
			b.Fatalf("fetcher.Fetch() job = %v, want nil", job)
		}
	}
}

func BenchmarkFetcherFetchStrictMultiQueueIdle(b *testing.B) {
	redisClient := newTestRedisClient(b)
	const namespace = "bench-fetch-strict-multi-idle"
	flushTestKeys(b, redisClient, namespace+":*")
	b.Cleanup(func() {
		flushTestKeys(b, redisClient, namespace+":*")
	})

	k := keys.New(namespace)
	f := newFetcher(redisClient, k, []senna.QueueConfig{
		{Name: "critical", Priority: 10},
		{Name: "mailers", Priority: 6},
		{Name: "default", Priority: 4},
		{Name: "reports", Priority: 2},
		{Name: "low", Priority: 1},
	}, 100*time.Millisecond, true)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		job, err := f.Fetch(ctx, "worker-1")
		if err != nil {
			b.Fatalf("fetcher.Fetch() error = %v, want nil", err)
		}
		if job != nil {
			b.Fatalf("fetcher.Fetch() job = %v, want nil", job)
		}
	}
}

func BenchmarkFetcherFetchPausedQueuesIdle(b *testing.B) {
	redisClient := newTestRedisClient(b)
	const namespace = "bench-fetch-paused-idle"
	flushTestKeys(b, redisClient, namespace+":*")
	b.Cleanup(func() {
		flushTestKeys(b, redisClient, namespace+":*")
	})

	k := keys.New(namespace)
	f := newFetcher(redisClient, k, []senna.QueueConfig{
		{Name: "critical", Priority: 10, Paused: true},
		{Name: "mailers", Priority: 6, Paused: true},
		{Name: "default", Priority: 4},
		{Name: "reports", Priority: 2, Paused: true},
		{Name: "low", Priority: 1, Paused: true},
	}, 100*time.Millisecond, false)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		job, err := f.Fetch(ctx, "worker-1")
		if err != nil {
			b.Fatalf("fetcher.Fetch() error = %v, want nil", err)
		}
		if job != nil {
			b.Fatalf("fetcher.Fetch() job = %v, want nil", job)
		}
	}
}

func BenchmarkFetcherFetchSequentialLockedQueueIdle(b *testing.B) {
	redisClient := newTestRedisClient(b)
	const namespace = "bench-fetch-sequential-locked-idle"
	flushTestKeys(b, redisClient, namespace+":*")
	b.Cleanup(func() {
		flushTestKeys(b, redisClient, namespace+":*")
	})

	k := keys.New(namespace)
	f := newFetcher(redisClient, k, []senna.QueueConfig{
		{Name: "transforms", Priority: 1, Sequential: true},
	}, 100*time.Millisecond, false)
	ctx := context.Background()

	job := senna.NewJob("bench_fetch_job", nil)
	data, err := job.Marshal()
	if err != nil {
		b.Fatalf("Job.Marshal() error = %v, want nil", err)
	}
	if err := redisClient.LPush(ctx, k.Queue("transforms"), string(data)).Err(); err != nil {
		b.Fatalf("LPush(%q) error = %v, want nil", k.Queue("transforms"), err)
	}
	if err := redisClient.Set(ctx, k.SequentialLock("transforms"), "worker-1", time.Minute).Err(); err != nil {
		b.Fatalf("Set(%q) error = %v, want nil", k.SequentialLock("transforms"), err)
	}

	if job, err := f.Fetch(ctx, "worker-2"); err != nil || job != nil {
		b.Fatalf("warm fetcher.Fetch() = (%v, %v), want (nil, nil)", job, err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		job, err := f.Fetch(ctx, "worker-2")
		if err != nil {
			b.Fatalf("fetcher.Fetch() error = %v, want nil", err)
		}
		if job != nil {
			b.Fatalf("fetcher.Fetch() job = %v, want nil", job)
		}
	}
}

func BenchmarkFetcherBlockingFetchLoaded(b *testing.B) {
	redisClient := newTestRedisClient(b)
	const namespace = "bench-fetch-loaded"
	flushTestKeys(b, redisClient, namespace+":*")
	b.Cleanup(func() {
		flushTestKeys(b, redisClient, namespace+":*")
	})

	k := keys.New(namespace)
	f := newFetcher(redisClient, k, []senna.QueueConfig{{Name: "default", Priority: 1}}, 100*time.Millisecond, false)
	seedFetchJobs(b, redisClient, k, b.N)

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		job, err := f.BlockingFetch(ctx, "worker-1", time.Second)
		if err != nil {
			b.Fatalf("fetcher.BlockingFetch() error = %v, want nil", err)
		}
		if job == nil {
			b.Fatal("fetcher.BlockingFetch() job = nil, want job")
		}
	}
}

func BenchmarkWorkerUpdateBatchProgress(b *testing.B) {
	redisClient := newTestRedisClient(b)
	const namespace = "bench-batch-progress"
	flushTestKeys(b, redisClient, namespace+":*")
	b.Cleanup(func() {
		flushTestKeys(b, redisClient, namespace+":*")
	})

	w, err := New(&Config{
		Redis:     getTestRedisConfig(),
		Namespace: namespace,
		Settings:  senna.DefaultWorkerSettings(),
	})
	if err != nil {
		b.Fatalf("worker.New() error = %v, want nil", err)
	}
	defer func() { _ = w.redis.Close() }()

	const batchID = "bench-batch"
	jobs := make([]*senna.Job, b.N)
	for i := range jobs {
		job := senna.NewJob("bench_batch_job", nil)
		job.BatchID = batchID
		jobs[i] = job
	}
	seedBatchProgress(b, redisClient, w.keys, batchID, jobs)

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for _, job := range jobs {
		if err := w.updateBatchProgress(ctx, job, batch.ResultSuccess); err != nil {
			b.Fatalf("Worker.updateBatchProgress() error = %v, want nil", err)
		}
	}
}

func seedFetchJobs(b *testing.B, redisClient *redis.Client, k *keys.Keys, count int) {
	b.Helper()

	ctx := context.Background()
	pipe := redisClient.Pipeline()
	for range count {
		job := senna.NewJob("bench_fetch_job", nil)
		data, err := job.Marshal()
		if err != nil {
			b.Fatalf("Job.Marshal() error = %v, want nil", err)
		}
		pipe.LPush(ctx, k.Queue("default"), string(data))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		b.Fatalf("seed fetch jobs pipeline error = %v, want nil", err)
	}
}

func seedBatchProgress(b *testing.B, redisClient *redis.Client, k *keys.Keys, batchID string, jobs []*senna.Job) {
	b.Helper()

	ctx := context.Background()
	state := senna.BatchState{
		ID:        batchID,
		Total:     len(jobs),
		Pending:   len(jobs),
		CreatedAt: time.Now(),
	}
	data, err := json.Marshal(state)
	if err != nil {
		b.Fatalf("json.Marshal(BatchState) error = %v, want nil", err)
	}
	if err := redisClient.Set(ctx, k.Batch(batchID), string(data), 0).Err(); err != nil {
		b.Fatalf("Set(%q, batch state) error = %v, want nil", k.Batch(batchID), err)
	}

	const chunkSize = 1000
	for start := 0; start < len(jobs); start += chunkSize {
		end := min(start+chunkSize, len(jobs))
		members := make([]any, 0, end-start)
		for _, job := range jobs[start:end] {
			members = append(members, job.ID)
		}
		if err := redisClient.SAdd(ctx, k.BatchJobs(batchID), members...).Err(); err != nil {
			b.Fatalf("SAdd(%q, job IDs) error = %v, want nil", k.BatchJobs(batchID), err)
		}
	}
}
