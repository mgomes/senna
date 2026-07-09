# Senna

<p align="center">
  <img src="senna-logo.png" width="30%" alt="Senna">
</p>

---

Senna is a background job processing library for Go, backed by Redis or Valkey. It provides reliable job queuing, scheduling, retries, rate limiting, and batch processing with a clean, middleware-based architecture.

## Supported Backends

- **Redis** 6.2+
- **Valkey** 7.2+

## Features

- Persistent job queues backed by Redis or Valkey
- Scheduled jobs (run at a specific time or after a delay)
- Automatic retries with configurable backoff
- Multiple queues with weighted or strict priority
- Sequential queues for ordered, one-at-a-time processing
- Distributed rate limiting (bucket, sliding window, leaky bucket, concurrent, points-based)
- Job batching with completion callbacks
- Iterable jobs for resumable large dataset processing
- Job argument encryption (AES-GCM)
- Unique jobs to prevent duplicates
- Graceful shutdown with in-flight job completion
- Middleware support for cross-cutting concerns
- Per-job concurrency limits

## Installation

Senna requires Go 1.25 or later.

```bash
go get github.com/mgomes/senna
```

## Quick Start

### Enqueuing Jobs

```go
c, err := client.New(&client.Config{
    Redis:     senna.RedisConfig{Addr: "localhost:6379"},
    Namespace: "myapp",
})
if err != nil {
    log.Fatal(err)
}
defer c.Close()

// Enqueue immediately
c.Enqueue(ctx, "send_email", map[string]any{
    "to":      "user@example.com",
    "subject": "Welcome!",
})

// Enqueue with delay
c.EnqueueIn(ctx, 5*time.Minute, "send_reminder", map[string]any{"user_id": 123})

// Enqueue to specific queue
c.Enqueue(ctx, "process_payment", args, client.WithQueue("critical"))
```

Queue names must not be empty or only whitespace. Senna validates that at enqueue time, but it does not verify that a worker is currently configured for every valid queue name. A job sent to a queue no worker consumes will remain queued until a worker starts consuming that queue.

### Processing Jobs

```go
w, err := worker.New(&worker.Config{
    Redis:     senna.RedisConfig{Addr: "localhost:6379"},
    Namespace: "myapp",
    Settings: senna.WorkerSettings{
        Concurrency: 10,
        Queues: []senna.QueueConfig{
            {Name: "critical", Priority: 10},
            {Name: "default", Priority: 5},
        },
    },
})
if err != nil {
    log.Fatal(err)
}

w.Register("send_email", func(ctx context.Context, job *senna.Job) error {
    to := job.Args["to"].(string)
    // Send email...
    return nil
})

w.Run(ctx) // Blocks until shutdown signal
```

## Documentation

For detailed documentation, see the [Wiki](../../wiki):

- **[Architecture Decisions](docs/adr)** - Durable design commitments and tradeoffs
- **[Getting Started](../../wiki/Getting-Started)** - Installation and setup
- **[Enqueuing Jobs](../../wiki/Enqueuing-Jobs)** - Client API, scheduling, bulk enqueue
- **[Workers](../../wiki/Workers)** - Configuration, handlers, graceful shutdown
- **[Queues](../../wiki/Queues)** - Priority modes, sequential queues, dedicated workers
- **[Batches](../../wiki/Batches)** - Grouping jobs with callbacks
- **[Iterable Jobs](../../wiki/Iterable-Jobs)** - Resumable large dataset processing
- **[Rate Limiters](../../wiki/Rate-Limiters)** - Distributed rate limiting algorithms
- **[Periodic Jobs](../../wiki/Periodic-Jobs)** - Cron-based scheduling
- **[Middleware](../../wiki/Middleware)** - Built-in and custom middleware
- **[Error Handling](../../wiki/Error-Handling)** - Retries, backoff, dead letter queue

## License

MIT License. See LICENSE file for details.
