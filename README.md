# Senna

Senna is a background job processing library for Go, backed by Redis or Valkey. It provides reliable job queuing, scheduling, retries, rate limiting, and batch processing with a clean, middleware-based architecture.

## Supported Backends

Senna officially supports:

- **Redis** 6.0+
- **Valkey** 7.2+

Both backends are fully supported and tested. As Valkey continues to evolve independently from Redis, we are committed to maintaining compatibility with both.

## Features

- Persistent job queues backed by Redis or Valkey
- Scheduled jobs (run at a specific time or after a delay)
- Automatic retries with configurable backoff
- Multiple queue support with weighted priority
- Distributed rate limiting (bucket, sliding window, leaky bucket, concurrent, points-based)
- Job batching with completion callbacks
- Job argument encryption (AES-GCM)
- Unique jobs to prevent duplicates
- Graceful shutdown with in-flight job completion
- Middleware support for cross-cutting concerns
- Per-job concurrency limits

## Installation

```bash
go get github.com/mgomes/senna
```

## Quick Start

### Enqueuing Jobs (Client)

The client is used to enqueue jobs from your application code.

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/mgomes/senna"
)

func main() {
    // Create a client
    client, err := senna.NewClient(&senna.ClientConfig{
        Redis: senna.RedisConfig{
            Addr: "localhost:6379",
        },
        Namespace: "myapp",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    ctx := context.Background()

    // Enqueue a job immediately
    job, err := client.Enqueue(ctx, "send_email", map[string]any{
        "to":      "user@example.com",
        "subject": "Welcome!",
        "body":    "Thanks for signing up.",
    })
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Enqueued job: %s", job.ID)

    // Enqueue a job to run in 5 minutes
    job, err = client.EnqueueIn(ctx, 5*time.Minute, "send_reminder", map[string]any{
        "user_id": 123,
    })
    if err != nil {
        log.Fatal(err)
    }

    // Enqueue a job to run at a specific time
    runAt := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
    job, err = client.EnqueueAt(ctx, runAt, "new_year_notification", nil)
    if err != nil {
        log.Fatal(err)
    }

    // Enqueue to a specific queue
    job, err = client.Enqueue(ctx, "process_payment", map[string]any{
        "order_id": 456,
        "amount":   99.99,
    }, senna.WithQueue("critical"))
    if err != nil {
        log.Fatal(err)
    }
}
```

### Processing Jobs (Worker)

The worker fetches jobs from queues and processes them using registered handlers.

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/mgomes/senna"
)

func main() {
    // Create a worker
    worker, err := senna.NewWorker(&senna.WorkerConfig{
        Redis: senna.RedisConfig{
            Addr: "localhost:6379",
        },
        Namespace: "myapp",
        Settings: senna.WorkerSettings{
            Concurrency: 10,
            Queues: []senna.QueueConfig{
                {Name: "critical", Priority: 10},
                {Name: "default", Priority: 5},
                {Name: "low", Priority: 1},
            },
            ShutdownTimeout: 30 * time.Second,
            PollInterval:    100 * time.Millisecond,
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    // Register job handlers
    worker.Register("send_email", func(ctx context.Context, job *senna.Job) error {
        to := job.Args["to"].(string)
        subject := job.Args["subject"].(string)
        body := job.Args["body"].(string)

        fmt.Printf("Sending email to %s: %s\n", to, subject)
        // ... send email logic ...

        return nil
    })

    worker.Register("process_payment", func(ctx context.Context, job *senna.Job) error {
        orderID := int(job.Args["order_id"].(float64))
        amount := job.Args["amount"].(float64)

        fmt.Printf("Processing payment for order %d: $%.2f\n", orderID, amount)
        // ... payment logic ...

        return nil
    }, senna.WithMaxRetries(3), senna.WithJobTimeout(30*time.Second))

    // Run the worker (blocks until shutdown signal)
    ctx := context.Background()
    if err := worker.Run(ctx); err != nil {
        log.Fatal(err)
    }
}
```

## Job Options

### Enqueue Options

```go
// Send to a specific queue
client.Enqueue(ctx, "job_type", args, senna.WithQueue("critical"))

// Set retry count
client.Enqueue(ctx, "job_type", args, senna.WithRetry(5))

// Unique job (prevents duplicates)
client.Enqueue(ctx, "sync_user", args,
    senna.WithUniqueKey("user:123:sync", time.Hour))

// Encrypt job arguments
client.Enqueue(ctx, "sensitive_job", args, senna.WithEncryption())

// Delay execution
client.Enqueue(ctx, "job_type", args, senna.WithDelay(10*time.Minute))
```

### Handler Options

```go
// Limit retries
worker.Register("job_type", handler, senna.WithMaxRetries(3))

// Set job timeout
worker.Register("job_type", handler, senna.WithJobTimeout(5*time.Minute))

// Limit concurrent executions of this job type
worker.Register("api_sync", handler, senna.WithMaxConcurrency(2))
```

## Middleware

Senna uses a middleware pattern for cross-cutting concerns. Middleware wraps job handlers and can perform actions before and after job execution.

### Built-in Middleware

```go
import "log/slog"

// Logging middleware
logger := slog.Default()
worker.Use(senna.LoggingMiddleware(logger))

// Timeout middleware (applies to all jobs)
worker.Use(senna.TimeoutMiddleware(5 * time.Minute))

// Recovery middleware (automatically added)
// Catches panics and converts them to errors
```

### Custom Middleware

```go
func MetricsMiddleware() senna.Middleware {
    return func(next senna.Handler) senna.Handler {
        return func(ctx context.Context, job *senna.Job) error {
            start := time.Now()

            err := next(ctx, job)

            duration := time.Since(start)
            if err != nil {
                metrics.JobFailed(job.Type, duration)
            } else {
                metrics.JobSucceeded(job.Type, duration)
            }

            return err
        }
    }
}

worker.Use(MetricsMiddleware())
```

## Rate Limiting

Senna provides several distributed rate limiting algorithms. All limiters are backed by Redis/Valkey and work across multiple worker instances.

### Bucket Rate Limiter

Fixed window rate limiting. Allows N requests per time interval.

```go
import "github.com/mgomes/senna/ratelimit"

// Allow 100 requests per minute
limiter := ratelimit.Bucket(worker.Redis(), ratelimit.BucketConfig{
    Name:     "api-calls",
    Limit:    100,
    Interval: time.Minute,
    Policy:   ratelimit.PolicySkip, // Skip if over limit (vs PolicyRaise)
})

// Use as middleware
worker.Use(senna.RateLimitMiddleware(limiter))

// Or use directly
err := limiter.WithinLimit(ctx, func() error {
    return callExternalAPI()
})
```

### Sliding Window Rate Limiter

More accurate rate limiting using a sliding time window.

```go
limiter := ratelimit.Window(worker.Redis(), ratelimit.WindowConfig{
    Name:     "api-calls",
    Limit:    100,
    Interval: time.Minute,
    Policy:   ratelimit.PolicySkip,
})
```

### Leaky Bucket Rate Limiter

Smooths out bursts by draining requests at a constant rate.

```go
limiter := ratelimit.Leaky(worker.Redis(), ratelimit.LeakyConfig{
    Name:      "notifications",
    Capacity:  50,            // Bucket size
    DrainTime: time.Second,   // Time to drain entire bucket
    Policy:    ratelimit.PolicySkip,
})
```

### Concurrent Limiter

Limits the number of concurrent operations (like a semaphore).

```go
limiter := ratelimit.Concurrent(worker.Redis(), ratelimit.ConcurrentConfig{
    Name:        "db-connections",
    Limit:       10,
    LockTimeout: 30 * time.Second, // Auto-release after this time
    Policy:      ratelimit.PolicySkip,
})
```

### Points-Based Limiter

Variable cost rate limiting. Useful when different operations have different costs.

```go
limiter := ratelimit.Points(worker.Redis(), ratelimit.PointsConfig{
    Name:       "api-quota",
    Capacity:   1000,         // Total points
    RefillTime: time.Hour,    // Time to fully refill
    Policy:     ratelimit.PolicySkip,
})

// Consume different amounts
err := limiter.WithinLimitCost(ctx, 10, func() error {
    return heavyOperation()
})

err = limiter.WithinLimitCost(ctx, 1, func() error {
    return lightOperation()
})
```

### Rate Limit with Rescheduling

Instead of failing jobs when rate limited, reschedule them:

```go
limiter := ratelimit.Bucket(worker.Redis(), ratelimit.BucketConfig{
    Name:     "external-api",
    Limit:    60,
    Interval: time.Minute,
})

// Jobs will be retried later instead of failing
worker.Use(senna.RateLimitMiddlewareWithReschedule(limiter))
```

## Batch Jobs

Process related jobs as a group and get notified when all complete.

```go
// Create a batch
batch := senna.NewBatch().
    Add("process_image", map[string]any{"image_id": 1}).
    Add("process_image", map[string]any{"image_id": 2}).
    Add("process_image", map[string]any{"image_id": 3}).
    OnCompleteCallback("batch_finished")

// Enqueue the batch
err := client.EnqueueBatch(ctx, batch)

// Register the callback handler
worker.Register("batch_finished", func(ctx context.Context, job *senna.Job) error {
    batchID := job.Args["batch_id"].(string)
    fmt.Printf("Batch %s completed!\n", batchID)
    return nil
})
```

You can also set callbacks for success (all jobs succeeded) or death (any job failed permanently):

```go
batch := senna.NewBatch().
    Add("job1", nil).
    Add("job2", nil).
    OnCompleteCallback("on_complete").    // Always called
    OnSuccessCallback("on_success").      // Called if all succeed
    OnDeathCallback("on_death")           // Called if any fail permanently
```

## Encryption

Encrypt sensitive job arguments at rest. Arguments are encrypted with AES-GCM before being stored.

```go
// Generate a 32-byte key (store securely, e.g., in environment variables)
key := make([]byte, 32)
// ... load key from secure storage ...

// Client with encryption
client, err := senna.NewClient(&senna.ClientConfig{
    Redis:     senna.RedisConfig{Addr: "localhost:6379"},
    Namespace: "myapp",
    Encryption: &senna.EncryptionSettings{
        Enabled: true,
        Key:     key,
    },
})

// Enqueue an encrypted job
client.Enqueue(ctx, "process_pii", map[string]any{
    "ssn":         "123-45-6789",
    "card_number": "4111111111111111",
}, senna.WithEncryption())

// Worker with encryption (uses same key)
worker, err := senna.NewWorker(&senna.WorkerConfig{
    Redis:     senna.RedisConfig{Addr: "localhost:6379"},
    Namespace: "myapp",
    Encryption: &senna.EncryptionSettings{
        Enabled: true,
        Key:     key,
    },
})

// Handler receives decrypted arguments
worker.Register("process_pii", func(ctx context.Context, job *senna.Job) error {
    ssn := job.Args["ssn"].(string) // Already decrypted
    // ...
    return nil
})
```

## Unique Jobs

Prevent duplicate jobs from being enqueued using a unique key:

```go
// Only one sync job per user can be enqueued within the TTL
_, err := client.Enqueue(ctx, "sync_user_data", map[string]any{
    "user_id": 123,
}, senna.WithUniqueKey("sync:user:123", time.Hour))

// Second enqueue with same key returns DuplicateJobError
_, err = client.Enqueue(ctx, "sync_user_data", map[string]any{
    "user_id": 123,
}, senna.WithUniqueKey("sync:user:123", time.Hour))

if err != nil {
    var dupErr *senna.DuplicateJobError
    if errors.As(err, &dupErr) {
        // Job already enqueued, skip
    }
}
```

The unique key is automatically cleared when the job completes or fails permanently.

## Configuration

### Redis Configuration

```go
senna.RedisConfig{
    Addr:         "localhost:6379",
    Password:     "secret",
    DB:           0,
    PoolSize:     100,
    MinIdleConns: 10,
    DialTimeout:  5 * time.Second,
    ReadTimeout:  3 * time.Second,
    WriteTimeout: 3 * time.Second,
}
```

### Worker Settings

```go
senna.WorkerSettings{
    Concurrency:     10,                   // Number of concurrent workers
    Queues:          []senna.QueueConfig{  // Queues to process
        {Name: "critical", Priority: 10},
        {Name: "default", Priority: 5},
        {Name: "low", Priority: 1},
    },
    ShutdownTimeout: 30 * time.Second,     // Time to wait for jobs during shutdown
    PollInterval:    100 * time.Millisecond, // How often to check for new jobs
    HeartbeatRate:   5 * time.Second,      // Worker heartbeat interval
}
```

### Client Settings

```go
senna.ClientSettings{
    DefaultQueue: "default",  // Queue used when none specified
    DefaultRetry: 25,         // Default retry count
}
```

## Error Handling

### Retry Behavior

By default, failed jobs are retried with exponential backoff. The default backoff formula is:

```
delay = (attempt^4) + 15 + (attempt * 10) seconds
```

You can use the retry middleware with custom backoff:

```go
// Exponential backoff with max
backoff := senna.ExponentialBackoff(time.Second, time.Hour)
worker.Use(senna.RetryMiddleware(3, backoff))
```

### Returning Errors

```go
worker.Register("my_job", func(ctx context.Context, job *senna.Job) error {
    // Return an error to trigger retry
    if err := doWork(); err != nil {
        return err // Will be retried with backoff
    }

    // Return RetryableError for custom retry timing
    if shouldRetryLater() {
        return &senna.RetryableError{
            Job:     job,
            Cause:   errors.New("external service unavailable"),
            RetryIn: 5 * time.Minute,
        }
    }

    // Return MaxRetriesExceededError to move to dead queue immediately
    if permanentFailure() {
        return &senna.MaxRetriesExceededError{
            Job:   job,
            Cause: errors.New("invalid data"),
        }
    }

    return nil
})
```

### Dead Jobs

Jobs that exceed their retry limit are moved to the dead queue where they can be inspected or retried manually.

## License

MIT License. See LICENSE file for details.
