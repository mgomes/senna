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
    "github.com/mgomes/senna/client"
)

func main() {
    // Create a client
    c, err := client.New(&client.Config{
        Redis: senna.RedisConfig{
            Addr: "localhost:6379",
        },
        Namespace: "myapp",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    ctx := context.Background()

    // Enqueue a job immediately
    job, err := c.Enqueue(ctx, "send_email", map[string]any{
        "to":      "user@example.com",
        "subject": "Welcome!",
        "body":    "Thanks for signing up.",
    })
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("Enqueued job: %s", job.ID)

    // Enqueue a job to run in 5 minutes
    job, err = c.EnqueueIn(ctx, 5*time.Minute, "send_reminder", map[string]any{
        "user_id": 123,
    })
    if err != nil {
        log.Fatal(err)
    }

    // Enqueue a job to run at a specific time
    runAt := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
    job, err = c.EnqueueAt(ctx, runAt, "new_year_notification", nil)
    if err != nil {
        log.Fatal(err)
    }

    // Enqueue to a specific queue
    job, err = c.Enqueue(ctx, "process_payment", map[string]any{
        "order_id": 456,
        "amount":   99.99,
    }, client.WithQueue("critical"))
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
    "github.com/mgomes/senna/worker"
)

func main() {
    // Create a worker
    w, err := worker.New(&worker.Config{
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
    w.Register("send_email", func(ctx context.Context, job *senna.Job) error {
        to := job.Args["to"].(string)
        subject := job.Args["subject"].(string)
        body := job.Args["body"].(string)

        fmt.Printf("Sending email to %s: %s\n", to, subject)
        // ... send email logic ...

        return nil
    })

    w.Register("process_payment", func(ctx context.Context, job *senna.Job) error {
        orderID := int(job.Args["order_id"].(float64))
        amount := job.Args["amount"].(float64)

        fmt.Printf("Processing payment for order %d: $%.2f\n", orderID, amount)
        // ... payment logic ...

        return nil
    }, worker.WithMaxRetries(3), worker.WithJobTimeout(30*time.Second))

    // Run the worker (blocks until shutdown signal)
    ctx := context.Background()
    if err := w.Run(ctx); err != nil {
        log.Fatal(err)
    }
}
```

## Job Options

### Enqueue Options

```go
// Send to a specific queue
c.Enqueue(ctx, "job_type", args, client.WithQueue("critical"))

// Set retry count
c.Enqueue(ctx, "job_type", args, client.WithRetry(5))

// Unique job (prevents duplicates)
c.Enqueue(ctx, "sync_user", args,
    client.WithUniqueKey("user:123:sync", time.Hour))

// Encrypt job arguments
c.Enqueue(ctx, "sensitive_job", args, client.WithEncryption())

// Delay execution
c.Enqueue(ctx, "job_type", args, client.WithDelay(10*time.Minute))
```

### Handler Options

```go
// Limit retries
w.Register("job_type", handler, worker.WithMaxRetries(3))

// Set job timeout
w.Register("job_type", handler, worker.WithJobTimeout(5*time.Minute))

// Limit concurrent executions of this job type
w.Register("api_sync", handler, worker.WithMaxConcurrency(2))
```

## Middleware

Senna uses a middleware pattern for cross-cutting concerns. Middleware wraps job handlers and can perform actions before and after job execution.

### Built-in Middleware

```go
import "log/slog"

// Logging middleware
logger := slog.Default()
w.Use(senna.LoggingMiddleware(logger))

// Timeout middleware (applies to all jobs)
w.Use(senna.TimeoutMiddleware(5 * time.Minute))

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

w.Use(MetricsMiddleware())
```

## Rate Limiting

Senna provides several distributed rate limiting algorithms. All limiters are backed by Redis/Valkey and work across multiple worker instances.

### Bucket Rate Limiter

Fixed window rate limiting. Allows N requests per time interval.

```go
import "github.com/mgomes/senna/ratelimit"

// Allow 100 requests per minute
limiter := ratelimit.Bucket(w.Redis(), ratelimit.BucketConfig{
    Name:     "api-calls",
    Limit:    100,
    Interval: time.Minute,
    Policy:   ratelimit.PolicySkip, // Skip if over limit (vs PolicyRaise)
})

// Use as middleware
w.Use(senna.RateLimitMiddleware(limiter))

// Or use directly
err := limiter.WithinLimit(ctx, func() error {
    return callExternalAPI()
})
```

### Sliding Window Rate Limiter

More accurate rate limiting using a sliding time window.

```go
limiter := ratelimit.Window(w.Redis(), ratelimit.WindowConfig{
    Name:     "api-calls",
    Limit:    100,
    Interval: time.Minute,
    Policy:   ratelimit.PolicySkip,
})
```

### Leaky Bucket Rate Limiter

Smooths out bursts by draining requests at a constant rate.

```go
limiter := ratelimit.Leaky(w.Redis(), ratelimit.LeakyConfig{
    Name:      "notifications",
    Capacity:  50,            // Bucket size
    DrainTime: time.Second,   // Time to drain entire bucket
    Policy:    ratelimit.PolicySkip,
})
```

### Concurrent Limiter

Limits the number of concurrent operations (like a semaphore).

```go
limiter := ratelimit.Concurrent(w.Redis(), ratelimit.ConcurrentConfig{
    Name:        "db-connections",
    Limit:       10,
    LockTimeout: 30 * time.Second, // Auto-release after this time
    Policy:      ratelimit.PolicySkip,
})
```

### Points-Based Limiter

Variable cost rate limiting. Useful when different operations have different costs.

```go
limiter := ratelimit.Points(w.Redis(), ratelimit.PointsConfig{
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
limiter := ratelimit.Bucket(w.Redis(), ratelimit.BucketConfig{
    Name:     "external-api",
    Limit:    60,
    Interval: time.Minute,
})

// Jobs will be retried later instead of failing
w.Use(senna.RateLimitMiddlewareWithReschedule(limiter))
```

## Batch Jobs

Batches allow you to monitor a collection of jobs as a group. You can create a set of jobs to execute in parallel and then execute callbacks when all jobs are finished.

### Creating a Batch

```go
// Create a batch
batch := client.NewBatch().
    WithDescription("Process uploaded spreadsheet").
    Add("process_row", map[string]any{"row_id": 1}).
    Add("process_row", map[string]any{"row_id": 2}).
    Add("process_row", map[string]any{"row_id": 3}).
    OnCompleteCallback("batch_finished")

// Enqueue the batch atomically
err := c.EnqueueBatch(ctx, batch)
fmt.Printf("Started Batch %s\n", batch.ID)
```

### Callbacks

Senna can notify you when a batch completes with three callback types:

1. **complete** - when all jobs in the batch have run once, successful or not
2. **success** - when all jobs in the batch have completed successfully
3. **death** - the first time a batch job dies (exhausts retries)

```go
batch := client.NewBatch().
    Add("job1", nil).
    Add("job2", nil).
    OnCompleteCallback("on_complete").           // Always called when all jobs finish
    OnSuccessCallback("on_success").             // Only if ALL jobs succeed
    OnDeathCallback("on_death")                  // First time any job dies

// Register callback handlers
w.Register("on_complete", func(ctx context.Context, job *senna.Job) error {
    batchID := job.Args["batch_id"].(string)
    fmt.Printf("Batch %s completed\n", batchID)
    return nil
})
```

### Callback Options

You can pass options to callbacks that will be included in the callback job's args:

```go
batch := client.NewBatch().
    Add("sync_user", map[string]any{"user_id": 123}).
    OnSuccessCallback("notify_user", map[string]any{
        "email": "user@example.com",
        "template": "sync_complete",
    })

// In the callback handler:
w.Register("notify_user", func(ctx context.Context, job *senna.Job) error {
    batchID := job.Args["batch_id"].(string)
    email := job.Args["email"].(string)
    template := job.Args["template"].(string)
    // Send notification...
    return nil
})
```

### Callback Queue

You can specify a different queue for callback jobs:

```go
batch := client.NewBatch().
    Add("job1", nil).
    OnCompleteCallback("on_complete").
    WithCallbackQueue("critical")  // Callbacks run on "critical" queue
```

### Adding Jobs Dynamically

You can add jobs to a batch from within an executing job:

```go
w.Register("parent_job", func(ctx context.Context, job *senna.Job) error {
    // Get batch handle from context
    batch := worker.BatchFromContext(ctx)
    if batch == nil {
        return nil // Not in a batch
    }

    fmt.Printf("Working within batch %s\n", batch.BID())

    // Add more jobs to this batch
    for _, childID := range getChildIDs() {
        if err := batch.Add(ctx, "child_job", map[string]any{"id": childID}); err != nil {
            return err
        }
    }

    return nil
})
```

### Batch Status

Query the status of a batch programmatically:

```go
status := c.BatchStatus(batchID)
if err := status.Refresh(ctx); err != nil {
    log.Fatal(err)
}

fmt.Printf("Total: %d\n", status.Total())       // Total jobs in batch
fmt.Printf("Pending: %d\n", status.Pending())   // Jobs not yet complete
fmt.Printf("Successes: %d\n", status.Successes())
fmt.Printf("Failures: %d\n", status.Failures())
fmt.Printf("Complete: %v\n", status.Complete()) // All jobs have run
fmt.Printf("Dead: %v\n", status.Dead())         // Any job has died

// Block until batch completes
ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
defer cancel()
if err := status.Join(ctx); err != nil {
    log.Fatal(err)
}
```

### Canceling a Batch

You can invalidate a batch so remaining jobs skip execution:

```go
// From within a batch job:
batch := worker.BatchFromContext(ctx)
if err := batch.Invalidate(ctx); err != nil {
    return err
}

// Jobs should check validity:
w.Register("cancelable_job", func(ctx context.Context, job *senna.Job) error {
    valid, err := worker.ValidWithinBatch(ctx)
    if err != nil || !valid {
        return nil // Skip execution
    }
    // Do work...
    return nil
})
```

### Iterating Batches

You can iterate through all known batches:

```go
batchSet := senna.NewBatchSet(redisClient, "myapp")
err := batchSet.Each(ctx, func(status *senna.BatchStatus) error {
    fmt.Printf("Batch %s: %d pending\n", status.BID(), status.Pending())
    return nil
})

// Iterate dead batches (those with failed jobs)
deadSet := senna.NewDeadBatchSet(redisClient, "myapp")
err = deadSet.Each(ctx, func(status *senna.BatchStatus) error {
    failedJIDs, _ := status.FailedJIDs(ctx)
    fmt.Printf("Dead batch %s: %v\n", status.BID(), failedJIDs)
    return nil
})
```

### Notes

- Batches expire after 30 days if not completed
- Death and success callbacks are not mutually exclusive - death firing means success won't fire without manual intervention
- Don't disable retries in batch jobs - if a job fails without retrying, it disappears and the batch may never complete
- Empty batches (with zero jobs) are valid and will immediately fire callbacks

## Encryption

Encrypt sensitive job arguments at rest. Arguments are encrypted with AES-GCM before being stored.

```go
// Generate a 32-byte key (store securely, e.g., in environment variables)
key := make([]byte, 32)
// ... load key from secure storage ...

// Client with encryption
c, err := client.New(&client.Config{
    Redis:     senna.RedisConfig{Addr: "localhost:6379"},
    Namespace: "myapp",
    Encryption: &senna.EncryptionSettings{
        Enabled: true,
        Key:     key,
    },
})

// Enqueue an encrypted job
c.Enqueue(ctx, "process_pii", map[string]any{
    "ssn":         "123-45-6789",
    "card_number": "4111111111111111",
}, client.WithEncryption())

// Worker with encryption (uses same key)
w, err := worker.New(&worker.Config{
    Redis:     senna.RedisConfig{Addr: "localhost:6379"},
    Namespace: "myapp",
    Encryption: &senna.EncryptionSettings{
        Enabled: true,
        Key:     key,
    },
})

// Handler receives decrypted arguments
w.Register("process_pii", func(ctx context.Context, job *senna.Job) error {
    ssn := job.Args["ssn"].(string) // Already decrypted
    // ...
    return nil
})
```

## Unique Jobs

Prevent duplicate jobs from being enqueued using a unique key:

```go
// Only one sync job per user can be enqueued within the TTL
_, err := c.Enqueue(ctx, "sync_user_data", map[string]any{
    "user_id": 123,
}, client.WithUniqueKey("sync:user:123", time.Hour))

// Second enqueue with same key returns DuplicateJobError
_, err = c.Enqueue(ctx, "sync_user_data", map[string]any{
    "user_id": 123,
}, client.WithUniqueKey("sync:user:123", time.Hour))

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
    StrictPriority:  false,                // Use strict ordering instead of weighted random
}
```

### Queue Priority Modes

By default, Senna uses **weighted random** queue selection. With priorities of 10:5:1, the critical queue will be checked ~62.5% of the time, default ~31.25%, and low ~6.25%. This ensures all queues get processed while still favoring higher priority.

Enable **strict priority** to always process higher priority queues first:

```go
senna.WorkerSettings{
    Queues: []senna.QueueConfig{
        {Name: "critical", Priority: 10},
        {Name: "default", Priority: 5},
        {Name: "low", Priority: 1},
    },
    StrictPriority: true,
}
```

With strict priority, jobs in the `critical` queue are always processed before `default`, and `default` before `low`. This can lead to starvation of lower priority queues if higher priority queues always have work.

### Client Settings

```go
client.Settings{
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
w.Use(senna.RetryMiddleware(3, backoff))
```

### Returning Errors

```go
w.Register("my_job", func(ctx context.Context, job *senna.Job) error {
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
