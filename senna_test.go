package senna

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgomes/senna/ratelimit"
)

func TestWithinLimit_Success(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "senna:ratelimit:*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:     "test-within-limit",
		Limit:    10,
		Interval: time.Second,
	})

	executed := false
	err := WithinLimit(limiter, func() error {
		executed = true
		return nil
	})

	if err != nil {
		t.Fatalf("WithinLimit failed: %v", err)
	}
	if !executed {
		t.Error("function should have been executed")
	}
}

func TestWithinLimit_FunctionError(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "senna:ratelimit:*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:     "test-within-limit-error",
		Limit:    10,
		Interval: time.Second,
	})

	expectedErr := errors.New("function error")
	err := WithinLimit(limiter, func() error {
		return expectedErr
	})

	if err != expectedErr {
		t.Errorf("expected error to propagate, got %v", err)
	}
}

func TestWithinLimit_OverLimit(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "senna:ratelimit:*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:        "test-within-limit-over",
		Limit:       2,
		Interval:    time.Second,
		WaitTimeout: 10 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	for range 2 {
		WithinLimit(limiter, func() error { return nil })
	}

	err := WithinLimit(limiter, func() error { return nil })

	var overLimitErr *ratelimit.OverLimitError
	if !errors.As(err, &overLimitErr) {
		t.Fatalf("expected OverLimitError, got %T: %v", err, err)
	}
}

func TestWithinLimitCtx_RespectsContext(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "senna:ratelimit:*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:        "test-within-limit-ctx",
		Limit:       1,
		Interval:    time.Second,
		WaitTimeout: 5 * time.Second,
	})

	WithinLimitCtx(context.Background(), limiter, func() error { return nil })

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := WithinLimitCtx(ctx, limiter, func() error { return nil })

	if err == nil {
		t.Error("expected context deadline exceeded error")
	}
}

func TestRateLimitMiddleware_AllowsWithinLimit(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "senna:ratelimit:*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:     "test-mw-allow",
		Limit:    10,
		Interval: time.Second,
	})

	middleware := RateLimitMiddleware(limiter)

	called := false
	handler := middleware(func(ctx context.Context, job *Job) error {
		called = true
		return nil
	})

	job := NewJob("test_job", nil)
	err := handler(context.Background(), job)

	if err != nil {
		t.Fatalf("handler failed: %v", err)
	}
	if !called {
		t.Error("handler should have been called")
	}
}

func TestRateLimitMiddleware_BlocksOverLimit(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "senna:ratelimit:*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:        "test-mw-block",
		Limit:       1,
		Interval:    time.Second,
		WaitTimeout: 10 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	middleware := RateLimitMiddleware(limiter)

	callCount := 0
	handler := middleware(func(ctx context.Context, job *Job) error {
		callCount++
		return nil
	})

	job := NewJob("test_job", nil)
	handler(context.Background(), job)

	err := handler(context.Background(), job)

	if callCount != 1 {
		t.Errorf("expected 1 call, got %d", callCount)
	}

	var overLimitErr *ratelimit.OverLimitError
	if !errors.As(err, &overLimitErr) {
		t.Fatalf("expected OverLimitError, got %T: %v", err, err)
	}
}

func TestRateLimitMiddlewareWithReschedule_AllowsWithinLimit(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "senna:ratelimit:*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:     "test-mw-reschedule-allow",
		Limit:    10,
		Interval: time.Second,
	})

	middleware := RateLimitMiddlewareWithReschedule(limiter)

	called := false
	handler := middleware(func(ctx context.Context, job *Job) error {
		called = true
		return nil
	})

	job := NewJob("test_job", nil)
	err := handler(context.Background(), job)

	if err != nil {
		t.Fatalf("handler failed: %v", err)
	}
	if !called {
		t.Error("handler should have been called")
	}
}

func TestRateLimitMiddlewareWithReschedule_ReturnsRetryableError(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "senna:ratelimit:*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:        "test-mw-reschedule",
		Limit:       1,
		Interval:    time.Second,
		WaitTimeout: 10 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	middleware := RateLimitMiddlewareWithReschedule(limiter)

	handler := middleware(func(ctx context.Context, job *Job) error {
		return nil
	})

	job := NewJob("test_job", nil)
	handler(context.Background(), job)

	err := handler(context.Background(), job)

	var retryErr *RetryableError
	if !errors.As(err, &retryErr) {
		t.Fatalf("expected RetryableError, got %T: %v", err, err)
	}
	if retryErr.RetryIn <= 0 {
		t.Errorf("expected positive RetryIn, got %v", retryErr.RetryIn)
	}
}

func TestWithRateLimiter_SetsOption(t *testing.T) {
	client := newTestRedisClient(t)

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:     "test-option",
		Limit:    10,
		Interval: time.Second,
	})

	opts := &JobOptions{}
	opt := WithRateLimiter(limiter)
	opt(opts)

	if opts.RateLimiter == nil {
		t.Error("RateLimiter should be set")
	}
	if opts.RateLimiter.Name() != "test-option" {
		t.Errorf("expected limiter name 'test-option', got '%s'", opts.RateLimiter.Name())
	}
}

func TestRateLimitMiddleware_ConcurrentAccess(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "senna:ratelimit:*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:        "test-mw-concurrent",
		Limit:       5,
		Interval:    time.Second,
		WaitTimeout: 10 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	middleware := RateLimitMiddleware(limiter)

	var successCount atomic.Int32
	handler := middleware(func(ctx context.Context, job *Job) error {
		successCount.Add(1)
		return nil
	})

	done := make(chan struct{})
	for range 20 {
		go func() {
			job := NewJob("test_job", nil)
			handler(context.Background(), job)
			done <- struct{}{}
		}()
	}

	for range 20 {
		<-done
	}

	if successCount.Load() > 5 {
		t.Errorf("expected at most 5 successful calls, got %d", successCount.Load())
	}
}

func TestWithinLimit_UnlimitedLimiter(t *testing.T) {
	limiter := ratelimit.Unlimited("test-unlimited")

	callCount := 0
	for range 100 {
		err := WithinLimit(limiter, func() error {
			callCount++
			return nil
		})
		if err != nil {
			t.Fatalf("WithinLimit failed: %v", err)
		}
	}

	if callCount != 100 {
		t.Errorf("expected 100 calls, got %d", callCount)
	}
}

func TestRateLimitMiddleware_PreservesJobContext(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "senna:ratelimit:*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:     "test-mw-context",
		Limit:    10,
		Interval: time.Second,
	})

	middleware := RateLimitMiddleware(limiter)

	var receivedJob *Job
	handler := middleware(func(ctx context.Context, job *Job) error {
		receivedJob = job
		return nil
	})

	job := NewJob("test_job", map[string]any{"key": "value"})
	handler(context.Background(), job)

	if receivedJob == nil {
		t.Fatal("job should be passed to handler")
	}
	if receivedJob.ID != job.ID {
		t.Errorf("expected job ID '%s', got '%s'", job.ID, receivedJob.ID)
	}
	if receivedJob.Args["key"] != "value" {
		t.Errorf("expected args['key']='value', got '%v'", receivedJob.Args["key"])
	}
}

func TestRateLimitMiddleware_PropagatesHandlerError(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "senna:ratelimit:*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:     "test-mw-error",
		Limit:    10,
		Interval: time.Second,
	})

	middleware := RateLimitMiddleware(limiter)

	expectedErr := errors.New("handler error")
	handler := middleware(func(ctx context.Context, job *Job) error {
		return expectedErr
	})

	job := NewJob("test_job", nil)
	err := handler(context.Background(), job)

	if err != expectedErr {
		t.Errorf("expected error to propagate, got %v", err)
	}
}
