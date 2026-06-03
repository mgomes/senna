package senna

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgomes/senna/ratelimit"
)

func waitForRateLimitWindow(interval, guard time.Duration) {
	if interval <= 0 || guard <= 0 || guard >= interval {
		return
	}

	for {
		offset := time.Duration(time.Now().UnixNano()) % interval
		if interval-offset >= guard {
			return
		}
		time.Sleep(interval - offset)
	}
}

type stubLimiter struct {
	name          string
	waitTime      time.Duration
	acquireErr    error
	releaseErr    error
	releaseCalled bool
}

func (l *stubLimiter) WithinLimit(ctx context.Context, fn func() error) error {
	waitTime, err := l.Acquire(ctx)
	if err != nil {
		return err
	}
	if waitTime > 0 {
		return &ratelimit.OverLimitError{LimiterName: l.Name(), LimiterType: "stub", RetryIn: waitTime}
	}
	err = fn()
	return errors.Join(err, l.Release(ctx))
}

func (l *stubLimiter) Acquire(ctx context.Context) (time.Duration, error) {
	return l.waitTime, l.acquireErr
}

func (l *stubLimiter) Release(ctx context.Context) error {
	l.releaseCalled = true
	return l.releaseErr
}

func (l *stubLimiter) Name() string {
	if l.name == "" {
		return "stub"
	}
	return l.name
}

func TestWithinLimit_Success(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "senna:ratelimit:bucket:senna-test-within-limit*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:     "senna-test-within-limit",
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
	flushTestKeys(t, client, "senna:ratelimit:bucket:senna-test-within-limit-error*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:     "senna-test-within-limit-error",
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
	flushTestKeys(t, client, "senna:ratelimit:bucket:senna-test-within-limit-over*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:        "senna-test-within-limit-over",
		Limit:       2,
		Interval:    time.Second,
		WaitTimeout: 10 * time.Millisecond,
		Policy:      ratelimit.PolicySkip,
	})

	waitForRateLimitWindow(time.Second, 200*time.Millisecond)
	for range 2 {
		_ = WithinLimit(limiter, func() error { return nil })
	}

	err := WithinLimit(limiter, func() error { return nil })

	var overLimitErr *ratelimit.OverLimitError
	if !errors.As(err, &overLimitErr) {
		t.Fatalf("expected OverLimitError, got %T: %v", err, err)
	}
}

func TestWithinLimitCtx_RespectsContext(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "senna:ratelimit:concurrent:senna-test-within-limit-ctx*")

	// Use concurrent limiter - it's more predictable for this test
	// because it tracks active operations rather than time windows
	limiter := ratelimit.Concurrent(client, ratelimit.ConcurrentConfig{
		Name:        "senna-test-within-limit-ctx",
		Limit:       1,
		LockTimeout: 5 * time.Second,
		WaitTimeout: 5 * time.Second,
	})

	// First call acquires the only slot and holds it (we don't release)
	blocker := make(chan struct{})
	go func() {
		_ = WithinLimitCtx(context.Background(), limiter, func() error {
			<-blocker // Block until test is done
			return nil
		})
	}()

	// Give the goroutine time to acquire the lock
	time.Sleep(50 * time.Millisecond)

	// Second call should block waiting for the slot, then context times out
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := WithinLimitCtx(ctx, limiter, func() error { return nil })

	// Release the blocker
	close(blocker)

	if err == nil {
		t.Error("expected error when context times out")
	}
}

func TestRateLimitMiddleware_AllowsWithinLimit(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "senna:ratelimit:bucket:senna-test-mw-allow*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:     "senna-test-mw-allow",
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
	flushTestKeys(t, client, "senna:ratelimit:bucket:senna-test-mw-block*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:        "senna-test-mw-block",
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
	waitForRateLimitWindow(time.Second, 200*time.Millisecond)
	_ = handler(context.Background(), job)

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
	flushTestKeys(t, client, "senna:ratelimit:bucket:senna-test-mw-reschedule-allow*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:     "senna-test-mw-reschedule-allow",
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
	flushTestKeys(t, client, "senna:ratelimit:bucket:senna-test-mw-reschedule*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:        "senna-test-mw-reschedule",
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
	waitForRateLimitWindow(time.Second, 200*time.Millisecond)
	_ = handler(context.Background(), job)

	err := handler(context.Background(), job)

	var retryErr *RetryableError
	if !errors.As(err, &retryErr) {
		t.Fatalf("expected RetryableError, got %T: %v", err, err)
	}
	if retryErr.RetryIn <= 0 {
		t.Errorf("expected positive RetryIn, got %v", retryErr.RetryIn)
	}
}

func TestRateLimitMiddlewareWithReschedule_ReturnsReleaseError(t *testing.T) {
	releaseErr := errors.New("release failed")
	limiter := &stubLimiter{releaseErr: releaseErr}
	handler := RateLimitMiddlewareWithReschedule(limiter)(func(ctx context.Context, job *Job) error {
		return nil
	})

	err := handler(context.Background(), NewJob("test_job", nil))
	if !errors.Is(err, releaseErr) {
		t.Fatalf("RateLimitMiddlewareWithReschedule() error = %v, want release error", err)
	}
	if !limiter.releaseCalled {
		t.Fatal("RateLimitMiddlewareWithReschedule() did not release limiter")
	}
}

func TestRateLimitMiddlewareWithReschedule_JoinsHandlerAndReleaseErrors(t *testing.T) {
	handlerErr := errors.New("handler failed")
	releaseErr := errors.New("release failed")
	limiter := &stubLimiter{releaseErr: releaseErr}
	handler := RateLimitMiddlewareWithReschedule(limiter)(func(ctx context.Context, job *Job) error {
		return handlerErr
	})

	err := handler(context.Background(), NewJob("test_job", nil))
	if !errors.Is(err, handlerErr) {
		t.Fatalf("RateLimitMiddlewareWithReschedule() error = %v, want handler error", err)
	}
	if !errors.Is(err, releaseErr) {
		t.Fatalf("RateLimitMiddlewareWithReschedule() error = %v, want release error", err)
	}
}

func TestRateLimitMiddleware_ConcurrentAccess(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "senna:ratelimit:bucket:senna-test-mw-concurrent*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:        "senna-test-mw-concurrent",
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
	waitForRateLimitWindow(time.Second, 200*time.Millisecond)
	for range 20 {
		go func() {
			job := NewJob("test_job", nil)
			_ = handler(context.Background(), job)
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
	flushTestKeys(t, client, "senna:ratelimit:bucket:senna-test-mw-context*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:     "senna-test-mw-context",
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
	_ = handler(context.Background(), job)

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
	flushTestKeys(t, client, "senna:ratelimit:bucket:senna-test-mw-error*")

	limiter := ratelimit.Bucket(client, ratelimit.BucketConfig{
		Name:     "senna-test-mw-error",
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
