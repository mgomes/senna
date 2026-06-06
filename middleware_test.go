package senna

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestChain(t *testing.T) {
	t.Parallel()
	var order []string

	m1 := func(next Handler) Handler {
		return func(ctx context.Context, job *Job) error {
			order = append(order, "m1-before")
			err := next(ctx, job)
			order = append(order, "m1-after")
			return err
		}
	}

	m2 := func(next Handler) Handler {
		return func(ctx context.Context, job *Job) error {
			order = append(order, "m2-before")
			err := next(ctx, job)
			order = append(order, "m2-after")
			return err
		}
	}

	handler := func(ctx context.Context, job *Job) error {
		order = append(order, "handler")
		return nil
	}

	chained := Chain(m1, m2)(handler)
	_ = chained(context.Background(), NewJob("test", nil))

	expected := []string{"m1-before", "m2-before", "handler", "m2-after", "m1-after"}
	if len(order) != len(expected) {
		t.Fatalf("expected %d calls, got %d", len(expected), len(order))
	}
	for i, v := range expected {
		if order[i] != v {
			t.Errorf("expected order[%d]='%s', got '%s'", i, v, order[i])
		}
	}
}

func TestChain_Empty(t *testing.T) {
	t.Parallel()
	called := false
	handler := func(ctx context.Context, job *Job) error {
		called = true
		return nil
	}

	chained := Chain()(handler)
	_ = chained(context.Background(), NewJob("test", nil))

	if !called {
		t.Error("handler should be called with empty chain")
	}
}

func TestLoggingMiddleware(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	middleware := LoggingMiddleware(logger)

	called := false
	handler := middleware(func(ctx context.Context, job *Job) error {
		called = true
		return nil
	})

	err := handler(context.Background(), NewJob("test", nil))
	if err != nil {
		t.Fatalf("handler failed: %v", err)
	}
	if !called {
		t.Error("handler should be called")
	}
}

func TestLoggingMiddleware_Error(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	middleware := LoggingMiddleware(logger)

	expectedErr := errors.New("test error")
	handler := middleware(func(ctx context.Context, job *Job) error {
		return expectedErr
	})

	err := handler(context.Background(), NewJob("test", nil))
	if err != expectedErr {
		t.Fatalf("expected error to propagate, got %v", err)
	}
}

func TestRecoveryMiddleware(t *testing.T) {
	t.Parallel()
	middleware := RecoveryMiddleware()

	handler := middleware(func(ctx context.Context, job *Job) error {
		panic("test panic")
	})

	err := handler(context.Background(), NewJob("test", nil))
	if err == nil {
		t.Fatal("expected error from panic")
	}
	if err.Error()[:6] != "panic:" {
		t.Errorf("expected panic error, got %v", err)
	}
}

func TestRecoveryMiddleware_NoPanic(t *testing.T) {
	t.Parallel()
	middleware := RecoveryMiddleware()

	called := false
	handler := middleware(func(ctx context.Context, job *Job) error {
		called = true
		return nil
	})

	err := handler(context.Background(), NewJob("test", nil))
	if err != nil {
		t.Fatalf("handler failed: %v", err)
	}
	if !called {
		t.Error("handler should be called")
	}
}

func TestTimeoutMiddleware(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		middleware := TimeoutMiddleware(time.Minute)

		handler := middleware(func(ctx context.Context, job *Job) error {
			synctest.Wait()
			if err := ctx.Err(); err != nil {
				t.Fatalf("ctx.Err() before timeout = %v, want nil", err)
			}

			<-ctx.Done()
			return ctx.Err()
		})

		err := handler(context.Background(), NewJob("test", nil))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("handler() error = %v, want %v", err, context.DeadlineExceeded)
		}
	})
}

func TestTimeoutMiddleware_WaitsForHandlerReturn(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		middleware := TimeoutMiddleware(time.Minute)
		started := make(chan struct{})
		release := make(chan struct{})

		handler := middleware(func(ctx context.Context, job *Job) error {
			close(started)
			<-ctx.Done()
			<-release
			return nil
		})

		done := make(chan error, 1)
		go func() {
			done <- handler(context.Background(), NewJob("test", nil))
		}()

		<-started
		time.Sleep(time.Minute)
		synctest.Wait()

		select {
		case err := <-done:
			close(release)
			t.Fatalf("handler returned before wrapped function completed: %v", err)
		default:
		}

		close(release)
		synctest.Wait()

		if err := <-done; err != nil {
			t.Fatalf("handler returned error = %v, want nil", err)
		}
	})
}

func TestTimeoutMiddleware_CompletesInTime(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		middleware := TimeoutMiddleware(time.Hour)

		handler := middleware(func(ctx context.Context, job *Job) error {
			time.Sleep(time.Minute)
			return ctx.Err()
		})

		err := handler(context.Background(), NewJob("test", nil))
		if err != nil {
			t.Fatalf("handler() error = %v, want nil", err)
		}
	})
}

func TestTimeoutMiddleware_PropagatesError(t *testing.T) {
	t.Parallel()
	middleware := TimeoutMiddleware(500 * time.Millisecond)

	expectedErr := errors.New("handler error")
	handler := middleware(func(ctx context.Context, job *Job) error {
		return expectedErr
	})

	err := handler(context.Background(), NewJob("test", nil))
	if err != expectedErr {
		t.Fatalf("expected error to propagate, got %v", err)
	}
}

func TestRetryMiddleware(t *testing.T) {
	t.Parallel()
	backoff := func(attempt int) time.Duration {
		return time.Duration(attempt) * time.Second
	}
	middleware := RetryMiddleware(3, backoff)

	job := NewJob("test", nil)
	job.RetryCount = 0

	expectedErr := errors.New("temporary error")
	handler := middleware(func(ctx context.Context, j *Job) error {
		return expectedErr
	})

	err := handler(context.Background(), job)

	var retryErr *RetryableError
	if !errors.As(err, &retryErr) {
		t.Fatalf("expected RetryableError, got %T", err)
	}
	if retryErr.RetryIn != 0 {
		t.Errorf("expected retry in 0s for attempt 0, got %v", retryErr.RetryIn)
	}
}

func TestRetryMiddleware_MaxRetriesExceeded(t *testing.T) {
	t.Parallel()
	backoff := func(attempt int) time.Duration {
		return time.Second
	}
	middleware := RetryMiddleware(3, backoff)

	job := NewJob("test", nil)
	job.RetryCount = 3

	expectedErr := errors.New("persistent error")
	handler := middleware(func(ctx context.Context, j *Job) error {
		return expectedErr
	})

	err := handler(context.Background(), job)

	var maxRetriesErr *MaxRetriesExceededError
	if !errors.As(err, &maxRetriesErr) {
		t.Fatalf("expected MaxRetriesExceededError, got %T", err)
	}
}

func TestRetryMiddleware_Success(t *testing.T) {
	t.Parallel()
	backoff := func(attempt int) time.Duration {
		return time.Second
	}
	middleware := RetryMiddleware(3, backoff)

	job := NewJob("test", nil)

	handler := middleware(func(ctx context.Context, j *Job) error {
		return nil
	})

	err := handler(context.Background(), job)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestExponentialBackoff(t *testing.T) {
	t.Parallel()
	backoff := ExponentialBackoff(time.Second, time.Minute)

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{0, time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{5, 32 * time.Second},
		{6, time.Minute},
		{10, time.Minute},
	}

	for _, tt := range tests {
		result := backoff(tt.attempt)
		if result != tt.expected {
			t.Errorf("attempt %d: expected %v, got %v", tt.attempt, tt.expected, result)
		}
	}
}

func TestDefaultBackoff(t *testing.T) {
	t.Parallel()
	backoff := DefaultBackoff()

	result := backoff(0)
	if result != 15*time.Second {
		t.Errorf("attempt 0: expected 15s, got %v", result)
	}

	result = backoff(1)
	if result != 26*time.Second {
		t.Errorf("attempt 1: expected 26s, got %v", result)
	}
}

func TestMiddlewareChain_Concurrent(t *testing.T) {
	t.Parallel()
	var count atomic.Int32

	m := func(next Handler) Handler {
		return func(ctx context.Context, job *Job) error {
			count.Add(1)
			return next(ctx, job)
		}
	}

	handler := Chain(m, m, m)(func(ctx context.Context, job *Job) error {
		return nil
	})

	done := make(chan struct{})
	for range 100 {
		go func() {
			_ = handler(context.Background(), NewJob("test", nil))
			done <- struct{}{}
		}()
	}

	for range 100 {
		<-done
	}

	if count.Load() != 300 {
		t.Errorf("expected 300 middleware calls, got %d", count.Load())
	}
}
