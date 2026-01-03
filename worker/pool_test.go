package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgomes/senna"
)

func TestHandlerRegistry_Register(t *testing.T) {
	registry := newHandlerRegistry()

	called := false
	registry.Register("test_job", func(ctx context.Context, job *senna.Job) error {
		called = true
		return nil
	}, nil)

	job := senna.NewJob("test_job", nil)
	_, err := registry.process(context.Background(), job)
	if err != nil {
		t.Fatalf("process failed: %v", err)
	}
	if !called {
		t.Error("handler should have been called")
	}
}

func TestHandlerRegistry_Register_WithOptions(t *testing.T) {
	registry := newHandlerRegistry()

	opts := &JobOptions{
		MaxRetries: 3,
		Timeout:    time.Second,
	}

	registry.Register("test_job", func(ctx context.Context, job *senna.Job) error {
		return nil
	}, opts)

	job := senna.NewJob("test_job", nil)
	returnedOpts, err := registry.process(context.Background(), job)
	if err != nil {
		t.Fatalf("process failed: %v", err)
	}
	if returnedOpts == nil {
		t.Fatal("expected options to be returned")
	}
	if returnedOpts.MaxRetries != 3 {
		t.Errorf("expected MaxRetries 3, got %d", returnedOpts.MaxRetries)
	}
}

func TestHandlerRegistry_Use(t *testing.T) {
	registry := newHandlerRegistry()

	var order []string

	registry.Use(func(next senna.Handler) senna.Handler {
		return func(ctx context.Context, job *senna.Job) error {
			order = append(order, "mw1")
			return next(ctx, job)
		}
	})

	registry.Use(func(next senna.Handler) senna.Handler {
		return func(ctx context.Context, job *senna.Job) error {
			order = append(order, "mw2")
			return next(ctx, job)
		}
	})

	registry.Register("test_job", func(ctx context.Context, job *senna.Job) error {
		order = append(order, "handler")
		return nil
	}, nil)

	job := senna.NewJob("test_job", nil)
	_, _ = registry.process(context.Background(), job)

	expected := []string{"mw1", "mw2", "handler"}
	if len(order) != len(expected) {
		t.Fatalf("expected %d calls, got %d: %v", len(expected), len(order), order)
	}
	for i, v := range expected {
		if order[i] != v {
			t.Errorf("expected order[%d]='%s', got '%s'", i, v, order[i])
		}
	}
}

func TestHandlerRegistry_Process_Success(t *testing.T) {
	registry := newHandlerRegistry()

	result := ""
	registry.Register("test_job", func(ctx context.Context, job *senna.Job) error {
		result = job.Args["message"].(string)
		return nil
	}, nil)

	job := senna.NewJob("test_job", map[string]any{"message": "hello"})
	_, err := registry.process(context.Background(), job)
	if err != nil {
		t.Fatalf("process failed: %v", err)
	}
	if result != "hello" {
		t.Errorf("expected 'hello', got '%s'", result)
	}
}

func TestHandlerRegistry_Process_JobNotFound(t *testing.T) {
	registry := newHandlerRegistry()

	job := senna.NewJob("unknown_job", nil)
	_, err := registry.process(context.Background(), job)

	var notFoundErr *senna.JobNotFoundError
	if !errors.As(err, &notFoundErr) {
		t.Fatalf("expected JobNotFoundError, got %T: %v", err, err)
	}
}

func TestHandlerRegistry_Process_WithTimeout(t *testing.T) {
	registry := newHandlerRegistry()

	opts := &JobOptions{
		Timeout: 50 * time.Millisecond,
	}

	registry.Register("slow_job", func(ctx context.Context, job *senna.Job) error {
		time.Sleep(500 * time.Millisecond)
		return nil
	}, opts)

	job := senna.NewJob("slow_job", nil)
	_, err := registry.process(context.Background(), job)

	if err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestHandlerRegistry_Process_SetsProcessedAt(t *testing.T) {
	registry := newHandlerRegistry()

	registry.Register("test_job", func(ctx context.Context, job *senna.Job) error {
		return nil
	}, nil)

	job := senna.NewJob("test_job", nil)
	if job.ProcessedAt != nil {
		t.Error("ProcessedAt should be nil before processing")
	}

	before := time.Now()
	_, _ = registry.process(context.Background(), job)
	after := time.Now()

	if job.ProcessedAt == nil {
		t.Fatal("ProcessedAt should be set after processing")
	}
	if job.ProcessedAt.Before(before) || job.ProcessedAt.After(after) {
		t.Errorf("ProcessedAt should be between %v and %v", before, after)
	}
}

func TestHandlerRegistry_Process_WithMaxConcurrency(t *testing.T) {
	registry := newHandlerRegistry()

	opts := &JobOptions{
		MaxConcurrency: 2,
	}

	var maxConcurrent atomic.Int32
	var currentConcurrent atomic.Int32

	registry.Register("limited_job", func(ctx context.Context, job *senna.Job) error {
		current := currentConcurrent.Add(1)
		defer currentConcurrent.Add(-1)

		for {
			max := maxConcurrent.Load()
			if current <= max || maxConcurrent.CompareAndSwap(max, current) {
				break
			}
		}

		time.Sleep(50 * time.Millisecond)
		return nil
	}, opts)

	done := make(chan struct{})
	for range 10 {
		go func() {
			job := senna.NewJob("limited_job", nil)
			_, _ = registry.process(context.Background(), job)
			done <- struct{}{}
		}()
	}

	for range 10 {
		<-done
	}

	if maxConcurrent.Load() > 2 {
		t.Errorf("max concurrency exceeded limit, got %d", maxConcurrent.Load())
	}
}

func TestHandlerRegistry_Process_ReturnsError(t *testing.T) {
	registry := newHandlerRegistry()

	expectedErr := errors.New("job failed")
	registry.Register("failing_job", func(ctx context.Context, job *senna.Job) error {
		return expectedErr
	}, nil)

	job := senna.NewJob("failing_job", nil)
	_, err := registry.process(context.Background(), job)

	if err != expectedErr {
		t.Errorf("expected error to propagate, got %v", err)
	}
}

func TestHandlerRegistry_ConcurrentRegistration(t *testing.T) {
	registry := newHandlerRegistry()

	done := make(chan struct{})
	for i := range 10 {
		go func(id int) {
			registry.Register("job_"+string(rune('a'+id)), func(ctx context.Context, job *senna.Job) error {
				return nil
			}, nil)
			done <- struct{}{}
		}(i)
	}

	for range 10 {
		<-done
	}
}
