package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgomes/senna"
	"github.com/mgomes/senna/internal/keys"
	"github.com/mgomes/senna/ratelimit"
	"github.com/redis/go-redis/v9"
)

type testRateLimiter struct {
	acquired atomic.Int32
	released atomic.Int32
}

func (l *testRateLimiter) WithinLimit(ctx context.Context, fn func() error) error {
	return fn()
}

func (l *testRateLimiter) Acquire(ctx context.Context) (ratelimit.Lease, time.Duration, error) {
	l.acquired.Add(1)
	return testRateLimiterLease{limiter: l}, 0, nil
}

type testRateLimiterLease struct {
	limiter *testRateLimiter
}

func (l testRateLimiterLease) Release(ctx context.Context) error {
	l.limiter.released.Add(1)
	return nil
}

func (l *testRateLimiter) Name() string {
	return "test-rate-limiter"
}

// testIterableHandler is a simple iterable handler for testing
type testIterableHandler struct {
	items     []any
	processed []any
	mu        sync.Mutex
}

func newTestIterableHandler(items []any) *testIterableHandler {
	return &testIterableHandler{items: items}
}

func (h *testIterableHandler) BuildIterator(ctx context.Context, job *senna.Job, cursor senna.Cursor) (senna.Iterator, error) {
	offset := 0
	if cursor != nil {
		offset, _ = senna.CursorTo[int](cursor)
	}
	return senna.SliceIterator(h.items, offset), nil
}

func (h *testIterableHandler) ProcessItem(ctx context.Context, job *senna.Job, item any) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.processed = append(h.processed, item)
	return nil
}

func (h *testIterableHandler) Processed() []any {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := make([]any, len(h.processed))
	copy(result, h.processed)
	return result
}

type closeErrorIterator struct {
	senna.Iterator
	err error
}

func (it closeErrorIterator) Close() error {
	return it.err
}

type closeErrorIterableHandler struct {
	*testIterableHandler
	closeErr error
}

func (h *closeErrorIterableHandler) BuildIterator(ctx context.Context, job *senna.Job, cursor senna.Cursor) (senna.Iterator, error) {
	iter, err := h.testIterableHandler.BuildIterator(ctx, job, cursor)
	if err != nil {
		return nil, err
	}
	return closeErrorIterator{Iterator: iter, err: h.closeErr}, nil
}

type instrumentedIterableHandler struct {
	*testIterableHandler
	afterProcess func()
	processErr   error
}

func (h *instrumentedIterableHandler) ProcessItem(ctx context.Context, job *senna.Job, item any) error {
	if err := h.testIterableHandler.ProcessItem(ctx, job, item); err != nil {
		return err
	}
	if h.afterProcess != nil {
		h.afterProcess()
	}
	return h.processErr
}

func TestIterable_ProcessAll(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-iterable:*")

	k := keys.New("test-iterable")

	handler := newTestIterableHandler([]any{1, 2, 3, 4, 5})

	w := &Worker{
		id:       "worker-1",
		redis:    client,
		keys:     k,
		handlers: newHandlerRegistry(),
		fetcher:  newFetcher(client, k, []senna.QueueConfig{{Name: "default", Priority: 1}}, 100*time.Millisecond, false),
	}

	w.RegisterIterable("test_iterable", handler)

	ctx := context.Background()

	// Create a job
	job := senna.NewJob("test_iterable", nil)
	job.Queue = "default"
	data, _ := job.Marshal()
	client.LPush(ctx, k.Queue("default"), string(data))

	// Process the job
	fetched, err := w.fetcher.Fetch(ctx, w.id)
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if fetched == nil {
		t.Fatal("expected job, got nil")
	}

	iterHandler, iterOpts, ok := w.handlers.GetIterable(fetched.Type)
	if !ok {
		t.Fatal("expected iterable handler")
	}

	err = w.processIterable(ctx, fetched, iterHandler, iterOpts)
	if err != nil {
		t.Fatalf("processIterable failed: %v", err)
	}

	// Check all items were processed
	processed := handler.Processed()
	if len(processed) != 5 {
		t.Errorf("processed %d items, want 5", len(processed))
	}

	// Check iteration state was deleted (job completed)
	exists, _ := client.Exists(ctx, k.IterationState(job.ID)).Result()
	if exists != 0 {
		t.Error("iteration state should be deleted on completion")
	}
}

func TestIterable_CloseErrorPreventsCompletion(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-iterable-close:*")

	k := keys.New("test-iterable-close")
	closeErr := errors.New("close failed")
	handler := &closeErrorIterableHandler{
		testIterableHandler: newTestIterableHandler([]any{1, 2}),
		closeErr:            closeErr,
	}

	w := &Worker{
		id:    "worker-1",
		redis: client,
		keys:  k,
	}

	ctx := context.Background()
	job := senna.NewJob("test_iterable", nil)

	var completeCalled atomic.Bool
	opts := &IterableJobOptions{
		CursorSaveInterval: defaultCursorSaveInterval,
		MaxRetries:         senna.DefaultRetryCount,
		RetryBackoff:       senna.DefaultBackoff(),
		Callbacks: &senna.IterableCallbacks{
			OnComplete: func(ctx context.Context, job *senna.Job, state *senna.IterationState) error {
				completeCalled.Store(true)
				return nil
			},
		},
	}

	err := w.processIterable(ctx, job, handler, opts)
	if !errors.Is(err, closeErr) {
		t.Fatalf("processIterable() error = %v, want close error %v", err, closeErr)
	}
	if completeCalled.Load() {
		t.Fatal("OnComplete called despite iterator close failure")
	}

	state, err := w.loadIterationState(ctx, k.IterationState(job.ID))
	if err != nil {
		t.Fatalf("loadIterationState() error = %v, want nil", err)
	}
	if state == nil {
		t.Fatal("iteration state = nil, want saved state after close failure")
	}
	if state.TotalItems != 2 {
		t.Errorf("iteration state total items = %d, want 2", state.TotalItems)
	}
}

func TestIterable_SaveErrorOnItemErrorIsReturned(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-iterable-save-item-error:*")

	k := keys.New("test-iterable-save-item-error")
	processErr := errors.New("process failed")
	handler := &instrumentedIterableHandler{
		testIterableHandler: newTestIterableHandler([]any{1}),
		processErr:          processErr,
		afterProcess: func() {
			if err := client.Close(); err != nil {
				t.Fatalf("Client.Close() error = %v, want nil", err)
			}
		},
	}

	w := &Worker{
		id:    "worker-1",
		redis: client,
		keys:  k,
	}

	ctx := context.Background()
	job := senna.NewJob("test_iterable", nil)
	opts := &IterableJobOptions{
		CursorSaveInterval: defaultCursorSaveInterval,
		MaxRetries:         senna.DefaultRetryCount,
		RetryBackoff:       senna.DefaultBackoff(),
	}

	err := w.processIterable(ctx, job, handler, opts)
	if !errors.Is(err, processErr) {
		t.Fatalf("processIterable() error = %v, want process error %v", err, processErr)
	}
	if !errors.Is(err, redis.ErrClosed) {
		t.Fatalf("processIterable() error = %v, want Redis close error", err)
	}
}

func TestIterable_SaveErrorBeforeCompletionPreventsOnComplete(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-iterable-save-complete:*")

	k := keys.New("test-iterable-save-complete")
	handler := &instrumentedIterableHandler{
		testIterableHandler: newTestIterableHandler([]any{1}),
		afterProcess: func() {
			if err := client.Close(); err != nil {
				t.Fatalf("Client.Close() error = %v, want nil", err)
			}
		},
	}

	w := &Worker{
		id:    "worker-1",
		redis: client,
		keys:  k,
	}

	var completeCalled atomic.Bool
	opts := &IterableJobOptions{
		CursorSaveInterval: defaultCursorSaveInterval,
		MaxRetries:         senna.DefaultRetryCount,
		RetryBackoff:       senna.DefaultBackoff(),
		Callbacks: &senna.IterableCallbacks{
			OnComplete: func(ctx context.Context, job *senna.Job, state *senna.IterationState) error {
				completeCalled.Store(true)
				return nil
			},
		},
	}

	err := w.processIterable(context.Background(), senna.NewJob("test_iterable", nil), handler, opts)
	if !errors.Is(err, redis.ErrClosed) {
		t.Fatalf("processIterable() error = %v, want Redis close error", err)
	}
	if completeCalled.Load() {
		t.Fatal("OnComplete called after completion state save failed")
	}
}

func TestIterable_SaveErrorOnInterruptDoesNotReturnInterrupted(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-iterable-save-interrupt:*")

	k := keys.New("test-iterable-save-interrupt")
	handler := &instrumentedIterableHandler{
		testIterableHandler: newTestIterableHandler([]any{1, 2}),
		afterProcess: func() {
			if err := client.Close(); err != nil {
				t.Fatalf("Client.Close() error = %v, want nil", err)
			}
		},
	}

	w := &Worker{
		id:    "worker-1",
		redis: client,
		keys:  k,
	}

	var stopCalled atomic.Bool
	opts := &IterableJobOptions{
		CursorSaveInterval: defaultCursorSaveInterval,
		MaxItemsPerRun:     1,
		MaxRetries:         senna.DefaultRetryCount,
		RetryBackoff:       senna.DefaultBackoff(),
		Callbacks: &senna.IterableCallbacks{
			OnStop: func(ctx context.Context, job *senna.Job, state *senna.IterationState) error {
				stopCalled.Store(true)
				return nil
			},
		},
	}

	err := w.processIterable(context.Background(), senna.NewJob("test_iterable", nil), handler, opts)
	if !errors.Is(err, redis.ErrClosed) {
		t.Fatalf("processIterable() error = %v, want Redis close error", err)
	}
	if isInterruptedError(err) {
		t.Fatalf("processIterable() error = %v, want non-interrupted save failure", err)
	}
	if stopCalled.Load() {
		t.Fatal("OnStop called after interrupt state save failed")
	}
}

func TestIterable_MaxItemsAfterCancellationReturnsInterrupted(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-iterable-cancelled-max-items:*")

	k := keys.New("test-iterable-cancelled-max-items")
	ctx, cancel := context.WithCancel(context.Background())
	handler := &instrumentedIterableHandler{
		testIterableHandler: newTestIterableHandler([]any{1, 2}),
		afterProcess:        cancel,
	}

	w := &Worker{
		id:    "worker-1",
		redis: client,
		keys:  k,
	}

	var stopCalled atomic.Bool
	opts := &IterableJobOptions{
		CursorSaveInterval: defaultCursorSaveInterval,
		MaxItemsPerRun:     1,
		MaxRetries:         senna.DefaultRetryCount,
		RetryBackoff:       senna.DefaultBackoff(),
		Callbacks: &senna.IterableCallbacks{
			OnStop: func(ctx context.Context, job *senna.Job, state *senna.IterationState) error {
				stopCalled.Store(true)
				return nil
			},
		},
	}
	job := senna.NewJob("test_iterable", nil)

	err := w.processIterable(ctx, job, handler, opts)
	if !isInterruptedError(err) {
		t.Fatalf("processIterable() error = %v, want InterruptedError", err)
	}
	if !stopCalled.Load() {
		t.Fatal("OnStop not called after cancelled max-items interruption")
	}

	state, err := w.loadIterationState(context.Background(), k.IterationState(job.ID))
	if err != nil {
		t.Fatalf("loadIterationState() error = %v, want nil", err)
	}
	if state == nil {
		t.Fatal("iteration state = nil, want saved state")
	}
	cursor, err := senna.CursorTo[int](state.Cursor)
	if err != nil {
		t.Fatalf("CursorTo[int](%v) error = %v, want nil", state.Cursor, err)
	}
	if cursor != 1 {
		t.Errorf("cursor = %d, want 1", cursor)
	}
	if state.TotalItems != 1 {
		t.Errorf("total items = %d, want 1", state.TotalItems)
	}
}

func TestIterable_PeriodicCheckpointAfterCancellationReturnsInterrupted(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-iterable-cancelled-checkpoint:*")

	k := keys.New("test-iterable-cancelled-checkpoint")
	ctx, cancel := context.WithCancel(context.Background())
	handler := &instrumentedIterableHandler{
		testIterableHandler: newTestIterableHandler([]any{1, 2}),
		afterProcess: func() {
			cancel()
			time.Sleep(5 * time.Millisecond)
		},
	}

	w := &Worker{
		id:    "worker-1",
		redis: client,
		keys:  k,
	}

	var stopCalled atomic.Bool
	opts := &IterableJobOptions{
		CursorSaveInterval: time.Millisecond,
		MaxRetries:         senna.DefaultRetryCount,
		RetryBackoff:       senna.DefaultBackoff(),
		Callbacks: &senna.IterableCallbacks{
			OnStop: func(ctx context.Context, job *senna.Job, state *senna.IterationState) error {
				stopCalled.Store(true)
				return nil
			},
		},
	}
	job := senna.NewJob("test_iterable", nil)

	err := w.processIterable(ctx, job, handler, opts)
	if !isInterruptedError(err) {
		t.Fatalf("processIterable() error = %v, want InterruptedError", err)
	}
	if !stopCalled.Load() {
		t.Fatal("OnStop not called after cancelled checkpoint interruption")
	}

	state, err := w.loadIterationState(context.Background(), k.IterationState(job.ID))
	if err != nil {
		t.Fatalf("loadIterationState() error = %v, want nil", err)
	}
	if state == nil {
		t.Fatal("iteration state = nil, want saved state")
	}
	cursor, err := senna.CursorTo[int](state.Cursor)
	if err != nil {
		t.Fatalf("CursorTo[int](%v) error = %v, want nil", state.Cursor, err)
	}
	if cursor != 1 {
		t.Errorf("cursor = %d, want 1", cursor)
	}
	if state.TotalItems != 1 {
		t.Errorf("total items = %d, want 1", state.TotalItems)
	}
}

func TestIterable_CompletionAfterCancellationUsesLiveContext(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-iterable-cancelled-completion:*")

	k := keys.New("test-iterable-cancelled-completion")
	ctx, cancel := context.WithCancel(context.Background())
	handler := &instrumentedIterableHandler{
		testIterableHandler: newTestIterableHandler([]any{1}),
		afterProcess:        cancel,
	}

	w := &Worker{
		id:    "worker-1",
		redis: client,
		keys:  k,
	}

	var completeCalled atomic.Bool
	opts := &IterableJobOptions{
		CursorSaveInterval: defaultCursorSaveInterval,
		MaxRetries:         senna.DefaultRetryCount,
		RetryBackoff:       senna.DefaultBackoff(),
		Callbacks: &senna.IterableCallbacks{
			OnComplete: func(ctx context.Context, job *senna.Job, state *senna.IterationState) error {
				completeCalled.Store(true)
				return nil
			},
		},
	}
	job := senna.NewJob("test_iterable", nil)

	if err := w.processIterable(ctx, job, handler, opts); err != nil {
		t.Fatalf("processIterable() error = %v, want nil", err)
	}
	if !completeCalled.Load() {
		t.Fatal("OnComplete not called after cancelled terminal checkpoint")
	}

	exists, err := client.Exists(context.Background(), k.IterationState(job.ID)).Result()
	if err != nil {
		t.Fatalf("Exists(%q) error = %v, want nil", k.IterationState(job.ID), err)
	}
	if exists != 0 {
		t.Errorf("Exists(%q) = %d, want 0", k.IterationState(job.ID), exists)
	}
}

func TestIterable_MiddlewareAndRateLimiter(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-iterable-mw:*")

	k := keys.New("test-iterable-mw")
	handler := newTestIterableHandler([]any{1})

	w := &Worker{
		id:       "worker-1",
		redis:    client,
		keys:     k,
		handlers: newHandlerRegistry(),
		fetcher:  newFetcher(client, k, []senna.QueueConfig{{Name: "default", Priority: 1}}, 100*time.Millisecond, false),
	}

	var middlewareCalled atomic.Bool
	w.Use(func(next senna.Handler) senna.Handler {
		return func(ctx context.Context, job *senna.Job) error {
			middlewareCalled.Store(true)
			return next(ctx, job)
		}
	})

	limiter := &testRateLimiter{}
	w.RegisterIterable("test_iterable", handler, WithIterableRateLimiter(limiter))

	job := senna.NewJob("test_iterable", nil)
	job.Queue = "default"

	ctx := context.Background()
	w.processJob(ctx, job)

	if !middlewareCalled.Load() {
		t.Fatal("expected middleware to be applied for iterable job")
	}
	if limiter.acquired.Load() != 1 {
		t.Fatalf("expected rate limiter acquire once, got %d", limiter.acquired.Load())
	}
}

func TestIterable_CursorPersistence(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-iterable-cursor:*")

	k := keys.New("test-iterable-cursor")

	handler := newTestIterableHandler([]any{1, 2, 3, 4, 5})

	w := &Worker{
		id:       "worker-1",
		redis:    client,
		keys:     k,
		handlers: newHandlerRegistry(),
		fetcher:  newFetcher(client, k, []senna.QueueConfig{{Name: "default", Priority: 1}}, 100*time.Millisecond, false),
	}

	w.RegisterIterable("test_iterable", handler,
		WithCursorSaveInterval(10*time.Millisecond),
		WithMaxItemsPerRun(2), // Process only 2 items per run
	)

	ctx := context.Background()

	// Create a job
	job := senna.NewJob("test_iterable", nil)
	job.Queue = "default"
	data, _ := job.Marshal()
	client.LPush(ctx, k.Queue("default"), string(data))

	// Fetch and process first run (should process 2 items)
	fetched, _ := w.fetcher.Fetch(ctx, w.id)
	iterHandler, iterOpts, _ := w.handlers.GetIterable(fetched.Type)

	err := w.processIterable(ctx, fetched, iterHandler, iterOpts)
	if err == nil {
		t.Fatal("expected InterruptedError, got nil")
	}

	if !isInterruptedError(err) {
		t.Fatalf("expected InterruptedError, got %T: %v", err, err)
	}

	// Check state was saved
	state, err := w.loadIterationState(ctx, k.IterationState(fetched.ID))
	if err != nil {
		t.Fatalf("loadIterationState failed: %v", err)
	}
	if state == nil {
		t.Fatal("expected iteration state to be saved")
	}

	// Check cursor position
	cursorPos, _ := senna.CursorTo[int](state.Cursor)
	if cursorPos != 2 {
		t.Errorf("cursor position = %d, want 2", cursorPos)
	}

	if state.TotalItems != 2 {
		t.Errorf("total items = %d, want 2", state.TotalItems)
	}

	// Process remaining items with a fresh handler to verify cursor resumption
	handler2 := newTestIterableHandler([]any{1, 2, 3, 4, 5})
	w.handlers.RegisterIterable("test_iterable", handler2, &IterableJobOptions{
		CursorSaveInterval: 10 * time.Millisecond,
		MaxItemsPerRun:     2,
	})

	// Simulate requeue by pushing the same job back
	client.RPush(ctx, k.Queue("default"), string(data))

	// Fetch and process second run
	fetched2, _ := w.fetcher.Fetch(ctx, w.id)
	iterHandler2, iterOpts2, _ := w.handlers.GetIterable(fetched2.Type)

	// Note: Need to use same job ID for cursor lookup
	fetched2.ID = fetched.ID

	err = w.processIterable(ctx, fetched2, iterHandler2, iterOpts2)
	if !isInterruptedError(err) {
		t.Fatalf("expected InterruptedError, got %T: %v", err, err)
	}

	// Check state shows progress
	state2, _ := w.loadIterationState(ctx, k.IterationState(fetched.ID))
	if state2.TotalItems != 4 {
		t.Errorf("total items after run 2 = %d, want 4", state2.TotalItems)
	}

	// Third run should complete
	handler3 := newTestIterableHandler([]any{1, 2, 3, 4, 5})
	w.handlers.RegisterIterable("test_iterable", handler3, &IterableJobOptions{
		CursorSaveInterval: 10 * time.Millisecond,
		MaxItemsPerRun:     2,
	})

	client.RPush(ctx, k.Queue("default"), string(data))
	fetched3, _ := w.fetcher.Fetch(ctx, w.id)
	iterHandler3, iterOpts3, _ := w.handlers.GetIterable(fetched3.Type)
	fetched3.ID = fetched.ID

	err = w.processIterable(ctx, fetched3, iterHandler3, iterOpts3)
	if err != nil {
		t.Fatalf("expected completion, got error: %v", err)
	}

	// State should be deleted after completion
	exists, _ := client.Exists(ctx, k.IterationState(fetched.ID)).Result()
	if exists != 0 {
		t.Error("iteration state should be deleted on completion")
	}
}

func isInterruptedError(err error) bool {
	var interruptedErr *senna.InterruptedError
	return errors.As(err, &interruptedErr)
}

func TestIterable_Callbacks(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-iterable-cb:*")

	k := keys.New("test-iterable-cb")

	handler := newTestIterableHandler([]any{1, 2, 3})

	var callbackOrder []string
	var mu sync.Mutex

	callbacks := &senna.IterableCallbacks{
		OnStart: func(ctx context.Context, job *senna.Job) error {
			mu.Lock()
			callbackOrder = append(callbackOrder, "start")
			mu.Unlock()
			return nil
		},
		OnComplete: func(ctx context.Context, job *senna.Job, state *senna.IterationState) error {
			mu.Lock()
			callbackOrder = append(callbackOrder, "complete")
			mu.Unlock()
			return nil
		},
	}

	w := &Worker{
		id:       "worker-1",
		redis:    client,
		keys:     k,
		handlers: newHandlerRegistry(),
		fetcher:  newFetcher(client, k, []senna.QueueConfig{{Name: "default", Priority: 1}}, 100*time.Millisecond, false),
	}

	w.RegisterIterable("test_iterable", handler, WithIterableCallbacks(callbacks))

	ctx := context.Background()

	job := senna.NewJob("test_iterable", nil)
	job.Queue = "default"
	data, _ := job.Marshal()
	client.LPush(ctx, k.Queue("default"), string(data))

	fetched, _ := w.fetcher.Fetch(ctx, w.id)
	iterHandler, iterOpts, _ := w.handlers.GetIterable(fetched.Type)

	err := w.processIterable(ctx, fetched, iterHandler, iterOpts)
	if err != nil {
		t.Fatalf("processIterable failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(callbackOrder) != 2 {
		t.Errorf("expected 2 callbacks, got %d", len(callbackOrder))
	}
	if callbackOrder[0] != "start" {
		t.Errorf("first callback = %s, want start", callbackOrder[0])
	}
	if callbackOrder[1] != "complete" {
		t.Errorf("second callback = %s, want complete", callbackOrder[1])
	}
}

func TestIterable_SkipItem(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-iterable-skip:*")

	k := keys.New("test-iterable-skip")

	// Handler that skips even numbers
	handler := &skipEvenHandler{items: []int{1, 2, 3, 4, 5}}

	w := &Worker{
		id:       "worker-1",
		redis:    client,
		keys:     k,
		handlers: newHandlerRegistry(),
		fetcher:  newFetcher(client, k, []senna.QueueConfig{{Name: "default", Priority: 1}}, 100*time.Millisecond, false),
	}

	w.RegisterIterable("test_iterable", handler)

	ctx := context.Background()

	job := senna.NewJob("test_iterable", nil)
	job.Queue = "default"
	data, _ := job.Marshal()
	client.LPush(ctx, k.Queue("default"), string(data))

	fetched, _ := w.fetcher.Fetch(ctx, w.id)
	iterHandler, iterOpts, _ := w.handlers.GetIterable(fetched.Type)

	err := w.processIterable(ctx, fetched, iterHandler, iterOpts)
	if err != nil {
		t.Fatalf("processIterable failed: %v", err)
	}

	// Only odd numbers should be processed (1, 3, 5)
	if len(handler.processed) != 3 {
		t.Errorf("processed %d items, want 3", len(handler.processed))
	}
}

type skipEvenHandler struct {
	items     []int
	processed []int
	mu        sync.Mutex
}

func (h *skipEvenHandler) BuildIterator(ctx context.Context, job *senna.Job, cursor senna.Cursor) (senna.Iterator, error) {
	offset := 0
	if cursor != nil {
		offset, _ = senna.CursorTo[int](cursor)
	}
	return senna.SliceIterator(h.items, offset), nil
}

func (h *skipEvenHandler) ProcessItem(ctx context.Context, job *senna.Job, item any) error {
	n := item.(int)
	if n%2 == 0 {
		return &senna.SkipItemError{Reason: "even number"}
	}
	h.mu.Lock()
	h.processed = append(h.processed, n)
	h.mu.Unlock()
	return nil
}

func TestIterable_StopEarly(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-iterable-stop:*")

	k := keys.New("test-iterable-stop")

	// Handler that stops when it finds 3
	handler := &stopAtHandler{items: []int{1, 2, 3, 4, 5}, stopAt: 3}

	w := &Worker{
		id:       "worker-1",
		redis:    client,
		keys:     k,
		handlers: newHandlerRegistry(),
		fetcher:  newFetcher(client, k, []senna.QueueConfig{{Name: "default", Priority: 1}}, 100*time.Millisecond, false),
	}

	w.RegisterIterable("test_iterable", handler)

	ctx := context.Background()

	job := senna.NewJob("test_iterable", nil)
	job.Queue = "default"
	data, _ := job.Marshal()
	client.LPush(ctx, k.Queue("default"), string(data))

	fetched, _ := w.fetcher.Fetch(ctx, w.id)
	iterHandler, iterOpts, _ := w.handlers.GetIterable(fetched.Type)

	err := w.processIterable(ctx, fetched, iterHandler, iterOpts)
	if err != nil {
		t.Fatalf("processIterable failed: %v", err)
	}

	// Should have processed 1, 2, then stopped at 3
	if len(handler.processed) != 2 {
		t.Errorf("processed %d items, want 2", len(handler.processed))
	}

	// State should be deleted (completed successfully)
	exists, _ := client.Exists(ctx, k.IterationState(fetched.ID)).Result()
	if exists != 0 {
		t.Error("iteration state should be deleted on completion")
	}
}

type stopAtHandler struct {
	items     []int
	stopAt    int
	processed []int
	mu        sync.Mutex
}

func (h *stopAtHandler) BuildIterator(ctx context.Context, job *senna.Job, cursor senna.Cursor) (senna.Iterator, error) {
	offset := 0
	if cursor != nil {
		offset, _ = senna.CursorTo[int](cursor)
	}
	return senna.SliceIterator(h.items, offset), nil
}

func (h *stopAtHandler) ProcessItem(ctx context.Context, job *senna.Job, item any) error {
	n := item.(int)
	if n == h.stopAt {
		return &senna.StopIterationError{Reason: "found target"}
	}
	h.mu.Lock()
	h.processed = append(h.processed, n)
	h.mu.Unlock()
	return nil
}

func TestIterable_Cancellation(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-iterable-cancel:*")

	k := keys.New("test-iterable-cancel")

	handler := newTestIterableHandler([]any{1, 2, 3, 4, 5})

	var cancelCalled, stopCalled bool
	callbacks := &senna.IterableCallbacks{
		OnCancel: func(ctx context.Context, job *senna.Job, state *senna.IterationState) error {
			cancelCalled = true
			return nil
		},
		OnStop: func(ctx context.Context, job *senna.Job, state *senna.IterationState) error {
			stopCalled = true
			return nil
		},
	}

	w := &Worker{
		id:       "worker-1",
		redis:    client,
		keys:     k,
		handlers: newHandlerRegistry(),
		fetcher:  newFetcher(client, k, []senna.QueueConfig{{Name: "default", Priority: 1}}, 100*time.Millisecond, false),
	}

	w.RegisterIterable("test_iterable", handler, WithIterableCallbacks(callbacks))

	ctx := context.Background()

	job := senna.NewJob("test_iterable", nil)
	job.Queue = "default"
	stateKey := k.IterationState(job.ID)

	// Pre-create cancelled state
	cancelledState := &senna.IterationState{
		JobID:     job.ID,
		Cancelled: true,
	}
	_ = w.saveIterationState(ctx, stateKey, cancelledState)

	data, _ := job.Marshal()
	client.LPush(ctx, k.Queue("default"), string(data))

	fetched, _ := w.fetcher.Fetch(ctx, w.id)
	iterHandler, iterOpts, _ := w.handlers.GetIterable(fetched.Type)

	err := w.processIterable(ctx, fetched, iterHandler, iterOpts)
	if err != nil {
		t.Fatalf("processIterable should return nil for cancelled job, got: %v", err)
	}

	if !cancelCalled {
		t.Error("OnCancel callback not called")
	}
	if !stopCalled {
		t.Error("OnStop callback not called")
	}

	// No items should be processed (cancelled before first item)
	processed := handler.Processed()
	if len(processed) != 0 {
		t.Errorf("processed %d items, want 0", len(processed))
	}
}

func TestIterable_Interruption(t *testing.T) {
	client := newTestRedisClient(t)
	flushTestKeys(t, client, "test-iterable-interrupt:*")

	k := keys.New("test-iterable-interrupt")

	// Handler that cancels context after processing first item
	ctx, cancel := context.WithCancel(context.Background())
	handler := &interruptingHandler{
		items:  []int{1, 2, 3, 4, 5},
		cancel: cancel,
	}

	var stopCalled bool
	callbacks := &senna.IterableCallbacks{
		OnStop: func(ctx context.Context, job *senna.Job, state *senna.IterationState) error {
			stopCalled = true
			return nil
		},
	}

	w := &Worker{
		id:       "worker-1",
		redis:    client,
		keys:     k,
		handlers: newHandlerRegistry(),
		fetcher:  newFetcher(client, k, []senna.QueueConfig{{Name: "default", Priority: 1}}, 100*time.Millisecond, false),
	}

	w.RegisterIterable("test_iterable", handler, WithIterableCallbacks(callbacks))

	job := senna.NewJob("test_iterable", nil)
	job.Queue = "default"
	data, _ := job.Marshal()
	client.LPush(context.Background(), k.Queue("default"), string(data))

	fetched, _ := w.fetcher.Fetch(context.Background(), w.id)
	iterHandler, iterOpts, _ := w.handlers.GetIterable(fetched.Type)

	err := w.processIterable(ctx, fetched, iterHandler, iterOpts)
	if !isInterruptedError(err) {
		t.Fatalf("expected InterruptedError, got %T: %v", err, err)
	}

	if !stopCalled {
		t.Error("OnStop callback not called")
	}

	// State should be saved
	state, _ := w.loadIterationState(context.Background(), k.IterationState(fetched.ID))
	if state == nil {
		t.Error("expected iteration state to be saved on interruption")
	}

	// Should have processed at least one item before interruption
	if len(handler.processed) < 1 {
		t.Errorf("processed %d items, want at least 1", len(handler.processed))
	}
}

type interruptingHandler struct {
	items     []int
	processed []int
	cancel    context.CancelFunc
	mu        sync.Mutex
}

func (h *interruptingHandler) BuildIterator(ctx context.Context, job *senna.Job, cursor senna.Cursor) (senna.Iterator, error) {
	offset := 0
	if cursor != nil {
		offset, _ = senna.CursorTo[int](cursor)
	}
	return senna.SliceIterator(h.items, offset), nil
}

func (h *interruptingHandler) ProcessItem(ctx context.Context, job *senna.Job, item any) error {
	h.mu.Lock()
	h.processed = append(h.processed, item.(int))
	count := len(h.processed)
	h.mu.Unlock()

	// Cancel context after first item
	if count == 1 {
		h.cancel()
	}
	return nil
}
