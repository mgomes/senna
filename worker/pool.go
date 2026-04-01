package worker

import (
	"context"
	"sync"
	"time"

	"github.com/mgomes/senna"
	"github.com/mgomes/senna/ratelimit"
)

// handlerRegistry manages job handlers and middleware.
// It's a simplified pool that just dispatches jobs to handlers.
type handlerRegistry struct {
	handlers         map[string]handlerEntry
	iterableHandlers map[string]iterableHandlerEntry
	middleware       []senna.Middleware
	mu               sync.RWMutex
}

type handlerEntry struct {
	handler senna.Handler
	options *JobOptions
	sema    chan struct{}
}

type iterableHandlerEntry struct {
	handler senna.IterableHandler
	options *IterableJobOptions
}

// JobOptions describes execution settings derived from JobOption values.
type JobOptions struct {
	MaxRetries     int
	RetryBackoff   senna.BackoffFunc
	Timeout        time.Duration
	MaxConcurrency int
	Unique         *UniqueConfig
	RateLimiter    ratelimit.Limiter
}

// UniqueConfig stores uniqueness metadata for a job type.
type UniqueConfig struct {
	Key string
	TTL time.Duration
}

func newHandlerRegistry() *handlerRegistry {
	return &handlerRegistry{
		handlers:         make(map[string]handlerEntry),
		iterableHandlers: make(map[string]iterableHandlerEntry),
	}
}

func (r *handlerRegistry) Register(jobType string, handler senna.Handler, opts *JobOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var sema chan struct{}
	if opts != nil && opts.MaxConcurrency > 0 {
		sema = make(chan struct{}, opts.MaxConcurrency)
	}

	r.handlers[jobType] = handlerEntry{
		handler: handler,
		options: opts,
		sema:    sema,
	}
}

func (r *handlerRegistry) RegisterIterable(jobType string, handler senna.IterableHandler, opts *IterableJobOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.iterableHandlers[jobType] = iterableHandlerEntry{
		handler: handler,
		options: opts,
	}
}

// GetIterable returns the iterable handler for a job type, if registered.
func (r *handlerRegistry) GetIterable(jobType string) (senna.IterableHandler, *IterableJobOptions, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entry, ok := r.iterableHandlers[jobType]
	if !ok {
		return nil, nil, false
	}
	return entry.handler, entry.options, true
}

func (r *handlerRegistry) Use(mw ...senna.Middleware) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.middleware = append(r.middleware, mw...)
}

func (r *handlerRegistry) middlewareChain() []senna.Middleware {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.middleware) == 0 {
		return nil
	}
	chain := make([]senna.Middleware, len(r.middleware))
	copy(chain, r.middleware)
	return chain
}

func (r *handlerRegistry) process(ctx context.Context, job *senna.Job) (*JobOptions, error) {
	r.mu.RLock()
	entry, ok := r.handlers[job.Type]
	middleware := r.middleware
	r.mu.RUnlock()

	if !ok {
		return nil, &senna.JobNotFoundError{JobID: job.ID}
	}

	handler := entry.handler
	if entry.options != nil && entry.options.Timeout > 0 {
		handler = senna.TimeoutMiddleware(entry.options.Timeout)(handler)
	}
	if entry.options != nil && entry.options.RateLimiter != nil {
		handler = senna.RateLimitMiddlewareWithReschedule(entry.options.RateLimiter)(handler)
	}

	if len(middleware) > 0 {
		handler = senna.Chain(middleware...)(handler)
	}

	if entry.sema != nil {
		select {
		case entry.sema <- struct{}{}:
			defer func() { <-entry.sema }()
		case <-ctx.Done():
			return entry.options, ctx.Err()
		}
	}

	now := time.Now()
	job.ProcessedAt = &now

	return entry.options, handler(ctx, job)
}
