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
	handlers   map[string]handlerEntry
	middleware []senna.Middleware
	mu         sync.RWMutex
}

type handlerEntry struct {
	handler senna.Handler
	options *JobOptions
	sema    chan struct{}
}

type JobOptions struct {
	MaxRetries     int
	RetryBackoff   senna.BackoffFunc
	Timeout        time.Duration
	MaxConcurrency int
	Unique         *UniqueConfig
	RateLimiter    ratelimit.Limiter
}

type UniqueConfig struct {
	Key string
	TTL time.Duration
}

func newHandlerRegistry() *handlerRegistry {
	return &handlerRegistry{
		handlers: make(map[string]handlerEntry),
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

func (r *handlerRegistry) Use(mw ...senna.Middleware) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.middleware = append(r.middleware, mw...)
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
		handler = rateLimitMiddlewareWithReschedule(entry.options.RateLimiter)(handler)
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

func rateLimitMiddlewareWithReschedule(limiter ratelimit.Limiter) senna.Middleware {
	return func(next senna.Handler) senna.Handler {
		return func(ctx context.Context, job *senna.Job) error {
			waitTime, err := limiter.Acquire(ctx)
			if err != nil {
				return err
			}
			if waitTime > 0 {
				return &senna.RetryableError{
					Job:     job,
					Cause:   &ratelimit.OverLimitError{LimiterName: limiter.Name(), LimiterType: "unknown", RetryIn: waitTime},
					RetryIn: waitTime,
				}
			}
			defer func() { _ = limiter.Release(ctx) }()
			return next(ctx, job)
		}
	}
}
