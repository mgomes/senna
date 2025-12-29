package senna

import (
	"context"
	"sync"
	"time"

	"github.com/mgomes/senna/ratelimit"
)

type workerPool struct {
	concurrency int
	handlers    map[string]handlerEntry
	middleware  []Middleware
	jobs        chan *Job
	wg          sync.WaitGroup
	mu          sync.RWMutex
}

type handlerEntry struct {
	handler Handler
	options *JobOptions
	sema    chan struct{}
}

type JobOptions struct {
	MaxRetries     int
	RetryBackoff   BackoffFunc
	Timeout        time.Duration
	MaxConcurrency int
	Unique         *UniqueConfig
	RateLimiter    ratelimit.Limiter
}

type UniqueConfig struct {
	Key string
	TTL time.Duration
}

func newWorkerPool(concurrency int) *workerPool {
	return &workerPool{
		concurrency: concurrency,
		handlers:    make(map[string]handlerEntry),
		jobs:        make(chan *Job, concurrency*2),
	}
}

func (p *workerPool) Register(jobType string, handler Handler, opts *JobOptions) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var sema chan struct{}
	if opts != nil && opts.MaxConcurrency > 0 {
		sema = make(chan struct{}, opts.MaxConcurrency)
	}

	p.handlers[jobType] = handlerEntry{
		handler: handler,
		options: opts,
		sema:    sema,
	}
}

func (p *workerPool) Use(mw ...Middleware) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.middleware = append(p.middleware, mw...)
}

func (p *workerPool) Start(ctx context.Context) {
	for i := 0; i < p.concurrency; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}
}

func (p *workerPool) worker(ctx context.Context, id int) {
	defer p.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-p.jobs:
			if !ok {
				return
			}
			_, _ = p.process(ctx, job)
		}
	}
}

func (p *workerPool) process(ctx context.Context, job *Job) (*JobOptions, error) {
	p.mu.RLock()
	entry, ok := p.handlers[job.Type]
	middleware := p.middleware
	p.mu.RUnlock()

	if !ok {
		return nil, &JobNotFoundError{JobID: job.ID}
	}

	handler := entry.handler
	if entry.options != nil && entry.options.Timeout > 0 {
		handler = TimeoutMiddleware(entry.options.Timeout)(handler)
	}
	if entry.options != nil && entry.options.RateLimiter != nil {
		handler = RateLimitMiddlewareWithReschedule(entry.options.RateLimiter)(handler)
	}

	if len(middleware) > 0 {
		handler = Chain(middleware...)(handler)
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

func (p *workerPool) Submit(job *Job) bool {
	select {
	case p.jobs <- job:
		return true
	default:
		return false
	}
}

func (p *workerPool) SubmitWait(ctx context.Context, job *Job) bool {
	select {
	case p.jobs <- job:
		return true
	case <-ctx.Done():
		return false
	}
}

func (p *workerPool) Stop() {
	close(p.jobs)
}

func (p *workerPool) Wait() {
	p.wg.Wait()
}

func (p *workerPool) Drain(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
