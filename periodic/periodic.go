package periodic

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/mgomes/senna"
	"github.com/mgomes/senna/internal/keys"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
)

// Job represents a periodic job that runs on a schedule.
type Job struct {
	Name     string
	Schedule string
	JobType  string
	Args     map[string]any
	Queue    string
	Retry    int

	schedule cron.Schedule
}

// Option configures a periodic job.
type Option func(*Job)

// WithQueue sets the queue for enqueued jobs.
func WithQueue(queue string) Option {
	return func(j *Job) {
		j.Queue = queue
	}
}

// WithRetry sets the retry count for enqueued jobs.
func WithRetry(n int) Option {
	return func(j *Job) {
		j.Retry = n
	}
}

// WithArgs sets the arguments for enqueued jobs.
func WithArgs(args map[string]any) Option {
	return func(j *Job) {
		j.Args = args
	}
}

// Scheduler manages periodic jobs and ensures only one worker enqueues each job.
type Scheduler struct {
	redis    redis.Cmdable
	keys     *keys.Keys
	jobs     []*Job
	parser   cron.Parser
	interval time.Duration
	mu       sync.RWMutex
	stopCh   chan struct{}
	done     chan struct{}
}

// NewScheduler creates a new periodic job scheduler.
func NewScheduler(redis redis.Cmdable, k *keys.Keys) *Scheduler {
	return &Scheduler{
		redis: redis,
		keys:  k,
		jobs:  make([]*Job, 0),
		parser: cron.NewParser(
			cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow,
		),
		interval: 15 * time.Second,
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Register adds a periodic job to the scheduler.
func (s *Scheduler) Register(cronExpr, jobType string, opts ...Option) error {
	schedule, err := s.parser.Parse(cronExpr)
	if err != nil {
		return fmt.Errorf("invalid cron expression %q: %w", cronExpr, err)
	}

	job := &Job{
		Name:     jobType,
		Schedule: cronExpr,
		JobType:  jobType,
		Queue:    "default",
		Retry:    senna.DefaultRetryCount,
		schedule: schedule,
	}

	for _, opt := range opts {
		opt(job)
	}

	s.mu.Lock()
	s.jobs = append(s.jobs, job)
	s.mu.Unlock()

	return nil
}

// Start begins the periodic job scheduler.
func (s *Scheduler) Start(ctx context.Context) {
	go s.run(ctx)
}

// Stop halts the scheduler and waits for it to finish.
func (s *Scheduler) Stop() {
	close(s.stopCh)
	<-s.done
}

func (s *Scheduler) run(ctx context.Context) {
	defer close(s.done)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	// Run immediately on start
	s.checkAndEnqueue(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.checkAndEnqueue(ctx)
		}
	}
}

func (s *Scheduler) checkAndEnqueue(ctx context.Context) {
	s.mu.RLock()
	jobs := make([]*Job, len(s.jobs))
	copy(jobs, s.jobs)
	s.mu.RUnlock()

	now := time.Now()

	for _, job := range jobs {
		if s.shouldEnqueue(now, job) {
			if s.claimSlot(ctx, job, now) {
				s.enqueueJob(ctx, job)
			}
		}
	}
}

// shouldEnqueue determines if a job should run at the current time.
func (s *Scheduler) shouldEnqueue(now time.Time, job *Job) bool {
	// Get the start of the current minute
	currentMinute := now.Truncate(time.Minute)

	// Check if the current minute matches the schedule
	next := job.schedule.Next(currentMinute.Add(-time.Second))
	return next.Equal(currentMinute)
}

// claimSlot uses atomic Redis operations to claim the right to enqueue a job.
// Returns true if this worker claimed the slot, false if another worker did.
func (s *Scheduler) claimSlot(ctx context.Context, job *Job, now time.Time) bool {
	// Key format: periodic:{job_name}:{minute_timestamp}
	// TTL of 1 hour ensures cleanup even if job runs late
	minuteTS := now.Truncate(time.Minute).Unix()
	key := fmt.Sprintf("%s:%d", s.keys.PeriodicLock(job.Name), minuteTS)

	ok, err := s.redis.SetNX(ctx, key, "1", time.Hour).Result()
	if err != nil {
		slog.ErrorContext(ctx, "failed to claim periodic slot",
			"job", job.Name,
			"error", err,
		)
		return false
	}

	return ok
}

func (s *Scheduler) enqueueJob(ctx context.Context, job *Job) {
	j := senna.NewJob(job.JobType, job.Args)
	j.Queue = job.Queue
	j.Retry = job.Retry

	data, err := j.Marshal()
	if err != nil {
		slog.ErrorContext(ctx, "failed to marshal periodic job",
			"job", job.Name,
			"error", err,
		)
		return
	}

	// Add queue to known queues set
	if err := s.redis.SAdd(ctx, s.keys.Queues(), job.Queue).Err(); err != nil {
		slog.ErrorContext(ctx, "failed to add queue to set",
			"job", job.Name,
			"queue", job.Queue,
			"error", err,
		)
	}

	if err := s.redis.LPush(ctx, s.keys.Queue(job.Queue), string(data)).Err(); err != nil {
		slog.ErrorContext(ctx, "failed to enqueue periodic job",
			"job", job.Name,
			"error", err,
		)
		return
	}

	// Record history for observability
	s.recordHistory(ctx, job)

	slog.InfoContext(ctx, "enqueued periodic job",
		"job", job.Name,
		"queue", job.Queue,
	)
}

// HistoryEntry records when a periodic job was enqueued.
type HistoryEntry struct {
	EnqueuedAt time.Time `json:"enqueued_at"`
}

func (s *Scheduler) recordHistory(ctx context.Context, job *Job) {
	entry := HistoryEntry{
		EnqueuedAt: time.Now(),
	}

	data, _ := json.Marshal(entry)
	historyKey := fmt.Sprintf("%s:history", s.keys.PeriodicLock(job.Name))

	pipe := s.redis.Pipeline()
	pipe.LPush(ctx, historyKey, string(data))
	pipe.LTrim(ctx, historyKey, 0, 99) // Keep last 100 entries
	pipe.Expire(ctx, historyKey, 7*24*time.Hour)
	if _, err := pipe.Exec(ctx); err != nil {
		slog.WarnContext(ctx, "failed to record periodic job history", "job", job.Name, "error", err)
	}
}

// Jobs returns all registered periodic jobs.
func (s *Scheduler) Jobs() []*Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	jobs := make([]*Job, len(s.jobs))
	copy(jobs, s.jobs)
	return jobs
}

// History returns the recent execution history for a job.
func (s *Scheduler) History(ctx context.Context, jobName string) ([]HistoryEntry, error) {
	historyKey := fmt.Sprintf("%s:history", s.keys.PeriodicLock(jobName))

	entries, err := s.redis.LRange(ctx, historyKey, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	history := make([]HistoryEntry, 0, len(entries))
	for _, entry := range entries {
		var h HistoryEntry
		if err := json.Unmarshal([]byte(entry), &h); err != nil {
			continue
		}
		history = append(history, h)
	}

	return history, nil
}
