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
	"github.com/mgomes/senna/internal/lua"
	"github.com/mgomes/senna/internal/script"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
)

const periodicSlotTTL = time.Hour

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

// SchedulerOption configures a periodic scheduler.
type SchedulerOption func(*Scheduler)

// WithQueue sets the queue for enqueued jobs.
func WithQueue(queue string) Option {
	return func(j *Job) {
		j.Queue = queue
	}
}

// WithPollInterval sets how often the scheduler checks registered jobs.
func WithPollInterval(d time.Duration) SchedulerOption {
	return func(s *Scheduler) {
		s.interval = d
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
	redis       redis.Cmdable
	keys        *keys.Keys
	jobs        []*Job
	parser      cron.Parser
	interval    time.Duration
	mu          sync.RWMutex
	lifecycleMu sync.Mutex
	running     bool
	stopping    bool
	stopCh      chan struct{}
	done        chan struct{}
}

// NewScheduler creates a new periodic job scheduler.
func NewScheduler(redis redis.Cmdable, k *keys.Keys, opts ...SchedulerOption) *Scheduler {
	done := make(chan struct{})
	close(done)

	s := &Scheduler{
		redis: redis,
		keys:  k,
		jobs:  make([]*Job, 0),
		parser: cron.NewParser(
			cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow,
		),
		interval: senna.DefaultWorkerSettings().PeriodicPollInterval,
		stopCh:   make(chan struct{}),
		done:     done,
	}

	for _, opt := range opts {
		opt(s)
	}
	if s.interval <= 0 {
		s.interval = senna.DefaultWorkerSettings().PeriodicPollInterval
	}

	return s
}

// PollInterval returns how often the scheduler checks registered jobs.
func (s *Scheduler) PollInterval() time.Duration {
	return s.interval
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
		Queue:    senna.DefaultQueueName,
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
	s.lifecycleMu.Lock()
	if s.running || s.stopping {
		s.lifecycleMu.Unlock()
		return
	}
	s.stopCh = make(chan struct{})
	s.done = make(chan struct{})
	stopCh := s.stopCh
	done := s.done
	s.running = true
	s.lifecycleMu.Unlock()

	go s.run(ctx, stopCh, done)
}

// Stop halts the scheduler and waits for it to finish.
func (s *Scheduler) Stop() {
	s.lifecycleMu.Lock()
	if !s.running && !s.stopping {
		done := s.done
		s.lifecycleMu.Unlock()
		<-done
		return
	}
	if !s.stopping {
		close(s.stopCh)
		s.stopping = true
	}
	done := s.done
	s.lifecycleMu.Unlock()

	<-done
}

func (s *Scheduler) run(ctx context.Context, stopCh <-chan struct{}, done chan<- struct{}) {
	defer func() {
		close(done)
		s.lifecycleMu.Lock()
		if s.done == done {
			s.running = false
			s.stopping = false
		}
		s.lifecycleMu.Unlock()
	}()

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	// Run immediately on start
	s.checkAndEnqueue(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-stopCh:
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
			enqueued, err := s.claimAndEnqueue(ctx, job, now)
			if err != nil {
				slog.ErrorContext(ctx, "failed to enqueue periodic job",
					"job", job.Name,
					"error", err,
				)
				continue
			}
			if enqueued {
				s.recordHistory(ctx, job)
				slog.InfoContext(ctx, "enqueued periodic job",
					"job", job.Name,
					"queue", job.Queue,
				)
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
	key := s.periodicSlotKey(job, now)

	ok, err := s.redis.SetNX(ctx, key, "1", periodicSlotTTL).Result()
	if err != nil {
		slog.ErrorContext(ctx, "failed to claim periodic slot",
			"job", job.Name,
			"error", err,
		)
		return false
	}

	return ok
}

func (s *Scheduler) claimAndEnqueue(ctx context.Context, job *Job, now time.Time) (bool, error) {
	data, err := marshalJob(job)
	if err != nil {
		return false, fmt.Errorf("marshal periodic job %s: %w", job.Name, err)
	}

	keys := []string{
		s.periodicSlotKey(job, now),
		s.keys.Queues(),
		s.keys.Queue(job.Queue),
	}

	result, err := lua.PeriodicEnqueueScript.Run(
		ctx,
		s.redis,
		keys,
		"1",
		int64(periodicSlotTTL/time.Second),
		job.Queue,
		string(data),
	)
	if err != nil {
		return false, fmt.Errorf("claim and enqueue periodic job %s: %w", job.Name, err)
	}

	enqueued, err := script.Int(result)
	if err != nil {
		return false, fmt.Errorf("decode periodic enqueue result: %w", err)
	}

	return enqueued == 1, nil
}

func (s *Scheduler) periodicSlotKey(job *Job, now time.Time) string {
	// Key format: periodic:{job_name}:{minute_timestamp}.
	minuteTS := now.Truncate(time.Minute).Unix()
	return fmt.Sprintf("%s:%d", s.keys.PeriodicLock(job.Name), minuteTS)
}

func (s *Scheduler) enqueueJob(ctx context.Context, job *Job) error {
	data, err := marshalJob(job)
	if err != nil {
		return fmt.Errorf("marshal periodic job %s: %w", job.Name, err)
	}

	if err := s.redis.SAdd(ctx, s.keys.Queues(), job.Queue).Err(); err != nil {
		return fmt.Errorf("add queue %q to set: %w", job.Queue, err)
	}

	if err := s.redis.LPush(ctx, s.keys.Queue(job.Queue), string(data)).Err(); err != nil {
		return fmt.Errorf("enqueue periodic job %s: %w", job.Name, err)
	}

	s.recordHistory(ctx, job)

	slog.InfoContext(ctx, "enqueued periodic job",
		"job", job.Name,
		"queue", job.Queue,
	)

	return nil
}

func marshalJob(job *Job) ([]byte, error) {
	j := senna.NewJob(job.JobType, job.Args)
	j.Queue = job.Queue
	j.Retry = job.Retry

	return j.Marshal()
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
