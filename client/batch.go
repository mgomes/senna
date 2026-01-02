package client

import (
	"fmt"
	"strings"
	"time"

	"github.com/mgomes/senna"
)

// CallbackConfig holds configuration for a batch callback.
type CallbackConfig struct {
	JobType string         `json:"job_type"`
	Options map[string]any `json:"options,omitempty"`
}

// Batch represents a collection of jobs that can be monitored as a group.
type Batch struct {
	ID            string          `json:"id"`
	Description   string          `json:"description,omitempty"`
	Jobs          []*senna.Job    `json:"jobs"`
	OnComplete    *CallbackConfig `json:"on_complete,omitempty"`
	OnSuccess     *CallbackConfig `json:"on_success,omitempty"`
	OnDeath       *CallbackConfig `json:"on_death,omitempty"`
	CallbackQueue string          `json:"callback_queue,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	err           error           `json:"-"`
}

// NewBatch creates a new batch with a unique ID.
func NewBatch() *Batch {
	return &Batch{
		ID:        senna.NewJob("", nil).ID,
		CreatedAt: time.Now(),
		Jobs:      make([]*senna.Job, 0),
	}
}

// WithDescription sets an optional description for the batch.
func (b *Batch) WithDescription(desc string) *Batch {
	b.Description = desc
	return b
}

// Add adds a job to the batch.
func (b *Batch) Add(jobType string, args map[string]any, opts ...EnqueueOption) *Batch {
	if b.err != nil {
		return b
	}

	cfg := &enqueueConfig{
		queue: "default",
		retry: 25,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	if unsupported := unsupportedBatchOptions(cfg); len(unsupported) > 0 {
		b.err = fmt.Errorf("batch.Add does not support options: %s", strings.Join(unsupported, ", "))
		return b
	}

	job := senna.NewJob(jobType, args)
	job.Queue = cfg.queue
	job.Retry = cfg.retry
	b.Jobs = append(b.Jobs, job)
	return b
}

// OnCompleteCallback sets the callback to fire when all jobs have executed
// (whether successful or not).
func (b *Batch) OnCompleteCallback(jobType string, options ...map[string]any) *Batch {
	b.OnComplete = &CallbackConfig{JobType: jobType}
	if len(options) > 0 {
		b.OnComplete.Options = options[0]
	}
	return b
}

// OnSuccessCallback sets the callback to fire when all jobs have completed successfully.
func (b *Batch) OnSuccessCallback(jobType string, options ...map[string]any) *Batch {
	b.OnSuccess = &CallbackConfig{JobType: jobType}
	if len(options) > 0 {
		b.OnSuccess.Options = options[0]
	}
	return b
}

// OnDeathCallback sets the callback to fire the first time a batch job dies
// (exhausts all retries).
func (b *Batch) OnDeathCallback(jobType string, options ...map[string]any) *Batch {
	b.OnDeath = &CallbackConfig{JobType: jobType}
	if len(options) > 0 {
		b.OnDeath.Options = options[0]
	}
	return b
}

// WithCallbackQueue sets the queue for callback jobs (defaults to "default").
func (b *Batch) WithCallbackQueue(queue string) *Batch {
	b.CallbackQueue = queue
	return b
}

func unsupportedBatchOptions(cfg *enqueueConfig) []string {
	var unsupported []string
	if cfg.uniqueKey != "" {
		unsupported = append(unsupported, "WithUniqueKey")
	}
	if cfg.encrypt {
		unsupported = append(unsupported, "WithEncryption")
	}
	if cfg.delay > 0 {
		unsupported = append(unsupported, "WithDelay")
	}
	if !cfg.at.IsZero() {
		unsupported = append(unsupported, "WithScheduleAt")
	}
	if cfg.batchID != "" {
		unsupported = append(unsupported, "WithBatch")
	}
	return unsupported
}
