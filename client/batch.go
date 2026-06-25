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
	ParentID      string          `json:"parent_id,omitempty"`
	Jobs          []*senna.Job    `json:"jobs"`
	OnComplete    *CallbackConfig `json:"on_complete,omitempty"`
	OnSuccess     *CallbackConfig `json:"on_success,omitempty"`
	OnDeath       *CallbackConfig `json:"on_death,omitempty"`
	CallbackQueue string          `json:"callback_queue,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	err           error           `json:"-"`

	autoflushChunkSize int
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

// WithParent sets the parent batch ID for this batch.
// This allows parent batches to wait for child batches to complete.
func (b *Batch) WithParent(parentID string) *Batch {
	b.ParentID = parentID
	return b
}

// Add adds a job to the batch.
func (b *Batch) Add(jobType string, args map[string]any, opts ...EnqueueOption) *Batch {
	if b.err != nil {
		return b
	}

	cfg := &enqueueConfig{
		queue: senna.DefaultQueueName,
		retry: senna.DefaultRetryCount,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	if unsupported := unsupportedBatchOptions(cfg); len(unsupported) > 0 {
		b.err = fmt.Errorf("batch.Add does not support options: %s", strings.Join(unsupported, ", "))
		return b
	}
	if err := validateQueueName(cfg.queue); err != nil {
		b.err = err
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

func validateBatchCallbackOptions(b *Batch) error {
	if err := validateCallbackOptions("on_complete", b.OnComplete); err != nil {
		return err
	}
	if err := validateCallbackOptions("on_success", b.OnSuccess); err != nil {
		return err
	}
	if err := validateCallbackOptions("on_death", b.OnDeath); err != nil {
		return err
	}
	return nil
}

func validateCallbackOptions(callbackName string, cfg *CallbackConfig) error {
	if cfg == nil {
		return nil
	}
	for key := range cfg.Options {
		if isReservedCallbackOptionKey(key) {
			return fmt.Errorf("%w: %s option %q", ErrReservedCallbackOption, callbackName, key)
		}
	}
	return nil
}

func isReservedCallbackOptionKey(key string) bool {
	switch key {
	case "batch_id", "parent_id":
		return true
	default:
		return false
	}
}

// WithCallbackQueue sets the queue for callback jobs.
func (b *Batch) WithCallbackQueue(queue string) *Batch {
	b.CallbackQueue = queue
	return b
}

// WithAutoflush enqueues initial batch jobs in chunks.
//
// This trades the default single-script initial enqueue atomicity for bounded
// Redis command size. If a later chunk fails, earlier jobs may already be
// queued; Senna cleans up the batch state so those jobs run without batch
// tracking.
func (b *Batch) WithAutoflush(chunkSize int) *Batch {
	if chunkSize <= 0 {
		b.err = ErrInvalidChunkSize
		return b
	}
	b.autoflushChunkSize = chunkSize
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
	if cfg.chunkSizeSet {
		unsupported = append(unsupported, "WithBulkChunkSize")
	}
	return unsupported
}
