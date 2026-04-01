package senna

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Job represents a queued unit of work.
type Job struct {
	ID              string         `json:"jid"`
	Type            string         `json:"class"`
	Queue           string         `json:"queue"`
	Args            map[string]any `json:"args"`
	Retry           int            `json:"retry"`
	RetryCount      int            `json:"retry_count"`
	CreatedAt       time.Time      `json:"created_at"`
	EnqueuedAt      time.Time      `json:"enqueued_at"`
	ProcessedAt     *time.Time     `json:"processed_at,omitempty"`
	FailedAt        *time.Time     `json:"failed_at,omitempty"`
	Error           string         `json:"error,omitempty"`
	BatchID         string         `json:"bid,omitempty"`
	CallbackBatchID string         `json:"callback_bid,omitempty"` // If set, this job is a callback for the specified batch
	UniqueKey       string         `json:"unique_key,omitempty"`
	UniqueTTL       time.Duration  `json:"unique_ttl,omitempty"`
	Encrypted       bool           `json:"encrypted,omitempty"`
	raw             string         `json:"-"`
}

// NewJob constructs a job with default queue, retry, and timestamp values.
func NewJob(jobType string, args map[string]any) *Job {
	now := time.Now()
	return &Job{
		ID:         uuid.New().String(),
		Type:       jobType,
		Queue:      DefaultQueueName,
		Args:       args,
		Retry:      DefaultRetryCount,
		RetryCount: 0,
		CreatedAt:  now,
		EnqueuedAt: now,
	}
}

// Marshal encodes the job as JSON.
func (j *Job) Marshal() ([]byte, error) {
	return json.Marshal(j)
}

// UnmarshalJob decodes a JSON job payload.
func UnmarshalJob(data []byte) (*Job, error) {
	var job Job
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, err
	}
	return &job, nil
}

// Raw returns the raw Redis payload associated with the job.
func (j *Job) Raw() string {
	return j.raw
}

// SetRaw stores the raw Redis payload associated with the job.
func (j *Job) SetRaw(raw string) {
	j.raw = raw
}

// Handler processes a job.
type Handler func(ctx context.Context, job *Job) error
