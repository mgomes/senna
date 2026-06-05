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
	finalization    *JobFinalization
}

// JobFinalization records the chosen outcome for an in-flight job whose
// handler has already returned. Workers persist it before updating batch
// counters so recovered jobs can resume the same finalization path without
// running the handler again.
type JobFinalization struct {
	Operation string    `json:"operation"`
	Error     string    `json:"error,omitempty"`
	RetryAt   time.Time `json:"retry_at,omitempty"`
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

// MarshalJSON encodes the job and its private finalization marker.
func (j *Job) MarshalJSON() ([]byte, error) {
	type jobAlias Job
	return json.Marshal(&struct {
		*jobAlias
		Finalization *JobFinalization `json:"finalization,omitempty"`
	}{
		jobAlias:     (*jobAlias)(j),
		Finalization: j.finalization,
	})
}

// UnmarshalJSON decodes the job and its private finalization marker.
func (j *Job) UnmarshalJSON(data []byte) error {
	type jobAlias Job
	aux := &struct {
		*jobAlias
		Finalization *JobFinalization `json:"finalization,omitempty"`
	}{
		jobAlias: (*jobAlias)(j),
	}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	j.finalization = aux.Finalization
	return nil
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

// Finalization returns the persisted finalization marker, if one exists.
func (j *Job) Finalization() *JobFinalization {
	if j.finalization == nil {
		return nil
	}
	finalization := *j.finalization
	return &finalization
}

// SetFinalization stores the finalization marker for this job.
func (j *Job) SetFinalization(finalization JobFinalization) {
	j.finalization = &finalization
}

// ClearFinalization removes the finalization marker from this job.
func (j *Job) ClearFinalization() {
	j.finalization = nil
}

// Handler processes a job.
type Handler func(ctx context.Context, job *Job) error
