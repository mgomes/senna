package senna

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Job struct {
	ID          string         `json:"jid"`
	Type        string         `json:"class"`
	Queue       string         `json:"queue"`
	Args        map[string]any `json:"args"`
	Retry       int            `json:"retry"`
	RetryCount  int            `json:"retry_count"`
	CreatedAt   time.Time      `json:"created_at"`
	EnqueuedAt  time.Time      `json:"enqueued_at"`
	ProcessedAt *time.Time     `json:"processed_at,omitempty"`
	FailedAt    *time.Time     `json:"failed_at,omitempty"`
	Error       string         `json:"error,omitempty"`
	BatchID     string         `json:"bid,omitempty"`
	UniqueKey   string         `json:"unique_key,omitempty"`
	UniqueTTL   time.Duration  `json:"unique_ttl,omitempty"`
	Encrypted   bool           `json:"encrypted,omitempty"`
	raw         string         `json:"-"`
}

func NewJob(jobType string, args map[string]any) *Job {
	now := time.Now()
	return &Job{
		ID:         uuid.New().String(),
		Type:       jobType,
		Queue:      "default",
		Args:       args,
		Retry:      25,
		RetryCount: 0,
		CreatedAt:  now,
		EnqueuedAt: now,
	}
}

func (j *Job) Marshal() ([]byte, error) {
	return json.Marshal(j)
}

func UnmarshalJob(data []byte) (*Job, error) {
	var job Job
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, err
	}
	return &job, nil
}

type Handler func(ctx context.Context, job *Job) error
