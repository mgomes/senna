package client

import (
	"time"

	"github.com/mgomes/senna"
)

type Batch struct {
	ID          string      `json:"id"`
	Description string      `json:"description,omitempty"`
	Jobs        []*senna.Job `json:"jobs"`
	OnComplete  string      `json:"on_complete,omitempty"`
	OnSuccess   string      `json:"on_success,omitempty"`
	OnDeath     string      `json:"on_death,omitempty"`
	CreatedAt   time.Time   `json:"created_at"`
}

func NewBatch() *Batch {
	return &Batch{
		ID:        senna.NewJob("", nil).ID,
		CreatedAt: time.Now(),
		Jobs:      make([]*senna.Job, 0),
	}
}

func (b *Batch) Add(jobType string, args map[string]any, opts ...EnqueueOption) *Batch {
	cfg := &enqueueConfig{
		queue: "default",
		retry: 25,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	job := senna.NewJob(jobType, args)
	job.Queue = cfg.queue
	job.Retry = cfg.retry
	b.Jobs = append(b.Jobs, job)
	return b
}

func (b *Batch) OnCompleteCallback(jobType string) *Batch {
	b.OnComplete = jobType
	return b
}

func (b *Batch) OnSuccessCallback(jobType string) *Batch {
	b.OnSuccess = jobType
	return b
}

func (b *Batch) OnDeathCallback(jobType string) *Batch {
	b.OnDeath = jobType
	return b
}
