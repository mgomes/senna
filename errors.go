package senna

import (
	"fmt"
	"time"
)

// RetryableError indicates a job should be retried after RetryIn.
type RetryableError struct {
	Job     *Job
	Cause   error
	RetryIn time.Duration
}

// Error implements error.
func (e *RetryableError) Error() string {
	return fmt.Sprintf("retryable error for job %s: %v (retry in %v)", e.Job.ID, e.Cause, e.RetryIn)
}

// Unwrap returns the underlying error that caused the retry.
func (e *RetryableError) Unwrap() error {
	return e.Cause
}

// MaxRetriesExceededError indicates a job exhausted its retry budget.
type MaxRetriesExceededError struct {
	Job   *Job
	Cause error
}

// Error implements error.
func (e *MaxRetriesExceededError) Error() string {
	return fmt.Sprintf("max retries exceeded for job %s after %d attempts: %v", e.Job.ID, e.Job.RetryCount, e.Cause)
}

// Unwrap returns the underlying error that exhausted the retry budget.
func (e *MaxRetriesExceededError) Unwrap() error {
	return e.Cause
}

// JobNotFoundError indicates no handler was registered for a job.
type JobNotFoundError struct {
	JobID string
}

// Error implements error.
func (e *JobNotFoundError) Error() string {
	return fmt.Sprintf("job not found: %s", e.JobID)
}

// QueuePausedError indicates a job was enqueued to a paused queue.
type QueuePausedError struct {
	Queue string
}

// Error implements error.
func (e *QueuePausedError) Error() string {
	return fmt.Sprintf("queue is paused: %s", e.Queue)
}

// DuplicateJobError indicates a unique job lock already exists.
type DuplicateJobError struct {
	UniqueKey string
}

// Error implements error.
func (e *DuplicateJobError) Error() string {
	return fmt.Sprintf("duplicate job with unique key: %s", e.UniqueKey)
}

// BatchNotFoundError indicates batch state could not be found in Redis.
type BatchNotFoundError struct {
	BatchID string
}

// Error implements error.
func (e *BatchNotFoundError) Error() string {
	return fmt.Sprintf("batch not found: %s", e.BatchID)
}
