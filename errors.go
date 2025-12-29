package senna

import (
	"fmt"
	"time"
)

type RetryableError struct {
	Job     *Job
	Cause   error
	RetryIn time.Duration
}

func (e *RetryableError) Error() string {
	return fmt.Sprintf("retryable error for job %s: %v (retry in %v)", e.Job.ID, e.Cause, e.RetryIn)
}

func (e *RetryableError) Unwrap() error {
	return e.Cause
}

type MaxRetriesExceededError struct {
	Job   *Job
	Cause error
}

func (e *MaxRetriesExceededError) Error() string {
	return fmt.Sprintf("max retries exceeded for job %s after %d attempts: %v", e.Job.ID, e.Job.RetryCount, e.Cause)
}

func (e *MaxRetriesExceededError) Unwrap() error {
	return e.Cause
}

type JobNotFoundError struct {
	JobID string
}

func (e *JobNotFoundError) Error() string {
	return fmt.Sprintf("job not found: %s", e.JobID)
}

type QueuePausedError struct {
	Queue string
}

func (e *QueuePausedError) Error() string {
	return fmt.Sprintf("queue is paused: %s", e.Queue)
}

type DuplicateJobError struct {
	UniqueKey string
}

func (e *DuplicateJobError) Error() string {
	return fmt.Sprintf("duplicate job with unique key: %s", e.UniqueKey)
}

type BatchNotFoundError struct {
	BatchID string
}

func (e *BatchNotFoundError) Error() string {
	return fmt.Sprintf("batch not found: %s", e.BatchID)
}
