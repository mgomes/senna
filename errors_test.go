package senna

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRetryableError_Error(t *testing.T) {
	t.Parallel()
	job := NewJob("test_job", nil)
	cause := errors.New("temporary failure")
	retryIn := 5 * time.Second

	err := &RetryableError{
		Job:     job,
		Cause:   cause,
		RetryIn: retryIn,
	}

	msg := err.Error()
	if msg == "" {
		t.Error("error message should not be empty")
	}
	if !strings.Contains(msg, job.ID) {
		t.Errorf("error message should contain job ID, got: %s", msg)
	}
	if !strings.Contains(msg, "temporary failure") {
		t.Errorf("error message should contain cause, got: %s", msg)
	}
	if !strings.Contains(msg, "5s") {
		t.Errorf("error message should contain retry duration, got: %s", msg)
	}
}

func TestRetryableError_Unwrap(t *testing.T) {
	t.Parallel()
	cause := errors.New("underlying error")
	err := &RetryableError{
		Job:   NewJob("test", nil),
		Cause: cause,
	}

	unwrapped := err.Unwrap()
	if unwrapped != cause {
		t.Errorf("expected unwrapped error to be cause, got: %v", unwrapped)
	}

	if !errors.Is(err, cause) {
		t.Error("errors.Is should return true for wrapped cause")
	}
}

func TestMaxRetriesExceededError_Error(t *testing.T) {
	t.Parallel()
	job := NewJob("failing_job", nil)
	job.RetryCount = 5
	cause := errors.New("persistent failure")

	err := &MaxRetriesExceededError{
		Job:   job,
		Cause: cause,
	}

	msg := err.Error()
	if msg == "" {
		t.Error("error message should not be empty")
	}
	if !strings.Contains(msg, job.ID) {
		t.Errorf("error message should contain job ID, got: %s", msg)
	}
	if !strings.Contains(msg, "5") {
		t.Errorf("error message should contain retry count, got: %s", msg)
	}
	if !strings.Contains(msg, "persistent failure") {
		t.Errorf("error message should contain cause, got: %s", msg)
	}
}

func TestMaxRetriesExceededError_Unwrap(t *testing.T) {
	t.Parallel()
	cause := errors.New("underlying error")
	err := &MaxRetriesExceededError{
		Job:   NewJob("test", nil),
		Cause: cause,
	}

	unwrapped := err.Unwrap()
	if unwrapped != cause {
		t.Errorf("expected unwrapped error to be cause, got: %v", unwrapped)
	}

	if !errors.Is(err, cause) {
		t.Error("errors.Is should return true for wrapped cause")
	}
}

func TestJobNotFoundError_Error(t *testing.T) {
	t.Parallel()
	err := &JobNotFoundError{JobID: "job-123-abc"}

	msg := err.Error()
	if msg == "" {
		t.Error("error message should not be empty")
	}
	if !strings.Contains(msg, "job-123-abc") {
		t.Errorf("error message should contain job ID, got: %s", msg)
	}
}

func TestQueuePausedError_Error(t *testing.T) {
	t.Parallel()
	err := &QueuePausedError{Queue: "critical"}

	msg := err.Error()
	if msg == "" {
		t.Error("error message should not be empty")
	}
	if !strings.Contains(msg, "critical") {
		t.Errorf("error message should contain queue name, got: %s", msg)
	}
}

func TestDuplicateJobError_Error(t *testing.T) {
	t.Parallel()
	err := &DuplicateJobError{UniqueKey: "user:123:sync"}

	msg := err.Error()
	if msg == "" {
		t.Error("error message should not be empty")
	}
	if !strings.Contains(msg, "user:123:sync") {
		t.Errorf("error message should contain unique key, got: %s", msg)
	}
}

func TestBatchNotFoundError_Error(t *testing.T) {
	t.Parallel()
	err := &BatchNotFoundError{BatchID: "batch-456-xyz"}

	msg := err.Error()
	if msg == "" {
		t.Error("error message should not be empty")
	}
	if !strings.Contains(msg, "batch-456-xyz") {
		t.Errorf("error message should contain batch ID, got: %s", msg)
	}
}

func TestErrors_TypeAssertions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
	}{
		{"RetryableError", &RetryableError{Job: NewJob("t", nil)}},
		{"MaxRetriesExceededError", &MaxRetriesExceededError{Job: NewJob("t", nil)}},
		{"JobNotFoundError", &JobNotFoundError{JobID: "x"}},
		{"QueuePausedError", &QueuePausedError{Queue: "q"}},
		{"DuplicateJobError", &DuplicateJobError{UniqueKey: "k"}},
		{"BatchNotFoundError", &BatchNotFoundError{BatchID: "b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err.Error() == "" {
				t.Errorf("%s.Error() should not be empty", tt.name)
			}
		})
	}
}

func TestRetryableError_ErrorsAs(t *testing.T) {
	t.Parallel()
	original := &RetryableError{
		Job:     NewJob("test", nil),
		Cause:   errors.New("fail"),
		RetryIn: time.Second,
	}

	var target *RetryableError
	if !errors.As(original, &target) {
		t.Error("errors.As should match RetryableError")
	}
	if target.RetryIn != time.Second {
		t.Errorf("expected RetryIn 1s, got %v", target.RetryIn)
	}
}

func TestMaxRetriesExceededError_ErrorsAs(t *testing.T) {
	t.Parallel()
	job := NewJob("test", nil)
	job.RetryCount = 10
	original := &MaxRetriesExceededError{
		Job:   job,
		Cause: errors.New("fail"),
	}

	var target *MaxRetriesExceededError
	if !errors.As(original, &target) {
		t.Error("errors.As should match MaxRetriesExceededError")
	}
	if target.Job.RetryCount != 10 {
		t.Errorf("expected RetryCount 10, got %d", target.Job.RetryCount)
	}
}
