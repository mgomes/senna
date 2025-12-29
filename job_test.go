package senna

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewJob(t *testing.T) {
	args := map[string]any{
		"user_id": 123,
		"email":   "test@example.com",
	}

	job := NewJob("send_email", args)

	if job.ID == "" {
		t.Error("job ID should not be empty")
	}
	if job.Type != "send_email" {
		t.Errorf("expected type 'send_email', got '%s'", job.Type)
	}
	if job.Queue != "default" {
		t.Errorf("expected queue 'default', got '%s'", job.Queue)
	}
	if job.Args["user_id"] != 123 {
		t.Errorf("expected user_id 123, got %v", job.Args["user_id"])
	}
	if job.Args["email"] != "test@example.com" {
		t.Errorf("expected email 'test@example.com', got %v", job.Args["email"])
	}
	if job.Retry != 25 {
		t.Errorf("expected retry 25, got %d", job.Retry)
	}
	if job.RetryCount != 0 {
		t.Errorf("expected retry_count 0, got %d", job.RetryCount)
	}
	if job.CreatedAt.IsZero() {
		t.Error("created_at should not be zero")
	}
	if job.EnqueuedAt.IsZero() {
		t.Error("enqueued_at should not be zero")
	}
}

func TestNewJob_UniqueIDs(t *testing.T) {
	ids := make(map[string]bool)

	for range 1000 {
		job := NewJob("test", nil)
		if ids[job.ID] {
			t.Fatalf("duplicate job ID generated: %s", job.ID)
		}
		ids[job.ID] = true
	}
}

func TestJob_Marshal(t *testing.T) {
	job := NewJob("process_data", map[string]any{
		"data_id": 456,
		"options": map[string]any{
			"format": "json",
		},
	})
	job.Queue = "critical"
	job.Retry = 10
	job.BatchID = "batch-123"
	job.UniqueKey = "unique-key"

	data, err := job.Marshal()
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if parsed["jid"] != job.ID {
		t.Errorf("expected jid '%s', got '%v'", job.ID, parsed["jid"])
	}
	if parsed["class"] != "process_data" {
		t.Errorf("expected class 'process_data', got '%v'", parsed["class"])
	}
	if parsed["queue"] != "critical" {
		t.Errorf("expected queue 'critical', got '%v'", parsed["queue"])
	}
	if parsed["bid"] != "batch-123" {
		t.Errorf("expected bid 'batch-123', got '%v'", parsed["bid"])
	}
	if parsed["unique_key"] != "unique-key" {
		t.Errorf("expected unique_key 'unique-key', got '%v'", parsed["unique_key"])
	}
}

func TestUnmarshalJob(t *testing.T) {
	original := NewJob("test_job", map[string]any{
		"key": "value",
	})
	original.Queue = "high"
	original.Retry = 5
	original.RetryCount = 2
	original.BatchID = "batch-456"

	data, err := original.Marshal()
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	parsed, err := UnmarshalJob(data)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if parsed.ID != original.ID {
		t.Errorf("expected ID '%s', got '%s'", original.ID, parsed.ID)
	}
	if parsed.Type != original.Type {
		t.Errorf("expected Type '%s', got '%s'", original.Type, parsed.Type)
	}
	if parsed.Queue != original.Queue {
		t.Errorf("expected Queue '%s', got '%s'", original.Queue, parsed.Queue)
	}
	if parsed.Retry != original.Retry {
		t.Errorf("expected Retry %d, got %d", original.Retry, parsed.Retry)
	}
	if parsed.RetryCount != original.RetryCount {
		t.Errorf("expected RetryCount %d, got %d", original.RetryCount, parsed.RetryCount)
	}
	if parsed.BatchID != original.BatchID {
		t.Errorf("expected BatchID '%s', got '%s'", original.BatchID, parsed.BatchID)
	}
}

func TestUnmarshalJob_InvalidJSON(t *testing.T) {
	_, err := UnmarshalJob([]byte("not valid json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestJob_MarshalWithTimestamps(t *testing.T) {
	job := NewJob("test", nil)

	now := time.Now()
	job.ProcessedAt = &now
	job.FailedAt = &now
	job.Error = "something went wrong"

	data, err := job.Marshal()
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	parsed, err := UnmarshalJob(data)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if parsed.ProcessedAt == nil {
		t.Error("expected ProcessedAt to be set")
	}
	if parsed.FailedAt == nil {
		t.Error("expected FailedAt to be set")
	}
	if parsed.Error != "something went wrong" {
		t.Errorf("expected Error 'something went wrong', got '%s'", parsed.Error)
	}
}

func TestJob_MarshalEncrypted(t *testing.T) {
	job := NewJob("sensitive_job", map[string]any{
		"_encrypted": "base64encodeddata",
	})
	job.Encrypted = true

	data, err := job.Marshal()
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	parsed, err := UnmarshalJob(data)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if !parsed.Encrypted {
		t.Error("expected Encrypted to be true")
	}
	if parsed.Args["_encrypted"] != "base64encodeddata" {
		t.Errorf("expected encrypted args, got %v", parsed.Args)
	}
}

func TestJob_MarshalNilArgs(t *testing.T) {
	job := NewJob("no_args_job", nil)

	data, err := job.Marshal()
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	parsed, err := UnmarshalJob(data)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if parsed.Args != nil && len(parsed.Args) != 0 {
		t.Errorf("expected nil or empty args, got %v", parsed.Args)
	}
}

func TestJob_MarshalComplexArgs(t *testing.T) {
	job := NewJob("complex_job", map[string]any{
		"string":  "value",
		"number":  42,
		"float":   3.14,
		"bool":    true,
		"null":    nil,
		"array":   []any{1, 2, 3},
		"nested":  map[string]any{"key": "value"},
	})

	data, err := job.Marshal()
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	parsed, err := UnmarshalJob(data)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if parsed.Args["string"] != "value" {
		t.Errorf("expected string 'value', got %v", parsed.Args["string"])
	}
	if parsed.Args["number"].(float64) != 42 {
		t.Errorf("expected number 42, got %v", parsed.Args["number"])
	}
	if parsed.Args["bool"] != true {
		t.Errorf("expected bool true, got %v", parsed.Args["bool"])
	}
}
