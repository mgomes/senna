package senna

import (
	"encoding/json"
	"testing"
	"time"
	"unicode/utf8"
)

func FuzzJobMarshalRoundTrip(f *testing.F) {
	f.Add("send_email", "default", "user_id", "123", 25, 0, int64(time.Minute), false)
	f.Add("", "", "", "", 0, 0, int64(0), true)
	f.Add("batch_callback", "critical", "payload", "hello", -1, 99, int64(-time.Second), true)

	f.Fuzz(func(
		t *testing.T,
		jobType string,
		queue string,
		argKey string,
		argValue string,
		retry int,
		retryCount int,
		uniqueTTL int64,
		encrypted bool,
	) {
		if !smallValidString(jobType) || !smallValidString(queue) || !smallValidString(argKey) || !smallValidString(argValue) {
			t.Skip()
		}

		createdAt := time.Unix(1_704_067_200, int64(retry&0xffff)).UTC()
		processedAt := createdAt.Add(time.Duration(retryCount) * time.Second)
		job := &Job{
			ID:          "jid-" + jobType,
			Type:        jobType,
			Queue:       queue,
			Args:        map[string]any{"key": argKey, "value": argValue},
			Retry:       retry,
			RetryCount:  retryCount,
			CreatedAt:   createdAt,
			EnqueuedAt:  createdAt.Add(time.Second),
			ProcessedAt: &processedAt,
			BatchID:     "bid-" + argKey,
			UniqueKey:   "unique-" + argValue,
			UniqueTTL:   time.Duration(uniqueTTL),
			Encrypted:   encrypted,
		}

		data, err := job.Marshal()
		if err != nil {
			t.Fatalf("Job.Marshal() error = %v", err)
		}

		got, err := UnmarshalJob(data)
		if err != nil {
			t.Fatalf("UnmarshalJob(Job.Marshal()) error = %v", err)
		}

		if got.ID != job.ID {
			t.Errorf("UnmarshalJob(Job.Marshal()).ID = %q, want %q", got.ID, job.ID)
		}
		if got.Type != job.Type {
			t.Errorf("UnmarshalJob(Job.Marshal()).Type = %q, want %q", got.Type, job.Type)
		}
		if got.Queue != job.Queue {
			t.Errorf("UnmarshalJob(Job.Marshal()).Queue = %q, want %q", got.Queue, job.Queue)
		}
		if got.Retry != job.Retry {
			t.Errorf("UnmarshalJob(Job.Marshal()).Retry = %d, want %d", got.Retry, job.Retry)
		}
		if got.RetryCount != job.RetryCount {
			t.Errorf("UnmarshalJob(Job.Marshal()).RetryCount = %d, want %d", got.RetryCount, job.RetryCount)
		}
		if !got.CreatedAt.Equal(job.CreatedAt) {
			t.Errorf("UnmarshalJob(Job.Marshal()).CreatedAt = %v, want %v", got.CreatedAt, job.CreatedAt)
		}
		if got.ProcessedAt == nil || !got.ProcessedAt.Equal(processedAt) {
			t.Errorf("UnmarshalJob(Job.Marshal()).ProcessedAt = %v, want %v", got.ProcessedAt, processedAt)
		}
		if got.BatchID != job.BatchID {
			t.Errorf("UnmarshalJob(Job.Marshal()).BatchID = %q, want %q", got.BatchID, job.BatchID)
		}
		if got.UniqueKey != job.UniqueKey {
			t.Errorf("UnmarshalJob(Job.Marshal()).UniqueKey = %q, want %q", got.UniqueKey, job.UniqueKey)
		}
		if got.UniqueTTL != job.UniqueTTL {
			t.Errorf("UnmarshalJob(Job.Marshal()).UniqueTTL = %v, want %v", got.UniqueTTL, job.UniqueTTL)
		}
		if got.Encrypted != job.Encrypted {
			t.Errorf("UnmarshalJob(Job.Marshal()).Encrypted = %t, want %t", got.Encrypted, job.Encrypted)
		}
		if got.Args["key"] != argKey {
			t.Errorf("UnmarshalJob(Job.Marshal()).Args[key] = %v, want %q", got.Args["key"], argKey)
		}
		if got.Args["value"] != argValue {
			t.Errorf("UnmarshalJob(Job.Marshal()).Args[value] = %v, want %q", got.Args["value"], argValue)
		}
	})
}

func FuzzBatchStateJSONRoundTrip(f *testing.F) {
	f.Add("bid-1", "daily import", "parent-1", "callback_job", 10, 3, 1, 6, false)
	f.Add("", "", "", "", 0, 0, 0, 0, true)

	f.Fuzz(func(
		t *testing.T,
		id string,
		description string,
		parentID string,
		callbackJobType string,
		total int,
		pending int,
		failures int,
		successes int,
		dead bool,
	) {
		if !smallValidString(id) ||
			!smallValidString(description) ||
			!smallValidString(parentID) ||
			!smallValidString(callbackJobType) {
			t.Skip()
		}

		createdAt := time.Unix(1_704_067_200, int64(total&0xffff)).UTC()
		state := BatchState{
			ID:            id,
			Description:   description,
			ParentID:      parentID,
			Total:         total,
			Pending:       pending,
			Failures:      failures,
			Successes:     successes,
			Dead:          dead,
			CreatedAt:     createdAt,
			OnComplete:    &CallbackInfo{JobType: callbackJobType, Options: map[string]any{"description": description}},
			CallbackQueue: "callbacks",
			FailedJIDs:    []string{id, parentID},
			Invalidated:   failures > total,
		}

		data, err := json.Marshal(state)
		if err != nil {
			t.Fatalf("json.Marshal(BatchState) error = %v", err)
		}

		var got BatchState
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("json.Unmarshal(json.Marshal(BatchState)) error = %v", err)
		}

		if got.ID != state.ID {
			t.Errorf("BatchState.ID = %q, want %q", got.ID, state.ID)
		}
		if got.Description != state.Description {
			t.Errorf("BatchState.Description = %q, want %q", got.Description, state.Description)
		}
		if got.ParentID != state.ParentID {
			t.Errorf("BatchState.ParentID = %q, want %q", got.ParentID, state.ParentID)
		}
		if got.Total != state.Total || got.Pending != state.Pending || got.Failures != state.Failures || got.Successes != state.Successes {
			t.Errorf("BatchState counters = total:%d pending:%d failures:%d successes:%d, want total:%d pending:%d failures:%d successes:%d",
				got.Total, got.Pending, got.Failures, got.Successes,
				state.Total, state.Pending, state.Failures, state.Successes,
			)
		}
		if got.Dead != state.Dead {
			t.Errorf("BatchState.Dead = %t, want %t", got.Dead, state.Dead)
		}
		if !got.CreatedAt.Equal(state.CreatedAt) {
			t.Errorf("BatchState.CreatedAt = %v, want %v", got.CreatedAt, state.CreatedAt)
		}
		if got.OnComplete == nil || got.OnComplete.JobType != state.OnComplete.JobType {
			t.Errorf("BatchState.OnComplete = %+v, want %+v", got.OnComplete, state.OnComplete)
		}
		if len(got.FailedJIDs) != len(state.FailedJIDs) {
			t.Errorf("BatchState.FailedJIDs length = %d, want %d", len(got.FailedJIDs), len(state.FailedJIDs))
		}
		if got.Invalidated != state.Invalidated {
			t.Errorf("BatchState.Invalidated = %t, want %t", got.Invalidated, state.Invalidated)
		}
	})
}

func smallValidString(s string) bool {
	return len(s) <= 1024 && utf8.ValidString(s)
}
