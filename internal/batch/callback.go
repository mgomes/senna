package batch

import (
	"time"

	senna "github.com/mgomes/senna"
	"github.com/mgomes/senna/internal/keys"
)

const BatchTTL = 30 * 24 * time.Hour

// Result identifies how a job or child batch completed. The string values are
// part of the batch completion Lua script protocol and must not be changed.
type Result string

const (
	// ResultSuccess marks a successful completion.
	ResultSuccess Result = "success"
	// ResultFailure marks a failed (but not yet dead) completion.
	ResultFailure Result = "failure"
	// ResultDeath marks a job that exhausted its retries.
	ResultDeath Result = "death"
	// ResultInvalidated marks a batch that was invalidated.
	ResultInvalidated Result = "invalidated"
	// ResultEmptySuccess completes an empty batch through the completion script.
	ResultEmptySuccess Result = "empty_success"
)

// CompletionKeys returns the Redis keys used by the batch completion script.
func CompletionKeys(k *keys.Keys, batchID string) []string {
	return []string{
		k.Batch(batchID),
		k.BatchJobs(batchID),
		k.BatchFailed(batchID),
		k.DeadBatches(),
		k.BatchCallbacks(batchID),
		k.Queues(),
	}
}

// CompletionArgs returns the Redis arguments used by the batch completion script.
func CompletionArgs(k *keys.Keys, jobID string, result Result) []any {
	now := time.Now().Format(time.RFC3339Nano)
	return []any{
		jobID,
		string(result),
		senna.DefaultRetryCount,
		now,
		k.Queue(""),
	}
}

// ParentResultType maps a completion result to the result a parent batch should
// record, returning false when there is no parent to propagate to.
func ParentResultType(result *CompleteResult) (Result, bool) {
	if !result.CompletedNow || result.ParentID == "" {
		return "", false
	}
	if result.Dead {
		return ResultDeath, true
	}
	if result.Invalidated {
		return ResultInvalidated, true
	}
	return ResultSuccess, true
}
