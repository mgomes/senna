package batch

import (
	"time"

	senna "github.com/mgomes/senna"
	"github.com/mgomes/senna/internal/keys"
)

const BatchTTL = 30 * 24 * time.Hour

// ResultEmptySuccess completes an empty batch through the completion script.
const ResultEmptySuccess = "empty_success"

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
func CompletionArgs(k *keys.Keys, jobID, result string) []any {
	now := time.Now().Format(time.RFC3339Nano)
	return []any{
		jobID,
		result,
		senna.DefaultRetryCount,
		now,
		k.Queue(""),
	}
}

func ParentResultType(result *CompleteResult) (string, bool) {
	if !result.CompletedNow || result.ParentID == "" {
		return "", false
	}
	if result.Dead {
		return "death", true
	}
	if result.Invalidated {
		return "invalidated", true
	}
	return "success", true
}
