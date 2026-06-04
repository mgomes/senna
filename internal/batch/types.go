package batch

// Error codes returned in the Error field of batch Lua script results.
const (
	// ErrCodeNotFound indicates the batch state no longer exists in Redis.
	ErrCodeNotFound = "batch_not_found"
	// ErrCodeInvalidated indicates the batch has been invalidated.
	ErrCodeInvalidated = "batch_invalidated"
	// ErrCodeComplete indicates the batch has already completed.
	ErrCodeComplete = "batch_complete"
)

// CompleteResult is the response from the batch_complete Lua script.
type CompleteResult struct {
	Pending          int    `json:"pending"`
	Successes        int    `json:"successes"`
	Failures         int    `json:"failures"`
	Dead             bool   `json:"dead"`
	Invalidated      bool   `json:"invalidated,omitempty"`
	ParentID         string `json:"parent_id,omitempty"`
	CompletedNow     bool   `json:"completed_now,omitempty"`
	Error            string `json:"error,omitempty"`
	AlreadyProcessed bool   `json:"already_processed,omitempty"`
}
