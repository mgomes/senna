package batch

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
