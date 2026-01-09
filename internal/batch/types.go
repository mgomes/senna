package batch

// CompleteResult is the response from the batch_complete Lua script.
type CompleteResult struct {
	Callbacks        []Callback `json:"callbacks"`
	Pending          int        `json:"pending"`
	Successes        int        `json:"successes"`
	Failures         int        `json:"failures"`
	Dead             bool       `json:"dead"`
	Invalidated      bool       `json:"invalidated,omitempty"`
	CallbackQueue    string     `json:"callback_queue"`
	ParentID         string     `json:"parent_id,omitempty"`
	CompletedNow     bool       `json:"completed_now,omitempty"`
	Error            string     `json:"error,omitempty"`
	AlreadyProcessed bool       `json:"already_processed,omitempty"`
}

// Callback represents a batch callback to enqueue.
type Callback struct {
	CallbackType string         `json:"callback_type"`
	JobType      string         `json:"job_type"`
	Options      map[string]any `json:"options,omitempty"`
}
