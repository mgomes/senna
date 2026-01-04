package worker

import (
	_ "embed"

	"github.com/mgomes/senna/internal/script"
)

//go:embed lua/batch_complete.lua
var batchCompleteLua string

//go:embed lua/batch_add_jobs.lua
var batchAddJobsLua string

//go:embed lua/batch_invalidate.lua
var batchInvalidateLua string

//go:embed lua/enqueue_scheduled.lua
var enqueueScheduledLua string

//go:embed lua/sequential_fetch.lua
var sequentialFetchLua string

var batchCompleteScript = script.New("batch_complete", batchCompleteLua)
var batchAddJobsScript = script.New("batch_add_jobs", batchAddJobsLua)
var batchInvalidateScript = script.New("batch_invalidate", batchInvalidateLua)
var enqueueScheduledScript = script.New("enqueue_scheduled", enqueueScheduledLua)
var sequentialFetchScript = script.New("sequential_fetch", sequentialFetchLua)
