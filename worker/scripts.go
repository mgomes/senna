package worker

import (
	_ "embed"

	"github.com/mgomes/senna/internal/lua"
	"github.com/mgomes/senna/internal/script"
)

//go:embed lua/enqueue_scheduled.lua
var enqueueScheduledLua string

//go:embed lua/fetch_job.lua
var fetchJobLua string

//go:embed lua/sequential_fetch.lua
var sequentialFetchLua string

//go:embed lua/discard_sequential_fetch.lua
var discardSequentialFetchLua string

//go:embed lua/requeue_orphaned.lua
var requeueOrphanedLua string

//go:embed lua/release_sequential_lock.lua
var releaseSequentialLockLua string

//go:embed lua/ack_job.lua
var ackJobLua string

//go:embed lua/retry_job.lua
var retryJobLua string

//go:embed lua/move_to_dead_job.lua
var moveToDeadJobLua string

//go:embed lua/requeue_job.lua
var requeueJobLua string

//go:embed lua/mark_job_finalization.lua
var markJobFinalizationLua string

//go:embed lua/sanitize_job_finalization.lua
var sanitizeJobFinalizationLua string

var (
	batchCompleteScript           = lua.BatchCompleteScript
	batchCallbackCompleteScript   = lua.BatchCallbackCompleteScript
	batchAddJobsScript            = lua.BatchAddJobsScript
	batchInvalidateScript         = lua.BatchInvalidateScript
	ackJobScript                  = script.New("ack-job", ackJobLua)
	retryJobScript                = script.New("retry-job", retryJobLua)
	moveToDeadJobScript           = script.New("move-to-dead-job", moveToDeadJobLua)
	requeueJobScript              = script.New("requeue-job", requeueJobLua)
	markJobFinalizationScript     = script.New("mark-job-finalization", markJobFinalizationLua)
	sanitizeJobFinalizationScript = script.New("sanitize-job-finalization", sanitizeJobFinalizationLua)
	fetchJobScript                = script.New("fetch-job", fetchJobLua)
	enqueueScheduledScript        = script.New("enqueue_scheduled", enqueueScheduledLua)
	sequentialFetchScript         = script.New("sequential_fetch", sequentialFetchLua)
	discardSequentialFetchScript  = script.New("discard_sequential_fetch", discardSequentialFetchLua)
	requeueOrphanedScript         = script.New("requeue_orphaned", requeueOrphanedLua)
	releaseSequentialLockScript   = script.New("release_sequential_lock", releaseSequentialLockLua)
)
