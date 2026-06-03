package worker

import (
	_ "embed"

	"github.com/mgomes/senna/internal/lua"
	"github.com/mgomes/senna/internal/script"
)

//go:embed lua/enqueue_scheduled.lua
var enqueueScheduledLua string

//go:embed lua/sequential_fetch.lua
var sequentialFetchLua string

//go:embed lua/requeue_orphaned.lua
var requeueOrphanedLua string

var (
	batchCompleteScript         = lua.BatchCompleteScript
	batchCallbackCompleteScript = lua.BatchCallbackCompleteScript
	batchAddJobsScript          = lua.BatchAddJobsScript
	batchInvalidateScript       = lua.BatchInvalidateScript
	enqueueScheduledScript      = script.New("enqueue_scheduled", enqueueScheduledLua)
	sequentialFetchScript       = script.New("sequential_fetch", sequentialFetchLua)
	requeueOrphanedScript       = script.New("requeue_orphaned", requeueOrphanedLua)
)
