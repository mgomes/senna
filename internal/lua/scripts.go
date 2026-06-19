package lua

import (
	_ "embed"

	"github.com/mgomes/senna/internal/script"
)

//go:embed batch_complete.lua
var batchCompleteLua string

//go:embed batch_add_child.lua
var batchAddChildLua string

//go:embed batch_remove_child.lua
var batchRemoveChildLua string

//go:embed batch_callback_complete.lua
var batchCallbackCompleteLua string

//go:embed batch_add_jobs.lua
var batchAddJobsLua string

//go:embed batch_invalidate.lua
var batchInvalidateLua string

//go:embed enqueue_unique_now.lua
var enqueueUniqueNowLua string

//go:embed enqueue_now.lua
var enqueueNowLua string

//go:embed enqueue_unique_at.lua
var enqueueUniqueAtLua string

//go:embed batch_enqueue_jobs.lua
var batchEnqueueJobsLua string

//go:embed periodic_enqueue.lua
var periodicEnqueueLua string

var (
	BatchCompleteScript         = script.New("batch_complete", batchCompleteLua)
	BatchAddChildScript         = script.New("batch_add_child", batchAddChildLua)
	BatchRemoveChildScript      = script.New("batch_remove_child", batchRemoveChildLua)
	BatchCallbackCompleteScript = script.New("batch_callback_complete", batchCallbackCompleteLua)
	BatchAddJobsScript          = script.New("batch_add_jobs", batchAddJobsLua)
	BatchInvalidateScript       = script.New("batch_invalidate", batchInvalidateLua)
	EnqueueNowScript            = script.New("enqueue_now", enqueueNowLua)
	EnqueueUniqueNowScript      = script.New("enqueue_unique_now", enqueueUniqueNowLua)
	EnqueueUniqueAtScript       = script.New("enqueue_unique_at", enqueueUniqueAtLua)
	BatchEnqueueJobsScript      = script.New("batch_enqueue_jobs", batchEnqueueJobsLua)
	PeriodicEnqueueScript       = script.New("periodic_enqueue", periodicEnqueueLua)
)
