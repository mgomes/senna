package client

import (
	"github.com/mgomes/senna/internal/lua"
)

var (
	batchCompleteScript    = lua.BatchCompleteScript
	batchAddChildScript    = lua.BatchAddChildScript
	batchRemoveChildScript = lua.BatchRemoveChildScript
	enqueueNowScript       = lua.EnqueueNowScript
	enqueueUniqueNowScript = lua.EnqueueUniqueNowScript
	enqueueUniqueAtScript  = lua.EnqueueUniqueAtScript
	batchEnqueueJobsScript = lua.BatchEnqueueJobsScript
)
