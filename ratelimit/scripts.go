package ratelimit

import _ "embed"

//go:embed lua/window.lua
var windowLua string

//go:embed lua/bucket.lua
var bucketLua string

//go:embed lua/leaky.lua
var leakyLua string

//go:embed lua/points_check.lua
var pointsCheckLua string

//go:embed lua/points_adjust.lua
var pointsAdjustLua string

//go:embed lua/concurrent_init.lua
var concurrentInitLua string

//go:embed lua/concurrent_acquire.lua
var concurrentAcquireLua string

//go:embed lua/concurrent_release.lua
var concurrentReleaseLua string

//go:embed lua/concurrent_reclaim.lua
var concurrentReclaimLua string
