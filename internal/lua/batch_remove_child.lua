-- batch_remove_child.lua
-- Remove a child batch from a parent batch (rollback batch_add_child).
-- KEYS[1] = parent batch metadata key
-- KEYS[2] = parent batch progress hash key
-- KEYS[3] = parent batch jobs set key
-- ARGV[1] = child batch id
-- Returns: JSON with result

local batch_key = KEYS[1]
local progress_key = KEYS[2]
local jobs_key = KEYS[3]
local child_id = ARGV[1]

local function bool_value(value)
    return value == "1" or value == "true"
end

local function bool_field(field)
    return bool_value(redis.call("HGET", progress_key, field))
end

local batch_type = redis.call("TYPE", batch_key).ok
if batch_type == "none" then
    -- Parent already gone, nothing to roll back
    return cjson.encode({success = true, parent_gone = true})
end
if batch_type ~= "string" then
    return redis.error_reply("batch key has type " .. batch_type .. ", want string")
end

local progress_type = redis.call("TYPE", progress_key).ok
if progress_type ~= "none" and progress_type ~= "hash" then
    return redis.error_reply("batch progress key has type " .. progress_type .. ", want hash")
end

local jobs_type = redis.call("TYPE", jobs_key).ok
if jobs_type ~= "none" and jobs_type ~= "set" then
    return redis.error_reply("batch jobs key has type " .. jobs_type .. ", want set")
end

local batch_data
local batch

local function load_batch()
    if not batch then
        batch_data = batch_data or redis.call("GET", batch_key)
        batch = cjson.decode(batch_data)
    end
    return batch
end

local batch_ttl = redis.call("PTTL", batch_key)
if progress_type == "none" then
    local legacy = load_batch()
    redis.call("HSET", progress_key,
        "id", legacy.id or "",
        "parent_id", legacy.parent_id or "",
        "callback_queue", legacy.callback_queue or "",
        "total", legacy.total or 0,
        "pending", legacy.pending or redis.call("SCARD", jobs_key),
        "failures", legacy.failures or 0,
        "successes", legacy.successes or 0,
        "callbacks_pending", legacy.callbacks_pending or 0,
        "callback_seq", legacy.callback_seq or 0,
        "dead", legacy.dead and "1" or "0",
        "death_fired", legacy.death_fired and "1" or "0",
        "complete_fired", legacy.complete_fired and "1" or "0",
        "success_fired", legacy.success_fired and "1" or "0",
        "invalidated", legacy.invalidated and "1" or "0"
    )
    if batch_ttl > 0 then
        redis.call("PEXPIRE", progress_key, batch_ttl)
    end
end

-- Remove child from jobs set
local removed = redis.call("SREM", jobs_key, child_id)
if removed == 0 then
    -- Child wasn't in set, nothing to roll back
    return cjson.encode({success = true, not_found = true})
end

-- Decrement counters
local total = redis.call("HINCRBY", progress_key, "total", -1)
if total < 0 then
    redis.call("HSET", progress_key, "total", "0")
end

local pending = redis.call("HINCRBY", progress_key, "pending", -1)
if pending < 0 then
    pending = 0
    redis.call("HSET", progress_key, "pending", "0")
end

-- If pending is now 0, restore completion flags regardless of callbacks_pending.
-- This undoes the flag clearing that batch_add_child performs when reopening
-- a completed parent. Without this, a rollback could leave the parent with
-- pending=0 but complete_fired=false.
--
-- We must restore complete_fired even when callbacks are still pending because
-- batch_callback_complete.lua requires complete_fired=true to propagate to the
-- parent. If we wait for callbacks_pending==0, those callbacks would finish
-- but never propagate since complete_fired would still be false.
if pending == 0 then
    redis.call("HSET", progress_key, "complete_fired", "1")
    -- Restore success_fired only if the batch wasn't dead or invalidated
    if not bool_field("dead") and not bool_field("invalidated") then
        redis.call("HSET", progress_key, "success_fired", "1")
    end
end

return cjson.encode({success = true})
