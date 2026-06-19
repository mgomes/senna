-- batch_callback_complete.lua
-- Called when a batch callback job completes.
-- This decrements callbacks_pending and checks if we should propagate to parent.
-- KEYS[1] = batch metadata key
-- KEYS[2] = batch progress hash key
-- KEYS[3] = batch callbacks set key
-- ARGV[1] = callback job ID
-- Returns: JSON with propagation info

local batch_key = KEYS[1]
local progress_key = KEYS[2]
local callbacks_key = KEYS[3]
local job_id = ARGV[1]

local function bool_value(value)
    return value == "1" or value == "true"
end

local function bool_field(field)
    return bool_value(redis.call("HGET", progress_key, field))
end

local function number_field(field, default)
    local value = redis.call("HGET", progress_key, field)
    if not value then
        return default or 0
    end
    return tonumber(value) or default or 0
end

local batch_type = redis.call("TYPE", batch_key).ok
if batch_type == "none" then
    return '{"error":"batch_not_found"}'
end
if batch_type ~= "string" then
    return redis.error_reply("batch key has type " .. batch_type .. ", want string")
end

local progress_type = redis.call("TYPE", progress_key).ok
if progress_type ~= "none" and progress_type ~= "hash" then
    return redis.error_reply("batch progress key has type " .. progress_type .. ", want hash")
end

local callbacks_type = redis.call("TYPE", callbacks_key).ok
if callbacks_type ~= "none" and callbacks_type ~= "set" then
    return redis.error_reply("batch callbacks key has type " .. callbacks_type .. ", want set")
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

if progress_type == "none" then
    local legacy = load_batch()
    redis.call("HSET", progress_key,
        "id", legacy.id or "",
        "parent_id", legacy.parent_id or "",
        "callback_queue", legacy.callback_queue or "",
        "total", legacy.total or 0,
        "pending", legacy.pending or 0,
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
    local batch_ttl = redis.call("PTTL", batch_key)
    if batch_ttl > 0 then
        redis.call("PEXPIRE", progress_key, batch_ttl)
    end
end

local parent_id = redis.call("HGET", progress_key, "parent_id")
if not parent_id or parent_id == "" then
    parent_id = nil
end

-- Remove callback job ID from set - if already removed, this is a duplicate
local removed = redis.call("SREM", callbacks_key, job_id)
if removed == 0 then
    -- Already processed, return current state without decrementing
    return cjson.encode({
        already_processed = true,
        callbacks_pending = number_field("callbacks_pending", 0),
        pending = number_field("pending", 0),
        should_propagate = false,
        parent_id = parent_id,
        dead = bool_field("dead"),
        invalidated = bool_field("invalidated")
    })
end

-- Decrement callbacks_pending
local callbacks_pending = redis.call("HINCRBY", progress_key, "callbacks_pending", -1)
if callbacks_pending < 0 then
    callbacks_pending = 0
    redis.call("HSET", progress_key, "callbacks_pending", "0")
end

-- Check if we should now propagate to parent
-- This happens when all jobs AND all callbacks are complete
local should_propagate = false
local pending = number_field("pending", 0)
if pending == 0 and callbacks_pending == 0 and bool_field("complete_fired") then
    should_propagate = true
end

local result = {
    callbacks_pending = callbacks_pending,
    pending = pending,
    should_propagate = should_propagate,
    parent_id = parent_id,
    dead = bool_field("dead"),
    invalidated = bool_field("invalidated")
}

return cjson.encode(result)
