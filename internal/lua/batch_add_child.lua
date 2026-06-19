-- batch_add_child.lua
-- Add a child batch to a parent batch without enqueueing jobs.
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
    return cjson.encode({error = "batch_not_found"})
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

if bool_field("invalidated") then
    return cjson.encode({error = "batch_invalidated"})
end

-- Note: We allow adding children even after complete_fired.
-- This enables the Sidekiq-style workflow pattern where callbacks
-- "reopen" the parent batch to add the next step.
-- Adding a child increments pending, so we reset complete_fired.

-- Only increment counters if child was newly added (idempotency)
local added = redis.call("SADD", jobs_key, child_id)
if added == 0 then
    -- Child already exists, nothing to do
    return cjson.encode({success = true, already_exists = true})
end

-- Ensure the jobs set has a TTL matching the batch.
-- For empty parent batches, SADD creates this set without expiration.
-- Copy the batch's TTL to prevent unbounded memory growth.
if batch_ttl > 0 then
    redis.call("PEXPIRE", jobs_key, batch_ttl)
end

redis.call("HINCRBY", progress_key, "total", 1)
redis.call("HINCRBY", progress_key, "pending", 1)

-- Reset completion flags since we have new pending work
-- This allows callbacks to fire again after the new children complete
if bool_field("complete_fired") then
    redis.call("HSET", progress_key,
        "complete_fired", "0",
        "success_fired", "0"
    )
end

return cjson.encode({success = true})
