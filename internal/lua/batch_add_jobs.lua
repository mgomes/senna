-- batch_add_jobs.lua
-- Atomically add jobs to an existing batch and enqueue them.
-- KEYS[1] = batch metadata key
-- KEYS[2] = batch progress hash key
-- KEYS[3] = batch jobs set key
-- ARGV[1] = number of jobs to add
-- ARGV[2..n] = alternating: job_id, queue_key, job_data (3 args per job)
-- Returns: JSON with result

local batch_key = KEYS[1]
local progress_key = KEYS[2]
local jobs_key = KEYS[3]
local num_jobs = tonumber(ARGV[1])

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

-- Check if batch is invalidated
if bool_field("invalidated") then
    return cjson.encode({error = "batch_invalidated"})
end

-- Check if batch is already complete (callbacks fired)
if bool_field("complete_fired") then
    return cjson.encode({error = "batch_complete"})
end

-- Args are: job_id, queue_key, job_data (3 per job). Validate every queue
-- before mutating so wrong-type destinations cannot leave phantom batch jobs.
for i = 0, num_jobs - 1 do
    local base = 2 + (i * 3)
    local queue_key = ARGV[base + 1]

    local queue_type = redis.call("TYPE", queue_key).ok
    if queue_type ~= "none" and queue_type ~= "list" then
        return redis.error_reply("queue key has type " .. queue_type .. ", want list")
    end
end

-- Process jobs: add to pending set and enqueue atomically
for i = 0, num_jobs - 1 do
    local base = 2 + (i * 3)
    local job_id = ARGV[base]
    local queue_key = ARGV[base + 1]
    local job_data = ARGV[base + 2]

    redis.call("SADD", jobs_key, job_id)
    redis.call("LPUSH", queue_key, job_data)
end

if batch_ttl > 0 then
    redis.call("PEXPIRE", jobs_key, batch_ttl)
end

local total = redis.call("HINCRBY", progress_key, "total", num_jobs)
local pending = redis.call("HINCRBY", progress_key, "pending", num_jobs)

return cjson.encode({
    success = true,
    total = total,
    pending = pending
})
