-- batch_complete.lua
-- Atomically update batch progress when a job completes.
-- KEYS[1] = batch metadata key
-- KEYS[2] = batch progress hash key
-- KEYS[3] = batch jobs set key
-- KEYS[4] = batch failed set key
-- KEYS[5] = dead batches set key
-- KEYS[6] = batch callbacks set key
-- KEYS[7] = known queues set key
-- ARGV[1] = job ID
-- ARGV[2] = "success", "failure", "death", "invalidated", or "empty_success"
-- ARGV[3] = default retry count for callback jobs
-- ARGV[4] = timestamp for callback job created_at and enqueued_at
-- ARGV[5] = queue key prefix (e.g., "senna:queue:")
-- Returns: JSON with completion status

local batch_key = KEYS[1]
local progress_key = KEYS[2]
local jobs_key = KEYS[3]
local failed_key = KEYS[4]
local dead_batches_key = KEYS[5]
local callbacks_key = KEYS[6]
local queues_key = KEYS[7]
local job_id = ARGV[1]
local result_type = ARGV[2]
local default_retry = tonumber(ARGV[3]) or 25
local now = ARGV[4]
local queue_prefix = ARGV[5]

local function bool_value(value)
    return value == "1" or value == "true"
end

local batch_type = redis.call("TYPE", batch_key).ok
if batch_type == "none" then
    return '{"error":"batch_not_found"}'
end
if batch_type ~= "string" then
    return redis.error_reply("batch key has type " .. batch_type .. ", want string")
end

local jobs_type = redis.call("TYPE", jobs_key).ok
if jobs_type ~= "none" and jobs_type ~= "set" then
    return redis.error_reply("batch jobs key has type " .. jobs_type .. ", want set")
end

local progress_type = redis.call("TYPE", progress_key).ok
if progress_type ~= "none" and progress_type ~= "hash" then
    return redis.error_reply("batch progress key has type " .. progress_type .. ", want hash")
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
    local batch_ttl = redis.call("PTTL", batch_key)
    if batch_ttl > 0 then
        redis.call("PEXPIRE", progress_key, batch_ttl)
    end
end

local progress_values = redis.call("HMGET", progress_key,
    "pending",
    "successes",
    "failures",
    "callbacks_pending",
    "dead",
    "invalidated",
    "death_fired",
    "complete_fired",
    "success_fired",
    "id",
    "parent_id"
)

local function progress_number(index, default)
    local value = progress_values[index]
    if not value then
        return default or 0
    end
    return tonumber(value) or default or 0
end

local function progress_bool(index)
    return bool_value(progress_values[index])
end

local function progress_string(index)
    local value = progress_values[index]
    if not value or value == "" then
        return nil
    end
    return value
end

local pending_before = progress_number(1, redis.call("SCARD", jobs_key))
local pending_after = pending_before
local successes_before = progress_number(2, 0)
local failures_before = progress_number(3, 0)
local callbacks_pending_before = progress_number(4, 0)
local callbacks_pending_after = callbacks_pending_before
local dead_after = progress_bool(5)
local invalidated_after = progress_bool(6)
local death_fired_after = progress_bool(7)
local complete_fired_after = progress_bool(8)
local success_fired_after = progress_bool(9)
local batch_id = progress_string(10)
local parent_id = progress_string(11)

local remove_pending_job = false
if result_type == "empty_success" then
    if pending_before ~= 0 then
        return '{"error":"batch_not_empty"}'
    end
    if complete_fired_after then
        return '{"already_processed":true}'
    end
elseif result_type ~= "failure" then
    if redis.call("SISMEMBER", jobs_key, job_id) == 0 then
        return '{"already_processed":true}'
    end
    remove_pending_job = true
else
    if redis.call("SISMEMBER", jobs_key, job_id) == 0 then
        return '{"already_processed":true}'
    end
end

local callback_list = {}
local add_failed_job = false
local add_dead_batch = false
local successes_delta = 0
local failures_delta = 0

local function make_callback(callback_type, callback_config)
    if callback_config == nil then
        return
    end

    local job_type = ""
    if type(callback_config.job_type) == "string" then
        job_type = callback_config.job_type
    end

    table.insert(callback_list, {
        callback_type = callback_type,
        job_type = job_type,
        options = callback_config.options
    })
end

if result_type == "success" then
    successes_delta = 1
    pending_after = pending_after - 1
elseif result_type == "failure" then
    local failed_type = redis.call("TYPE", failed_key).ok
    if failed_type ~= "none" and failed_type ~= "set" then
        return redis.error_reply("batch failed key has type " .. failed_type .. ", want set")
    end

    if redis.call("SISMEMBER", failed_key, job_id) == 0 then
        failures_delta = 1
        add_failed_job = true
    end
elseif result_type == "death" then
    local failed_type = redis.call("TYPE", failed_key).ok
    if failed_type ~= "none" and failed_type ~= "set" then
        return redis.error_reply("batch failed key has type " .. failed_type .. ", want set")
    end

    if redis.call("SISMEMBER", failed_key, job_id) == 0 then
        failures_delta = 1
        add_failed_job = true
    end
    pending_after = pending_after - 1

    if not invalidated_after then
        dead_after = true
        add_dead_batch = true

        if not death_fired_after then
            local metadata = load_batch()
            if metadata.on_death then
                death_fired_after = true
                make_callback("death", metadata.on_death)
            end
        end
    end
elseif result_type == "invalidated" then
    pending_after = pending_after - 1
    invalidated_after = true
end

if pending_after < 0 then
    pending_after = 0
end

if pending_after == 0 and not complete_fired_after then
    complete_fired_after = true

    local metadata = load_batch()
    if metadata.on_complete then
        make_callback("complete", metadata.on_complete)
    end

    if not dead_after and not invalidated_after and not success_fired_after and metadata.on_success then
        success_fired_after = true
        make_callback("success", metadata.on_success)
    end
end

local num_callbacks = #callback_list
local callback_queue
if num_callbacks > 0 then
    callback_queue = redis.call("HGET", progress_key, "callback_queue")
    if not callback_queue or callback_queue == "" then
        local metadata = load_batch()
        if type(metadata.callback_queue) == "string" and metadata.callback_queue ~= "" then
            callback_queue = metadata.callback_queue
        else
            callback_queue = "default"
        end
    end

    local callbacks_type = redis.call("TYPE", callbacks_key).ok
    if callbacks_type ~= "none" and callbacks_type ~= "set" then
        return redis.error_reply("batch callbacks key has type " .. callbacks_type .. ", want set")
    end

    local queues_type = redis.call("TYPE", queues_key).ok
    if queues_type ~= "none" and queues_type ~= "set" then
        return redis.error_reply("queues key has type " .. queues_type .. ", want set")
    end

    local queue_key = queue_prefix .. callback_queue
    local queue_type = redis.call("TYPE", queue_key).ok
    if queue_type ~= "none" and queue_type ~= "list" then
        return redis.error_reply("queue key has type " .. queue_type .. ", want list")
    end

    callbacks_pending_after = callbacks_pending_after + num_callbacks
end

if add_dead_batch then
    local dead_batches_type = redis.call("TYPE", dead_batches_key).ok
    if dead_batches_type ~= "none" and dead_batches_type ~= "set" then
        return redis.error_reply("dead batches key has type " .. dead_batches_type .. ", want set")
    end
end

local completed_now = false
if pending_after == 0 and callbacks_pending_after == 0 and complete_fired_after then
    completed_now = true
end

if successes_delta ~= 0 then
    redis.call("HINCRBY", progress_key, "successes", successes_delta)
end
if failures_delta ~= 0 then
    redis.call("HINCRBY", progress_key, "failures", failures_delta)
end

local progress_updates = {}
local function update_progress(field, value)
    table.insert(progress_updates, field)
    table.insert(progress_updates, value)
end

if result_type == "success" or result_type == "death" or result_type == "invalidated" then
    update_progress("pending", pending_after)
end
if dead_after then
    update_progress("dead", "1")
end
if invalidated_after then
    update_progress("invalidated", "1")
end
if death_fired_after then
    update_progress("death_fired", "1")
end
if complete_fired_after then
    update_progress("complete_fired", "1")
end
if success_fired_after then
    update_progress("success_fired", "1")
end
if #progress_updates > 0 then
    redis.call("HSET", progress_key, unpack(progress_updates))
end
if num_callbacks > 0 then
    redis.call("HINCRBY", progress_key, "callbacks_pending", num_callbacks)
end

if remove_pending_job then
    redis.call("SREM", jobs_key, job_id)
end

if add_failed_job then
    redis.call("SADD", failed_key, job_id)
    redis.call("EXPIRE", failed_key, 2592000)
end

if not batch_id then
    batch_id = load_batch().id
end

if add_dead_batch then
    redis.call("SADD", dead_batches_key, batch_id)
end

local callback_jobs = {}
if num_callbacks > 0 then
    for _, callback in ipairs(callback_list) do
        local callback_seq = redis.call("HINCRBY", progress_key, "callback_seq", 1)

        local args = {
            batch_id = batch_id
        }
        if parent_id and parent_id ~= "" then
            args.parent_id = parent_id
        end
        if type(callback.options) == "table" then
            for key, value in pairs(callback.options) do
                args[key] = value
            end
        end

        local callback_job_id = batch_id .. ":callback:" .. callback_seq
        local callback_job = {
            jid = callback_job_id,
            ["class"] = callback.job_type,
            queue = callback_queue,
            args = args,
            retry = default_retry,
            retry_count = 0,
            created_at = now,
            enqueued_at = now,
            callback_bid = batch_id
        }

        table.insert(callback_jobs, {
            id = callback_job_id,
            data = cjson.encode(callback_job)
        })
    end
end

if num_callbacks > 0 then
    redis.call("SADD", queues_key, callback_queue)

    for _, callback_job in ipairs(callback_jobs) do
        redis.call("SADD", callbacks_key, callback_job.id)
        redis.call("LPUSH", queue_prefix .. callback_queue, callback_job.data)
    end
    redis.call("EXPIRE", callbacks_key, 2592000)
end

local result = {
    pending = pending_after,
    successes = successes_before + successes_delta,
    failures = failures_before + failures_delta,
    dead = dead_after,
    invalidated = invalidated_after,
    parent_id = parent_id,
    completed_now = completed_now,
    callbacks_pending = callbacks_pending_after
}

return cjson.encode(result)
