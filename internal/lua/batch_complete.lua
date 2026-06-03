-- batch_complete.lua
-- Atomically update batch state when a job completes
-- KEYS[1] = batch state key
-- KEYS[2] = batch jobs set key
-- KEYS[3] = batch failed set key
-- KEYS[4] = dead batches set key
-- KEYS[5] = batch callbacks set key
-- KEYS[6] = known queues set key
-- ARGV[1] = job ID
-- ARGV[2] = "success", "failure", "death", "invalidated", or "empty_success"
-- ARGV[3] = default retry count for callback jobs
-- ARGV[4] = timestamp for callback job created_at and enqueued_at
-- ARGV[5] = queue key prefix (e.g., "senna:queue:")
-- Returns: JSON with completion status

local batch_key = KEYS[1]
local jobs_key = KEYS[2]
local failed_key = KEYS[3]
local dead_batches_key = KEYS[4]
local callbacks_key = KEYS[5]
local queues_key = KEYS[6]
local job_id = ARGV[1]
local result_type = ARGV[2]
local default_retry = tonumber(ARGV[3]) or 25
local now = ARGV[4]
local queue_prefix = ARGV[5]

local batch_type = redis.call('TYPE', batch_key).ok
if batch_type == 'none' then
    return '{"error":"batch_not_found"}'
end
if batch_type ~= 'string' then
    return redis.error_reply('batch key has type ' .. batch_type .. ', want string')
end

local jobs_type = redis.call('TYPE', jobs_key).ok
if jobs_type ~= 'none' and jobs_type ~= 'set' then
    return redis.error_reply('batch jobs key has type ' .. jobs_type .. ', want set')
end

-- Get current batch state
local batch_data = redis.call('GET', batch_key)
local batch = cjson.decode(batch_data)

-- For legacy batches that lack a pending count, initialize it from the jobs set.
if batch.pending == nil then
    batch.pending = redis.call('SCARD', jobs_key)
end

local remove_pending_job = false
if result_type == "empty_success" then
    if batch.pending ~= 0 then
        return '{"error":"batch_not_empty"}'
    end
    if batch.complete_fired then
        return '{"already_processed":true}'
    end
elseif result_type ~= "failure" then
    if redis.call('SISMEMBER', jobs_key, job_id) == 0 then
        return '{"already_processed":true}'
    end
    remove_pending_job = true
else
    if redis.call('SISMEMBER', jobs_key, job_id) == 0 then
        return '{"already_processed":true}'
    end
end

local callback_list = {}
local completed_now = false
local add_failed_job = false
local add_dead_batch = false

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

-- Update counters based on result
if result_type == "success" then
    batch.successes = (batch.successes or 0) + 1
    batch.pending = (batch.pending or 1) - 1
elseif result_type == "failure" then
    local failed_type = redis.call('TYPE', failed_key).ok
    if failed_type ~= 'none' and failed_type ~= 'set' then
        return redis.error_reply('batch failed key has type ' .. failed_type .. ', want set')
    end

    if redis.call('SISMEMBER', failed_key, job_id) == 0 then
        batch.failures = (batch.failures or 0) + 1
        add_failed_job = true
    end
elseif result_type == "death" then
    local failed_type = redis.call('TYPE', failed_key).ok
    if failed_type ~= 'none' and failed_type ~= 'set' then
        return redis.error_reply('batch failed key has type ' .. failed_type .. ', want set')
    end

    if redis.call('SISMEMBER', failed_key, job_id) == 0 then
        batch.failures = (batch.failures or 0) + 1
        add_failed_job = true
    end
    batch.pending = (batch.pending or 1) - 1

    -- Only track death state and fire callbacks if not invalidated
    if not batch.invalidated then
        batch.dead = true

        -- Add to dead batches set (for iteration)
        add_dead_batch = true

        -- Fire death callback only once
        if not batch.death_fired and batch.on_death then
            batch.death_fired = true
            make_callback("death", batch.on_death)
        end
    end
elseif result_type == "invalidated" then
    -- Child batch was invalidated - mark parent as invalidated too
    -- This prevents on_success from firing but doesn't trigger on_death
    batch.pending = (batch.pending or 1) - 1
    batch.invalidated = true
end

-- Check if all jobs are complete (pending == 0)
if batch.pending == 0 and not batch.complete_fired then
    batch.complete_fired = true

    -- Fire complete callback
    if batch.on_complete then
        make_callback("complete", batch.on_complete)
    end

    -- Fire success callback only if no deaths and not invalidated
    if not batch.dead and not batch.invalidated and not batch.success_fired and batch.on_success then
        batch.success_fired = true
        make_callback("success", batch.on_success)
    end
end

local callback_queue = 'default'
if type(batch.callback_queue) == "string" then
    callback_queue = batch.callback_queue
end

-- Track how many callbacks are pending
-- We only set completed_now (propagate to parent) when both jobs AND callbacks are done
local num_callbacks = #callback_list
if num_callbacks > 0 then
    local callbacks_type = redis.call('TYPE', callbacks_key).ok
    if callbacks_type ~= 'none' and callbacks_type ~= 'set' then
        return redis.error_reply('batch callbacks key has type ' .. callbacks_type .. ', want set')
    end

    local queues_type = redis.call('TYPE', queues_key).ok
    if queues_type ~= 'none' and queues_type ~= 'set' then
        return redis.error_reply('queues key has type ' .. queues_type .. ', want set')
    end

    local queue_key = queue_prefix .. callback_queue
    local queue_type = redis.call('TYPE', queue_key).ok
    if queue_type ~= 'none' and queue_type ~= 'list' then
        return redis.error_reply('queue key has type ' .. queue_type .. ', want list')
    end

    batch.callbacks_pending = (batch.callbacks_pending or 0) + num_callbacks
end

-- Only propagate to parent when:
-- 1. All jobs are complete (pending == 0)
-- 2. All callbacks are done (callbacks_pending == 0 or no callbacks were added)
-- 3. complete_fired is true (batch just completed)
if batch.pending == 0 and (batch.callbacks_pending or 0) == 0 and batch.complete_fired then
    completed_now = true
end

if add_dead_batch then
    local dead_batches_type = redis.call('TYPE', dead_batches_key).ok
    if dead_batches_type ~= 'none' and dead_batches_type ~= 'set' then
        return redis.error_reply('dead batches key has type ' .. dead_batches_type .. ', want set')
    end
end

if remove_pending_job then
    redis.call('SREM', jobs_key, job_id)
end

if add_failed_job then
    redis.call('SADD', failed_key, job_id)
    -- Set TTL on failed set (30 days = 2592000 seconds)
    redis.call('EXPIRE', failed_key, 2592000)
end

if add_dead_batch then
    redis.call('SADD', dead_batches_key, batch.id)
end

local callback_jobs = {}
if num_callbacks > 0 then
    local queue_key = queue_prefix .. callback_queue

    if type(batch.callback_seq) ~= "number" then
        batch.callback_seq = 0
    end

    for _, callback in ipairs(callback_list) do
        batch.callback_seq = batch.callback_seq + 1

        local args = {
            batch_id = batch.id
        }
        if type(batch.parent_id) == "string" and batch.parent_id ~= "" then
            args.parent_id = batch.parent_id
        end
        if type(callback.options) == "table" then
            for key, value in pairs(callback.options) do
                args[key] = value
            end
        end

        local callback_job_id = batch.id .. ":callback:" .. batch.callback_seq
        local callback_job = {
            jid = callback_job_id,
            ["class"] = callback.job_type,
            queue = callback_queue,
            args = args,
            retry = default_retry,
            retry_count = 0,
            created_at = now,
            enqueued_at = now,
            callback_bid = batch.id
        }

        table.insert(callback_jobs, {
            id = callback_job_id,
            data = cjson.encode(callback_job)
        })
    end
end

-- Save updated batch state
redis.call('SET', batch_key, cjson.encode(batch), 'KEEPTTL')

if num_callbacks > 0 then
    redis.call('SADD', queues_key, callback_queue)

    for _, callback_job in ipairs(callback_jobs) do
        redis.call('SADD', callbacks_key, callback_job.id)
        redis.call('LPUSH', queue_prefix .. callback_queue, callback_job.data)
    end
    redis.call('EXPIRE', callbacks_key, 2592000)
end

local result = {
    pending = batch.pending or 0,
    successes = batch.successes or 0,
    failures = batch.failures or 0,
    dead = batch.dead or false,
    invalidated = batch.invalidated or false,
    parent_id = batch.parent_id,
    completed_now = completed_now,
    callbacks_pending = batch.callbacks_pending or 0
}

return cjson.encode(result)
