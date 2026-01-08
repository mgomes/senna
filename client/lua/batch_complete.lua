-- batch_complete.lua
-- Atomically update batch state when a job completes
-- KEYS[1] = batch state key
-- KEYS[2] = batch jobs set key
-- KEYS[3] = batch failed set key
-- KEYS[4] = dead batches set key
-- ARGV[1] = job ID
-- ARGV[2] = "success" or "death"
-- Returns: JSON with callbacks to fire

local batch_key = KEYS[1]
local jobs_key = KEYS[2]
local failed_key = KEYS[3]
local dead_batches_key = KEYS[4]
local job_id = ARGV[1]
local result_type = ARGV[2]

-- Get current batch state
local batch_data = redis.call('GET', batch_key)
if not batch_data then
    return '{"error":"batch_not_found"}'
end

local batch = cjson.decode(batch_data)

-- For legacy batches that lack a pending count, initialize it from the jobs set.
if batch.pending == nil then
    batch.pending = redis.call('SCARD', jobs_key)
end

-- Remove job from pending set
local removed = 0
if result_type ~= "failure" then
    removed = redis.call('SREM', jobs_key, job_id)
    if removed == 0 then
        -- Job already processed, skip
        return '{"already_processed":true}'
    end
else
    -- For failures (retries), ensure the job is part of the batch without removing it.
    local member = redis.call('SISMEMBER', jobs_key, job_id)
    if member == 0 then
        return '{"already_processed":true}'
    end
end

-- Track callbacks as encoded JSON strings (each is a complete JSON object)
local callback_list = {}
local completed_now = false

-- Helper to build a callback object with proper escaping
local function make_callback(callback_type, callback_config)
    local cb = {
        callback_type = callback_type,
        job_type = callback_config.job_type
    }
    if callback_config.options then
        cb.options = callback_config.options
    end
    return cjson.encode(cb)
end

-- Update counters based on result
if result_type == "success" then
    batch.successes = (batch.successes or 0) + 1
    batch.pending = (batch.pending or 1) - 1
elseif result_type == "failure" then
    local newly_failed = redis.call('SADD', failed_key, job_id)
    if newly_failed == 1 then
        batch.failures = (batch.failures or 0) + 1
        -- Set TTL on failed set (30 days = 2592000 seconds)
        redis.call('EXPIRE', failed_key, 2592000)
    end
elseif result_type == "death" then
    local newly_failed = redis.call('SADD', failed_key, job_id)
    if newly_failed == 1 then
        batch.failures = (batch.failures or 0) + 1
        -- Set TTL on failed set (30 days = 2592000 seconds)
        redis.call('EXPIRE', failed_key, 2592000)
    end
    batch.pending = (batch.pending or 1) - 1

    -- Only track death state and fire callbacks if not invalidated
    if not batch.invalidated then
        batch.dead = true

        -- Track failed job ID
        redis.call('SADD', failed_key, job_id)

        -- Add to dead batches set (for iteration)
        redis.call('SADD', dead_batches_key, batch.id)

        -- Fire death callback only once
        if not batch.death_fired and batch.on_death then
            batch.death_fired = true
            table.insert(callback_list, make_callback("death", batch.on_death))
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
        table.insert(callback_list, make_callback("complete", batch.on_complete))
    end

    -- Fire success callback only if no deaths and not invalidated
    if not batch.dead and not batch.invalidated and not batch.success_fired and batch.on_success then
        batch.success_fired = true
        table.insert(callback_list, make_callback("success", batch.on_success))
    end
end

-- Track how many callbacks are pending
-- We only set completed_now (propagate to parent) when both jobs AND callbacks are done
local num_callbacks = #callback_list
if num_callbacks > 0 then
    batch.callbacks_pending = (batch.callbacks_pending or 0) + num_callbacks
end

-- Only propagate to parent when:
-- 1. All jobs are complete (pending == 0)
-- 2. All callbacks are done (callbacks_pending == 0 or no callbacks were added)
-- 3. complete_fired is true (batch just completed)
if batch.pending == 0 and (batch.callbacks_pending or 0) == 0 and batch.complete_fired then
    completed_now = true
end

-- Save updated batch state
redis.call('SET', batch_key, cjson.encode(batch), 'KEEPTTL')

-- Build result with proper escaping
-- Use manual array construction to ensure callbacks is always an array (not object when empty)
local callbacks_json = '[' .. table.concat(callback_list, ',') .. ']'
local result = {
    pending = batch.pending or 0,
    successes = batch.successes or 0,
    failures = batch.failures or 0,
    dead = batch.dead or false,
    invalidated = batch.invalidated or false,
    callback_queue = batch.callback_queue or 'default',
    parent_id = batch.parent_id,
    completed_now = completed_now,
    callbacks_pending = batch.callbacks_pending or 0
}
-- Encode result, then inject the callbacks array to ensure it stays an array
local result_json = cjson.encode(result)
-- Insert callbacks array before the closing brace
result_json = result_json:sub(1, -2) .. ',"callbacks":' .. callbacks_json .. '}'

return result_json
