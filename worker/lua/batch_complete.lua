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

-- Check if batch is invalidated
if batch.invalidated then
    return '{"invalidated":true}'
end

-- Remove job from pending set
local removed = redis.call('SREM', jobs_key, job_id)
if removed == 0 then
    -- Job already processed, skip
    return '{"already_processed":true}'
end

-- Update counters based on result
-- Track callbacks as simple strings to build JSON manually
local callback_list = {}

if result_type == "success" then
    batch.successes = (batch.successes or 0) + 1
    batch.pending = (batch.pending or 1) - 1
elseif result_type == "death" then
    batch.failures = (batch.failures or 0) + 1
    batch.pending = (batch.pending or 1) - 1
    batch.dead = true

    -- Track failed job ID
    redis.call('SADD', failed_key, job_id)

    -- Add to dead batches set (for iteration)
    redis.call('SADD', dead_batches_key, batch.id)

    -- Fire death callback only once
    if not batch.death_fired and batch.on_death then
        batch.death_fired = true
        local cb = '{"callback_type":"death","job_type":"' .. batch.on_death.job_type .. '"'
        if batch.on_death.options then
            cb = cb .. ',"options":' .. cjson.encode(batch.on_death.options)
        end
        cb = cb .. '}'
        table.insert(callback_list, cb)
    end
end

-- Check if all jobs are complete (pending == 0)
if batch.pending == 0 and not batch.complete_fired then
    batch.complete_fired = true

    -- Fire complete callback
    if batch.on_complete then
        local cb = '{"callback_type":"complete","job_type":"' .. batch.on_complete.job_type .. '"'
        if batch.on_complete.options then
            cb = cb .. ',"options":' .. cjson.encode(batch.on_complete.options)
        end
        cb = cb .. '}'
        table.insert(callback_list, cb)
    end

    -- Fire success callback only if no deaths
    if not batch.dead and not batch.success_fired and batch.on_success then
        batch.success_fired = true
        local cb = '{"callback_type":"success","job_type":"' .. batch.on_success.job_type .. '"'
        if batch.on_success.options then
            cb = cb .. ',"options":' .. cjson.encode(batch.on_success.options)
        end
        cb = cb .. '}'
        table.insert(callback_list, cb)
    end
end

-- Save updated batch state
redis.call('SET', batch_key, cjson.encode(batch), 'KEEPTTL')

-- Build JSON result manually to ensure callbacks is always an array
local callbacks_json = '[' .. table.concat(callback_list, ',') .. ']'
local result = '{"callbacks":' .. callbacks_json
result = result .. ',"pending":' .. (batch.pending or 0)
result = result .. ',"successes":' .. (batch.successes or 0)
result = result .. ',"failures":' .. (batch.failures or 0)
result = result .. ',"dead":' .. (batch.dead and 'true' or 'false')
result = result .. ',"callback_queue":"' .. (batch.callback_queue or 'default') .. '"}'

return result
