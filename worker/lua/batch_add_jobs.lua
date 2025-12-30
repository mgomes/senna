-- batch_add_jobs.lua
-- Atomically add jobs to an existing batch
-- KEYS[1] = batch state key
-- KEYS[2] = batch jobs set key
-- ARGV[1] = number of jobs to add
-- ARGV[2...] = job IDs to add
-- Returns: JSON with result

local batch_key = KEYS[1]
local jobs_key = KEYS[2]
local num_jobs = tonumber(ARGV[1])

-- Get current batch state
local batch_data = redis.call('GET', batch_key)
if not batch_data then
    return cjson.encode({error = "batch_not_found"})
end

local batch = cjson.decode(batch_data)

-- Check if batch is invalidated
if batch.invalidated then
    return cjson.encode({error = "batch_invalidated"})
end

-- Check if batch is already complete (callbacks fired)
if batch.complete_fired then
    return cjson.encode({error = "batch_complete"})
end

-- Add job IDs to the pending set and update counters
for i = 2, num_jobs + 1 do
    local job_id = ARGV[i]
    redis.call('SADD', jobs_key, job_id)
end

batch.total = (batch.total or 0) + num_jobs
batch.pending = (batch.pending or 0) + num_jobs

-- Save updated batch state
redis.call('SET', batch_key, cjson.encode(batch), 'KEEPTTL')

return cjson.encode({
    success = true,
    total = batch.total,
    pending = batch.pending
})
