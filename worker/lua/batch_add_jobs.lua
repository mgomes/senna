-- batch_add_jobs.lua
-- Atomically add jobs to an existing batch and enqueue them
-- KEYS[1] = batch state key
-- KEYS[2] = batch jobs set key
-- ARGV[1] = number of jobs to add
-- ARGV[2..n] = alternating: job_id, queue_key, job_data (3 args per job)
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

-- Process jobs: add to pending set and enqueue atomically
-- Args are: job_id, queue_key, job_data (3 per job)
for i = 0, num_jobs - 1 do
    local base = 2 + (i * 3)
    local job_id = ARGV[base]
    local queue_key = ARGV[base + 1]
    local job_data = ARGV[base + 2]

    -- Add job ID to pending set
    redis.call('SADD', jobs_key, job_id)
    -- Enqueue the job
    redis.call('LPUSH', queue_key, job_data)
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
