-- batch_enqueue_jobs.lua
-- Atomically track and enqueue the initial jobs for a batch.
-- KEYS[1] = batch jobs set key
-- ARGV[1] = number of jobs to enqueue
-- ARGV[2] = batch jobs set ttl seconds
-- ARGV[3..n] = alternating: job_id, queue_key, job_data (3 args per job)
-- Returns: JSON with result

local jobs_key = KEYS[1]
local num_jobs = tonumber(ARGV[1])
local ttl_seconds = tonumber(ARGV[2])

local jobs_type = redis.call('TYPE', jobs_key).ok
if jobs_type ~= 'none' and jobs_type ~= 'set' then
    return redis.error_reply('batch jobs key has type ' .. jobs_type .. ', want set')
end

-- Args are: job_id, queue_key, job_data (3 per job). Validate every queue
-- before mutating so wrong-type destinations cannot leave partial queue writes.
for i = 0, num_jobs - 1 do
    local base = 3 + (i * 3)
    local queue_key = ARGV[base + 1]

    local queue_type = redis.call('TYPE', queue_key).ok
    if queue_type ~= 'none' and queue_type ~= 'list' then
        return redis.error_reply('queue key has type ' .. queue_type .. ', want list')
    end
end

for i = 0, num_jobs - 1 do
    local base = 3 + (i * 3)
    local job_id = ARGV[base]
    local queue_key = ARGV[base + 1]
    local job_data = ARGV[base + 2]

    redis.call('SADD', jobs_key, job_id)
    redis.call('LPUSH', queue_key, job_data)
end

if ttl_seconds and ttl_seconds > 0 then
    redis.call('EXPIRE', jobs_key, ttl_seconds)
end

return cjson.encode({success = true})
