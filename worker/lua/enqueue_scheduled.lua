-- Atomically pop due jobs from a sorted set and push them to their queues
-- KEYS[1] = sorted set key (scheduled or retry)
-- KEYS[2] = queues set key (to track known queues)
-- ARGV[1] = max score (current timestamp)
-- ARGV[2] = limit (max jobs to pop)
-- ARGV[3] = queue key prefix (e.g., "senna:queue:")
-- Returns: number of jobs enqueued

local zset_key = KEYS[1]
local queues_key = KEYS[2]
local max_score = ARGV[1]
local limit = tonumber(ARGV[2])
local queue_prefix = ARGV[3]

-- Get jobs with score <= max_score
local items = redis.call('ZRANGEBYSCORE', zset_key, '-inf', max_score, 'LIMIT', 0, limit)

if #items == 0 then
    return 0
end

-- Parse each job, remove from zset, and push to queue
-- Use pcall to safely handle malformed JSON without aborting the script
local enqueued = 0
for _, data in ipairs(items) do
    local ok, job = pcall(cjson.decode, data)
    if ok and type(job) == "table" then
        local queue = job.queue or "default"
        if type(queue) == "string" and queue ~= "" then
            local queue_key = queue_prefix .. queue

            -- Remove this item from the sorted set
            redis.call('ZREM', zset_key, data)

            -- Add queue to known queues set
            redis.call('SADD', queues_key, queue)

            -- Push job to queue
            redis.call('LPUSH', queue_key, data)
            enqueued = enqueued + 1
        else
            -- Invalid queue value, remove but don't enqueue (job is lost)
            redis.call('ZREM', zset_key, data)
        end
    else
        -- Malformed JSON, remove but don't enqueue (job is lost)
        redis.call('ZREM', zset_key, data)
    end
end

return enqueued
