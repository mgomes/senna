--!df flags=allow-undeclared-keys
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
    local should_enqueue = false
    local queue = "default"

    if ok and type(job) == "table" then
        -- Use queue from job, defaulting to "default" if nil/missing
        -- Empty string is allowed (preserves existing behavior)
        if job.queue == nil then
            should_enqueue = true
        elseif type(job.queue) == "string" then
            queue = job.queue
            should_enqueue = true
        end
        -- Non-string queue value (e.g., number, boolean) falls through with should_enqueue = false
    end

    -- Always remove from the sorted set
    redis.call('ZREM', zset_key, data)

    if should_enqueue then
        local queue_key = queue_prefix .. queue

        -- Add queue to known queues set
        redis.call('SADD', queues_key, queue)

        -- Push job to queue
        redis.call('LPUSH', queue_key, data)
        enqueued = enqueued + 1
    end
end

return enqueued
