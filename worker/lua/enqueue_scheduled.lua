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

local zset_type = redis.call('TYPE', zset_key).ok
if zset_type == 'none' then
    return 0
end
if zset_type ~= 'zset' then
    return redis.error_reply('source key has type ' .. zset_type .. ', want zset')
end

-- Get jobs with score <= max_score
local items = redis.call('ZRANGEBYSCORE', zset_key, '-inf', max_score, 'LIMIT', 0, limit)

if #items == 0 then
    return 0
end

-- Use pcall to safely handle malformed JSON without aborting the script
local targets = {}
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

    if should_enqueue then
        local queue_key = queue_prefix .. queue

        local queue_type = redis.call('TYPE', queue_key).ok
        if queue_type ~= 'none' and queue_type ~= 'list' then
            return redis.error_reply('queue key has type ' .. queue_type .. ', want list')
        end

        table.insert(targets, {queue = queue, key = queue_key, data = data})
    end
end

if #targets > 0 then
    local queues_type = redis.call('TYPE', queues_key).ok
    if queues_type ~= 'none' and queues_type ~= 'set' then
        return redis.error_reply('queues key has type ' .. queues_type .. ', want set')
    end
end

-- Always remove due items from the sorted set after validation. Malformed or
-- unsupported payloads are discarded, preserving the previous behavior.
for _, data in ipairs(items) do
    redis.call('ZREM', zset_key, data)
end

for _, target in ipairs(targets) do
    redis.call('SADD', queues_key, target.queue)
    redis.call('LPUSH', target.key, target.data)
end

return #targets
