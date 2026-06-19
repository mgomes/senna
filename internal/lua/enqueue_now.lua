-- enqueue_now.lua
-- Atomically register a queue and enqueue a job.
-- KEYS[1] = queues set key
-- KEYS[2] = queue list key
-- ARGV[1] = queue name
-- ARGV[2] = job data
-- Returns: 1 when enqueued.

local queues_type = redis.call('TYPE', KEYS[1]).ok
if queues_type ~= 'none' and queues_type ~= 'set' then
    return redis.error_reply('queues key has type ' .. queues_type .. ', want set')
end

local queue_type = redis.call('TYPE', KEYS[2]).ok
if queue_type ~= 'none' and queue_type ~= 'list' then
    return redis.error_reply('queue key has type ' .. queue_type .. ', want list')
end

redis.call('SADD', KEYS[1], ARGV[1])
redis.call('LPUSH', KEYS[2], ARGV[2])

return 1
