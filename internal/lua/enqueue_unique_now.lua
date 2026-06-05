-- enqueue_unique_now.lua
-- Atomically claim a unique key and enqueue a job.
-- KEYS[1] = unique key
-- KEYS[2] = queues set key
-- KEYS[3] = queue list key
-- ARGV[1] = job id
-- ARGV[2] = unique ttl milliseconds
-- ARGV[3] = queue name
-- ARGV[4] = job data
-- Returns: 1 if enqueued, 0 if the unique key already exists.

local unique_type = redis.call('TYPE', KEYS[1]).ok
if unique_type ~= 'none' and unique_type ~= 'string' then
    return redis.error_reply('unique key has type ' .. unique_type .. ', want string')
end
if unique_type == 'string' then
    return 0
end

local queues_type = redis.call('TYPE', KEYS[2]).ok
if queues_type ~= 'none' and queues_type ~= 'set' then
    return redis.error_reply('queues key has type ' .. queues_type .. ', want set')
end

local queue_type = redis.call('TYPE', KEYS[3]).ok
if queue_type ~= 'none' and queue_type ~= 'list' then
    return redis.error_reply('queue key has type ' .. queue_type .. ', want list')
end

local claimed = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', tonumber(ARGV[2]))
if not claimed then
    return 0
end

redis.call('SADD', KEYS[2], ARGV[3])
redis.call('LPUSH', KEYS[3], ARGV[4])

return 1
