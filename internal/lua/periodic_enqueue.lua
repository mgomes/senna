-- periodic_enqueue.lua
-- Atomically claim a periodic slot and enqueue its job.
-- KEYS[1] = periodic lock key
-- KEYS[2] = queues set key
-- KEYS[3] = queue list key
-- ARGV[1] = lock value
-- ARGV[2] = lock ttl seconds
-- ARGV[3] = queue name
-- ARGV[4] = job data
-- Returns: 1 if enqueued, 0 if the slot was already claimed.

local lock_type = redis.call('TYPE', KEYS[1]).ok
if lock_type ~= 'none' and lock_type ~= 'string' then
    return redis.error_reply('periodic lock key has type ' .. lock_type .. ', want string')
end
if lock_type == 'string' then
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

local claimed = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', tonumber(ARGV[2]))
if not claimed then
    return 0
end

redis.call('SADD', KEYS[2], ARGV[3])
redis.call('LPUSH', KEYS[3], ARGV[4])

return 1
