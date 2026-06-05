-- enqueue_unique_at.lua
-- Atomically claim a unique key and schedule a job.
-- KEYS[1] = unique key
-- KEYS[2] = scheduled zset key
-- ARGV[1] = job id
-- ARGV[2] = unique ttl milliseconds
-- ARGV[3] = scheduled score
-- ARGV[4] = job data
-- Returns: 1 if scheduled, 0 if the unique key already exists.

local unique_type = redis.call('TYPE', KEYS[1]).ok
if unique_type ~= 'none' and unique_type ~= 'string' then
    return redis.error_reply('unique key has type ' .. unique_type .. ', want string')
end
if unique_type == 'string' then
    return 0
end

local scheduled_type = redis.call('TYPE', KEYS[2]).ok
if scheduled_type ~= 'none' and scheduled_type ~= 'zset' then
    return redis.error_reply('scheduled key has type ' .. scheduled_type .. ', want zset')
end

local claimed = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', tonumber(ARGV[2]))
if not claimed then
    return 0
end

redis.call('ZADD', KEYS[2], tonumber(ARGV[3]), ARGV[4])

return 1
