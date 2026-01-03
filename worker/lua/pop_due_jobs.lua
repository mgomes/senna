-- Atomically pop jobs from a sorted set where score <= max_score
-- KEYS[1] = sorted set key
-- ARGV[1] = max score (current timestamp)
-- ARGV[2] = limit (max jobs to pop)
-- Returns: array of job data strings

local key = KEYS[1]
local max_score = ARGV[1]
local limit = tonumber(ARGV[2])

-- Get jobs with score <= max_score
local items = redis.call('ZRANGEBYSCORE', key, '-inf', max_score, 'LIMIT', 0, limit)

if #items == 0 then
    return {}
end

-- Remove the items we're returning
redis.call('ZREM', key, unpack(items))

return items
