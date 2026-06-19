local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window_us = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local redis_time = redis.call("TIME")
local now_us = (tonumber(redis_time[1]) * 1000000) + tonumber(redis_time[2])

local bucket_ts = math.floor(now_us / window_us) * window_us
local bucket_key = key .. ":" .. bucket_ts

-- Atomically increment first, then check
local current = tonumber(redis.call("INCR", bucket_key))
redis.call("EXPIRE", bucket_key, ttl)

if current > limit then
    -- Exceeded limit, decrement back and reject
    redis.call("DECR", bucket_key)
    local reset_at = bucket_ts + window_us
    local retry_in_us = reset_at - now_us
    return {0, current - 1, retry_in_us}
end

return {1, current, 0}
