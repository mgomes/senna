local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window_us = tonumber(ARGV[2])
local now_us = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local bucket_ts = math.floor(now_us / window_us) * window_us
local bucket_key = key .. ":" .. bucket_ts
local current = tonumber(redis.call("GET", bucket_key) or "0")

if current >= limit then
    local reset_at = bucket_ts + window_us
    local retry_in_us = reset_at - now_us
    return {0, current, retry_in_us}
end

redis.call("INCR", bucket_key)
redis.call("EXPIRE", bucket_key, ttl)
return {1, current + 1, 0}
