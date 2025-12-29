local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local bucket_ts = math.floor(now_ms / window_ms) * window_ms
local bucket_key = key .. ":" .. bucket_ts
local current = tonumber(redis.call("GET", bucket_key) or "0")

if current >= limit then
    local reset_at = bucket_ts + window_ms
    local retry_in_ms = reset_at - now_ms
    return {0, current, retry_in_ms}
end

redis.call("INCR", bucket_key)
redis.call("EXPIRE", bucket_key, ttl)
return {1, current + 1, 0}
