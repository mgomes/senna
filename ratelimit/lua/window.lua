local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local member = ARGV[4]
local ttl = tonumber(ARGV[5])

redis.call("ZREMRANGEBYSCORE", key, "-inf", now_ms - window_ms)

local count = redis.call("ZCARD", key)

if count >= limit then
    local oldest = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
    if #oldest >= 2 then
        local retry_in_ms = tonumber(oldest[2]) + window_ms - now_ms
        return {0, count, retry_in_ms}
    end
    return {0, count, window_ms}
end

redis.call("ZADD", key, now_ms, member)
redis.call("EXPIRE", key, ttl)
return {1, count + 1, 0}
