local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local member = ARGV[4]
local ttl = tonumber(ARGV[5])

redis.call("ZREMRANGEBYSCORE", key, "-inf", now - window)

local count = redis.call("ZCARD", key)

if count >= limit then
    local oldest = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
    if #oldest >= 2 then
        local retry_in = tonumber(oldest[2]) + window - now
        return {0, count, retry_in}
    end
    return {0, count, window}
end

redis.call("ZADD", key, now, member)
redis.call("EXPIRE", key, ttl)
return {1, count + 1, 0}
