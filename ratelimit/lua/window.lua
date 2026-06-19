local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window_us = tonumber(ARGV[2])
local member = ARGV[3]
local ttl = tonumber(ARGV[4])
local redis_time = redis.call("TIME")
local now_us = (tonumber(redis_time[1]) * 1000000) + tonumber(redis_time[2])

redis.call("ZREMRANGEBYSCORE", key, "-inf", now_us - window_us)

local count = redis.call("ZCARD", key)

if count >= limit then
    local oldest = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
    if #oldest >= 2 then
        local retry_in_us = tonumber(oldest[2]) + window_us - now_us
        if retry_in_us < 0 then
            retry_in_us = 0
        end
        return {0, count, retry_in_us}
    end
    return {0, count, window_us}
end

redis.call("ZADD", key, now_us, member)
redis.call("EXPIRE", key, ttl)
return {1, count + 1, 0}
