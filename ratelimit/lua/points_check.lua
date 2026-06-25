local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_time_us = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

if not cost or cost <= 0 then
    return redis.error_reply("points cost must be positive")
end

local redis_time = redis.call("TIME")
local now_us = (tonumber(redis_time[1]) * 1000000) + tonumber(redis_time[2])

local state = redis.call("HMGET", key, "points", "last_refill")
local points = tonumber(state[1] or capacity)
local last_refill = tonumber(state[2] or now_us)

local elapsed_us = now_us - last_refill
local refilled = (elapsed_us * capacity) / refill_time_us
points = math.min(capacity, points + refilled)

if points < cost then
    local needed = cost - points
    local wait_time_us = math.ceil((needed * refill_time_us) / capacity)
    return {0, points, wait_time_us}
end

points = points - cost
redis.call("HMSET", key, "points", points, "last_refill", now_us)
redis.call("EXPIRE", key, ttl)
return {1, points, 0}
