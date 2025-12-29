local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_time_ms = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local now_ms = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])

local state = redis.call("HMGET", key, "points", "last_refill")
local points = tonumber(state[1] or capacity)
local last_refill = tonumber(state[2] or now_ms)

local elapsed_ms = now_ms - last_refill
local refilled = (elapsed_ms * capacity) / refill_time_ms
points = math.min(capacity, points + refilled)

if points < cost then
    local needed = cost - points
    local wait_time_ms = math.ceil((needed * refill_time_ms) / capacity)
    return {0, points, wait_time_ms}
end

points = points - cost
redis.call("HMSET", key, "points", points, "last_refill", now_ms)
redis.call("EXPIRE", key, ttl)
return {1, points, 0}
