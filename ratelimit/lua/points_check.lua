local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])

local state = redis.call("HMGET", key, "points", "last_refill")
local points = tonumber(state[1] or tostring(capacity))
local last_refill = tonumber(state[2] or tostring(now))

local elapsed = now - last_refill
local refilled = elapsed * refill_rate
points = math.min(capacity, points + refilled)

if points < cost then
    local needed = cost - points
    local wait_time = needed / refill_rate
    return {0, points, wait_time}
end

points = points - cost
redis.call("HMSET", key, "points", points, "last_refill", now)
redis.call("EXPIRE", key, ttl)
return {1, points, 0}
