local key = KEYS[1]
local diff = tonumber(ARGV[1])
local max_refund = tonumber(ARGV[2])
local capacity = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

if not diff then
    return redis.error_reply("points adjustment must be numeric")
end

if max_refund and diff > max_refund then
    return redis.error_reply("points refund exceeds acquired estimate")
end

local current = tonumber(redis.call("HGET", key, "points") or "0")
local new_points = math.max(0, math.min(capacity, current + diff))
redis.call("HSET", key, "points", new_points)
redis.call("EXPIRE", key, ttl)
return new_points
