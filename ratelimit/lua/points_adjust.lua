local key = KEYS[1]
local diff = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

local current = tonumber(redis.call("HGET", key, "points") or "0")
local new_points = math.max(0, math.min(capacity, current + diff))
redis.call("HSET", key, "points", new_points)
redis.call("EXPIRE", key, ttl)
return new_points
