local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local drain_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local state = redis.call("HMGET", key, "level", "last_drip")
local level = tonumber(state[1] or "0")
local last_drip = tonumber(state[2] or tostring(now))

local elapsed = now - last_drip
local drained = elapsed * drain_rate
level = math.max(0, level - drained)

if level >= capacity then
    local wait_time = (level - capacity + 1) / drain_rate
    return {0, level, wait_time}
end

level = level + 1
redis.call("HMSET", key, "level", level, "last_drip", now)
redis.call("EXPIRE", key, ttl)
return {1, level, 0}
