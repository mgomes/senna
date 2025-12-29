local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local drain_time_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local state = redis.call("HMGET", key, "level", "last_drip")
local level = tonumber(state[1] or "0")
local last_drip = tonumber(state[2] or now_ms)

local elapsed_ms = now_ms - last_drip
local drained = (elapsed_ms * capacity) / drain_time_ms
level = math.max(0, level - drained)

if level + 1 > capacity then
    local wait_time_ms = math.ceil(((level + 1 - capacity) * drain_time_ms) / capacity)
    return {0, level, wait_time_ms}
end

level = level + 1
redis.call("HMSET", key, "level", level, "last_drip", now_ms)
redis.call("EXPIRE", key, ttl)
return {1, level, 0}
