local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local drain_time_us = tonumber(ARGV[2])
local now_us = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

local state = redis.call("HMGET", key, "level", "last_drip")
local level = tonumber(state[1] or "0")
local last_drip = tonumber(state[2] or now_us)

local elapsed_us = now_us - last_drip
local drained = (elapsed_us * capacity) / drain_time_us
level = math.max(0, level - drained)

if level + 1 > capacity then
    local overfill = (level + 1) - capacity
    local wait_time_us = math.ceil((overfill * drain_time_us) / capacity)
    return {0, level, wait_time_us}
end

level = level + 1
redis.call("HMSET", key, "level", level, "last_drip", now_us)
redis.call("EXPIRE", key, ttl)
return {1, level, 0}
