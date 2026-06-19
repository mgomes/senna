local slots_key = KEYS[1]
local locks_key = KEYS[2]
local lock_id = ARGV[1]
local ttl = tonumber(ARGV[2])
local redis_time = redis.call("TIME")
local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)

local slot = redis.call("LPOP", slots_key)
if not slot then
    return {0, 0}
end

redis.call("HSET", locks_key, lock_id, now_ms)
redis.call("EXPIRE", slots_key, ttl)
redis.call("EXPIRE", locks_key, ttl)

return {1, redis.call("HLEN", locks_key)}
