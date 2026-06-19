-- Reclaim expired concurrent locks
-- KEYS[1] = slots key
-- KEYS[2] = locks hash key
-- KEYS[3] = metrics key
-- ARGV[1] = lock_timeout_ms
-- ARGV[2] = ttl
-- Returns: number of locks reclaimed

local slots_key = KEYS[1]
local locks_key = KEYS[2]
local metrics_key = KEYS[3]
local lock_timeout_ms = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local redis_time = redis.call("TIME")
local now_ms = (tonumber(redis_time[1]) * 1000) + math.floor(tonumber(redis_time[2]) / 1000)
local threshold = now_ms - lock_timeout_ms

local locks = redis.call("HGETALL", locks_key)
local reclaimed = 0

for i = 1, #locks, 2 do
  local lock_id = locks[i]
  local acquired_at = tonumber(locks[i + 1])
  if acquired_at and acquired_at < threshold then
    redis.call("HDEL", locks_key, lock_id)
    redis.call("RPUSH", slots_key, "slot")
    reclaimed = reclaimed + 1
  end
end

if reclaimed > 0 then
  redis.call("HINCRBY", metrics_key, "reclaimed", reclaimed)
  redis.call("EXPIRE", slots_key, ttl)
  redis.call("EXPIRE", locks_key, ttl)
  redis.call("EXPIRE", metrics_key, ttl)
end

return reclaimed
