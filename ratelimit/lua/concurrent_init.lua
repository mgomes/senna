-- Initialize concurrent limiter slots exactly once
-- KEYS[1] = slots key
-- KEYS[2] = locks hash key
-- KEYS[3] = init sentinel key
-- ARGV[1] = limit
-- ARGV[2] = ttl
-- Returns: number of slots created (may be 0 if already initialized)

local slots_key = KEYS[1]
local locks_key = KEYS[2]
local init_key = KEYS[3]
local limit = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])

-- If init key exists, refresh TTLs and exit
if redis.call("EXISTS", init_key) == 1 then
  if redis.call("LLEN", slots_key) > 0 then
    redis.call("EXPIRE", slots_key, ttl)
  end
  redis.call("EXPIRE", locks_key, ttl)
  redis.call("EXPIRE", init_key, ttl)
  return 0
end

-- Claim initialization
if redis.call("SETNX", init_key, "1") == 0 then
  return 0
end
redis.call("EXPIRE", init_key, ttl)

-- Account for existing slots/locks (e.g., after sentinel expiry)
local current_slots = redis.call("LLEN", slots_key)
local held_locks = redis.call("HLEN", locks_key)
local total_known = current_slots + held_locks
local slots_to_add = limit - total_known

if slots_to_add > 0 then
  for i = 1, slots_to_add do
    redis.call("RPUSH", slots_key, "slot")
  end
  redis.call("EXPIRE", slots_key, ttl)
end

redis.call("EXPIRE", locks_key, ttl)

return math.max(slots_to_add, 0)
