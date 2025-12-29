local slots_key = KEYS[1]
local locks_key = KEYS[2]
local lock_id = ARGV[1]
local ttl = tonumber(ARGV[2])

local deleted = redis.call("HDEL", locks_key, lock_id)
if deleted == 0 then
    return 0
end

redis.call("RPUSH", slots_key, "slot")
redis.call("EXPIRE", slots_key, ttl)
redis.call("EXPIRE", locks_key, ttl)

return 1
