local slots_key = KEYS[1]
local locks_key = KEYS[2]
local init_key = KEYS[3]
local limit = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])

local already_init = redis.call("GET", init_key)
if already_init then
    return 0
end

local set_result = redis.call("SETNX", init_key, "1")
if set_result == 0 then
    return 0
end

for i = 1, limit do
    redis.call("RPUSH", slots_key, "slot")
end

redis.call("EXPIRE", slots_key, ttl)
redis.call("EXPIRE", locks_key, ttl)
redis.call("EXPIRE", init_key, ttl)

return limit
