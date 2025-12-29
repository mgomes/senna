local slots_key = KEYS[1]
local locks_key = KEYS[2]
local now = tonumber(ARGV[1])
local lock_timeout = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])

local locks = redis.call("HGETALL", locks_key)
local reclaimed = 0

for i = 1, #locks, 2 do
    local lock_id = locks[i]
    local acquired_at = tonumber(locks[i + 1])

    if acquired_at and (now - acquired_at) > lock_timeout then
        redis.call("HDEL", locks_key, lock_id)
        redis.call("RPUSH", slots_key, "slot")
        reclaimed = reclaimed + 1
    end
end

if reclaimed > 0 then
    redis.call("EXPIRE", slots_key, ttl)
    redis.call("EXPIRE", locks_key, ttl)
end

return reclaimed
