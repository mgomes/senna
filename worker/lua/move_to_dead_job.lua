local in_flight_type = redis.call("TYPE", KEYS[1]).ok
if in_flight_type ~= "none" and in_flight_type ~= "list" then
	return redis.error_reply("in-flight key has type " .. in_flight_type .. ", want list")
end

local dead_type = redis.call("TYPE", KEYS[2]).ok
if dead_type ~= "none" and dead_type ~= "zset" then
	return redis.error_reply("dead key has type " .. dead_type .. ", want zset")
end

local removed = redis.call("LREM", KEYS[1], 1, ARGV[1])
if removed > 0 then
	if #KEYS >= 3 then
		redis.call("DEL", KEYS[3])
	end
	redis.call("ZADD", KEYS[2], ARGV[2], ARGV[3])
end
return removed
