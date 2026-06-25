local in_flight_type = redis.call("TYPE", KEYS[1]).ok
if in_flight_type ~= "none" and in_flight_type ~= "list" then
	return redis.error_reply("in-flight key has type " .. in_flight_type .. ", want list")
end

local retry_type = redis.call("TYPE", KEYS[2]).ok
if retry_type ~= "none" and retry_type ~= "zset" then
	return redis.error_reply("retry key has type " .. retry_type .. ", want zset")
end

local removed = redis.call("LREM", KEYS[1], 1, ARGV[1])
if removed > 0 then
	if #KEYS >= 3 then
		redis.call("DEL", KEYS[3])
	end
	redis.call("ZADD", KEYS[2], ARGV[2], ARGV[3])
end
return removed
