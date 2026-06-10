local in_flight_type = redis.call("TYPE", KEYS[1]).ok
if in_flight_type ~= "none" and in_flight_type ~= "list" then
	return redis.error_reply("in-flight key has type " .. in_flight_type .. ", want list")
end

local removed = redis.call("LREM", KEYS[1], 1, ARGV[1])
if redis.call("GET", KEYS[2]) == ARGV[2] then
	redis.call("DEL", KEYS[2])
end
return removed
