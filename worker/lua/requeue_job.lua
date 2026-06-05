local in_flight_type = redis.call("TYPE", KEYS[1]).ok
if in_flight_type ~= "none" and in_flight_type ~= "list" then
	return redis.error_reply("in-flight key has type " .. in_flight_type .. ", want list")
end

local queue_type = redis.call("TYPE", KEYS[2]).ok
if queue_type ~= "none" and queue_type ~= "list" then
	return redis.error_reply("queue key has type " .. queue_type .. ", want list")
end

local removed = redis.call("LREM", KEYS[1], 1, ARGV[1])
if removed > 0 then
	redis.call("RPUSH", KEYS[2], ARGV[2])
end
return removed
