local in_flight_type = redis.call("TYPE", KEYS[1]).ok
if in_flight_type ~= "none" and in_flight_type ~= "list" then
	return redis.error_reply("in-flight key has type " .. in_flight_type .. ", want list")
end

local index = redis.call("LPOS", KEYS[1], ARGV[1])
if index == false or index == nil then
	return 0
end

redis.call("LSET", KEYS[1], index, ARGV[2])
return 1
