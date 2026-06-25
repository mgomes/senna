local in_flight_type = redis.call("TYPE", KEYS[1]).ok
if in_flight_type ~= "none" and in_flight_type ~= "list" then
	return redis.error_reply("in-flight key has type " .. in_flight_type .. ", want list")
end

local delete_unique_key = false
if #KEYS >= 2 and ARGV[2] ~= nil then
	local unique_type = redis.call("TYPE", KEYS[2]).ok
	if unique_type == "string" and redis.call("GET", KEYS[2]) == ARGV[2] then
		delete_unique_key = true
	end
end

local removed = redis.call("LREM", KEYS[1], 1, ARGV[1])
if removed > 0 and delete_unique_key then
	redis.call("DEL", KEYS[2])
end
return removed
