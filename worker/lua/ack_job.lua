local in_flight_type = redis.call("TYPE", KEYS[1]).ok
if in_flight_type ~= "none" and in_flight_type ~= "list" then
	return redis.error_reply("in-flight key has type " .. in_flight_type .. ", want list")
end

local delete_unique_key = false
if #KEYS >= 3 and ARGV[2] ~= nil then
	local unique_type = redis.call("TYPE", KEYS[3]).ok
	if unique_type == "string" and redis.call("GET", KEYS[3]) == ARGV[2] then
		delete_unique_key = true
	end
end

local removed = redis.call("LREM", KEYS[1], 1, ARGV[1])
if removed > 0 then
	if #KEYS >= 2 then
		redis.call("DEL", KEYS[2])
	end
	if delete_unique_key then
		redis.call("DEL", KEYS[3])
	end
end
return removed
