local in_flight_type = redis.call("TYPE", KEYS[1]).ok
if in_flight_type ~= "none" and in_flight_type ~= "list" then
	return redis.error_reply("in-flight key has type " .. in_flight_type .. ", want list")
end

local function replace_first(key)
	local index = redis.call("LPOS", key, ARGV[1])
	if index ~= false and index ~= nil then
		redis.call("LSET", key, index, ARGV[2])
		return true
	end
	return false
end

local function contains_finalized(key)
	local index = redis.call("LPOS", key, ARGV[2])
	return index ~= false and index ~= nil
end

if replace_first(KEYS[1]) then
	return 1
end
if contains_finalized(KEYS[1]) then
	return 1
end

if #KEYS >= 2 then
	local queue_type = redis.call("TYPE", KEYS[2]).ok
	if queue_type ~= "none" and queue_type ~= "list" then
		return redis.error_reply("queue key has type " .. queue_type .. ", want list")
	end
	if replace_first(KEYS[2]) then
		return 2
	end
	if contains_finalized(KEYS[2]) then
		return 2
	end
end

return 0
