local queue_type = redis.call("TYPE", KEYS[1]).ok
if queue_type ~= "none" and queue_type ~= "list" then
	return redis.error_reply("queue key has type " .. queue_type .. ", want list")
end

local in_flight_type = redis.call("TYPE", KEYS[2]).ok
if in_flight_type ~= "none" and in_flight_type ~= "list" then
	return redis.error_reply("in-flight key has type " .. in_flight_type .. ", want list")
end

local payload = redis.call("RPOP", KEYS[1])
if not payload then
	return {"empty", ""}
end

local finalization_prefix = ARGV[1]
local in_flight_payload = payload
local ok, job = pcall(cjson.decode, payload)
if ok and type(job) == "table" and job.finalization ~= nil then
	local trusted = false
	if type(job.finalization) == "table" and type(job.jid) == "string" and job.jid ~= "" then
		local trust_key = finalization_prefix .. job.jid
		local trust_type = redis.call("TYPE", trust_key).ok
		if trust_type == "string" and redis.call("GET", trust_key) == payload then
			trusted = true
		end
	end
	if not trusted then
		job.finalization = nil
		in_flight_payload = cjson.encode(job)
	end
end

redis.call("LPUSH", KEYS[2], in_flight_payload)
return {"job", in_flight_payload}
