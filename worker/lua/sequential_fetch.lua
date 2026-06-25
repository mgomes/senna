-- Atomically fetch from a sequential queue
-- Only acquires lock if a job is actually claimed
-- KEYS[1] = queue key
-- KEYS[2] = in-flight key
-- KEYS[3] = lock key
-- ARGV[1] = worker ID
-- ARGV[2] = lock TTL in milliseconds
-- ARGV[3] = finalization key prefix

local queue_key = KEYS[1]
local inflight_key = KEYS[2]
local lock_key = KEYS[3]
local worker_id = ARGV[1]
local lock_ttl = tonumber(ARGV[2])
local finalization_prefix = ARGV[3]

local queue_type = redis.call("TYPE", queue_key).ok
if queue_type ~= "none" and queue_type ~= "list" then
	return redis.error_reply("queue key has type " .. queue_type .. ", want list")
end

local in_flight_type = redis.call("TYPE", inflight_key).ok
if in_flight_type ~= "none" and in_flight_type ~= "list" then
	return redis.error_reply("in-flight key has type " .. in_flight_type .. ", want list")
end

-- Check if another worker holds the lock
local holder = redis.call("GET", lock_key)
if holder and holder ~= worker_id then
    -- Another worker holds the lock
    return {"locked", ""}
end

-- Try to fetch a job (LMOVE from queue to in-flight)
local job = redis.call("LMOVE", queue_key, inflight_key, "RIGHT", "LEFT")
if not job then
    -- Queue is empty, don't acquire lock
    return {"empty", ""}
end

local in_flight_payload = job
local ok, decoded = pcall(cjson.decode, job)
if ok and type(decoded) == "table" and type(decoded.finalization) == "table" then
    local trusted = false
    if type(decoded.jid) == "string" and decoded.jid ~= "" then
        local trust_key = finalization_prefix .. decoded.jid
        local trust_type = redis.call("TYPE", trust_key).ok
        if trust_type == "string" and redis.call("GET", trust_key) == job then
            trusted = true
        end
    end
    if not trusted then
        decoded.finalization = nil
        in_flight_payload = cjson.encode(decoded)
    end
end

if in_flight_payload ~= job then
    local index = redis.call("LPOS", inflight_key, job)
    if index ~= false and index ~= nil then
        redis.call("LSET", inflight_key, index, in_flight_payload)
    end
end

-- Successfully fetched a job, acquire/refresh the lock
redis.call("SET", lock_key, worker_id, "PX", lock_ttl)

return {"job", in_flight_payload}
