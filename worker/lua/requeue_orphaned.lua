-- Atomically requeue jobs from an orphaned in-flight list.
-- KEYS[1] = in-flight list key
-- KEYS[2] = known queues set key
-- ARGV[1] = queue key prefix (e.g., "senna:queue:")
-- Returns: number of jobs requeued

local inflight_key = KEYS[1]
local queues_key = KEYS[2]
local queue_prefix = ARGV[1]

local inflight_type = redis.call("TYPE", inflight_key).ok
if inflight_type == "none" then
	return 0
end
if inflight_type ~= "list" then
	return redis.error_reply("in-flight key has type " .. inflight_type .. ", want list")
end

local queues_type = redis.call("TYPE", queues_key).ok
if queues_type ~= "none" and queues_type ~= "set" then
	return redis.error_reply("queues key has type " .. queues_type .. ", want set")
end

local jobs = redis.call("LRANGE", inflight_key, 0, -1)
if #jobs == 0 then
	redis.call("DEL", inflight_key)
	return 0
end

local targets = {}
local function is_valid_job(job)
	return type(job) == "table"
		and type(job.jid) == "string"
		and job.jid ~= ""
		and type(job["class"]) == "string"
		and job["class"] ~= ""
end

for _, data in ipairs(jobs) do
	local ok, job = pcall(cjson.decode, data)
	if ok and is_valid_job(job) then
		local queue = "default"
		local should_enqueue = false

		if job.queue == nil then
			should_enqueue = true
		elseif type(job.queue) == "string" then
			queue = job.queue
			should_enqueue = true
		end

		if should_enqueue then
			local queue_key = queue_prefix .. queue
			local queue_type = redis.call("TYPE", queue_key).ok
			if queue_type ~= "none" and queue_type ~= "list" then
				return redis.error_reply("queue key has type " .. queue_type .. ", want list")
			end
			table.insert(targets, {queue = queue, key = queue_key, data = data})
		end
	end
end

local requeued = 0
for _, target in ipairs(targets) do
	redis.call("SADD", queues_key, target.queue)
	redis.call("LPUSH", target.key, target.data)
	requeued = requeued + 1
end

redis.call("DEL", inflight_key)
return requeued
