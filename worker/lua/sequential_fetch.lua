-- Atomically fetch from a sequential queue
-- Only acquires lock if a job is actually claimed
-- KEYS[1] = queue key
-- KEYS[2] = in-flight key
-- KEYS[3] = lock key
-- ARGV[1] = worker ID
-- ARGV[2] = lock TTL in milliseconds

local queue_key = KEYS[1]
local inflight_key = KEYS[2]
local lock_key = KEYS[3]
local worker_id = ARGV[1]
local lock_ttl = tonumber(ARGV[2])

-- Check if another worker holds the lock
local holder = redis.call("GET", lock_key)
if holder and holder ~= worker_id then
    -- Another worker holds the lock
    return nil
end

-- Try to fetch a job (LMOVE from queue to in-flight)
local job = redis.call("LMOVE", queue_key, inflight_key, "RIGHT", "LEFT")
if not job then
    -- Queue is empty, don't acquire lock
    return nil
end

-- Successfully fetched a job, acquire/refresh the lock
redis.call("SET", lock_key, worker_id, "PX", lock_ttl)

return job
