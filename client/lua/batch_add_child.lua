-- batch_add_child.lua
-- Add a child batch to a parent batch without enqueueing jobs
-- KEYS[1] = parent batch state key
-- KEYS[2] = parent batch jobs set key
-- ARGV[1] = child batch id
-- Returns: JSON with result

local batch_key = KEYS[1]
local jobs_key = KEYS[2]
local child_id = ARGV[1]

local batch_data = redis.call('GET', batch_key)
if not batch_data then
    return cjson.encode({error = "batch_not_found"})
end

local batch = cjson.decode(batch_data)

if batch.invalidated then
    return cjson.encode({error = "batch_invalidated"})
end

-- Note: We allow adding children even after complete_fired.
-- This enables the Sidekiq-style workflow pattern where callbacks
-- "reopen" the parent batch to add the next step.
-- Adding a child increments pending, so we reset complete_fired.

-- For legacy batches that lack a pending count, initialize it from the jobs set.
if batch.pending == nil then
    batch.pending = redis.call('SCARD', jobs_key)
end

-- Only increment counters if child was newly added (idempotency)
local added = redis.call('SADD', jobs_key, child_id)
if added == 0 then
    -- Child already exists, nothing to do
    return cjson.encode({success = true, already_exists = true})
end

batch.total = (batch.total or 0) + 1
batch.pending = batch.pending + 1

-- Reset completion flags since we have new pending work
-- This allows callbacks to fire again after the new children complete
if batch.complete_fired then
    batch.complete_fired = false
    batch.success_fired = false
end

redis.call('SET', batch_key, cjson.encode(batch), 'KEEPTTL')

return cjson.encode({success = true})
