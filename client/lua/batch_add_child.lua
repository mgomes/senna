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

if batch.complete_fired then
    return cjson.encode({error = "batch_complete"})
end

redis.call('SADD', jobs_key, child_id)
batch.total = (batch.total or 0) + 1
batch.pending = (batch.pending or 0) + 1

redis.call('SET', batch_key, cjson.encode(batch), 'KEEPTTL')

return cjson.encode({success = true})
