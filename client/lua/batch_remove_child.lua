-- batch_remove_child.lua
-- Remove a child batch from a parent batch (rollback batch_add_child)
-- KEYS[1] = parent batch state key
-- KEYS[2] = parent batch jobs set key
-- ARGV[1] = child batch id
-- Returns: JSON with result

local batch_key = KEYS[1]
local jobs_key = KEYS[2]
local child_id = ARGV[1]

local batch_data = redis.call('GET', batch_key)
if not batch_data then
    -- Parent already gone, nothing to roll back
    return cjson.encode({success = true, parent_gone = true})
end

local batch = cjson.decode(batch_data)

-- Remove child from jobs set
local removed = redis.call('SREM', jobs_key, child_id)
if removed == 0 then
    -- Child wasn't in set, nothing to roll back
    return cjson.encode({success = true, not_found = true})
end

-- Decrement counters
batch.total = (batch.total or 1) - 1
if batch.total < 0 then
    batch.total = 0
end

batch.pending = (batch.pending or 1) - 1
if batch.pending < 0 then
    batch.pending = 0
end

redis.call('SET', batch_key, cjson.encode(batch), 'KEEPTTL')

return cjson.encode({success = true})
