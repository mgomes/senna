-- batch_callback_complete.lua
-- Called when a batch callback job completes
-- This decrements callbacks_pending and checks if we should propagate to parent
-- KEYS[1] = batch state key
-- KEYS[2] = batch callbacks set key
-- ARGV[1] = callback job ID
-- Returns: JSON with propagation info

local batch_key = KEYS[1]
local callbacks_key = KEYS[2]
local job_id = ARGV[1]

-- Get current batch state
local batch_data = redis.call('GET', batch_key)
if not batch_data then
    return '{"error":"batch_not_found"}'
end

local batch = cjson.decode(batch_data)

-- Remove callback job ID from set - if already removed, this is a duplicate
local removed = redis.call('SREM', callbacks_key, job_id)
if removed == 0 then
    -- Already processed, return current state without decrementing
    return cjson.encode({
        already_processed = true,
        callbacks_pending = batch.callbacks_pending or 0,
        pending = batch.pending or 0,
        should_propagate = false,
        parent_id = batch.parent_id,
        dead = batch.dead or false,
        invalidated = batch.invalidated or false
    })
end

-- Decrement callbacks_pending
batch.callbacks_pending = (batch.callbacks_pending or 1) - 1
if batch.callbacks_pending < 0 then
    batch.callbacks_pending = 0
end

-- Check if we should now propagate to parent
-- This happens when all jobs AND all callbacks are complete
local should_propagate = false
if batch.pending == 0 and batch.callbacks_pending == 0 and batch.complete_fired then
    should_propagate = true
end

-- Save updated batch state
redis.call('SET', batch_key, cjson.encode(batch), 'KEEPTTL')

local result = {
    callbacks_pending = batch.callbacks_pending,
    pending = batch.pending or 0,
    should_propagate = should_propagate,
    parent_id = batch.parent_id,
    dead = batch.dead or false,
    invalidated = batch.invalidated or false
}

return cjson.encode(result)
