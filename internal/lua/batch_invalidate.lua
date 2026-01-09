-- batch_invalidate.lua
-- Invalidate a batch so all remaining jobs will skip execution
-- KEYS[1] = batch state key
-- Returns: JSON with result

local batch_key = KEYS[1]

-- Get current batch state
local batch_data = redis.call('GET', batch_key)
if not batch_data then
    return cjson.encode({error = "batch_not_found"})
end

local batch = cjson.decode(batch_data)

-- Mark as invalidated
batch.invalidated = true

-- Save updated batch state
redis.call('SET', batch_key, cjson.encode(batch), 'KEEPTTL')

return cjson.encode({
    success = true,
    pending = batch.pending
})
