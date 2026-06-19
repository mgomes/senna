-- batch_invalidate.lua
-- Invalidate a batch so all remaining jobs will skip execution.
-- KEYS[1] = batch metadata key
-- KEYS[2] = batch progress hash key
-- Returns: JSON with result

local batch_key = KEYS[1]
local progress_key = KEYS[2]

local function number_field(field, default)
    local value = redis.call("HGET", progress_key, field)
    if not value then
        return default or 0
    end
    return tonumber(value) or default or 0
end

local batch_type = redis.call("TYPE", batch_key).ok
if batch_type == "none" then
    return cjson.encode({error = "batch_not_found"})
end
if batch_type ~= "string" then
    return redis.error_reply("batch key has type " .. batch_type .. ", want string")
end

local progress_type = redis.call("TYPE", progress_key).ok
if progress_type ~= "none" and progress_type ~= "hash" then
    return redis.error_reply("batch progress key has type " .. progress_type .. ", want hash")
end

local batch_data
local batch

local function load_batch()
    if not batch then
        batch_data = batch_data or redis.call("GET", batch_key)
        batch = cjson.decode(batch_data)
    end
    return batch
end

if progress_type == "none" then
    local legacy = load_batch()
    redis.call("HSET", progress_key,
        "id", legacy.id or "",
        "parent_id", legacy.parent_id or "",
        "callback_queue", legacy.callback_queue or "",
        "total", legacy.total or 0,
        "pending", legacy.pending or 0,
        "failures", legacy.failures or 0,
        "successes", legacy.successes or 0,
        "callbacks_pending", legacy.callbacks_pending or 0,
        "callback_seq", legacy.callback_seq or 0,
        "dead", legacy.dead and "1" or "0",
        "death_fired", legacy.death_fired and "1" or "0",
        "complete_fired", legacy.complete_fired and "1" or "0",
        "success_fired", legacy.success_fired and "1" or "0",
        "invalidated", legacy.invalidated and "1" or "0"
    )
    local batch_ttl = redis.call("PTTL", batch_key)
    if batch_ttl > 0 then
        redis.call("PEXPIRE", progress_key, batch_ttl)
    end
end

-- Mark as invalidated
redis.call("HSET", progress_key, "invalidated", "1")

return cjson.encode({
    success = true,
    pending = number_field("pending", 0)
})
