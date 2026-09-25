-- Tag index: one sorted set per tag, job ids scored by creation time.

local function jobTags(opts)
  -- Most jobs have no tags: skip decoding their options.
  if not opts or not string.find(opts, '"tags"', 1, true) then
    return {}
  end
  local ok, decoded = pcall(cjson.decode, opts)
  if ok and type(decoded.tags) == 'table' then
    return decoded.tags
  end
  return {}
end

local function indexTags(tagPrefix, opts, jobId, timestamp)
  for _, tag in ipairs(jobTags(opts)) do
    redis.call('ZADD', tagPrefix .. tag, timestamp, jobId)
  end
end

-- Deletes a job hash and removes it from its tag indexes. Every script that
-- deletes jobs goes through here so the indexes never point at missing jobs.
local function deleteJob(jobPrefix, tagPrefix, jobId)
  local jobKey = jobPrefix .. jobId
  for _, tag in ipairs(jobTags(redis.call('HGET', jobKey, 'opts'))) do
    redis.call('ZREM', tagPrefix .. tag, jobId)
  end
  redis.call('DEL', jobKey)
end
