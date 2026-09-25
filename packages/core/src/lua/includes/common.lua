-- Shared helpers, spliced into scripts by the loader (see scripts.ts).

-- Current time in ms from the Redis clock, so every worker shares one clock.
local function nowMs()
  local time = redis.call('TIME')
  return tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
end

-- Adds a job to the wait set: priority in the high bits, insertion order in
-- the low bits (ADR 0002).
local function pushWaiting(waitKey, jobKey, jobId)
  local fields = redis.call('HMGET', jobKey, 'priority', 'seq')
  redis.call('ZADD', waitKey, tonumber(fields[1]) * 4294967296 + tonumber(fields[2]), jobId)
end

-- Wakes one idle worker blocked on the marker list. Markers are hints, so the
-- list is capped instead of tracking exact counts.
local function wakeWorker(markerKey)
  redis.call('LPUSH', markerKey, '1')
  redis.call('LTRIM', markerKey, 0, 99)
end
