--[[
  Moves an active job to the completed or failed set, if the caller still
  holds its lock (ADR 0003).

  KEYS[1] active
  KEYS[2] target set (completed or failed)
  KEYS[3] events
  KEYS[4] job key prefix
  KEYS[5] metrics key prefix
  KEYS[6] tag key prefix

  ARGV[1] job id
  ARGV[2] lock token
  ARGV[3] event name ('completed' or 'failed')
  ARGV[4] result field ('returnValue' or 'failedReason')
  ARGV[5] result value
  ARGV[6] stacktrace ('' when none)
  ARGV[7] retention: -1 keeps all, 0 removes the job, N keeps the newest N
  ARGV[8] max events stream length

  Returns 0 on success, -1 when the lock is not held by the caller,
  -2 when the job is not active.
]]
--@include common
--@include tags
local now = nowMs()

local jobId = ARGV[1]
local jobKey = KEYS[4] .. jobId
local lockKey = jobKey .. ':lock'

if redis.call('GET', lockKey) ~= ARGV[2] then
  return -1
end
if redis.call('ZREM', KEYS[1], jobId) == 0 then
  return -2
end
redis.call('DEL', lockKey)

-- Per-minute metrics for the dashboard, kept for 24 hours. Wait time is
-- measured from creation to the start of the final attempt.
local times = redis.call('HMGET', jobKey, 'timestamp', 'processedOn')
local createdAt = tonumber(times[1]) or now
local startedAt = tonumber(times[2]) or now
local bucketKey = KEYS[5] .. string.format('%d', math.floor(now / 60000) * 60000)
redis.call('HINCRBY', bucketKey, ARGV[3], 1)
redis.call('HINCRBY', bucketKey, 'runtime', now - startedAt)
redis.call('HINCRBY', bucketKey, 'wait', startedAt - createdAt)
redis.call('EXPIRE', bucketKey, 86400)

local keep = tonumber(ARGV[7])
if keep == 0 then
  deleteJob(KEYS[4], KEYS[6], jobId)
else
  redis.call('HINCRBY', jobKey, 'attemptsMade', 1)
  redis.call('HSET', jobKey, ARGV[4], ARGV[5], 'finishedOn', now)
  if ARGV[6] ~= '' then
    redis.call('HSET', jobKey, 'stacktrace', ARGV[6])
  end
  redis.call('ZADD', KEYS[2], now, jobId)

  if keep > 0 then
    local excess = redis.call('ZCARD', KEYS[2]) - keep
    if excess > 0 then
      local old = redis.call('ZRANGE', KEYS[2], 0, excess - 1)
      for _, id in ipairs(old) do
        deleteJob(KEYS[4], KEYS[6], id)
      end
      redis.call('ZREMRANGEBYRANK', KEYS[2], 0, excess - 1)
    end
  end
end

redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[8], '*', 'event', ARGV[3], 'jobId', jobId)

return 0
