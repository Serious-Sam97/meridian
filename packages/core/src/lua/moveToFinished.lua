--[[
  Moves an active job to the completed or failed set, if the caller still
  holds its lock (ADR 0003).

  KEYS[1] active
  KEYS[2] target set (completed or failed)
  KEYS[3] events
  KEYS[4] job key prefix

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
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

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

local keep = tonumber(ARGV[7])
if keep == 0 then
  redis.call('DEL', jobKey)
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
        redis.call('DEL', KEYS[4] .. id)
      end
      redis.call('ZREMRANGEBYRANK', KEYS[2], 0, excess - 1)
    end
  end
end

redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[8], '*', 'event', ARGV[3], 'jobId', jobId)

return 0
