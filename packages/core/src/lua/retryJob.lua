--[[
  Moves a failed active job back to wait (or to delayed when it has a
  backoff) for another attempt, if the caller still holds its lock.

  KEYS[1] active
  KEYS[2] wait
  KEYS[3] delayed
  KEYS[4] marker
  KEYS[5] events
  KEYS[6] job key prefix

  ARGV[1] job id
  ARGV[2] lock token
  ARGV[3] backoff delay (ms)
  ARGV[4] failed reason
  ARGV[5] stacktrace
  ARGV[6] max events stream length

  Returns 0 on success, -1 when the lock is not held by the caller,
  -2 when the job is not active.
]]
--@include common
--@include receipts
local now = nowMs()

local jobId = ARGV[1]
local jobKey = KEYS[6] .. jobId
local lockKey = jobKey .. ':lock'

if redis.call('GET', lockKey) ~= ARGV[2] then
  -- A resend of a call that already succeeded is not a lost lock.
  if wasSettledBy(jobKey, ARGV[2]) then
    return 0
  end
  return -1
end
if redis.call('ZREM', KEYS[1], jobId) == 0 then
  return -2
end
redis.call('DEL', lockKey)
markSettled(jobKey, ARGV[2])

redis.call('HINCRBY', jobKey, 'attemptsMade', 1)
redis.call('HSET', jobKey, 'failedReason', ARGV[4], 'stacktrace', ARGV[5])

local delay = tonumber(ARGV[3])
if delay > 0 then
  redis.call('ZADD', KEYS[3], now + delay, jobId)
else
  pushWaiting(KEYS[2], jobKey, jobId)
end

wakeWorker(KEYS[4])

redis.call('XADD', KEYS[5], 'MAXLEN', '~', ARGV[6], '*',
  'event', 'retrying', 'jobId', jobId, 'delay', delay, 'reason', ARGV[4])

return 0
