--[[
  Moves a failed job back to the wait set with a fresh set of attempts.
  Used for manual retries from the API or the dashboard.

  KEYS[1] failed
  KEYS[2] wait
  KEYS[3] marker
  KEYS[4] events
  KEYS[5] job key prefix

  ARGV[1] job id
  ARGV[2] max events stream length

  Returns 1 when the job was moved, 0 when it is not in the failed set.
]]
--@include common

local jobId = ARGV[1]
local jobKey = KEYS[5] .. jobId

if redis.call('ZREM', KEYS[1], jobId) == 0 then
  return 0
end

redis.call('HSET', jobKey, 'attemptsMade', 0)
redis.call('HDEL', jobKey, 'stalledCount', 'failedReason', 'stacktrace', 'processedOn', 'finishedOn')

pushWaiting(KEYS[2], jobKey, jobId)
wakeWorker(KEYS[3])

redis.call('XADD', KEYS[4], 'MAXLEN', '~', ARGV[2], '*', 'event', 'waiting', 'jobId', jobId)

return 1
