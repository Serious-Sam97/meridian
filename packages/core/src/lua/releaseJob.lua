--[[
  Hands an active job back to the wait set without counting an attempt,
  used when a worker shuts down before the job finished.

  KEYS[1] active
  KEYS[2] wait
  KEYS[3] marker
  KEYS[4] events
  KEYS[5] job key prefix

  ARGV[1] job id
  ARGV[2] lock token
  ARGV[3] max events stream length

  Returns 0 on success, -1 when the lock is not held by the caller,
  -2 when the job is not active.
]]
--@include common

local jobId = ARGV[1]
local jobKey = KEYS[5] .. jobId
local lockKey = jobKey .. ':lock'

if redis.call('GET', lockKey) ~= ARGV[2] then
  return -1
end
if redis.call('ZREM', KEYS[1], jobId) == 0 then
  return -2
end
redis.call('DEL', lockKey)
redis.call('HDEL', jobKey, 'processedOn')

pushWaiting(KEYS[2], jobKey, jobId)
wakeWorker(KEYS[3])

redis.call('XADD', KEYS[4], 'MAXLEN', '~', ARGV[3], '*', 'event', 'released', 'jobId', jobId)

return 0
