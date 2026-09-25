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
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

local jobId = ARGV[1]
local jobKey = KEYS[6] .. jobId
local lockKey = jobKey .. ':lock'

if redis.call('GET', lockKey) ~= ARGV[2] then
  return -1
end
if redis.call('ZREM', KEYS[1], jobId) == 0 then
  return -2
end
redis.call('DEL', lockKey)

redis.call('HINCRBY', jobKey, 'attemptsMade', 1)
redis.call('HSET', jobKey, 'failedReason', ARGV[4], 'stacktrace', ARGV[5])

local delay = tonumber(ARGV[3])
if delay > 0 then
  redis.call('ZADD', KEYS[3], now + delay, jobId)
else
  local fields = redis.call('HMGET', jobKey, 'priority', 'seq')
  redis.call('ZADD', KEYS[2], tonumber(fields[1]) * 4294967296 + tonumber(fields[2]), jobId)
end

redis.call('LPUSH', KEYS[4], '1')
redis.call('LTRIM', KEYS[4], 0, 99)

redis.call('XADD', KEYS[5], 'MAXLEN', '~', ARGV[6], '*',
  'event', 'retrying', 'jobId', jobId, 'delay', delay, 'reason', ARGV[4])

return 0
