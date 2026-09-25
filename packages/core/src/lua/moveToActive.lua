--[[
  Promotes due delayed jobs, then takes the next job from the wait set,
  moves it to active and locks it.

  KEYS[1] wait
  KEYS[2] active
  KEYS[3] delayed
  KEYS[4] events
  KEYS[5] job key prefix
  KEYS[6] meta

  ARGV[1] lock token
  ARGV[2] lock duration (ms)
  ARGV[3] max events stream length

  Returns { jobId, flattened job hash } when a job was taken. Otherwise returns
  { msUntilNextDelayed }, or { -1 } when nothing is scheduled or the queue
  is paused.
]]
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

-- Bounded batch so one call never blocks Redis for long.
local due = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now, 'LIMIT', 0, 1000)
for _, id in ipairs(due) do
  local fields = redis.call('HMGET', KEYS[5] .. id, 'priority', 'seq')
  redis.call('ZREM', KEYS[3], id)
  redis.call('ZADD', KEYS[1], tonumber(fields[1]) * 4294967296 + tonumber(fields[2]), id)
  redis.call('XADD', KEYS[4], 'MAXLEN', '~', ARGV[3], '*', 'event', 'waiting', 'jobId', id)
end

if redis.call('HEXISTS', KEYS[6], 'paused') == 1 then
  return { -1 }
end

local popped = redis.call('ZPOPMIN', KEYS[1])
if #popped == 0 then
  local nextDelayed = redis.call('ZRANGE', KEYS[3], 0, 0, 'WITHSCORES')
  if #nextDelayed == 0 then
    return { -1 }
  end
  return { tonumber(nextDelayed[2]) - now }
end

local jobId = popped[1]
local jobKey = KEYS[5] .. jobId

redis.call('SET', jobKey .. ':lock', ARGV[1], 'PX', ARGV[2])
redis.call('ZADD', KEYS[2], now, jobId)
redis.call('HSET', jobKey, 'processedOn', now)

redis.call('XADD', KEYS[4], 'MAXLEN', '~', ARGV[3], '*', 'event', 'active', 'jobId', jobId)

return { jobId, redis.call('HGETALL', jobKey) }
