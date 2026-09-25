--[[
  Takes the next job from the wait set, moves it to active and locks it.

  KEYS[1] wait
  KEYS[2] active
  KEYS[3] events
  KEYS[4] job key prefix

  ARGV[1] lock token
  ARGV[2] lock duration (ms)
  ARGV[3] max events stream length

  Returns { jobId, flattened job hash } or an empty array when there is no job.
]]
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

local popped = redis.call('ZPOPMIN', KEYS[1])
if #popped == 0 then
  return {}
end

local jobId = popped[1]
local jobKey = KEYS[4] .. jobId

redis.call('SET', jobKey .. ':lock', ARGV[1], 'PX', ARGV[2])
redis.call('ZADD', KEYS[2], now, jobId)
redis.call('HSET', jobKey, 'processedOn', now)

redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[3], '*', 'event', 'active', 'jobId', jobId)

return { jobId, redis.call('HGETALL', jobKey) }
