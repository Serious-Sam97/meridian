--[[
  Adds a job to the wait set, or to the delayed set when it has a delay.

  KEYS[1] id counter
  KEYS[2] wait
  KEYS[3] delayed
  KEYS[4] marker
  KEYS[5] events
  KEYS[6] job key prefix

  ARGV[1] custom job id, or '' to generate one
  ARGV[2] job name
  ARGV[3] data (JSON)
  ARGV[4] options (JSON)
  ARGV[5] priority
  ARGV[6] delay (ms)
  ARGV[7] max events stream length

  Returns { jobId, created } where created is 0 when a job with the same
  custom id already exists (adding is idempotent on the id).
]]
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)

local jobId = ARGV[1]
if jobId ~= '' and redis.call('EXISTS', KEYS[6] .. jobId) == 1 then
  return { jobId, 0 }
end

local seq = redis.call('INCR', KEYS[1])
if jobId == '' then
  jobId = tostring(seq)
end

local priority = tonumber(ARGV[5])
local delay = tonumber(ARGV[6])

redis.call('HSET', KEYS[6] .. jobId,
  'name', ARGV[2],
  'data', ARGV[3],
  'opts', ARGV[4],
  'priority', priority,
  'seq', seq,
  'timestamp', now,
  'attemptsMade', 0)

local event
if delay > 0 then
  redis.call('ZADD', KEYS[3], now + delay, jobId)
  event = 'delayed'
else
  -- priority in the high bits, insertion order in the low bits (ADR 0002)
  redis.call('ZADD', KEYS[2], priority * 4294967296 + seq, jobId)
  event = 'waiting'
end

-- Wake an idle worker. Delayed jobs also wake one so it can shorten its sleep.
redis.call('LPUSH', KEYS[4], '1')
redis.call('LTRIM', KEYS[4], 0, 99)

redis.call('XADD', KEYS[5], 'MAXLEN', '~', ARGV[7], '*', 'event', event, 'jobId', jobId, 'name', ARGV[2])

return { jobId, 1 }
