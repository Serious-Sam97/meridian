--[[
  Creates or updates a job scheduler and makes sure the job for its next run
  exists (ADR 0006).

  KEYS[1] schedulers
  KEYS[2] scheduler hash
  KEYS[3] id counter
  KEYS[4] wait
  KEYS[5] delayed
  KEYS[6] marker
  KEYS[7] events
  KEYS[8] job key prefix
  KEYS[9] tag key prefix

  ARGV[1] scheduler id
  ARGV[2] schedule (JSON)
  ARGV[3] job name
  ARGV[4] job data (JSON)
  ARGV[5] job options template (JSON)
  ARGV[6] next run (ms)
  ARGV[7] max events stream length
]]
--@include createJob

local now = nowMs()
local id = ARGV[1]
local nextRun = tonumber(ARGV[6])

-- The schedule changed: drop the job planned for the old next run, unless a
-- worker already took it.
local previous = redis.call('HGET', KEYS[2], 'next')
if previous and tonumber(previous) ~= nextRun then
  local oldJobId = 'repeat:' .. id .. ':' .. previous
  if redis.call('ZREM', KEYS[5], oldJobId) == 1 then
    deleteJob(KEYS[8], KEYS[9], oldJobId)
  end
end

redis.call('HSET', KEYS[2],
  'schedule', ARGV[2], 'name', ARGV[3], 'data', ARGV[4], 'opts', ARGV[5], 'next', nextRun)
redis.call('HSETNX', KEYS[2], 'createdAt', now)
redis.call('ZADD', KEYS[1], nextRun, id)

local opts = cjson.decode(ARGV[5])
opts['repeat'] = { scheduler = id, runAt = nextRun }
local keys = {
  id = KEYS[3], wait = KEYS[4], delayed = KEYS[5],
  marker = KEYS[6], events = KEYS[7], jobPrefix = KEYS[8], tagPrefix = KEYS[9],
}
createJob(keys, 'repeat:' .. id .. ':' .. nextRun, ARGV[3], ARGV[4], cjson.encode(opts),
  opts.priority or 0, math.max(nextRun - now, 0), ARGV[7], now)

return 1
