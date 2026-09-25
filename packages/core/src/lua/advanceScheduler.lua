--[[
  Moves a scheduler from the run being processed to the next one and creates
  the job for it. A compare-and-set on `next` makes sure exactly one worker
  advances each run (ADR 0006).

  KEYS: same as upsertScheduler

  ARGV[1] scheduler id
  ARGV[2] run being processed (ms)
  ARGV[3] next run (ms)
  ARGV[4] max events stream length

  Returns 1 when advanced, 0 when the scheduler is gone or was already moved.
]]
--@include createJob

local id = ARGV[1]
local current = redis.call('HGET', KEYS[2], 'next')
if not current or tonumber(current) ~= tonumber(ARGV[2]) then
  return 0
end

local now = nowMs()
local nextRun = tonumber(ARGV[3])
local template = redis.call('HMGET', KEYS[2], 'name', 'data', 'opts')

redis.call('HSET', KEYS[2], 'next', nextRun)
redis.call('HINCRBY', KEYS[2], 'iterations', 1)
redis.call('ZADD', KEYS[1], nextRun, id)

local opts = cjson.decode(template[3])
opts['repeat'] = { scheduler = id, runAt = nextRun }
local keys = {
  id = KEYS[3], wait = KEYS[4], delayed = KEYS[5],
  marker = KEYS[6], events = KEYS[7], jobPrefix = KEYS[8],
}
createJob(keys, 'repeat:' .. id .. ':' .. nextRun, template[1], template[2], cjson.encode(opts),
  opts.priority or 0, math.max(nextRun - now, 0), ARGV[4], now)

return 1
