--[[
  Deletes a scheduler and the job planned for its next run, unless a worker
  already took that job.

  KEYS[1] schedulers
  KEYS[2] scheduler hash
  KEYS[3] delayed
  KEYS[4] job key prefix

  ARGV[1] scheduler id

  Returns 1 when removed, 0 when there was no such scheduler.
]]
local id = ARGV[1]
local nextRun = redis.call('HGET', KEYS[2], 'next')
if not nextRun then
  return 0
end

local jobId = 'repeat:' .. id .. ':' .. nextRun
if redis.call('ZREM', KEYS[3], jobId) == 1 then
  redis.call('DEL', KEYS[4] .. jobId)
end

redis.call('DEL', KEYS[2])
redis.call('ZREM', KEYS[1], id)
return 1
