--[[
  Deletes a job that is not currently being processed.

  KEYS[1] wait
  KEYS[2] delayed
  KEYS[3] completed
  KEYS[4] failed
  KEYS[5] events
  KEYS[6] job key prefix

  ARGV[1] job id
  ARGV[2] max events stream length

  Returns 1 when removed, 0 when the job does not exist, -1 when it is
  active (its worker owns it; removing it would break the lock protocol).
]]
local jobId = ARGV[1]
local jobKey = KEYS[6] .. jobId

if redis.call('EXISTS', jobKey .. ':lock') == 1 then
  return -1
end
if redis.call('EXISTS', jobKey) == 0 then
  return 0
end

for i = 1, 4 do
  redis.call('ZREM', KEYS[i], jobId)
end
redis.call('DEL', jobKey)

redis.call('XADD', KEYS[5], 'MAXLEN', '~', ARGV[2], '*', 'event', 'removed', 'jobId', jobId)

return 1
