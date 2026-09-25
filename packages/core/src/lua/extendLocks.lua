--[[
  Extends the locks of every job a worker is processing, in one round trip.

  KEYS[1] job key prefix

  ARGV[1] lock duration (ms)
  ARGV[2..] pairs of job id, lock token

  Returns the ids whose lock is no longer held by the caller.
]]
local lost = {}

for i = 2, #ARGV, 2 do
  local jobId = ARGV[i]
  local lockKey = KEYS[1] .. jobId .. ':lock'
  if redis.call('GET', lockKey) == ARGV[i + 1] then
    redis.call('PEXPIRE', lockKey, ARGV[1])
  else
    table.insert(lost, jobId)
  end
end

return lost
