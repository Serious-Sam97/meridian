--[[
  Finds active jobs whose lock expired (their worker died or froze) and moves
  them back to wait, or to failed once they stalled too often (ADR 0003).

  Runs at most once per interval across all workers, guarded by a SET NX key.

  KEYS[1] stalled-check throttle key
  KEYS[2] active
  KEYS[3] wait
  KEYS[4] failed
  KEYS[5] marker
  KEYS[6] events
  KEYS[7] job key prefix

  ARGV[1] check interval (ms)
  ARGV[2] max stalled count
  ARGV[3] max events stream length

  Returns { recoveredIds, failedIds }.
]]
if not redis.call('SET', KEYS[1], '1', 'PX', ARGV[1], 'NX') then
  return { {}, {} }
end

local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
local maxStalled = tonumber(ARGV[2])

local recovered = {}
local failed = {}

-- A job enters active together with its lock in one script, so an active
-- job without a lock can only mean its lock expired.
for _, jobId in ipairs(redis.call('ZRANGE', KEYS[2], 0, -1)) do
  local jobKey = KEYS[7] .. jobId
  if redis.call('EXISTS', jobKey .. ':lock') == 0 then
    redis.call('ZREM', KEYS[2], jobId)
    local stalledCount = redis.call('HINCRBY', jobKey, 'stalledCount', 1)

    if stalledCount > maxStalled then
      redis.call('HSET', jobKey,
        'failedReason', 'job stalled more than the allowed ' .. maxStalled .. ' time(s)',
        'finishedOn', now)
      redis.call('ZADD', KEYS[4], now, jobId)
      redis.call('XADD', KEYS[6], 'MAXLEN', '~', ARGV[3], '*', 'event', 'failed', 'jobId', jobId)
      table.insert(failed, jobId)
    else
      local fields = redis.call('HMGET', jobKey, 'priority', 'seq')
      redis.call('ZADD', KEYS[3], tonumber(fields[1]) * 4294967296 + tonumber(fields[2]), jobId)
      redis.call('XADD', KEYS[6], 'MAXLEN', '~', ARGV[3], '*', 'event', 'stalled', 'jobId', jobId)
      table.insert(recovered, jobId)
    end
  end
end

if #recovered > 0 then
  redis.call('LPUSH', KEYS[5], '1')
  redis.call('LTRIM', KEYS[5], 0, 99)
end

return { recovered, failed }
