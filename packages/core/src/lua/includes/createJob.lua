--@include common
--@include tags

-- Creates a job and puts it in the wait set, or the delayed set when it has a
-- delay. Adding is idempotent on a custom id. Returns jobId, created (1 or 0).
--
-- k holds the queue keys: id, wait, delayed, marker, events, jobPrefix, tagPrefix.
local function createJob(k, jobId, name, data, opts, priority, delay, maxEvents, now)
  if jobId ~= '' and redis.call('EXISTS', k.jobPrefix .. jobId) == 1 then
    return jobId, 0
  end

  local seq = redis.call('INCR', k.id)
  if jobId == '' then
    jobId = tostring(seq)
  end
  local jobKey = k.jobPrefix .. jobId

  redis.call('HSET', jobKey,
    'name', name,
    'data', data,
    'opts', opts,
    'priority', priority,
    'seq', seq,
    'timestamp', now,
    'attemptsMade', 0)
  indexTags(k.tagPrefix, opts, jobId, now)

  local event
  if delay > 0 then
    redis.call('ZADD', k.delayed, now + delay, jobId)
    event = 'delayed'
  else
    pushWaiting(k.wait, jobKey, jobId)
    event = 'waiting'
  end

  -- Wake an idle worker. Delayed jobs also wake one so it can shorten its sleep.
  wakeWorker(k.marker)
  redis.call('XADD', k.events, 'MAXLEN', '~', maxEvents, '*', 'event', event, 'jobId', jobId, 'name', name)

  return jobId, 1
end
