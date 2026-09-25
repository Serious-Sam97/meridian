-- When a connection drops, ioredis resends commands whose reply was lost, so
-- a script can run twice for one call. Every call carries a unique token;
-- these short-lived receipts let the second run recognise the first one's
-- work instead of reporting a lost lock or claiming a second job.
local RECEIPT_TTL = 60000

local function markSettled(jobKey, token)
  redis.call('SET', jobKey .. ':settled', token, 'PX', RECEIPT_TTL)
end

local function wasSettledBy(jobKey, token)
  return redis.call('GET', jobKey .. ':settled') == token
end
