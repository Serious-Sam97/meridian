--[[
  Adds a job to the wait set, or to the delayed set when it has a delay
  (see includes/createJob.lua).

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
--@include createJob

local keys = {
  id = KEYS[1], wait = KEYS[2], delayed = KEYS[3],
  marker = KEYS[4], events = KEYS[5], jobPrefix = KEYS[6],
}
local jobId, created = createJob(keys, ARGV[1], ARGV[2], ARGV[3], ARGV[4],
  tonumber(ARGV[5]), tonumber(ARGV[6]), ARGV[7], nowMs())

return { jobId, created }
