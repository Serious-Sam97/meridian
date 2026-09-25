export interface QueueKeys {
  /** Prefix shared by every key of the queue, e.g. `meridian:{emails}:` */
  base: string;
  id: string;
  wait: string;
  delayed: string;
  active: string;
  completed: string;
  failed: string;
  marker: string;
  meta: string;
  events: string;
  stalledCheck: string;
  /** Counter for the current rate limit window. */
  limiter: string;
  /** Prefix for per-minute metric buckets; the bucket start (ms) is appended. */
  metricsPrefix: string;
  /** Sorted set of scheduler ids by next run time (ADR 0006). */
  schedulers: string;
  scheduler(id: string): string;
  /** Prefix for job hashes; the job id is appended inside Lua scripts. */
  jobPrefix: string;
  job(id: string): string;
  lock(id: string): string;
}

/** Keys used by the scheduler scripts, in the order they expect them. */
export function schedulerScriptKeys(keys: QueueKeys, id: string): string[] {
  return [
    keys.schedulers,
    keys.scheduler(id),
    keys.id,
    keys.wait,
    keys.delayed,
    keys.marker,
    keys.events,
    keys.jobPrefix,
  ];
}

/**
 * Builds the key names for a queue. The queue name is wrapped in `{}` so all
 * keys hash to the same Redis Cluster slot and can be used by a single script
 * (see ADR 0002).
 */
export function queueKeys(queue: string, prefix = 'meridian'): QueueKeys {
  if (!queue || /[{}]/.test(queue)) {
    throw new Error(`Invalid queue name "${queue}": must be non-empty and not contain braces`);
  }

  const base = `${prefix}:{${queue}}:`;
  const jobPrefix = `${base}job:`;

  return {
    base,
    id: `${base}id`,
    wait: `${base}wait`,
    delayed: `${base}delayed`,
    active: `${base}active`,
    completed: `${base}completed`,
    failed: `${base}failed`,
    marker: `${base}marker`,
    meta: `${base}meta`,
    events: `${base}events`,
    stalledCheck: `${base}stalled-check`,
    limiter: `${base}limiter`,
    metricsPrefix: `${base}metrics:`,
    schedulers: `${base}schedulers`,
    scheduler: (id) => `${base}scheduler:${id}`,
    jobPrefix,
    job: (id) => `${jobPrefix}${id}`,
    lock: (id) => `${jobPrefix}${id}:lock`,
  };
}
