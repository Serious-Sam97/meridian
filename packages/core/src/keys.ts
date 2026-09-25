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
  /** Prefix for job hashes; the job id is appended inside Lua scripts. */
  jobPrefix: string;
  job(id: string): string;
  lock(id: string): string;
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
    jobPrefix,
    job: (id) => `${jobPrefix}${id}`,
    lock: (id) => `${jobPrefix}${id}:lock`,
  };
}
