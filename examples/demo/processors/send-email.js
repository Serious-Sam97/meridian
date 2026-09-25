import { setTimeout as sleep } from 'node:timers/promises';
import { UnrecoverableError } from '@meridian/core';

/** Pretends to call an email provider: fast, with occasional transient errors. */
export default async function sendEmail(job, signal) {
  if (!job.data.to.includes('@')) throw new UnrecoverableError(`invalid address: ${job.data.to}`);
  await sleep(40 + Math.random() * 120, undefined, { signal });
  if (Math.random() < 0.05) throw new Error('SMTP 421: service not available, try again later');
  return { messageId: `msg_${job.id}_${Date.now().toString(36)}` };
}
