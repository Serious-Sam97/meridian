/**
 * The worker no longer owns the job: its lock expired and the job was
 * recovered by the stalled-job checker. The result of this attempt is dropped.
 */
export class LockLostError extends Error {
  override readonly name = 'LockLostError';

  constructor(readonly jobId: string) {
    super(`Lost the lock for job ${jobId}; the result of this attempt was discarded`);
  }
}
