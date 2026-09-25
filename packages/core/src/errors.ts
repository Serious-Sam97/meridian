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

/**
 * Passed as the abort reason when close() times out: the job is handed back
 * to the queue for another worker and this attempt's result is discarded.
 */
export class WorkerClosingError extends Error {
  override readonly name = 'WorkerClosingError';

  constructor() {
    super('Worker is shutting down; the job was released back to the queue');
  }
}

/**
 * Throw from a processor to fail the job immediately, without using the
 * remaining attempts. For errors a retry cannot fix, such as invalid input.
 */
export class UnrecoverableError extends Error {
  override readonly name = 'UnrecoverableError';
}
