// Entry point of a worker process started by a ProcessPool. It runs one
// Worker for one queue and never outlives its supervisor (ADR 0004).
import { pathToFileURL } from 'node:url';
import { type Processor, Worker } from '@meridian/core';
import { CHILD_CONFIG_ENV, type ChildConfig } from './child-config.js';

async function loadProcessor(path: string): Promise<Processor<unknown, unknown>> {
  const mod = (await import(pathToFileURL(path).href)) as { default?: unknown };
  if (typeof mod.default !== 'function') {
    throw new TypeError(`${path} must export the processor function as its default export`);
  }
  return mod.default as Processor<unknown, unknown>;
}

async function main(): Promise<void> {
  const raw = process.env[CHILD_CONFIG_ENV];
  if (!raw)
    throw new Error(`${CHILD_CONFIG_ENV} is not set; this file is started by a ProcessPool`);
  const config = JSON.parse(raw) as ChildConfig;

  const worker = new Worker(config.queue, await loadProcessor(config.processor), {
    ...config.workerOptions,
    connection: config.connection,
    prefix: config.prefix,
    concurrency: config.concurrency,
  });
  worker.on('error', (err) => console.error(`[meridian:${config.queue}:${process.pid}]`, err));

  let stopping = false;
  const stop = async () => {
    if (stopping) return;
    stopping = true;
    clearInterval(memoryTimer);
    clearTimeout(lifetimeTimer);
    await worker.close({ timeout: config.shutdownTimeout });
    process.exit(0);
  };

  // Recycling: tell the pool this exit is planned, then drain and exit.
  const { maxMemory, maxJobs, maxTime } = config.recycle ?? {};
  const recycle = (reason: string) => {
    if (stopping) return;
    process.send?.({ type: 'recycle', reason });
    void stop();
  };
  const checkMemory = () => {
    const mb = process.memoryUsage().rss / 1024 / 1024;
    if (maxMemory && mb > maxMemory) recycle(`memory ${Math.round(mb)} MB > ${maxMemory} MB`);
  };
  let handled = 0;
  const afterJob = () => {
    handled++;
    if (maxJobs && handled >= maxJobs) recycle(`handled ${handled} jobs`);
    else checkMemory();
  };
  worker.on('completed', afterJob);
  worker.on('failed', afterJob);
  worker.on('retrying', afterJob);
  // Memory can also grow while a long job runs, so check on a timer too.
  const memoryTimer = maxMemory ? setInterval(checkMemory, 5_000) : undefined;
  const lifetimeTimer = maxTime
    ? setTimeout(() => recycle(`lived ${maxTime} ms`), maxTime)
    : undefined;

  process.on('message', (message: { type?: string }) => {
    if (message?.type === 'shutdown') void stop();
  });
  // The IPC channel closes when the supervisor dies: do not keep running as an orphan.
  process.on('disconnect', () => void stop());
  process.on('SIGTERM', () => void stop());
  process.on('SIGINT', () => void stop());

  process.send?.({ type: 'ready' });
}

main().catch((err) => {
  console.error('[meridian] worker process failed to start:', err);
  process.exit(1);
});
