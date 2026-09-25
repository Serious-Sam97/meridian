#!/usr/bin/env node
import { existsSync } from 'node:fs';
import { loadConfig } from './config.js';
import { Supervisor } from './supervisor.js';

const USAGE = `Usage: meridian-supervisor [config]

Starts the supervisors defined in a config file (default: meridian.config.js
or meridian.config.mjs in the current directory). Stops gracefully on
SIGTERM or SIGINT; a second signal exits immediately.`;

function log(message: string): void {
  console.log(`${new Date().toISOString()} ${message}`);
}

function findConfig(arg: string | undefined): string {
  if (arg) return arg;
  const found = ['meridian.config.js', 'meridian.config.mjs'].find((file) => existsSync(file));
  if (!found) {
    console.error(`No config file given and none found.\n\n${USAGE}`);
    process.exit(1);
  }
  return found;
}

async function main(): Promise<void> {
  const arg = process.argv[2];
  if (arg === '--help' || arg === '-h') {
    console.log(USAGE);
    return;
  }

  const configPath = findConfig(arg);
  const supervisors = (await loadConfig(configPath)).map((options) => new Supervisor(options));

  for (const supervisor of supervisors) {
    supervisor.on('scaled', (allocation) => {
      const summary = Object.entries(allocation)
        .map(([queue, count]) => `${queue}=${count}`)
        .join(' ');
      log(`[${supervisor.name}] processes: ${summary}`);
    });
    supervisor.on('crash', (queue, pid, restartIn) =>
      log(`[${supervisor.name}] worker ${pid} for ${queue} crashed, restarting in ${restartIn}ms`),
    );
    supervisor.on('recycle', (queue, pid, reason) =>
      log(`[${supervisor.name}] recycled worker ${pid} for ${queue}: ${reason}`),
    );
    supervisor.on('error', (err) => log(`[${supervisor.name}] error: ${err.stack ?? err.message}`));
  }

  let stopping = false;
  const shutdown = async (signal: NodeJS.Signals) => {
    if (stopping) {
      log(`received ${signal} again, exiting immediately`);
      process.exit(1);
    }
    stopping = true;
    log(`received ${signal}, waiting for running jobs to finish...`);
    await Promise.all(supervisors.map((s) => s.stop()));
    log('stopped');
    process.exit(0);
  };
  process.on('SIGTERM', (signal) => void shutdown(signal));
  process.on('SIGINT', (signal) => void shutdown(signal));

  await Promise.all(supervisors.map((s) => s.start()));
  log(`started ${supervisors.map((s) => s.name).join(', ')} using ${configPath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
