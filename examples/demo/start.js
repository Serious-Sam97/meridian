// Starts the whole demo: supervisor, dashboard and producer.
// Run from the repository root with `npm run demo` (it builds the packages first).
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../../', import.meta.url));
const here = fileURLToPath(new URL('.', import.meta.url));

const processes = [
  ['supervisor', [`${root}packages/supervisor/dist/cli.js`, `${here}meridian.config.js`]],
  ['dashboard', [`${root}packages/dashboard/dist/cli.js`, '--port', process.env.PORT ?? '3000']],
  ['producer', [`${here}producer.js`]],
].map(([name, args]) => {
  const child = spawn(process.execPath, args, {
    stdio: ['ignore', 'pipe', 'pipe'],
    env: process.env,
  });
  const prefix = `[${name}]`.padEnd(13);
  const print = (chunk) => {
    for (const line of chunk.toString().trimEnd().split('\n')) console.log(`${prefix}${line}`);
  };
  child.stdout.on('data', print);
  child.stderr.on('data', print);
  return child;
});

console.log(`\nDashboard: http://127.0.0.1:${process.env.PORT ?? '3000'}  (Ctrl+C to stop)\n`);

let stopping = false;
const stop = () => {
  if (stopping) return;
  stopping = true;
  console.log('\nStopping: the supervisor lets running jobs finish first...');
  for (const child of processes) child.kill('SIGTERM');
};
process.on('SIGINT', stop);
process.on('SIGTERM', stop);
await Promise.all(processes.map((child) => new Promise((resolve) => child.once('exit', resolve))));
