import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import type { Redis } from 'ioredis';

export type ScriptName =
  | 'addJob'
  | 'extendLocks'
  | 'moveToActive'
  | 'moveToFinished'
  | 'moveStalledJobs'
  | 'retryJob';

interface LoadedScript {
  source: string;
  sha: string;
}

const cache = new Map<ScriptName, LoadedScript>();

function load(name: ScriptName): LoadedScript {
  let script = cache.get(name);
  if (!script) {
    const source = readFileSync(new URL(`./lua/${name}.lua`, import.meta.url), 'utf8');
    script = { source, sha: createHash('sha1').update(source).digest('hex') };
    cache.set(name, script);
  }
  return script;
}

/**
 * Runs a Lua script with EVALSHA, falling back to EVAL when Redis does not have
 * it cached yet (after a restart or SCRIPT FLUSH). EVAL caches it again.
 */
export async function runScript<T = unknown>(
  client: Redis,
  name: ScriptName,
  keys: string[],
  args: (string | number)[],
): Promise<T> {
  const { source, sha } = load(name);
  try {
    return (await client.evalsha(sha, keys.length, ...keys, ...args)) as T;
  } catch (err) {
    if (err instanceof Error && err.message.startsWith('NOSCRIPT')) {
      return (await client.eval(source, keys.length, ...keys, ...args)) as T;
    }
    throw err;
  }
}
