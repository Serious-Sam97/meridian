import { dirname, isAbsolute, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import type { SupervisorOptions } from './supervisor.js';

/** A config file exports one supervisor, or several (like Horizon's supervisors per environment). */
export type MeridianConfig = SupervisorOptions | SupervisorOptions[];

/** Identity function that gives config files type checking and autocompletion. */
export function defineConfig(config: MeridianConfig): MeridianConfig {
  return config;
}

/**
 * Imports a config file. Processor paths are resolved relative to the file,
 * so a config works no matter which directory the CLI is started from.
 */
export async function loadConfig(path: string): Promise<SupervisorOptions[]> {
  const absolute = resolve(path);
  const mod = (await import(pathToFileURL(absolute).href)) as { default?: MeridianConfig };
  if (!mod.default) throw new Error(`${path} must have a default export`);

  const list = Array.isArray(mod.default) ? mod.default : [mod.default];
  if (list.length === 0) throw new Error(`${path} does not define any supervisor`);

  return list.map((options) => ({
    ...options,
    queues: Object.fromEntries(
      Object.entries(options.queues).map(([queue, config]) => [
        queue,
        {
          ...config,
          processor: isAbsolute(config.processor)
            ? config.processor
            : resolve(dirname(absolute), config.processor),
        },
      ]),
    ),
  }));
}
