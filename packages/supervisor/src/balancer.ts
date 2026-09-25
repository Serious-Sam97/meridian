export type BalanceStrategy = 'simple' | 'auto';

export interface QueueLoad {
  name: string;
  waiting: number;
  active: number;
  /** Average processing time over recent minutes, in ms. 0 when unknown. */
  avgRuntime: number;
  /** Jobs each process of this queue runs at once. */
  concurrency: number;
}

export interface BalanceOptions {
  strategy: BalanceStrategy;
  /** Processes every queue keeps, even when idle. */
  minProcesses: number;
  /** Processes shared by all queues of the supervisor. */
  maxProcesses: number;
  /** Most processes a single queue gains or loses per balancing round. */
  maxShift: number;
}

/** Processes per queue name. */
export type Allocation = Record<string, number>;

/**
 * Decides how many processes each queue should run (ADR 0004). Pure and
 * deterministic, so it can be tested without processes or Redis.
 *
 * Queues missing from `current` start directly at their target; the others
 * move towards it by at most `maxShift`.
 */
export function balance(
  loads: QueueLoad[],
  current: Allocation,
  options: BalanceOptions,
): Allocation {
  validate(loads, options);
  if (loads.length === 0) return {};

  const targets =
    options.strategy === 'simple' ? simpleTargets(loads, options) : autoTargets(loads, options);
  return applyShift(loads, targets, current, options);
}

/** Splits maxProcesses evenly; the remainder goes to the first queues by name. */
function simpleTargets(loads: QueueLoad[], options: BalanceOptions): Allocation {
  const names = loads.map((l) => l.name).sort();
  const base = Math.floor(options.maxProcesses / names.length);
  const extra = options.maxProcesses % names.length;
  return Object.fromEntries(names.map((name, i) => [name, base + (i < extra ? 1 : 0)]));
}

/**
 * Every queue gets minProcesses. The spare capacity is shared in proportion to
 * each queue's time to clear (waiting × avgRuntime), but a queue never gets
 * more processes than it can keep busy: ceil((waiting + active) / concurrency).
 * Capacity a queue cannot use is offered to the others (water-filling), and
 * capacity nobody needs stays unused.
 */
function autoTargets(loads: QueueLoad[], options: BalanceOptions): Allocation {
  const { minProcesses, maxProcesses } = options;
  const need = new Map<string, number>();
  const weight = new Map<string, number>();
  for (const load of loads) {
    const busy = Math.ceil((load.waiting + load.active) / load.concurrency);
    need.set(load.name, Math.max(0, busy - minProcesses));
    // With no metrics yet, fall back to the number of waiting jobs.
    weight.set(load.name, load.waiting * Math.max(load.avgRuntime, 1));
  }

  const share = new Map<string, number>();
  let spare = maxProcesses - loads.length * minProcesses;
  let open = loads
    .map((l) => l.name)
    .filter((name) => (need.get(name) ?? 0) > 0 && (weight.get(name) ?? 0) > 0);

  while (open.length > 0 && spare > 0) {
    const totalWeight = open.reduce((sum, name) => sum + (weight.get(name) ?? 0), 0);
    const proportional = (name: string) => (spare * (weight.get(name) ?? 0)) / totalWeight;
    const saturated = open.filter((name) => proportional(name) >= (need.get(name) ?? 0));

    if (saturated.length === 0) {
      for (const name of open) share.set(name, proportional(name));
      break;
    }
    for (const name of saturated) {
      share.set(name, need.get(name) ?? 0);
      spare -= need.get(name) ?? 0;
    }
    open = open.filter((name) => !saturated.includes(name));
  }

  return Object.fromEntries(
    Object.entries(roundShares(loads, share)).map(([name, extra]) => [name, minProcesses + extra]),
  );
}

/** Rounds fractional shares to integers with the largest-remainder method. */
function roundShares(loads: QueueLoad[], share: Map<string, number>): Allocation {
  const result: Allocation = {};
  let total = 0;
  for (const { name } of loads) {
    const value = share.get(name) ?? 0;
    result[name] = Math.floor(value + 1e-9);
    total += value;
  }

  let leftover = Math.floor(total + 1e-9) - Object.values(result).reduce((a, b) => a + b, 0);
  const byRemainder = loads
    .map(({ name }) => ({ name, remainder: (share.get(name) ?? 0) - (result[name] ?? 0) }))
    .filter((entry) => entry.remainder > 1e-9)
    .sort((a, b) => b.remainder - a.remainder || a.name.localeCompare(b.name));
  for (const { name } of byRemainder) {
    if (leftover <= 0) break;
    result[name] = (result[name] ?? 0) + 1;
    leftover--;
  }
  return result;
}

/**
 * Moves each queue towards its target by at most maxShift, then trims
 * scale-ups if the total would exceed maxProcesses. Scale-downs are never
 * trimmed, so capacity freed by one queue is available to others right away.
 * maxProcesses is a hard cap: it holds even if that means a faster shift.
 */
function applyShift(
  loads: QueueLoad[],
  targets: Allocation,
  current: Allocation,
  options: BalanceOptions,
): Allocation {
  const result: Allocation = {};
  for (const { name } of loads) {
    const target = targets[name] ?? options.minProcesses;
    const now = current[name];
    const next =
      now === undefined
        ? target
        : now + Math.max(-options.maxShift, Math.min(options.maxShift, target - now));
    result[name] = Math.max(options.minProcesses, next);
  }

  let excess = Object.values(result).reduce((a, b) => a + b, 0) - options.maxProcesses;
  while (excess > 0) {
    // Take one process from the queue that grew the most this round.
    const [name] = Object.entries(result)
      .map(([n, count]) => [n, count - (current[n] ?? options.minProcesses)] as const)
      .filter(([n, growth]) => growth > 0 && (result[n] ?? 0) > options.minProcesses)
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))[0] ?? [undefined];
    if (name === undefined) break;
    result[name] = (result[name] ?? 0) - 1;
    excess--;
  }

  // Still over: a new queue claimed its minimum while the others could only
  // shrink by maxShift. The cap wins over smooth scaling, so take the rest
  // from the largest queues.
  while (excess > 0) {
    const [name] = Object.entries(result)
      .filter(([, count]) => count > options.minProcesses)
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))[0] ?? [undefined];
    if (name === undefined) break;
    result[name] = (result[name] ?? 0) - 1;
    excess--;
  }
  return result;
}

function validate(loads: QueueLoad[], options: BalanceOptions): void {
  const { minProcesses, maxProcesses, maxShift } = options;
  if (!Number.isInteger(minProcesses) || minProcesses < 0) {
    throw new RangeError('minProcesses must be a non-negative integer');
  }
  if (!Number.isInteger(maxShift) || maxShift < 1) {
    throw new RangeError('maxShift must be a positive integer');
  }
  if (!Number.isInteger(maxProcesses) || maxProcesses < loads.length * minProcesses) {
    throw new RangeError(
      `maxProcesses (${maxProcesses}) must cover minProcesses (${minProcesses}) for each of the ${loads.length} queues`,
    );
  }
  for (const load of loads) {
    if (!(load.concurrency >= 1)) throw new RangeError(`concurrency of ${load.name} must be >= 1`);
  }
}
