import { describe, expect, it } from 'vitest';
import { type BalanceOptions, balance, type QueueLoad } from '../src/balancer.js';

function load(name: string, overrides: Partial<QueueLoad> = {}): QueueLoad {
  return { name, waiting: 0, active: 0, avgRuntime: 0, concurrency: 1, ...overrides };
}

const auto: BalanceOptions = { strategy: 'auto', minProcesses: 1, maxProcesses: 10, maxShift: 100 };

describe('balance', () => {
  describe('simple', () => {
    const simple: BalanceOptions = { ...auto, strategy: 'simple' };

    it('splits processes evenly, giving the remainder to the first queues by name', () => {
      const result = balance([load('c'), load('a'), load('b')], {}, simple);
      expect(result).toEqual({ a: 4, b: 3, c: 3 });
    });

    it('ignores workload', () => {
      const result = balance([load('a', { waiting: 1_000 }), load('b')], {}, simple);
      expect(result).toEqual({ a: 5, b: 5 });
    });
  });

  describe('auto', () => {
    it('keeps every queue at minProcesses when all are idle', () => {
      expect(balance([load('a'), load('b')], {}, auto)).toEqual({ a: 1, b: 1 });
    });

    it('shares spare capacity in proportion to time to clear', () => {
      // a: 100 jobs × 30ms = 3000, b: 100 jobs × 10ms = 1000 → spare 8 split 6/2
      const result = balance(
        [load('a', { waiting: 100, avgRuntime: 30 }), load('b', { waiting: 100, avgRuntime: 10 })],
        {},
        auto,
      );
      expect(result).toEqual({ a: 7, b: 3 });
    });

    it('does not give a queue more processes than it can keep busy', () => {
      // 3 waiting jobs at concurrency 1 can use at most 3 processes.
      const result = balance([load('a', { waiting: 3, avgRuntime: 1_000 }), load('b')], {}, auto);
      expect(result).toEqual({ a: 3, b: 1 });
    });

    it('counts concurrency when estimating how many processes a queue needs', () => {
      const result = balance([load('a', { waiting: 20, concurrency: 10 }), load('b')], {}, auto);
      expect(result).toEqual({ a: 2, b: 1 });
    });

    it('offers capacity a small queue cannot use to the others', () => {
      // a would get most of the spare by weight but only needs 2 processes.
      const result = balance(
        [
          load('a', { waiting: 2, avgRuntime: 100_000 }),
          load('b', { waiting: 50, avgRuntime: 10 }),
        ],
        {},
        auto,
      );
      expect(result).toEqual({ a: 2, b: 8 });
    });

    it('uses the number of waiting jobs when there are no metrics yet', () => {
      const result = balance(
        [load('a', { waiting: 300 }), load('b', { waiting: 100 })],
        {},
        { ...auto, maxProcesses: 6 },
      );
      expect(result).toEqual({ a: 4, b: 2 });
    });
  });

  describe('shifting', () => {
    it('moves at most maxShift processes per round', () => {
      const loads = [load('a', { waiting: 1_000 }), load('b')];
      const options = { ...auto, maxShift: 2 };

      const round1 = balance(loads, { a: 1, b: 1 }, options);
      const round2 = balance(loads, round1, options);
      const round3 = balance(loads, round2, options);
      const round4 = balance(loads, round3, options);
      const round5 = balance(loads, round4, options);

      expect([round1, round2, round3, round4, round5].map((r) => r.a)).toEqual([3, 5, 7, 9, 9]);
    });

    it('scales down gradually too', () => {
      const result = balance([load('a'), load('b')], { a: 9, b: 1 }, { ...auto, maxShift: 3 });
      expect(result).toEqual({ a: 6, b: 1 });
    });

    it('starts queues it has not seen before directly at their target', () => {
      const result = balance([load('a', { waiting: 100 })], {}, { ...auto, maxShift: 1 });
      expect(result).toEqual({ a: 10 });
    });

    it('trims scale-ups so the total never exceeds maxProcesses', () => {
      // b drops by at most 1 per round, so a cannot take all it wants at once.
      const result = balance(
        [load('a', { waiting: 1_000 }), load('b')],
        { a: 1, b: 9 },
        { ...auto, maxShift: 5 },
      );
      expect(result).toEqual({ a: 6, b: 4 });
      expect(Object.values(result).reduce((a, b) => a + b, 0)).toBeLessThanOrEqual(10);
    });
  });

  it('shrinks other queues faster than maxShift to make room for a new queue', () => {
    // Found by the random test below: the cap must hold when a queue appears.
    const result = balance(
      [load('a', { waiting: 100 }), load('b')],
      { a: 5 },
      { strategy: 'simple', minProcesses: 2, maxProcesses: 5, maxShift: 1 },
    );
    expect(result).toEqual({ a: 3, b: 2 });
  });

  it('rejects options that cannot satisfy minProcesses', () => {
    expect(() =>
      balance([load('a'), load('b'), load('c')], {}, { ...auto, minProcesses: 4 }),
    ).toThrow(/maxProcesses/);
  });

  it('keeps its invariants for random inputs', () => {
    let seed = 42;
    const random = () => {
      seed = (seed * 1_103_515_245 + 12_345) % 2 ** 31;
      return seed / 2 ** 31;
    };
    const int = (max: number) => Math.floor(random() * (max + 1));

    for (let i = 0; i < 2_000; i++) {
      const queueCount = 1 + int(5);
      const minProcesses = int(2);
      const options: BalanceOptions = {
        strategy: random() < 0.5 ? 'auto' : 'simple',
        minProcesses,
        maxProcesses: queueCount * minProcesses + int(20),
        maxShift: 1 + int(5),
      };
      const loads = Array.from({ length: queueCount }, (_, q) =>
        load(`q${q}`, {
          waiting: int(3) === 0 ? 0 : int(5_000),
          active: int(20),
          avgRuntime: int(2_000),
          concurrency: 1 + int(9),
        }),
      );
      const current = Object.fromEntries(
        loads.filter(() => random() < 0.7).map((l) => [l.name, minProcesses + int(4)]),
      );

      const result = balance(loads, current, options);
      const total = Object.values(result).reduce((a, b) => a + b, 0);

      expect(Object.keys(result).sort()).toEqual(loads.map((l) => l.name).sort());
      for (const [name, count] of Object.entries(result)) {
        expect(Number.isInteger(count)).toBe(true);
        expect(count).toBeGreaterThanOrEqual(minProcesses);
        const before = current[name];
        if (before !== undefined && count > before) {
          expect(count - before).toBeLessThanOrEqual(options.maxShift);
        }
      }
      expect(total).toBeLessThanOrEqual(options.maxProcesses);
    }
  });
});
