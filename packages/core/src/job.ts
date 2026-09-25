import type { JobOptions } from './types.js';

export class Job<Data = unknown, Result = unknown> {
  constructor(
    readonly id: string,
    readonly name: string,
    readonly data: Data,
    readonly opts: JobOptions,
    readonly timestamp: number,
    public attemptsMade = 0,
    public processedOn?: number,
    public finishedOn?: number,
    public failedReason?: string,
    public stacktrace?: string,
    public returnValue?: Result,
    public progress?: unknown,
  ) {}

  static fromHash<Data, Result>(id: string, hash: Record<string, string>): Job<Data, Result> {
    return new Job<Data, Result>(
      id,
      hash.name ?? '',
      parseJson(hash.data) as Data,
      (parseJson(hash.opts) ?? {}) as JobOptions,
      Number(hash.timestamp),
      Number(hash.attemptsMade ?? 0),
      optionalNumber(hash.processedOn),
      optionalNumber(hash.finishedOn),
      hash.failedReason,
      hash.stacktrace,
      parseJson(hash.returnValue) as Result | undefined,
      parseJson(hash.progress),
    );
  }
}

function parseJson(value: string | undefined): unknown {
  return value === undefined ? undefined : JSON.parse(value);
}

function optionalNumber(value: string | undefined): number | undefined {
  return value === undefined ? undefined : Number(value);
}
