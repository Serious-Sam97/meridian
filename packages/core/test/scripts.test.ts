import { describe, expect, it } from 'vitest';
import { preprocess } from '../src/scripts.js';

describe('lua preprocessing', () => {
  it('splices includes into the script', () => {
    const source = preprocess('retryJob');
    expect(source).not.toContain('--@include');
    expect(source).toContain('local function nowMs()');
  });

  it('defines each include once', () => {
    const source = preprocess('retryJob');
    expect(source.match(/local function nowMs\(\)/g)).toHaveLength(1);
  });
});
