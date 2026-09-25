import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  resolve: {
    // Test against the TypeScript sources of sibling packages, not their builds.
    alias: {
      '@meridian/core': fileURLToPath(new URL('./packages/core/src/index.ts', import.meta.url)),
      '@meridian/supervisor': fileURLToPath(
        new URL('./packages/supervisor/src/index.ts', import.meta.url),
      ),
    },
  },
  test: {
    include: ['packages/*/test/**/*.test.ts'],
    testTimeout: 15_000,
    hookTimeout: 15_000,
  },
});
