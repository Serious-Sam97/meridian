// tsc does not copy non-TS assets, so the Lua scripts are copied into dist by hand.
import { cpSync } from 'node:fs';

cpSync(new URL('../src/lua', import.meta.url), new URL('../dist/lua', import.meta.url), {
  recursive: true,
});
