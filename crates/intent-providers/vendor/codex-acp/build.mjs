// Modified by Intent: build an offline adapter bundle with dependency notices and content identity.
import { build } from 'esbuild';
import { createHash } from 'node:crypto';
import { readFile, writeFile, readdir } from 'node:fs/promises';
import { dirname, join } from 'node:path';

const result = await build({
  entryPoints: ['src/index.ts'],
  bundle: true,
  platform: 'node',
  format: 'esm',
  outfile: 'dist/codex-acp.mjs',
  legalComments: 'inline',
  metafile: true,
  // Polyfill `require` for CJS modules bundled into ESM output
  banner: {
    js: "import { createRequire as __createRequire } from 'module'; const require = __createRequire(import.meta.url);",
  },
});

const bundle = await readFile('dist/codex-acp.mjs');
await writeFile('dist/version', createHash('sha256').update(bundle).digest('hex'));
const packages = new Map();
for (const input of Object.keys(result.metafile.inputs).sort()) {
  if (!input.startsWith('node_modules/')) continue;
  let dir = dirname(input);
  while (dir.startsWith('node_modules/')) {
    try {
      const metadata = JSON.parse(await readFile(join(dir, 'package.json'), 'utf8'));
      if (metadata.name && metadata.version) {
        const key = `${metadata.name}@${metadata.version}`;
        if (!packages.has(key)) {
          const files = (await readdir(dir)).filter(name => /^(license|copying|notice)/i.test(name)).sort();
          if (!files.length) throw new Error(`Missing license: ${key}`);
          packages.set(key, `${key} (${metadata.license})\n` + (await Promise.all(files.map(name => readFile(join(dir, name), 'utf8')))).join('\n'));
        }
        break;
      }
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
    dir = dirname(dir);
  }
}
await writeFile('dist/THIRD_PARTY_LICENSES', [...packages.values()].join('\n\n-----\n\n'));
