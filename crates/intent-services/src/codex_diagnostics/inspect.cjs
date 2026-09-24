// Local inspection only. Never import the adapter, execute npm, or consult a
// catalog. Node's own module resolver ties Codex to the selected npm entrypoint.
const fs = require('node:fs');
const path = require('node:path');
const { createRequire } = require('node:module');
const [entry, expectedVersion, runtimeOverride] = process.argv.slice(1);
const unknown = reason => { process.stdout.write(JSON.stringify({ reason })); };
const read = file => {
  const fd = fs.openSync(file, 'r');
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.size > 65536) throw new Error();
    const buffer = Buffer.alloc(65537);
    const length = fs.readSync(fd, buffer, 0, buffer.length, 0);
    if (length > 65536) throw new Error();
    return buffer.subarray(0, length).toString('utf8');
  } finally { fs.closeSync(fd); }
};
const isNodeEntrypoint = file => {
  const fd = fs.openSync(file, 'r');
  try {
    if (!fs.fstatSync(fd).isFile()) return false;
    const header = Buffer.alloc(128);
    const length = fs.readSync(fd, header, 0, header.length, 0);
    return header.subarray(0, length).toString('utf8').split(/\r?\n/, 1)[0] === '#!/usr/bin/env node';
  } finally { fs.closeSync(fd); }
};
const packageForBin = (entry, name, binName) => {
  const file = fs.realpathSync(entry);
  if (!/\.(?:c|m)?js$/.test(file) || !isNodeEntrypoint(file)) return null;
  let root = path.dirname(file);
  let manifest;
  for (let depth = 0; depth < 4; depth++) {
    const candidate = path.join(root, 'package.json');
    if (fs.existsSync(candidate)) { manifest = JSON.parse(read(candidate)); break; }
    root = path.dirname(root);
  }
  const bin = typeof manifest?.bin === 'string' ? manifest.bin : manifest?.bin?.[binName];
  if (manifest?.name !== name || typeof bin !== 'string' ||
      fs.realpathSync(path.resolve(root, bin)) !== file) return null;
  return { file, manifest };
};
try {
  const selected = packageForBin(entry, '@agentclientprotocol/codex-acp', 'codex-acp');
  if (!selected) {
    unknown('opaque');
  } else if (expectedVersion && selected.manifest.version !== expectedVersion) {
    unknown('mismatch');
  } else {
    const adapter = selected.file;
    // Use the adapter's actual resolution rule, then verify the selected
    // runtime bin too: CODEX_PATH may itself be an npm-installing wrapper.
    let runtime = null;
    let runtime_reason = 'missing';
    try {
      const candidate = runtimeOverride || createRequire(adapter).resolve('@openai/codex/bin/codex.js');
      runtime = packageForBin(candidate, '@openai/codex', 'codex')?.file ?? null;
      if (!runtime) runtime_reason = 'opaque';
    } catch {}
    process.stdout.write(JSON.stringify({ adapter, runtime, runtime_reason }));
  }
} catch { unknown('unreadable'); }
