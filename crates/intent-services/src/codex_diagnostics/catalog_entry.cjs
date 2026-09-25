// Loaded only by the explicitly requested, selected npm launch. Record the
// Node entrypoint that actually executes, never a PATH/cache candidate. The
// Rust inspector separately checks its exact pin and runtime dependency.
const fs = require('node:fs');
const path = require('node:path');
try {
  const entry = fs.realpathSync(process.argv[1]);
  let root = path.dirname(entry);
  for (let depth = 0; depth < 4; depth++, root = path.dirname(root)) {
    const manifestPath = path.join(root, 'package.json');
    if (!fs.existsSync(manifestPath)) continue;
    const fd = fs.openSync(manifestPath, 'r');
    let manifest;
    try {
      if (!fs.fstatSync(fd).isFile() || fs.fstatSync(fd).size > 65536) break;
      const buffer = Buffer.alloc(65537);
      const length = fs.readSync(fd, buffer, 0, buffer.length, 0);
      if (length > 65536) break;
      manifest = JSON.parse(buffer.subarray(0, length).toString('utf8'));
    } finally { fs.closeSync(fd); }
    const bin = typeof manifest.bin === 'string' ? manifest.bin : manifest.bin?.['codex-acp'];
    if (manifest.name === '@agentclientprotocol/codex-acp' && typeof bin === 'string'
        && fs.realpathSync(path.resolve(root, bin)) === entry && entry.length < 8192) {
      fs.writeFileSync(process.env.INTENT_CODEX_ENTRY, JSON.stringify(entry), {flag:'wx',mode:0o600});
      delete process.env.NODE_OPTIONS;
      delete process.env.INTENT_CODEX_ENTRY;
    }
    break;
  }
} catch {}
