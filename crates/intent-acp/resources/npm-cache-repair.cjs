'use strict';

const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { spawnSync } = require('node:child_process');

function repair() {
  const [spec, npm, expires] = process.argv.slice(1);
  const timely = () => Date.now() < Number(expires);
  if (!timely()) return false;
  const split = spec.lastIndexOf('@');
  if (split <= 0) return false;
  const name = spec.slice(0, split);
  const version = spec.slice(split + 1);
  const config = spawnSync(npm, ['config', 'get', 'cache', '--workspaces=false'], {
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'ignore'],
    timeout: 2000,
    killSignal: 'SIGKILL',
    maxBuffer: 65536,
    shell: false,
  });
  if (config.error || config.status !== 0 || !timely()) return false;
  const cache = config.stdout.replace(/[\r\n]+$/, '');
  if (!path.isAbsolute(cache) || /[\r\n]/.test(cache)) return false;
  const key = crypto.createHash('sha512').update(spec).digest('hex').slice(0, 16);
  const npx = path.join(cache, '_npx');
  const install = path.join(npx, key);
  const directory = p => fs.lstatSync(p, { bigint: true }).isDirectory();
  if (![cache, npx, install].every(directory)) return false;
  const identity = fs.lstatSync(install, { bigint: true });
  const manifest = path.join(install, 'package.json');
  try {
    fs.lstatSync(manifest);
    return false;
  } catch (error) {
    if (error.code !== 'ENOENT') return false;
  }
  let dependency = path.join(install, 'node_modules');
  if (!directory(dependency)) return false;
  for (const segment of name.split('/')) {
    dependency = path.join(dependency, segment);
    if (!directory(dependency)) return false;
  }
  const installedPath = path.join(dependency, 'package.json');
  const installedMetadata = fs.lstatSync(installedPath);
  if (!installedMetadata.isFile() || installedMetadata.size > 65536) return false;
  let installed;
  const read = fs.openSync(installedPath, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  try {
    const bytes = Buffer.alloc(65537);
    let length = 0;
    while (length < bytes.length) {
      const count = fs.readSync(read, bytes, length, bytes.length - length, null);
      if (count === 0) break;
      length += count;
    }
    if (length > 65536) return false;
    installed = JSON.parse(bytes.toString('utf8', 0, length));
  } finally {
    fs.closeSync(read);
  }
  if (installed.name !== name || installed.version !== version || !timely()) return false;
  const candidate = path.join(install, `.intent-manifest-${crypto.randomUUID()}.tmp`);
  let temporary;
  let output;
  try {
    output = fs.openSync(candidate, 'wx', 0o600);
    temporary = candidate;
    fs.writeFileSync(output, JSON.stringify({ private: true, dependencies: { [name]: version } }));
    fs.fsyncSync(output);
    fs.closeSync(output);
    output = undefined;
    if (![cache, npx, install].every(directory) || !timely()) return false;
    const current = fs.lstatSync(install, { bigint: true });
    if (identity.dev !== current.dev || identity.ino !== current.ino) return false;
    fs.linkSync(candidate, manifest);
    return true;
  } finally {
    if (output !== undefined) fs.closeSync(output);
    if (temporary) fs.unlinkSync(temporary);
  }
}

try {
  process.exitCode = repair() ? 0 : 1;
} catch {
  process.exitCode = 1;
}
