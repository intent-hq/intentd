#!/usr/bin/env node
// Smoke-tests a local intent-node image: start it with its default command,
// check intentd, the promised tools and the gh version, check no intentd runs,
// then remove the container.
// Usage: node smoke-image.mjs <image-ref> <intentd-version>
import { execFileSync } from 'node:child_process';
import { parseVersion, smokeChecks, verifyImageSmoke } from './intent-node.mjs';

function docker(...args) {
  return execFileSync('docker', args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'inherit'] });
}

function run(container, command) {
  console.log(`$ ${command}`);
  const stdout = docker('exec', container, 'sh', '-c', command);
  process.stdout.write(stdout);
  return stdout;
}

function main() {
  const [ref, rawVersion] = process.argv.slice(2);
  if (!ref || !rawVersion) throw new Error('usage: smoke-image.mjs <image-ref> <intentd-version>');
  const version = parseVersion(rawVersion);
  const checks = smokeChecks();
  const container = docker('run', '--detach', '--rm', '--platform', 'linux/amd64', ref).trim();
  console.log(`Started container ${container.slice(0, 12)} from ${ref}`);
  try {
    const outputs = {
      version: run(container, checks.version),
      idle: run(container, checks.idle),
      gh: run(container, checks.gh),
    };
    run(container, `set -e; for tool in ${checks.tools.join(' ')}; do printf '%s: ' "$tool"; command -v "$tool"; done`);
    run(container, 'git --version; git lfs version; rg --version | head -1; node --version; pnpm --version; python3 --version; claude --version; codex --version');
    const problems = verifyImageSmoke(version, outputs);
    if (problems.length) throw new Error(problems.join('\n'));
    console.log(`Smoke test passed for ${ref} (intentd ${version})`);
  } finally {
    docker('rm', '--force', container);
    console.log(`Removed container ${container.slice(0, 12)}`);
  }
}

try {
  main();
} catch (error) {
  console.error(`::error::${error?.stack ?? error}`);
  process.exit(1);
}
