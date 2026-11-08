#!/usr/bin/env node
// Smoke-tests one intent-node build: create a sandbox, check intentd, the
// promised tools, the gh version and the size, check no intentd runs yet, then kill it.
// Usage: E2B_API_KEY=... node smoke.mjs <template-ref> <intentd-version> <size>
import { Sandbox } from 'e2b';
import { parseSize, parseVersion, redact, smokeChecks, verifySmoke } from './intent-node.mjs';

async function run(sandbox, command) {
  console.log(`$ ${command}`);
  const result = await sandbox.commands.run(command, { timeoutMs: 60_000 });
  process.stdout.write(result.stdout);
  if (result.stderr) process.stdout.write(result.stderr);
  return result.stdout;
}

async function main() {
  const [ref, rawVersion, sizeId] = process.argv.slice(2);
  if (!ref || !rawVersion || !sizeId) throw new Error('usage: smoke.mjs <template-ref> <intentd-version> <size>');
  const version = parseVersion(rawVersion);
  parseSize(sizeId);
  const apiKey = process.env.E2B_API_KEY;
  if (!apiKey) throw new Error('E2B_API_KEY is not set');
  const checks = smokeChecks();
  const sandbox = await Sandbox.create(ref, { apiKey, timeoutMs: 300_000 });
  console.log(`Created sandbox ${sandbox.sandboxId} from ${ref}`);
  try {
    const outputs = {
      version: await run(sandbox, checks.version),
      idle: await run(sandbox, checks.idle),
      cpus: await run(sandbox, checks.cpus),
      memKb: await run(sandbox, checks.memKb),
      gh: await run(sandbox, checks.gh),
    };
    await run(sandbox, `set -e; for tool in ${checks.tools.join(' ')}; do printf '%s: ' "$tool"; command -v "$tool"; done`);
    await run(sandbox, 'git --version; git lfs version; rg --version | head -1; node --version; pnpm --version; python3 --version; claude --version; codex --version');
    const problems = verifySmoke(version, sizeId, outputs);
    if (problems.length) throw new Error(problems.join('\n'));
    console.log(`Smoke test passed for ${ref} (intentd ${version}, ${sizeId})`);
  } finally {
    await sandbox.kill();
    console.log(`Killed sandbox ${sandbox.sandboxId}`);
  }
}

main().catch(error => {
  console.error(`::error::${redact(error?.stack ?? error, process.env.E2B_API_KEY)}`);
  process.exit(1);
});
