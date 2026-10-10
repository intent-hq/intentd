// Keep even an unexpected node:test assertion/SDK error out of raw CI/TAP logs.
import { spawn, spawnSync } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import diagnostics from './pi-diagnostics.cjs';
const { Capture, LIMITS } = diagnostics;
const [name, ...files] = process.argv.slice(2);
if (!/^[a-z-]+\.tap$/.test(name ?? '') || !files.length || !process.env.PI_ACP_EVIDENCE_DIR) {
  throw new Error('Usage: PI_ACP_EVIDENCE_DIR=... node pi-fixture-runner.mjs name.tap test-files...');
}
const directory = process.env.PI_ACP_EVIDENCE_DIR;
mkdirSync(directory, { recursive: true });
const env = { ...process.env, NODE_OPTIONS: '' };
delete env.NODE_TEST_CONTEXT;
const child = spawn(process.execPath, ['--test', '--test-reporter=tap', ...files], {
  env, stdio: ['ignore', 'pipe', 'pipe'], detached: process.platform !== 'win32',
});
const capture = new Capture({ limit: LIMITS.tap - 1024 });
child.stdout.on('data', chunk => capture.push(chunk));
child.stderr.on('data', chunk => capture.push(chunk));
child.on('error', () => capture.push('Fixture runner could not spawn node\n'));
let timedOut = false;
const timer = setTimeout(() => {
  timedOut = true;
  if (process.platform === 'win32') spawnSync('taskkill.exe', ['/PID', String(child.pid), '/T', '/F'], { timeout: 5000, stdio: 'ignore' });
  else { try { process.kill(-child.pid, 'SIGKILL'); } catch (error) { if (error.code !== 'ESRCH') throw error; } }
}, 240_000);
child.on('close', (code, signal) => {
  clearTimeout(timer);
  const result = capture.snapshot(true);
  const metadata = { ...result, text: undefined, code, signal, timedOut,
    evidenceRun: process.env.PI_DIAGNOSTICS_RUN_ID ?? 'local' };
  const text = result.text + '\n# diagnostic capture ' + JSON.stringify(metadata) + '\n';
  writeFileSync(join(directory, name), text);
  process.stdout.write(text);
  process.exitCode = code === 0 && !timedOut ? 0 : 1;
});
