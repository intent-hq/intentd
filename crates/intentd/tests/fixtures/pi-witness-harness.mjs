// The CI harness owns its test children, including the real Pi CLI. Record the
// requested shutdown before killing a child; never label that status natural.
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync, readdirSync, statSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import diagnostics from './pi-diagnostics.cjs';
const { LIMITS } = diagnostics;
export function readWitnesses(directory) {
  if (!existsSync(directory)) return [];
  const files = readdirSync(directory).filter(name => name.endsWith('.json'));
  assert.ok(files.length <= LIMITS.invocations, 'Too many fixture invocations');
  return files.map(name => {
    const file = join(directory, name);
    assert.ok(statSync(file).size <= LIMITS.witness, 'Oversized child evidence');
    return JSON.parse(readFileSync(file, 'utf8'));
  }).sort((a, b) => a.startedAt - b.startedAt);
}
export async function stopWitnesses(directory) {
  const records = readWitnesses(directory);
  for (const record of records) {
    if (record.close || !record.pid) continue;
    const file = join(directory, record.invocationId + '.json');
    writeFileSync(file + '.stop', 'harness cleanup');
    try { process.kill(record.pid, 'SIGTERM'); } catch (error) { if (error.code !== 'ESRCH') throw error; }
    const deadline = Date.now() + 3000;
    while (Date.now() < deadline && !JSON.parse(readFileSync(file, 'utf8')).close) await delay(20);
    if (!JSON.parse(readFileSync(file, 'utf8')).close) {
      if (process.platform === 'win32') {
        const run = spawnSync('taskkill.exe', ['/PID', String(record.pid), '/T', '/F'], { timeout: 3000 });
        assert.equal(run.status, 0, 'Fixture child force-stop failed');
      } else {
        try { process.kill(record.pid, 'SIGKILL'); } catch (error) { if (error.code !== 'ESRCH') throw error; }
      }
      const forcedDeadline = Date.now() + 3000;
      while (Date.now() < forcedDeadline && !JSON.parse(readFileSync(file, 'utf8')).close) await delay(20);
    }
    assert.ok(JSON.parse(readFileSync(file, 'utf8')).close, 'Missing child close after harness cleanup');
  }
  return readWitnesses(directory);
}
