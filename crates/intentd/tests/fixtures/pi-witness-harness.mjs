// The CI harness owns its test children, including the real Pi CLI. Record the
// requested shutdown before killing a child; never label that status natural.
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync, readdirSync, statSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import diagnostics from './pi-diagnostics.cjs';
const { LIMITS, failureReport, safeText } = diagnostics;
export function readWitnesses(directory) {
  if (!existsSync(directory)) return [];
  const files = [...new Set(readdirSync(directory).filter(name => name.endsWith('.json') || name.endsWith('.json.observer-error'))
    .map(name => name.replace(/\.observer-error$/, '')))];
  assert.ok(files.length <= LIMITS.invocations, 'Too many fixture invocations');
  return files.map(name => {
    const file = join(directory, name);
    const errorFile = file + '.observer-error';
    let failure;
    if (existsSync(errorFile)) {
      assert.ok(statSync(errorFile).size <= LIMITS.witness, 'Oversized observer error');
      failure = JSON.parse(readFileSync(errorFile, 'utf8'));
    }
    if (!existsSync(file)) return { ...failure, argv: [], close: null };
    assert.ok(statSync(file).size <= LIMITS.witness, 'Oversized child evidence');
    const record = JSON.parse(readFileSync(file, 'utf8'));
    return failure ? { ...record, observerError: failure.observerError } : record;
  }).sort((a, b) => a.startedAt - b.startedAt);
}
function processState(pid) {
  if (!pid) return 'no pid';
  try { process.kill(pid, 0); return 'alive'; }
  catch (error) { return error.code === 'ESRCH' ? 'exited' : error.code ?? error.message; }
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
        assert.equal(run.status, 0, failureReport('Fixture child force-stop failed', {
          status: run.status, signal: run.signal, error: run.error?.message,
          childState: processState(record.pid), observerState: processState(record.observerPid),
          stdout: safeText(run.stdout ?? '', { limit: 1024 }),
          stderr: safeText(run.stderr ?? '', { limit: 1024 }),
        }, readWitnesses(directory).filter(child => child.invocationId === record.invocationId)));
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
