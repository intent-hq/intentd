// Functional checks of the actual node:test failure/after-hook/audit path.
// No adapter installation, model credentials, or platform simulation required.
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { once } from 'node:events';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { assertWindowsTaskkillComplete } from './pi-session-cleanup.mjs';
import { assertWitnessesClosed, readWitnesses, stopWitnesses } from './pi-witness-harness.mjs';

const helper = new URL('./pi-session-cleanup.mjs', import.meta.url).href;
// In particular, do not inherit NODE_TEST_CONTEXT: a nested `node --test`
// otherwise suppresses its run as recursive, defeating this functional check.
const childEnv = Object.fromEntries(Object.entries(process.env).filter(([key]) =>
  ['path', 'systemroot', 'windir', 'comspec', 'pathext', 'temp', 'tmp'].includes(key.toLowerCase())));

test('cleanup waits for child publication when child work finishes before spawn returns', { timeout: 15_000 }, async () => {
  const root = mkdtempSync(join(tmpdir(), 'intent-pi-late-witness-'));
  const directory = join(root, 'witnesses');
  mkdirSync(directory);
  writeFileSync(join(directory, 'route'), 'session/load');
  const ready = join(root, 'child-ready');
  const release = join(root, 'cleanup-started');
  const childEntry = join(root, 'child.cjs');
  const preload = join(root, 'delay-spawn-return.cjs');
  writeFileSync(childEntry, `
    const { writeFileSync } = require('node:fs');
    setInterval(() => {}, 1000);
    writeFileSync(${JSON.stringify(ready)}, String(process.pid));
  `);
  // Hold the observer inside spawn until the harness has begun cleanup. This
  // reproduces child work winning the first-publication race without a sleep.
  writeFileSync(preload, `
    const cp = require('node:child_process');
    const { existsSync } = require('node:fs');
    const spawn = cp.spawn;
    cp.spawn = (...args) => {
      const child = spawn(...args);
      const deadline = Date.now() + 10_000;
      while (!existsSync(${JSON.stringify(release)})) {
        if (Date.now() >= deadline) throw new Error('Cleanup handshake timed out');
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
      }
      return child;
    };
  `);
  const observer = spawn(process.execPath, ['--require', preload,
    fileURLToPath(new URL('./pi-startup-witness.cjs', import.meta.url))], {
    env: { ...childEnv, NODE_OPTIONS: '', NODE_DISABLE_COMPILE_CACHE: '1',
      PI_TEST_WITNESS_DIR: directory, PI_TEST_CHILD_ENTRY: childEntry, PI_TEST_RUN_ID: 'late-child' },
    stdio: 'ignore', detached: process.platform !== 'win32',
  });
  const closed = once(observer, 'close');
  closed.catch(() => {});
  let childPid;
  try {
    const deadline = Date.now() + 5000;
    while (!existsSync(ready) && Date.now() < deadline) await delay(10);
    assert.ok(existsSync(ready), 'Actual child must run before observer publication resumes');
    childPid = Number(readFileSync(ready, 'utf8'));
    const stopping = stopWitnesses(directory);
    writeFileSync(release, 'cleanup started');
    const records = await stopping;
    assert.equal(records.length, 1, 'Cleanup must not omit the late child');
    assert.equal(records[0].pid, childPid);
    assert.ok(records[0].close, 'Cleanup must retain actual child close');
    assert.equal(records[0].close.kind, 'harness');
    assert.deepEqual(readWitnesses(directory), records);
  } finally {
    writeFileSync(release, 'release on failure');
    if (!childPid && existsSync(ready)) childPid = Number(readFileSync(ready, 'utf8'));
    if (childPid) {
      try { process.kill(childPid, 'SIGKILL'); } catch (error) { if (error.code !== 'ESRCH') throw error; }
    }
    const timer = setTimeout(() => {
      if (process.platform === 'win32') {
        spawnSync('taskkill.exe', ['/PID', String(observer.pid), '/T', '/F'], { timeout: 3000, stdio: 'ignore' });
      } else {
        try { process.kill(-observer.pid, 'SIGKILL'); } catch (error) { if (error.code !== 'ESRCH') throw error; }
      }
    }, 2000);
    try { await closed; } finally { clearTimeout(timer); rmSync(root, { recursive: true, force: true }); }
  }
});

test('cleanup rejects an observer publication error instead of skipping its missing child pid', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'intent-pi-observer-error-'));
  try {
    writeFileSync(join(directory, 'failed.json.observer-error'), JSON.stringify({
      invocationId: 'failed', observerPid: process.pid, pid: null,
      observerError: { text: 'EIO: publication denied' },
    }));
    await assert.rejects(stopWitnesses(directory), /publication denied/);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test('cleanup bounds the wait for a registered observer that never publishes its child', { timeout: 10_000 }, async () => {
  const directory = mkdtempSync(join(tmpdir(), 'intent-pi-missing-pid-'));
  try {
    writeFileSync(join(directory, 'stalled.json'), JSON.stringify({
      invocationId: 'stalled', observerPid: process.pid, pid: null, close: null,
    }));
    await assert.rejects(stopWitnesses(directory), /Missing child pid after observer registration/);
    assert.equal(readWitnesses(directory)[0].close, null, 'Timeout must not synthesize child close');
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test('final witness validation rejects a record appearing during adapter teardown', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'intent-pi-final-close-'));
  try {
    assert.deepEqual(await stopWitnesses(directory), []);
    const file = join(directory, 'late.json');
    const record = { invocationId: 'late', pid: 123, close: null };
    writeFileSync(file, JSON.stringify(record));
    assert.throws(() => assertWitnessesClosed(readWitnesses(directory)), /Missing child close/);
    record.close = { code: 73, signal: null, kind: 'natural' };
    writeFileSync(file, JSON.stringify(record));
    assert.deepEqual(assertWitnessesClosed(readWitnesses(directory)), [record]);
    writeFileSync(file + '.observer-error', JSON.stringify({ observerError: { text: 'EIO: final write failed' } }));
    assert.throws(() => assertWitnessesClosed(readWitnesses(directory)), /final write failed/);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});
for (const bodyFails of [false, true]) {
  for (const failure of ['none', 'stopClients', 'removeRoot', 'missing', 'stale']) {
    test(`cleanup audit: body ${bodyFails ? 'fails' : 'passes'}, cleanup ${failure}`, () => {
      const dir = mkdtempSync(join(tmpdir(), 'intent-pi-cleanup-check-'));
      try {
        const driver = join(dir, 'driver.test.mjs');
        const output = join(dir, 'evidence.json');
        // Output is outside the disposable resource root, just like native CI.
        writeFileSync(driver, `
          import test from 'node:test';
          import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
          import { finishCleanup, assertCleanupComplete } from ${JSON.stringify(helper)};
          const root = ${JSON.stringify(join(dir, 'resource'))};
          const output = ${JSON.stringify(output)};
          const expected = { runId: 'current-run', root };
          const failure = ${JSON.stringify(failure)};
          const evidence = { ...expected, result: ${JSON.stringify(bodyFails ? 'failed' : 'passed')} };
          mkdirSync(root);
          const body = test('original body', t => {
            t.after(() => finishCleanup(evidence, {
              stopClients() { if (failure === 'stopClients') throw new Error('STOP_FAILURE_SENTINEL'); },
              closeLifetime() {},
              ...(failure === 'removeRoot' ? { removeRoot() { throw new Error('REMOVE_FAILURE_SENTINEL'); } } : {}),
            }, record => {
              if (failure === 'missing') return;
              if (failure === 'stale') record.runId = 'previous-run';
              writeFileSync(output, JSON.stringify(record));
            }));
            if (${bodyFails}) throw new Error('BODY_FAILURE_SENTINEL');
          });
          test('independent cleanup audit', async () => {
            await body;
            assertCleanupComplete(JSON.parse(readFileSync(output, 'utf8')), expected);
          });
        `);
        const run = spawnSync(process.execPath, ['--test', '--test-reporter=tap', driver], {
          encoding: 'utf8', timeout: 10_000,
          env: { ...childEnv, NODE_OPTIONS: '', NODE_DISABLE_COMPILE_CACHE: '1' },
        });
        assert.ifError(run.error);
        assert.equal(run.signal, null);
        assert.equal(run.status, bodyFails || failure !== 'none' ? 1 : 0, run.stdout + run.stderr);
        assert.match(run.stdout, new RegExp(`^${bodyFails ? 'not ok' : 'ok'} 1 - original body$`, 'm'));
        assert.match(run.stdout, new RegExp(`^${failure === 'none' ? 'ok' : 'not ok'} 2 - independent cleanup audit$`, 'm'));
        if (bodyFails) assert.match(run.stdout, /BODY_FAILURE_SENTINEL/);
        if (failure === 'stopClients') assert.match(run.stdout, /STOP_FAILURE_SENTINEL/);
        if (failure === 'removeRoot') assert.match(run.stdout, /REMOVE_FAILURE_SENTINEL/);
        if (failure === 'stale') assert.match(run.stdout, /Stale cleanup evidence/);
        if (failure !== 'missing') {
          const evidence = JSON.parse(readFileSync(output, 'utf8'));
          assert.equal(evidence.cleanup.completed, true);
          assert.equal(evidence.cleanup.rootRemoved, failure !== 'removeRoot');
        }
        assert.equal(existsSync(join(dir, 'resource')), failure === 'removeRoot');
      } finally {
        rmSync(dir, { recursive: true, force: true });
      }
    });
  }
}

test('Windows taskkill missing-process race waits for this child close event', async () => {
  const child = { exitCode: null, signalCode: null };
  let release;
  const closed = new Promise(resolve => { release = resolve; });
  let settled = false;
  const result = assertWindowsTaskkillComplete(
    { status: 128, signal: null, stderr: 'process not found' }, child, () => closed,
  ).finally(() => { settled = true; });
  result.catch(() => {});
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(settled, false, 'exit128 alone must not prove cleanup');
  child.exitCode = 0;
  release();
  await result;
});

test('Windows taskkill still rejects kill errors and unproven closure', async () => {
  for (const killed of [
    { status: 1, stderr: 'access denied' },
    { status: null, error: new Error('spawn failed') },
    { status: 128, error: new Error('timed out'), stderr: 'process not found' },
    { status: 128, signal: 'SIGTERM', stderr: 'process not found' },
  ]) {
    await assert.rejects(assertWindowsTaskkillComplete(killed, { exitCode: 0, signalCode: null }, async () => {}));
  }
  await assert.rejects(assertWindowsTaskkillComplete(
    { status: 128 }, { exitCode: null, signalCode: null }, async () => { throw new Error('close deadline'); },
  ), /close deadline/);
  await assert.rejects(assertWindowsTaskkillComplete(
    { status: 128 }, { exitCode: null, signalCode: null }, async () => {},
  ), /exit not observed/);
});

test('Windows successful taskkill preserves the caller close and witness checks', async () => {
  await assertWindowsTaskkillComplete({ status: 0 }, { exitCode: null, signalCode: null },
    () => assert.fail('normal success retains caller close handling'));
});
