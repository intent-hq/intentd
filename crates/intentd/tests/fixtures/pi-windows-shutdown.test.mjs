import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import test from 'node:test';
import { stopWindowsProcessTree } from './pi-windows-shutdown.mjs';

test('process-not-found still waits for the actual child close event', async () => {
  const child = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore' });
  const closed = once(child, 'close');
  let killed = false;
  try {
    await stopWindowsProcessTree(child, closed, { kill() {
      assert.equal(child.exitCode, null);
      child.kill();
      killed = true;
      return { status: 128, stderr: 'process not found' };
    } });
    assert.ok(killed);
    assert.ok(child.exitCode !== null || child.signalCode !== null);
  } finally { child.kill(); await closed; }
});

test('process-not-found cannot hide a missing close event', async () => {
  await assert.rejects(stopWindowsProcessTree({ pid: 42, exitCode: null, signalCode: null },
    new Promise(() => {}), { timeoutMs: 10, kill: () => ({ status: 128 }) }), /shutdown timed out/);
});

test('successful taskkill also requires observed close', async () => {
  await assert.rejects(stopWindowsProcessTree({ pid: 42, exitCode: null, signalCode: null },
    new Promise(() => {}), { timeoutMs: 10, kill: () => ({ status: 0 }) }), /shutdown timed out/);
});

test('permission and execution failures remain visible', async () => {
  for (const status of [1, 5, null]) {
    await assert.rejects(stopWindowsProcessTree({ pid: 42, exitCode: null, signalCode: null },
      Promise.resolve(), { kill: () => ({ status, stderr: 'denied' }) }), /taskkill: denied/);
  }
});

test('an already exited child does not invoke taskkill', async () => {
  await stopWindowsProcessTree({ exitCode: 0, signalCode: null }, Promise.resolve(), {
    kill() { assert.fail('already exited'); },
  });
});
