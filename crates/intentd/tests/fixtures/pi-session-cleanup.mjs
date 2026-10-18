import assert from 'node:assert/strict';
import { existsSync, rmSync } from 'node:fs';
import diagnostics from './pi-diagnostics.cjs';
const { failureReport } = diagnostics;

// Do every cleanup step even when a previous one fails. Never rely on an
// after-hook throw: node:test can hide it behind the original body failure.
export async function finishCleanup(evidence, { stopClients, closeLifetime, removeRoot }, publish) {
  const steps = {};
  for (const [name, action] of Object.entries({
    stopClients,
    closeLifetime,
    removeRoot: removeRoot ?? (() => rmSync(evidence.root, {
      recursive: true, force: true, maxRetries: 5, retryDelay: 100,
    })),
  })) {
    try {
      await action();
      steps[name] = { result: 'passed' };
    } catch (error) {
      steps[name] = { result: 'failed', error: failureReport(`cleanup ${name}`, error) };
    }
  }
  evidence.cleanup = {
    completed: true, steps, rootRemoved: !existsSync(evidence.root),
  };
  // Published outside the per-case root, only after deletion was attempted and
  // observed. Missing output (including a write error) is rejected by the audit.
  await publish(evidence);
}

export function assertCleanupComplete(evidence, { runId, root }) {
  assert.ok(evidence, 'Missing post-cleanup evidence');
  assert.equal(evidence.runId, runId, 'Stale cleanup evidence');
  assert.equal(evidence.root, root, 'Wrong cleanup root');
  assert.equal(evidence.cleanup?.completed, true, 'Missing completed cleanup record');
  for (const name of ['stopClients', 'closeLifetime', 'removeRoot']) {
    const step = evidence.cleanup.steps?.[name];
    assert.equal(step?.result, 'passed', `Cleanup ${name} failed or missing: ${step?.error ?? ''}`);
  }
  assert.equal(evidence.cleanup.rootRemoved, true, 'Cleanup did not remove the root');
  assert.equal(existsSync(root), false, 'Cleanup root still exists');
}

// Use the lossless summary computed before diagnostic arrays are capped.
export function assertNoUnexpectedStreamErrors(record) {
  const summary = record.streamErrorSummary;
  assert.ok(summary && Number.isSafeInteger(summary.total) && summary.total >= 0
    && Number.isSafeInteger(summary.unexpected) && summary.unexpected >= 0
    && summary.unexpected <= summary.total, 'Missing or invalid stream-error summary');
  assert.ok(summary.total >= (record.streamErrors?.length ?? 0), 'Inconsistent stream-error summary');
  assert.ok(summary.unexpected >= (record.streamErrors ?? []).filter(row => row?.expected === false).length
    && summary.total - summary.unexpected >= (record.streamErrors ?? []).filter(row => row?.expected === true).length,
    'Inconsistent stream-error summary');
  assert.equal(summary.unexpected, 0, 'Unexpected MCP stream error');
}

// Keep the Windows tree-kill outcome separate from descendant witness checks.
export async function assertWindowsTaskkillComplete(killed, child, waitForClose) {
  assert.ifError(killed.error);
  assert.ok(!killed.signal, `taskkill interrupted: ${killed.signal}`);
  if (killed.status === 128) {
    // The process can exit while spawnSync blocks delivery of Node's close
    // event. A missing PID alone is insufficient: observe this child's close
    // under the caller's deadline before accepting the race. Descendant
    // witnesses are still checked independently by the caller.
    await waitForClose();
    assert.ok(child.exitCode !== null || child.signalCode !== null, 'adapter exit not observed');
    return;
  }
  assert.equal(killed.status, 0, `taskkill: ${killed.stderr}`);
}
