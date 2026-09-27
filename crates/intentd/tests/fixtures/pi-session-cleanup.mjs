import assert from 'node:assert/strict';
import { existsSync, rmSync } from 'node:fs';

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
      steps[name] = { result: 'failed', error: String(error.stack ?? error) };
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
