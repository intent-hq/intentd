import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';

export async function stopWindowsProcessTree(child, closed, { timeoutMs = 10_000, kill = spawnSync } = {}) {
  if (child.exitCode === null && child.signalCode === null) {
    const killed = kill('taskkill.exe', ['/PID', String(child.pid), '/T', '/F'], { timeout: timeoutMs });
    // Windows can observe a natural exit before Node delivers 'close'. A 128
    // result is acceptable only if the bounded close observation below succeeds.
    assert.ok(killed.status === 0 || killed.status === 128, `taskkill: ${killed.stderr}`);
  }
  let timer;
  try {
    await Promise.race([closed, new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error('Windows adapter shutdown timed out')), timeoutMs);
    })]);
  } finally { clearTimeout(timer); }
}
