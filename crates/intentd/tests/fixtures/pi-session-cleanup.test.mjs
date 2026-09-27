// Functional checks of the actual node:test failure/after-hook/audit path.
// No adapter installation, model credentials, or platform simulation required.
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

const helper = new URL('./pi-session-cleanup.mjs', import.meta.url).href;
// In particular, do not inherit NODE_TEST_CONTEXT: a nested `node --test`
// otherwise suppresses its run as recursive, defeating this functional check.
const childEnv = Object.fromEntries(Object.entries(process.env).filter(([key]) =>
  ['path', 'systemroot', 'windir', 'comspec', 'pathext', 'temp', 'tmp'].includes(key.toLowerCase())));
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
