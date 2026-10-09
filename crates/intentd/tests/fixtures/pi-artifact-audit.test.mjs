import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { auditArtifact, auditDirectory, auditText } from './pi-artifact-audit.mjs';
import { readWitnesses, stopWitnesses } from './pi-witness-harness.mjs';

const fixture = name => fileURLToPath(new URL(name, import.meta.url));
const secret = 'CANARY_pi_fixture_secret_9387';
const env = Object.fromEntries(Object.entries(process.env).filter(([key]) =>
  ['path', 'systemroot', 'windir', 'comspec', 'pathext', 'temp', 'tmp'].includes(key.toLowerCase())));

test('independent auditor rejects stale, missing, oversized and unsafe evidence', () => {
  const root = mkdtempSync(join(tmpdir(), 'pi-artifact-audit-'));
  try {
    assert.throws(() => auditDirectory(root, 'current'), /Missing startup-new/);
    assert.throws(() => auditArtifact({ diagnosticSchema: 1, evidenceRun: 'old' }, { run: 'current' }), /Stale diagnostic run/);
    assert.throws(() => auditText(secret), /Unredacted/);
    assert.throws(() => auditText('\x1b[31mred'), /Terminal control/);
    assert.throws(() => auditText('\\u001bred'), /Escaped terminal/);
    for (const name of ['startup-new', 'startup-new-large', 'startup-load', 'startup-load-large', 'runtime-unix']) {
      writeFileSync(join(root, name + '.json'), ' '.repeat(262145));
    }
    assert.throws(() => auditDirectory(root, 'current', 'linux'), /Oversized/);
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('TAP runner sanitizes unexpected failing assertions and caps output on disk and console', () => {
  const root = mkdtempSync(join(tmpdir(), 'pi-tap-bound-'));
  try {
    const driver = join(root, 'failure.test.mjs');
    writeFileSync(driver, `import test from 'node:test';\nimport {writeSync} from 'node:fs';\nconst secret = ${JSON.stringify(secret)};\ntest('unexpected error', () => { writeSync(2, 'to\\x1b[31mken=' + secret + '\\n'); throw new Error('Authorization: Bearer ' + secret); });\n`);
    const run = spawnSync(process.execPath, [fixture('./pi-fixture-runner.mjs'), 'failed.tap', driver], {
      env: { ...env, PI_ACP_EVIDENCE_DIR: root, PI_DIAGNOSTICS_RUN_ID: 'runner-proof' }, encoding: 'utf8', timeout: 15000,
    });
    assert.equal(run.status, 1);
    const output = readFileSync(join(root, 'failed.tap'), 'utf8');
    auditText(output);
    auditText(run.stdout);
    assert.ok(Buffer.byteLength(output) <= 131072);
    assert.match(output, /not ok/);
    writeFileSync(driver, `import test from 'node:test';\nimport {writeSync} from 'node:fs';\ntest('large output', () => { writeSync(2, 'original failure\\n' + 'é'.repeat(300000) + 'token=' + ${JSON.stringify(secret)}); });\n`);
    const large = spawnSync(process.execPath, [fixture('./pi-fixture-runner.mjs'), 'large.tap', driver], {
      env: { ...env, PI_ACP_EVIDENCE_DIR: root, PI_DIAGNOSTICS_RUN_ID: 'runner-proof' }, encoding: 'utf8', timeout: 15000,
    });
    assert.equal(large.status, 0);
    const capped = readFileSync(join(root, 'large.tap'), 'utf8');
    assert.ok(Buffer.byteLength(capped) <= 131072);
    assert.ok(Buffer.byteLength(large.stdout) <= 131072);
    assert.match(capped, /"truncated":true/);
    auditText(capped);
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('harness reaps a known child after its launcher has already exited', async () => {
  const root = mkdtempSync(join(tmpdir(), 'pi-orphan-fixture-'));
  const directory = join(root, 'witness');
  mkdirSync(directory);
  writeFileSync(join(directory, 'route'), 'orphan-control');
  const childEntry = join(root, 'child.cjs');
  writeFileSync(childEntry, "process.stderr.write('child alive\\n'); setInterval(() => {}, 1000);\n");
  let records = [];
  try {
    const launcher = join(root, 'launcher.cjs');
    writeFileSync(launcher, `const {spawn}=require('node:child_process'); const child=spawn(process.execPath,[${JSON.stringify(fixture('./pi-startup-witness.cjs'))}],{stdio:'ignore'}); child.unref(); process.exit(42);`);
    const run = spawnSync(process.execPath, [launcher], { env: { ...env, PI_TEST_CHILD_ENTRY: childEntry,
      PI_TEST_WITNESS_DIR: directory, PI_TEST_RUN_ID: 'orphan-control' }, timeout: 10000 });
    assert.equal(run.status, 42);
    const deadline = Date.now() + 5000;
    while (!(records = readWitnesses(directory)).length && Date.now() < deadline) await delay(20);
    assert.equal(records.length, 1);
    await stopWitnesses(directory);
    const [record] = readWitnesses(directory);
    assert.equal(record.close.kind, 'harness');
    assert.throws(() => process.kill(record.pid, 0), { code: 'ESRCH' });
  } finally {
    for (const record of records) {
      for (const pid of [record.pid, record.observerPid]) {
        try { process.kill(pid, 'SIGKILL'); } catch (error) { if (error.code !== 'ESRCH') throw error; }
      }
    }
    rmSync(root, { recursive: true, force: true });
  }
  assert.equal(existsSync(root), false);
});

test('independent audit checks child proof and cleanup, not just the fixture result', () => {
  const child = { runId: 'case', invocationId: 'child', pid: 12345, route: 'session/new',
    close: { code: 73, signal: null, kind: 'natural' }, stderr: {
      text: 'PI_STARTUP_SENTINEL: deterministic child initialization failure\n',
      bytesSeen: 70, bytesRetained: 70, limitBytes: 8192, truncated: false, incompleteLineOmitted: false,
    } };
  const record = { diagnosticSchema: 1, evidenceRun: 'run', runId: 'case', root: join(tmpdir(), 'pi-audit-removed-root'),
    limits: { capture: 8192, artifact: 262144 }, artifactTruncated: false, route: 'new', oversized: false,
    cleanup: { completed: true, rootRemoved: true, steps: Object.fromEntries(['stopClients', 'closeLifetime', 'removeRoot'].map(name => [name, { result: 'passed' }])) },
    diagnostics: [child], response: { error: { code: -32603, message: 'Internal error', data: { details: 'Process exited' } } },
    failureReport: 'PI_STARTUP_SENTINEL "code":73',
  };
  const expected = { run: 'run', route: 'new', oversized: false };
  auditArtifact(record, expected);
  for (const mutate of [
    value => { value.diagnostics = []; },
    value => { value.diagnostics[0].runId = 'old'; },
    value => { value.diagnostics[0].close.kind = 'harness'; },
    value => { value.diagnostics[0].close = null; },
    value => { value.diagnostics[0].stderr.text = 'generic failure'; },
    value => { value.diagnostics[0].stderr.bytesSeen = 999999; },
    value => { value.failureReport = 'generic failure'; },
    value => { value.cleanup.steps.stopClients.result = 'failed'; },
    value => { value.response.error.message = 'PI_STARTUP_SENTINEL'; },
  ]) {
    const tampered = structuredClone(record);
    mutate(tampered);
    assert.throws(() => auditArtifact(tampered, expected));
  }
});

test('a failing stream beyond the detail cap fails both validators without banning optional truncation', async () => {
  const { default: diagnostics } = await import('./pi-diagnostics.cjs');
  const cleanup = await import('./pi-session-cleanup.mjs');
  const root = mkdtempSync(join(tmpdir(), 'pi-stream-summary-'));
  try {
    const record = { runId: 'case', root: join(root, 'already-removed'), result: 'passed',
      cleanup: { completed: true, rootRemoved: true, steps: Object.fromEntries(['stopClients', 'closeLifetime', 'removeRoot'].map(name => [name, { result: 'passed' }])) },
      replay: ['first:created', 'second:created'].map(text => ({ text })),
      clients: [{ witnesses: ['newSession', 'newSession', 'loadSession', 'loadSession'].map((route, index) => ({
        invocationId: String(index), runId: 'case', pid: 12345 + index, route, argv: ['--mode', 'rpc'],
        close: { code: null, signal: 'SIGTERM', kind: 'harness' },
        stderr: { text: '', bytesSeen: 0, bytesRetained: 0, limitBytes: 8192, truncated: false, incompleteLineOmitted: false },
      })) }],
      streamErrors: Array.from({ length: 128 }, () => ({ expected: true, message: 'retired socket' })),
      histories: Array(200).fill('optional history'),
    };
    const produce = () => diagnostics.publishEvidence(join(root, 'evidence.json'), record);
    let output = produce();
    const expected = { run: output.evidenceRun, runtime: true };
    auditArtifact(output, expected);
    assert.equal(output.artifactTruncated, true);
    record.streamErrors.push({ expected: false, message: 'unexpected 129th stream failure' });
    // Never trust a previously published summary when republishing full evidence.
    record.streamErrorSummary = { total: 0, unexpected: 0 };
    output = produce();
    assert.throws(() => auditArtifact(output, expected), /Unexpected MCP stream error/);
    assert.throws(() => cleanup.assertNoUnexpectedStreamErrors(output), /Unexpected MCP stream error/);
    assert.deepEqual(output.streamErrorSummary, { total: 129, unexpected: 1 });
    record.streamErrors[128].expected = true;
    const clean = produce();
    assert.deepEqual(clean.streamErrorSummary, { total: 129, unexpected: 0 });
    cleanup.assertNoUnexpectedStreamErrors(clean);
    auditArtifact(clean, expected);
    for (const total of [1, 129]) {
      const contradictory = { ...clean, streamErrors: [{ expected: false, message: 'retained unexpected error' }],
        streamErrorSummary: { total, unexpected: 0 } };
      assert.throws(() => cleanup.assertNoUnexpectedStreamErrors(contradictory), /Inconsistent stream-error summary/);
      assert.throws(() => auditArtifact(contradictory, expected), /Inconsistent stream-error summary/);
    }
    for (const summary of [undefined, {}, { total: 0, unexpected: 0 }, { total: 129, unexpected: -1 }]) {
      const missingProof = { ...clean, streamErrorSummary: summary };
      assert.throws(() => cleanup.assertNoUnexpectedStreamErrors(missingProof));
      assert.throws(() => auditArtifact(missingProof, expected));
    }
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('actual failing TAP file and console redact supported credentials across control continuations', () => {
  const root = mkdtempSync(join(tmpdir(), 'pi-tap-whitespace-'));
  const value = 'SYNTHETIC_UNLISTED_CREDENTIAL_123456';
  try {
    const forms = ['\t', '\r', '\r\n', '\n# ', '\r\n# ', '\n# # ', '\x1b[31m\r', '\\u001b[31m\r', '\\t', '\\r', '\\r\\n', '\\r\\n# ', '\\n# # ', '\\u0009', '\\x0d'];
    const labels = ['Bearer', 'authorization:', 'token=', 'password:', 'api_key:', 'access-token=', 'refresh_key:', 'authkey=', 'secret:', 'credential='];
    const driver = join(root, 'failure.test.mjs');
    writeFileSync(driver, `import test from 'node:test'; import {writeSync} from 'node:fs';
      test('unexpected fixture failure', () => {
        for (const label of ${JSON.stringify(labels)}) for (const separator of ${JSON.stringify(forms)}) writeSync(2, label + separator + ${JSON.stringify(value)} + '\\n');
        throw new Error('Bearer\\t' + ${JSON.stringify(value)});
      });`);
    const run = spawnSync(process.execPath, [fixture('./pi-fixture-runner.mjs'), 'failed.tap', driver], {
      env: { ...env, PI_ACP_EVIDENCE_DIR: root, PI_DIAGNOSTICS_RUN_ID: 'whitespace-proof' }, encoding: 'utf8', timeout: 15000,
    });
    assert.equal(run.status, 1, 'The test failure must remain a failure');
    for (const text of [readFileSync(join(root, 'failed.tap'), 'utf8'), run.stdout, run.stderr]) {
      assert.ok(!text.includes(value), 'Credential leaked through the runner');
      assert.ok(Buffer.byteLength(text) <= 131072);
      auditText(text);
    }
    for (const label of labels) for (const separator of forms) assert.throws(() => auditText(label + separator + value), 'Audit accepts a credential');
  } finally { rmSync(root, { recursive: true, force: true }); }
});
