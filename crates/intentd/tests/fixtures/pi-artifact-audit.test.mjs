import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { auditArtifact, auditDirectory, auditText } from './pi-artifact-audit.mjs';
import { readWitnesses, stopWitnesses } from './pi-witness-harness.mjs';
import diagnostics from './pi-diagnostics.cjs';

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

test('harness reaps a known child after its launcher has already exited', async t => {
  const value = 'SYNTHETIC_ORPHAN_SETUP_CREDENTIAL_1234';
  for (const scenario of ['ready', 'child failure', 'observer failure', 'readiness timeout', 'observer close failure']) await t.test(scenario, async () => {
    const root = mkdtempSync(join(tmpdir(), 'pi-orphan-fixture-'));
    const directory = join(root, 'witness');
    mkdirSync(directory);
    writeFileSync(join(directory, 'route'), 'orphan-control');
    const childEntry = join(root, 'child.cjs');
    writeFileSync(childEntry, scenario === 'child failure'
      ? `process.stderr.write('token=${value}\\n'); process.exitCode = 17;`
      : `setTimeout(() => process.stderr.write(${JSON.stringify(['ready', 'observer close failure'].includes(scenario) ? 'child alive\n' : 'child not ready\n')}), 200); setInterval(() => {}, 1000);`);
    let observerEntry = fixture('./pi-startup-witness.cjs');
    if (scenario === 'observer failure') {
      observerEntry = join(root, 'broken-observer.cjs');
      writeFileSync(observerEntry, `require('node:fs').writeSync(2, 'observer setup failed\\ntoken=${value}\\n' + 'é'.repeat(100000)); process.exitCode = 17;`);
    }
    if (scenario === 'observer close failure') {
      observerEntry = join(root, 'close-failure-observer.cjs');
      // Fail the real witness publication after a live child exits. This leaves
      // missing close evidence even though cleanup must still reap every PID.
      writeFileSync(observerEntry, `const fs = require('node:fs');
        const rename = fs.renameSync;
        fs.renameSync = (from, to) => {
          const record = JSON.parse(fs.readFileSync(from, 'utf8'));
          if (record.exit || record.close) {
            fs.writeSync(2, 'observer close publication failed\\ntoken=${value}\\n');
            throw new Error('injected observer close publication failure');
          }
          return rename(from, to);
        };
        require(${JSON.stringify(fixture('./pi-startup-witness.cjs'))});`);
    }
    const launcherEntry = join(root, 'launcher.cjs');
    // Keep the launcher alive until the test observes a live child. Detaching is
    // required for the observer to survive its launcher on Windows; stderr is
    // owned and bounded by this test, not discarded or written to a raw file.
    writeFileSync(launcherEntry, `const {spawn}=require('node:child_process');
      const observer=spawn(process.execPath,[${JSON.stringify(observerEntry)}],{detached:true,stdio:['ignore','ignore',2]});
      process.send({observerPid:observer.pid});
      observer.on('error',error=>{process.stderr.write(error.message);process.send({observerFailed:true});});
      observer.on('exit',(code,signal)=>process.send({observerExit:{code,signal}}));
      process.on('message',message=>{if(message==='release'){observer.unref();process.disconnect();process.exit(42);}});`);
    const capture = new diagnostics.Capture({ limit: 2048 });
    const launcher = spawn(process.execPath, [launcherEntry], { env: { ...env, DD_TRACE_ENABLED: 'false', DD_TRACE_STARTUP_LOGS: 'false', PI_TEST_CHILD_ENTRY: childEntry,
      PI_TEST_WITNESS_DIR: directory, PI_TEST_RUN_ID: 'orphan-control' }, stdio: ['ignore', 'ignore', 'pipe', 'ipc'] });
    let observerPid, observerExit, launcherExit, launchError;
    let records = [];
    let primaryError;
    launcher.stderr.on('data', chunk => capture.push(chunk));
    launcher.on('error', error => { launchError = error; });
    launcher.on('exit', (code, signal) => { launcherExit = { code, signal }; });
    launcher.on('message', message => {
      observerPid ??= message.observerPid;
      observerExit ??= message.observerExit ?? (message.observerFailed ? { failed: true } : undefined);
    });
    const alive = pid => {
      if (!pid) return false;
      try { process.kill(pid, 0); return true; } catch (error) { if (error.code === 'ESRCH') return false; throw error; }
    };
    try {
      const deadline = Date.now() + 5000;
      let ready = false;
      while (Date.now() < deadline) {
        records = readWitnesses(directory);
        if (launchError || launcherExit || observerExit || records.some(record => record.close)) break;
        ready = records.length === 1 && records[0].stderr.text.includes('child alive\n')
          && alive(records[0].pid) && alive(observerPid);
        if (ready) break;
        await delay(20);
      }
      const setupReport = diagnostics.failureReport('orphan fixture readiness', {
        message: ready ? 'ready' : 'Child did not become ready', data: {
          launchError: launchError?.message, launcherExit, observerExit, observerPid, stderr: capture.snapshot(true),
        },
      }, records);
      auditText(setupReport);
      assert.ok(!setupReport.includes(value));
      assert.ok(Buffer.byteLength(setupReport) <= diagnostics.LIMITS.report);
      t.diagnostic(setupReport);
      assert.equal(ready, ['ready', 'observer close failure'].includes(scenario), setupReport);
      if (ready) {
        // Exit the launcher only AFTER readiness, then prove the orphan is still
        // live before asking the harness to clean it up.
        launcher.send('release');
        const exitDeadline = Date.now() + 3000;
        while (!launcherExit && Date.now() < exitDeadline) await delay(20);
        assert.deepEqual(launcherExit, { code: 42, signal: null }, setupReport);
        assert.ok(alive(observerPid) && alive(records[0].pid), 'Ready child did not survive launcher exit');
        await stopWitnesses(directory);
        const [record] = readWitnesses(directory);
        assert.equal(record.close.kind, 'harness');
        assert.equal(alive(record.pid), false);
      } else {
        if (scenario === 'observer failure') {
          assert.match(setupReport, /observer setup failed/);
          assert.equal(capture.snapshot(true).truncated, true);
        } else {
          assert.equal(records.length, 1, setupReport);
          if (scenario === 'child failure') assert.equal(records[0].close?.code, 17, setupReport);
          else assert.ok(!records[0].close && alive(records[0].pid), setupReport);
        }
      }
    } catch (error) {
      primaryError = error;
    } finally {
      // Retain PID ownership even if setup fails before any witness is written.
      // Refresh the records so a child created during a failed setup is reaped.
      const failures = [];
      try { await stopWitnesses(directory); } catch (error) { failures.push(error); }
      records = readWitnesses(directory);
      const pids = [launcher.pid, observerPid, ...records.flatMap(record => [record.pid, record.observerPid])].filter(Boolean);
      // The observer owns an isolated fixture group: cover a child that was
      // spawned just before a failed setup, before it could publish its PID.
      if (observerPid) {
        if (process.platform === 'win32') {
          if (alive(observerPid)) {
            const killed = spawnSync('taskkill.exe', ['/PID', String(observerPid), '/T', '/F'], { timeout: 3000, stdio: 'ignore' });
            if (killed.status !== 0 && alive(observerPid)) failures.push(new Error('Observer tree cleanup failed'));
          }
        } else {
          try { process.kill(-observerPid, 'SIGKILL'); } catch (error) { if (error.code !== 'ESRCH') failures.push(error); }
        }
      }
      for (const pid of new Set(pids)) {
        try { process.kill(pid, 'SIGKILL'); } catch (error) { if (error.code !== 'ESRCH') failures.push(error); }
      }
      const cleanupDeadline = Date.now() + 3000;
      while (pids.some(alive) && Date.now() < cleanupDeadline) await delay(20);
      if (pids.some(alive)) failures.push(new Error('Fixture process survived cleanup'));
      const observerStderr = capture.snapshot(true);
      const survivingPids = pids.filter(alive);
      launcher.stderr.destroy();
      if (launcher.connected) launcher.disconnect();
      rmSync(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
      // Preserve the first failure even when cleanup also fails. The observer
      // can emit its useful error only after the readiness snapshot above.
      const describe = error => error && ({ name: error.name, message: error.message,
        code: error.code, data: error.data });
      const report = diagnostics.failureReport('orphan fixture cleanup', {
        primaryError: describe(primaryError), observerStderr,
        cleanupErrors: failures.map(describe), survivingPids,
      }, records);
      if (scenario === 'observer close failure') {
        assert.ok(primaryError, 'The missing close must fail the original cleanup');
        assert.ok(failures.length, 'Repeated cleanup must not waive missing close evidence');
        assert.match(report, /primaryError/);
        assert.match(report, /Missing child close|Fixture child force-stop failed/);
        assert.match(report, /cleanupErrors/);
        assert.match(report, /observer close publication failed/);
        assert.ok(!report.includes(value));
        auditText(report);
        assert.ok(Buffer.byteLength(report) <= diagnostics.LIMITS.report);
        assert.equal(pids.some(alive), false, report);
      } else {
        assert.ok(!primaryError && failures.length === 0, report);
      }
      assert.equal(existsSync(root), false);
    }
  });
});

test('observer publishes actual exit and close after transient witness rename denial', () => {
  const root = mkdtempSync(join(tmpdir(), 'pi-observer-rename-'));
  try {
    writeFileSync(join(root, 'route'), 'publication-recovery');
    const childEntry = join(root, 'child.cjs');
    writeFileSync(childEntry, 'process.stdout.write("unchanged output\\n"); process.exitCode = 73;');
    const preload = join(root, 'transient-publication.cjs');
    writeFileSync(preload, `const fs = require('node:fs');
      const original = fs.renameSync;
      let calls = 0;
      fs.renameSync = (...args) => {
        if (++calls === 2) throw Object.assign(new Error('witness rename denied'), { code: 'EPERM', syscall: 'rename' });
        return original(...args);
      };`);
    const run = spawnSync(process.execPath, ['--require', preload, fixture('./pi-startup-witness.cjs')], {
      env: { ...env, PI_TEST_CHILD_ENTRY: childEntry, PI_TEST_WITNESS_DIR: root, PI_TEST_RUN_ID: 'publication-recovery' },
      stdio: ['ignore', 'pipe', 'ignore'], encoding: 'utf8', timeout: 5000,
    });
    assert.equal(run.status, 73, 'Observer must preserve the actual child exit');
    assert.equal(run.stdout, 'unchanged output\n');
    const records = readWitnesses(root);
    assert.equal(records.length, 1, 'Publication recovery must not spawn another child');
    assert.deepEqual(records[0].exit, { code: 73, signal: null, kind: 'natural' });
    assert.deepEqual(records[0].close, records[0].exit);
    assert.equal(records[0].observerError, undefined);
    assert.equal(existsSync(join(root, records[0].invocationId + '.json.tmp')), false);
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('observer publication failures remain visible when the adapter discards stderr', () => {
  const root = mkdtempSync(join(tmpdir(), 'pi-observer-failure-'));
  try {
    writeFileSync(join(root, 'route'), 'publication-failure');
    const childEntry = join(root, 'child.cjs');
    writeFileSync(childEntry, 'process.stdout.write("unchanged child output\\n"); process.exitCode = 73;');
    const preload = join(root, 'fail-publication.cjs');
    writeFileSync(preload, `const fs = require('node:fs');
      const original = fs.renameSync;
      let calls = 0;
      fs.renameSync = (...args) => {
        if (++calls > 1) throw Object.assign(new Error('witness rename denied\\ntoken=${secret}\\n' + 'x'.repeat(100000)), { code: 'EPERM', syscall: 'rename' });
        return original(...args);
      };`);
    const run = spawnSync(process.execPath, ['--require', preload, fixture('./pi-startup-witness.cjs')], {
      env: { ...env, PI_TEST_CHILD_ENTRY: childEntry, PI_TEST_WITNESS_DIR: root, PI_TEST_RUN_ID: 'publication-failure' },
      stdio: ['ignore', 'pipe', 'ignore'], encoding: 'utf8', timeout: 5000,
    });
    assert.equal(run.status, 1, 'Observer must still fail on publication error');
    assert.equal(run.stdout, 'unchanged child output\n');
    const [record] = readWitnesses(root);
    assert.equal(record.close, null, 'Do not fabricate child close on observer failure');
    assert.match(record.observerError?.text ?? '', /EPERM.*rename/s);
    assert.ok(!JSON.stringify(record).includes(secret));
    assert.equal(record.observerError.truncated, true);
    assert.ok(Buffer.byteLength(record.observerError.text) <= 2048);
    auditText(JSON.stringify(record));
    assert.match(diagnostics.failureReport('fixture cleanup', new Error('missing close'), [record]), /EPERM/);
  } finally { rmSync(root, { recursive: true, force: true }); }
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
    value => { value.diagnostics[0].observerError = { text: 'publication failed' }; },
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

test('producer and independent auditor agree on supported credential syntax', async () => {
  const { default: diagnostics } = await import('./pi-diagnostics.cjs');
  const value = 'SYNTHETIC_CONTRACT_CREDENTIAL_2468';
  const labels = ['authorization', 'token', 'password', 'secret', 'credential',
    ...['access', 'refresh', 'auth', 'api'].flatMap(prefix => ['token', 'key'].flatMap(suffix => ['', '_', '-'].map(joiner => prefix + joiner + suffix)))];
  const cases = [];
  for (const label of labels) for (const quote of ['"', "'"]) for (const escapes of [0, 1, 3, 7]) {
    for (const separator of [' ', '\r\n# ', '\\r\\n# \\# ', '\\x0d']) {
      const escapedQuote = '\\'.repeat(escapes) + quote;
      cases.push({ raw: `${escapedQuote}${label}${escapedQuote}:${separator}${escapedQuote}${value}${escapedQuote}`, value });
    }
  }
  for (const separator of [' ', '\t', '\r\n# ', '\\r\\n# \\# ', '\\u0009']) {
    cases.push({ raw: `Bearer${separator}${value}`, value });
  }
  cases.push({ raw: `https://user:${value}@example.test`, value }, { raw: secret, value: secret });
  for (const row of cases) {
    assert.throws(() => auditText(row.raw), 'Auditor missed a supported credential');
    const bytes = Buffer.from('original é error\n' + row.raw + '\n');
    for (let split = 0; split <= bytes.length; split++) {
      const capture = new diagnostics.Capture();
      capture.push(bytes.subarray(0, split));
      assert.ok(!capture.snapshot().text.includes(row.value), 'Partial capture leaked');
      capture.push(bytes.subarray(split));
      const text = capture.snapshot(true).text;
      assert.ok(!text.includes(row.value), 'Producer missed a supported credential');
      auditText(text);
    }
    for (const limit of [64, 128]) {
      const text = diagnostics.safeText('original error\n' + row.raw.repeat(100), { limit }).text;
      assert.ok(!text.includes('SYNTHETIC_') && !text.includes('CANARY_'), 'Truncation leaked a credential prefix');
      assert.ok(Buffer.byteLength(text) <= limit);
      auditText(text);
    }
    auditText(diagnostics.failureReport('loadSession', new Error(row.raw)));
    auditText(JSON.stringify(diagnostics.safeValue({ message: row.raw })));
  }
  for (const path of ['C:\\temp\\runtime\\test.json', '\\\\server\\share\\token-files\\test.json']) {
    assert.equal(diagnostics.safeText(path).text, path);
    auditText(path);
  }
});

test('actual failing runner redacts nested JSON errors before TAP and console publication', () => {
  const root = mkdtempSync(join(tmpdir(), 'pi-tap-nested-error-'));
  const value = 'SYNTHETIC_NESTED_DIAGNOSTIC_VALUE_4567';
  try {
    const driver = join(root, 'nested.test.mjs');
    writeFileSync(driver, `import test from 'node:test'; import {writeSync} from 'node:fs';
      test('nested error', () => {
        for (const label of ['token', 'password', 'api_key', 'authorization']) {
          let message = JSON.stringify({details: JSON.stringify({[label]: ${JSON.stringify(value)}})});
          for (let depth = 0; depth < 5; depth++) { writeSync(2, message + '\\n'); message = JSON.stringify({details: message}); }
        }
        throw new Error(JSON.stringify({details: JSON.stringify({token: ${JSON.stringify(value)}})}));
      });`);
    const run = spawnSync(process.execPath, [fixture('./pi-fixture-runner.mjs'), 'nested.tap', driver], {
      env: { ...env, PI_ACP_EVIDENCE_DIR: root, PI_DIAGNOSTICS_RUN_ID: 'nested-proof' }, encoding: 'utf8', timeout: 15000,
    });
    assert.equal(run.status, 1, 'Intentional test failure must remain visible');
    for (const text of [readFileSync(join(root, 'nested.tap'), 'utf8'), run.stdout, run.stderr]) {
      assert.ok(!text.includes(value), 'Nested error leaked before publication');
      assert.ok(Buffer.byteLength(text) <= 131072);
      auditText(text);
    }
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('Windows zero exits require completed-session replacement proof without rewriting child status', () => {
  const root = mkdtempSync(join(tmpdir(), 'pi-retirement-proof-'));
  try {
    const record = { runId: 'replacement', platform: 'win32', root: join(root, 'removed'), result: 'passed',
      cleanup: { completed: true, rootRemoved: true, steps: Object.fromEntries(['stopClients', 'closeLifetime', 'removeRoot'].map(name => [name, { result: 'passed' }])) },
      streamErrors: [], replay: ['first:created', 'second:created'].map(text => ({ text })),
      diagnostics: ['newSession', 'newSession', 'loadSession', 'loadSession'].map((route, index) => ({
        invocationId: String(index), runId: 'replacement', pid: 12340 + index, startedAt: index + 1, route, argv: ['--mode', 'rpc'],
        close: { code: index % 2 ? 1 : 0, signal: null, kind: index % 2 ? 'harness' : 'natural' },
        stderr: { text: '', bytesSeen: 0, bytesRetained: 0, limitBytes: 8192, truncated: false, incompleteLineOmitted: false },
      })),
      rpcLifecycle: ['newSession', 'newSession', 'loadSession', 'loadSession'].map((method, index) => ({
        invocationId: String(index), method, sessionId: `session-${index % 2}`, callCompleted: true, promptStopReason: 'end_turn',
        ...(index % 2 ? {} : { replacement: { invocationId: String(index + 1), priorLive: true } }),
      })),
      histories: Array(200).fill('optional history'),
    };
    const output = diagnostics.publishEvidence(join(root, 'record.json'), record);
    const expected = { run: output.evidenceRun, runtime: true };
    auditArtifact(output, expected);
    assert.equal(output.artifactTruncated, true);
    assert.deepEqual(output.rpcLifecycle, record.rpcLifecycle, 'Mandatory replacement proof was truncated');
    assert.deepEqual(output.diagnostics[0].close, { code: 0, signal: null, kind: 'natural' });
    for (const index of [0, 2]) for (const change of [
      value => { value.rpcLifecycle = undefined; },
      value => { value.platform = 'linux'; },
      value => { value.diagnostics[index].close.code = 73; },
      value => { value.diagnostics[index].close.signal = 'SIGTERM'; },
      value => { value.rpcLifecycle[index].replacement = undefined; },
      value => { value.rpcLifecycle[index].replacement.priorLive = false; },
      value => { value.rpcLifecycle[index].replacement.invocationId = String(index); },
      value => { value.rpcLifecycle[index].replacement.invocationId = 'missing'; },
      value => { value.rpcLifecycle[index].promptStopReason = 'cancelled'; },
      value => { value.rpcLifecycle[index].callCompleted = false; },
      value => { value.rpcLifecycle[index].method = 'prompt'; },
      value => { value.rpcLifecycle[index + 1].callCompleted = false; },
      value => { value.rpcLifecycle[index + 1].sessionId = value.rpcLifecycle[index].sessionId; },
      value => { value.diagnostics[index + 1].startedAt = 0; },
      value => { value.diagnostics[index + 1].route = 'prompt'; },
      value => { value.rpcLifecycle.push(value.rpcLifecycle[index]); },
    ]) {
      const tampered = structuredClone(output);
      change(tampered);
      assert.throws(() => auditArtifact(tampered, expected), 'Unproven natural exit accepted');
    }
    const premature = structuredClone(output);
    premature.diagnostics[1].close = { code: 0, signal: null, kind: 'natural' };
    assert.throws(() => auditArtifact(premature, expected), 'Last child exited naturally without replacement');
  } finally { rmSync(root, { recursive: true, force: true }); }
});
