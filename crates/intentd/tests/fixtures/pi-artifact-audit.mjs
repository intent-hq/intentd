// Independent consumer of on-disk evidence. Deliberately does not use the
// producer's sanitizer or trust a fixture's "passed" flag as diagnostic proof.
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { existsSync, readFileSync, readdirSync, statSync, writeFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export function auditArtifact(record, { run, route, oversized, runtime = false }) {
  assert.equal(record.diagnosticSchema, 1, 'Missing diagnostic schema');
  assert.equal(record.evidenceRun, run, 'Stale diagnostic run');
  assert.equal(record.limits?.capture, 8192, 'Wrong capture cap');
  assert.equal(record.limits?.artifact, 262144, 'Wrong artifact cap');
  assert.equal(typeof record.artifactTruncated, 'boolean', 'Missing truncation metadata');
  assert.equal(record.cleanup?.completed, true, 'Missing cleanup completion');
  assert.equal(record.cleanup.rootRemoved, true, 'Temporary root not removed');
  assert.equal(existsSync(record.root), false, 'Temporary root still exists');
  for (const step of ['stopClients', 'closeLifetime', 'removeRoot']) {
    assert.equal(record.cleanup.steps?.[step]?.result, 'passed', `Cleanup ${step} failed`);
  }
  assert.ok(record.diagnostics?.length, 'Missing independent child evidence');
  const ids = new Set();
  for (const child of record.diagnostics) {
    assert.equal(child.runId, record.runId, 'Stale child evidence');
    assert.ok(child.invocationId && !ids.has(child.invocationId), 'Duplicate child invocation');
    ids.add(child.invocationId);
    assert.ok(Number.isInteger(child.pid) && child.pid > 0, 'Missing actual child PID');
    assert.ok(child.route, 'Missing invocation route');
    assert.ok(child.close, 'Missing actual child close');
    assert.ok(['natural', 'harness', 'adapter'].includes(child.close.kind), 'Unknown child close kind');
    const stderr = child.stderr;
    assert.ok(stderr && typeof stderr.text === 'string', 'Missing stderr capture');
    assert.equal(stderr.limitBytes, 8192);
    assert.ok(stderr.bytesRetained <= 8192 && stderr.bytesRetained >= 0);
    assert.ok(stderr.bytesSeen >= stderr.bytesRetained);
    assert.ok(Buffer.byteLength(stderr.text) <= 8192);
    assert.equal(typeof stderr.truncated, 'boolean');
    assert.equal(typeof stderr.incompleteLineOmitted, 'boolean');
    if (stderr.bytesSeen > stderr.bytesRetained) assert.equal(stderr.truncated, true);
  }
  if (runtime) {
    const summary = record.streamErrorSummary;
    assert.ok(summary && Number.isSafeInteger(summary.total) && summary.total >= 0
      && Number.isSafeInteger(summary.unexpected) && summary.unexpected >= 0
      && summary.unexpected <= summary.total, 'Missing or invalid stream-error summary');
    assert.ok(summary.total >= (record.streamErrors?.length ?? 0), 'Inconsistent stream-error summary');
    assert.equal(summary.unexpected, 0, 'Unexpected MCP stream error');
    assert.equal(record.result, 'passed', 'Real runtime fixture failed');
    assert.deepEqual(record.replay?.map(row => row.text), ['first:created', 'second:created'], 'Saved-session replay missing');
    const rpc = record.diagnostics.filter(child => child.argv.includes('rpc'));
    assert.equal(rpc.filter(child => child.route === 'newSession').length, 2);
    assert.equal(rpc.filter(child => child.route === 'loadSession').length, 2);
    assert.ok(rpc.every(child => ['harness', 'adapter'].includes(child.close.kind)), 'Successful RPC children must record requested shutdown');
  } else {
    assert.equal(record.route, route);
    assert.equal(record.oversized, oversized);
    const exits = record.diagnostics.filter(child => child.close.kind === 'natural' && child.close.code === 73);
    assert.equal(exits.length, 1, 'Need exactly one natural exit, without retry');
    const child = exits[0];
    assert.equal(child.route, `session/${route}`);
    assert.equal(child.close.signal, null);
    assert.match(child.stderr.text, /PI_STARTUP_SENTINEL: deterministic child initialization failure/);
    assert.equal(child.stderr.truncated, oversized);
    assert.ok(record.response?.error, 'Missing observed ACP error');
    assert.ok(!JSON.stringify(record.response.error).includes('PI_STARTUP_SENTINEL'), 'ACP error was rewritten');
    assert.match(record.failureReport, /PI_STARTUP_SENTINEL/);
    assert.match(record.failureReport, /"code":73/);
    assert.ok(Buffer.byteLength(record.failureReport) <= 16384, 'Oversized failure report');
  }
}
export function auditText(text) {
  // Independently reject unmasked Bearer values, including TAP/JSON escape
  // spellings. Do not use the producer's normalization or sanitizer here.
  for (const match of text.matchAll(/\bBearer(?:\r?\n[ \t]*#[ \t]*|\s|\\+(?:[trn]|x(?:09|0a|0d)|u00(?:09|0a|0d)))+([^\\\s"']+)/gi)) {
    assert.equal(match[1], '[REDACTED]', 'Unredacted Bearer credential');
  }
  assert.ok(!text.includes('CANARY_pi_fixture_secret_9387'), 'Unredacted synthetic credential');
  assert.doesNotMatch(text, /[\x00-\x09\x0b-\x1f\x7f-\x9f]/, 'Terminal control in diagnostic text');
  assert.doesNotMatch(text, /\\(?:u00(?:1b|9b|9d)|x1b)/i, 'Escaped terminal control in diagnostic text');
}
export function auditDirectory(directory, run, platform = process.platform) {
  assert.ok(run, 'Expected current run identity is required');
  const required = ['startup-new', 'startup-new-large', 'startup-load', 'startup-load-large',
    ...(platform === 'win32' ? ['runtime-bare', 'runtime-absolute'] : ['runtime-unix'])];
  for (const name of required) assert.ok(existsSync(join(directory, `${name}.json`)), `Missing ${name} evidence`);
  for (const name of readdirSync(directory)) {
    if (['package-lock.json', 'pi-package-lock.json', 'package.txt', 'adapter-integrity.json'].includes(name)) continue;
    const file = join(directory, name);
    assert.ok(statSync(file).isFile(), 'Unexpected diagnostic directory');
    assert.ok(name.endsWith('.json') || name.endsWith('.tap'), 'Unexpected diagnostic file');
    assert.ok(statSync(file).size <= (name.endsWith('.tap') ? 131072 : 262144), `Oversized ${name}`);
    const text = readFileSync(file, 'utf8');
    auditText(text);
    if (name.endsWith('.json')) {
      const record = JSON.parse(text);
      // Decode JSON strings too, so JSON escaping cannot hide terminal controls.
      const walk = value => {
        if (typeof value === 'string') auditText(value);
        else if (value && typeof value === 'object') Object.values(value).forEach(walk);
      };
      walk(record);
      if (required.includes(name.slice(0, -5))) auditArtifact(record, { run,
        route: name.includes('-new') ? 'new' : 'load', oversized: name.includes('-large'), runtime: name.startsWith('runtime-') });
    }
  }
  for (const name of ['test', 'cleanup-checks', 'diagnostic-checks', 'startup', 'runtime']) {
    const text = readFileSync(join(directory, `${name}.tap`), 'utf8');
    const metadata = JSON.parse(text.split('\n# diagnostic capture ').at(-1));
    assert.equal(metadata.evidenceRun, run, 'Stale TAP capture');
    assert.equal(metadata.code, 0, `Fixture ${name} failed`);
    assert.equal(metadata.timedOut, false, 'Fixture timed out');
  }
}
function digestPackage(entry) {
  const root = resolve(entry, '../..');
  const hash = createHash('sha256');
  const walk = directory => {
    for (const name of readdirSync(directory).sort()) {
      const file = join(directory, name);
      if (statSync(file).isDirectory()) walk(file);
      else { hash.update(file.slice(root.length)); hash.update(readFileSync(file)); }
    }
  };
  walk(root);
  return hash.digest('hex');
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const directory = process.env.PI_ACP_EVIDENCE_DIR;
  const run = process.env.PI_DIAGNOSTICS_RUN_ID;
  const integrity = join(directory, 'adapter-integrity.json');
  if (process.argv.includes('--record-package')) {
    writeFileSync(integrity, JSON.stringify({ run, sha256: digestPackage(process.env.PI_ACP_TEST_ENTRY) }));
  } else {
    try {
      auditDirectory(directory, run);
      const before = JSON.parse(readFileSync(integrity, 'utf8'));
      assert.equal(before.run, run, 'Stale adapter integrity record');
      assert.equal(digestPackage(process.env.PI_ACP_TEST_ENTRY), before.sha256, 'Installed adapter package was modified');
      console.log('Pi diagnostics, cleanup, saved replay and unchanged package audit passed.');
    } catch { console.error('Pi artifact audit failed; inspect the bounded evidence and test results.'); process.exitCode = 1; }
  }
}
