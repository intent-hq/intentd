import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtempSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import diagnostics from './pi-diagnostics.cjs';
const { Capture, LIMITS, safeText, safeValue, failureReport, publishEvidence } = diagnostics;
const secret = 'CANARY_pi_fixture_secret_9387';

test('stream chunks, UTF8 and controls cannot split credentials past redaction', () => {
  const raw = Buffer.from(`original é failure\nAuthorization: Bearer ${secret}\nto\x1b[31mken=${secret}\nhttps://user:${secret}@example.test\n\x1b]0;${secret}\x07done\n`);
  for (let split = 0; split <= raw.length; split++) {
    const capture = new Capture({ secrets: [secret, 'token'] });
    capture.push(raw.subarray(0, split));
    assert.ok(!capture.snapshot().text.includes(secret));
    capture.push(raw.subarray(split));
    const result = capture.snapshot(true);
    assert.match(result.text, /original é failure/);
    assert.ok(!result.text.includes(secret));
    assert.doesNotMatch(result.text, /[\x00-\x09\x0b-\x1f\x7f-\x9f\ufffd]/);
  }
});

test('capture bounds memory and drops partial lines at truncation without secret suffix leaks', () => {
  for (let offset = 0; offset < secret.length + 4; offset++) {
    const capture = new Capture({ limit: 128, secrets: [secret] });
    capture.push(Buffer.from('original error\n' + 'x'.repeat(100 - offset) + `token=${secret}` + 'é'.repeat(100_000)));
    const result = capture.snapshot(true);
    assert.ok(capture.retainedBytes <= 128);
    assert.ok(Buffer.byteLength(result.text) <= 128);
    assert.equal(result.truncated, true);
    assert.equal(result.bytesSeen > result.bytesRetained, true);
    assert.match(result.text, /original error/);
    assert.ok(!result.text.includes('CANARY'));
    assert.ok(!result.text.includes('\ufffd'));
  }
});

test('patterns run before literal replacement and controls cannot hide a key', () => {
  for (const raw of [`token=${secret}`, `to\\u001b[0mken=${secret}`, `to\x1b[0mken=${secret}`, `"api_key": "${secret}"`, `Authorization: Bearer ${secret}`, `https://user:${secret}@example.test`]) {
    assert.ok(!safeText(raw, { secrets: ['token', 'api_key', 'Authorization'] }).text.includes(secret));
  }
});

test('bounded failure report keeps ACP error separate from independent child evidence', () => {
  const witness = { invocationId: 'one', route: 'loadSession', close: { code: 73, signal: null, kind: 'natural' }, stderr: new Capture() };
  witness.stderr.push(Buffer.from('original child failure\n'));
  witness.stderr = witness.stderr.snapshot(true);
  const report = failureReport('loadSession', { message: 'generic destroyed stream' }, [witness]);
  assert.match(report, /generic destroyed stream/);
  assert.match(report, /original child failure/);
  assert.match(report, /73/);
  assert.ok(Buffer.byteLength(failureReport('loadSession', { message: `token=${secret}\n` + 'x'.repeat(1_000_000) }, [witness])) <= LIMITS.report);
});

test('nested errors, oversized fields and multibyte strings obey artifact bounds', () => {
  const result = safeValue({ error: { token: secret, message: `password=${secret}\n` + 'é'.repeat(500_000) }, rows: Array(10000).fill('x') });
  assert.ok(!JSON.stringify(result).includes(secret));
  assert.ok(Buffer.byteLength(JSON.stringify(result)) < LIMITS.artifact);
  assert.ok(result.truncated);
});


test('JSON expansion and nested ACP error details cannot evade report limits or redaction', () => {
  const capture = new Capture();
  capture.push(('"\n').repeat(4096));
  const stderr = capture.snapshot(true);
  assert.ok(Buffer.byteLength(JSON.stringify(stderr.text)) <= LIMITS.capture);
  assert.equal(stderr.truncated, true);
  const error = Object.assign(new Error('generic ACP failure'), { data: {
    details: 'to\x1b[31mken=UNLISTED_SECRET\n' + 'é'.repeat(100_000),
  } });
  const report = failureReport('loadSession', error, []);
  assert.ok(!report.includes('UNLISTED_SECRET'));
  assert.ok(!report.includes(secret));
  assert.ok(Buffer.byteLength(report) <= LIMITS.report);
});


test('credential keys containing controls and large key sets stay sanitized and bounded', () => {
  const value = { 'to\x1b[31mken': 'UNLISTED_SECRET', API_TOKEN: 'UNLISTED_SECRET',
    ...Object.fromEntries(Array.from({ length: 1000 }, (_, i) => ['é'.repeat(1000) + i, 'é'.repeat(1000)])) };
  const result = safeValue(value);
  assert.ok(!JSON.stringify(result).includes('UNLISTED_SECRET'));
  assert.ok(Buffer.byteLength(JSON.stringify(result)) < LIMITS.artifact);
  assert.equal(result.truncated, true);
});


test('artifact truncation preserves cleanup, replay and stream-error proof', () => {
  const root = mkdtempSync(join(tmpdir(), 'pi-publish-bound-'));
  try {
    const file = join(root, 'evidence.json');
    const output = publishEvidence(file, { requests: Array(200).fill({ text: 'x'.repeat(10000) }),
      root, runId: 'current', streamErrors: [{ message: 'stream failed', expected: false }],
      cleanup: { completed: true, rootRemoved: false }, replay: [{ text: 'saved' }],
      clients: [{ witnesses: [{ invocationId: 'actual-child' }] }],
    });
    assert.equal(output.artifactTruncated, true);
    assert.equal(output.streamErrors[0].expected, false);
    assert.equal(output.cleanup.rootRemoved, false);
    assert.equal(output.replay[0].text, 'saved');
    assert.equal(output.diagnostics[0].invocationId, 'actual-child');
    assert.ok(statSync(file).size <= LIMITS.artifact);
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test('raw and escaped credential whitespace stays redacted across every chunk boundary and output surface', () => {
  const value = 'SYNTHETIC_UNLISTED_CREDENTIAL_123456';
  const root = mkdtempSync(join(tmpdir(), 'pi-whitespace-redaction-'));
  try {
    for (const separator of ['\t', '\r', '\r\n', '\n# ', '\r\n# ', '\\t', '\\r', '\\r\\n', '\\x09', '\\x0d', '\\u0009', '\\u000d', '\\\\t', '\\\\r']) {
      const raw = Buffer.from(`original é failure\nBearer${separator}${value}\n`);
      for (let split = 0; split <= raw.length; split++) {
        const capture = new Capture();
        capture.push(raw.subarray(0, split));
        assert.ok(!capture.snapshot().text.includes(value), 'Partial capture leaks a credential');
        capture.push(raw.subarray(split));
        const result = capture.snapshot(true);
        assert.ok(!result.text.includes(value), 'Completed capture leaks a credential');
        assert.match(result.text, /original é failure/);
      }
      assert.ok(!failureReport('loadSession', new Error(raw.toString())).includes(value), 'Failure report leaks a credential');
      const record = publishEvidence(join(root, 'evidence.json'), { error: raw.toString() });
      assert.ok(!JSON.stringify(record).includes(value), 'JSON artifact leaks a credential');
    }
    assert.equal(safeText('C:\\temp\\runtime\\test.json').text, 'C:\\temp\\runtime\\test.json', 'Windows paths must not be unescaped as whitespace');
  } finally { rmSync(root, { recursive: true, force: true }); }
});
