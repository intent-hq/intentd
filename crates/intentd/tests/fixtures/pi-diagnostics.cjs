// Test/CI evidence only. stdout and ACP responses are never rewritten.
// Prefix capture deliberately omits an incomplete line: a credential or control
// sequence cut by a stream/chunk/byte boundary must not become a visible suffix.
const { writeFileSync, renameSync } = require('node:fs');
const LIMITS = Object.freeze({ capture: 8192, witness: 16384, report: 16384, artifact: 262144, tap: 131072, invocations: 16 });
const SECRET = 'CANARY_pi_fixture_secret_9387';
const controls = text => text
  .replace(/\\+(?:u00([0-9a-f]{2})|x([0-9a-f]{2}))/gi, (match, unicode, hex) => {
    const code = parseInt(unicode ?? hex, 16);
    return code < 32 || (code >= 127 && code <= 159) ? String.fromCharCode(code) : match;
  })
  .replace(/(?:\x1b\]|\x9d)[\s\S]*?(?:\x07|\x1b\\|\x9c|$)/g, '')
  .replace(/\x1b[P^_X][\s\S]*?(?:\x1b\\|$)/g, '')
  .replace(/(?:\x1b\[|\x9b)[0-?]*[ -/]*[@-~]/g, '')
  .replace(/\x1b(?:\[[^\n]*|[^\n]?)$/g, '')
  .replace(/\x1b[ -/]*[@-~]/g, '')
  .replace(/[\t\r\v\f]/g, ' ')
  .replace(/[\x00-\x09\x0b-\x1f\x7f-\x9f]/g, '');
function sanitize(text, secrets) {
  // Match credential syntax BEFORE literal values: a value equal to "token"
  // must not erase the label that tells us to redact the following value.
  text = controls(text)
    // Node TAP/JSON may spell whitespace as backslash escapes. Normalize only
    // credential boundaries (including TAP comment continuation), not paths like C:\\temp.
    .replace(/\bBearer(?:(?:\n|\\+n)[ \t]*(?:\\*#[ \t]*)+|\s|\\+[trn])+/gi, 'Bearer ')
    .replace(/\b(?:authorization|(?:access|refresh|auth|api)[_-]?(?:token|key)|token|password|secret|credential)["']?(?:(?:\n|\\+n)[ \t]*(?:\\*#[ \t]*)+|\s|\\+[trn])*[:=](?:(?:\n|\\+n)[ \t]*(?:\\*#[ \t]*)+|\s|\\+[trn])*[^\n]*/gi, '[REDACTED]')
    .replace(/\bBearer\s+[^\s"']+/gi, 'Bearer [REDACTED]')
    .replace(/(https?:\/\/)[^\s/]+@/gi, '$1[REDACTED]@');
  for (const secret of [...secrets].map(controls).filter(Boolean).sort((a, b) => b.length - a.length)) {
    text = text.split(secret).join('[REDACTED]');
  }
  return text;
}
function utf8Prefix(text, limit) {
  const bytes = Buffer.from(text);
  if (bytes.length <= limit) return text;
  let end = limit;
  while (end > 0 && (bytes[end] & 0xc0) === 0x80) end--;
  return bytes.subarray(0, end).toString('utf8');
}
class Capture {
  constructor({ limit = LIMITS.capture, secrets = [SECRET] } = {}) {
    this.limit = limit;
    this.secrets = secrets;
    this.buffer = Buffer.alloc(limit);
    this.retainedBytes = 0;
    this.bytesSeen = 0;
  }
  push(chunk) {
    if (!Buffer.isBuffer(chunk)) chunk = Buffer.from(chunk);
    this.bytesSeen = Math.min(Number.MAX_SAFE_INTEGER, this.bytesSeen + chunk.length);
    const n = Math.min(chunk.length, this.limit - this.retainedBytes);
    chunk.copy(this.buffer, this.retainedBytes, 0, n);
    this.retainedBytes += n;
  }
  snapshot(final = false) {
    let bytes = this.buffer.subarray(0, this.retainedBytes);
    const cut = this.bytesSeen > this.retainedBytes;
    if (!final || cut) bytes = bytes.subarray(0, bytes.lastIndexOf(10) + 1);
    let text = sanitize(bytes.toString('utf8'), this.secrets);
    const truncated = cut || Buffer.byteLength(JSON.stringify(text)) > this.limit;
    if (truncated) {
      // Quotes/newlines can double in JSON. Leave room for the visible marker
      // and bound the encoded string too, before it reaches a file or report.
      text = utf8Prefix(text, Math.max(0, Math.floor(this.limit / 2) - 32)) + '\n[diagnostic truncated]\n';
    }
    return { text, bytesSeen: this.bytesSeen, bytesRetained: this.retainedBytes,
      limitBytes: this.limit, truncated, incompleteLineOmitted: bytes.length < this.retainedBytes };
  }
}
function safeText(text, options) {
  const capture = new Capture(options);
  capture.push(String(text));
  return capture.snapshot(true);
}
// Bound diagnostic copies independently of live protocol objects. Never feed
// this projection back into the adapter or successful-session assertions.
function safeValue(value) {
  let remaining = 65536;
  let nodes = 0;
  let truncated = false;
  function visit(value, depth = 0) {
    if (++nodes > 2048 || depth > 16 || remaining < 64) { truncated = true; return '[omitted]'; }
    if (typeof value === 'string') {
      const captured = safeText(value, { limit: Math.min(LIMITS.capture, remaining) });
      remaining -= Buffer.byteLength(captured.text);
      truncated ||= captured.truncated;
      return captured.text;
    }
    if (Array.isArray(value)) {
      truncated ||= value.length > 128;
      return value.slice(0, 128).map(item => visit(item, depth + 1));
    }
    if (value && typeof value === 'object') {
      const entries = [];
      for (const key in value) {
        if (!Object.hasOwn(value, key)) continue;
        if (entries.length === 128 || nodes >= 2048 || remaining < 64) { truncated = true; break; }
        const name = safeText(key, { limit: Math.min(256, remaining) });
        remaining -= Buffer.byteLength(name.text);
        truncated ||= name.truncated;
        const sensitive = /^(?:authorization|(?:access|refresh|auth|api)[_-]?(?:token|key)|token|password|secret|credential)$/i.test(controls(key));
        entries.push([name.text, sensitive ? '[REDACTED]' : visit(value[key], depth + 1)]);
      }
      return Object.fromEntries(entries);
    }
    return value;
  }
  return { value: visit(value), truncated };
}
function failureReport(method, error, witnesses = []) {
  const child = witnesses.filter(row => row.argv?.includes('rpc')).at(-1) ?? witnesses.at(-1);
  const acp = safeValue(error instanceof Error ? { name: error.name, message: error.message, data: error.data,
    causes: error instanceof AggregateError ? error.errors.slice(0, 8).map(cause => String(cause.message ?? cause)) : undefined } : error);
  const acpError = safeText(JSON.stringify(acp.value), { limit: 4096 });
  const report = { method, acpError, independentChild: child ?? null };
  const text = JSON.stringify(report);
  if (Buffer.byteLength(text) <= LIMITS.report) return text;
  return JSON.stringify({ method, acpError, independentChild: child ? {
    invocationId: child.invocationId, route: child.route, close: child.close,
    stderr: safeText(child.stderr?.text ?? '', { limit: 2048 }),
  } : null, truncated: true });
}
function summarizeStreamErrors(errors) {
  if (!Array.isArray(errors)) return undefined;
  let unexpected = 0;
  for (const error of errors) if (error?.expected !== true) unexpected++;
  return { total: errors.length, unexpected };
}
function publishEvidence(file, record) {
  const streamErrorSummary = summarizeStreamErrors(record.streamErrors);
  // Mandatory proof precedes bulky optional request/history transcripts, so
  // exhausting the artifact budget cannot hide cleanup or stream failures.
  const primary = Object.fromEntries(['root', 'runId', 'route', 'oversized', 'result', 'controls',
    'cleanup', 'streamErrors', 'replay', 'failureReport', 'error', 'harnessError',
    'response', 'platform', 'node', 'pin', 'piVersion', 'commandKind']
    .filter(key => Object.hasOwn(record, key)).map(key => [key, record[key]]));
  const safe = safeValue({ ...primary,
    diagnostics: record.clients?.flatMap(client => client.witnesses ?? []) ?? [], ...record });
  const output = { ...safe.value, streamErrorSummary, diagnosticSchema: 1, evidenceRun: process.env.PI_DIAGNOSTICS_RUN_ID ?? 'local',
    limits: LIMITS, artifactTruncated: safe.truncated };
  const json = JSON.stringify(output, null, 2);
  if (Buffer.byteLength(json) > LIMITS.artifact) throw new Error('Diagnostic artifact exceeds byte cap');
  writeFileSync(file, json);
  return output;
}
function writeWitness(file, record) {
  const json = JSON.stringify(record);
  if (Buffer.byteLength(json) > LIMITS.witness) throw new Error('Child witness exceeds byte cap');
  writeFileSync(file + '.tmp', json);
  renameSync(file + '.tmp', file);
}
module.exports = { Capture, LIMITS, SECRET, safeText, safeValue, failureReport, publishEvidence, writeWitness, summarizeStreamErrors };
