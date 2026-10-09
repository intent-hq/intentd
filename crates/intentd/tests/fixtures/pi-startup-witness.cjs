// Supported PI_ACP_PI_COMMAND observer, used only by the test harness.
// Pi stdin/stdout stay inherited; no ACP/RPC response is synthesized or retried.
const { spawn } = require('node:child_process');
const { randomUUID } = require('node:crypto');
const { existsSync, readFileSync, readdirSync } = require('node:fs');
const { join } = require('node:path');
const { Capture, LIMITS, safeText, writeWitness } = require('./pi-diagnostics.cjs');
const directory = process.env.PI_TEST_WITNESS_DIR;
if (readdirSync(directory).filter(name => name.endsWith('.json')).length >= LIMITS.invocations) {
  throw new Error('Fixture invocation limit exceeded');
}
const invocationId = randomUUID();
const file = join(directory, invocationId + '.json');
const record = { invocationId, runId: process.env.PI_TEST_RUN_ID,
  route: safeText(readFileSync(join(directory, 'route'), 'utf8')).text,
  argv: process.argv.slice(2).map(arg => safeText(arg).text),
  startedAt: Date.now(), observerPid: process.pid, pid: null, close: null };
const capture = new Capture();
let requestedSignal;
const exitKind = () => existsSync(file + '.stop') ? 'harness' : requestedSignal ? 'adapter' : 'natural';
const publish = final => writeWitness(file, { ...record, stderr: capture.snapshot(final) });
const child = spawn(process.execPath, [process.env.PI_TEST_CHILD_ENTRY, ...process.argv.slice(2)], {
  stdio: ['inherit', 'inherit', 'pipe'],
});
record.pid = child.pid ?? null;
for (const signal of ['SIGTERM', 'SIGINT']) process.on(signal, () => {
  requestedSignal = signal;
  child.kill(signal);
});
publish(false);
child.stderr.on('data', chunk => {
  const retaining = capture.retainedBytes < LIMITS.capture;
  capture.push(chunk);
  if (retaining) publish(false);
});
child.on('error', error => { record.spawnError = safeText(error.message); });
child.on('exit', (code, signal) => {
  record.exit = { code, signal, kind: exitKind() };
  publish(false);
});
child.on('close', (code, signal) => {
  record.close = record.exit ?? { code, signal, kind: exitKind() };
  publish(true);
  process.exitCode = code ?? 76;
});
