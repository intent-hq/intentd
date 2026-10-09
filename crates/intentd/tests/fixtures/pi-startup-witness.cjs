// Test-only observer at PI_ACP_PI_COMMAND. Records the actual fixture child's
// stderr and close status before returning that status to the unmodified adapter.
// This evidence file is NOT a production error-reporting fix.
const { spawn } = require('node:child_process');
const { appendFileSync } = require('node:fs');

const child = spawn(process.execPath, [process.env.PI_TEST_CHILD_ENTRY, ...process.argv.slice(2)], {
  stdio: ['inherit', 'inherit', 'pipe'],
});
const chunks = [];
let bytes = 0;
child.stderr.on('data', chunk => {
  const retained = chunk.subarray(0, Math.max(0, 4096 - bytes));
  if (retained.length) chunks.push(retained);
  bytes += chunk.length;
  process.stderr.write(chunk);
});
child.on('error', error => {
  appendFileSync(process.env.PI_TEST_WITNESS, JSON.stringify({ spawnError: error.message }) + '\n');
  process.exitCode = 75;
});
child.on('close', (code, signal) => {
  appendFileSync(process.env.PI_TEST_WITNESS, JSON.stringify({
    pid: child.pid, code, signal, stderr: Buffer.concat(chunks).toString('utf8'), truncated: bytes > 4096,
  }) + '\n');
  process.exitCode = code ?? 76;
});
