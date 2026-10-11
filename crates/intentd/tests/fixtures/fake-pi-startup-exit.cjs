// Deterministic early startup failure: receive the handshake, but never answer it.
// No timers, credentials, or installed Pi configuration participate in this case.
const { appendFileSync, writeSync } = require('node:fs');
const { createInterface } = require('node:readline');

createInterface({ input: process.stdin }).on('line', line => {
  const command = JSON.parse(line);
  appendFileSync(process.env.PI_TEST_JOURNAL, JSON.stringify({
    event: 'startup-request', pid: process.pid, argv: process.argv.slice(2), command: command.type,
  }) + '\n');
  if (command.type !== 'get_state') process.exit(74);
  writeSync(2, 'PI_STARTUP_SENTINEL: deterministic child initialization failure\n');
  writeSync(2, 'to\x1b[31mken=CANARY_pi_fixture_secret_9387\n');
  if (process.env.PI_TEST_LARGE_STDERR === '1') writeSync(2, 'oversized: ' + 'é'.repeat(100_000) + '\n');
  process.exit(73);
});
