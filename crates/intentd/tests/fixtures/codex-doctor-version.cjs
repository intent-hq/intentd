// Preamble for the shared catalog fixture: observe offline --version calls.
if (process.argv.includes('--version')) {
  const fs = require('node:fs');
  const path = require('node:path');
  const config = JSON.parse(fs.readFileSync(path.join(fixture, 'fixture.json'), 'utf8'));
  fs.appendFileSync(path.join(fixture, 'events.jsonl'), JSON.stringify({role, version:true})+'\n');
  const mode = config[role === 'acp' ? 'versionAcp' : 'versionRaw'];
  process.stderr.write('credential-canary account-canary user@example.invalid\n');
  if (mode === 'invalid') {
    process.stdout.write('credential-canary\u001b[31m\n');
    process.exit(0);
  }
  if (mode === 'timeout') {
    // Deliberately never answer; doctor must enforce its production deadline.
    setInterval(() => {}, 1000);
    return;
  }
}
