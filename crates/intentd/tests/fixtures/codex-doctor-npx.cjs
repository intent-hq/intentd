// Copy a deterministic package into the isolated npm cache. Never invoke npm.
const fs = require('node:fs');
const path = require('node:path');
const args = process.argv.slice(2);
if (args.length === 1 && args[0] === '--version') {
  console.log('11.0.0');
  process.exit(0);
}
fs.appendFileSync(path.join(fixture, 'events.jsonl'), JSON.stringify({role:'npx',args,home:process.env.HOME})+'\n');
if (args[0] !== '--workspaces=false' || args[1] !== '-y' || !args[2]?.startsWith('@agentclientprotocol/codex-acp@')) {
  throw new Error('unexpected package request');
}
const target = path.join(process.env.HOME, 'npm-cache', 'node_modules');
fs.cpSync(path.join(fixture, 'node_modules'), target, {recursive:true});
const adapter = path.join(target, '@agentclientprotocol/codex-acp/dist/index.js');
const child = require('node:child_process').spawn(process.execPath, [adapter, ...args.slice(3)], {stdio:'inherit',env:process.env});
child.on('exit', code => process.exit(code || 0));
