// Copy a deterministic package into the isolated npm cache. Never invoke npm.
// The shared fake npx checks the launch argv and passes only the arguments
// after the package.
const fs = require('node:fs');
const path = require('node:path');
const args = process.argv.slice(2);
fs.appendFileSync(path.join(fixture, 'events.jsonl'), JSON.stringify({role:'npx',args,home:process.env.HOME})+'\n');
const target = path.join(process.env.HOME, 'npm-cache', 'node_modules');
fs.cpSync(path.join(fixture, 'node_modules'), target, {recursive:true});
const adapter = path.join(target, '@agentclientprotocol/codex-acp/dist/index.js');
const child = require('node:child_process').spawn(process.execPath, [adapter, ...args], {stdio:'inherit',env:process.env});
child.on('exit', code => process.exit(code || 0));
