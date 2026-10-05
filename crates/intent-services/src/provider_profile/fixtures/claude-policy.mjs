// Native policy parity against the same synthetic matrix as the Rust regression.
// No credentials, model prompts, or changes to real managed settings.
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import assert from 'node:assert/strict';
import http from 'node:http';
import { spawn, spawnSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';

const modules = path.resolve(process.argv[2]);
const sdk = path.join(modules, '@anthropic-ai/claude-agent-sdk');
assert.equal(JSON.parse(await fs.readFile(path.join(sdk, 'package.json'))).version, '0.3.280');
const cli = path.join(modules, '@anthropic-ai/claude-agent-sdk-linux-x64/claude');
const cases = JSON.parse(await fs.readFile(new URL('./claude-policy-cases.json', import.meta.url)));
const root = await fs.mkdtemp(path.join(os.tmpdir(), 'intent-claude-policy-'));
const home = path.join(root, 'home');
const cwd = path.join(root, 'repo');
const admin = path.join(root, 'admin');
for (const dir of [home, cwd, admin]) await fs.mkdir(dir);
const safeEnv = { HOME: home, USERPROFILE: home, CLAUDE_CONFIG_DIR: home, PATH: process.env.PATH,
  CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: '1', DISABLE_AUTOUPDATER: '1' };
for (const key of Object.keys(process.env)) delete process.env[key];
Object.assign(process.env, safeEnv);
const { query } = await import(pathToFileURL(path.join(sdk, 'sdk.mjs')));
const namespace = ['--unshare-user', '--die-with-parent', '--ro-bind', '/', '/', '--dev', '/dev', '--proc', '/proc',
  '--bind', root, root, '--tmpfs', '/etc', '--ro-bind', '/etc/passwd', '/etc/passwd',
  '--ro-bind', '/etc/group', '/etc/group', '--ro-bind', admin, '/etc/claude-code', '--'];
assert.equal(spawnSync('bwrap', [...namespace, cli, '--version'], { env: safeEnv, encoding: 'utf8' }).stdout.trim(), '2.1.280 (Claude Code)');
const serverFile = path.join(root, 'mcp.mjs');
await fs.writeFile(serverFile, `import readline from 'node:readline';
for await (const line of readline.createInterface({input:process.stdin})) {
 const m=JSON.parse(line); if (!('id' in m)) continue;
 const result=m.method==='initialize'?{protocolVersion:'2024-11-05',capabilities:{tools:{}},serverInfo:{name:'fixture',version:'1'}}:m.method==='tools/list'?{tools:[]}:{};
 console.log(JSON.stringify({jsonrpc:'2.0',id:m.id,result}));
}
`);
// Remote admission is measured by inventory, independent of a successful MCP
// handshake. Both allowed HTTP and SSE endpoints are local and return promptly.
const remote = http.createServer((_req, res) => { res.writeHead(404); res.end(); });
await new Promise(resolve => remote.listen(0, '127.0.0.1', resolve));
const endpoint = `http://127.0.0.1:${remote.address().port}`;
const translate = value => {
  if (typeof value === 'string') return value.replace('http://fixture.invalid', endpoint);
  if (Array.isArray(value)) return value.map(translate);
  if (value && typeof value === 'object') {
    const result = Object.fromEntries(Object.entries(value).map(([k,v]) => [k, translate(v)]));
    if (result.serverCommand?.[0] === 'approved-server') result.serverCommand = [process.execPath, serverFile, ...result.serverCommand.slice(1)];
    if (result.command === 'approved-server') { result.command = process.execPath; result.args = [serverFile, ...result.args]; }
    return result;
  }
  return value;
};
try {
  for (const fixture of cases) {
    const c = translate(fixture);
    await fs.writeFile(path.join(admin, 'managed-settings.json'), JSON.stringify(c.policy));
    let finish, childClosed;
    const waiting = new Promise(resolve => { finish = resolve; });
    const q = query({ prompt: (async function* () { await waiting; })(), options: {
      cwd, env: safeEnv, pathToClaudeCodeExecutable: cli, settingSources: [], strictMcpConfig: true,
      settings: { disableClaudeAiConnectors: true }, mcpServers: { [c.name]: c.server },
      spawnClaudeCodeProcess: options => {
        const child = spawn('bwrap', [...namespace, options.command, ...options.args], {
          cwd: options.cwd, env: options.env, stdio: ['pipe', 'pipe', 'pipe'], signal: options.signal,
        });
        childClosed = new Promise(resolve => child.once('close', resolve));
        return child;
      },
    } });
    const timer = setTimeout(() => q.close(), 15000);
    try {
      await q.initializationResult();
      const servers = await q.mcpServerStatus();
      assert.equal(servers.some(s => s.name === c.name), c.allowed, c.label);
      console.log(`PASS ${c.label}: ${c.allowed ? 'admitted' : 'rejected'}`);
    } finally {
      clearTimeout(timer); finish(); q.close(); await childClosed;
    }
  }
  console.log(`PASS Claude 2.1.280 policy parity: ${cases.length} cases`);
} finally {
  await new Promise(resolve => remote.close(resolve));
  await fs.rm(root, { recursive: true, force: true });
}
