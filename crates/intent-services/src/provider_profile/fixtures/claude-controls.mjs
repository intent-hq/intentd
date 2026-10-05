// Credential-free native loader fixture. No user prompt or model request is sent.
// Run with an absolute pinned node_modules directory; uses only synthetic homes.
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import assert from 'node:assert/strict';
import { pathToFileURL } from 'node:url';
import { spawn, spawnSync } from 'node:child_process';
import readline from 'node:readline';

const modules = path.resolve(process.argv[2]);
const sdkRoot = path.join(modules, '@anthropic-ai/claude-agent-sdk');
const adapterRoot = path.join(modules, '@agentclientprotocol/claude-agent-acp');
assert.equal(JSON.parse(await fs.readFile(path.join(sdkRoot, 'package.json'))).version, '0.3.280');
assert.equal(JSON.parse(await fs.readFile(path.join(adapterRoot, 'package.json'))).version, '0.81.1');
const cli = path.join(modules, '@anthropic-ai/claude-agent-sdk-linux-x64/claude');
const root = await fs.mkdtemp(path.join(os.tmpdir(), 'intent-claude-controls-'));
const home = path.join(root, 'home');
const cwd = path.join(root, 'repo');
const profile = path.join(home, '.claude');
const write = async (p, data) => { await fs.mkdir(path.dirname(p), { recursive: true }); await fs.writeFile(p, data); };
const marker = path.join(root, 'native-started');
const homeMarker = path.join(root, 'home-started');
const approvedMarker = path.join(root, 'approved-started');
const pluginMarker = path.join(root, 'plugin-started');
const managedMarker = path.join(root, 'managed-started');
const admin = path.join(root, 'admin');
await fs.mkdir(admin);
const server = path.join(root, 'mcp.mjs');
await write(server, `import fs from 'node:fs'; import readline from 'node:readline';
fs.appendFileSync(process.argv[2], 'started\\n');
for await (const line of readline.createInterface({input:process.stdin})) {
 const m=JSON.parse(line); if (!('id' in m)) continue;
 const result=m.method==='initialize'?{protocolVersion:'2024-11-05',capabilities:{tools:{}},serverInfo:{name:'fixture',version:'1'}}:m.method==='tools/list'?{tools:[{name:'echo',description:'Fixture',inputSchema:{type:'object',properties:{}}}]}:{};
 console.log(JSON.stringify({jsonrpc:'2.0',id:m.id,result}));
}
`);
const mcp = markerPath => ({ command: process.execPath, args: [server, markerPath] });
const plugin = path.join(root, 'plugin');
await write(path.join(plugin, '.claude-plugin/plugin.json'), JSON.stringify({ name: 'fixture-plugin', version: '1.0.0' }));
await write(path.join(plugin, '.mcp.json'), JSON.stringify({ mcpServers: { sentinel: mcp(pluginMarker) } }));
await write(path.join(plugin, 'skills/plugin-sentinel/SKILL.md'), '---\nname: plugin-sentinel\ndescription: Fixture only\n---\nDo not invoke.\n');
await fs.mkdir(cwd, { recursive: true });
spawnSync('git', ['init', '-q', cwd]);
await write(path.join(cwd, '.mcp.json'), JSON.stringify({ mcpServers: { native: mcp(marker) } }));
// Cover the configured home and the legacy location without touching real state.
for (const base of [home, profile]) {
  await write(path.join(base, '.claude.json'), JSON.stringify({ mcpServers: { 'native-home': mcp(homeMarker) } }));
}
for (const base of [profile, path.join(cwd, '.claude')]) {
  const name = base === profile ? 'home-sentinel' : 'project-sentinel';
  await write(path.join(base, 'skills', name, 'SKILL.md'), `---\nname: ${name}\ndescription: Fixture only\n---\nDo not invoke.\n`);
  await write(path.join(base, 'settings.json'), JSON.stringify({ enableAllProjectMcpServers: true }));
}
const safeEnv = { HOME: home, USERPROFILE: home, CLAUDE_CONFIG_DIR: profile, PATH: process.env.PATH,
  CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: '1', DISABLE_AUTOUPDATER: '1' };
// Import after removing inherited credentials/configuration, including SDK env reads.
for (const key of Object.keys(process.env)) delete process.env[key];
Object.assign(process.env, safeEnv);
const { query } = await import(pathToFileURL(path.join(sdkRoot, 'sdk.mjs')));
const supplied = process.argv[3] ? JSON.parse(await fs.readFile(process.argv[3], 'utf8')) : null;
const approvedConfig = path.join(root, 'approved.json');
await write(approvedConfig, JSON.stringify({ mcpServers: { approved: mcp(approvedMarker) } }));
const emptyConfig = path.join(root, 'empty.json');
await write(emptyConfig, '{"mcpServers":{}}');
const namespace = ['--unshare-user', '--die-with-parent', '--ro-bind', '/', '/', '--dev', '/dev', '--proc', '/proc', '--bind', root, root,
  '--tmpfs', '/etc', '--ro-bind', '/etc/passwd', '/etc/passwd', '--ro-bind', '/etc/group', '/etc/group', '--ro-bind', admin, '/etc/claude-code', '--'];
assert.equal(spawnSync('bwrap', [...namespace, cli, '--version'], { env: safeEnv, encoding: 'utf8' }).stdout.trim(), '2.1.280 (Claude Code)');
async function acpFixture() {
  const adapter = spawn('bwrap', [...namespace, process.execPath, path.join(adapterRoot, 'dist/index.js')], {
    cwd, env: { ...safeEnv, CLAUDE_CODE_EXECUTABLE: cli }, stdio: ['pipe', 'pipe', 'pipe'],
  });
  const closed = new Promise(resolve => adapter.once('close', resolve));
  const pending = new Map();
  let nextId = 0;
  let commands;
  let commandsReady;
  let stderr = '';
  adapter.stderr.on('data', data => { stderr += data; });
  readline.createInterface({ input: adapter.stdout }).on('line', line => {
    const msg = JSON.parse(line);
    if (msg.id !== undefined && pending.has(msg.id)) {
      const { resolve, reject } = pending.get(msg.id); pending.delete(msg.id);
      if (msg.error) reject(new Error(JSON.stringify(msg.error))); else resolve(msg.result);
    }
    if (msg.params?.update?.sessionUpdate === 'available_commands_update') {
      commands = msg.params.update.availableCommands;
      commandsReady?.();
    }
  });
  const call = (method, params) => new Promise((resolve, reject) => {
    const id = ++nextId; pending.set(id, { resolve, reject });
    adapter.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n');
  });
  const timer = setTimeout(() => {
    for (const request of pending.values()) request.reject(new Error('ACP fixture timed out: ' + stderr.slice(-1500)));
    adapter.kill(); commandsReady?.();
  }, 25000);
  try {
    await call('initialize', { protocolVersion: 1, clientInfo: { name: 'intent-fixture', version: '1' }, clientCapabilities: {} });
    const meta = supplied?.session_meta ?? { claudeCode: { options: { strictMcpConfig: true, settingSources: [], settings: { disableClaudeAiConnectors: true }, extraArgs: { 'disable-slash-commands': '' } } } };
    const params = { cwd, mcpServers: [{ name: 'approved', ...mcp(approvedMarker), env: [] }], _meta: meta };
    const created = await call('session/new', params);
    if (!commands) await new Promise(resolve => { commandsReady = resolve; });
    assert(commands, 'ACP command inventory absent');
    assert(!commands.some(x => /sentinel/.test(x.name)), 'ACP ignored loader metadata');
    commands = null;
    const loaded = await call('session/load', { ...params, sessionId: created.sessionId });
    if (!commands) await new Promise(resolve => { commandsReady = resolve; });
    assert(commands, 'ACP load command inventory absent');
    assert(!commands.some(x => /sentinel/.test(x.name)), 'ACP load restored native skills');
    assert.equal(await fs.stat(marker).catch(() => null), null, 'ACP started native MCP');
    assert.equal(await fs.stat(homeMarker).catch(() => null), null, 'ACP started home MCP');
    console.log(JSON.stringify({ acp: 'new/load', sessionId: created.sessionId, loadResponse: !!loaded, commands: commands.map(x => x.name) }));
  } finally {
    clearTimeout(timer); adapter.kill(); await closed;
    for (const p of [marker, homeMarker, approvedMarker, pluginMarker]) await fs.rm(p, { force: true });
  }
}
try {
  for (const mode of ['baseline', 'sdk', 'native', 'ephemeral', 'managed-baseline', 'managed-strict']) {
    const controlled = !mode.endsWith('baseline');
    const managed = mode.startsWith('managed');
    if (mode === 'managed-baseline') await acpFixture();
    if (managed) await write(path.join(admin, 'managed-mcp.json'), JSON.stringify({ mcpServers: { managed: mcp(managedMarker) } }));
    const config = mode === 'ephemeral' ? emptyConfig : approvedConfig;
    const options = supplied ? supplied.session_meta.claudeCode.options : {
      strictMcpConfig: true, settingSources: [], settings: { disableClaudeAiConnectors: true },
      extraArgs: { 'disable-slash-commands': '' },
    };
    const nativeArgs = supplied ? supplied.runtime_args.map(arg => arg === '/__fixture__/mcp.json' ? config : arg)
      : ['--strict-mcp-config', '--mcp-config', config, '--setting-sources', '',
          '--settings', '{"disableClaudeAiConnectors":true}', '--disable-slash-commands'];
    let finish;
    let childClosed;
    const waiting = new Promise(resolve => { finish = resolve; });
    const q = query({ prompt: (async function* () { await waiting; })(), options: {
      cwd, env: safeEnv, pathToClaudeCodeExecutable: cli,
      stderr: data => process.stderr.write(data),
      ...(!['native', 'ephemeral'].includes(mode) ? { plugins: [{ type: 'local', path: plugin }] } : {}),
      systemPrompt: supplied?.session_meta.systemPrompt ?? { type: 'preset', preset: 'claude_code' },
      ...(mode === 'sdk' || mode === 'managed-strict' ? { ...options, mcpServers: { approved: mcp(approvedMarker) } }
        : { settingSources: ['user', 'project', 'local'] }),
      spawnClaudeCodeProcess: spawnOptions => {
        const child = spawn('bwrap', [
        ...namespace,
        spawnOptions.command, ...spawnOptions.args,
        ...(['native', 'ephemeral'].includes(mode) ? [...nativeArgs, '--plugin-dir', plugin] : []),
      ], { cwd: spawnOptions.cwd, env: spawnOptions.env, stdio: ['pipe', 'pipe', 'pipe'], signal: spawnOptions.signal });
        childClosed = new Promise(resolve => child.once('close', resolve));
        return child;
      },
    } });
    const timeout = setTimeout(() => q.close(), 25000);
    try {
      let init;
      if (mode === 'managed-strict') {
        await assert.rejects(q.initializationResult(), /exited|enterprise|MCP/i);
        const native = spawnSync('bwrap', [...namespace, cli, '--strict-mcp-config', '--mcp-config', approvedConfig,
          '--print', '--input-format', 'stream-json', '--output-format', 'stream-json', '--verbose'],
          { cwd, env: safeEnv, input: '', encoding: 'utf8', timeout: 15000 });
        assert.notEqual(native.status, 0);
        assert.match(native.stdout + native.stderr, /enterprise MCP config/i);
        assert.equal(await fs.stat(approvedMarker).catch(() => null), null);
        console.log('PASS: managed-mcp.json rejects strict dynamic configuration before approved startup');
        continue;
      }
      init = await q.initializationResult();
      const commands = await q.supportedCommands();
      let servers = await q.mcpServerStatus();
      while (servers.some(x => x.status === 'pending')) servers = await q.mcpServerStatus();
      console.log(JSON.stringify({ mode, commands: commands.map(x => x.name), servers, initKeys: Object.keys(init) }));
      if (mode === 'managed-baseline') {
        assert.deepEqual(servers.map(x => x.name), ['managed']);
        await fs.access(managedMarker);
      } else if (!controlled) {
        assert(commands.some(x => x.name === 'project-sentinel'), 'native skill positive control absent');
        assert(servers.some(x => x.name === 'native'), 'native MCP positive control absent');
        await fs.access(marker);
        await fs.access(homeMarker);
        await fs.access(pluginMarker);
        assert(commands.some(x => x.name === 'fixture-plugin:plugin-sentinel'), 'plugin skill positive control absent');
      } else {
        assert.deepEqual(commands, [], 'skills/commands remained');
        assert.deepEqual(servers.map(x => x.name), mode === 'ephemeral' ? [] : ['approved']);
        if (mode !== 'ephemeral') await fs.access(approvedMarker);
        assert.equal(await fs.stat(marker).catch(() => null), null, 'native MCP started');
        assert.equal(await fs.stat(homeMarker).catch(() => null), null, 'home MCP started');
        assert.equal(await fs.stat(pluginMarker).catch(() => null), null, 'plugin MCP started');
        const reloaded = await q.reloadSkills();
        console.log(JSON.stringify({ mode, reloaded }));
        assert.deepEqual(await q.supportedCommands(), [], 'reload restored skill/command');
        await q.reloadPlugins();
        assert.deepEqual(await q.supportedCommands(), [], 'plugin reload restored skill/command');
        assert.equal(await fs.stat(pluginMarker).catch(() => null), null, 'plugin reload started MCP');
      }
    } finally {
      clearTimeout(timeout); finish(); q.close();
      await childClosed;
      for (const p of [marker, homeMarker, approvedMarker, pluginMarker, managedMarker]) await fs.rm(p, { force: true });
    }
  }
} finally { await fs.rm(root, { recursive: true, force: true }); }
