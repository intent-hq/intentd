// Public acquisition -> prepared npx command -> real ACP, synthetic loopback API only.
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import assert from 'node:assert/strict';
import readline from 'node:readline';
import { spawn } from 'node:child_process';
import { createRequire } from 'node:module';

const modules = path.resolve(process.argv[2]);
const harness = path.resolve(process.argv[3]), testName = process.argv[4];
const native = path.join(modules, '@anthropic-ai/claude-agent-sdk-linux-x64/claude');
for (const [pkg, version] of [['@agentclientprotocol/claude-agent-acp', '0.81.1'], ['@anthropic-ai/claude-agent-sdk', '0.3.280']]) {
  assert.equal(JSON.parse(await fs.readFile(path.join(modules, pkg, 'package.json'))).version, version);
}
const root = await fs.mkdtemp(path.join(os.tmpdir(), 'intent-acquired-acp-'));
const home = path.join(root, 'home'), cwd = path.join(root, 'repo'), state = path.join(root, 'state');
const admin = path.join(root, 'admin'), bin = path.join(root, 'bin');
for (const dir of [home, cwd, state, admin, bin, path.join(root, "tmp")]) await fs.mkdir(dir, { mode: 0o700 });
await fs.mkdir(path.join(home, ".claude"), { mode: 0o700 });
await fs.symlink(native, path.join(bin, 'claude'));
const cache = path.join(root, 'npm'), packageRoot = path.dirname(modules);
const cacheEntry = path.join(cache, '_npx', path.basename(packageRoot));
await fs.mkdir(cacheEntry, { recursive: true });
for (const file of ['package.json', 'package-lock.json']) await fs.copyFile(path.join(packageRoot, file), path.join(cacheEntry, file));
await fs.symlink(modules, path.join(cacheEntry, 'node_modules'));
const npmRoot = path.resolve(path.dirname(await fs.realpath('/usr/bin/npx')), '..');
const cacache = createRequire(import.meta.url)(path.join(npmRoot, 'node_modules/cacache'));
const manifestKey = 'make-fetch-happen:request-cache:https://registry.npmjs.org/@agentclientprotocol%2fclaude-agent-acp';
const manifest = await cacache.get(path.resolve(modules, '../../..', '_cacache'), manifestKey);
await cacache.put(path.join(cache, '_cacache'), manifestKey, manifest.data, {metadata: manifest.metadata});

const write = async (p, text) => { await fs.mkdir(path.dirname(p), { recursive: true }); await fs.writeFile(p, text); };
const mcpScript = path.join(root, 'mcp.mjs');
await write(mcpScript, `import fs from 'node:fs';import readline from 'node:readline';
fs.appendFileSync(process.argv[2], 'start\\n');
for await(const line of readline.createInterface({input:process.stdin})) {
 const m=JSON.parse(line);if(m.id===undefined)continue;
 let result={};
 if(m.method==='initialize')result={protocolVersion:'2024-11-05',capabilities:{tools:{}},serverInfo:{name:'fixture',version:'1'}};
 if(m.method==='tools/list')result={tools:[{name:'echo',description:'Owned fixture echo',inputSchema:{type:'object',properties:{}}}]};
 if(m.method==='tools/call'){fs.appendFileSync(process.argv[2],'call\\n');result={content:[{type:'text',text:'OWNED-TOOL-RESULT'}]};}
 console.log(JSON.stringify({jsonrpc:'2.0',id:m.id,result}));
}
`);
const mcp = name => ({ command: process.execPath, args: [mcpScript, path.join(root, name)] });
await write(path.join(home, '.claude.json'), JSON.stringify({ mcpServers: { host: mcp('host') } }));
await write(path.join(cwd, '.mcp.json'), JSON.stringify({ mcpServers: { ambient: mcp('ambient') } }));
await write(path.join(cwd, '.claude/skills/ambient/SKILL.md'), '---\nname: ambient\ndescription: AMBIENT-SKILL-MARKER\n---\nNever load.');
await write(path.join(home, '.claude/skills/host/SKILL.md'), '---\nname: host\ndescription: HOST-SKILL-MARKER\n---\nNever load.');
await write(path.join(root, 'CLAUDE.md'), 'ANCESTOR-INSTRUCTIONS-SENTINEL');
await write(path.join(cwd, 'CLAUDE.md'), 'ROOT-INSTRUCTIONS-SENTINEL\n@docs/instructions.md');
await write(path.join(cwd, '.claude/CLAUDE.md'), 'PROJECT-CLAUDE-INSTRUCTIONS-SENTINEL\n@../docs/instructions.md\n`@../docs/ignored.md`');
await write(path.join(cwd, 'CLAUDE.local.md'), 'LOCAL-INSTRUCTIONS-SENTINEL');
await write(path.join(cwd, 'docs/instructions.md'), 'IMPORTED-INSTRUCTIONS-SENTINEL\n@nested.md');
await write(path.join(cwd, 'docs/nested.md'), 'NESTED-INSTRUCTIONS-SENTINEL\n@instructions.md');
await write(path.join(cwd, 'docs/ignored.md'), 'CODE-IMPORT-MUST-NOT-LOAD');
await write(path.join(cwd, '.claude/rules/tests.md'), 'RULES-INSTRUCTIONS-SENTINEL');
const instructionMarkers = ['ANCESTOR','ROOT','PROJECT-CLAUDE','LOCAL','IMPORTED','NESTED','RULES'].map(s=>s+'-INSTRUCTIONS-SENTINEL');
await write(path.join(cwd, 'approved.json'), JSON.stringify({ approved: mcp('approved'), 'workspace-mcp': mcp('bridge') }));
const seen = [];
let shouldCallTool = false;
const server = http.createServer(async (req, res) => {
  let raw = ''; for await (const chunk of req) raw += chunk;
  if (!req.url.startsWith('/v1/messages') || req.url.includes('count_tokens')) { res.writeHead(200, { 'content-type': 'application/json' }); res.end('{"input_tokens":1}'); return; }
  const body = JSON.parse(raw); seen.push({ body, key: req.headers['x-api-key'] });
  const tool = shouldCallTool && (body.tools ?? []).find(t => t.name === 'mcp__approved__echo');
  shouldCallTool = false;
  const block = tool ? { type: 'tool_use', id: 'tool_fixture', name: tool.name, input: {} } : { type: 'text', text: '' };
  const events = [
    ['message_start', { type: 'message_start', message: { id: 'msg_fixture', type: 'message', role: 'assistant', model: body.model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 1, output_tokens: 0 } } }],
    ['content_block_start', { type: 'content_block_start', index: 0, content_block: block }],
    ...(!tool ? [['content_block_delta', { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: 'Fixture complete.' } }]] : []),
    ['content_block_stop', { type: 'content_block_stop', index: 0 }],
    ['message_delta', { type: 'message_delta', delta: { stop_reason: tool ? 'tool_use' : 'end_turn', stop_sequence: null }, usage: { output_tokens: 1 } }],
    ['message_stop', { type: 'message_stop' }],
  ];
  res.writeHead(200, { 'content-type': 'text/event-stream' });
  for (const [event, data] of events) res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  res.end();
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const env = { PATH: `${bin}:/usr/bin:/bin`, HOME: home, USERPROFILE: home, SHELL: '/bin/false', TMPDIR: path.join(root, 'tmp'), CLAUDE_CONFIG_DIR: path.join(home, '.claude'),
  ANTHROPIC_API_KEY: 'synthetic-acp-key', ANTHROPIC_BASE_URL: `http://127.0.0.1:${server.address().port}`,
  CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: '1', DISABLE_AUTOUPDATER: '1',
  npm_config_cache: cache, npm_config_offline: 'true',
  INTENT_ACP_FIXTURE_CHILD: '1', INTENT_ACP_FIXTURE_STATE: state,
};
const namespace = ['--unshare-user', '--die-with-parent', '--ro-bind', '/', '/', '--dev', '/dev', '--proc', '/proc',
  '--bind', root, root, '--tmpfs', '/etc', '--ro-bind', '/etc/passwd', '/etc/passwd', '--ro-bind', '/etc/group', '/etc/group', '--ro-bind', admin, '/etc/claude-code', '--'];
async function acquired(ephemeral) {
  const child = spawn('bwrap', [...namespace, harness, '--exact', testName, '--ignored', '--nocapture'], {
    cwd, env: { ...env, ...(ephemeral ? { INTENT_ACP_FIXTURE_EPHEMERAL: '1' } : {}) }, stdio: ['pipe', 'pipe', 'pipe'],
  });
  let stderr = ''; child.stderr.on('data', chunk => stderr += chunk);
  const closed = new Promise(resolve => child.once('close', resolve));
  const launch = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => { child.kill(); reject(new Error('acquisition timed out: ' + stderr)); }, 30000);
    readline.createInterface({ input: child.stdout }).on('line', line => {
      if (line.startsWith('LAUNCH:')) { clearTimeout(timer); resolve(JSON.parse(line.slice(7))); }
    });
    child.once('close', code => { clearTimeout(timer); reject(new Error(`acquisition exited ${code}: ${stderr}`)); });
  });
  return { launch, release: async () => { child.stdin.end('\n'); assert.equal(await closed, 0, stderr); } };
}
async function adapter(launch, negative = false) {
  if (!negative) assert.equal(await fs.realpath(launch.env.CLAUDE_CODE_EXECUTABLE), await fs.realpath(native));
  const child = spawn('bwrap', [...namespace, launch.program, ...launch.args], {
    cwd: launch.cwd, env: launch.env, stdio: ['pipe', 'pipe', 'pipe'], detached: true,
  });
  const closed = new Promise(resolve => child.once('close', resolve));
  const pending = new Map(); let nextId = 0, stderr = '';
  child.stderr.on('data', data => stderr += data);
  child.once('close', code => { for (const p of pending.values()) { clearTimeout(p.timer); p.reject(new Error(`adapter exited ${code}: ${stderr}`)); } pending.clear(); });
  readline.createInterface({ input: child.stdout }).on('line', line => {
    const msg = JSON.parse(line);
    if (msg.method && msg.id !== undefined) {
      const option = msg.params?.options?.find(o => o.kind === 'allow_once' || o.kind === 'allow_always');
      child.stdin.write(JSON.stringify({jsonrpc:'2.0',id:msg.id,result:option ? {outcome:{outcome:'selected',optionId:option.optionId}} : {}})+'\n');
    } else if (pending.has(msg.id)) {
      const { resolve, reject, timer } = pending.get(msg.id); pending.delete(msg.id); clearTimeout(timer);
      if (msg.error) reject(new Error(JSON.stringify(msg.error))); else resolve(msg.result);
    }
  });
  const call = (method, params) => new Promise((resolve, reject) => {
    const id = ++nextId;
    const timer = setTimeout(() => reject(new Error(`${method} timed out: ${stderr.slice(-2500)}`)), 30000);
    pending.set(id, { resolve, reject, timer });
    child.stdin.write(JSON.stringify({jsonrpc:'2.0',id,method,params})+'\n');
  });
  const assertNative = async () => {
    const expected = await fs.realpath(native), pending = [child.pid];
    let found = false;
    for (let visited = 0; pending.length && visited < 128; visited++) {
      const pid = pending.pop();
      if (await fs.readlink(`/proc/${pid}/exe`).catch(() => null) === expected) found = true;
      const children = await fs.readFile(`/proc/${pid}/task/${pid}/children`, 'utf8').catch(() => '');
      pending.push(...children.trim().split(/\s+/).filter(Boolean).map(Number));
    }
    assert(found, 'ACP did not execute the exact acquired native binary');
  };
  return { call, assertNative, stop: async () => {
    for (const p of pending.values()) clearTimeout(p.timer);
    try { process.kill(-child.pid, 'SIGKILL'); } catch (e) { if (e.code !== 'ESRCH') throw e; }
    await closed;
  } };
}
const bridge = path.join(bin, 'intent-fixture-bridge');
await write(bridge, `#!/usr/bin/node
const net=require('net');const addr=process.argv[process.argv.indexOf('--connect')+1];const split=addr.lastIndexOf(':');
const sock=net.connect(Number(addr.slice(split+1)),addr.slice(0,split));process.stdin.pipe(sock);sock.pipe(process.stdout);
sock.on('error',()=>process.exit(1));sock.on('close',()=>process.exit(0));
`);
await fs.chmod(bridge, 0o700);
async function managerFixture() {
  const before = seen.length;
  const child = spawn('bwrap', [...namespace, harness, '--exact', 'agent_manager::tests::managed_interactive_native_lifecycle', '--ignored', '--nocapture'], {
    cwd, env: { ...env, INTENT_MANAGED_SESSION_FIXTURE:'1', INTENT_MANAGED_BRIDGE:bridge }, stdio:['ignore','pipe','pipe'],
  });
  let stdout='', stderr=''; child.stdout.on('data', x=>stdout+=x); child.stderr.on('data', x=>stderr+=x);
  const code = await new Promise(resolve=>child.once('close',resolve));
  assert.equal(code,0,stdout+'\n'+stderr);
  assert(stdout.includes('PASS actual AgentManager'));
  const requests = seen.slice(before);
  // Native Claude also sends tool-free warmup/title requests. Check isolation
  // on every request; the two actual conversation turns must carry our bridge.
  const turns = [];
  for(const {body,key} of requests) {
    assert.equal(key,env.ANTHROPIC_API_KEY); assert.equal(body.model,'claude-sonnet-4-6');
    const tools=(body.tools??[]).map(t=>t.name); assert.equal(new Set(tools).size,tools.length);
    assert(!tools.some(t=>t.startsWith('mcp__ambient__')||t.startsWith('mcp__host__')));
    assert(!JSON.stringify(body.system).includes('HOST-SKILL-MARKER'));
    const content=body.messages?.at(-1)?.content;
    if(Array.isArray(content) && content.some(b=>['MANAGED-FIRST-TURN','MANAGED-RESUMED-TURN'].includes(b.text))) {
      turns.push(body);
      assert(tools.some(t=>t.startsWith('mcp__workspace-mcp__')));
      assert(JSON.stringify(body.system).includes('AMBIENT-SKILL-MARKER'));
      for (const marker of instructionMarkers) assert.equal(JSON.stringify(body.system).split(marker).length-1, 1, `managed new/load must preserve ${marker} once`);
      assert(!JSON.stringify(body.system).includes('CODE-IMPORT-MUST-NOT-LOAD'));
    }
  }
  assert.equal(turns.length,2,'both actual conversation turns must reach the model');
  assert(JSON.stringify(turns.at(-1).messages).includes('MANAGED-FIRST-TURN'));
  for(const name of ['host','ambient']) assert.equal(await fs.stat(path.join(root,name)).catch(()=>null),null);
  console.log('PASS actual AgentManager new/load/respawn and effective catalog');
}
let active, held;
try {
  let sessionId;
  for (const mode of ['new', 'load', 'ephemeral']) {
    held = await acquired(mode === 'ephemeral');
    active = await adapter(held.launch);
    await active.call('initialize', { protocolVersion: 1, clientInfo: { name: 'intent-fixture', version: '1' }, clientCapabilities: {} });
    const params = { cwd, mcpServers: held.launch.servers, _meta: held.launch.meta };
    if (mode === 'load') await active.call('session/load', { ...params, sessionId });
    else sessionId = (await active.call('session/new', params)).sessionId;
    await active.assertNative();
    const before = seen.length;
    shouldCallTool = mode === 'new';
    await active.call('session/prompt', { sessionId, prompt: [{ type: 'text', text: mode === 'load' ? 'RESUMED-TURN' : 'INITIAL-TURN' }] });
    assert(seen.length > before, `${mode} made no model request`);
    for (const {body,key} of seen.slice(before)) {
      assert.equal(key, env.ANTHROPIC_API_KEY);
      assert.equal(body.model, 'claude-sonnet-4-6');
      assert(JSON.stringify(body.system).includes('OWNED-INTERACTIVE-INSTRUCTIONS'));
      assert(!JSON.stringify(body.system).includes('AMBIENT-SKILL-MARKER'));
      const tools = (body.tools ?? []).map(t => t.name);
      if (mode === 'ephemeral') assert.deepEqual(tools, []);
      else {
        assert.equal(tools.filter(t => t === 'mcp__workspace-mcp__echo').length, 1);
        assert.equal(tools.filter(t => t === 'mcp__approved__echo').length, 1);
      }
      if (mode === 'load') assert(JSON.stringify(body.messages).includes('INITIAL-TURN'), 'respawn lost native history');
    }
    if (mode === 'new') assert((await fs.readFile(path.join(root, 'approved'), 'utf8')).includes('call\n'), 'approved tool did not execute');
    for (const name of ['host', 'ambient']) assert.equal(await fs.stat(path.join(root, name)).catch(() => null), null);
    await active.stop(); active = null;
    await held.release(); held = null;
    console.log(`PASS acquired ACP ${mode}`);
  }
  held = await acquired(true);
  active = await adapter({ ...held.launch, env: { ...held.launch.env, CLAUDE_CODE_EXECUTABLE: path.join(root, 'missing-native') } }, true);
  await active.call('initialize', { protocolVersion: 1, clientInfo: { name: 'intent-fixture', version: '1' }, clientCapabilities: {} });
  await assert.rejects(active.call('session/new', { cwd, mcpServers: [], _meta: held.launch.meta }));
  await active.stop(); active = null;
  await held.release(); held = null;
  console.log('PASS acquired executable cannot fall back to SDK bundled runtime');
  await managerFixture();
  // Control arm: prove these ordinary sources reach the same pinned native
  // runtime with its original source discovery, using synthetic auth only.
  held = await acquired(true);
  active = await adapter(held.launch);
  await active.call('initialize', {protocolVersion:1,clientInfo:{name:'instructions-control',version:'1'},clientCapabilities:{}});
  const nativeMeta = structuredClone(held.launch.meta);
  nativeMeta.systemPrompt = 'EXISTING-INTERACTIVE-SYSTEM-PROMPT';
  nativeMeta.claudeCode.options.settingSources = ['user','project','local'];
  const nativeSession = (await active.call('session/new', {cwd,mcpServers:[],_meta:nativeMeta})).sessionId;
  const beforeNative = seen.length;
  await active.call('session/prompt', {sessionId:nativeSession,prompt:[{type:'text',text:'INSTRUCTION-CONTROL-TURN'}]});
  const nativeInput = JSON.stringify(seen.slice(beforeNative));
  for (const marker of instructionMarkers) assert(nativeInput.includes(marker), `native baseline must load ${marker}`);
  assert(!nativeInput.includes('CODE-IMPORT-MUST-NOT-LOAD'));
  await active.stop(); active = null;
  await held.release(); held = null;
  console.log('PASS managed new/load instruction sources match pinned native baseline');
  console.log('PASS acquired ACP new/load/respawn and ephemeral inventory');
} finally {
  if (active) await active.stop();
  if (held) await held.release();
  server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
  await fs.rm(root, { recursive: true, force: true });
}
