// Actual published Pi CLI + shipping ACP adapter + bundled Intent extension.
// Only the model HTTP endpoint and MCP bridge are deterministic local fixtures;
// this does not claim live-provider authentication or full-daemon coverage.
// PI_ACP_TEST_ENTRY=<adapter dist/index.js> PI_CLI_TEST_ENTRY=<pi dist/cli.js>
// PI_ACP_EVIDENCE_DIR=<artifact directory> node --test <this file>
// Unix only: Intent's production extension delivery requires a sh wrapper.
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { EventEmitter, once } from 'node:events';
import { chmodSync, copyFileSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { createServer as createHttpServer } from 'node:http';
import { createRequire } from 'node:module';
import { createServer as createTcpServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { createInterface } from 'node:readline';
import { Readable, Writable } from 'node:stream';
import test from 'node:test';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { assertCleanupComplete, finishCleanup } from './pi-session-cleanup.mjs';

assert.notEqual(process.platform, 'win32', 'Intent MCP extension delivery is Unix-only');
assert.ok(process.env.PI_ACP_TEST_ENTRY && process.env.PI_CLI_TEST_ENTRY, 'Select the real adapter and Pi CLI');
const adapterEntry = resolve(process.env.PI_ACP_TEST_ENTRY);
const piEntry = resolve(process.env.PI_CLI_TEST_ENTRY);
const { ClientSideConnection, ndJsonStream } = await import(pathToFileURL(createRequire(adapterEntry).resolve('@agentclientprotocol/sdk')).href);
const launchEnv = Object.fromEntries(Object.entries(process.env).filter(([key]) =>
  ['path', 'temp', 'tmp'].includes(key.toLowerCase())));
const quoteSh = value => `'${value.replaceAll("'", "'\\''")}'`;
function bounded(promise, description) {
  let timer;
  return Promise.race([promise, new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`Timeout: ${description}`)), 20_000);
  })]).finally(() => clearTimeout(timer));
}
const option = (result, id) => {
  const value = result.configOptions.find(option => option.id === id);
  assert.ok(value, `Missing config option ${id}`);
  return value;
};
const jsonLines = file => readFileSync(file, 'utf8').trim().split('\n').filter(Boolean).map(JSON.parse);
const filesUnder = root => readdirSync(root, { withFileTypes: true }).flatMap(entry => {
  const path = join(root, entry.name);
  return entry.isDirectory() ? filesUnder(path) : [path];
});
let evidence;
let expected;

const runtime = test('published adapter with real minimum Pi: lifecycle, models, thinking and MCP', { timeout: 180_000 }, async t => {
  const root = mkdtempSync(join(tmpdir(), 'intent-pi-runtime-'));
  const clients = [];
  const sockets = new Set();
  const requests = [];
  const bridgeRequests = [];
  const modelEvents = new EventEmitter();
  const cwd = join(root, 'workspace & (session)');
  const home = join(root, 'home');
  const agentDir = join(home, '.pi', 'agent');
  const runId = randomUUID();
  evidence = { runId, root, platform: process.platform, node: process.version, adapterEntry, piEntry,
    requests, bridgeRequests, clients: [], sessions: [] };
  expected = { runId, root };
  let modelServer;
  let bridge;
  t.after(() => finishCleanup(evidence, {
    async stopClients() {
      const results = await Promise.allSettled(clients.map(client => client.stop()));
      const failures = results.filter(result => result.status === 'rejected');
      if (failures.length) throw new AggregateError(failures.map(result => result.reason));
    },
    async closeLifetime() {
      for (const socket of sockets) socket.destroy();
      modelServer?.closeAllConnections();
      await Promise.all([modelServer, bridge].filter(server => server?.listening).map(server =>
        bounded(new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve())), 'fixture server close')));
    },
  }, record => {
    if (process.env.PI_ACP_EVIDENCE_DIR) {
      mkdirSync(process.env.PI_ACP_EVIDENCE_DIR, { recursive: true });
      writeFileSync(join(process.env.PI_ACP_EVIDENCE_DIR, 'runtime.json'), JSON.stringify(record, null, 2));
    }
    t.diagnostic(JSON.stringify({ runId, root, cleanup: record.cleanup }));
  }));
  try {
    mkdirSync(cwd, { recursive: true });
    mkdirSync(agentDir, { recursive: true });
    bridge = createTcpServer(socket => {
      sockets.add(socket);
      socket.on('close', () => sockets.delete(socket));
      socket.on('error', () => {});
      createInterface({ input: socket }).on('line', line => {
        const message = JSON.parse(line);
        bridgeRequests.push(message);
        if (message.id === undefined) return;
        let result;
        switch (message.method) {
          case 'initialize': result = { protocolVersion: '2024-11-05', capabilities: { tools: {} }, serverInfo: { name: 'pi-runtime-fixture', version: '1' } }; break;
          case 'tools/list': result = { tools: [{ name: 'intent_echo', description: 'Echo the input', inputSchema: {
            type: 'object', properties: { input: { type: 'string' } }, required: ['input'],
          } }] }; break;
          case 'tools/call': result = { content: [{ type: 'text', text: `echo:${message.params.arguments.input}` }] }; break;
          default: socket.write(JSON.stringify({ jsonrpc: '2.0', id: message.id, error: { code: -32601, message: 'Unexpected method' } }) + '\n'); return;
        }
        socket.write(JSON.stringify({ jsonrpc: '2.0', id: message.id, result }) + '\n');
      });
    });
    bridge.listen(0, '127.0.0.1');
    await bounded(once(bridge, 'listening'), 'MCP bridge listen');
    modelServer = createHttpServer(async (request, response) => {
      const body = JSON.parse(await Array.fromAsync(request).then(chunks => Buffer.concat(chunks).toString()));
      requests.push({ path: request.url, body });
      response.writeHead(200, { 'Content-Type': 'text/event-stream' });
      const prompt = body.messages.filter(message => message.role === 'user').at(-1)?.content;
      const text = typeof prompt === 'string' ? prompt : prompt?.map(part => part.text ?? '').join('');
      const send = (delta, finish_reason = null) => response.write(`data: ${JSON.stringify({ id: 'local-completion', object: 'chat.completion.chunk', model: body.model, choices: [{ index: 0, delta, finish_reason }] })}\n\n`);
      send({ role: 'assistant' });
      if (text?.endsWith(':cancel')) {
        response.once('close', () => modelEvents.emit('cancelled'));
        modelEvents.emit('pending');
        return;
      }
      if (body.messages.at(-1)?.role !== 'tool') {
        send({ tool_calls: [{ index: 0, id: 'echo-call', type: 'function', function: { name: 'intent_echo', arguments: JSON.stringify({ input: text }) } }] });
        send({}, 'tool_calls');
      } else {
        send({ content: `answer:${text}` });
        send({}, 'stop');
      }
      response.end('data: [DONE]\n\n');
    });
    modelServer.listen(0, '127.0.0.1');
    await bounded(once(modelServer, 'listening'), 'model endpoint listen');
    writeFileSync(join(agentDir, 'models.json'), JSON.stringify({ providers: { local: {
      baseUrl: `http://127.0.0.1:${modelServer.address().port}/v1`, api: 'openai-completions', apiKey: 'local-fixture',
      compat: { supportsDeveloperRole: false, supportsReasoningEffort: true },
      models: [{ id: 'reasoner', reasoning: true }, { id: 'plain', reasoning: false }],
    } } }));
    const extension = join(root, 'intent extension.ts');
    copyFileSync(fileURLToPath(new URL('../../../intent-services/src/pi_mcp_extension.ts', import.meta.url)), extension);
    const wrapper = join(root, 'pi');
    writeFileSync(wrapper, `#!/bin/sh\nexec ${[process.execPath, piEntry, '-e', extension].map(quoteSh).join(' ')} "$@"\n`);
    chmodSync(wrapper, 0o755);
    const env = { ...launchEnv, HOME: home, USERPROFILE: home, PI_CODING_AGENT_DIR: agentDir,
      PI_ACP_PI_COMMAND: wrapper, INTENTD_MCP_BRIDGE_ADDR: `127.0.0.1:${bridge.address().port}`,
      NODE_OPTIONS: '', NODE_DISABLE_COMPILE_CACHE: '1' };
    const version = spawnSync(process.execPath, [piEntry, '--version'], { env, encoding: 'utf8', timeout: 20_000 });
    assert.equal(version.status, 0, version.stderr);
    evidence.piVersion = version.stdout.trim();
    const config = readFileSync(fileURLToPath(new URL('../../../intent-providers/src/config.rs', import.meta.url)), 'utf8');
    assert.equal(evidence.piVersion, config.match(/pub const PI_CLI_MIN_VERSION: &str = "([^"]+)";/)[1]);
    const start = async () => {
      const child = spawn(process.execPath, [adapterEntry], { cwd, env, stdio: ['pipe', 'pipe', 'pipe'], detached: true });
      const closed = once(child, 'close');
      closed.catch(() => {});
      const record = { pid: child.pid, stderr: '', calls: [], updates: [], permissions: [] };
      evidence.clients.push(record);
      child.stderr.on('data', data => { record.stderr += data; });
      const connection = new ClientSideConnection(() => ({
        async sessionUpdate(update) { record.updates.push(update); },
        async requestPermission(request) {
          record.permissions.push(request);
          const allow = request.options.find(option => option.kind === 'allow_once');
          assert.ok(allow, 'Tool call must offer one-time permission');
          return { outcome: { outcome: 'selected', optionId: allow.optionId } };
        },
      }), ndJsonStream(Writable.toWeb(child.stdin), Readable.toWeb(child.stdout)));
      let stopped = false;
      const client = {
        record,
        async call(method, params) {
          const call = { method, params };
          record.calls.push(call);
          try { call.result = await bounded(connection[method](params), method); return call.result; }
          catch (error) { call.error = String(error); throw error; }
        },
        async stop() {
          if (stopped) return;
          try { process.kill(-child.pid, 'SIGKILL'); } catch (error) { if (error.code !== 'ESRCH') throw error; }
          await bounded(closed, 'adapter and real Pi tree shutdown');
          stopped = true;
        },
      };
      clients.push(client);
      const init = await client.call('initialize', { protocolVersion: 1, clientCapabilities: {}, clientInfo: { name: 'intent-runtime-test', version: '1' } });
      assert.equal(init.protocolVersion, 1);
      assert.equal(init.agentCapabilities.loadSession, true);
      assert.equal(`pi-acp@${init.agentInfo.version}`, config.match(/pub const PI_ACP_NPX_PACKAGE: &str = "([^"]+)";/)[1]);
      return client;
    };
    let client = await start();
    for (const label of ['first', 'second']) {
      // Pi persists model/thinking selections as defaults. Reset only the
      // isolated settings so each new session exercises the off-only default.
      writeFileSync(join(agentDir, 'settings.json'), JSON.stringify({ defaultProvider: 'local', defaultModel: 'plain', defaultThinkingLevel: 'medium' }));
      const created = await client.call('newSession', { cwd, mcpServers: [] });
      const sessionId = created.sessionId;
      evidence.sessions.push({ sessionId, label });
      assert.deepEqual(option(created, 'model').options.map(model => model.value).sort(), ['local/plain', 'local/reasoner']);
      assert.equal(option(created, 'model').currentValue, 'local/plain');
      assert.deepEqual(option(created, 'thought_level').options.map(level => level.value), ['off']);
      const set = (configId, value) => client.call('setSessionConfigOption', { sessionId, configId, value });
      const plain = await set('model', 'local/plain');
      assert.equal(option(plain, 'model').currentValue, 'local/plain');
      assert.deepEqual(option(plain, 'thought_level').options.map(level => level.value), ['off']);
      assert.equal(option(plain, 'thought_level').currentValue, 'off');
      const reasoner = await set('model', 'local/reasoner');
      assert.deepEqual(option(reasoner, 'thought_level').options.map(level => level.value), ['off', 'minimal', 'low', 'medium', 'high']);
      assert.equal(option(reasoner, 'thought_level').currentValue, 'medium');
      assert.equal(option(await set('thought_level', 'high'), 'thought_level').currentValue, 'high');
      const prompt = `${label}:created`;
      assert.equal((await client.call('prompt', { sessionId, prompt: [{ type: 'text', text: prompt }] })).stopReason, 'end_turn');
      assert.ok(client.record.updates.some(event => event.sessionId === sessionId && event.update.content?.text === `answer:${prompt}`));
    }
    assert.notEqual(evidence.sessions[0].sessionId, evidence.sessions[1].sessionId);
    await client.stop();
    client = await start();
    for (const { sessionId, label } of evidence.sessions) {
      const updateStart = client.record.updates.length;
      await client.call('loadSession', { sessionId, cwd, mcpServers: [] });
      const replay = client.record.updates.slice(updateStart).filter(event => event.update.sessionUpdate === 'user_message_chunk');
      assert.deepEqual(replay.map(event => event.update.content.text), [`${label}:created`]);
      // Exercise the bundled extension's reconnect path with the real Pi tool executor.
      for (const socket of sockets) socket.destroy();
      assert.equal((await client.call('prompt', { sessionId, prompt: [{ type: 'text', text: `${label}:resumed` }] })).stopReason, 'end_turn');
    }
    const { sessionId } = evidence.sessions[0];
    const pending = bounded(once(modelEvents, 'pending'), 'real Pi model request');
    const cancelled = bounded(once(modelEvents, 'cancelled'), 'real Pi HTTP cancellation');
    const prompt = client.call('prompt', { sessionId, prompt: [{ type: 'text', text: 'first:cancel' }] });
    prompt.catch(() => {});
    cancelled.catch(() => {});
    await pending;
    await client.call('cancel', { sessionId });
    assert.equal((await prompt).stopReason, 'cancelled');
    await cancelled;
    await client.stop();
    const calls = bridgeRequests.filter(request => request.method === 'tools/call');
    assert.deepEqual(calls.map(call => [call.params.name, call.params.arguments.input]), [
      ['intent_echo', 'first:created'], ['intent_echo', 'second:created'],
      ['intent_echo', 'first:resumed'], ['intent_echo', 'second:resumed'],
    ]);
    assert.ok(bridgeRequests.filter(request => request.method === 'initialize').length >= 4, 'MCP reconnects handshake again');
    assert.ok(requests.every(request => request.path === '/v1/chat/completions' && request.body.model === 'reasoner' && request.body.reasoning_effort === 'high'));
    assert.ok(requests.some(request => request.body.tools.some(tool => tool.function.name === 'intent_echo')), 'Real extension registers the tool with Pi');
    assert.ok(requests.some(request => request.body.messages.some(message => message.role === 'tool' && message.content.includes('echo:first:created'))), 'MCP output reaches the real Pi model conversation');
    evidence.histories = filesUnder(join(agentDir, 'sessions')).filter(file => file.endsWith('.jsonl')).map(file => ({ file, entries: jsonLines(file) }));
    assert.equal(evidence.histories.length, 2);
    for (const { sessionId, label } of evidence.sessions) {
      const history = evidence.histories.find(history => history.entries[0].id === sessionId);
      assert.ok(history, 'Each real Pi session has its own file');
      const users = history.entries.filter(entry => entry.message?.role === 'user').map(entry => entry.message.content[0].text);
      assert.deepEqual(users, [`${label}:created`, `${label}:resumed`, ...(label === 'first' ? ['first:cancel'] : [])]);
    }
    evidence.result = 'passed';
  } catch (error) {
    evidence.result = 'failed';
    evidence.error = String(error.stack ?? error);
    throw error;
  }
});

test('independent cleanup audit for real Pi compatibility', async () => {
  await runtime;
  const record = process.env.PI_ACP_EVIDENCE_DIR
    ? JSON.parse(readFileSync(join(process.env.PI_ACP_EVIDENCE_DIR, 'runtime.json'), 'utf8')) : evidence;
  assertCleanupComplete(record, expected);
});
