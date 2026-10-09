// Actual published Pi CLI + shipping ACP adapter + bundled Intent extension.
// Only the model HTTP endpoint and MCP bridge are deterministic local fixtures;
// this does not claim live-provider authentication or full-daemon coverage.
// PI_ACP_TEST_ENTRY=<adapter dist/index.js> PI_CLI_TEST_ENTRY=<pi dist/cli.js>
// PI_ACP_EVIDENCE_DIR=<artifact directory> node --test <this file>
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
import { assertCleanupComplete, assertNoUnexpectedStreamErrors, finishCleanup } from './pi-session-cleanup.mjs';
import diagnostics from './pi-diagnostics.cjs';
import { readWitnesses, stopWitnesses } from './pi-witness-harness.mjs';
const { Capture, failureReport, publishEvidence, summarizeStreamErrors } = diagnostics;
const observer = fileURLToPath(new URL('./pi-startup-witness.cjs', import.meta.url));

const windows = process.platform === 'win32';
assert.ok(process.env.PI_ACP_TEST_ENTRY && process.env.PI_CLI_TEST_ENTRY, 'Select the real adapter and Pi CLI');
const adapterEntry = resolve(process.env.PI_ACP_TEST_ENTRY);
const piEntry = resolve(process.env.PI_CLI_TEST_ENTRY);
const { ClientSideConnection, ndJsonStream } = await import(pathToFileURL(createRequire(adapterEntry).resolve('@agentclientprotocol/sdk')).href);
const launchEnv = Object.fromEntries(Object.entries(process.env).filter(([key]) =>
  ['path', 'systemroot', 'windir', 'comspec', 'pathext', 'temp', 'tmp'].includes(key.toLowerCase())));
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
const completedEvidence = new Map();
const expectedEvidence = new Map();
const runtimes = [];

for (const commandKind of windows ? ['bare', 'absolute'] : ['unix']) {
runtimes.push(test(`published adapter with real minimum Pi: lifecycle, models, thinking and MCP (${commandKind})`, { timeout: 180_000 }, async t => {
  const root = mkdtempSync(join(tmpdir(), 'intent-pi-runtime-'));
  const witnessRoot = join(root, 'witnesses');
  mkdirSync(witnessRoot);
  const clients = [];
  const sockets = new Set();
  const retiringSockets = new WeakSet();
  const requests = [];
  const bridgeRequests = [];
  const modelEvents = new EventEmitter();
  const cwd = join(root, 'workspace & (session) ^ 100% !PI_PATH_TOKEN! %PI_PATH_TOKEN%');
  const home = join(root, 'home & (session) ^ 100% !PI_PATH_TOKEN! %PI_PATH_TOKEN%');
  const agentDir = join(home, '.pi', 'agent');
  const runId = randomUUID();
  const evidence = { runId, root, platform: process.platform, node: process.version, adapterEntry, piEntry,
    commandKind, requests, bridgeRequests, streamErrors: [], clients: [], sessions: [], rpcLifecycle: [] };
  const retireSocket = socket => { retiringSockets.add(socket); socket.destroy(); };
  expectedEvidence.set(commandKind, { runId, root });
  let modelServer;
  let bridge;
  t.after(() => finishCleanup(evidence, {
    async stopClients() {
      const results = await Promise.allSettled(clients.map(client => client.stop()));
      const failures = results.filter(result => result.status === 'rejected');
      if (failures.length) throw new AggregateError(failures.map(result => result.reason));
    },
    async closeLifetime() {
      for (const socket of sockets) retireSocket(socket);
      modelServer?.closeAllConnections();
      await Promise.all([modelServer, bridge].filter(server => server?.listening).map(server =>
        bounded(new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve())), 'fixture server close')));
    },
  }, record => {
    record.streamErrorSummary = summarizeStreamErrors(record.streamErrors);
    if (process.env.PI_ACP_EVIDENCE_DIR) {
      mkdirSync(process.env.PI_ACP_EVIDENCE_DIR, { recursive: true });
      publishEvidence(join(process.env.PI_ACP_EVIDENCE_DIR, `runtime-${commandKind}.json`), record);
    }
    completedEvidence.set(commandKind, record);
    t.diagnostic(JSON.stringify({ commandKind, runId, root, cleanup: record.cleanup }));
  }));
  try {
    mkdirSync(cwd, { recursive: true });
    mkdirSync(agentDir, { recursive: true });
    bridge = createTcpServer(socket => {
      sockets.add(socket);
      socket.on('close', () => sockets.delete(socket));
      socket.on('error', error => evidence.streamErrors.push({
        stream: 'MCP socket', code: error.code, message: error.message,
        expected: retiringSockets.has(socket) && ['ECONNRESET', 'EPIPE'].includes(error.code),
      }));
      const lines = createInterface({ input: socket });
      lines.on('error', () => lines.close());
      lines.on('line', line => {
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
    const extension = join(home, 'intent extension & (a) ^ 100% !PI_PATH_TOKEN! %PI_PATH_TOKEN%.ts');
    copyFileSync(fileURLToPath(new URL('../../../intent-services/src/pi_mcp_extension.ts', import.meta.url)), extension);
    const wrapper = join(home, windows ? 'intent-pi.cmd' : 'pi');
    const realPi = join(home, 'pi.cmd');
    if (windows) {
      copyFileSync(fileURLToPath(new URL('../../../intent-services/src/pi_mcp_wrapper.cmd', import.meta.url)), wrapper);
      writeFileSync(realPi, '@echo off\r\nsetlocal DisableDelayedExpansion\r\n"%INTENTD_TEST_NODE%" "%INTENTD_TEST_OBSERVER%" %*\r\n');
    } else {
      writeFileSync(wrapper, `#!/bin/sh\nexec ${[process.execPath, observer, '-e', extension].map(quoteSh).join(' ')} "$@"\n`);
      chmodSync(wrapper, 0o755);
    }
    mkdirSync(join(agentDir, 'extensions'));
    writeFileSync(join(agentDir, 'extensions', 'user.ts'), 'export default pi => pi.registerTool({ name: "user_echo", label: "User echo", description: "User extension", parameters: { type: "object", properties: {} }, async execute() { return { content: [{ type: "text", text: "user extension" }] }; } });\n');
    const env = { ...launchEnv, HOME: home, USERPROFILE: home, PI_CODING_AGENT_DIR: agentDir,
      PI_ACP_PI_COMMAND: wrapper, INTENTD_MCP_BRIDGE_ADDR: `127.0.0.1:${bridge.address().port}`,
      INTENTD_PI_COMMAND: commandKind === 'bare' ? 'pi' : realPi, INTENTD_PI_EXTENSION: extension,
      INTENTD_TEST_NODE: process.execPath,
      INTENTD_TEST_OBSERVER: observer, PI_TEST_CHILD_ENTRY: piEntry, PI_TEST_RUN_ID: runId,
      PI_PATH_TOKEN: 'UNEXPECTED_EXPANSION', PATH: `${home}${windows ? ';' : ':'}${launchEnv.PATH ?? launchEnv.Path ?? ''}`,
      NODE_OPTIONS: '', NODE_DISABLE_COMPILE_CACHE: '1' };
    const version = spawnSync(process.execPath, [piEntry, '--version'], { env, encoding: 'utf8', timeout: 20_000 });
    assert.equal(version.status, 0, version.stderr);
    evidence.piVersion = version.stdout.trim();
    const config = readFileSync(fileURLToPath(new URL('../../../intent-providers/src/config.rs', import.meta.url)), 'utf8');
    assert.equal(evidence.piVersion, config.match(/pub const PI_CLI_MIN_VERSION: &str = "([^"]+)";/)[1]);
    const start = async () => {
      const directory = join(witnessRoot, String(clients.length));
      mkdirSync(directory);
      writeFileSync(join(directory, 'route'), 'initialize');
      const child = spawn(process.execPath, [adapterEntry], { cwd, env: { ...env, PI_TEST_WITNESS_DIR: directory }, stdio: ['pipe', 'pipe', 'pipe'], detached: !windows });
      const closed = once(child, 'close');
      closed.catch(() => {});
      const capture = new Capture();
      const record = { pid: child.pid, stderr: capture.snapshot(), calls: [], updates: [], permissions: [] };
      child.on('close', () => { record.stderr = capture.snapshot(true); });
      evidence.clients.push(record);
      child.stderr.on('data', data => { capture.push(data); record.stderr = capture.snapshot(); });
      const connection = new ClientSideConnection(() => ({
        async sessionUpdate(update) { record.updates.push(update); },
        async requestPermission(request) {
          record.permissions.push(request);
          const allow = request.options.find(option => option.kind === 'allow_once');
          assert.ok(allow, 'Tool call must offer one-time permission');
          return { outcome: { outcome: 'selected', optionId: allow.optionId } };
        },
      }), ndJsonStream(Writable.toWeb(child.stdin), Readable.toWeb(child.stdout)));
      let stopPromise;
      let activeRpc;
      const client = {
        record,
        async call(method, params) {
          writeFileSync(join(directory, 'route'), method);
          const call = { method, params };
          record.calls.push(call);
          try {
            const opensSession = method === 'newSession' || method === 'loadSession';
            const before = opensSession ? readWitnesses(directory) : [];
            const prior = before.find(child => child.invocationId === activeRpc?.invocationId);
            let priorLive = false;
            if (prior?.pid && !prior.close) {
              try { process.kill(prior.pid, 0); priorLive = true; }
              catch (error) { if (error.code !== 'ESRCH') throw error; }
            }
            call.result = await bounded(connection[method](params), method);
            if (opensSession) {
              const created = readWitnesses(directory).filter(child => child.argv.includes('rpc')
                && !before.some(previous => previous.invocationId === child.invocationId));
              assert.equal(created.length, 1, 'Successful session call must identify one actual RPC child');
              const lifecycle = { invocationId: created[0].invocationId, method,
                sessionId: method === 'newSession' ? call.result.sessionId : params.sessionId, callCompleted: true };
              // Preserve observed close.kind/code. This is independent call evidence,
              // not a fabricated signal: Windows shell disposal may leave Pi exiting0.
              if (activeRpc && priorLive) activeRpc.replacement = { invocationId: lifecycle.invocationId, priorLive };
              evidence.rpcLifecycle.push(lifecycle);
              activeRpc = lifecycle;
            } else if (method === 'prompt' && activeRpc?.sessionId === params.sessionId) {
              activeRpc.promptStopReason = call.result.stopReason;
            }
            return call.result;
          }
          catch (error) {
            call.error = failureReport(method, error, readWitnesses(directory));
            throw new Error(call.error);
          }
        },
        stop() {
          return stopPromise ??= (async () => {
            for (const socket of sockets) retiringSockets.add(socket);
            let witnessError;
            try { record.witnesses = await stopWitnesses(directory); } catch (error) { witnessError = error; }
            if (windows) {
              if (child.exitCode === null) {
                const killed = spawnSync('taskkill.exe', ['/PID', String(child.pid), '/T', '/F'], { timeout: 20_000 });
                assert.equal(killed.status, 0, `taskkill: ${killed.stderr}`);
              }
            } else {
              try { process.kill(-child.pid, 'SIGKILL'); } catch (error) { if (error.code !== 'ESRCH') throw error; }
            }
            await bounded(closed, 'adapter and real Pi tree shutdown');
            record.shutdown = { exitCode: child.exitCode, signalCode: child.signalCode };
            record.witnesses = readWitnesses(directory);
            if (witnessError) throw witnessError;
          })();
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
      (evidence.replay ??= []).push({ sessionId, text: `${label}:created` });
      // Exercise the bundled extension's reconnect path with the real Pi tool executor.
      for (const socket of sockets) retireSocket(socket);
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
    assert.ok(requests.every(request => request.body.tools.some(tool => tool.function.name === 'user_echo')), 'User extensions remain enabled');
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
    evidence.error = failureReport('runtime fixture', error, evidence.clients.flatMap(client => client.witnesses ?? []));
    throw new Error(evidence.error);
  }
}));
}

test('independent cleanup audit for real Pi compatibility', async () => {
  await Promise.all(runtimes);
  for (const [commandKind, expected] of expectedEvidence) {
    const record = process.env.PI_ACP_EVIDENCE_DIR
      ? JSON.parse(readFileSync(join(process.env.PI_ACP_EVIDENCE_DIR, `runtime-${commandKind}.json`), 'utf8')) : completedEvidence.get(commandKind);
    assertCleanupComplete(record, expected);
    assertNoUnexpectedStreamErrors(record);
    assert.equal(record.result, 'passed', record.error);
  }
});
