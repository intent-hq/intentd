// Regression against the actual published adapter, through its public ACP wire.
// PI_ACP_TEST_ENTRY=<pi-acp/dist/index.js> PI_ACP_EVIDENCE_DIR=<directory>
// node --test --test-reporter=tap <this file>
// Expected RED on pi-acp@0.0.34: both startup errors lose child stderr/status.
// The witness, successful-session controls, and cleanup audit must still pass.
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { createInterface } from 'node:readline';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { assertCleanupComplete, finishCleanup } from './pi-session-cleanup.mjs';

assert.ok(process.env.PI_ACP_TEST_ENTRY, 'Select the actual published adapter');
const adapterEntry = resolve(process.env.PI_ACP_TEST_ENTRY);
const fixture = name => fileURLToPath(new URL(name, import.meta.url));
const config = readFileSync(fixture('../../../intent-providers/src/config.rs'), 'utf8');
const pin = config.match(/pub const PI_ACP_NPX_PACKAGE: &str = "([^"]+)";/)[1];
const windows = process.platform === 'win32';
const marker = 'PI_STARTUP_SENTINEL: deterministic child initialization failure';
const quoteSh = value => `'${value.replaceAll("'", "'\\''")}'`;
const launchEnv = Object.fromEntries(Object.entries(process.env).filter(([key]) =>
  ['path', 'systemroot', 'windir', 'comspec', 'pathext', 'temp', 'tmp'].includes(key.toLowerCase())));
const jsonLines = path => existsSync(path)
  ? readFileSync(path, 'utf8').trim().split('\n').filter(Boolean).map(JSON.parse) : [];
function bounded(promise, description) {
  let timer;
  return Promise.race([promise, new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`Timeout: ${description}`)), 10_000);
  })]).finally(() => clearTimeout(timer));
}

function startAdapter(cwd, env, records) {
  const child = spawn(process.execPath, [adapterEntry], { cwd, env, stdio: 'pipe', detached: !windows });
  const closed = once(child, 'close');
  closed.catch(() => {});
  const record = { pid: child.pid, transcript: [], stderr: '' };
  records.push(record);
  const pending = new Map();
  let nextId = 1;
  let failure;
  const fail = error => {
    failure = error;
    for (const p of pending.values()) p.reject(error);
    pending.clear();
  };
  child.on('error', fail);
  child.stdin.on('error', fail);
  child.on('exit', (code, signal) => fail(new Error(`Adapter exited: ${code}/${signal}`)));
  child.stderr.on('data', chunk => { record.stderr += chunk; });
  const lines = createInterface({ input: child.stdout });
  lines.on('line', line => {
    let message;
    try { message = JSON.parse(line); } catch { fail(new Error(`Non-JSON ACP output: ${line}`)); return; }
    record.transcript.push({ direction: 'received', message });
    if (message.method) {
      if (message.id !== undefined) fail(new Error(`Unexpected client request: ${line}`));
      return;
    }
    const waiter = pending.get(message.id);
    if (!waiter) { fail(new Error(`Unexpected ACP response: ${line}`)); return; }
    pending.delete(message.id);
    waiter.resolve(message);
  });
  let stopping;
  return {
    record,
    async request(method, params) {
      if (failure) throw failure;
      const id = nextId++;
      const message = { jsonrpc: '2.0', id, method, params };
      record.transcript.push({ direction: 'sent', message });
      const response = new Promise((resolve, reject) => pending.set(id, { resolve, reject }));
      child.stdin.write(JSON.stringify(message) + '\n');
      return bounded(response, method).finally(() => pending.delete(id));
    },
    stop() {
      return stopping ??= (async () => {
        if (windows) {
          if (child.exitCode === null && child.signalCode === null) {
            const killed = spawnSync('taskkill.exe', ['/PID', String(child.pid), '/T', '/F'], { timeout: 10_000 });
            assert.equal(killed.status, 0, `taskkill: ${killed.stderr}`);
          }
        } else {
          try { process.kill(-child.pid, 'SIGKILL'); } catch (error) { if (error.code !== 'ESRCH') throw error; }
        }
        await bounded(closed, 'adapter tree shutdown');
        record.forcedAdapterShutdown = { code: child.exitCode, signal: child.signalCode };
        lines.close();
      })();
    },
  };
}

const completed = new Map();
const expected = new Map();
const runs = [];
for (const route of ['new', 'load']) {
  runs.push(test(`early Pi exit during session/${route}`, { timeout: 60_000 }, async t => {
    const root = mkdtempSync(join(tmpdir(), 'intent-pi-startup-'));
    const runId = randomUUID();
    const evidence = { root, runId, route, platform: process.platform, node: process.version, pin, adapterEntry, clients: [] };
    expected.set(route, { root, runId });
    const clients = [];
    const sockets = new Set();
    const lifetime = createServer(socket => {
      sockets.add(socket);
      socket.on('error', () => {});
      socket.on('close', () => sockets.delete(socket));
    });
    t.after(() => finishCleanup(evidence, {
      async stopClients() {
        const results = await Promise.allSettled(clients.map(client => client.stop()));
        const failures = results.filter(result => result.status === 'rejected');
        if (failures.length) throw new AggregateError(failures.map(result => result.reason));
      },
      async closeLifetime() {
        await bounded(Promise.all([...sockets].map(async socket => {
          const closed = once(socket, 'close');
          socket.end();
          await closed;
        })), 'fixture lifetime sockets');
        if (lifetime.listening) await bounded(new Promise((resolve, reject) =>
          lifetime.close(error => error ? reject(error) : resolve())), 'fixture lifetime listener');
      },
    }, record => {
      completed.set(route, record);
      if (process.env.PI_ACP_EVIDENCE_DIR) {
        mkdirSync(process.env.PI_ACP_EVIDENCE_DIR, { recursive: true });
        writeFileSync(join(process.env.PI_ACP_EVIDENCE_DIR, `startup-${route}.json`), JSON.stringify(record, null, 2));
      }
    }));
    lifetime.listen(0, '127.0.0.1');
    await bounded(once(lifetime, 'listening'), 'fixture lifetime listener');
    const cwd = join(root, 'work');
    const home = join(root, 'home');
    const sessionDir = join(root, 'sessions');
    for (const path of [cwd, home, sessionDir]) mkdirSync(path);
    const journal = join(root, 'journal.jsonl');
    const witness = join(root, 'witness.jsonl');
    const launcher = join(root, windows ? 'pi.cmd' : 'pi');
    const observer = fixture('./pi-startup-witness.cjs');
    writeFileSync(launcher, windows
      ? `@echo off\r\n"${process.execPath}" "${observer}" %*\r\n`
      : `#!/bin/sh\nexec ${[process.execPath, observer].map(quoteSh).join(' ')} "$@"\n`);
    if (!windows) chmodSync(launcher, 0o755);
    const env = { ...launchEnv, HOME: home, USERPROFILE: home, PI_CODING_AGENT_DIR: join(home, '.pi', 'agent'),
      PI_ACP_PI_COMMAND: launcher, PI_TEST_CHILD_ENTRY: fixture('./fake-pi-session-rpc.cjs'),
      PI_TEST_SESSION_DIR: sessionDir, PI_TEST_JOURNAL: journal, PI_TEST_WITNESS: witness,
      PI_TEST_LIFETIME_PORT: String(lifetime.address().port), NODE_OPTIONS: '', NODE_DISABLE_COMPILE_CACHE: '1' };
    const start = async childEntry => {
      const client = startAdapter(cwd, { ...env, PI_TEST_CHILD_ENTRY: childEntry }, evidence.clients);
      clients.push(client);
      const init = await client.request('initialize', { protocolVersion: 1, clientCapabilities: {},
        clientInfo: { name: 'intent-pi-startup-regression', version: '1' } });
      assert.equal(`pi-acp@${init.result?.agentInfo?.version}`, pin);
      assert.equal(init.result.agentCapabilities.loadSession, true);
      return client;
    };
    try {
      let saved;
      await t.test('control: successful create, persisted session, and replay', async () => {
        const client = await start(env.PI_TEST_CHILD_ENTRY);
        const created = await client.request('session/new', { cwd, mcpServers: [] });
        assert.ok(created.result?.sessionId, JSON.stringify(created));
        saved = created.result.sessionId;
        const prompt = await client.request('session/prompt', { sessionId: saved, prompt: [{ type: 'text', text: 'startup-control' }] });
        assert.equal(prompt.result?.stopReason, 'end_turn');
        await client.stop();
        const restored = await start(env.PI_TEST_CHILD_ENTRY);
        const loaded = await restored.request('session/load', { sessionId: saved, cwd, mcpServers: [] });
        assert.ok(loaded.result, JSON.stringify(loaded));
        const replay = restored.record.transcript.filter(row => row.message.method === 'session/update'
          && row.message.params.update.sessionUpdate === 'user_message_chunk');
        assert.deepEqual(replay.map(row => row.message.params.update.content.text), ['startup-control']);
        await restored.stop();
        evidence.lifecycleControl = 'passed';
      });
      assert.equal(evidence.lifecycleControl, 'passed', 'Successful-session control must establish and replay a saved session');
      const client = await start(fixture('./fake-pi-startup-exit.cjs'));
      const response = await client.request(`session/${route}`, {
        cwd, mcpServers: [], ...(route === 'load' ? { sessionId: saved } : {}),
      });
      evidence.response = response;
      evidence.witness = jsonLines(witness);
      evidence.pi = jsonLines(journal);
      await t.test('control: launcher observes actual child stderr and exit 73', () => {
        const observed = evidence.witness.filter(row => row.code === 73);
        assert.equal(observed.length, 1, 'One natural child exit; no retry');
        assert.equal(observed[0].signal, null);
        // Host-level tracing may add lines even with NODE_OPTIONS cleared.
        assert.ok(observed[0].stderr.includes(marker + '\n'), observed[0].stderr);
        assert.equal(observed[0].truncated, false);
        const requests = evidence.pi.filter(row => row.event === 'startup-request');
        assert.equal(requests.length, 1);
        assert.equal(requests[0].pid, observed[0].pid, 'Witness must describe the child that received get_state');
        assert.equal(requests[0].command, 'get_state');
        const savedFile = evidence.pi.find(row => row.event === 'spawn').sessionFile;
        assert.deepEqual(requests[0].argv, ['--mode', 'rpc', '--no-themes',
          ...(route === 'load' ? ['--session', savedFile] : [])]);
        assert.ok(response.error, 'Startup must fail through ACP, not a harness timeout');
        evidence.controls = 'passed';
      });
      await t.test('regression: ACP startup error preserves original child stderr', () => {
        assert.ok(JSON.stringify(response.error).includes(marker), JSON.stringify(response.error));
      });
      await t.test('regression: ACP startup error preserves original child exit status', () => {
        assert.match(JSON.stringify(response.error), /(?:exit(?:ed)?(?:\s+with)?(?:\s+code)?|code)[\s"=:]+73\b/i);
      });
    } catch (error) {
      evidence.harnessError = String(error.stack ?? error);
      throw error;
    }
  }));
}

test('independent cleanup audit for early Pi exits', async () => {
  await Promise.all(runs);
  assert.equal(expected.size, 2);
  for (const [route, identity] of expected) {
    const record = process.env.PI_ACP_EVIDENCE_DIR
      ? JSON.parse(readFileSync(join(process.env.PI_ACP_EVIDENCE_DIR, `startup-${route}.json`), 'utf8'))
      : completed.get(route);
    assertCleanupComplete(record, identity);
    assert.equal(record.controls, 'passed', record.harnessError);
  }
});
