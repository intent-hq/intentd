// Run against an installed *actual* adapter, not a reimplementation of its spawn:
// PI_ACP_TEST_ENTRY=/absolute/path/to/pi-acp/dist/index.js node --test <this file>
// CI reads the shipping pin from intent-providers/src/config.rs and requires
// every native Windows and Unix matrix case, including the cleanup audit.
import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import {
  chmodSync, copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync,
  readdirSync, writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { createServer } from 'node:net';
import { dirname, join, resolve } from 'node:path';
import { createInterface } from 'node:readline';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { assertCleanupComplete, finishCleanup } from './pi-session-cleanup.mjs';

assert.ok(process.env.PI_ACP_TEST_ENTRY, 'PI_ACP_TEST_ENTRY must select the adapter under test');
const adapterEntry = resolve(process.env.PI_ACP_TEST_ENTRY);
assert.ok(existsSync(adapterEntry), `Missing adapter: ${adapterEntry}`);
const windows = process.platform === 'win32';
const fixture = fileURLToPath(new URL('./fake-pi-session-rpc.cjs', import.meta.url));
const timeoutMs = 10_000;
// Supply only OS launch prerequisites. Tests must not depend on host model
// credentials, Node preloads, user Pi configuration, or tracing injections.
const launchEnv = Object.fromEntries(Object.entries(process.env).filter(([key]) =>
  ['path', 'systemroot', 'windir', 'comspec', 'pathext', 'temp', 'tmp'].includes(key.toLowerCase())));
const readJsonLines = file => existsSync(file)
  ? readFileSync(file, 'utf8').trim().split('\n').filter(Boolean).map(JSON.parse) : [];
const quoteSh = value => `'${value.replaceAll("'", "'\\''")}'`;

function bounded(promise, description) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`Timeout: ${description}`)), timeoutMs); }),
  ]).finally(() => clearTimeout(timer));
}

class AcpClient {
  pending = new Map();
  notifications = [];
  transcript = [];
  stderr = '';
  nextId = 1;

  constructor(cwd, env) {
    this.child = spawn(process.execPath, [adapterEntry], {
      cwd, env, stdio: ['pipe', 'pipe', 'pipe'], detached: !windows,
    });
    this.exited = once(this.child, 'close');
    // Attach immediately so a spawn error cannot become an unhandled rejection.
    this.exited.catch(() => {});
    const fail = error => {
      this.failure = error;
      for (const pending of this.pending.values()) pending.reject(error);
      this.pending.clear();
    };
    this.child.on('error', fail);
    this.child.stdin.on('error', fail);
    this.child.on('exit', (code, signal) => fail(new Error(`Adapter exited: ${code}/${signal}; ${this.stderr}`)));
    this.child.stderr.on('data', data => { this.stderr += data; });
    createInterface({ input: this.child.stdout }).on('line', line => {
      let message;
      try { message = JSON.parse(line); } catch { fail(new Error(`Non-JSON ACP output: ${line}`)); return; }
      this.transcript.push({ direction: 'received', message });
      if (message.method) {
        if (message.id !== undefined) { fail(new Error(`Unexpected ACP client request: ${line}`)); return; }
        this.notifications.push(message);
        return;
      }
      const pending = this.pending.get(message.id);
      if (!pending) { fail(new Error(`Unexpected ACP response: ${line}`)); return; }
      this.pending.delete(message.id);
      if (message.error) pending.reject(new Error(JSON.stringify(message.error)));
      else pending.resolve(message.result);
    });
  }

  async request(method, params) {
    if (this.failure) throw this.failure;
    const id = this.nextId++;
    const message = { jsonrpc: '2.0', id, method, params };
    this.transcript.push({ direction: 'sent', message });
    const response = new Promise((resolve, reject) => this.pending.set(id, { resolve, reject }));
    this.child.stdin.write(JSON.stringify(message) + '\n');
    return bounded(response, method).finally(() => this.pending.delete(id));
  }

  async initialize() {
    const result = await this.request('initialize', {
      protocolVersion: 1, clientCapabilities: {}, clientInfo: { name: 'intentd-pi-path-regression', version: '1' },
    });
    assert.equal(result.protocolVersion, 1);
    assert.equal(result.agentCapabilities.loadSession, true);
  }

  async stop() {
    // Kill the whole tree: on Windows the adapter's child is CMD, whose Pi child
    // is otherwise orphaned by child.kill(). Await closure before removing files.
    if (windows) {
      if (this.child.exitCode === null) {
        const killed = spawnSync('taskkill.exe', ['/PID', String(this.child.pid), '/T', '/F'], { timeout: timeoutMs });
        assert.equal(killed.status, 0, `taskkill: ${killed.stderr}`);
      }
    } else {
      try { process.kill(-this.child.pid, 'SIGKILL'); } catch (error) { if (error.code !== 'ESRCH') throw error; }
    }
    await bounded(this.exited, 'adapter tree shutdown');
  }
}

function filesUnder(root) {
  return readdirSync(root, { withFileTypes: true }).flatMap(entry => {
    const path = join(root, entry.name);
    return entry.isDirectory() ? filesUnder(path) : [path];
  }).sort();
}

const cases = [
  ['spaces', 'session files with spaces'],
  ['metacharacters', 'project & (a) ^ 100% !PI_PATH_TOKEN! %PI_PATH_TOKEN%'],
  ['command-separator', 'sessions & pi-path-canary & tail'],
];
// Native .cmd/.bat tests are always registered on Windows; no skip/xfail mode.
// Unix executable and extension wrappers are controls, not Windows emulation.
const launchers = windows ? ['cmd', 'bat'] : ['executable', 'extension'];
const runId = randomUUID();
const completedEvidence = new Map();
const expectedEvidence = new Map();
const caseRuns = [];

for (const launcherKind of launchers) {
  for (const [caseName, directoryName] of cases) {
    const caseId = `${launcherKind}-${caseName}`;
    caseRuns.push(test(`${launcherKind}: ACP create, restart, load and resume (${caseName})`, { timeout: 90_000 }, async t => {
      const root = mkdtempSync(join(tmpdir(), 'intent-pi-'));
      const clients = [];
      const sessions = [];
      const evidence = { runId, root, platform: process.platform, node: process.version,
        adapterEntry, launcherKind, caseName, sessions };
      expectedEvidence.set(caseId, { runId, root });
      let closeLifetime = async () => {};
      t.after(() => finishCleanup(evidence, {
        async stopClients() {
          const results = await Promise.allSettled(clients.map(async client => {
            if (client.child.exitCode === null && client.child.signalCode === null) await client.stop();
          }));
          const errors = results.filter(result => result.status === 'rejected').map(result => result.reason);
          if (errors.length) throw new AggregateError(errors, 'Adapter shutdown failed');
        },
        closeLifetime: () => closeLifetime(),
      }, record => {
        if (process.env.PI_ACP_EVIDENCE_DIR) {
          mkdirSync(process.env.PI_ACP_EVIDENCE_DIR, { recursive: true });
          writeFileSync(join(process.env.PI_ACP_EVIDENCE_DIR, `${caseId}.json`), JSON.stringify(record, null, 2));
        }
        completedEvidence.set(caseId, record);
        t.diagnostic(JSON.stringify({ caseId, runId, root, cleanup: record.cleanup }));
      }));
      const fixtureSockets = new Set();
      const lifetime = createServer(socket => {
        fixtureSockets.add(socket);
        socket.on('error', () => {});
        socket.on('close', () => fixtureSockets.delete(socket));
      });
      closeLifetime = async () => {
        await bounded(Promise.all([...fixtureSockets].map(async socket => {
          const closed = new Promise(resolve => socket.once('close', resolve));
          socket.end();
          await closed;
        })), 'orphaned fixture shutdown');
        await bounded(new Promise((resolve, reject) => lifetime.close(error => error ? reject(error) : resolve())),
          'fixture lifetime listener shutdown');
      };
      lifetime.listen(0, '127.0.0.1');
      await bounded(once(lifetime, 'listening'), 'fixture lifetime listener');
      const cwd = join(root, 'work');
      const home = join(root, 'home');
      const sessionDir = join(root, directoryName);
      const journal = join(root, 'pi-journal.jsonl');
      for (const dir of [cwd, home, sessionDir]) mkdirSync(dir);
      const fixtureCopy = join(root, 'fake-pi.cjs');
      copyFileSync(fixture, fixtureCopy);
      const extensionPath = join(sessionDir, 'extension & (a) ^ 100% !PI_PATH_TOKEN! %PI_PATH_TOKEN%.ts');
      const extensionArgs = launcherKind === 'extension' ? ['-e', extensionPath] : [];
      if (extensionArgs.length) writeFileSync(extensionPath, 'export default () => {};\n');

      // Keep the launcher itself simple here to isolate --session truncation:
      // session/new has no --session; session/load must supply the recorded path.
      const launcher = join(root, windows ? `pi.${launcherKind}` : 'pi');
      if (windows) {
        writeFileSync(launcher, `@echo off\r\n"${process.execPath}" "${fixtureCopy}" %*\r\n`);
      } else {
        const args = [process.execPath, fixtureCopy, ...extensionArgs].map(quoteSh).join(' ');
        writeFileSync(launcher, `#!/bin/sh\nexec ${args} "$@"\n`);
        chmodSync(launcher, 0o755);
      }
      const canary = join(cwd, windows ? 'pi-path-canary.cmd' : 'pi-path-canary');
      writeFileSync(canary, windows ? '@echo off\r\necho executed>shell-canary.txt\r\n'
        : '#!/bin/sh\nprintf executed > shell-canary.txt\n');
      if (!windows) chmodSync(canary, 0o755);
      const env = { ...launchEnv,
        HOME: home, USERPROFILE: home, PI_CODING_AGENT_DIR: join(home, '.pi', 'agent'),
        PI_ACP_PI_COMMAND: launcher, PI_TEST_SESSION_DIR: sessionDir, PI_TEST_JOURNAL: journal,
        PI_PATH_TOKEN: 'UNEXPECTED_EXPANSION', NODE_OPTIONS: '', NODE_DISABLE_COMPILE_CACHE: '1',
        PI_TEST_LIFETIME_PORT: String(lifetime.address().port),
        PATH: `${cwd}${windows ? ';' : ':'}${process.env.PATH ?? ''}`,
      };
      const start = async () => {
        const client = new AcpClient(cwd, env);
        clients.push(client);
        await client.initialize();
        return client;
      };
      try {
        let client = await start();
        for (const label of ['first', 'second']) {
          const created = await client.request('session/new', { cwd, mcpServers: [] });
          const sessionId = created.sessionId;
          assert.equal(typeof sessionId, 'string');
          const record = readJsonLines(journal).filter(row => row.event === 'spawn').at(-1);
          assert.deepEqual(record.argv, [...extensionArgs, '--mode', 'rpc', '--no-themes']);
          assert.equal(record.cwd, cwd);
          assert.equal(dirname(record.sessionFile), sessionDir);
          sessions.push({ sessionId, sessionFile: record.sessionFile, label });
          const result = await client.request('session/prompt', { sessionId, prompt: [{ type: 'text', text: `${label}:created` }] });
          assert.equal(result.stopReason, 'end_turn');
        }
        assert.notEqual(sessions[0].sessionId, sessions[1].sessionId);
        assert.notEqual(sessions[0].sessionFile, sessions[1].sessionFile);
        await client.stop();
        client = await start();
        for (const { sessionId, sessionFile, label } of sessions) {
          client.notifications.length = 0;
          await client.request('session/load', { sessionId, cwd, mcpServers: [] });
          const record = readJsonLines(journal).filter(row => row.event === 'spawn').at(-1);
          assert.deepEqual(record.argv, [...extensionArgs, '--mode', 'rpc', '--no-themes', '--session', sessionFile],
            'The actual Pi process must receive one complete --session argument');
          assert.equal(record.sessionFile, sessionFile);
          assert.equal(record.cwd, cwd);
          const replay = client.notifications.filter(msg => msg.method === 'session/update'
            && msg.params.update.sessionUpdate === 'user_message_chunk');
          assert.deepEqual(replay.map(msg => [msg.params.sessionId, msg.params.update.content.text]),
            [[sessionId, `${label}:created`]], 'Load must replay only this session');
          const result = await client.request('session/prompt', { sessionId, prompt: [{ type: 'text', text: `${label}:resumed` }] });
          assert.equal(result.stopReason, 'end_turn');
        }
        await client.stop();
        for (const { sessionFile, sessionId, label } of sessions) {
          const entries = readJsonLines(sessionFile);
          assert.equal(entries[0].id, sessionId);
          assert.deepEqual(entries.slice(1).map(row => row.message.content[0].text), [`${label}:created`, `${label}:resumed`]);
        }
        assert.equal(readJsonLines(journal).filter(row => row.event === 'spawn').length, 4);
        assert.deepEqual(filesUnder(sessionDir), [...sessions.map(session => session.sessionFile),
          ...(extensionArgs.length ? [extensionPath] : [])].sort(), 'No truncated or expanded session file');
        assert.deepEqual(readdirSync(root).sort(), [directoryName, 'fake-pi.cjs', 'home',
          windows ? `pi.${launcherKind}` : 'pi', 'pi-journal.jsonl', 'work'].sort(), 'No truncated file beside the session directory');
        assert.equal(existsSync(join(cwd, 'shell-canary.txt')), false, 'Path text must never execute a command');
        evidence.result = 'passed';
      } catch (error) {
        evidence.result = 'failed';
        evidence.error = String(error.stack ?? error);
        throw error;
      } finally {
        evidence.pi = readJsonLines(journal);
        evidence.acp = clients.map(client => ({ transcript: client.transcript, stderr: client.stderr }));
        evidence.files = filesUnder(root);
        evidence.histories = sessions.map(session => ({ ...session, entries: readJsonLines(session.sessionFile) }));
        t.diagnostic(JSON.stringify({ platform: process.platform, node: process.version, adapterEntry, launcherKind, caseName,
          result: evidence.result, spawns: evidence.pi.filter(row => row.event === 'spawn') }));
      }
    }));
  }
}

// This is a separate test, not an after hook on an already-failing path test.
// It rejects missing/stale output and failed cleanup even for the expected old
// Windows pin failures. CI always runs the full file, including this audit.
test('independent cleanup audit for every Pi session case', async () => {
  await Promise.all(caseRuns);
  assert.equal(expectedEvidence.size, launchers.length * cases.length, 'Missing case cleanup registration');
  for (const [caseId, expected] of expectedEvidence) {
    const evidence = process.env.PI_ACP_EVIDENCE_DIR
      ? JSON.parse(readFileSync(join(process.env.PI_ACP_EVIDENCE_DIR, `${caseId}.json`), 'utf8'))
      : completedEvidence.get(caseId);
    assertCleanupComplete(evidence, expected);
  }
});
