import {test} from 'node:test';
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {createInterface} from 'node:readline';
import {mkdtemp, writeFile, rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';

const adapter = fileURLToPath(new URL('../dist/codex-acp.mjs', import.meta.url));
const mock = fileURLToPath(new URL('./mock-codex.mjs', import.meta.url));

test('shipped adapter follows host upgrades for models and prompts without npm', {skip: process.platform === 'win32', timeout: 15000}, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'intent host codex '));
  try {
    for (const model of ['device-old', 'device-new-model']) {
      await writeFile(join(directory, 'codex'), `#!/bin/sh\nexec '${process.execPath}' '${mock}' "$@" '${model}'\n`, {mode: 0o755});
      const child = spawn(process.execPath, [adapter], {
        env: {PATH: directory, HOME: directory, CODEX_HOME: directory, NODE_DISABLE_COMPILE_CACHE: '1'},
        stdio: ['pipe', 'pipe', 'pipe'],
      });
      let stderr = '';
      child.stderr.on('data', chunk => { stderr += chunk; });
      const pending = new Map();
      const notifications = [];
      let id = 0;
      createInterface({input: child.stdout}).on('line', line => {
        const message = JSON.parse(line);
        if (message.id !== undefined) pending.get(message.id)?.(message);
        else notifications.push(message);
      });
      child.on('exit', () => {
        for (const resolve of pending.values()) resolve({error: {message: stderr}});
      });
      const rpc = async (method, params) => {
        const requestId = ++id;
        const response = new Promise(resolve => pending.set(requestId, resolve));
        child.stdin.write(JSON.stringify({jsonrpc: '2.0', id: requestId, method, params}) + '\n');
        const message = await response;
        pending.delete(requestId);
        assert.equal(message.error, undefined, JSON.stringify(message.error));
        return message.result;
      };
      try {
        await rpc('initialize', {protocolVersion: 1, clientCapabilities: {}});
        const session = await rpc('session/new', {cwd: directory, mcpServers: []});
        assert.ok(session.configOptions.find(option => option.id === 'model').options.some(option => option.value === model));
        const result = await rpc('session/prompt', {sessionId: session.sessionId, prompt: [{type: 'text', text: 'hello'}]});
        assert.equal(result.stopReason, 'end_turn');
        assert.ok(notifications.some(message => message.params?.update?.content?.text === `reply from ${model}`));
      } finally {
        if (child.exitCode === null && child.signalCode === null) {
          child.stdin.end();
          await new Promise(resolve => child.once('exit', resolve));
        }
      }
    }
  } finally {
    await rm(directory, {recursive: true, force: true});
  }
});
