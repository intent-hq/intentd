#!/usr/bin/env node
// Minimal ACP agent for the `intentd node` runner tests, driven by argv so a
// node configuration's provider `args` can select the behavior.
//
//   --exit-on-start <code>  exit immediately with <code>
//   --hang-setup            never answer session/new
//   --detached-helper       start the `sleep` child in its own process group
//
// On start it writes `agent.pid` (its pid and a same-group `sleep` child's
// pid) into its working directory, so tests can check that stop kills the
// whole process tree. A prompt whose text contains "block" parks until
// session/cancel; "slow" waits 300 ms; "many" streams 200 extra chunks;
// "exit" answers and then exits with code 0, leaving its `sleep` child
// running; every turn first streams one agent_message_chunk "echo: <text>".
import readline from 'node:readline';
import fs from 'node:fs';
import { spawn } from 'node:child_process';

const argv = process.argv.slice(2);
const exitAt = argv.indexOf('--exit-on-start');
if (exitAt >= 0) process.exit(Number(argv[exitAt + 1] ?? 1));
const hangSetup = argv.includes('--hang-setup');
const detachedHelper = argv.includes('--detached-helper');

const sleeper = spawn('sleep', ['300'], { stdio: 'ignore', detached: detachedHelper });
fs.writeFileSync('agent.pid', JSON.stringify({ agent: process.pid, child: sleeper.pid }));

const SESSION = 'node-test-session';
const parked = [];

function send(message) {
  process.stdout.write(JSON.stringify({ jsonrpc: '2.0', ...message }) + '\n');
}

function promptText(params) {
  return (params?.prompt ?? []).map((block) => block?.text ?? '').join('');
}

readline.createInterface({ input: process.stdin }).on('line', async (line) => {
  let msg;
  try {
    msg = JSON.parse(line);
  } catch {
    return;
  }
  if (msg.id === undefined || msg.method === undefined) {
    if (msg.method === 'session/cancel') {
      while (parked.length) send({ id: parked.shift(), result: { stopReason: 'cancelled' } });
    }
    return;
  }
  switch (msg.method) {
    case 'initialize':
      return send({ id: msg.id, result: { protocolVersion: 1, agentCapabilities: {} } });
    case 'authenticate':
      return send({ id: msg.id, result: {} });
    case 'session/new':
      if (hangSetup) return;
      return send({ id: msg.id, result: { sessionId: SESSION } });
    case 'session/prompt': {
      const text = promptText(msg.params);
      send({
        method: 'session/update',
        params: {
          sessionId: SESSION,
          update: { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: `echo: ${text}` } },
        },
      });
      if (text.includes('many')) {
        for (let i = 0; i < 200; i++) {
          send({
            method: 'session/update',
            params: {
              sessionId: SESSION,
              update: { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: `chunk ${i}` } },
            },
          });
        }
      }
      if (text.includes('exit')) {
        send({ id: msg.id, result: { stopReason: 'end_turn' } });
        setTimeout(() => process.exit(0), 50);
        return;
      }
      if (text.includes('block')) {
        parked.push(msg.id);
        return;
      }
      if (text.includes('slow')) await new Promise((resolve) => setTimeout(resolve, 300));
      return send({ id: msg.id, result: { stopReason: 'end_turn' } });
    }
    default:
      return send({ id: msg.id, error: { code: -32601, message: 'method not found' } });
  }
});
