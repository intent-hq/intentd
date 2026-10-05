// Credential-free Pi RPC fixture. Persistence uses the received argv, never the
// harness's expected path, so a shell-split --session cannot pass the test.
const fs = require('node:fs');
const path = require('node:path');
const readline = require('node:readline');
const { randomUUID } = require('node:crypto');
const { createConnection } = require('node:net');

// Independent lifetime channel: killing a CMD parent can orphan its Node child.
// The harness can still reap every fixture, even after the adapter has crashed.
const lifetime = createConnection({ host: '127.0.0.1', port: Number(process.env.PI_TEST_LIFETIME_PORT) });
lifetime.on('end', () => process.exit(0));
lifetime.on('error', () => process.exit(2));

const argv = process.argv.slice(2);
const sessionIndex = argv.indexOf('--session');
const sessionFile = sessionIndex < 0
  ? path.join(process.env.PI_TEST_SESSION_DIR, `${randomUUID()}.jsonl`)
  : argv[sessionIndex + 1];
const journal = process.env.PI_TEST_JOURNAL;
function record(event) {
  fs.appendFileSync(journal, JSON.stringify({ pid: process.pid, ...event }) + '\n');
}
record({ event: 'spawn', argv, cwd: process.cwd(), sessionFile });

if (!fs.existsSync(sessionFile)) {
  fs.writeFileSync(sessionFile, JSON.stringify({
    type: 'session', version: 3, id: randomUUID(), cwd: process.cwd(),
    timestamp: new Date().toISOString(),
  }) + '\n');
}
function entries() {
  return fs.readFileSync(sessionFile, 'utf8').trim().split('\n').map(JSON.parse);
}
const sessionId = entries()[0].id;
const model = { provider: 'fixture', id: 'fixture-model', name: 'Fixture model', reasoning: true };
function send(message) {
  process.stdout.write(JSON.stringify(message) + '\n');
}
function messages() {
  return entries().filter(entry => entry.type === 'message').map(entry => entry.message);
}

readline.createInterface({ input: process.stdin }).on('line', line => {
  const command = JSON.parse(line);
  record({ event: 'request', command: command.type, sessionFile });
  let data;
  switch (command.type) {
    case 'get_state':
      data = { sessionId, sessionFile, model, thinkingLevel: 'medium', isStreaming: false,
        followUpMode: 'all', steeringMode: 'all', messageCount: messages().length };
      break;
    case 'get_available_models': data = { models: [model] }; break;
    case 'get_available_thinking_levels': data = { levels: ['off', 'medium', 'high'] }; break;
    case 'get_commands': data = { commands: [] }; break;
    case 'get_messages': data = { messages: messages() }; break;
    case 'get_session_stats': data = { sessionId, sessionFile, totalMessages: messages().length }; break;
    case 'set_session_name': data = {}; break;
    case 'prompt': {
      const message = { role: 'user', content: [{ type: 'text', text: command.message }] };
      fs.appendFileSync(sessionFile, JSON.stringify({
        type: 'message', id: randomUUID(), timestamp: new Date().toISOString(), message,
      }) + '\n');
      data = {};
      break;
    }
    default:
      send({ type: 'response', id: command.id, command: command.type, success: false,
        error: `Unexpected fixture command: ${command.type}` });
      return;
  }
  send({ type: 'response', id: command.id, command: command.type, success: true, data });
  if (command.type === 'prompt') {
    send({ type: 'agent_start' });
    send({ type: 'agent_end', messages: [] });
    send({ type: 'agent_settled' });
  }
}).on('close', () => process.exit(0));

// Last-resort bound if the harness itself crashes before closing the channel.
setTimeout(() => process.exit(2), 60_000).unref();
