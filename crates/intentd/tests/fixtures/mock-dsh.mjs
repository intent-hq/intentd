// Mock DeepSeek Harness CLI (`dsh`) for the intentd WSS E2E suite.
//
// Replays the shapes captured from dsh 0.2.2-rc.5: `--version`,
// `--profile acp --patch <overlay> --dump-config`, and the `--profile acp`
// ACP stdio server. Like the real launcher, a missing `--patch` file stops
// startup, and the dump reflects the overlay: a row is disabled only when the
// overlay names its id. The ACP server exits on stdin EOF (as dsh does).
// Like dsh, `session/new` keeps a folder per session under `$DSH_HOME/sessions`,
// and `session/resume` reopens one in a new process with the default config
// and no history replay; `session/load` is not offered (-32601).
//
// Env:
//   MOCK_DSH_LOG             JSONL log: argv/env per launch and every ACP frame
//   MOCK_DSH_DUMP            the recorded `--dump-config` fixture
//   MOCK_DSH_RENAMED_UPLOAD  "1": the session-log row ships under a new id
//   DEEPSEEK_API_KEY         present: two DeepSeek models and working prompts
//   DSH_HOME                 logged per launch (a key profile runs in its own home)
import { appendFileSync, readFileSync, existsSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';

const args = process.argv.slice(2);
const log = (dir, msg) => {
  if (process.env.MOCK_DSH_LOG) appendFileSync(process.env.MOCK_DSH_LOG, JSON.stringify({ dir, msg }) + '\n');
};
const hasKey = Boolean(process.env.DEEPSEEK_API_KEY);
log('launch', {
  args,
  telemetryDisabled: process.env.DSH_TELEMETRY_DISABLED ?? null,
  hasKey,
  dshHome: process.env.DSH_HOME ?? null,
});

if (args[0] === '--version' || args[0] === '-V') {
  process.stdout.write('0.2.2-rc.5\n');
  process.exit(0);
}

const patchAt = args.indexOf('--patch');
const overlay = patchAt >= 0 ? args[patchAt + 1] : undefined;
if (overlay !== undefined && !existsSync(overlay)) {
  process.stderr.write(`dsh: cannot read patch overlay ${overlay}\n`);
  process.exit(1);
}
const overlayIds = overlay
  ? readFileSync(overlay, 'utf8')
      .split('\n- id: ')
      .map((block) => block.replace(/^- id: /, '').split('\n'))
      .filter((lines) => lines.some((l) => l.trim() === 'disabled: true'))
      .map((lines) => lines[0].trim())
  : [];

if (args.includes('--dump-config')) {
  const UPLOAD_AND_SUBAGENT_ROWS = new Set([
    'session-log-deepseek', 'session-telemetry-otel', 'tool-subagent', 'tool-subagent-fork', 'tool-agent-team',
  ]);
  const blocks = readFileSync(process.env.MOCK_DSH_DUMP, 'utf8').split(/\n(?=- id: )/);
  const out = blocks.map((block) => {
    const match = /^- id: (\S+)/.exec(block);
    if (!match) return block;
    let id = match[1];
    // The shipped profile: these rows are enabled unless an overlay disables them.
    if (UPLOAD_AND_SUBAGENT_ROWS.has(id)) block = block.replace(/\n  disabled: true(?=\n|$)/, '');
    if (id === 'session-log-deepseek' && process.env.MOCK_DSH_RENAMED_UPLOAD === '1') {
      id = 'session-log-deepseek-v2';
      block = block.replace(/^- id: \S+/, `- id: ${id}`);
    }
    if (overlayIds.includes(id) && !/\n  disabled: true(?=\n|$)/.test(block)) {
      block = block.replace(/(\n  name: [^\n]+)/, '$1\n  disabled: true');
    }
    return block;
  });
  process.stdout.write(out.join('\n'));
  process.exit(0);
}

if (args[0] !== '--profile' || args[1] !== 'acp') {
  process.stderr.write(`mock dsh: unsupported args ${JSON.stringify(args)}\n`);
  process.exit(2);
}

const send = (msg) => {
  log('out', msg);
  process.stdout.write(JSON.stringify(msg) + '\n');
};
const update = (sessionId, u) =>
  send({ jsonrpc: '2.0', method: 'session/update', params: { sessionId, update: u } });

const FLASH = '["deepseek-official","deepseek-v4-flash"]';
const PRO = '["deepseek-official","deepseek-v4-pro"]';
const models = hasKey
  ? [{ value: FLASH, name: 'deepseek-v4-flash' }, { value: PRO, name: 'deepseek-v4-pro' }]
  : [{ value: FLASH, name: 'deepseek-v4-flash' }];
const EFFORTS = ['off', 'low', 'high', 'max'];
const sessions = new Map();
const sessionDir = (id) => join(process.env.DSH_HOME ?? '.dsh', 'sessions', id);
const configOptions = (state) => [
  {
    id: 'model', name: 'Model', category: 'model', type: 'select', currentValue: state.model,
    options: [{ group: 'deepseek-official', name: 'DeepSeek', options: models }],
  },
  {
    id: 'reasoning_effort', name: 'Reasoning effort', category: 'thought_level', type: 'select',
    currentValue: state.effort,
    options: EFFORTS.map((value) => ({ value, name: value[0].toUpperCase() + value.slice(1) })),
  },
];

let buf = '';
process.stdin.on('data', (chunk) => {
  buf += chunk;
  let i;
  while ((i = buf.indexOf('\n')) >= 0) {
    const line = buf.slice(0, i).trim();
    buf = buf.slice(i + 1);
    if (line) handle(JSON.parse(line));
  }
});
process.stdin.on('end', () => process.exit(0));

function handle(msg) {
  log('in', msg);
  if (msg.id !== undefined && !msg.method) return;
  const reply = (result) => send({ jsonrpc: '2.0', id: msg.id, result });
  const fail = (code, message) => send({ jsonrpc: '2.0', id: msg.id, error: { code, message } });
  switch (msg.method) {
    case 'initialize':
      return reply({
        protocolVersion: 1,
        agentInfo: { name: 'deepseek-harness-acp', version: '0.0.1' },
        agentCapabilities: {
          mcpCapabilities: { http: true },
          promptCapabilities: { image: false, audio: false, embeddedContext: false },
          sessionCapabilities: { close: {}, list: {}, resume: {} },
        },
        authMethods: [],
      });
    case 'session/new': {
      const sessionId = randomUUID();
      const state = { model: FLASH, effort: 'high' };
      sessions.set(sessionId, state);
      mkdirSync(sessionDir(sessionId), { recursive: true });
      return reply({ sessionId, configOptions: configOptions(state) });
    }
    case 'session/resume': {
      const { sessionId } = msg.params;
      if (!existsSync(sessionDir(sessionId))) return fail(-32602, `Invalid params: unknown session ${sessionId}`);
      const state = sessions.get(sessionId) ?? { model: FLASH, effort: 'high' };
      sessions.set(sessionId, state);
      return reply({ configOptions: configOptions(state) });
    }
    case 'session/set_config_option': {
      const state = sessions.get(msg.params.sessionId);
      if (!state) return fail(-32602, 'Invalid params: unknown session');
      const { configId, value } = msg.params;
      if (configId === 'model') {
        if (!models.some((m) => m.value === value)) return fail(-32602, `Invalid params: unknown model ${value}`);
        state.model = value;
      } else if (configId === 'reasoning_effort') {
        if (!EFFORTS.includes(value)) return fail(-32602, `Invalid params: unknown effort ${value}`);
        state.effort = value;
      } else {
        return fail(-32602, `Invalid params: unknown option ${configId}`);
      }
      return reply({ configOptions: configOptions(state) });
    }
    case 'session/prompt': {
      const { sessionId } = msg.params;
      if (!hasKey) {
        return fail(-32603, 'Internal error: turn failed: llm-deepseek: no API key for provider route "deepseek-official"; store DEEPSEEK_API_KEY through the credentials service (the web Models page writes it), or export DEEPSEEK_API_KEY in the launching environment');
      }
      const toolCallId = 'call_00_bash';
      update(sessionId, { sessionUpdate: 'tool_call', toolCallId, title: 'bash', kind: 'execute', status: 'pending', rawInput: { command: 'git status --short' } });
      update(sessionId, { sessionUpdate: 'tool_call_update', toolCallId, status: 'completed', rawOutput: { output: '' } });
      update(sessionId, { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: 'dsh mock done' } });
      return reply({ stopReason: 'end_turn' });
    }
    case 'session/cancel':
      return;
    case 'session/close':
      sessions.delete(msg.params.sessionId);
      return reply({});
    default:
      if (msg.id !== undefined) return send({ jsonrpc: '2.0', id: msg.id, error: { code: -32601, message: `"Method not found": ${msg.method}` } });
  }
}
