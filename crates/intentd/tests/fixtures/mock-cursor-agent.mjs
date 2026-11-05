// Mock Cursor CLI (`cursor-agent`) for the intentd WSS E2E suite.
//
// Replays the shapes captured from cursor-agent 2026.11.05-86047df (Cursor
// spike transcripts): `--version`, `models`, and the hidden `acp` server.
// The ACP turn sends the frames intentd must handle for Cursor:
//   - a shell tool call whose id contains a newline,
//   - `session/request_permission` with allow-once / allow-always / reject-once
//     (allow-always listed first, so a first-non-destructive pick would be wrong),
//   - a `cursor/ask_question` request (no sessionId),
//   - an MCP tool call titled `MCP: tool` then `<server>: <tool>`.
// Like the real CLI, `acp` does NOT exit when stdin closes; intentd must kill it.
//
// Env:
//   MOCK_CURSOR_RPC_LOG     JSONL log of every frame in either direction
//   MOCK_CURSOR_PID_FILE    the `acp` process writes "<pid> <worker pid>" here
//   MOCK_CURSOR_VERSION     `--version` output (default 2026.11.05-86047df)
//   MOCK_CURSOR_SIGNED_OUT  "1": `models`, the model list and `session/new` fail with auth errors
//   MOCK_CURSOR_REJECT_MODELS  '|'-separated model option values (variant ids contain commas) set_config_option rejects
import { appendFileSync, writeFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';

const args = process.argv.slice(2);
const log = (dir, msg) => {
  if (process.env.MOCK_CURSOR_RPC_LOG) {
    appendFileSync(process.env.MOCK_CURSOR_RPC_LOG, JSON.stringify({ dir, msg }) + '\n');
  }
};
const signedOut = process.env.MOCK_CURSOR_SIGNED_OUT === '1';
const AUTH_ERROR =
  "Error: Authentication required. Run 'agent login', pass --api-key/--auth-token, or set CURSOR_API_KEY/CURSOR_AUTH_TOKEN.";

if (args[0] === '--version' || args[0] === '-v') {
  log('cli', { args });
  process.stdout.write((process.env.MOCK_CURSOR_VERSION ?? '2026.11.05-86047df') + '\n');
  process.exit(0);
}

if (args[0] === 'status') {
  log('cli', { args });
  process.stdout.write(JSON.stringify(signedOut
    ? { status: 'unauthenticated', isAuthenticated: false, hasAccessToken: false, hasRefreshToken: false, message: 'Not logged in' }
    : { status: 'authenticated', isAuthenticated: true, hasAccessToken: true, hasRefreshToken: true }) + '\n');
  process.exit(0);
}

// Like the real CLI's color library: FORCE_COLOR=1 wins over NO_COLOR, and
// the colored output breaks a naive parser.
const colorize = process.env.FORCE_COLOR === '1';
const tint = (text) => (colorize ? `\u001b[36m${text}\u001b[39m` : text);

if (args[0] === 'models') {
  log('cli', { args, FORCE_COLOR: process.env.FORCE_COLOR ?? null, NO_COLOR: process.env.NO_COLOR ?? null });
  if (colorize) {
    // Without the daemon's FORCE_COLOR=0 the ids carry escapes and auth
    // detection / parsing would degrade to unknown.
    process.stdout.write(`${tint('auto')} - Auto (current, default)\n`);
    process.exit(0);
  }
  if (signedOut) {
    process.stderr.write(AUTH_ERROR + '\n');
    process.exit(1);
  }
  process.stdout.write(
    [
      'Available models',
      '',
      'auto - Auto (current, default)',
      'gpt-5.2 - GPT-5.2',
      'grok-4.7-low-fast - Grok 4.7  Low Fast​​',
      'claude-opus-5-5-high - Claude Opus 5.5 1M High',
      '',
      "Tip: use --model <id> (or /model <id> in interactive mode) to switch. Parameterized models also accept quoted overrides, e.g. --model 'claude-opus-4-8[context=1m,effort=high,fast=false]'.",
      '',
    ].join('\n'),
  );
  process.exit(0);
}

if (args[0] !== 'acp') {
  process.stderr.write(`mock cursor-agent: unsupported args ${JSON.stringify(args)}\n`);
  process.exit(2);
}

// Like the real CLI's `worker-server`: a child in the same process group that
// outlives a SIGTERM sent to the main pid alone.
const worker = spawn(process.execPath, ['-e', "process.on('SIGTERM',()=>{});setInterval(()=>{},1e9)"], { stdio: 'ignore' });
if (process.env.MOCK_CURSOR_PID_FILE) {
  writeFileSync(process.env.MOCK_CURSOR_PID_FILE, `${process.pid} ${worker.pid}`);
}
// The real `cursor-agent acp` keeps running after stdin EOF.
setInterval(() => {}, 60_000);

const send = (msg) => {
  log('out', msg);
  process.stdout.write(JSON.stringify(msg) + '\n');
};
let nextId = 0;
const pending = new Map();
const request = (method, params) =>
  new Promise((resolve) => {
    const id = nextId++;
    pending.set(id, resolve);
    send({ jsonrpc: '2.0', id, method, params });
  });
const update = (sessionId, u) =>
  send({ jsonrpc: '2.0', method: 'session/update', params: { sessionId, update: u } });

// Base models (`cursor/list_available_models`) and this session's variants.
const baseModels = [
  { value: 'default', name: 'Auto', configOptions: [] },
  { value: 'gpt-5.2', name: 'GPT-5.2', configOptions: [] },
  { value: 'grok-4.7', name: 'Grok 4.7  Low Fast\u200b\u200b', configOptions: [] },
  { value: 'claude-sonnet-4', name: 'Claude Sonnet 4', configOptions: [] },
];
const modelOptions = [
  { value: 'default[]', name: 'Auto' },
  { value: 'gpt-5.2[reasoning=medium,fast=false]', name: 'gpt-5.2' },
  { value: 'grok-4.7[context=256k,reasoning_effort=high,fast=true]', name: 'grok-4.7' },
  { value: 'claude-sonnet-4[thinking=false,context=200k]', name: 'claude-sonnet-4' },
];
const rejected = (process.env.MOCK_CURSOR_REJECT_MODELS ?? '').split('|').filter(Boolean);
const configOptions = (current = 'default[]') => [
  {
    id: 'mode', name: 'Mode', description: 'Controls how the agent executes tasks', category: 'mode',
    type: 'select', currentValue: 'agent',
    options: [
      { value: 'agent', name: 'Agent', description: 'Full agent capabilities with tool access' },
      { value: 'plan', name: 'Plan', description: 'Read-only mode for planning and designing before implementation' },
      { value: 'ask', name: 'Ask', description: 'Q&A mode - no edits or command execution' },
    ],
  },
  {
    id: 'model', name: 'Model', description: 'Controls which model variant is used for responses',
    category: 'model', type: 'select', currentValue: current, options: modelOptions,
  },
];

let cancelCurrent = null;

async function runTurn(sessionId) {
  update(sessionId, { sessionUpdate: 'session_info_update', title: 'Mock Cursor turn' });
  update(sessionId, { sessionUpdate: 'agent_thought_chunk', content: { type: 'text', text: 'Checking the repository status.' } });
  const shellId = 'call-d5f81713-ef9b-4563-81e1-9fccb289dc3b-0\nfc_4488f13b-7cc1-9ba7-9fe8-679b98c595e1_0';
  const command = 'git status --short | head -5; echo "EXIT:$?"';
  update(sessionId, { sessionUpdate: 'tool_call', toolCallId: shellId, title: `\`${command}\``, kind: 'execute', status: 'pending', rawInput: { command } });
  update(sessionId, { sessionUpdate: 'tool_call_update', toolCallId: shellId, status: 'in_progress' });
  const permission = await request('session/request_permission', {
    sessionId,
    toolCall: { toolCallId: shellId, title: `\`${command}\``, kind: 'execute', status: 'pending', content: [{ type: 'content', content: { type: 'text', text: 'Not in allowlist: echo, git status, head' } }] },
    options: [
      { optionId: 'allow-always', name: 'Allow always', kind: 'allow_always' },
      { optionId: 'allow-once', name: 'Allow once', kind: 'allow_once' },
      { optionId: 'reject-once', name: 'Reject', kind: 'reject_once' },
    ],
  });
  const approved = permission.result?.outcome?.outcome === 'selected' && permission.result.outcome.optionId !== 'reject-once';
  update(sessionId, { sessionUpdate: 'tool_call_update', toolCallId: shellId, status: approved ? 'completed' : 'failed', rawOutput: { exitCode: 0, stdout: 'EXIT:0\n', stderr: '' } });

  const answer = await request('cursor/ask_question', {
    toolCallId: 'ask_1',
    title: 'Pick a framework',
    questions: [{ id: 'q1', prompt: 'Which framework?', options: [{ id: 'a', label: 'React' }, { id: 'b', label: 'Svelte' }], allowMultiple: false }],
  });
  const askOutcome = answer.result?.outcome?.outcome ?? `error:${answer.error?.code}`;

  const taskId = 'call-27e9fb5d-2f35-4699-83db-5f3c01ce559a-3\nfc_93650954-f819-9d9f-9e36-f87a8cad6bd7_0';
  update(sessionId, { sessionUpdate: 'tool_call', toolCallId: taskId, title: 'Task: Reply with HELLO', kind: 'other', status: 'pending', rawInput: { _toolName: 'task', prompt: 'Reply with HELLO.', description: 'Reply with HELLO' } });
  update(sessionId, { sessionUpdate: 'tool_call_update', toolCallId: taskId, status: 'completed', rawOutput: { durationMs: 1473, isBackground: false } });
  await request('cursor/task', { toolCallId: taskId, description: 'Reply with HELLO', prompt: 'Reply with HELLO.', subagentType: 'unspecified', model: 'default', agentId: '29a69723-caee-4eb1-ae48-1b5141dea32b', durationMs: 1473 });

  const mcpId = 'call-319b6c00-0bab-4561-a905-9e27aefbeb53-1\nfc_56615f74-5643-917a-ac9f-479d2fe25945_0';
  update(sessionId, { sessionUpdate: 'tool_call', toolCallId: mcpId, title: 'MCP: tool', kind: 'other', status: 'pending', rawInput: {} });
  update(sessionId, { sessionUpdate: 'tool_call_update', toolCallId: mcpId, title: 'workspace-mcp: workspace_api', rawInput: { providerIdentifier: 'workspace-mcp', toolName: 'workspace_api', args: { code: 'return 1', summary: 'probe' } } });
  update(sessionId, { sessionUpdate: 'tool_call_update', toolCallId: mcpId, status: 'completed', rawOutput: { success: true } });

  update(sessionId, { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: `cursor mock done (ask=${askOutcome})` } });
  return { stopReason: 'end_turn' };
}

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
// Deliberately no 'end' handler: stdin EOF does not stop the server.

async function handle(msg) {
  log('in', msg);
  if (msg.id !== undefined && !msg.method) {
    const resolve = pending.get(msg.id);
    pending.delete(msg.id);
    resolve?.(msg);
    return;
  }
  const reply = (result) => send({ jsonrpc: '2.0', id: msg.id, result });
  const fail = (code, message, data) => send({ jsonrpc: '2.0', id: msg.id, error: { code, message, ...(data ? { data } : {}) } });
  switch (msg.method) {
    case 'initialize':
      return reply({
        protocolVersion: 1,
        agentCapabilities: {
          loadSession: true,
          mcpCapabilities: { http: true, sse: true },
          promptCapabilities: { audio: false, embeddedContext: false, image: true },
          sessionCapabilities: { list: {} },
        },
        authMethods: [{ id: 'cursor_login', name: 'Cursor Login', description: "Authenticate using existing Cursor login credentials. Run 'agent login' first if not logged in." }],
      });
    case 'authenticate':
      return fail(-32602, 'mock: authenticate must never be called (it would open a browser login)');
    case 'session/new':
    case 'session/load': {
      if (msg.method === 'session/load') {
        // Like the real CLI: replay the stored history before answering.
        const sid = msg.params.sessionId;
        update(sid, { sessionUpdate: 'user_message_chunk', content: { type: 'text', text: 'check git status' } });
        update(sid, { sessionUpdate: 'tool_call', toolCallId: 'replay-0-4', title: 'workspace-mcp: workspace_api', kind: 'other', status: 'pending', rawInput: { providerIdentifier: 'workspace-mcp', toolName: 'workspace_api', args: {} } });
        update(sid, { sessionUpdate: 'tool_call_update', toolCallId: 'replay-0-4', status: 'completed', rawOutput: { success: true } });
        update(sid, { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: 'cursor mock done (ask=skipped)' } });
      }
      if (signedOut) {
        return fail(-32000, 'Authentication required', { message: "Authentication required. Please run 'agent login' first, then call authenticate() with methodId 'cursor_login'." });
      }
      const sessionId = msg.params.sessionId ?? randomUUID();
      reply({
        ...(msg.method === 'session/new' ? { sessionId } : {}),
        modes: { currentModeId: 'agent', availableModes: [{ id: 'agent', name: 'Agent' }, { id: 'plan', name: 'Plan' }, { id: 'ask', name: 'Ask' }] },
        models: { currentModelId: 'default[]', availableModels: modelOptions.map((o) => ({ modelId: o.value, name: o.name })) },
        configOptions: configOptions(),
      });
      setTimeout(() => update(sessionId, { sessionUpdate: 'available_commands_update', availableCommands: [{ name: 'copy-request-id', description: 'Copy the last request ID to clipboard' }] }), 0);
      return;
    }
    case 'session/set_config_option': {
      const { configId, value } = msg.params;
      if (configId === 'model' && (!modelOptions.some((o) => o.value === value) || rejected.includes(value))) {
        return fail(-32602, `Invalid model value: ${value}`);
      }
      return reply({ configOptions: configOptions(configId === 'model' ? value : undefined) });
    }
    case 'cursor/list_available_models':
      if (signedOut) {
        return fail(-32000, 'Authentication required', { message: "Authentication required. Please run 'agent login' first, then call authenticate() with methodId 'cursor_login'." });
      }
      return reply({ models: baseModels });
    case 'session/set_mode':
      return reply({});
    case 'session/prompt': {
      const cancelled = new Promise((resolve) => { cancelCurrent = () => resolve({ stopReason: 'cancelled' }); });
      const waitForCancel = JSON.stringify(msg.params.prompt).includes('WAIT_FOR_CANCEL');
      if (waitForCancel) {
        update(msg.params.sessionId, { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: 'working until stopped' } });
      }
      const result = await Promise.race([waitForCancel ? new Promise(() => {}) : runTurn(msg.params.sessionId), cancelled]);
      cancelCurrent = null;
      return reply(result);
    }
    case 'session/cancel':
      cancelCurrent?.();
      return;
    default:
      if (msg.id !== undefined) return fail(-32601, `Method not found: ${msg.method}`);
  }
}
