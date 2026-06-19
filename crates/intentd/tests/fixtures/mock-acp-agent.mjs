#!/usr/bin/env node
// Deterministic mock ACP agent for intentd's hermetic E2E (spec §13.2).
//
// Speaks JSON-RPC 2.0 over stdin/stdout in the shapes the Rust ACP client
// parses (initialize / authenticate / session/new / session/prompt). On
// session/prompt it exercises the agent→BE MCP loop for real: it reads the
// generated `--mcp-config`, spawns the `workspace-mcp` server command from it
// (the `intentd mcp-bridge` proxy), and issues an MCP `tools/call` so the BE
// state changes — NOT an in-process shortcut. Behavior is driven by the JSON in
// MOCK_AGENT_BEHAVIOR: { toolCall: { name, arguments }, response }.
import readline from 'node:readline';
import fs from 'node:fs';
import { spawn } from 'node:child_process';

const SESSION_ID = 'mock-session-1';

function send(obj) {
  process.stdout.write(JSON.stringify(obj) + '\n');
}
function result(id, res) {
  send({ jsonrpc: '2.0', id, result: res });
}
function note(method, params) {
  send({ jsonrpc: '2.0', method, params });
}
function log(msg) {
  process.stderr.write(`[mock-agent] ${msg}\n`);
}

function mcpConfigPath() {
  const argv = process.argv;
  const i = argv.indexOf('--mcp-config');
  return i >= 0 && i + 1 < argv.length ? argv[i + 1] : null;
}

// Spawn the configured workspace-mcp server, perform an MCP initialize +
// tools/call, and resolve with the call response. Rejects on any transport error
// so the test fails loudly rather than silently skipping the mutation.
function callWorkspaceTool(toolCall) {
  return new Promise((resolve, reject) => {
    const path = mcpConfigPath();
    if (!path) return reject(new Error('no --mcp-config provided'));
    const cfg = JSON.parse(fs.readFileSync(path, 'utf8'));
    const srv = cfg.mcpServers && cfg.mcpServers['workspace-mcp'];
    if (!srv) return reject(new Error('no workspace-mcp server in config'));
    log(`spawning bridge: ${srv.command} ${(srv.args || []).join(' ')}`);
    const child = spawn(srv.command, srv.args || [], {
      env: { ...process.env, ...(srv.env || {}) },
      stdio: ['pipe', 'pipe', 'inherit'],
    });
    child.on('error', reject);
    const rl = readline.createInterface({ input: child.stdout });
    const pending = new Map();
    rl.on('line', (line) => {
      const trimmed = line.trim();
      if (!trimmed) return;
      let msg;
      try {
        msg = JSON.parse(trimmed);
      } catch {
        return;
      }
      const fn = pending.get(msg.id);
      if (fn) {
        pending.delete(msg.id);
        fn(msg);
      }
    });
    const request = (id, method, params) =>
      new Promise((res) => {
        pending.set(id, res);
        child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n');
      });

    (async () => {
      await request(1, 'initialize', {});
      const resp = await request(2, 'tools/call', {
        name: toolCall.name,
        arguments: toolCall.arguments || {},
      });
      child.stdin.end();
      child.kill();
      if (resp.error) return reject(new Error(`tool call error: ${JSON.stringify(resp.error)}`));
      resolve(resp.result);
    })().catch(reject);
  });
}

async function handlePrompt(id) {
  let behavior = {};
  try {
    behavior = JSON.parse(process.env.MOCK_AGENT_BEHAVIOR || '{}');
  } catch {
    behavior = {};
  }
  if (behavior.toolCall) {
    try {
      const res = await callWorkspaceTool(behavior.toolCall);
      log(`tool call ok: ${JSON.stringify(res).slice(0, 120)}`);
    } catch (err) {
      log(`tool call failed: ${err.message}`);
      return result(id, { stopReason: 'refusal' });
    }
  }
  const text = behavior.response || 'Mock agent completed.';
  note('session/update', {
    sessionId: SESSION_ID,
    update: { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text } },
  });
  result(id, { stopReason: 'end_turn' });
}

async function dispatch(msg) {
  switch (msg.method) {
    case 'initialize':
      return result(msg.id, { protocolVersion: 1, agentCapabilities: { loadSession: false } });
    case 'authenticate':
      return result(msg.id, {});
    case 'session/new':
      return result(msg.id, { sessionId: SESSION_ID });
    case 'session/load':
      return send({ jsonrpc: '2.0', id: msg.id, error: { code: -32601, message: 'no load' } });
    case 'session/prompt':
      return handlePrompt(msg.id);
    case 'session/cancel':
      return; // notification — no reply
    default:
      if (msg.id !== undefined)
        send({ jsonrpc: '2.0', id: msg.id, error: { code: -32601, message: `no ${msg.method}` } });
  }
}

const rl = readline.createInterface({ input: process.stdin, terminal: false });
rl.on('line', async (line) => {
  const trimmed = line.trim();
  if (!trimmed) return;
  try {
    await dispatch(JSON.parse(trimmed));
  } catch (err) {
    log(`dispatch error: ${err.message}`);
  }
});
