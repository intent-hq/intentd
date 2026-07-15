#!/usr/bin/env node
// Deterministic mock ACP agent for intentd's hermetic E2E (spec §13.2).
//
// Speaks JSON-RPC 2.0 over stdin/stdout in the shapes the Rust ACP client
// parses (initialize / authenticate / session/new / session/prompt). On
// session/prompt it exercises the agent→BE MCP loop for real: it reads the
// generated `--mcp-config`, spawns the `workspace-mcp` server command from it
// (the `intentd mcp-bridge` proxy), and issues an MCP `tools/call` so the BE
// state changes — NOT an in-process shortcut. Behavior is driven by the JSON in
// MOCK_AGENT_BEHAVIOR: { toolCall: { name, arguments }, response }. An optional
// `rules` array of prompt-matched variants ({ ifPromptContains, toolCall?,
// toolCalls?, response?, delayMs? }) lets ONE behavior drive a delegating
// parent and several distinct children (first matching rule wins; falls back
// to the top-level behavior).
import readline from 'node:readline';
import fs from 'node:fs';
import { spawn } from 'node:child_process';

const SESSION_ID = 'mock-session-1';

// Per-process turn counter + the ids of prompts parked by `blockUntilCancel`.
// These persist across messages within ONE child, so a follow-up prompt landing
// with `promptCount > 1` proves the daemon resumed the SAME process (keep-alive)
// rather than respawning it.
let promptCount = 0;
const pendingPromptIds = [];

// Agent→client request correlation: track outgoing request IDs separately from
// incoming prompt IDs. The daemon responds to each client call with a matching id.
let nextClientCallId = 1;
const pendingClientCalls = new Map();

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

// Issue an agent→client request (fs/*, terminal/*, session/request_permission).
// Returns a promise that resolves with the response result or rejects with an error.
// The correlation is via the JSON-RPC id we assign (nextClientCallId).
function callClientService(method, params) {
  return new Promise((resolve, reject) => {
    const id = nextClientCallId++;
    pendingClientCalls.set(id, { resolve, reject });
    send({ jsonrpc: '2.0', id, method, params });
  });
}

function extractPromptText(params) {
  const prompt = params && params.prompt;
  if (!Array.isArray(prompt)) return '';
  return prompt
    .map((b) => (b && typeof b.text === 'string' ? b.text : ''))
    .join(' ');
}

// First `rules` entry whose `ifPromptContains` marker appears in the prompt
// text wins; otherwise the top-level behavior applies.
function selectBehavior(behavior, promptText) {
  if (Array.isArray(behavior.rules)) {
    for (const rule of behavior.rules) {
      if (
        typeof rule.ifPromptContains === 'string' &&
        rule.ifPromptContains.length > 0 &&
        promptText.includes(rule.ifPromptContains)
      ) {
        return rule;
      }
    }
  }
  return behavior;
}

async function handlePrompt(id, params) {
  promptCount += 1;
  let behavior = {};
  try {
    behavior = JSON.parse(process.env.MOCK_AGENT_BEHAVIOR || '{}');
  } catch {
    behavior = {};
  }
  // Keep-alive interrupt test: the FIRST turn streams a chunk then parks without
  // resolving, so the daemon can issue `agent.stop` mid-turn. It is left pending
  // until a `session/cancel` arrives; the child stays alive for the follow-up.
  if (behavior.blockUntilCancel && promptCount === 1) {
    note('session/update', {
      sessionId: SESSION_ID,
      update: { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: 'streaming-before-cancel' } },
    });
    pendingPromptIds.push(id);
    return;
  }
  // Per-prompt park: when this prompt's text contains the configured marker, the
  // session parks until `session/cancel`. Lets one MOCK_AGENT_BEHAVIOR drive both
  // a delegating parent (no match → fall through to `toolCall`) and a parked
  // child whose delegated `agentInstructions` carry the marker.
  const promptText = extractPromptText(params);
  if (
    typeof behavior.parkIfPromptContains === 'string' &&
    behavior.parkIfPromptContains.length > 0 &&
    promptText.includes(behavior.parkIfPromptContains)
  ) {
    pendingPromptIds.push(id);
    return;
  }
  const active = selectBehavior(behavior, promptText);
  // Optional per-rule delay BEFORE the tool calls so a test can observe
  // intermediate state (e.g. a parent's waiting flags) while this agent's
  // turn is still in flight.
  const ruleDelayMs = Number.isFinite(active.delayMs) ? active.delayMs : 0;
  if (ruleDelayMs > 0) {
    await new Promise((r) => setTimeout(r, ruleDelayMs));
  }
  const toolCalls = Array.isArray(active.toolCalls)
    ? active.toolCalls
    : active.toolCall
      ? [active.toolCall]
      : [];
  for (const toolCall of toolCalls) {
    try {
      const res = await callWorkspaceTool(toolCall);
      log(`tool call ok: ${JSON.stringify(res).slice(0, 120)}`);
    } catch (err) {
      log(`tool call failed: ${err.message}`);
      return result(id, { stopReason: 'refusal' });
    }
  }
  // Client calls: agent→client requests (fs/*, terminal/*, session/request_permission).
  // Each entry in active.clientCalls is { method, params, assertResult?, assertError? }.
  // We issue the call, await the daemon's response, and optionally assert on it.
  const clientCalls = Array.isArray(active.clientCalls) ? active.clientCalls : [];
  for (const call of clientCalls) {
    try {
      const resp = await callClientService(call.method, call.params);
      log(`client call ok: ${call.method} → ${JSON.stringify(resp).slice(0, 120)}`);
      // Optional assertion on the result
      if (call.assertResult !== undefined) {
        const expected = JSON.stringify(call.assertResult);
        const actual = JSON.stringify(resp);
        if (expected !== actual) {
          log(`assertion failed: expected ${expected}, got ${actual}`);
          return result(id, { stopReason: 'refusal' });
        }
      }
    } catch (err) {
      log(`client call failed: ${call.method} → ${err.message}`);
      // If the call defines assertError, treat this as expected
      if (call.assertError !== undefined) {
        log(`client call error matched expectation: ${JSON.stringify(call.assertError)}`);
      } else {
        return result(id, { stopReason: 'refusal' });
      }
    }
  }
  const base = active.response || behavior.response || 'Mock agent completed.';
  // In keep-alive mode, stamp the turn count so a resumed follow-up turn is
  // distinguishable from a fresh spawn (which would report `turn=1`).
  const text = behavior.blockUntilCancel ? `${base} turn=${promptCount}` : base;
  // Optional per-turn delay (MS) so a test can set up queue state during the
  // first turn before it resolves. Only applied to the FIRST turn so subsequent
  // queue-drained turns proceed at full speed.
  const delayMs = Number.isFinite(behavior.firstTurnDelayMs) ? behavior.firstTurnDelayMs : 0;
  if (delayMs > 0 && promptCount === 1) {
    await new Promise((r) => setTimeout(r, delayMs));
  }
  note('session/update', {
    sessionId: SESSION_ID,
    update: { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text } },
  });
  result(id, { stopReason: 'end_turn' });
}

// Attempt-counting for deterministic failure modes. Reads/writes a counter file
// whose path is passed via MOCK_AGENT_ATTEMPT_FILE env var. Returns the current
// attempt number (1-based) and increments the file for the next spawn.
function getAndIncrementAttempt() {
  const path = process.env.MOCK_AGENT_ATTEMPT_FILE;
  if (!path) return 1;
  let count = 1;
  try {
    if (fs.existsSync(path)) {
      count = parseInt(fs.readFileSync(path, 'utf8'), 10) || 1;
    }
  } catch {}
  try {
    fs.writeFileSync(path, String(count + 1), 'utf8');
  } catch {}
  return count;
}

async function dispatch(msg) {
  let behavior = {};
  try {
    behavior = JSON.parse(process.env.MOCK_AGENT_BEHAVIOR || '{}');
  } catch {
    behavior = {};
  }

  switch (msg.method) {
    case 'initialize':
      return result(msg.id, { protocolVersion: 1, agentCapabilities: { loadSession: false } });
    case 'authenticate':
      return result(msg.id, {});
    case 'session/new': {
      // Deterministic failure mode: ignore session/new for the first N attempts
      if (typeof behavior.ignoreSessionNewAttempts === 'number' && behavior.ignoreSessionNewAttempts > 0) {
        const attempt = getAndIncrementAttempt();
        if (attempt <= behavior.ignoreSessionNewAttempts) {
          log(`ignoring session/new (attempt ${attempt}/${behavior.ignoreSessionNewAttempts})`);
          // Stall without responding (timeout will trigger)
          return;
        }
      }
      return result(msg.id, { sessionId: SESSION_ID });
    }
    case 'session/load':
      return send({ jsonrpc: '2.0', id: msg.id, error: { code: -32601, message: 'no load' } });
    case 'session/prompt':
      return handlePrompt(msg.id, msg.params);
    case 'session/cancel':
      // Resolve any turn parked by `blockUntilCancel` with a `cancelled` stop
      // reason and stay alive for a follow-up (resume) prompt — the observable
      // keep-alive interrupt. Notification itself gets no reply.
      while (pendingPromptIds.length) {
        result(pendingPromptIds.shift(), { stopReason: 'cancelled' });
      }
      return;
    default:
      if (msg.id !== undefined)
        send({ jsonrpc: '2.0', id: msg.id, error: { code: -32601, message: `no ${msg.method}` } });
  }
}

// Deterministic failure mode: exit immediately on launch for the first N spawns.
// This triggers "agent stdout closed" handshake failure during initialize.
let exitBehavior = {};
try {
  exitBehavior = JSON.parse(process.env.MOCK_AGENT_BEHAVIOR || '{}');
} catch {}
if (typeof exitBehavior.exitImmediatelyAttempts === 'number' && exitBehavior.exitImmediatelyAttempts > 0) {
  const attempt = getAndIncrementAttempt();
  if (attempt <= exitBehavior.exitImmediatelyAttempts) {
    log(`exiting immediately (attempt ${attempt}/${exitBehavior.exitImmediatelyAttempts})`);
    process.exit(1);
  }
}

const rl = readline.createInterface({ input: process.stdin, terminal: false });
rl.on('line', async (line) => {
  const trimmed = line.trim();
  if (!trimmed) return;
  let msg;
  try {
    msg = JSON.parse(trimmed);
  } catch (err) {
    log(`parse error: ${err.message}`);
    return;
  }
  // If this is a response to a client call we issued (has an id and either result or error),
  // resolve or reject the pending promise. Otherwise dispatch it as a daemon→agent request.
  if (msg.id !== undefined && (msg.result !== undefined || msg.error !== undefined)) {
    const pending = pendingClientCalls.get(msg.id);
    if (pending) {
      pendingClientCalls.delete(msg.id);
      if (msg.error) {
        pending.reject(new Error(JSON.stringify(msg.error)));
      } else {
        pending.resolve(msg.result);
      }
      return;
    }
  }
  // Not a client-call response; dispatch it
  try {
    await dispatch(msg);
  } catch (err) {
    log(`dispatch error: ${err.message}`);
  }
});
