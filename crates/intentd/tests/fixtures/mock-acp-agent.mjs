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
// toolCalls?, response?, delayMs?, rawUpdates? }) lets ONE behavior drive a
// delegating parent and several distinct children (first matching rule wins;
// falls back to the top-level behavior). `rawUpdates` is an array of
// session/update `update` objects echoed verbatim before the text response.
import readline from 'node:readline';
import fs from 'node:fs';
import { spawn } from 'node:child_process';

const SESSION_ID = 'mock-session-1';

// Whether the current provider session was established via `session/load`
// (resume) rather than `session/new`. Per-process: a respawned child that the
// daemon resumes into sees `true`; a fresh `session/new` resets it. Drives the
// `failPromptIfLoadedRpcError` behavior (monorepo#940 poisoned-session e2e).
let sessionFromLoad = false;

// Per-process turn counter + the ids of prompts parked by `blockUntilCancel`.
// These persist across messages within ONE child, so a follow-up prompt landing
// with `promptCount > 1` proves the daemon resumed the SAME process (keep-alive)
// rather than respawning it.
let promptCount = 0;
const pendingPromptIds = [];
// Tool calls parked by `parkMidToolCall`; on `session/cancel` each gets a
// title-less failed `tool_call_update` echo before the prompt resolves (STAB-124).
const cancelledToolCallIds = [];

// Agent→client request correlation: track outgoing request IDs separately from
// incoming prompt IDs. The daemon responds to each client call with a matching id.
let nextClientCallId = 1;
const pendingClientCalls = new Map();

// MCP servers delivered in the `session/new` request's `mcpServers` field
// (STAB-156): providers like claude-code/codex/droid consume MCP servers from
// the ACP session setup instead of a `--mcp-config` file. Stashed here so
// `callWorkspaceTool` can spawn the bridge from either delivery mechanism.
let sessionMcpServers = [];

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

// Session-lifecycle log: one JSON line per session/new | session/load —
// { method, sessionId, pid } — when MOCK_AGENT_SESSION_LOG points at a file.
// Lets e2e tests assert exactly which session ids the daemon offered to which
// child process (e.g. that a cross-provider switch never issues session/load
// with the old provider's id — monorepo#907).
function logSessionCall(method, sessionId) {
  const path = process.env.MOCK_AGENT_SESSION_LOG;
  if (!path) return;
  try {
    fs.appendFileSync(path, JSON.stringify({ method, sessionId, pid: process.pid }) + '\n');
  } catch (err) {
    log(`session log write failed: ${err.message}`);
  }
}

function mcpConfigPath() {
  const argv = process.argv;
  const i = argv.indexOf('--mcp-config');
  return i >= 0 && i + 1 < argv.length ? argv[i + 1] : null;
}

// Resolve the workspace-mcp server entry from the generated `--mcp-config`
// file or the session-setup stash (STAB-156). Returns { command, args, env }
// or null when neither delivery mechanism carried the bridge.
function resolveWorkspaceMcpServer() {
  const path = mcpConfigPath();
  if (path) {
    const cfg = JSON.parse(fs.readFileSync(path, 'utf8'));
    const srv = (cfg.mcpServers && cfg.mcpServers['workspace-mcp']) || null;
    if (srv) return srv;
  }
  // ACP session-setup delivery: the `session/new` `mcpServers` array
  // carries untagged stdio entries { name, command, args, env: [{name,value}] }.
  // Also the fallback when an `--mcp-config` file exists but lacks the
  // bridge entry, so either delivery mechanism can win.
  const entry = sessionMcpServers.find((s) => s && s.name === 'workspace-mcp');
  if (entry) {
    return {
      command: entry.command,
      args: entry.args || [],
      env: Object.fromEntries((entry.env || []).map((e) => [e.name, e.value])),
    };
  }
  return null;
}

// Spawn the configured workspace-mcp server, perform an MCP initialize +
// tools/call, and resolve with the call response. Rejects on any transport error
// so the test fails loudly rather than silently skipping the mutation.
function callWorkspaceTool(toolCall) {
  return new Promise((resolve, reject) => {
    const srv = resolveWorkspaceMcpServer();
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

// Bridge-concurrency probe (monorepo#871): over ONE bridge connection, fire a
// long `tools/call` (agent JS that spins until `releaseFile` exists) and —
// while it is still in flight — a `tools/list` ping. The release file is only
// written AFTER the ping response arrives, so the long call can only complete
// if the bridge dispatched both requests concurrently; a serialized bridge
// deadlocks (ping parked behind the long call, which waits on the ping) and
// the probe times out. Resolves { toolCount, longCallOk }.
function runBridgeConcurrencyProbe(probe) {
  return new Promise((resolve, reject) => {
    const srv = resolveWorkspaceMcpServer();
    if (!srv) return reject(new Error('no workspace-mcp server in config'));
    log(`spawning bridge (concurrency probe): ${srv.command} ${(srv.args || []).join(' ')}`);
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
    const request = (id, method, params, timeoutMs = 20000) =>
      new Promise((res, rej) => {
        const timer = setTimeout(() => {
          pending.delete(id);
          rej(new Error(`bridge request timeout after ${timeoutMs}ms: ${method} id=${id}`));
        }, timeoutMs);
        pending.set(id, (msg) => {
          clearTimeout(timer);
          res(msg);
        });
        child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n');
      });

    (async () => {
      await request(1, 'initialize', {});
      // Long call fired WITHOUT awaiting: it blocks server-side until the
      // release file exists.
      const longCall = request(2, 'tools/call', {
        name: 'workspace_api',
        arguments: { code: probe.longCallCode, summary: 'long blocking call (concurrency probe)' },
      });
      longCall.catch(() => {}); // inspected below; avoid unhandled rejection
      // Liveness ping on the SAME connection while the long call is pending.
      // The old serialized bridge never answers this (deadlock → timeout).
      const toolsResp = await request(3, 'tools/list', {});
      const tools = (toolsResp.result && toolsResp.result.tools) || [];
      if (toolsResp.error || tools.length === 0) {
        throw new Error(`tools/list during long call failed: ${JSON.stringify(toolsResp)}`);
      }
      log(`tools/list answered during long call: ${tools.length} tool(s)`);
      // Ping answered → unblock the long call.
      fs.writeFileSync(probe.releaseFile, 'go');
      const longResp = await longCall;
      child.stdin.end();
      child.kill();
      if (longResp.error) {
        throw new Error(`long tools/call failed: ${JSON.stringify(longResp.error)}`);
      }
      resolve({ toolCount: tools.length, longCallOk: true });
    })().catch((err) => {
      child.stdin.end();
      child.kill();
      reject(err);
    });
  });
}

// Issue an agent→client request (fs/*, terminal/*, session/request_permission).
// Returns a promise that resolves with the response result or rejects with an error.
// The correlation is via the JSON-RPC id we assign (nextClientCallId).
// Includes a 30-second timeout to prevent hanging on non-replying daemon.
function callClientService(method, params, timeoutMs = 30000) {
  return new Promise((resolve, reject) => {
    const id = nextClientCallId++;
    const timeout = setTimeout(() => {
      pendingClientCalls.delete(id);
      reject(new Error(`client call timeout after ${timeoutMs}ms: ${method}`));
    }, timeoutMs);
    pendingClientCalls.set(id, {
      resolve: (result) => {
        clearTimeout(timeout);
        resolve(result);
      },
      reject: (err) => {
        clearTimeout(timeout);
        reject(err);
      },
    });
    send({ jsonrpc: '2.0', id, method, params });
  });
}

// Structural subset match: every key in `expected` must exist in `actual` with
// the same value (recursively for objects). Ignores extra keys in `actual`.
function structuralSubsetMatch(expected, actual) {
  if (expected === actual) return true;
  if (typeof expected !== 'object' || expected === null) return false;
  if (typeof actual !== 'object' || actual === null) return false;
  for (const key in expected) {
    if (!(key in actual)) return false;
    if (typeof expected[key] === 'object' && expected[key] !== null) {
      if (!structuralSubsetMatch(expected[key], actual[key])) return false;
    } else if (expected[key] !== actual[key]) {
      return false;
    }
  }
  return true;
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

// Optional `configOptions` on the session/new + session/load results.
// `MOCK_AGENT_THOUGHT_LEVEL=<currentValue>` advertises a reasoning-effort
// select under an adapter-specific id (`effort`, as claude-agent-acp names
// it) with `category: "thought_level"` — the category the daemon's generic
// effort application discovers it by (PROTOCOL §5.5). Omitted by default so
// existing tests see the bare `{ sessionId }` result.
function sessionConfigOptions() {
  const current = process.env.MOCK_AGENT_THOUGHT_LEVEL;
  if (!current) return {};
  return {
    configOptions: [
      {
        id: 'effort',
        name: 'Effort',
        category: 'thought_level',
        type: 'select',
        currentValue: current,
        options: [
          { value: 'low', name: 'Low' },
          { value: 'medium', name: 'Medium' },
          { value: 'high', name: 'High' },
        ],
      },
    ],
  };
}

async function handlePrompt(id, params) {
  promptCount += 1;
  // Record every prompt this child receives when MOCK_AGENT_PROMPT_LOG points
  // at a file — one JSON line per prompt ({ turn, text, blockTypes }) — so
  // e2e tests can assert exact outbound prompt assembly (e.g. the
  // FirstTurnPrepend `<system>` block fires on the first turn of a fresh
  // session and never repeats). `blockTypes` lists each prompt content
  // block's `type` in order, so tests can assert non-text blocks (e.g. a
  // redelivered image) reached the wire. Written BEFORE the turn resolves,
  // so a test that observed the turn's stream:end can read the line without
  // polling.
  const promptLog = process.env.MOCK_AGENT_PROMPT_LOG;
  if (promptLog) {
    try {
      const blocks = Array.isArray(params && params.prompt) ? params.prompt : [];
      fs.appendFileSync(
        promptLog,
        JSON.stringify({
          turn: promptCount,
          text: extractPromptText(params),
          blockTypes: blocks.map((b) => (b && typeof b.type === 'string' ? b.type : '')),
        }) + '\n',
      );
    } catch (err) {
      log(`prompt log write failed: ${err.message}`);
    }
  }
  let behavior = {};
  try {
    behavior = JSON.parse(process.env.MOCK_AGENT_BEHAVIOR || '{}');
  } catch {
    behavior = {};
  }
  // Deterministic mid-turn failure: die while the prompt is in flight for the
  // first N attempts (counter persists across spawns via MOCK_AGENT_ATTEMPT_FILE).
  // The exit closes stdout, so the daemon's pending session/prompt fails with
  // "agent stdout closed" — the mid-turn crash the STAB terminal-failure path
  // must surface (agent:failed + requeue for agent.retry).
  // The attempt counter is read at most once per prompt and shared by every
  // attempt-gated behavior below, so enabling several of them never
  // double-increments the counter and shifts a gating window.
  const exitGate =
    typeof behavior.exitDuringPromptAttempts === 'number' && behavior.exitDuringPromptAttempts > 0
      ? behavior.exitDuringPromptAttempts
      : 0;
  const rpcGate =
    typeof behavior.promptRpcErrorAttempts === 'number' && behavior.promptRpcErrorAttempts > 0
      ? behavior.promptRpcErrorAttempts
      : 0;
  const attempt = exitGate > 0 || rpcGate > 0 ? getAndIncrementAttempt() : 0;
  if (exitGate > 0 && attempt <= exitGate) {
    log(`exiting during prompt (attempt ${attempt}/${exitGate})`);
    process.exit(1);
  }
  // Provider prompt failure (monorepo#479): optionally stream one
  // agent_message_chunk (the provider's pre-failure warning) and then answer
  // `session/prompt` with the configured JSON-RPC error object — mirroring
  // codex-acp, which forwards its non-fatal "Model metadata … not found"
  // warning as agent text before the turn dies on a -32603 whose `data`
  // carries the real backend detail. When `promptRpcErrorAttempts` is set the
  // failure is attempt-gated (counter in MOCK_AGENT_ATTEMPT_FILE) so a retry
  // redrive can succeed; otherwise every prompt fails.
  if (behavior.promptRpcError) {
    const failing = rpcGate > 0 ? attempt <= rpcGate : true;
    if (failing) {
      if (typeof behavior.streamBeforeErrorText === 'string' && behavior.streamBeforeErrorText.length > 0) {
        note('session/update', {
          sessionId: SESSION_ID,
          update: {
            sessionUpdate: 'agent_message_chunk',
            content: { type: 'text', text: behavior.streamBeforeErrorText },
          },
        });
      }
      log(`failing session/prompt with JSON-RPC error ${behavior.promptRpcError.code}`);
      return send({ jsonrpc: '2.0', id, error: behavior.promptRpcError });
    }
  }
  // Poisoned-session shape (monorepo#940): deterministically fail EVERY prompt
  // on a session established via `session/load` with the configured JSON-RPC
  // error, while prompts on a fresh `session/new` succeed. Models a provider
  // whose resumed context is corrupted — resuming replays the rejection
  // forever; only a recreate (fresh session/new) recovers.
  if (behavior.failPromptIfLoadedRpcError && sessionFromLoad) {
    log(
      `failing session/prompt on load-established session with JSON-RPC error ${behavior.failPromptIfLoadedRpcError.code}`,
    );
    return send({ jsonrpc: '2.0', id, error: behavior.failPromptIfLoadedRpcError });
  }
  // STAB-114: Park BEFORE streaming any assistant content, so tests can interrupt
  // with zero output. Send session/update with agent_status (thinking) to establish
  // the session without emitting assistant content, then park. The live-turn will
  // have zero assistant blocks, triggering the combined-delivery path
  // (monorepo#1014: preempted message rides the interrupt turn's prompt).
  if (behavior.parkBeforeFirstChunk && promptCount === 1) {
    note('session/update', {
      sessionId: SESSION_ID,
      update: {
        sessionUpdate: 'agent_status',
        status: { type: 'in_progress', message: 'Thinking...' },
      },
    });
    pendingPromptIds.push(id);
    return;
  }
  // STAB-124: park MID-TOOL-CALL — emit a `tool_call` (in_progress) then park.
  // On `session/cancel` the mock echoes a title-less `tool_call_update`
  // (status failed, abort-error output) BEFORE resolving the parked prompt,
  // mirroring how a real provider reports the aborted tool. The daemon must
  // not fabricate an anonymous tool_use block from that stale echo.
  if (behavior.parkMidToolCall && promptCount === 1) {
    const toolCallId = 'tc_park_mid_tool';
    note('session/update', {
      sessionId: SESSION_ID,
      update: {
        sessionUpdate: 'tool_call',
        toolCallId,
        title: 'slow-tool: park until cancel',
        kind: 'execute',
        status: 'in_progress',
        rawInput: { marker: 'stab-124' },
      },
    });
    cancelledToolCallIds.push(toolCallId);
    pendingPromptIds.push(id);
    return;
  }
  // Keep-alive interrupt test: the FIRST turn streams a chunk then parks without
  // resolving, so the daemon can issue `agent.stop` mid-turn. It is left pending
  // until a `session/cancel` arrives; the child stays alive for the follow-up.
  if (behavior.blockUntilCancel && promptCount === 1) {
    // Newline-terminated: the daemon's mid-turn preview only surfaces
    // COMPLETED (newline-terminated) lines, and the live-turn overlay e2e
    // asserts this chunk is served while the turn is parked.
    note('session/update', {
      sessionId: SESSION_ID,
      update: { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: 'streaming-before-cancel\n' } },
    });
    pendingPromptIds.push(id);
    return;
  }
  // Idle-timeout warn-and-continue e2e: the first N turns go COMPLETELY silent
  // — no session/update at all — and park until `session/cancel` resolves them
  // (stopReason `cancelled`), modelling an agent whose turn hangs without any
  // activity. Turn-count-gated (not prompt-matched) so the daemon's injected
  // `[SYSTEM WARNING]` turns can be silenced too (the consecutive-timeout cap
  // path). Turns past N respond normally, with the `turn=N` stamp below
  // proving the daemon kept the SAME child alive across the timeouts.
  const silentTurns = Number.isFinite(behavior.silentUntilCancelTurns)
    ? behavior.silentUntilCancelTurns
    : 0;
  if (silentTurns > 0 && promptCount <= silentTurns) {
    log(`going silent until cancel (turn ${promptCount}/${silentTurns})`);
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
  // Suffix-matched park: like `parkIfPromptContains` but only when the prompt
  // ENDS with the marker. The daemon's session-recreation history replay
  // renders every prior row BEFORE the current message, so a marker from an
  // earlier turn appears mid-prompt on a fresh child's replayed prompt —
  // suffix matching parks only the turn whose OWN content carries the marker
  // (needed by the idle-timeout teardown e2e, where the warning turn on the
  // fresh child must respond normally despite the replayed marker).
  if (
    typeof behavior.parkIfPromptEndsWith === 'string' &&
    behavior.parkIfPromptEndsWith.length > 0 &&
    promptText.trimEnd().endsWith(behavior.parkIfPromptEndsWith)
  ) {
    log('parkIfPromptEndsWith: parking this turn');
    pendingPromptIds.push(id);
    return;
  }
  // Per-prompt exit: die mid-prompt whenever the prompt text carries the
  // configured marker (unconditional — every redrive of the marked prompt
  // dies again, so the silent-redrive budget is spent and the failure goes
  // terminal). Lets one MOCK_AGENT_BEHAVIOR drive a healthy delegating parent
  // (no match) and a child whose delegated instructions carry the marker and
  // must fail terminally (agent:failed).
  if (
    typeof behavior.exitIfPromptContains === 'string' &&
    behavior.exitIfPromptContains.length > 0 &&
    promptText.includes(behavior.exitIfPromptContains)
  ) {
    log(`exiting during prompt (marker "${behavior.exitIfPromptContains}" matched)`);
    process.exit(1);
  }
  const active = selectBehavior(behavior, promptText);
  // Bridge-concurrency probe (monorepo#871): { longCallCode, releaseFile }.
  // On success the turn resolves with the probe stats stamped into the text
  // response; any failure (deadlock timeout, tool error) resolves `refusal`.
  if (behavior.bridgeConcurrency) {
    try {
      const stats = await runBridgeConcurrencyProbe(behavior.bridgeConcurrency);
      note('session/update', {
        sessionId: SESSION_ID,
        update: {
          sessionUpdate: 'agent_message_chunk',
          content: {
            type: 'text',
            text: `bridge-concurrency ok toolCount=${stats.toolCount} longCallOk=${stats.longCallOk}`,
          },
        },
      });
      return result(id, { stopReason: 'end_turn' });
    } catch (err) {
      log(`bridge concurrency probe failed: ${err.message}`);
      return result(id, { stopReason: 'refusal' });
    }
  }
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
  const toolResults = [];
  for (const toolCall of toolCalls) {
    try {
      const res = await callWorkspaceTool(toolCall);
      log(`tool call ok: ${JSON.stringify(res).slice(0, 120)}`);
      toolResults.push({ toolCall, result: res });
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
      // If assertError was set, the call should have failed but succeeded instead
      if (call.assertError !== undefined) {
        log(`assertion failed: expected error but got success for ${call.method}`);
        return result(id, { stopReason: 'refusal' });
      }
      // Optional assertion on the result (structural subset match)
      if (call.assertResult !== undefined) {
        if (!structuralSubsetMatch(call.assertResult, resp)) {
          log(`assertion failed: expected subset ${JSON.stringify(call.assertResult)}, got ${JSON.stringify(resp)}`);
          return result(id, { stopReason: 'refusal' });
        }
      }
    } catch (err) {
      log(`client call failed: ${call.method} → ${err.message}`);
      // If the call defines assertError, verify it matches the expected error structure
      if (call.assertError !== undefined) {
        const errorData = err.errorData || {}; // JSON-RPC error from daemon
        const expectedCode = call.assertError.code;
        const expectedMessage = call.assertError.message;
        if (expectedCode !== undefined && errorData.code !== expectedCode) {
          log(`error code mismatch: expected ${expectedCode}, got ${errorData.code}`);
          return result(id, { stopReason: 'refusal' });
        }
        if (expectedMessage !== undefined && !String(errorData.message || '').includes(expectedMessage)) {
          log(`error message mismatch: expected substring "${expectedMessage}", got "${errorData.message}"`);
          return result(id, { stopReason: 'refusal' });
        }
        log(`client call error matched expectation: code=${errorData.code}`);
      } else {
        return result(id, { stopReason: 'refusal' });
      }
    }
  }
  const base = active.response || behavior.response || 'Mock agent completed.';
  // In keep-alive mode, stamp the turn count so a resumed follow-up turn is
  // distinguishable from a fresh spawn (which would report `turn=1`).
  // With echoCwd, stamp the child's working directory so e2e tests can assert
  // the daemon's spawn cwd (e.g. the dedicated chief-cwd dir, STAB-50).
  let text =
    behavior.blockUntilCancel || silentTurns > 0 ? `${base} turn=${promptCount}` : base;
  if (behavior.echoCwd) {
    text = `${text} cwd=${process.cwd()}`;
  }
  // Optional per-turn delay (MS) so a test can set up queue state during the
  // first turn before it resolves. Only applied to the FIRST turn so subsequent
  // queue-drained turns proceed at full speed.
  const delayMs = Number.isFinite(behavior.firstTurnDelayMs) ? behavior.firstTurnDelayMs : 0;
  if (delayMs > 0 && promptCount === 1) {
    await new Promise((r) => setTimeout(r, delayMs));
  }

  // Canned session/update sequence: each entry in active.rawUpdates is an
  // `update` object echoed verbatim as a session/update notification. Lets a
  // test drive exact tool_call / tool_call_update shapes (e.g. a completed
  // tool whose rawOutput carries a proposal-MIME resource item) without a
  // real MCP round-trip.
  const rawUpdates = Array.isArray(active.rawUpdates) ? active.rawUpdates : [];
  for (const update of rawUpdates) {
    note('session/update', { sessionId: SESSION_ID, update });
  }

  // Emit text response
  note('session/update', {
    sessionId: SESSION_ID,
    update: { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text } },
  });

  // Emit tool blocks if emitToolBlocks is enabled (opt-in for transcript persistence testing)
  if (active.emitToolBlocks && toolResults.length > 0) {
    for (const { toolCall, result } of toolResults) {
      // Emit tool_call notification (creates tool_use block in transcript)
      const toolCallId = `tc_${Math.random().toString(36).slice(2, 11)}`;
      note('session/update', {
        sessionId: SESSION_ID,
        update: {
          sessionUpdate: 'tool_call',
          toolCallId,
          title: toolCall.name,
          name: toolCall.name,
          kind: 'mcp',
          status: 'in_progress',
          rawInput: toolCall.arguments || {},
        },
      });

      // Emit tool_call_update with output (creates tool_result block in transcript).
      // The MCP result has { content: [...], isError?: boolean }. Each content item
      // can be text, image, or resource. The daemon will store the full array.
      //
      // With collapseToolOutput, emulate providers (e.g. auggie) that flatten
      // the MCP content items into `{ output: "<first text item's text>" }`,
      // dropping resource items entirely — the shape behind the proposal-lift
      // fallback (intent-hq/monorepo#511 regression class).
      //
      // With garbleToolOutput, emulate the worst-case provider echo: the
      // collapsed text is truncated and corrupted so neither the resource
      // item nor a parseable {ok, proposal} payload survives — only the
      // turn-attachment registry (deterministic attach) can recover it.
      if (result && result.content && Array.isArray(result.content)) {
        let rawOutput = result.content;
        if (active.collapseToolOutput || active.garbleToolOutput) {
          const firstText = result.content.find((c) => c && c.type === 'text');
          rawOutput = { output: firstText ? firstText.text : '' };
        }
        if (active.garbleToolOutput) {
          const text = String(rawOutput.output || '');
          rawOutput = { output: `[tool ran] ${text.slice(0, 40)}…(truncated by provider)` };
        }
        note('session/update', {
          sessionId: SESSION_ID,
          update: {
            sessionUpdate: 'tool_call_update',
            toolCallId,
            status: result.isError ? 'error' : 'completed',
            rawOutput,
          },
        });
      }
    }
  }

  // Optional end-of-turn token-usage snapshot (ACP `unstable_end_turn_token_usage`
  // extension): when the active behavior/rule carries a `usage` object it is
  // echoed verbatim on the PromptResponse, letting e2e tests drive the daemon's
  // live token-usage capture path (§5.23). Counts are cumulative per session.
  // An explicit `stopReason` on the active behavior/rule overrides the default
  // `end_turn`, letting e2e tests drive abnormal endings (`refusal`,
  // `max_tokens`, `max_turn_requests`) through the successful-turn path.
  const payload = {
    stopReason: typeof active.stopReason === 'string' ? active.stopReason : 'end_turn',
  };
  if (active.usage && typeof active.usage === 'object') {
    payload.usage = active.usage;
  }
  result(id, payload);
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
      // Slow cold-start simulation (monorepo#616): delay the initialize reply
      // by `initializeDelayMs` so tests can prove the daemon's handshake
      // timeout tolerates a slow-to-start agent (or trips when pinned lower).
      if (Number.isFinite(behavior.initializeDelayMs) && behavior.initializeDelayMs > 0) {
        log(`delaying initialize reply by ${behavior.initializeDelayMs}ms`);
        await new Promise((r) => setTimeout(r, behavior.initializeDelayMs));
      }
      // Advertise the `loadSession` capability when either knob opts in:
      // `loadSession: true` models the worst-case provider that silently
      // accepts a foreign session id (monorepo#907); `advertiseLoadSession`
      // makes the daemon's resume path (`session/load`) reachable for the
      // poisoned-session e2e (monorepo#940). Default stays false so existing
      // tests keep the no-resume behavior.
      return result(msg.id, {
        protocolVersion: 1,
        agentCapabilities: {
          loadSession: behavior.loadSession === true || behavior.advertiseLoadSession === true,
        },
      });
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
      // Stash the session-setup-delivered MCP servers (STAB-156) so
      // `callWorkspaceTool` can reach the bridge without an `--mcp-config`.
      // Always overwritten (defaulting to []) so a later session/new that
      // omits the field can't silently reuse a stale server list.
      sessionMcpServers = Array.isArray(msg.params && msg.params.mcpServers)
        ? msg.params.mcpServers
        : [];
      sessionFromLoad = false;
      logSessionCall('session/new', SESSION_ID);
      return result(msg.id, { sessionId: SESSION_ID, ...sessionConfigOptions() });
    }
    case 'session/load':
      // Mirror session/new's stash-overwrite so a loadSession-capable run (or
      // a test sending session/load first) can't observe a stale list.
      sessionMcpServers = Array.isArray(msg.params && msg.params.mcpServers)
        ? msg.params.mcpServers
        : [];
      logSessionCall('session/load', msg.params && msg.params.sessionId);
      // With `loadSession: true` behavior, accept ANY session id — including a
      // foreign one — modelling the worst-case provider monorepo#907 guards
      // against. With `advertiseLoadSession`, accept the resume (all
      // LoadSessionResponse fields are optional) and mark this session as
      // load-established so `failPromptIfLoadedRpcError` can fail the next
      // prompt (monorepo#940). Otherwise reject (capability was advertised
      // false anyway).
      if (behavior.loadSession === true || behavior.advertiseLoadSession === true) {
        if (behavior.advertiseLoadSession === true) {
          sessionFromLoad = true;
        }
        return result(msg.id, sessionConfigOptions());
      }
      return send({ jsonrpc: '2.0', id: msg.id, error: { code: -32601, message: 'no load' } });
    case 'session/set_mode':
      // Accept any mode change request (no-op for the mock).
      return result(msg.id, {});
    case 'session/set_config_option': {
      // Post-session model application for config-option-model providers
      // (claude-code-like). Record the exact wire params — one JSON line per
      // call ({ sessionId, configId, value }) — when MOCK_AGENT_CONFIG_LOG
      // points at a file, so e2e tests can assert the daemon issued the call
      // with the stored model exactly once per fresh session. The real
      // adapter's response echoes the updated configOptions list; the daemon
      // only checks for success, so a minimal echo suffices.
      const configLog = process.env.MOCK_AGENT_CONFIG_LOG;
      if (configLog) {
        try {
          fs.appendFileSync(configLog, JSON.stringify(msg.params || {}) + '\n');
        } catch (err) {
          log(`config log write failed: ${err.message}`);
        }
      }
      // Deterministic failure mode: reject the call (invalid params, e.g. an
      // unknown model id) so tests can assert the daemon logs a warning and
      // the turn still completes on the provider's default model.
      if (behavior.rejectSetConfigOption) {
        return send({
          jsonrpc: '2.0',
          id: msg.id,
          error: { code: -32602, message: 'unknown config value' },
        });
      }
      return result(msg.id, {
        configOptions: [
          {
            id: msg.params && msg.params.configId,
            currentValue: msg.params && msg.params.value,
          },
        ],
      });
    }
    case 'session/prompt':
      return handlePrompt(msg.id, msg.params);
    case 'session/cancel':
      // STAB-124: echo the abort for any tool call parked by `parkMidToolCall`
      // — a title-less `tool_call_update` (failed, abort-error output), the
      // shape real providers emit when a cancel lands mid-tool-call.
      while (cancelledToolCallIds.length) {
        note('session/update', {
          sessionId: SESSION_ID,
          update: {
            sessionUpdate: 'tool_call_update',
            toolCallId: cancelledToolCallIds.shift(),
            status: 'failed',
            rawOutput: { error: 'The operation was aborted' },
          },
        });
      }
      // Idle-timeout tail-bleed regression (monorepo#1599): with the opt-in
      // `tailAfterCancel` marker set, stream one trailing `agent_message_chunk`
      // carrying the marker BEFORE resolving the parked prompt — modelling a
      // child that emits late `session/update`s for the cancelled turn on its
      // ordered stdout ahead of the cancel response. Because stdout is ordered,
      // the straggler always precedes the resolving `result(...)` line — the
      // deterministic boundary the daemon's watermark drain relies on. Strictly
      // gated on the new key so every existing cancel behavior is unaffected.
      if (
        typeof behavior.tailAfterCancel === 'string' &&
        behavior.tailAfterCancel.length > 0 &&
        pendingPromptIds.length > 0
      ) {
        log(`emitting tail-after-cancel chunk: ${behavior.tailAfterCancel}`);
        note('session/update', {
          sessionId: SESSION_ID,
          update: {
            sessionUpdate: 'agent_message_chunk',
            content: { type: 'text', text: behavior.tailAfterCancel },
          },
        });
      }
      // Timeout→teardown regression (monorepo#1599 review): with the opt-in
      // `neverResolveOnCancel` flag set, leave parked prompts PENDING forever —
      // modelling a child that acknowledges nothing after `session/cancel` and
      // may keep streaming stragglers indefinitely. The daemon's watermark
      // await must time out and tear this child down instead of letting the
      // warning turn share its notifications channel. Strictly gated so every
      // existing cancel behavior is unaffected.
      if (behavior.neverResolveOnCancel) {
        log('neverResolveOnCancel: leaving parked prompt(s) unresolved');
        return;
      }
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

// Shutdown-reap e2e: when MOCK_AGENT_TREE_PID_FILE is set, spawn a long-lived
// grandchild (`sleep 300`) that inherits this process's group and record both
// pids ({ childPid, grandchildPid }) so the test can assert the daemon's
// shutdown kill sweep reaps the WHOLE provider tree (bridge-style grandchild
// included), not just the direct child.
const treePidFile = process.env.MOCK_AGENT_TREE_PID_FILE;
if (treePidFile) {
  const grandchild = spawn('sleep', ['300'], { stdio: 'ignore' });
  grandchild.on('error', (err) => log(`grandchild spawn failed: ${err.message}`));
  try {
    fs.writeFileSync(
      treePidFile,
      JSON.stringify({ childPid: process.pid, grandchildPid: grandchild.pid }) + '\n',
    );
  } catch (err) {
    log(`tree pid file write failed: ${err.message}`);
  }
}

// Harness-wake e2e (monorepo#855): when MOCK_AGENT_WAKE_TRIGGER_FILE is set,
// poll for the trigger file and — once it exists — emit one unsolicited
// `session/update` agent_message_chunk per non-empty trigger-file line,
// OUTSIDE any session/prompt. Deterministic stand-in for a provider harness
// waking the child on its own (compaction notice, background task output):
// the daemon must stream the burst as an implicit agent-initiated turn.
// One-shot per process; the test controls timing by creating the file.
const wakeTriggerFile = process.env.MOCK_AGENT_WAKE_TRIGGER_FILE;
if (wakeTriggerFile) {
  const poll = setInterval(() => {
    let lines;
    try {
      lines = fs
        .readFileSync(wakeTriggerFile, 'utf8')
        .split('\n')
        .filter((l) => l.trim().length > 0);
    } catch {
      return; // trigger not created yet
    }
    clearInterval(poll);
    log(`wake trigger fired: emitting ${lines.length} unsolicited chunk(s)`);
    for (const text of lines) {
      note('session/update', {
        sessionId: SESSION_ID,
        update: { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text } },
      });
    }
  }, 25);
  poll.unref?.();
}

// Dead-child recovery e2e (monorepo#764): when MOCK_AGENT_PID_FILE is set,
// append this spawn's pid (one line per process) so a test can SIGKILL the
// child out-of-band while the agent is idle and later prove the daemon
// respawned a FRESH process (a second, distinct pid line).
const pidFile = process.env.MOCK_AGENT_PID_FILE;
if (pidFile) {
  try {
    fs.appendFileSync(pidFile, String(process.pid) + '\n');
  } catch (err) {
    log(`pid file write failed: ${err.message}`);
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
  // JSON-RPC responses have no 'method' field.
  if (msg.id !== undefined && (msg.result !== undefined || msg.error !== undefined) && msg.method === undefined) {
    const pending = pendingClientCalls.get(msg.id);
    if (pending) {
      pendingClientCalls.delete(msg.id);
      if (msg.error) {
        const err = new Error(`JSON-RPC error ${msg.error.code}: ${msg.error.message}`);
        err.errorData = msg.error; // Attach full error object for assertions
        pending.reject(err);
      } else {
        pending.resolve(msg.result);
      }
      return;
    }
    // Unknown/expired id (e.g., after timeout cleanup) - drop the response instead of
    // dispatching it (responses have no 'method', so dispatch would fail/misbehave).
    log(`dropping response for unknown/expired id ${msg.id}`);
    return;
  }
  // Not a client-call response; dispatch it
  try {
    await dispatch(msg);
  } catch (err) {
    log(`dispatch error: ${err.message}`);
  }
});
