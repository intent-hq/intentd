#!/usr/bin/env node
// Minimal ACP agent for the `intentd node` runner tests, driven by argv so a
// node configuration's provider `args` can select the behavior.
//
//   --exit-on-start <code>  exit immediately with <code>
//   --hang-setup            never answer session/new
//   --detached-helper       start the `sleep` child in its own process group
//   --dump-env              write its environment to `agent.env.json` in its
//                           working directory (profile-overlay tests)
//   --fail-setup-echo <VAR> fail session/new with an error quoting $VAR
//   --auth-required-setup   fail session/new with ACP's auth-required error
//
// A prompt "echo-env <VAR>" streams $VAR as a chunk and then fails the turn
// with an error quoting it (credential echo tests); "auth-required" fails
// the turn with ACP's auth-required error (sign-in status tests).
//
// On start it writes `agent.pid` (its pid and a same-group `sleep` child's
// pid) into its working directory, so tests can check that stop kills the
// whole process tree. A prompt whose text contains "block" parks until
// session/cancel; "slow" waits 300 ms; "many" streams 200 extra chunks;
// The prompt "exit" (exactly) answers and then exits with code 0, leaving its `sleep` child
// running; "spawn-exit" starts another `sleep` in its own process group,
// writes its pid to `late-helper.pid`, answers and exits at once; every turn
// first streams one agent_message_chunk "echo: <text>". A prompt
// "wsapi <code>" instead runs <code> through the session's workspace MCP
// server (the `--connect` address of its `workspace-mcp` entry in
// session/new) and streams "wsapi: <result text>" (or "wsapi-error: ...").
// "wsapi-tool <code>" does the same and also reports the call as a
// `workspace_api` tool_call / tool_call_update carrying the MCP content
// items, as a real provider does. "mcp <server> <tool> <json args>" runs the
// stdio command of the session/new entry named <server> (as a real provider
// starts a user MCP server), lists its tools and calls <tool>, and streams
// "mcp: {tools, result}" (or "mcp-error: ..."). session/new writes the
// servers it got to `session-mcp.json`. A `<system>` block in the prompt is
// ignored. "echo-skills" streams "skills: <names>" from every `<name>` of an
// `<available_skills>` catalog anywhere in the raw prompt (or "skills: none").
// A prompt with a block "echo-raw" streams "raw: {cwd, raw}" with its working
// directory and the whole raw prompt (instructions included).
import readline from 'node:readline';
import crypto from 'node:crypto';
import fs from 'node:fs';
import net from 'node:net';
import { spawn } from 'node:child_process';

const argv = process.argv.slice(2);
const exitAt = argv.indexOf('--exit-on-start');
if (exitAt >= 0) process.exit(Number(argv[exitAt + 1] ?? 1));
const hangSetup = argv.includes('--hang-setup');
const failSetupAt = argv.indexOf('--fail-setup-echo');
const failSetupVar = failSetupAt >= 0 ? argv[failSetupAt + 1] : undefined;
const detachedHelper = argv.includes('--detached-helper');
const authRequiredSetup = argv.includes('--auth-required-setup');
const AUTH_REQUIRED = { code: -32000, message: 'Authentication required' };
if (argv.includes('--dump-env')) fs.writeFileSync('agent.env.json', JSON.stringify(process.env));

const sleeper = spawn('sleep', ['300'], { stdio: 'ignore', detached: detachedHelper });
fs.writeFileSync('agent.pid', JSON.stringify({ agent: process.pid, child: sleeper.pid }));

const SESSION = 'node-test-session';
const parked = [];
let mcpServers = [];

// One workspace_api call over the bridge's newline-delimited JSON-RPC: by
// running the entry's stdio command (`intentd mcp-bridge --connect ...`, as a
// real provider does) when it is intentd, else (unit tests, whose command is
// the test binary) by connecting to its `--connect` address directly. The
// stdio path records the command it ran in `mcp-bridge.json`.
function workspaceApi(code) {
  const server = mcpServers.find((s) => s.name === 'workspace-mcp');
  if (!server) return Promise.resolve({ isError: true, content: [{ text: 'no workspace-mcp server' }] });
  return new Promise((resolve, reject) => {
    let input;
    let output;
    let done;
    if (/(^|\/)intentd$/.test(server.command)) {
      const env = { ...process.env };
      for (const { name, value } of server.env ?? []) env[name] = value;
      fs.writeFileSync('mcp-bridge.json', JSON.stringify({ command: server.command, args: server.args }));
      const child = spawn(server.command, server.args, { env, stdio: ['pipe', 'pipe', 'ignore'] });
      child.on('error', reject);
      input = child.stdin;
      output = child.stdout;
      done = () => child.kill();
    } else {
      const [host, port] = server.args[server.args.indexOf('--connect') + 1].split(':');
      const socket = net.connect(Number(port), host);
      socket.on('error', reject);
      // The listener refuses connections that do not first present the
      // per-agent secret the daemon put in the entry's env.
      const token = (server.env ?? []).find(({ name }) => name === 'INTENTD_MCP_BRIDGE_TOKEN')?.value;
      if (token) socket.write(`intentd-mcp-bridge-auth ${token}\n`);
      input = socket;
      output = socket;
      done = () => socket.end();
    }
    let buffered = '';
    output.on('data', (data) => {
      buffered += data;
      let newline;
      while ((newline = buffered.indexOf('\n')) >= 0) {
        const reply = JSON.parse(buffered.slice(0, newline));
        buffered = buffered.slice(newline + 1);
        if (reply.id === 2) {
          done();
          resolve(reply.result ?? { isError: true, content: [{ text: JSON.stringify(reply.error) }] });
        }
      }
    });
    const frame = (message) => input.write(JSON.stringify({ jsonrpc: '2.0', ...message }) + '\n');
    frame({ id: 1, method: 'initialize', params: { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'node-test-agent', version: '0' } } });
    frame({ id: 2, method: 'tools/call', params: { name: 'workspace_api', arguments: { code, summary: 'node test' } } });
  });
}

// initialize, tools/list and tools/call <tool> on the stdio MCP server named
// `name` in session/new, through its own command and env.
function mcpCall(name, tool, args) {
  const server = mcpServers.find((s) => s.name === name);
  if (!server) return Promise.reject(new Error(`no MCP server ${name}`));
  return new Promise((resolve, reject) => {
    const env = { ...process.env };
    for (const { name: key, value } of server.env ?? []) env[key] = value;
    const child = spawn(server.command, server.args, { env, stdio: ['pipe', 'pipe', 'ignore'] });
    child.on('error', reject);
    const replies = {};
    let buffered = '';
    child.stdout.on('data', (data) => {
      buffered += data;
      let newline;
      while ((newline = buffered.indexOf('\n')) >= 0) {
        const reply = JSON.parse(buffered.slice(0, newline));
        buffered = buffered.slice(newline + 1);
        replies[reply.id] = reply;
        // List, then call, as a provider does: the bridge answers requests
        // concurrently, so a call sent with the list can finish first.
        if (reply.id === 2) {
          frame({ id: 3, method: 'tools/call', params: { name: tool, arguments: args } });
        }
        if (reply.id === 3) {
          child.kill();
          resolve({ init: replies[1], tools: replies[2], call: replies[3] });
        }
      }
    });
    const frame = (message) => child.stdin.write(JSON.stringify({ jsonrpc: '2.0', ...message }) + '\n');
    frame({ id: 1, method: 'initialize', params: { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'node-test-agent', version: '0' } } });
    frame({ method: 'notifications/initialized' });
    frame({ id: 2, method: 'tools/list', params: {} });
  });
}

function send(message) {
  process.stdout.write(JSON.stringify({ jsonrpc: '2.0', ...message }) + '\n');
}

// The user's text: head's `<system>` instructions block (sent with a remote
// agent's first prompt) is dropped so its words never trigger a behavior.
function promptText(params) {
  return (params?.prompt ?? [])
    .map((block) => block?.text ?? '')
    .join('')
    .replace(/<system>[\s\S]*?<\/system>/g, '')
    .trim()
    // A message from another agent (a sub-agent's first prompt) carries a
    // header naming the sender; the behavior is the text after it.
    .replace(/^\[MESSAGE FROM AGENT [^\]]*\]\s*/, '');
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
      mcpServers = msg.params?.mcpServers ?? [];
      fs.writeFileSync('session-mcp.json', JSON.stringify(mcpServers));
      if (authRequiredSetup) return send({ id: msg.id, error: AUTH_REQUIRED });
      if (failSetupVar) {
        const value = process.env[failSetupVar];
        return send({
          id: msg.id,
          error: { code: -32000, message: `rejected credential ${value}`, data: { credential: value } },
        });
      }
      return send({ id: msg.id, result: { sessionId: SESSION } });
    case 'session/prompt': {
      const text = promptText(msg.params);
      if ((msg.params?.prompt ?? []).some((block) => promptText({ prompt: [block] }) === 'echo-raw')) {
        const raw = (msg.params?.prompt ?? []).map((block) => block?.text ?? '').join('\n');
        send({
          method: 'session/update',
          params: {
            sessionId: SESSION,
            update: {
              sessionUpdate: 'agent_message_chunk',
              content: { type: 'text', text: `raw: ${JSON.stringify({ cwd: process.cwd(), raw })}` },
            },
          },
        });
        return send({ id: msg.id, result: { stopReason: 'end_turn' } });
      }
      if (text === 'echo-skills') {
        const raw = (msg.params?.prompt ?? []).map((block) => block?.text ?? '').join('\n');
        const catalog = raw.match(/<available_skills>([\s\S]*?)<\/available_skills>/);
        const names = catalog ? [...catalog[1].matchAll(/<name>([^<]*)<\/name>/g)].map((m) => m[1]) : [];
        send({
          method: 'session/update',
          params: {
            sessionId: SESSION,
            update: {
              sessionUpdate: 'agent_message_chunk',
              content: { type: 'text', text: `skills: ${names.length ? names.join(',') : 'none'}` },
            },
          },
        });
        return send({ id: msg.id, result: { stopReason: 'end_turn' } });
      }
      const mcp = text.match(/^mcp (\S+) (\S+) (.*)$/);
      if (mcp) {
        let line;
        try {
          const { init, tools, call } = await mcpCall(mcp[1], mcp[2], JSON.parse(mcp[3]));
          line = `mcp: ${JSON.stringify({ capabilities: init?.result?.capabilities, tools: (tools?.result?.tools ?? []).map((t) => t.name), result: call.result, error: call.error })}`;
        } catch (error) {
          line = `mcp-error: ${error}`;
        }
        send({
          method: 'session/update',
          params: {
            sessionId: SESSION,
            update: { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: line } },
          },
        });
        return send({ id: msg.id, result: { stopReason: 'end_turn' } });
      }
      const asTool = text.startsWith('wsapi-tool ');
      if (asTool || text.startsWith('wsapi ')) {
        const code = text.slice(asTool ? 'wsapi-tool '.length : 'wsapi '.length);
        const toolCallId = `tc_${process.hrtime.bigint()}`;
        if (asTool) {
          send({
            method: 'session/update',
            params: {
              sessionId: SESSION,
              update: {
                sessionUpdate: 'tool_call',
                toolCallId,
                title: 'workspace_api',
                name: 'workspace_api',
                kind: 'mcp',
                status: 'in_progress',
                rawInput: { code, summary: 'node test agent call' },
              },
            },
          });
        }
        let line;
        try {
          const result = await workspaceApi(code);
          if (asTool) {
            send({
              method: 'session/update',
              params: {
                sessionId: SESSION,
                update: {
                  sessionUpdate: 'tool_call_update',
                  toolCallId,
                  status: result.isError ? 'failed' : 'completed',
                  rawOutput: result.content ?? [],
                },
              },
            });
          }
          const body = (result.content?.[0]?.text ?? '').replace(/\s+/g, ' ');
          line = `${result.isError ? 'wsapi-error' : 'wsapi'}: ${body}`;
        } catch (error) {
          line = `wsapi-error: ${error}`;
        }
        send({
          method: 'session/update',
          params: {
            sessionId: SESSION,
            update: { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: line } },
          },
        });
        return send({ id: msg.id, result: { stopReason: 'end_turn' } });
      }
      send({
        method: 'session/update',
        params: {
          sessionId: SESSION,
          update: { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: `echo: ${text}` } },
        },
      });
      if (text === 'auth-required') return send({ id: msg.id, error: AUTH_REQUIRED });
      // The SHA-256 of a variable, never its value: `unset` without one.
      // An optional tag marks this turn's reply.
      const digest = text.match(/^sha-env (\S+)(?: (\S+))?$/);
      if (digest) {
        const value = process.env[digest[1]];
        const shown = value === undefined ? 'unset' : crypto.createHash('sha256').update(value).digest('hex');
        send({
          method: 'session/update',
          params: {
            sessionId: SESSION,
            update: { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: `sha ${digest[2] ?? '-'} ${shown}` } },
          },
        });
        return send({ id: msg.id, result: { stopReason: 'end_turn' } });
      }
      const echo = text.match(/^echo-env (\S+)$/);
      if (echo) {
        const value = process.env[echo[1]];
        send({
          method: 'session/update',
          params: {
            sessionId: SESSION,
            update: { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: `token ${value}` } },
          },
        });
        return send({ id: msg.id, error: { code: -32000, message: `rejected credential ${value}`, data: { credential: value } } });
      }
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
      if (text.includes('spawn-exit')) {
        const late = spawn('sleep', ['300'], { stdio: 'ignore', detached: true });
        fs.writeFileSync('late-helper.pid', String(late.pid));
        send({ id: msg.id, result: { stopReason: 'end_turn' } });
        process.exit(0);
      }
      // Only the exact prompt: a wake message such as a script monitor's
      // "… exited with code 0" must not end the provider.
      if (text === 'exit') {
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
