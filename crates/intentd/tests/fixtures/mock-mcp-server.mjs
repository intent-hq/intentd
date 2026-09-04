#!/usr/bin/env node
// Minimal mock MCP server for the `mcp.servers.*` UDS integration tests
// (PROTOCOL §5.22 / §18.3). Two modes:
//
// Default (stdio): newline-delimited JSON-RPC over stdio:
//   - `initialize`               → capability/serverInfo result
//   - `notifications/initialized`→ ignored (notification, no id)
//   - `tools/list`               → two tools (so toolCount == 2)
//   - `tools/call`               → text content `<name>:<args.input>` (echoes
//                                  the requested tool + input deterministically)
//   - `ping`                     → empty result (health check)
// Any other request id gets an empty result. Notifications are never answered.
//
// `--http`: MCP streamable-HTTP transport on 127.0.0.1 (ephemeral port). Every
// POSTed JSON-RPC request is answered with a plain JSON body via the same
// dispatch; notifications get 202. The bound port is announced on stdout as
// `PORT=<n>` so the harness can read it. With `--log-auth`, each POST also
// logs its Authorization header as `AUTH=<value|none>` so tests can assert
// the exact bearer token that crossed the wire.
//
// `--token`: mock OAuth token endpoint (RFC 6749 §6 refresh grant) on
// 127.0.0.1 (ephemeral port, announced as `PORT=<n>`). Every POST logs
// `HIT=<n> BODY=<form body>` and answers 200 with a fresh
// `refreshed-token-<n>` bag (lowercase `bearer`, `expires_in` 3600, rotated
// refresh token), so a test can count refreshes and assert the grant fields.

import { createInterface } from 'node:readline';
import { createServer } from 'node:http';

const TOOLS = [
  { name: 'echo', description: 'Echo back input', inputSchema: { type: 'object' } },
  { name: 'reverse', description: 'Reverse a string', inputSchema: { type: 'object' } },
];

// The JSON-RPC response for a request `msg`, or null for notifications.
function respond(msg) {
  if (msg.id === undefined || msg.id === null) {
    return null;
  }
  switch (msg.method) {
    case 'initialize':
      return {
        jsonrpc: '2.0',
        id: msg.id,
        result: {
          protocolVersion: '2024-11-05',
          capabilities: { tools: {} },
          serverInfo: { name: 'mock-mcp-server', version: '0.0.0' },
        },
      };
    case 'tools/list':
      return { jsonrpc: '2.0', id: msg.id, result: { tools: TOOLS } };
    case 'tools/call': {
      const name = msg.params?.name ?? 'unknown';
      const input = msg.params?.arguments?.input ?? '';
      return {
        jsonrpc: '2.0',
        id: msg.id,
        result: { content: [{ type: 'text', text: `${name}:${input}` }] },
      };
    }
    case 'ping':
      return { jsonrpc: '2.0', id: msg.id, result: {} };
    default:
      return { jsonrpc: '2.0', id: msg.id, result: {} };
  }
}

function runStdio() {
  const rl = createInterface({ input: process.stdin });
  rl.on('line', (line) => {
    const trimmed = line.trim();
    if (!trimmed) {
      return;
    }
    let msg;
    try {
      msg = JSON.parse(trimmed);
    } catch {
      return;
    }
    const reply = respond(msg);
    if (reply) {
      process.stdout.write(JSON.stringify(reply) + '\n');
    }
  });
}

function runHttp() {
  const logAuth = process.argv.includes('--log-auth');
  const server = createServer((req, res) => {
    if (req.method === 'DELETE') {
      res.writeHead(204).end();
      return;
    }
    if (logAuth && req.method === 'POST') {
      process.stdout.write(`AUTH=${req.headers.authorization ?? 'none'}\n`);
    }
    let body = '';
    req.on('data', (chunk) => (body += chunk));
    req.on('end', () => {
      let msg;
      try {
        msg = JSON.parse(body);
      } catch {
        res.writeHead(400).end();
        return;
      }
      const reply = respond(msg);
      if (!reply) {
        res.writeHead(202).end();
        return;
      }
      const payload = JSON.stringify(reply);
      res
        .writeHead(200, {
          'content-type': 'application/json',
          'mcp-session-id': 'mock-session',
        })
        .end(payload);
    });
  });
  server.listen(0, '127.0.0.1', () => {
    process.stdout.write(`PORT=${server.address().port}\n`);
  });
}

function runToken() {
  let hits = 0;
  const server = createServer((req, res) => {
    let body = '';
    req.on('data', (chunk) => (body += chunk));
    req.on('end', () => {
      hits += 1;
      process.stdout.write(`HIT=${hits} BODY=${body}\n`);
      const payload = JSON.stringify({
        access_token: `refreshed-token-${hits}`,
        token_type: 'bearer',
        expires_in: 3600,
        refresh_token: `rotated-refresh-${hits}`,
      });
      res.writeHead(200, { 'content-type': 'application/json' }).end(payload);
    });
  });
  server.listen(0, '127.0.0.1', () => {
    process.stdout.write(`PORT=${server.address().port}\n`);
  });
}

if (process.argv.includes('--token')) {
  runToken();
} else if (process.argv.includes('--http')) {
  runHttp();
} else {
  runStdio();
}
