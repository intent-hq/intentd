#!/usr/bin/env node
// Minimal mock MCP stdio server for the `mcp.servers.*` UDS integration test
// (PROTOCOL §5.22 / §18.3). Speaks newline-delimited JSON-RPC over stdio:
//   - `initialize`               → capability/serverInfo result
//   - `notifications/initialized`→ ignored (notification, no id)
//   - `tools/list`               → two tools (so toolCount == 2)
//   - `ping`                     → empty result (health check)
// Any other request id gets an empty result. Notifications are never answered.

import { createInterface } from 'node:readline';

const TOOLS = [
  { name: 'echo', description: 'Echo back input', inputSchema: { type: 'object' } },
  { name: 'reverse', description: 'Reverse a string', inputSchema: { type: 'object' } },
];

function send(message) {
  process.stdout.write(JSON.stringify(message) + '\n');
}

function handle(msg) {
  // Notifications (no id) are never answered.
  if (msg.id === undefined || msg.id === null) {
    return;
  }
  switch (msg.method) {
    case 'initialize':
      send({
        jsonrpc: '2.0',
        id: msg.id,
        result: {
          protocolVersion: '2024-11-05',
          capabilities: { tools: {} },
          serverInfo: { name: 'mock-mcp-server', version: '0.0.0' },
        },
      });
      break;
    case 'tools/list':
      send({ jsonrpc: '2.0', id: msg.id, result: { tools: TOOLS } });
      break;
    case 'ping':
      send({ jsonrpc: '2.0', id: msg.id, result: {} });
      break;
    default:
      send({ jsonrpc: '2.0', id: msg.id, result: {} });
      break;
  }
}

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
  handle(msg);
});
