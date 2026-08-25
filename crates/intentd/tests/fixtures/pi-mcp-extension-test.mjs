#!/usr/bin/env node
// Node-side test for the bundled pi MCP extension
// (intent-services/src/pi_mcp_extension.ts). Exercises the MCP client core
// against the mock-mcp-server.mjs fixture and the extension factory against a
// TCP bridge stand-in (per-connection mock child, socket↔stdio pipe — the
// mirror image of intentd's `mcp-bridge` proxy). Invoked from the Rust test
// `pi_mcp_extension_node.rs`:
//   node pi-mcp-extension-test.mjs <path to pi_mcp_extension.ts> <path to mock-mcp-server.mjs>
// Exits 0 on success, 1 on failure.

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtempSync, copyFileSync, rmSync } from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { PassThrough } from "node:stream";

const [extPath, mockPath] = process.argv.slice(2);
assert.ok(extPath && mockPath, "usage: pi-mcp-extension-test.mjs <ext.ts> <mock-mcp-server.mjs>");

// The extension is plain-JS-syntax ESM in a .ts file (loaded by pi via jiti);
// copy it to a .mjs so any ESM-capable node can import it, no type stripping
// needed.
const tmpDir = mkdtempSync(path.join(os.tmpdir(), "pi-mcp-ext-test-"));
const extMjs = path.join(tmpDir, "pi_mcp_extension.mjs");
copyFileSync(extPath, extMjs);
const ext = await import(pathToFileURL(extMjs).href);
const { McpLineClient, mapToolResult } = ext;

function spawnMock() {
  return spawn(process.execPath, [mockPath], { stdio: ["pipe", "pipe", "inherit"] });
}

// --- 1. Client core over the mock server's stdio -------------------------
{
  const child = spawnMock();
  const client = new McpLineClient(child.stdout, child.stdin);

  const init = await client.initialize({ name: "test", version: "0.0.0" });
  assert.equal(init.serverInfo.name, "mock-mcp-server", "initialize returns serverInfo");
  assert.ok(init.protocolVersion, "initialize returns protocolVersion");

  // Concurrent in-flight requests resolve independently.
  const [tools, ping] = await Promise.all([
    client.listTools(),
    client.request("ping", {}),
  ]);
  assert.deepEqual(ping, {}, "ping returns empty result");
  assert.deepEqual(
    tools.map((t) => t.name),
    ["echo", "reverse"],
    "tools/list returns the mock's two tools",
  );
  assert.equal(tools[0].inputSchema.type, "object", "tools carry a JSON-schema inputSchema");

  // tools/call round-trips (mock answers `<tool>:<args.input>` as text content).
  assert.deepEqual(await client.callTool("echo", { input: "x" }), {
    content: [{ type: "text", text: "echo:x" }],
  });

  // Server death rejects new requests instead of hanging.
  child.kill();
  await new Promise((resolve) => child.once("close", resolve));
  await new Promise((resolve) => setImmediate(resolve));
  assert.ok(client.isClosed(), "client observes the closed connection");
  await assert.rejects(client.request("ping", {}), /closed/);
}

// --- 2. Timeouts against a server that never answers ---------------------
{
  const silent = new McpLineClient(new PassThrough(), new PassThrough());
  await assert.rejects(
    silent.request("ping", {}, { timeoutMs: 50 }),
    /timed out/,
    "unanswered request times out",
  );
  silent.close();
}

// --- 3. mapToolResult mapping --------------------------------------------
{
  const ok = mapToolResult({
    content: [{ type: "text", text: "hi" }, { type: "image", data: "…" }],
    structuredContent: { a: 1 },
  });
  assert.equal(ok.content[0].text, "hi", "text content passes through");
  assert.equal(ok.content[1].type, "text", "non-text content is stringified");
  assert.deepEqual(ok.details, { a: 1 }, "structuredContent maps to details");

  assert.deepEqual(
    mapToolResult({}).content,
    [{ type: "text", text: "{}" }],
    "content-less results are stringified");

  assert.throws(
    () => mapToolResult({ isError: true, content: [{ type: "text", text: "boom" }] }),
    /boom/,
    "isError results throw so pi reports isError:true",
  );
}

// --- 4. Extension factory against a TCP bridge stand-in ------------------
const serverSockets = [];
const bridge = net.createServer((socket) => {
  serverSockets.push(socket);
  const child = spawnMock();
  socket.pipe(child.stdin);
  child.stdout.pipe(socket);
  socket.on("close", () => child.kill());
  socket.on("error", () => {});
});
await new Promise((resolve) => bridge.listen(0, "127.0.0.1", resolve));
const addr = `127.0.0.1:${bridge.address().port}`;

function fakePi() {
  return {
    tools: [],
    registerTool(def) {
      this.tools.push(def);
    },
    on() {},
  };
}

{
  process.env.INTENTD_MCP_BRIDGE_ADDR = addr;
  const pi = fakePi();
  await ext.default(pi);
  assert.deepEqual(
    pi.tools.map((t) => t.name),
    ["echo", "reverse"],
    "factory registers every bridge tool",
  );
  const echo = pi.tools[0];
  assert.equal(typeof echo.parameters, "object", "tool parameters carry the MCP inputSchema");
  assert.deepEqual(
    await echo.execute("tc-1", { input: "x" }),
    { content: [{ type: "text", text: "echo:x" }], details: {} },
    "execute forwards tools/call and maps the result",
  );

  // Mid-session drop: kill the bridge-side socket, next call reconnects.
  for (const s of serverSockets.splice(0)) s.destroy();
  await new Promise((resolve) => setTimeout(resolve, 50));
  const retried = await echo.execute("tc-2", { input: "y" });
  assert.equal(retried.content[0].text, "echo:y", "execute reconnects after a dropped connection");
}

// --- 5. Graceful degradation ----------------------------------------------
{
  // A throwing registerTool (e.g. name collision with a user-installed
  // extension's tool) skips that tool only; the rest still register.
  process.env.INTENTD_MCP_BRIDGE_ADDR = addr;
  const pi = fakePi();
  const realRegister = pi.registerTool.bind(pi);
  pi.registerTool = (def) => {
    if (def.name === "echo") throw new Error("name collision");
    realRegister(def);
  };
  await ext.default(pi);
  assert.deepEqual(
    pi.tools.map((t) => t.name),
    ["reverse"],
    "a throwing registerTool skips that tool only",
  );
}
{
  delete process.env.INTENTD_MCP_BRIDGE_ADDR;
  const pi = fakePi();
  await ext.default(pi);
  assert.equal(pi.tools.length, 0, "no env var: no tools, no crash");
}
{
  // A bound-then-closed listener yields a port with nothing listening.
  const dead = net.createServer();
  await new Promise((resolve) => dead.listen(0, "127.0.0.1", resolve));
  const deadAddr = `127.0.0.1:${dead.address().port}`;
  await new Promise((resolve) => dead.close(resolve));

  process.env.INTENTD_MCP_BRIDGE_ADDR = deadAddr;
  const pi = fakePi();
  await ext.default(pi);
  assert.equal(pi.tools.length, 0, "unreachable bridge: no tools, no crash");
}

for (const s of serverSockets) s.destroy();
await new Promise((resolve) => bridge.close(resolve));
rmSync(tmpDir, { recursive: true, force: true });
console.log("pi-mcp-extension-test: all assertions passed");
process.exit(0);
