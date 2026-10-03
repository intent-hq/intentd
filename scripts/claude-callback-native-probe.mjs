import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:net";
import { readFileSync, readdirSync } from "node:fs";

const METHOD = "_intent/session/register_mcp_callback";
const LIMIT = 65536;
let bytes = 0;
function emit(value) {
  const line = JSON.stringify(value) + "\n";
  bytes += Buffer.byteLength(line);
  if (bytes > 196608) throw new Error("bounded transcript exceeded");
  process.stdout.write(line);
}
function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
const adapter = spawn(process.execPath, ["--max-old-space-size=256", "/runtime/runtime/dist/index.js"],
  { cwd: "/work", env: { ...process.env }, stdio: ["pipe", "pipe", "pipe"] });
emit({ event: "adapter-start", pid: adapter.pid, entry: "/runtime/runtime/dist/index.js" });
let serial = 0, buffer = "", stderrBytes = 0, connections = 0, stall = false;
const pending = new Map(), frames = [], sockets = new Set(), observed = new Set();
let failedInput;
function rejectPending(error) {
  failedInput = error;
  for (const p of pending.values()) { clearTimeout(p.timer); p.reject(error); }
  pending.clear();
}
adapter.on("error", rejectPending);
adapter.on("exit", (code, signal) => {
  emit({ event: "adapter-exit", code, signal });
  rejectPending(new Error("adapter exited"));
});
adapter.stderr.on("data", chunk => {
  stderrBytes += chunk.length;
  if (stderrBytes > 32768) { rejectPending(new Error("adapter stderr limit")); adapter.kill("SIGTERM"); return; }
  emit({ event: "adapter-stderr", text: chunk.toString("utf8") });
});
adapter.stdout.on("data", chunk => {
  buffer += chunk.toString("utf8");
  if (Buffer.byteLength(buffer) > LIMIT) { rejectPending(new Error("ACP frame limit")); adapter.kill("SIGTERM"); return; }
  while (buffer.includes("\n")) {
    const at = buffer.indexOf("\n"), line = buffer.slice(0, at);
    buffer = buffer.slice(at + 1);
    if (!line.trim()) continue;
    try {
      const message = JSON.parse(line);
      emit({ event: "acp-received", message });
      if (message.id !== undefined && !message.method) {
        const p = pending.get(message.id);
        if (!p) throw new Error("foreign ACP response id");
        pending.delete(message.id); clearTimeout(p.timer);
        message.error ? p.reject(Object.assign(new Error("ACP refusal"), { response: message.error }))
          : p.resolve(message.result);
      } else if (message.id !== undefined) {
        adapter.stdin.write(JSON.stringify({ jsonrpc: "2.0", id: message.id,
          error: { code: -32601, message: "No client actions available in offline probe" } }) + "\n");
      }
    } catch (error) { rejectPending(error); adapter.kill("SIGTERM"); }
  }
});
function request(method, params, timeout = 10000) {
  assert(["initialize", "session/new", METHOD].includes(method));
  if (failedInput) return Promise.reject(failedInput);
  const id = ++serial, message = { jsonrpc: "2.0", id, method, params };
  emit({ event: "acp-sent", message });
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { pending.delete(id); reject(new Error("ACP request deadline")); }, timeout);
    pending.set(id, { resolve, reject, timer });
    adapter.stdin.write(JSON.stringify(message) + "\n");
  });
}
const server = createServer(socket => {
  sockets.add(socket);
  const connection = ++connections, held = stall;
  emit({ event: "bridge-connection", connection, held });
  if (connections > 2) { socket.destroy(); rejectPending(new Error("unexpected bridge reconnect")); return; }
  let input = "";
  socket.on("error", error => emit({ event: "bridge-socket-error", message: error.message }));
  socket.on("close", () => sockets.delete(socket));
  socket.on("data", chunk => {
    input += chunk.toString("utf8");
    if (Buffer.byteLength(input) > LIMIT) { socket.destroy(); rejectPending(new Error("MCP frame limit")); return; }
    while (input.includes("\n")) {
      const at = input.indexOf("\n"), line = input.slice(0, at); input = input.slice(at + 1);
      if (!line.trim()) continue;
      try {
        const message = JSON.parse(line);
        frames.push({ connection, method: message.method, id: message.id });
        emit({ event: "mcp-received", connection, message });
        if (held || message.id === undefined) continue;
        let result, error;
        if (message.method === "initialize") {
          assert.equal(typeof message.params?.protocolVersion, "string");
          result = { protocolVersion: message.params.protocolVersion, capabilities: { tools: {} },
            serverInfo: { name: "isolated-empty-callback", version: "1" } };
        } else if (message.method === "tools/list") result = { tools: [] };
        else if (message.method === "ping") result = {};
        else error = { code: -32601, message: "No private tools in this endpoint" };
        const response = { jsonrpc: "2.0", id: message.id, ...(error ? { error } : { result }) };
        emit({ event: "mcp-sent", connection, message: response });
        socket.write(JSON.stringify(response) + "\n");
      } catch (error) { socket.destroy(); rejectPending(error); }
    }
  });
});
const monitor = setInterval(() => {
  for (const pid of readdirSync("/proc").filter(x => /^[0-9]+$/.test(x))) {
    try {
      const argv = readFileSync("/proc/" + pid + "/cmdline").toString().split("\0");
      const executable = argv[0] ?? "";
      let kind;
      if (executable === process.env.CLAUDE_CODE_EXECUTABLE)
        kind = argv.includes("auth") && argv.includes("status") ? "native-auth-status" : "native-query";
      else if (executable === "/bridge/intentd" && argv.includes("mcp-bridge")) kind = "real-mcp-bridge";
      if (kind && !observed.has(pid + ":" + kind)) {
        observed.add(pid + ":" + kind); emit({ event: "observed-process", pid: Number(pid), kind });
      }
    } catch { /* A sampled original process may already have exited. */ }
  }
}, 20);
const cases = { initialize: "not-reached", registration: "not-reached",
  foreign: "not-reached", duplicate: "not-reached", stalled: "not-reached" };
try {
  await new Promise((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  const endpoint = "127.0.0.1:" + server.address().port;
  const init = await request("initialize", { protocolVersion: 1,
    clientCapabilities: { _meta: { intentCallbackRegistration: { version: 1 } } },
    clientInfo: { name: "isolated-native-control-probe", version: "1" } });
  assert.deepEqual(init._meta?.intentCallbackRegistration, { version: 1, method: METHOD });
  const opened = await request("session/new", { cwd: "/work", mcpServers: [],
    _meta: { claudeCode: { options: { settingSources: [], strictMcpConfig: true,
      persistSession: false, tools: [], plugins: [], additionalDirectories: [],
      allowDangerouslySkipPermissions: false } } } }, 12000);
  const receipt = opened._meta?.intentCallbackRegistration?.queryReceipt;
  assert.equal(typeof opened.sessionId, "string"); assert.equal(typeof receipt, "string");
  assert(receipt.length > 0);
  cases.initialize = "passed";
  const original = { version: 1, sessionId: opened.sessionId, queryReceipt: receipt,
    registrationId: "native-original", server: { type: "stdio", command: "/bridge/intentd",
      args: ["mcp-bridge", "--connect", endpoint], env: { INTENTD_DATA_DIR: "/home/probe/intentd" } } };
  const rejected = async value => {
    try { await request(METHOD, value); throw new Error("invalid registration accepted"); }
    catch (error) { if (!error.response) throw error; emit({ event: "expected-refusal", error: error.response }); }
  };
  await rejected({ ...original, queryReceipt: "foreign-probe-receipt" });
  assert.equal(connections, 0); cases.foreign = "passed";
  const result = await request(METHOD, original, 7000);
  emit({ event: "registration-outcome", result });
  assert.equal(result.status, "acknowledged"); assert.equal(result.queryReceipt, receipt);
  assert.equal(result.sessionId, opened.sessionId);
  assert.equal(result.registrationId, original.registrationId);
  assert.deepEqual(result.result, { added: [result.serverName], removed: [], errors: {} });
  assert.equal(connections, 1);
  for (const method of ["initialize", "notifications/initialized", "tools/list"])
    assert(frames.some(x => x.connection === 1 && x.method === method), "missing real bridge " + method);
  cases.registration = "passed";
  await rejected(original); await new Promise(setImmediate);
  assert.equal(connections, 1); cases.duplicate = "passed";
  stall = true;
  const uncertain = await request(METHOD, { ...original, registrationId: "native-stalled" }, 7000);
  emit({ event: "stalled-outcome", result: uncertain });
  assert(["uncertain", "failed"].includes(uncertain.status));
  assert.equal(connections, 2);
  assert(frames.some(x => x.connection === 2 && x.method === "initialize"));
  cases.stalled = "passed";
  emit({ event: "probe-result", disposition: "completed", cases, connections,
    limit: "transport fixture; no Services authority or model turn" });
} catch (error) {
  emit({ event: "probe-result", disposition: "refused-or-incomplete", cases, connections,
    error: { message: String(error.message).slice(0, 2048), ...(error.response ? { response: error.response } : {}) } });
  process.exitCode = 1;
} finally {
  clearInterval(monitor);
  rejectPending(new Error("probe cancellation"));
  for (const socket of sockets) socket.destroy();
  server.close();
  emit({ event: "cancellation-requested", adapterPid: adapter.pid });
  adapter.kill("SIGTERM");
  await Promise.race([new Promise(resolve => adapter.once("exit", resolve)), delay(300)]);
  if (adapter.exitCode === null && adapter.signalCode === null) adapter.kill("SIGKILL");
}
