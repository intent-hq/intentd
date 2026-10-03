// Explicit local da577fff fixture; this does not launch the native Claude CLI.
import { PassThrough, Readable, Writable } from "node:stream";
import { createInterface } from "node:readline";
import { pathToFileURL } from "node:url";
import { resolve } from "node:path";
import { readFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import assert from "node:assert/strict";

const root = process.argv[2];
const moduleAt = (path) => import(pathToFileURL(resolve(root, path)).href);
const { ScriptedQueryProcess } = await moduleAt("tests/scripted-query.mjs");
const { runAcp } = await moduleAt("dist/acp-agent.js");
const { ndJsonStream } = await moduleAt("node_modules/@agentclientprotocol/sdk/dist/acp.js");
const input = new PassThrough();
const quiet = { log() {}, error() {}, warn() {}, debug() {} };
const { agent } = runAcp(quiet, ndJsonStream(Writable.toWeb(process.stdout), Readable.toWeb(input)));
const processes = [];
const requests = [];
const allocated = [];
agent.probeCliAuthStatus = async () => null;
agent.claudeSubscriptionGuardActive = () => false;
agent.sendAvailableCommandsUpdate = async () => {};
const create = agent.createSession.bind(agent);
agent.createSession = async (params, options) => {
    const response = await create({ ...params, _meta: { ...params._meta, claudeCode: { options: {
        ...params._meta?.claudeCode?.options, settingSources: [],
        spawnClaudeCodeProcess(options) {
            const child = new ScriptedQueryProcess(options);
            processes.push(child);
            return child;
        },
    } } } }, options);
    allocated.push(agent.sessions[response.sessionId]);
    return response;
};

async function control(frame) {
    const p = frame.params ?? {};
    switch (frame.method) {
        case "fixture/inspect": return {
            requests, queries: processes.length,
            initializations: processes.map((child) => child.initializations),
            frames: processes.map((child) => child.frames),
        };
        case "fixture/call": return processes[p.query ?? 0].clients.get(p.name).rpc.request("tools/call", {
            name: "workspace_api", arguments: { code: p.code, summary: "callback delivery fixture" },
        });
        case "fixture/replace": return agent.createSession({ cwd: process.env.CALLBACK_TEST_ROOT, mcpServers: [] }, { reuseSessionId: p.sessionId });
        case "fixture/closed": agent.closeQueryStream(agent.sessions[p.sessionId]); return {};
        case "fixture/permissions":
            assert.throws(() => readFileSync("/etc/passwd"), { code: "ERR_ACCESS_DENIED" });
            assert.throws(() => spawnSync(process.execPath, ["--version"]), { code: "ERR_ACCESS_DENIED" });
            return { filesystem: "denied", native: "denied" };
        default: throw new Error("unknown fixture control");
    }
}
const lines = createInterface({ input: process.stdin });
lines.on("line", (line) => {
    const frame = JSON.parse(line);
    if (frame.method?.startsWith("fixture/")) {
        control(frame).then(
            (result) => process.stdout.write(`${JSON.stringify({ jsonrpc: "2.0", id: frame.id, result })}\n`),
            (error) => process.stdout.write(`${JSON.stringify({ jsonrpc: "2.0", id: frame.id, error: { code: -32000, message: error.message } })}\n`),
        );
    } else {
        requests.push({ method: frame.method, params: frame.params });
        input.write(`${line}\n`);
    }
});
lines.on("close", () => {
    for (const child of processes) child.kill();
    for (const session of allocated) agent.closeQueryStream(session);
    input.end();
});
