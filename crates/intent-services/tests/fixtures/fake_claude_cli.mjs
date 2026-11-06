#!/usr/bin/env node
// Stand-in Claude Code CLI for the claude_adapter_contract test.
//
// The real claude-agent-acp adapter selects it through CLAUDE_CODE_EXECUTABLE,
// so the Claude Agent SDK spawns it exactly as it would spawn the CLI. It makes
// no network calls and needs no account: it records its argv and every
// stream-json message the SDK writes to stdin (one JSON object per line in
// INTENT_FAKE_CLAUDE_LOG), answers control requests, and replies to each user
// turn with INTENT_FAKE_CLAUDE_REPLY.
import fs from "node:fs";
import readline from "node:readline";

const log = process.env.INTENT_FAKE_CLAUDE_LOG;
const reply = process.env.INTENT_FAKE_CLAUDE_REPLY ?? "FAKE-CLAUDE-REPLY";
const record = (entry) => fs.appendFileSync(log, JSON.stringify(entry) + "\n");
const send = (message) => process.stdout.write(JSON.stringify(message) + "\n");
const argv = process.argv.slice(2);

record({ kind: "argv", argv });

// Subcommands (`auth status --json`, `--version`, ...) are probes, not
// sessions: answer as a logged-out CLI and exit.
if (argv.length > 0 && !argv[0].startsWith("--")) {
  process.stdout.write(JSON.stringify({ loggedIn: false }) + "\n");
  process.exit(0);
}
if (argv.includes("--version")) {
  // Replaced on write with intent_providers::test_support::current_cli_version,
  // so the launch never treats this stand-in as too old.
  process.stdout.write("@CURRENT_CLI_VERSION@\n");
  process.exit(0);
}

let turn = 0;
readline.createInterface({ input: process.stdin }).on("line", (line) => {
  let message;
  try {
    message = JSON.parse(line);
  } catch {
    record({ kind: "unparsed-stdin", line });
    return;
  }
  record({ kind: "stdin", message });
  if (message.type === "control_request") {
    const response =
      message.request?.subtype === "initialize"
        ? {
            commands: [],
            agents: [],
            output_style: "default",
            available_output_styles: ["default"],
            models: [{ value: "default", displayName: "Default", description: "stand-in" }],
            account: {},
          }
        : {};
    send({
      type: "control_response",
      response: { subtype: "success", request_id: message.request_id, response },
    });
    return;
  }
  if (message.type !== "user") return;
  turn += 1;
  const session_id = message.session_id || "stand-in";
  const uuid = (n) => `00000000-0000-4000-8000-${String(turn * 10 + n).padStart(12, "0")}`;
  const usage = { input_tokens: 1, output_tokens: 1 };
  send({
    type: "system", subtype: "init", session_id, uuid: uuid(1), cwd: process.cwd(),
    tools: [], mcp_servers: [], model: "default", permissionMode: "default",
    slash_commands: [], apiKeySource: "none",
  });
  send({
    type: "assistant", session_id, uuid: uuid(2), parent_tool_use_id: null,
    message: {
      id: `msg_${turn}`, type: "message", role: "assistant", model: "default",
      content: [{ type: "text", text: reply }],
      stop_reason: "end_turn", stop_sequence: null, usage,
    },
  });
  send({
    type: "result", subtype: "success", session_id, uuid: uuid(3), is_error: false,
    result: reply, duration_ms: 1, duration_api_ms: 1, num_turns: 1, total_cost_usd: 0,
    usage, modelUsage: {}, permission_denials: [],
  });
});
