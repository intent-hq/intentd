//! Unit tests for the ephemeral one-shot ACP runner, driven by inline mock
//! adapters (node scripts speaking the same NDJSON JSON-RPC the real adapters
//! do) so no provider install is required.

use std::path::PathBuf;
use std::time::Duration;

use super::{run_one_shot_acp, OneShotCommand, OneShotError};

/// Write `body` as an executable-by-node mock adapter script and return a
/// launch command for it. The tempdir is returned so the caller keeps it
/// alive for the duration of the run.
fn mock_adapter(body: &str) -> (OneShotCommand, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let script = dir.path().join("mock-one-shot-adapter.mjs");
    std::fs::write(&script, body).expect("write mock adapter");
    let cmd = OneShotCommand::binary(
        PathBuf::from("node"),
        vec![script.to_string_lossy().into_owned()],
    );
    (cmd, dir)
}

/// Shared preamble: an NDJSON JSON-RPC loop that answers `initialize` and
/// `session/new`, then hands `session/prompt` to `onPrompt(id, msg)`.
const ADAPTER_PRELUDE: &str = r#"
import readline from 'node:readline';
const send = (o) => process.stdout.write(JSON.stringify(o) + '\n');
const result = (id, r) => send({ jsonrpc: '2.0', id, result: r });
const note = (method, params) => send({ jsonrpc: '2.0', method, params });
const chunk = (text) =>
  note('session/update', {
    sessionId: 's1',
    update: { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text } },
  });
const rl = readline.createInterface({ input: process.stdin, terminal: false });
rl.on('line', async (line) => {
  if (!line.trim()) return;
  const msg = JSON.parse(line);
  if (msg.method === 'initialize') return result(msg.id, { protocolVersion: 1, agentCapabilities: {} });
  if (msg.method === 'session/new') return result(msg.id, { sessionId: 's1' });
  if (msg.method === 'session/prompt') return onPrompt(msg.id, msg);
  if (msg.id !== undefined && msg.method === undefined) return onClientResponse(msg);
});
let onClientResponse = () => {};
"#;

#[tokio::test]
async fn one_shot_collects_streamed_reply_text() {
    let (cmd, _dir) = mock_adapter(&format!(
        "{ADAPTER_PRELUDE}
const onPrompt = (id) => {{
  chunk('Hello, ');
  chunk('world!');
  result(id, {{ stopReason: 'end_turn' }});
}};
"
    ));
    let text = run_one_shot_acp(cmd, "say hi", None, Duration::from_secs(30))
        .await
        .expect("one-shot succeeds");
    assert_eq!(text, "Hello, world!");
}

#[cfg(unix)]
#[tokio::test]
async fn prompt_timeout_reports_timeout_and_reaps_child() {
    // The adapter answers setup, then never resolves the prompt. The runner
    // must bound the prompt phase and leave no surviving process.
    let pidfile =
        std::env::temp_dir().join(format!("intent-one-shot-{}.pid", uuid::Uuid::new_v4()));
    let (cmd, _dir) = mock_adapter(&format!(
        "import fs from 'node:fs';
fs.writeFileSync({pidfile:?}, String(process.pid));
{ADAPTER_PRELUDE}
const onPrompt = () => {{}};
",
        pidfile = pidfile.to_string_lossy(),
    ));
    let err = run_one_shot_acp(cmd, "hang", None, Duration::from_millis(500))
        .await
        .unwrap_err();
    assert!(
        matches!(err, OneShotError::PromptTimeout),
        "expected PromptTimeout, got {err}"
    );

    let pid: i32 = std::fs::read_to_string(&pidfile)
        .expect("adapter wrote its pid")
        .trim()
        .parse()
        .expect("pid parses");
    std::fs::remove_file(&pidfile).ok();
    // `kill(pid, 0)` returns ESRCH once the reaped child is gone.
    for _ in 0..100 {
        if nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_err() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("adapter pid {pid} still alive after the one-shot timed out");
}

#[tokio::test]
async fn permission_request_is_auto_denied_and_turn_completes() {
    // The adapter asks for permission mid-turn and only finishes once it has
    // an answer: without the runner's auto-deny the turn would hang.
    let (cmd, _dir) = mock_adapter(&format!(
        "{ADAPTER_PRELUDE}
const onPrompt = (id) => {{
  onClientResponse = (msg) => {{
    chunk('denied=' + JSON.stringify(msg.result.outcome.outcome));
    result(id, {{ stopReason: 'end_turn' }});
  }};
  send({{
    jsonrpc: '2.0',
    id: 9001,
    method: 'session/request_permission',
    params: {{ sessionId: 's1', options: [] }},
  }});
}};
"
    ));
    let text = run_one_shot_acp(cmd, "touch a file", None, Duration::from_secs(30))
        .await
        .expect("one-shot succeeds after the auto-deny");
    assert_eq!(text, "denied=\"cancelled\"");
}

#[tokio::test]
async fn permission_request_during_setup_is_auto_denied() {
    // Regression: the adapter demands a permission answer BEFORE answering
    // `initialize` (and again before `session/new`). Without request
    // servicing during the setup phase these hang into a misreported
    // SetupTimeout instead of the documented auto-deny.
    let (cmd, _dir) = mock_adapter(&format!(
        "{ADAPTER_PRELUDE}
rl.removeAllListeners('line');
let denials = 0;
rl.on('line', (line) => {{
  if (!line.trim()) return;
  const msg = JSON.parse(line);
  if (msg.method === 'initialize' || msg.method === 'session/new') {{
    onClientResponse = (resp) => {{
      denials += 1;
      if (msg.method === 'initialize') return result(msg.id, {{ protocolVersion: 1, agentCapabilities: {{}} }});
      return result(msg.id, {{ sessionId: 's1' }});
    }};
    return send({{
      jsonrpc: '2.0',
      id: 9000 + denials,
      method: 'session/request_permission',
      params: {{ sessionId: 's1', options: [] }},
    }});
  }}
  if (msg.method === 'session/prompt') {{
    chunk('setup-denials=' + denials);
    return result(msg.id, {{ stopReason: 'end_turn' }});
  }}
  if (msg.id !== undefined && msg.method === undefined) return onClientResponse(msg);
}});
"
    ));
    let text = run_one_shot_acp(cmd, "hello", None, Duration::from_secs(30))
        .await
        .expect("setup-phase permission requests are auto-denied, not hung");
    assert_eq!(text, "setup-denials=2");
}

#[tokio::test]
async fn config_option_model_is_applied_after_session_new() {
    // A requested model for a provider with no CLI model flag rides
    // `session/set_config_option { configId: "model" }` between `session/new`
    // and `session/prompt`; the mock echoes what it received into the reply.
    let (cmd, _dir) = mock_adapter(&format!(
        "{ADAPTER_PRELUDE}
let applied = 'none';
rl.on('line', (line) => {{
  if (!line.trim()) return;
  const msg = JSON.parse(line);
  if (msg.method === 'session/set_config_option') {{
    applied = msg.params.configId + '=' + msg.params.value + '@' + msg.params.sessionId;
    return result(msg.id, {{}});
  }}
}});
const onPrompt = (id) => {{
  chunk('applied=' + applied);
  result(id, {{ stopReason: 'end_turn' }});
}};
"
    ));
    let text = run_one_shot_acp(cmd, "hello", Some("opus-x"), Duration::from_secs(30))
        .await
        .expect("one-shot succeeds");
    assert_eq!(text, "applied=model=opus-x@s1");
}

#[tokio::test]
async fn rejected_config_option_model_does_not_fail_the_completion() {
    // Best-effort contract: an adapter that rejects the model option (e.g.
    // unknown id or unsupported method) must not fail the completion — the
    // turn proceeds on the adapter's default model.
    let (cmd, _dir) = mock_adapter(&format!(
        "{ADAPTER_PRELUDE}
rl.on('line', (line) => {{
  if (!line.trim()) return;
  const msg = JSON.parse(line);
  if (msg.method === 'session/set_config_option') {{
    return send({{ jsonrpc: '2.0', id: msg.id, error: {{ code: -32601, message: 'nope' }} }});
  }}
}});
const onPrompt = (id) => {{
  chunk('default-model-reply');
  result(id, {{ stopReason: 'end_turn' }});
}};
"
    ));
    let text = run_one_shot_acp(cmd, "hello", Some("bogus-model"), Duration::from_secs(30))
        .await
        .expect("a rejected set_config_option must not fail the one-shot");
    assert_eq!(text, "default-model-reply");
}

#[cfg(unix)]
#[tokio::test]
async fn nonzero_exit_surfaces_typed_exited_error() {
    let cmd = OneShotCommand::binary(
        PathBuf::from("/bin/sh"),
        vec!["-c".to_string(), "echo boom >&2; exit 7".to_string()],
    );
    let err = run_one_shot_acp(cmd, "anything", None, Duration::from_secs(30))
        .await
        .unwrap_err();
    let OneShotError::Exited(detail) = err else {
        panic!("expected Exited, got {err}");
    };
    assert!(detail.contains("boom"), "detail: {detail}");
}

#[cfg(unix)]
#[tokio::test]
async fn garbage_stdout_surfaces_typed_transport_error() {
    // An adapter that never speaks JSON-RPC and exits 0: the pending
    // `initialize` fails when stdout closes, and the clean exit keeps the
    // failure attributed to the transport rather than a crash.
    let cmd = OneShotCommand::binary(
        PathBuf::from("/bin/sh"),
        vec!["-c".to_string(), "echo not json; exit 0".to_string()],
    );
    let err = run_one_shot_acp(cmd, "anything", None, Duration::from_secs(30))
        .await
        .unwrap_err();
    assert!(
        matches!(err, OneShotError::Transport(_)),
        "expected Transport, got {err}"
    );
}

#[tokio::test]
async fn missing_adapter_binary_surfaces_typed_spawn_error() {
    let cmd = OneShotCommand::binary(
        PathBuf::from("/nonexistent/intentd-one-shot-adapter"),
        Vec::new(),
    );
    let err = run_one_shot_acp(cmd, "anything", None, Duration::from_secs(5))
        .await
        .unwrap_err();
    assert!(
        matches!(err, OneShotError::Spawn(_)),
        "expected Spawn, got {err}"
    );
}
