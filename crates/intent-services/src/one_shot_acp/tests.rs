//! Unit tests for the ephemeral one-shot ACP runner, driven by inline mock
//! adapters (node scripts speaking the same NDJSON JSON-RPC the real adapters
//! do) so no provider install is required.

use std::path::PathBuf;
use std::task::Poll;
use std::time::Duration;

use intent_acp::{Connection, ConnectionHooks, IncomingRequest};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, Lines};
use tokio::sync::mpsc;

#[cfg(unix)]
use super::run_one_shot_acp_in;
use super::{run_one_shot_acp, serve_requests_while, OneShotCommand, OneShotError, Responder};
#[cfg(unix)]
use crate::acp_adapter::AdapterSlots;
use crate::test_support::test_tempdir;

/// Write `body` as an executable-by-node mock adapter script and return a
/// launch command for it. The tempdir is returned so the caller keeps it
/// alive for the duration of the run.
fn mock_adapter(body: &str) -> (OneShotCommand, tempfile::TempDir) {
    let dir = test_tempdir("intent-one-shot-");
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
const ADAPTER_PRELUDE: &str = r"
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
";

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
    let text = run_one_shot_acp(cmd, "say hi", None, None, Duration::from_secs(30))
        .await
        .expect("one-shot succeeds");
    assert_eq!(text, "Hello, world!");
}

/// Mock adapter that echoes the `session/new` params it received as the
/// reply, so the wire shape of setup is assertable.
const SESSION_NEW_ECHO_ADAPTER: &str = r"
let sessionNew = null;
rl.on('line', (line) => {
  if (!line.trim()) return;
  const msg = JSON.parse(line);
  if (msg.method === 'session/new') sessionNew = msg.params;
});
const onPrompt = (id) => {
  chunk(JSON.stringify(sessionNew));
  result(id, { stopReason: 'end_turn' });
};
";

#[tokio::test]
async fn session_meta_rides_session_new_verbatim() {
    let (cmd, _dir) = mock_adapter(&format!("{ADAPTER_PRELUDE}{SESSION_NEW_ECHO_ADAPTER}"));
    let meta = serde_json::json!({
        "systemPrompt": "utility",
        "claudeCode": { "options": { "tools": [] } },
    });
    let text = run_one_shot_acp(
        cmd,
        "hello",
        None,
        Some(meta.clone()),
        Duration::from_secs(30),
    )
    .await
    .expect("one-shot succeeds");
    let params: serde_json::Value = serde_json::from_str(&text).expect("echoed params parse");
    assert_eq!(params["_meta"], meta);
    assert_eq!(params["mcpServers"], serde_json::json!([]));
}

#[tokio::test]
async fn session_new_omits_meta_when_none_given() {
    let (cmd, _dir) = mock_adapter(&format!("{ADAPTER_PRELUDE}{SESSION_NEW_ECHO_ADAPTER}"));
    let text = run_one_shot_acp(cmd, "hello", None, None, Duration::from_secs(30))
        .await
        .expect("one-shot succeeds");
    let params: serde_json::Value = serde_json::from_str(&text).expect("echoed params parse");
    assert!(
        params.get("_meta").is_none(),
        "no session_meta must mean no `_meta` key at all, got: {params}"
    );
    assert_eq!(params["mcpServers"], serde_json::json!([]));
}

#[cfg(unix)]
#[tokio::test]
async fn prompt_timeout_reports_timeout_and_reaps_child() {
    // The adapter answers setup, then never resolves the prompt. The runner
    // must bound the prompt phase and leave no surviving process.
    let scratch = test_tempdir("intent-one-shot-pid-");
    let pidfile = scratch.path().join("adapter.pid");
    let (cmd, _dir) = mock_adapter(&format!(
        "import fs from 'node:fs';
fs.writeFileSync({pidfile:?}, String(process.pid));
{ADAPTER_PRELUDE}
const onPrompt = () => {{}};
",
        pidfile = pidfile.to_string_lossy(),
    ));
    // A private single-slot bound keeps the deliberately short budget honest:
    // under full-suite load the process-global bound can be saturated by
    // sibling tests for longer than 500ms, which would turn the asserted
    // PromptTimeout into a QueueTimeout (monorepo#2379).
    let slots = AdapterSlots::new(1);
    let err = run_one_shot_acp_in(&slots, cmd, "hang", None, None, Duration::from_millis(500))
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
    // `kill(pid, 0)` returns ESRCH once the reaped child is gone.
    for _ in 0..100 {
        if nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_err() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("adapter pid {pid} still alive after the one-shot timed out");
}

/// Regression (monorepo#5465): a flooding adapter that stops READING its
/// stdin while issuing thousands of client-served requests
/// (`session/request_permission` and an unknown method, alternating) fills
/// the transport's bounded writer channel (256 lines) plus the OS pipe.
/// Before the fix the prompt-phase `select!` awaited each `auto_respond`
/// inline, so the stalled response send starved the `session/prompt` timeout
/// and the one-shot hung until the child exited (on main this test only ends
/// when the 15 s harness guard fires). The phase budget must stay the hard
/// ceiling: the caller sees `PromptTimeout` within budget + a bounded margin
/// and the child is reaped.
#[cfg(unix)]
#[tokio::test]
async fn flooding_non_reading_adapter_cannot_stall_prompt_past_its_budget() {
    let scratch = test_tempdir("intent-one-shot-flood-");
    let pidfile = scratch.path().join("adapter.pid");
    let (cmd, _dir) = mock_adapter(&format!(
        "import fs from 'node:fs';
fs.writeFileSync({pidfile:?}, String(process.pid));
{ADAPTER_PRELUDE}
const onPrompt = () => {{
  // Stop consuming stdin (keep it open) and flood client-served requests
  // whose responses nobody will ever read; never resolve the prompt. Node
  // queues whatever the stdout pipe cannot take immediately and keeps
  // writing as the runner's reader drains it, so the flood reaches the
  // runner early in the prompt budget, and the runner's 256-line writer
  // channel + the unread stdin pipe wedge well inside it.
  rl.pause();
  process.stdin.pause();
  for (let i = 0; i < 6000; i++) {{
    if (i % 2 === 0) {{
      send({{ jsonrpc: '2.0', id: 10000 + i, method: 'session/request_permission', params: {{ sessionId: 's1', options: [] }} }});
    }} else {{
      send({{ jsonrpc: '2.0', id: 10000 + i, method: 'x/unknown', params: {{}} }});
    }}
  }}
  setInterval(() => {{}}, 1000);
}};
",
        pidfile = pidfile.to_string_lossy(),
    ));
    let budget = Duration::from_millis(500);
    // Upper bound = budget + spawn/setup + exit-observe/reap grace, with slack
    // for a loaded host. Observed ~1.8 s on a quiet host; the hang this guards
    // against never ends on its own.
    let margin = Duration::from_secs(4);
    let slots = AdapterSlots::new(1);
    let started = std::time::Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(15),
        run_one_shot_acp_in(&slots, cmd, "flood", None, None, budget),
    )
    .await;
    let elapsed = started.elapsed();
    let err = outcome
        .expect("a flooding non-reading adapter must not stall the one-shot past its budget")
        .unwrap_err();
    assert!(
        matches!(err, OneShotError::PromptTimeout),
        "expected PromptTimeout, got {err}"
    );
    assert!(elapsed >= budget, "finished before the budget: {elapsed:?}");
    assert!(
        elapsed < budget + margin,
        "PromptTimeout arrived {elapsed:?} after start; budget {budget:?} + margin {margin:?}"
    );

    let pid: i32 = std::fs::read_to_string(&pidfile)
        .expect("adapter wrote its pid")
        .trim()
        .parse()
        .expect("pid parses");
    for _ in 0..100 {
        if nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_err() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("adapter pid {pid} still alive after the one-shot timed out");
}

/// An in-memory [`Connection`] whose adapter side the test drives directly:
/// `peer_in` yields the lines the runner writes to the adapter's stdin and
/// `peer_out` feeds the adapter's stdout. `pipe` bounds each direction in
/// bytes, so a peer that never reads `peer_in` saturates the writer
/// deterministically. The unbounded receiver is the runner's client-served
/// request queue.
fn in_memory_connection(
    pipe: usize,
) -> (
    Connection,
    mpsc::UnboundedReceiver<IncomingRequest>,
    Lines<BufReader<DuplexStream>>,
    DuplexStream,
) {
    let (runner_stdin, peer_in) = tokio::io::duplex(pipe);
    let (peer_out, runner_stdout) = tokio::io::duplex(pipe);
    let (tx, rx) = mpsc::unbounded_channel();
    let hooks = ConnectionHooks {
        requests: Some(tx),
        ..ConnectionHooks::default()
    };
    let conn = Connection::new(runner_stdin, runner_stdout, None, hooks);
    (conn, rx, BufReader::new(peer_in).lines(), peer_out)
}

/// Poll `fut` exactly once. A `Connection` request registers its pending
/// slot and queues its line on that first poll, so this starts a phase
/// request without driving it any further.
async fn poll_once<F: std::future::Future>(fut: std::pin::Pin<&mut F>) -> Poll<F::Output> {
    let mut fut = Some(fut);
    std::future::poll_fn(move |cx| {
        Poll::Ready(
            fut.take()
                .expect("poll_once's future is polled exactly once")
                .poll(cx),
        )
    })
    .await
}

/// Wedge `conn`'s writer while its peer is not reading: with the pipe full
/// the writer task blocks inside a line, and further sends fill the bounded
/// channel until one no longer completes. Returns the number of `x/fill`
/// notifications that were accepted.
async fn saturate_writer(conn: &Connection) -> usize {
    let mut queued = 0usize;
    loop {
        assert!(queued < 10_000, "writer never saturated");
        let fill = conn.notify("x/fill", json!({}));
        tokio::pin!(fill);
        match poll_once(fill.as_mut()).await {
            Poll::Ready(res) => res.expect("writer open"),
            // Pending is not yet proof: the writer task may still be moving a
            // line out of the channel, or the task's cooperative budget may
            // be spent. Yield (which also refills the budget); the send that
            // stays pending after that is blocked by a full channel behind a
            // full pipe, which nothing drains while the peer is not reading.
            Poll::Pending => {
                tokio::task::yield_now().await;
                if poll_once(fill.as_mut()).await.is_pending() {
                    break;
                }
            }
        }
        queued += 1;
    }
    assert!(
        queued > 1,
        "expected the channel to hold lines before saturating"
    );
    queued
}

/// Feed `conn` the given client-served requests from the peer and wait until
/// the reader has forwarded all of them to the request queue.
async fn feed_requests(conn: &Connection, peer_out: &mut DuplexStream, requests: &[Value]) {
    let seq = conn.client_request_seq();
    let mut lines = String::new();
    for req in requests {
        lines.push_str(&req.to_string());
        lines.push('\n');
    }
    peer_out
        .write_all(lines.as_bytes())
        .await
        .expect("peer write");
    let expected = seq + requests.len() as u64;
    tokio::time::timeout(Duration::from_secs(5), async {
        while conn.client_request_seq() < expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reader forwards every request");
}

fn permission_request(id: u64) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "method": "session/request_permission",
            "params": { "sessionId": "s1", "options": [] } })
}

/// Regression for the bounded-send change: a response send that is still
/// `Pending` when a phase resolves must be carried into the next phase, not
/// dropped at the boundary. `Pending` there says nothing about the writer —
/// Tokio's cooperative budget makes a bounded `send` yield even on an empty
/// channel, and a full writer drains as soon as the adapter reads — so the
/// old inline await's guarantee (a well-behaved adapter's request is always
/// answered) only survives if the carried send keeps being polled. Here the
/// writer is genuinely full while the first phase resolves (the peer is not
/// reading), then the peer drains it during the next phase; the answer must
/// arrive. Driven in memory with one `Responder` across both phases, as the
/// runner's setup → model → prompt sequence does; the loop treats a phase
/// resolving `Ok` and on its budget identically, and the budget is what
/// makes the boundary ordering exact.
#[tokio::test]
async fn response_pending_at_a_phase_boundary_is_carried_into_the_next_phase() {
    let (conn, mut requests, mut peer_in, mut peer_out) = in_memory_connection(256);
    let mut responder = Responder::new(&conn);
    saturate_writer(&conn).await;
    feed_requests(&conn, &mut peer_out, &[permission_request(9001)]).await;

    // First phase: the request is dequeued, its send pends on capacity, and
    // the phase resolves while it is in flight.
    serve_requests_while(
        &mut responder,
        &mut requests,
        tokio::time::timeout(Duration::from_millis(50), std::future::pending::<()>()),
    )
    .await
    .expect_err("the phase clock ends the first phase");
    assert!(
        responder.in_flight(),
        "the pending send must survive the phase boundary"
    );
    assert!(requests.try_recv().is_err(), "the request was dequeued");

    // Next phase on the same connection: the peer now reads, the writer
    // drains, and the carried send must complete and reach the peer.
    let answer = serve_requests_while(
        &mut responder,
        &mut requests,
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let line = peer_in
                    .next_line()
                    .await
                    .expect("peer read")
                    .expect("peer line");
                let msg: Value = serde_json::from_str(&line).expect("json");
                if msg["id"] == json!(9001) {
                    return msg;
                }
                assert_eq!(msg["method"], "x/fill", "unexpected line: {line}");
            }
        }),
    )
    .await
    .expect("the send carried across the phase boundary was never answered");
    assert_eq!(answer["result"]["outcome"]["outcome"], "cancelled");
    assert!(!responder.in_flight());
}

/// Deterministic backpressure counterpart of the flood regression above: the
/// writer is provably wedged BEFORE the phase starts, so the response send
/// the loop starts is pending on writer capacity for the whole budget. A
/// 1-byte pipe nobody reads blocks the writer task inside its first line;
/// further sends then fill the bounded channel until one no longer completes
/// (see [`saturate_writer`]). Before the fix `serve_requests_while` awaited
/// that send inline and never returned; the phase must resolve on its budget
/// regardless.
#[tokio::test]
async fn phase_resolves_on_budget_while_a_response_send_is_pending_on_writer_capacity() {
    let (conn, mut requests, _peer_in, mut peer_out) = in_memory_connection(1);
    let mut responder = Responder::new(&conn);
    saturate_writer(&conn).await;
    feed_requests(
        &conn,
        &mut peer_out,
        &[
            permission_request(1),
            json!({ "jsonrpc": "2.0", "id": 2, "method": "x/unknown", "params": {} }),
        ],
    )
    .await;

    let budget = Duration::from_millis(300);
    let margin = Duration::from_secs(2);
    let started = std::time::Instant::now();
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        serve_requests_while(
            &mut responder,
            &mut requests,
            tokio::time::timeout(budget, std::future::pending::<()>()),
        ),
    )
    .await
    .expect("a response send pending on writer capacity must not stall the phase");
    let elapsed = started.elapsed();
    assert!(
        outcome.is_err(),
        "the phase clock, not the send, ends the phase"
    );
    assert!(elapsed >= budget, "finished before the budget: {elapsed:?}");
    assert!(
        elapsed < budget + margin,
        "phase resolved {elapsed:?} after start; budget {budget:?} + margin {margin:?}"
    );

    // The first request was dequeued and its send left in flight (the loop
    // pulls no further request while one is pending), so the second is still
    // queued: the send really was blocked on capacity, not merely slow.
    assert!(responder.in_flight(), "the blocked send is still in flight");
    let still_queued = requests.try_recv().expect("second request still queued");
    assert_eq!(still_queued.method, "x/unknown");
    assert!(requests.try_recv().is_err());
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
    let text = run_one_shot_acp(cmd, "touch a file", None, None, Duration::from_secs(30))
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
    let text = run_one_shot_acp(cmd, "hello", None, None, Duration::from_secs(30))
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
    let text = run_one_shot_acp(cmd, "hello", Some("opus-x"), None, Duration::from_secs(30))
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
    let text = run_one_shot_acp(
        cmd,
        "hello",
        Some("bogus-model"),
        None,
        Duration::from_secs(30),
    )
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
    let err = run_one_shot_acp(cmd, "anything", None, None, Duration::from_secs(30))
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
    let err = run_one_shot_acp(cmd, "anything", None, None, Duration::from_secs(30))
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
    let err = run_one_shot_acp(cmd, "anything", None, None, Duration::from_secs(5))
        .await
        .unwrap_err();
    assert!(
        matches!(err, OneShotError::Spawn(_)),
        "expected Spawn, got {err}"
    );
}

/// Count the lines the mock adapters have appended to `log` so far (each line
/// is one adapter that actually started).
fn started_count(log: &std::path::Path) -> usize {
    std::fs::read_to_string(log).map_or(0, |s| s.lines().filter(|l| !l.trim().is_empty()).count())
}

/// Poll `cond` until it holds, failing with `what` if it never does.
async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while std::time::Instant::now() < deadline {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {what}");
}

/// The daemon-wide adapter bound end to end (monorepo#2062): a burst of
/// concurrent one-shots spawns at most `limit` adapter chains, the rest wait
/// their turn rather than piling ~610 MB of provider CLI on top of each other,
/// a caller whose own timeout expires while queued gets the distinguishable
/// [`OneShotError::QueueTimeout`] (never a hang, never something a client
/// could read as a slow model), and every queued caller that does get a slot
/// still completes normally.
///
/// The mock adapters park in `session/prompt` until the test creates a release
/// file, so "how many started" is read at a moment the test controls rather
/// than raced against.
///
/// Runner-agnostic: the bound is a process-global `OnceLock`, so under a
/// single-process runner an earlier test may already have installed one and
/// the `init_adapter_slots` call below is a no-op. The test therefore asks for
/// a small cap but asserts against the limit it reads back, and sizes the
/// burst from that — so it exercises a real over-subscription either way and
/// never depends on unspecified test ordering.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn burst_is_bounded_queued_callers_complete_and_late_ones_report_queue_timeout() {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = dir.path().join("starts.log");
    let release = dir.path().join("release");
    let script = dir.path().join("parked-adapter.mjs");
    std::fs::write(
        &script,
        format!(
            "import fs from 'node:fs';
fs.appendFileSync({log:?}, process.pid + '\\n');
{ADAPTER_PRELUDE}
const onPrompt = (id) => {{
  const tick = setInterval(() => {{
    if (!fs.existsSync({release:?})) return;
    clearInterval(tick);
    chunk('ok');
    result(id, {{ stopReason: 'end_turn' }});
  }}, 25);
}};
",
            log = log.to_string_lossy(),
            release = release.to_string_lossy(),
        ),
    )
    .expect("write mock adapter");
    let launch = || {
        OneShotCommand::binary(
            PathBuf::from("node"),
            vec![script.to_string_lossy().into_owned()],
        )
    };

    // Ask for a small cap; use whatever is actually in force.
    crate::acp_adapter::init_adapter_slots(2);
    let limit = crate::acp_adapter::adapter_slot_limit() as usize;
    let burst = limit + 3;

    let runs: Vec<_> = (0..burst)
        .map(|_| {
            let cmd = launch();
            tokio::spawn(async move {
                run_one_shot_acp(cmd, "go", None, None, Duration::from_secs(30)).await
            })
        })
        .collect();

    // Everything that can start, has: the bound is saturated and the rest are
    // queued behind it.
    wait_until("the bound to fill", || started_count(&log) >= limit).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        started_count(&log),
        limit,
        "over-limit one-shots must queue, not spawn ({burst} concurrent calls, limit {limit})"
    );

    // A caller arriving into that full queue with a short timeout fails as a
    // queue timeout — and still spawns nothing.
    let queued_out = run_one_shot_acp(launch(), "go", None, None, Duration::from_millis(300))
        .await
        .unwrap_err();
    let OneShotError::QueueTimeout {
        waited_ms,
        limit: reported,
    } = queued_out
    else {
        panic!("expected QueueTimeout, got {queued_out}");
    };
    assert_eq!(
        reported as usize, limit,
        "the error names the configured cap"
    );
    assert!(
        waited_ms >= 250,
        "reported wait {waited_ms}ms is implausibly short"
    );
    assert_eq!(
        started_count(&log),
        limit,
        "a queue-timed-out call must not have spawned an adapter"
    );

    // Let the parked adapters finish: every queued caller gets its turn.
    std::fs::write(&release, "go").expect("write release");
    for (i, run) in runs.into_iter().enumerate() {
        let text = run
            .await
            .expect("task joins")
            .unwrap_or_else(|e| panic!("queued one-shot #{i} failed: {e}"));
        assert_eq!(text, "ok", "one-shot #{i}");
    }
    assert_eq!(
        started_count(&log),
        burst,
        "every queued caller must eventually run"
    );
}
