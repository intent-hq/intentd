//! Hermetic E2E regressions for MCP bridge resilience (monorepo#871).
//!
//! Scenario 1 — a slow `workspace_api` call must NOT head-of-line block the
//! same bridge connection: the mock agent fires a long `tools/call` (agent JS
//! that polls for a release file) and, while it is in flight, a `tools/list`
//! ping on the SAME connection. The release file is only written after the
//! ping answers, so the turn can only end cleanly if the daemon dispatched
//! both concurrently — the serialized pre-#871 bridge deadlocks (the provider
//! would time the ping out and evict the tool surface).
//!
//! Scenario 2 — a real `intentd mcp-bridge` subprocess survives a TCP blip:
//! an in-flight request already delivered to the listener gets the synthesized
//! non-retryable outcome-unknown `-32002` error (monorepo#1530), a gap request
//! gets the retryable `-32001` error (never silence), and once the listener is
//! back the bridge reconnects and serves requests again over the SAME stdio
//! session.
//!
//! Scenario 3 — startup race (monorepo#908): a bridge subprocess spawned
//! before its listener is reachable BUFFERS stdin (notably the MCP
//! `initialize` handshake) during the initial connect window instead of
//! answering with `-32001`; once the listener appears inside the window the
//! buffered request is forwarded and answered for real. On initial-window
//! exhaustion the bridge exits non-zero without ever writing a `-32001` for
//! the buffered requests.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use intent_acp::{EventSink, SpawnOptions};
use intent_core::{
    now_iso, AgentId, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention, WorkspaceId,
    WorkspaceStatus,
};
use intent_providers::ProviderConfig;
use intent_services::{AgentManager, BusEventSink, EventBus, Services};
use intent_store::Store;

fn workspace(id: &WorkspaceId, path: Option<std::path::PathBuf>) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "E2E Bridge Resilience".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.clone(),
        updated_at: ts,
        last_activity: None,
        tags: vec![],
        path: path.as_ref().map(|p| p.to_string_lossy().to_string()),
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: path.map(|p| p.to_string_lossy().to_string()),
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        pull_requests: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
        display_status: None,
        waiting: false,
    }
}

fn gate() -> Option<String> {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping bridge resilience e2e: node not on PATH");
        return None;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping bridge resilience e2e: script missing at {script}");
        return None;
    }
    Some(script)
}

/// Scenario 1: a long `workspace_api` call and a concurrent `tools/list` ping
/// over ONE bridge connection. The mock agent resolves `end_turn` only when
/// the ping was answered WHILE the long call was still in flight (release-file
/// gate) AND the long call then completed successfully; any deadlock or error
/// resolves `refusal` and fails the assertions below.
#[tokio::test]
async fn slow_tool_call_does_not_block_concurrent_tools_list() {
    let Some(script) = gate() else { return };

    let ws_root = std::env::temp_dir().join(format!("itd-e2e-bridge-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&ws_root).expect("mkdir ws_root");

    let db = std::env::temp_dir().join(format!("intentd-e2e-bridge-{}.db", uuid::Uuid::new_v4()));
    let store = Store::open(&db).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store.clone())
        .with_workspaces_root(ws_root.clone())
        .with_settings_registry(common::registry_with_default_provider(&ws_root))
        .with_event_bus(bus.clone());

    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws, Some(ws_root.clone())))
        .await
        .expect("insert ws");

    let agent_val = services
        .agent_create(
            ws.clone(),
            Some("E2E Bridge".into()),
            None,
            None,
            None,
            None,
            intent_core::AgentCreateExtra::default(),
        )
        .await
        .expect("create agent");
    let agent_id = AgentId::from(agent_val["agent"]["id"].as_str().unwrap());

    let script_static: &'static str = Box::leak(script.into_boxed_str());
    let base_args: &'static [&'static str] = Box::leak(vec![script_static].into_boxed_slice());
    let provider = ProviderConfig {
        command: "node",
        base_args,
        supports_authenticate: true,
        supports_mcp_config: true,
        mcp_config_flag: Some("--mcp-config"),
        ..*intent_providers::find_provider("mock").unwrap()
    };

    // The long call polls the workspace root via `ws.file.list` until the
    // release file appears — a real blocking tool call held open server-side.
    let release_file = ws_root.join("release.flag");
    let long_call_code = r"
        let found = false;
        while (!found) {
            const entries = await ws.file.list('.');
            found = entries.some((e) => e.name === 'release.flag');
        }
        return 'long-call-done';
    ";

    let behavior = serde_json::json!({
        "bridgeConcurrency": {
            "longCallCode": long_call_code,
            "releaseFile": release_file.to_string_lossy(),
        },
    })
    .to_string();

    let mut extra_env = BTreeMap::new();
    extra_env.insert("MOCK_AGENT_BEHAVIOR".to_string(), behavior);
    // Guarded agent cwd: context-engine children (auggie) write logs into
    // their cwd; a bare temp_dir() would leak them at the TMPDIR root.
    let cwd_dir = common::test_tempdir("itd-agent-cwd-");
    let cwd = cwd_dir.path().to_path_buf();
    let mut opts = SpawnOptions::new(&provider);
    opts.cwd = Some(&cwd);
    opts.extra_env = extra_env;

    let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus.clone()));
    let manager = AgentManager::new(services.clone(), sink, 8)
        .with_mcp_bridge_exe(env!("CARGO_BIN_EXE_intentd"));

    manager
        .create_agent(
            agent_id.clone(),
            ws.clone(),
            "E2E Bridge",
            "interactive",
            cwd.clone(),
            &opts,
        )
        .await
        .expect("create_agent");
    let acp_session = manager
        .start_session(&agent_id, cwd.clone(), &provider)
        .await
        .expect("start_session");
    let block: intent_acp::session::ContentBlock =
        serde_json::from_value(serde_json::json!({ "type": "text", "text": "probe bridge" }))
            .unwrap();
    let stop = manager
        .run_turn(&agent_id, &ws, &acp_session, vec![block], None)
        .await
        .expect("run_turn");
    // `end_turn` proves the whole gated sequence: the ping was answered while
    // the long call was in flight AND the long call then completed. A
    // serialized bridge deadlocks → probe timeout → `refusal`.
    assert_eq!(
        serde_json::to_value(stop).unwrap(),
        serde_json::json!("end_turn"),
        "bridge concurrency probe must resolve end_turn (refusal = deadlock or tool error)"
    );
    assert!(
        release_file.exists(),
        "release file written only after the concurrent tools/list answered"
    );

    manager.shutdown().await;
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
    }
    let _ = std::fs::remove_dir_all(&ws_root);
}

//
// Scenario 2 — real `intentd mcp-bridge` subprocess vs a TCP blip.
//

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::time::timeout;

/// JSON-RPC error code the bridge synthesizes while its TCP side is down
/// (see `intent_acp::mcp_bridge::BRIDGE_DISCONNECTED_CODE`).
const BRIDGE_DISCONNECTED_CODE: i64 = -32001;

/// JSON-RPC error code the bridge synthesizes for requests delivered to the
/// listener before a drop — outcome unknown, not retryable (monorepo#1530;
/// see `intent_acp::mcp_bridge::BRIDGE_OUTCOME_UNKNOWN_CODE`).
const BRIDGE_OUTCOME_UNKNOWN_CODE: i64 = -32002;

async fn write_json_line<W: tokio::io::AsyncWrite + Unpin>(w: &mut W, v: &serde_json::Value) {
    w.write_all(format!("{v}\n").as_bytes())
        .await
        .expect("write line");
    w.flush().await.expect("flush");
}

async fn read_json_line<R: tokio::io::AsyncBufRead + Unpin>(
    r: &mut R,
    what: &str,
) -> serde_json::Value {
    let mut line = String::new();
    let n = timeout(
        common::test_timeout(Duration::from_secs(30)),
        r.read_line(&mut line),
    )
    .await
    .unwrap_or_else(|_| panic!("timed out reading {what}"))
    .expect("read line");
    assert!(n > 0, "unexpected EOF reading {what}");
    serde_json::from_str(line.trim()).unwrap_or_else(|e| panic!("{what} not JSON ({e}): {line}"))
}

/// A real `intentd mcp-bridge --connect <addr>` subprocess survives a TCP
/// blip: the request in flight when the connection drops was delivered to the
/// listener, so it gets the non-retryable outcome-unknown `-32002` error
/// (monorepo#1530); a request sent during the gap gets the retryable `-32001`
/// error (never silence); and once the listener is back the bridge reconnects
/// on its own and serves requests again over the SAME stdio session.
#[tokio::test]
async fn bridge_subprocess_survives_tcp_blip_with_retryable_errors() {
    // Fake daemon listener the test controls end-to-end.
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let addr = listener.local_addr().expect("local addr");

    // Hermetic log dir so the subprocess's tracing appender never touches the
    // real data dir.
    let data_dir =
        std::env::temp_dir().join(format!("itd-e2e-bridge-log-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");

    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_intentd"))
        .args(["mcp-bridge", "--connect", &addr.to_string()])
        .env("INTENTD_DATA_DIR", &data_dir)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn mcp-bridge");
    let mut stdin = child.stdin.take().expect("child stdin");
    let mut stdout = BufReader::new(child.stdout.take().expect("child stdout"));

    // Connection 1: one request round-trips through the bridge.
    let (conn1, _) = timeout(
        common::test_timeout(Duration::from_secs(30)),
        listener.accept(),
    )
    .await
    .expect("bridge never connected")
    .expect("accept");
    let (read1, mut write1) = conn1.into_split();
    let mut lines1 = BufReader::new(read1).lines();

    write_json_line(
        &mut stdin,
        &serde_json::json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}),
    )
    .await;
    let fwd = timeout(
        common::test_timeout(Duration::from_secs(30)),
        lines1.next_line(),
    )
    .await
    .expect("forwarded request timed out")
    .expect("read forwarded")
    .expect("conn1 open");
    let fwd: serde_json::Value = serde_json::from_str(&fwd).expect("forwarded is JSON");
    assert_eq!(fwd["id"], serde_json::json!(1));
    write_json_line(
        &mut write1,
        &serde_json::json!({"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"workspace_api"}]}}),
    )
    .await;
    let resp = read_json_line(&mut stdout, "response 1").await;
    assert_eq!(resp["id"], serde_json::json!(1));
    assert_eq!(
        resp["result"]["tools"][0]["name"],
        serde_json::json!("workspace_api")
    );

    // Request 2 is forwarded but never answered: the daemon side drops the
    // connection AND the listener (a real blip — nothing to reconnect to yet).
    write_json_line(
        &mut stdin,
        &serde_json::json!({"jsonrpc":"2.0","id":2,"method":"tools/call"}),
    )
    .await;
    timeout(
        common::test_timeout(Duration::from_secs(30)),
        lines1.next_line(),
    )
    .await
    .expect("forwarded request 2 timed out")
    .expect("read forwarded 2")
    .expect("conn1 open");
    drop(write1);
    drop(lines1);
    drop(listener);

    // The in-flight request was delivered to the listener before the drop, so
    // it gets the synthesized non-retryable outcome-unknown error.
    let resp = read_json_line(&mut stdout, "in-flight error for id 2").await;
    assert_eq!(resp["id"], serde_json::json!(2));
    assert_eq!(
        resp["error"]["code"],
        serde_json::json!(BRIDGE_OUTCOME_UNKNOWN_CODE)
    );
    assert_eq!(resp["error"]["data"]["retryable"], serde_json::json!(false));

    // A request sent during the gap also errors retryably instead of hanging.
    write_json_line(
        &mut stdin,
        &serde_json::json!({"jsonrpc":"2.0","id":3,"method":"tools/list"}),
    )
    .await;
    let resp = read_json_line(&mut stdout, "gap error for id 3").await;
    assert_eq!(resp["id"], serde_json::json!(3));
    assert_eq!(
        resp["error"]["code"],
        serde_json::json!(BRIDGE_DISCONNECTED_CODE)
    );
    assert_eq!(resp["error"]["data"]["retryable"], serde_json::json!(true));

    // The daemon comes back on the SAME address; the bridge reconnects on its
    // own (within the 30s reconnect window, backoff-capped at 1s).
    let listener = TcpListener::bind(addr).await.expect("rebind");
    let (conn2, _) = timeout(
        common::test_timeout(Duration::from_secs(30)),
        listener.accept(),
    )
    .await
    .expect("bridge never reconnected")
    .expect("accept 2");
    let (read2, mut write2) = conn2.into_split();
    let mut lines2 = BufReader::new(read2).lines();

    write_json_line(
        &mut stdin,
        &serde_json::json!({"jsonrpc":"2.0","id":4,"method":"tools/list"}),
    )
    .await;
    let fwd = timeout(
        common::test_timeout(Duration::from_secs(30)),
        lines2.next_line(),
    )
    .await
    .expect("forwarded request 4 timed out")
    .expect("read forwarded 4")
    .expect("conn2 open");
    let fwd: serde_json::Value = serde_json::from_str(&fwd).expect("forwarded 4 is JSON");
    assert_eq!(fwd["id"], serde_json::json!(4));
    write_json_line(
        &mut write2,
        &serde_json::json!({"jsonrpc":"2.0","id":4,"result":{"tools":[{"name":"workspace_api"}]}}),
    )
    .await;
    let resp = read_json_line(&mut stdout, "response 4 after reconnect").await;
    assert_eq!(resp["id"], serde_json::json!(4));
    assert_eq!(
        resp["result"]["tools"][0]["name"],
        serde_json::json!("workspace_api"),
        "tool surface is served again after the blip"
    );

    // stdin EOF ends the bridge cleanly.
    drop(stdin);
    let status = timeout(common::test_timeout(Duration::from_secs(30)), child.wait())
        .await
        .expect("bridge did not exit on stdin EOF")
        .expect("wait");
    assert!(status.success(), "bridge must exit cleanly: {status:?}");

    let _ = std::fs::remove_dir_all(&data_dir);
}

//
// Scenario 3 — startup race: buffer stdin during the initial connect window
// (monorepo#908).
//

/// Spawn a real `intentd mcp-bridge --connect <addr>` subprocess with a
/// hermetic data dir, returning the child plus its piped stdin/stdout.
fn spawn_bridge_subprocess(
    addr: &std::net::SocketAddr,
    data_dir: &std::path::Path,
) -> (
    tokio::process::Child,
    tokio::process::ChildStdin,
    BufReader<tokio::process::ChildStdout>,
) {
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_intentd"))
        .args(["mcp-bridge", "--connect", &addr.to_string()])
        .env("INTENTD_DATA_DIR", data_dir)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn mcp-bridge");
    let stdin = child.stdin.take().expect("child stdin");
    let stdout = BufReader::new(child.stdout.take().expect("child stdout"));
    (child, stdin, stdout)
}

/// Scenario 3 (monorepo#908): `initialize` written while nothing is listening
/// yet is buffered through the initial connect window — never answered with
/// `-32001` — and gets the real server response once the listener is rebound
/// inside the window.
#[tokio::test]
async fn bridge_subprocess_buffers_initialize_during_startup_race() {
    // Reserve an address, then DROP the listener so nothing is accepting.
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);

    let data_dir =
        std::env::temp_dir().join(format!("itd-e2e-bridge-race-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");
    let (mut child, mut stdin, mut stdout) = spawn_bridge_subprocess(&addr, &data_dir);

    // Immediately write the MCP handshake — the bridge is now inside its
    // initial connect window with nothing listening.
    write_json_line(
        &mut stdin,
        &serde_json::json!({
            "jsonrpc":"2.0","id":1,"method":"initialize",
            "params":{"protocolVersion":"2024-11-05"}
        }),
    )
    .await;

    // Rebind the SAME address ~1.5s in — well inside the ~5.5s default
    // initial window — and accept the bridge's connection.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let listener = TcpListener::bind(addr).await.expect("rebind");
    let (conn, _) = timeout(
        common::test_timeout(Duration::from_secs(30)),
        listener.accept(),
    )
    .await
    .expect("bridge never connected after rebind")
    .expect("accept");
    let (read, mut write) = conn.into_split();
    let mut lines = BufReader::new(read).lines();

    // The buffered initialize is flushed to the fresh connection.
    let fwd = timeout(
        common::test_timeout(Duration::from_secs(30)),
        lines.next_line(),
    )
    .await
    .expect("buffered initialize never forwarded")
    .expect("read forwarded")
    .expect("conn open");
    let fwd: serde_json::Value = serde_json::from_str(&fwd).expect("forwarded is JSON");
    assert_eq!(fwd["id"], serde_json::json!(1));
    assert_eq!(fwd["method"], serde_json::json!("initialize"));
    write_json_line(
        &mut write,
        &serde_json::json!({
            "jsonrpc":"2.0","id":1,
            "result":{
                "protocolVersion":"2024-11-05",
                "capabilities":{"tools":{}},
                "serverInfo":{"name":"workspace-mcp","version":"0.0.0"}
            }
        }),
    )
    .await;

    // The FIRST id-1 message on stdout is the real response — a pre-fix
    // bridge writes the -32001 reject here instead.
    let resp = read_json_line(&mut stdout, "buffered initialize response").await;
    assert_eq!(resp["id"], serde_json::json!(1));
    assert!(
        resp.get("error").is_none(),
        "initialize must never be answered with an error: {resp}"
    );
    assert_eq!(
        resp["result"]["protocolVersion"],
        serde_json::json!("2024-11-05")
    );

    // stdin EOF ends the bridge cleanly.
    drop(stdin);
    let status = timeout(common::test_timeout(Duration::from_secs(30)), child.wait())
        .await
        .expect("bridge did not exit on stdin EOF")
        .expect("wait");
    assert!(status.success(), "bridge must exit cleanly: {status:?}");

    let _ = std::fs::remove_dir_all(&data_dir);
}

/// Scenario 3 exhaustion (monorepo#908): against a never-rebound address the
/// bridge exits NON-ZERO once the initial window is exhausted (~5.5s default)
/// and writes no `-32001` response for the buffered `initialize`.
#[tokio::test]
async fn bridge_subprocess_initial_window_exhaustion_exits_nonzero_without_errors() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);

    let data_dir =
        std::env::temp_dir().join(format!("itd-e2e-bridge-exhaust-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&data_dir).expect("mkdir data dir");
    let (mut child, mut stdin, mut stdout) = spawn_bridge_subprocess(&addr, &data_dir);

    write_json_line(
        &mut stdin,
        &serde_json::json!({
            "jsonrpc":"2.0","id":1,"method":"initialize",
            "params":{"protocolVersion":"2024-11-05"}
        }),
    )
    .await;

    // The default initial window is ~5.5s of backoff; the process must exit
    // non-zero within ~10s.
    let status = timeout(common::test_timeout(Duration::from_secs(10)), child.wait())
        .await
        .expect("bridge did not exit after initial-window exhaustion")
        .expect("wait");
    assert!(
        !status.success(),
        "initial-window exhaustion must exit non-zero: {status:?}"
    );

    // Nothing — in particular no -32001 — was written for the buffered
    // request.
    let mut out = String::new();
    tokio::io::AsyncReadExt::read_to_string(&mut stdout, &mut out)
        .await
        .expect("drain stdout");
    assert!(
        !out.contains("-32001"),
        "no -32001 may be written for buffered requests on exhaustion: {out}"
    );

    let _ = std::fs::remove_dir_all(&data_dir);
}
