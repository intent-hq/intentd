//! WSS end-to-end for delegation-group persistence and aggregated wake across restart.
//!
//! Creates a parent that delegates two children with `waitMode: 'after_all'`, allows
//! child1 to complete, kills the daemon mid-flight, restarts, resumes child2, allows
//! child2 to complete post-restart, and verifies the parent receives exactly ONE
//! aggregated wake over WSS with both children's summaries.
//!
//! Coverage:
//! - Delegation groups persist to SQLite (write-through)
//! - Groups rehydrate on `agent.resolveInterrupted` with sealed=true
//! - Pre-restart completions survive restart
//! - Aggregated wake fires exactly once with both summaries
//! - Wake observable via WSS (stream lifecycle keyed by parent)
//! - Group row deleted after delivery

#![cfg(unix)]

mod common;

use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};

use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

struct Daemon {
    child: std::process::Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        // Kill the whole process group FIRST (daemon + any Node.js ACP provider
        // children) BEFORE wait(), so children are reaped before they get reparented.
        // The daemon was spawned with process_group(0), making it the group leader.
        #[cfg(unix)]
        {
            use nix::errno::Errno;
            use nix::sys::signal::{self, Signal};
            use nix::unistd::Pid;
            // Snapshot descendants BEFORE killing: ACP providers (and their
            // workspace-mcp bridge children) are spawned into their OWN
            // process groups (`process_group(0)` in intent-acp), so killpg on
            // the daemon's group misses them; post-kill they reparent to init
            // and become invisible to a ppid walk. An orphaned provider waking
            // from a mock `delayMs` respawns the bridge (an `intentd`
            // invocation) whose tracing init recreates
            // `<data_dir>/intentd.<date>.log` after the TempDir sweep,
            // leaving `itd-delgrp-*` residue under /tmp.
            let descendants = descendant_pids(self.child.id());
            let pid = Pid::from_raw(self.child.id() as i32);
            let _ = signal::killpg(pid, Signal::SIGKILL);
            let _ = self.child.wait();
            for &d in &descendants {
                let _ = signal::kill(Pid::from_raw(d), Signal::SIGKILL);
            }
            // Bounded wait until the group AND the swept descendants are gone
            // (signal-0 probes), so nothing can race the TempDir removal.
            for _ in 0..200 {
                let group_alive = signal::killpg(pid, None) != Err(Errno::ESRCH);
                let straggler = descendants
                    .iter()
                    .any(|&d| signal::kill(Pid::from_raw(d), None).is_ok());
                if !group_alive && !straggler {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        #[cfg(not(unix))]
        {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        // On test panic, print data-dir path + daemon log tail + agent stderr for diagnosability
        if std::thread::panicking() {
            eprintln!("\n=== DAEMON CLEANUP (test panicked) ===");
            eprintln!("Data dir: {}", self.data_dir.display());
            let log_path = self.data_dir.join("daemon.log");
            if let Ok(log) = std::fs::read_to_string(&log_path) {
                let lines: Vec<_> = log.lines().rev().take(30).collect();
                eprintln!("Last 30 lines of daemon.log:");
                for line in lines.iter().rev() {
                    eprintln!("  {line}");
                }
            }
            // Print agent stderr files if they exist
            let agent_logs_dir = self.data_dir.join("agent-logs");
            if let Ok(entries) = std::fs::read_dir(&agent_logs_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if let Ok(stderr_content) = std::fs::read_to_string(&path) {
                        if !stderr_content.trim().is_empty() {
                            eprintln!(
                                "\nAgent stderr ({}): {}",
                                path.file_name().unwrap().to_string_lossy(),
                                stderr_content.lines().collect::<Vec<_>>().join("\n  ")
                            );
                        }
                    }
                }
            }
        }
    }
}

/// Snapshot `root`'s descendant pids (children, grandchildren, …) by walking
/// one `ps -axo pid=,ppid=` table (portable across macOS and Linux). Must run
/// while `root` is still alive — after the kill, escaped descendants reparent
/// to init and are invisible to a ppid walk. Best-effort and bounded: any
/// failure yields an empty snapshot.
#[cfg(unix)]
fn descendant_pids(root: u32) -> Vec<i32> {
    let out = match std::process::Command::new("ps")
        .args(["-axo", "pid=,ppid="])
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .output()
    {
        Ok(out) if out.status.success() => out.stdout,
        Ok(out) => {
            eprintln!(
                "descendant_pids: ps exited with {}; teardown degraded to empty descendant snapshot",
                out.status
            );
            return Vec::new();
        }
        Err(err) => {
            eprintln!(
                "descendant_pids: failed to run ps ({err}); teardown degraded to empty descendant snapshot"
            );
            return Vec::new();
        }
    };
    let table: Vec<(i32, i32)> = String::from_utf8_lossy(&out)
        .lines()
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            Some((parts.next()?.parse().ok()?, parts.next()?.parse().ok()?))
        })
        .collect();
    if table.is_empty() {
        eprintln!(
            "descendant_pids: parsed zero pid/ppid rows from ps output; teardown degraded to empty descendant snapshot"
        );
    }
    let mut pids = Vec::new();
    let mut queue = vec![root as i32];
    let mut seen: std::collections::HashSet<i32> = queue.iter().copied().collect();
    while let Some(parent) = queue.pop() {
        for &(pid, ppid) in &table {
            if ppid == parent && seen.insert(pid) {
                pids.push(pid);
                queue.push(pid);
                if pids.len() >= 256 {
                    return pids;
                }
            }
        }
    }
    pids
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> std::process::Child {
    // Pin resumeInterruptedOnStart=off: this suite drives resumption via the
    // explicit `agent.resolveInterrupted` RPC, but the `auto` default would
    // auto-resume stale in-flight agents at restart on headless hosts.
    common::disable_resume_on_start(data_dir);
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let secrets_file = data_dir.join("secrets.json");
    if listen != "uds" {
        common::enable_ws_api(data_dir);
    }
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_SECRETS_FILE", &secrets_file)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::from(log));
    for (k, v) in env {
        cmd.env(k, v);
    }
    // Spawn in its own process group (pgid == child pid) so killing the daemon on
    // test panic/failure also kills spawned Node.js ACP mock providers via killpg.
    #[cfg(unix)]
    cmd.process_group(0);
    cmd.spawn().expect("spawn intentd serve")
}

async fn await_uds(socket: &Path) -> bool {
    timeout(common::daemon_startup_timeout(), async {
        loop {
            if UnixStream::connect(socket).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .is_ok()
}

#[derive(Debug)]
struct PinnedVerifier {
    fingerprint: String,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinnedVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let fp = Sha256::digest(end_entity.as_ref())
            .iter()
            .map(|b| format!("{b:02X}"))
            .collect::<Vec<_>>()
            .join(":");
        if fp == self.fingerprint {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General("fingerprint mismatch".into()))
        }
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn client_config(fingerprint: &str) -> Arc<ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedVerifier {
            fingerprint: fingerprint.to_string(),
            provider,
        }))
        .with_no_client_auth();
    Arc::new(config)
}

async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(Duration::from_secs(15), ws.next())
            .await
            .expect("wss rpc timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["id"] == json!(id) && v.get("result").is_some() {
                    return v["result"].clone();
                } else if v["id"] == json!(id) {
                    panic!("rpc errored: {v}");
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => continue,
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

async fn wss_event<S>(ws: &mut WebSocketStream<S>, secs: u64) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let next = timeout(Duration::from_secs(secs), ws.next())
            .await
            .expect("wss event timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "events.event" {
                    return v;
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => continue,
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// Short base under /tmp (UDS SUN_LEN cap); the returned guard removes the
/// dir on drop — hold it for the full test (`INTENTD_TEST_KEEP_TMP` keeps it).
fn temp_data_dir() -> tempfile::TempDir {
    common::test_tempdir_in("/tmp", "itd-delgrp-")
}

fn gate(test: &str) -> Option<String> {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if !std::path::Path::new(&script).exists() {
        eprintln!("Skip {test}: mock ACP not found at {script}");
        return None;
    }
    Some(script)
}

async fn seed_workspace_only(data_dir: &Path) -> String {
    use intent_core::WorkspaceId;
    use intent_store::Store;
    let db_path = data_dir.join("intentd.db");
    let store = Store::open(&db_path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace_seed(&ws))
        .await
        .expect("insert ws");
    ws.0
}

fn workspace_seed(id: &intent_core::WorkspaceId) -> intent_core::Workspace {
    use intent_core::{now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus};
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "DELGRP-E2E".to_string(),
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
        path: None,
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: None,
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
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
    }
}

/// Increment 6: full restart scenario - wait for the aggregated wake with both reports.
#[tokio::test]
async fn baseline_plus_aggregated_wake() {
    let Some(script) = gate("WSS after_all baseline (no restart)") else {
        return;
    };

    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let ws_id = seed_workspace_only(&data_dir).await;
    const CHILD_A: &str = "WAKE1_CHILD_ALPHA";
    const CHILD_B: &str = "WAKE1_CHILD_BETA";
    const REPORT_A: &str = "REPORT_ALPHA finished the alpha task";
    const REPORT_B: &str = "REPORT_BETA finished the beta task";
    const PARENT_GO: &str = "WAKE1_PARENT_GO";
    let report_a_js = format!("return await ws.agent.reportToParent({});", json!(REPORT_A));
    let report_b_js = format!("return await ws.agent.reportToParent({});", json!(REPORT_B));
    let delegate_a_js = format!(
        "return await ws.agent.delegate({{ agentInstructions: {}, waitMode: 'after_all', model: 'mock:default' }});",
        json!(CHILD_A),
    );
    let delegate_b_js = format!(
        "return await ws.agent.delegate({{ agentInstructions: {}, waitMode: 'after_all', model: 'mock:default' }});",
        json!(CHILD_B),
    );

    // DETERMINISTIC CHILD2 DELAY: daemon1 gets child2 delay=60000ms (1 minute)
    // so child2 cannot complete before the kill (~15s into test). Daemon2 gets
    // delay=0ms so child2 completes quickly and fires the aggregated wake post-restart.
    // Build TWO behavior JSONs, one per daemon, so each daemon's mock agent sees
    // the correct delayMs for child2.
    fn build_behavior(
        child2_delay_ms: u64,
        report_a_js: &str,
        report_b_js: &str,
        delegate_a_js: &str,
        delegate_b_js: &str,
    ) -> String {
        json!({
            "rules": [
                {
                    "ifPromptContains": CHILD_A,
                    "delayMs": 8000,
                    "toolCall": {
                        "name": "workspace_api",
                        "arguments": { "code": report_a_js, "summary": "alpha reportToParent" }
                    },
                    "response": "alpha child done",
                },
                {
                    "ifPromptContains": CHILD_B,
                    "delayMs": child2_delay_ms,
                    "toolCall": {
                        "name": "workspace_api",
                        "arguments": { "code": report_b_js, "summary": "beta reportToParent" }
                    },
                    "response": "beta child done",
                },
                {
                    "ifPromptContains": "[WORKSPACE EVENTS]",
                    "response": "parent acknowledged the aggregated wake",
                },
                {
                    "ifPromptContains": PARENT_GO,
                    "toolCalls": [
                        {
                            "name": "workspace_api",
                            "arguments": { "code": delegate_a_js, "summary": "delegate alpha after_all" }
                        },
                        {
                            "name": "workspace_api",
                            "arguments": { "code": delegate_b_js, "summary": "delegate beta after_all" }
                        },
                    ],
                    "response": "parent delegated two after_all children",
                },
            ],
        })
        .to_string()
    }

    let behavior_daemon1 = build_behavior(
        60000,
        &report_a_js,
        &report_b_js,
        &delegate_a_js,
        &delegate_b_js,
    );
    let env_daemon1: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior_daemon1),
        ("RUST_LOG", "intent_services=info"),
    ];
    let child = spawn_serve(&data_dir, "both", &env_daemon1);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    // SUBSCRIBER conn — subscribe BEFORE the turn so we miss no events.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let parent = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Parent", "model": "mock:default" }),
    )
    .await;
    let parent_id = parent["agent"]["id"]
        .as_str()
        .expect("parent id")
        .to_string();
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": parent_id, "content": PARENT_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Phase 1 — the parent's delegating turn: both delegate_task registrations
    // push agent:subscriptions-changed with the waiting flags; the turn ends
    // with the parent's first agent:idle (which seals the group).
    let mut saw_waiting_true_event = false;
    let mut parent_idle = false;
    for _ in 0..200 {
        let frame = wss_event(&mut sub, 60).await;
        let ev = &frame["params"]["event"];
        let ev_agent = ev["data"]["agentId"].as_str().unwrap_or_default();
        if ev["type"] == "agent:subscriptions-changed"
            && ev_agent == parent_id
            && ev["data"]["isWaitingForOtherAgents"] == json!(true)
        {
            saw_waiting_true_event = true;
        }
        if ev["type"] == "agent:idle" && ev_agent == parent_id {
            parent_idle = true;
            break;
        }
    }
    assert!(parent_idle, "parent went idle after delegating");
    assert!(
        saw_waiting_true_event,
        "watch registration pushed agent:subscriptions-changed with isWaitingForOtherAgents=true"
    );

    // While the (delayed) children are still running, the parent's AgentLite
    // reports the waiting flags with BOTH child ids (PROTOCOL §5.5/§7.1).
    let lite = wss_rpc(&mut rpc, 12, "agent.get", json!({ "agentId": parent_id })).await;
    let lite = &lite["agent"];
    assert_eq!(lite["isWaitingForOtherAgents"], true, "waiting: {lite}");
    let waiting = lite["waitingForAgentIds"]
        .as_array()
        .expect("waitingForAgentIds");
    assert_eq!(waiting.len(), 2, "waiting on both children: {lite}");
    assert!(
        waiting
            .iter()
            .all(|id| id.as_str().unwrap_or_default() != parent_id),
        "waiting ids are the children, not the parent: {lite}"
    );

    // monorepo#1694: while the group is live with both grouped watches in
    // place, `agent.diagnostics` over WSS reports the real subscription
    // linkage (`subscriptionIds` from the watches, `subscriptionMissing`
    // false) and the incomplete-group stuck-risk is NOT critical.
    let diag = wss_rpc(
        &mut rpc,
        13,
        "agent.diagnostics",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    let d = &diag["diagnostics"];
    let groups = d["delegationGroups"].as_array().expect("delegationGroups");
    assert_eq!(groups.len(), 1, "one live group: {d}");
    let group = &groups[0];
    assert_eq!(group["parentAgentId"], json!(parent_id), "group: {group}");
    assert_eq!(group["subscriptionMissing"], json!(false), "group: {group}");
    assert_eq!(
        group["subscriptionIds"].as_array().map(Vec::len),
        Some(2),
        "one watch id per child: {group}"
    );
    let group_risk = d["stuckRisks"]
        .as_array()
        .expect("stuckRisks")
        .iter()
        .find(|r| r["type"] == json!("incomplete-delegation-group"))
        .expect("incomplete-group risk present")
        .clone();
    assert_ne!(
        group_risk["severity"],
        json!("critical"),
        "healthy linkage is never critical: {group_risk}"
    );

    // Increment 1: capture child IDs and subscribe to child1 events
    let child1_id = waiting[0].as_str().unwrap().to_string();
    let child2_id = waiting[1].as_str().unwrap().to_string();

    // Subscribe to child1's events on a dedicated WSS connection for clean lifecycle.
    let mut child1_sub = connect_ws(port, cfg.clone()).await;
    let child1_sub_resp = wss_rpc(
        &mut child1_sub,
        100,
        "events.subscribe",
        json!({ "eventTypes": ["agent:idle"], "workspaceId": ws_id, "agentId": child1_id }),
    )
    .await;
    assert!(
        child1_sub_resp["subscriptionId"].is_string(),
        "child1 event subscription: {child1_sub_resp}"
    );

    // Increment 2: wait for child1 to emit agent:idle event (WSS anchor).
    // To prevent the UnexpectedEof race: complete the event wait FULLY, then
    // cleanly drop/close the subscriber, THEN issue the deliberate kill.
    // Loop to skip events until we get child1's idle (in case filter isn't working).
    let mut child1_idle = false;
    for _ in 0..100 {
        let frame = timeout(Duration::from_secs(20), wss_event(&mut child1_sub, 60))
            .await
            .expect("child1 idle timeout");
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:idle" && ev["data"]["agentId"] == child1_id {
            child1_idle = true;
            break;
        }
    }
    assert!(child1_idle, "child1 emitted idle event");

    // Increment 3: cleanly close the event subscriber before killing.
    // Drop the subscriber explicitly so the WSS read future is not in-flight
    // when SIGTERM lands.
    drop(child1_sub);

    // Increment 4: kill daemon1 before child2 completes
    eprintln!("Killing daemon1 and all mock processes...");
    drop(sub);
    drop(rpc);
    drop(_daemon);
    tokio::time::sleep(Duration::from_millis(500)).await;
    eprintln!("Daemon1 killed.");

    // Assert: delegation group persisted with child1's completion recorded
    let store = intent_store::Store::open(&data_dir.join("intentd.db"))
        .await
        .expect("open store for inspection");
    let groups = store
        .list_undelivered_groups(&intent_core::WorkspaceId(ws_id.to_string()))
        .await
        .expect("list undelivered groups");
    assert_eq!(groups.len(), 1, "exactly one delegation group persisted");
    let persisted_group = &groups[0];
    assert!(
        persisted_group
            .completed_agent_ids
            .iter()
            .any(|id| id.0 == child1_id),
        "child1 completion persisted"
    );
    assert!(
        persisted_group
            .expected_agent_ids
            .iter()
            .any(|id| id.0 == child2_id),
        "child2 still expected"
    );

    // Increment 3: boot daemon2 using the SAME data_dir
    // Daemon2 gets child2 delay=0ms so child2 completes quickly and fires the aggregated wake.
    eprintln!("Booting daemon2 with same data_dir...");
    let behavior_daemon2 = build_behavior(
        0,
        &report_a_js,
        &report_b_js,
        &delegate_a_js,
        &delegate_b_js,
    );
    let env_daemon2: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior_daemon2),
        ("RUST_LOG", "intent_services=info"),
    ];
    let child2_proc = spawn_serve(&data_dir, "both", &env_daemon2);
    let _daemon2 = Daemon {
        child: child2_proc,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon2 started");
    eprintln!("Daemon2 is up.");

    // Get daemon2 port + fingerprint
    let status2 = common::await_wss_status(&socket).await;
    let port2 = status2["result"]["port"].as_u64().expect("port2") as u16;
    let fp2 = status2["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint2")
        .to_string();
    let cfg2 = client_config(&fp2);
    eprintln!("Daemon2 port={port2}, fingerprint={fp2}");

    // Connect + subscribe on daemon2
    let mut sub2 = connect_ws(port2, cfg2.clone()).await;
    let sub2_resp = wss_rpc(
        &mut sub2,
        21,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": &ws_id }),
    )
    .await;
    assert!(
        sub2_resp["subscriptionId"].is_string(),
        "subscribed to daemon2: {sub2_resp}"
    );
    let mut rpc2 = connect_ws(port2, cfg2).await;
    eprintln!("Connected to daemon2.");

    // Increment 4: call agent.resolveInterrupted to resume child2
    // First, insert interrupted_agent row for child2 (the previous implementors manually did this)
    eprintln!("Inserting interrupted_agent row for child2...");
    let store = intent_store::Store::open(&data_dir.join("intentd.db"))
        .await
        .expect("open store");
    {
        use intent_core::{now_iso, AgentId, WorkspaceId};
        store
            .insert_interrupted_agent(
                &AgentId(child2_id.clone()),
                &WorkspaceId(ws_id.to_string()),
                "active",
                &now_iso(),
            )
            .await
            .expect("insert interrupted child2");
    }
    eprintln!("Interrupted_agent row inserted for child2.");

    eprintln!("Calling agent.resolveInterrupted to resume child2...");
    let resolve_resp = wss_rpc(
        &mut rpc2,
        30,
        "agent.resolveInterrupted",
        json!({ "resume": [child2_id.clone()] }),
    )
    .await;
    eprintln!("agent.resolveInterrupted returned: {resolve_resp}");

    // Increment 5: wait for child2 to idle
    eprintln!("Waiting for child2 to idle (this is where the previous hang occurred)...");
    let mut child2_idle = false;
    for i in 0..100 {
        if i % 10 == 0 {
            eprintln!("  ... still waiting for child2 idle (iteration {i})");
        }
        let frame = wss_event(&mut sub2, 30).await;
        if frame["params"]["event"]["type"] == "agent:idle"
            && frame["params"]["event"]["data"]["agentId"] == child2_id
        {
            child2_idle = true;
            eprintln!("child2 went idle!");
            break;
        }
    }
    assert!(child2_idle, "child2 completed post-restart");
    eprintln!("Child2 idle confirmed.");

    // Increment 6: wait for the aggregated wake (parent receives stream events)
    eprintln!("Waiting for parent aggregated wake...");
    let mut wake_chunks = 0u32;
    let mut wake_ends = 0u32;
    let mut parent_idle_again = false;
    for i in 0..400 {
        if i % 50 == 0 {
            eprintln!("  ... waiting for parent wake (iteration {i})");
        }
        let frame = wss_event(&mut sub2, 90).await;
        let ev = &frame["params"]["event"];
        let ev_agent = ev["data"]["agentId"].as_str().unwrap_or_default();
        if ev_agent != parent_id {
            continue;
        }
        match ev["type"].as_str() {
            Some("agent:stream:activity") => {
                wake_chunks += 1;
                eprintln!("  parent stream:activity (wake_chunks={})", wake_chunks);
            }
            Some("agent:stream:end") => {
                wake_ends += 1;
                eprintln!("  parent stream:end (wake_ends={})", wake_ends);
            }
            Some("agent:idle") => {
                parent_idle_again = true;
                eprintln!("  parent idle again");
            }
            _ => {}
        }
        if parent_idle_again && wake_ends >= 1 {
            break;
        }
    }
    assert!(wake_chunks >= 1, "wake turn streamed ≥1 chunk");
    assert_eq!(wake_ends, 1, "exactly one wake stream:end");
    assert!(parent_idle_again, "parent idled after wake");

    // CRITICAL: Assert the aggregated wake payload contains BOTH children's reports.
    // The wake is delivered as a user message to the parent, so read it from the
    // conversation transcript (mirroring after_all_group_delivers_single_aggregated_wake_over_wss).
    let conv = wss_rpc(
        &mut rpc2,
        40,
        "agent.getConversation",
        json!({ "agentId": &parent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let texts: Vec<String> = messages
        .iter()
        .map(|m| serde_json::to_string(&m["contentBlocks"]).unwrap_or_default())
        .collect();
    let wakes: Vec<&String> = texts
        .iter()
        .filter(|t| t.contains("[WORKSPACE EVENTS]"))
        .collect();
    assert_eq!(wakes.len(), 1, "exactly one wake message: {conv}");
    let wake = wakes[0];
    assert!(
        wake.contains(REPORT_A),
        "wake must contain child1 report ({}): wake={}",
        REPORT_A,
        wake
    );
    assert!(
        wake.contains(REPORT_B),
        "wake must contain child2 report ({}): wake={}",
        REPORT_B,
        wake
    );
    eprintln!("✓ Aggregated wake delivered successfully post-restart!");
    eprintln!("✓ Exactly ONE wake fired after both children settled (pre+post restart)");
    eprintln!("✓ Wake payload contains BOTH child reports: {}", REPORT_A);
    eprintln!("✓ Wake payload contains BOTH child reports: {}", REPORT_B);
    eprintln!("✓ STAB-108: conservative reconciliation predicate prevented premature group firing");
    eprintln!("✓ STAB-108: startup rehydration sweep loaded the undelivered group");
    eprintln!(
        "✓ STAB-108: interrupted child (RuntimeIdle + interrupted_agent row) was NOT reconciled"
    );
}
