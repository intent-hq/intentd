//! WSS end-to-end (intent-hq/monorepo#2732): an AUTOMATIC parent wake into an
//! ARCHIVED workspace parks in the parent's queue instead of starting a turn
//! that auto-unarchives the workspace (the archive/auto-unarchive loop).
//!
//! Drives: create workspace → Watcher registers `ws.agent.watch` on
//! `WatchTarget` through the MCP bridge → target parks mid-turn (mock ACP
//! provider, `parkIfPromptContains`) → `workspace.archive` (the sweep
//! interrupts the target keep-alive, whose `agent:idle` fires the completion
//! watch) → asserts:
//! - the completion wake PARKS in the Watcher's queue (`agent.getQueue`),
//! - `workspace.get` still shows `archived: true` (the wake never
//!   auto-unarchived the workspace),
//! - after `workspace.unarchive`, the parked wake is delivered (the
//!   Watcher's conversation carries the wake turn's response).
//!
//! Gated on `node` + the mock script; skips cleanly otherwise.

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::net::UnixStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

type TlsWs = WebSocketStream<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>;

/// Live `intentd serve` process; killed (whole process group) and its data
/// dir removed on drop, with the daemon log echoed on panic.
struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        {
            use nix::sys::signal::{self, Signal};
            use nix::unistd::Pid;
            let pid = Pid::from_raw(self.child.id().cast_signed());
            let _ = signal::killpg(pid, Signal::SIGKILL);
        }
        let _ = self.child.wait();
        if std::thread::panicking() {
            if let Ok(log) = std::fs::read_to_string(self.data_dir.join("daemon.log")) {
                eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
            }
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-parkwake-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(data_dir);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    // Group leader so Daemon::drop can killpg the daemon + ACP children.
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.spawn().expect("spawn intentd serve")
}

async fn await_uds(socket: &Path, deadline: tokio::time::Instant) -> bool {
    tokio::time::timeout_at(deadline, async {
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

/// Pin the server's SHA-256 fingerprint (colon-UPPER hex over the DER cert).
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
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
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

async fn connect_ws(port: u16, cfg: Arc<ClientConfig>) -> TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Whole-test wall-clock budget (monorepo#1562): every wait below is clamped
/// so a stall panics naming its own step before nextest's 180s kill.
const TEST_BUDGET: Duration = Duration::from_secs(150);

#[derive(Clone, Copy)]
struct Budget {
    end: tokio::time::Instant,
}

impl Budget {
    fn start() -> Self {
        Self {
            end: tokio::time::Instant::now() + TEST_BUDGET,
        }
    }

    /// Deadline for one step: `secs` scaled by the multiplier, clamped to the
    /// whole-test budget.
    fn step(&self, secs: u64) -> tokio::time::Instant {
        let scaled = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(secs));
        scaled.min(self.end)
    }

    /// Deadline for one RPC round-trip (a live daemon answers in ms), clamped
    /// to the whole-test budget so a stall near the budget's end still panics
    /// before nextest's 180s kill.
    fn rpc_deadline(&self) -> tokio::time::Instant {
        let per_rpc =
            tokio::time::Instant::now() + common::rpc_read_timeout().min(Duration::from_secs(45));
        per_rpc.min(self.end)
    }
}

async fn wss_rpc(ws: &mut TlsWs, budget: Budget, id: i64, method: &str, params: Value) -> Value {
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    let deadline = budget.rpc_deadline();
    loop {
        let next = tokio::time::timeout_at(deadline, ws.next())
            .await
            .unwrap_or_else(|_| panic!("wss rpc {method} timed out"));
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["id"] == json!(id) {
                    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
                    return v["result"].clone();
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// Mock-agent gate (parity with the other WSS E2E suites).
fn gate(test: &str) -> Option<String> {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping {test}: node not on PATH");
        return None;
    }
    if !Path::new(&script).exists() {
        eprintln!("skipping {test}: mock script missing at {script}");
        return None;
    }
    Some(script)
}

async fn seed_workspace_only(data_dir: &Path) -> String {
    use intent_core::{
        now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
    };
    use intent_store::Store;
    let store = Store::open(&data_dir.join("intentd.db"))
        .await
        .expect("open store");
    let ts = now_iso();
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&Workspace {
            id: ws.clone(),
            title: "WSS-PARK-WAKE-E2E".to_string(),
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
            context_links: None,
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
            execution_environment: None,
        })
        .await
        .expect("insert ws");
    ws.0
}

async fn create_agent(rpc: &mut TlsWs, budget: Budget, id: i64, ws_id: &str, name: &str) -> String {
    let created = wss_rpc(
        rpc,
        budget,
        id,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": name, "model": "default", "provider": "mock" }),
    )
    .await;
    created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string()
}

/// Serialize the agent's conversation for substring assertions.
async fn conversation_text(
    rpc: &mut TlsWs,
    budget: Budget,
    id: i64,
    ws_id: &str,
    agent_id: &str,
) -> String {
    let convo = wss_rpc(
        rpc,
        budget,
        id,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    convo.to_string()
}

/// Poll the agent's conversation until `needle` appears (or panic at the
/// deadline). Returns the conversation text containing the needle.
async fn await_conversation_contains(
    rpc: &mut TlsWs,
    budget: Budget,
    req_id: &mut i64,
    ws_id: &str,
    agent_id: &str,
    needle: &str,
    deadline: tokio::time::Instant,
) -> String {
    loop {
        let text = conversation_text(rpc, budget, *req_id, ws_id, agent_id).await;
        *req_id += 1;
        if text.contains(needle) {
            return text;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "conversation for {agent_id} never contained {needle:?}: {text}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll `agent.getQueue` until the predicate holds (or panic at the deadline).
#[allow(clippy::too_many_arguments)]
async fn await_queue<F>(
    rpc: &mut TlsWs,
    budget: Budget,
    req_id: &mut i64,
    ws_id: &str,
    agent_id: &str,
    what: &str,
    deadline: tokio::time::Instant,
    predicate: F,
) -> Value
where
    F: Fn(&[Value]) -> bool,
{
    loop {
        let result = wss_rpc(
            rpc,
            budget,
            *req_id,
            "agent.getQueue",
            json!({ "workspaceId": ws_id, "agentId": agent_id }),
        )
        .await;
        *req_id += 1;
        let queue = result["queue"].as_array().cloned().unwrap_or_default();
        if predicate(&queue) {
            return result;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "queue for {agent_id} never satisfied: {what} (last: {result})"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

const DO_WATCH: &str = "PARKWAKE_DO_WATCH";
const TARGET_PARK: &str = "PARKWAKE_TARGET_PARK";

/// An automatic completion wake into an archived workspace parks in the
/// watcher's queue (no auto-unarchive); `workspace.unarchive` delivers it.
#[tokio::test]
async fn archived_workspace_parks_completion_wake_until_unarchive_over_wss() {
    let Some(script) = gate("WSS archive-parks-wakes E2E") else {
        return;
    };
    let budget = Budget::start();

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    // Watcher: registers the watch through the MCP bridge, and acknowledges
    // wake turns. Target: parks mid-turn on its marker so the archive sweep
    // interrupts it (whose idle fires the completion watch while archived).
    let watch_js = r"
        const agents = await ws.agent.list();
        const t = agents.find(a => a.name === 'WatchTarget');
        const r = await ws.agent.watch(t.id);
        return 'watched=' + r.ok + ' watchTarget=' + r.agentId;
    ";
    let behavior = json!({
        "parkIfPromptContains": TARGET_PARK,
        "rules": [
            { "ifPromptContains": "[WORKSPACE EVENTS]", "response": "watcher acknowledged wake" },
            {
                "ifPromptContains": DO_WATCH,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": watch_js, "summary": "register agent watch" }
                },
                "emitToolBlocks": true,
                "response": "watch registered",
            },
        ],
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
    ];
    let child = spawn_serve(&data_dir, &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(
        await_uds(&socket, budget.step(60)).await,
        "daemon did not start"
    );
    let status = tokio::time::timeout_at(budget.step(60), common::await_wss_status(&socket))
        .await
        .expect("daemon wss status not ready within budget");
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);
    let mut rpc = connect_ws(port, cfg.clone()).await;

    // Target FIRST (so the watcher's ws.agent.list lookup finds it), then the
    // watcher registers the watch through the bridge and settles idle.
    let target = create_agent(&mut rpc, budget, 10, &ws_id, "WatchTarget").await;
    let watcher = create_agent(&mut rpc, budget, 11, &ws_id, "Watcher").await;
    let sent = wss_rpc(
        &mut rpc,
        budget,
        12,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": watcher, "content": DO_WATCH }),
    )
    .await;
    assert_eq!(sent["success"], true, "watch send ok: {sent}");
    let mut req_id = 20i64;
    let text = await_conversation_contains(
        &mut rpc,
        budget,
        &mut req_id,
        &ws_id,
        &watcher,
        "watched=true",
        budget.step(60),
    )
    .await;
    assert!(
        text.contains(&format!("watchTarget={target}")),
        "watch names the target: {text}"
    );

    // Park the target mid-turn, then archive: the sweep interrupts the
    // target keep-alive, its idle fires the completion watch, and the wake
    // must PARK (the workspace is archived by the time it is delivered).
    let sent = wss_rpc(
        &mut rpc,
        budget,
        13,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": target, "content": TARGET_PARK }),
    )
    .await;
    assert_eq!(sent["success"], true, "target park send ok: {sent}");
    // Wait until the target's turn is live (the archive sweep only
    // interrupts an in-flight turn with a cancellable session).
    let deadline = budget.step(60);
    loop {
        let got = wss_rpc(
            &mut rpc,
            budget,
            req_id,
            "agent.get",
            json!({ "workspaceId": ws_id, "agentId": target }),
        )
        .await;
        req_id += 1;
        if got["agent"]["acpSessionId"].is_string() && got["agent"]["status"] == json!("active") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "target turn never went live: {got}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let archived = wss_rpc(
        &mut rpc,
        budget,
        100,
        "workspace.archive",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(archived["workspace"]["archived"], json!(true));

    // The completion wake parks in the watcher's queue...
    let mut q_id = 200i64;
    await_queue(
        &mut rpc,
        budget,
        &mut q_id,
        &ws_id,
        &watcher,
        "one parked completion wake",
        budget.step(60),
        |queue| !queue.is_empty(),
    )
    .await;

    // ...and the workspace STAYS archived: the wake never started a turn
    // that auto-unarchives it (the pre-fix loop).
    let fetched = wss_rpc(
        &mut rpc,
        budget,
        300,
        "workspace.get",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(
        fetched["workspace"]["archived"],
        json!(true),
        "workspace stays archived while the wake is parked: {fetched}"
    );

    // Unarchive delivers the parked wake: the watcher runs the wake turn.
    let unarchived = wss_rpc(
        &mut rpc,
        budget,
        301,
        "workspace.unarchive",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(unarchived["workspace"]["archived"], json!(false));
    let mut c_id = 400i64;
    await_conversation_contains(
        &mut rpc,
        budget,
        &mut c_id,
        &ws_id,
        &watcher,
        "watcher acknowledged wake",
        budget.step(60),
    )
    .await;
    // The parked entry left the queue.
    let mut fq_id = 500i64;
    await_queue(
        &mut rpc,
        budget,
        &mut fq_id,
        &ws_id,
        &watcher,
        "empty queue after unarchive",
        budget.step(30),
        <[serde_json::Value]>::is_empty,
    )
    .await;
}
