//! WSS end-to-end: `workspace.archive` stops an in-flight agent turn
//! (graceful interrupt — keep-alive `agent.stop` semantics) while preserving
//! the session for a later unarchive + resume.
//!
//! Drives: create workspace → start an agent turn that parks mid-flight (mock
//! ACP provider, `blockUntilCancel`) → `workspace.archive` → asserts:
//! - the §5.1 archive response (refreshed record: `archived: true`,
//!   `status: "Archived"`, `archivedAt` set),
//! - the `workspace:updated` archive delta shape per docs/protocol/06-events.md §6.5
//!   (`changes: { archived: true, status: "Archived", archivedAt: <ts> }`),
//! - the terminal `agent:stream:end` carrying `stopReason: "interrupted"`
//!   over `events.subscribe` (the turn was stopped, not completed),
//! - `agent.list` shows the session preserved (not deleted) and no longer
//!   responding,
//! - after `workspace.unarchive`, a follow-up message resumes the SAME
//!   provider child (the mock's per-process turn counter reports `turn=2`),
//!   proving interrupt-not-kill keep-alive semantics across the archive.
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
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

/// Live `intentd serve` process; killed (whole process group) and its data
/// dir removed on drop, with the daemon log echoed for post-mortems.
struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use nix::sys::signal::{self, Signal};
            use nix::unistd::Pid;
            let pid = Pid::from_raw(self.child.id() as i32);
            let _ = signal::killpg(pid, Signal::SIGKILL);
        }
        let _ = self.child.wait();
        let log_path = self.data_dir.join("daemon.log");
        if let Ok(log) = std::fs::read_to_string(&log_path) {
            eprintln!("=== DAEMON LOG ===\n{}\n=== END LOG ===", log);
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-archv-{}", &id[..8]));
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
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }
    for (k, v) in env {
        cmd.env(k, v);
    }
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

/// Open an authenticated WSS connection (token in the query string).
async fn connect_ws(port: u16, cfg: Arc<ClientConfig>) -> common::TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Send one JSON-RPC frame and return the result whose id matches; any
/// out-of-band notifications (`events.event`) are ignored.
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
                if v["id"] == json!(id) {
                    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
                    return v["result"].clone();
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

/// Read one `events.event` notification from a subscriber connection (bounded).
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
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping {test}: mock script missing at {script}");
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
        title: "WSS-ARCHIVE-E2E".to_string(),
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

/// `workspace.archive` stops an in-flight agent turn — graceful interrupt
/// (keep-alive `agent.stop` semantics), NOT a hard kill — and preserves the
/// session for a later unarchive + resume.
///
/// The mock's first turn streams "streaming-before-cancel" and parks at
/// `session/cancel` (`blockUntilCancel`); `workspace.archive` must resolve it
/// with a terminal `agent:stream:end` carrying `stopReason: "interrupted"`,
/// emit the §6.5 `workspace:updated` archive delta, and leave the session
/// listed (not deleted) with no turn in flight. After `workspace.unarchive`,
/// a follow-up message resumes the SAME provider child (the mock's
/// per-process counter reports `turn=2`).
#[tokio::test]
async fn archive_interrupts_in_flight_agent_and_preserves_session_over_wss() {
    let Some(script) = gate("WSS archive-interrupt E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    let behavior = json!({ "blockUntilCancel": true, "response": "resumed" }).to_string();
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
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    // SUBSCRIBER conn — subscribe BEFORE the turn so no event can be missed.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({
            "eventTypes": ["agent:*", "chat:stream:delta", "workspace:updated"],
            "workspaceId": ws_id,
        }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "WSS-ARCHIVE", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "first" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // First turn streams a chunk and parks at session/cancel — the archive
    // lands mid-turn by construction.
    let mut saw_block_chunk = false;
    for _ in 0..50 {
        let frame = wss_event(&mut sub, 30).await;
        if frame["params"]["event"]["type"] == "chat:stream:delta"
            && frame["params"]["event"]["data"]["content"]
                .as_str()
                .unwrap_or_default()
                .contains("streaming-before-cancel")
        {
            saw_block_chunk = true;
            break;
        }
    }
    assert!(saw_block_chunk, "first turn streamed a chunk and parked");

    // Archive mid-turn. §5.1 return shape: the refreshed workspace record.
    let archived = wss_rpc(
        &mut rpc,
        12,
        "workspace.archive",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(archived["workspace"]["id"], ws_id.as_str());
    assert_eq!(archived["workspace"]["archived"], json!(true));
    assert_eq!(archived["workspace"]["status"], json!("Archived"));
    assert!(
        archived["workspace"]["archivedAt"].is_string(),
        "archive response carries the persisted archivedAt: {archived}"
    );
    let archived_at = archived["workspace"]["archivedAt"].clone();

    // The archive emits the §6.5 workspace:updated delta, interrupts the
    // parked turn (terminal agent:stream:end with stopReason "interrupted"
    // — the keep-alive interrupt signature, distinguishable from a normal
    // turn end which carries no stopReason), AND fires the STAB-28
    // interrupt-path agent:idle. Relative order of the events is
    // unspecified; collect all three.
    let mut archive_delta = None;
    let mut interrupt_end = None;
    let mut interrupt_idle = None;
    for _ in 0..50 {
        if archive_delta.is_some() && interrupt_end.is_some() && interrupt_idle.is_some() {
            break;
        }
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        match event["type"].as_str() {
            Some("workspace:updated") => {
                archive_delta = Some(event["data"].clone());
            }
            Some("agent:stream:end") => {
                interrupt_end = Some(event["data"].clone());
            }
            Some("agent:idle") => {
                interrupt_idle = Some(event["data"].clone());
            }
            _ => {}
        }
    }

    // §6.5 archive delta: the full applied WorkspaceUpdate, `<ts>` equal to
    // the persisted archivedAt from the RPC response.
    let archive_delta = archive_delta.expect("workspace.archive published workspace:updated");
    assert_eq!(
        archive_delta,
        json!({
            "workspaceId": ws_id,
            "changes": {
                "archived": true,
                "status": "Archived",
                "archivedAt": archived_at,
            }
        }),
        "archive delta shape per docs/protocol/06-events.md §6.5"
    );

    let interrupt_end = interrupt_end.expect("archive interrupted the in-flight turn");
    assert_eq!(
        interrupt_end["agentId"].as_str().unwrap_or_default(),
        agent_id,
        "terminal stream:end names the interrupted agent: {interrupt_end}"
    );
    assert_eq!(
        interrupt_end["stopReason"], "interrupted",
        "archive interrupts (keep-alive), so stream:end carries stopReason: {interrupt_end}"
    );

    // The STAB-28 interrupt-path idle fires AFTER the archive persisted
    // `Archived`, so it carries the additive `workspaceArchived: true`
    // suppression hint per docs/protocol/06-events.md §6.5 (notification clients stay
    // quiet for parked workspaces without a follow-up workspace.get).
    let interrupt_idle = interrupt_idle.expect("archive interrupt emitted agent:idle");
    assert_eq!(
        interrupt_idle["agentId"].as_str().unwrap_or_default(),
        agent_id,
        "interrupt-path idle names the interrupted agent: {interrupt_idle}"
    );
    assert_eq!(
        interrupt_idle["reason"], "interrupted",
        "interrupt-path idle carries reason interrupted: {interrupt_idle}"
    );
    assert_eq!(
        interrupt_idle["workspaceArchived"],
        json!(true),
        "idle in an archived workspace carries workspaceArchived: true: {interrupt_idle}"
    );

    // Session preserved: agent.list still serves the session (not deleted)
    // and it is no longer responding. Poll briefly — the worker releases the
    // busy slot just after the terminal stream:end.
    let mut settled = false;
    for i in 0..40 {
        let listed = wss_rpc(
            &mut rpc,
            20 + i,
            "agent.list",
            json!({ "workspaceId": ws_id }),
        )
        .await;
        let agents = listed["agents"].as_array().expect("agents array");
        let row = agents
            .iter()
            .find(|a| a["id"] == agent_id.as_str())
            .unwrap_or_else(|| panic!("archived workspace keeps the session listed: {listed}"));
        if row["isResponding"] == false && row["turnInFlight"] == false {
            settled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        settled,
        "no responding agents after archive (session preserved, turn stopped)"
    );

    // Unarchive: §6.5 delta clears archivedAt with an explicit null.
    let unarchived = wss_rpc(
        &mut rpc,
        70,
        "workspace.unarchive",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(unarchived["workspace"]["archived"], json!(false));
    assert_eq!(unarchived["workspace"]["status"], json!("Active"));
    assert!(unarchived["workspace"].get("archivedAt").is_none());

    let mut unarchive_delta = None;
    for _ in 0..50 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["type"] == "workspace:updated" {
            unarchive_delta = Some(event["data"].clone());
            break;
        }
    }
    assert_eq!(
        unarchive_delta.expect("workspace.unarchive published workspace:updated"),
        json!({
            "workspaceId": ws_id,
            "changes": {
                "archived": false,
                "status": "Active",
                "archivedAt": null,
            }
        }),
        "unarchive delta shape per docs/protocol/06-events.md §6.5"
    );

    // Keep-alive across the archive: the follow-up resumes the SAME provider
    // child — the mock's per-process turn counter reports turn=2. A hard
    // kill would have respawned the child (fresh counter, turn=1).
    let resumed = wss_rpc(
        &mut rpc,
        71,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "second" }),
    )
    .await;
    assert_eq!(resumed["success"], true, "resume sendMessage ok: {resumed}");

    let mut saw_resume_chunk = false;
    let mut saw_resume_end = false;
    for _ in 0..50 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        match event["type"].as_str() {
            Some("chat:stream:delta") => {
                if event["data"]["content"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("turn=2")
                {
                    saw_resume_chunk = true;
                }
            }
            Some("agent:stream:end") => {
                assert!(
                    event["data"].get("stopReason").is_none(),
                    "normal stream:end carries no stopReason: {event}"
                );
                saw_resume_end = true;
                break;
            }
            _ => {}
        }
    }
    assert!(
        saw_resume_chunk,
        "post-unarchive turn resumed the SAME process (mock reported turn=2)"
    );
    assert!(
        saw_resume_end,
        "resumed turn emits its own terminal stream:end"
    );

    // The resumed turn's settlement idle fires in the now-Active workspace:
    // `workspaceArchived` is OMITTED (absent, never `false`) per the
    // additive-field convention in docs/protocol/06-events.md §6.5.
    let mut resume_idle = None;
    for _ in 0..50 {
        let frame = wss_event(&mut sub, 30).await;
        let event = &frame["params"]["event"];
        if event["type"] == "agent:idle" {
            resume_idle = Some(event["data"].clone());
            break;
        }
    }
    let resume_idle = resume_idle.expect("resumed turn emitted its settlement agent:idle");
    assert!(
        resume_idle.get("workspaceArchived").is_none(),
        "active workspace omits workspaceArchived (absent, never false): {resume_idle}"
    );
}
