//! WSS end-to-end for the widened `agent.wakeOrCreate` (C1d-10a).
//!
//! Boots a real `intentd serve --listen both`, seeds a workspace + task note,
//! and drives the widened composite over a pinned TLS WebSocket. The mock
//! agent is NOT needed — `agent.wakeOrCreate` persists the delivered context
//! message directly (matching the pre-widening path), so these tests are
//! hermetic and skip cleanly when `node` is missing (they don't gate on it).
//!
//! Coverage:
//! - Backward compat: pre-widening 3-required-params call still succeeds.
//! - Create branch: rich `create.*` payload lands on the persisted session +
//!   metadata blob; response carries widened `action`/`agentName`/`taskTitle`.
//! - Wake branch after previous create: most-recent-first pick and
//!   `cleanedUpAgentIds` byte-for-byte when an assignment goes stale.
//! - Depth-guard rejection: `delegationDepth == MAX_DELEGATION_DEPTH` yields a
//!   JSON-RPC `-32602` envelope (PROTOCOL §9).

#![cfg(unix)]

use std::net::{Ipv4Addr, TcpListener as StdTcpListener};
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
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn free_port() -> u16 {
    StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-woc-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .arg("--listen")
        .arg(listen)
        .env("INTENTD_DATA_DIR", data_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.spawn().expect("spawn intentd serve")
}

async fn await_uds(socket: &Path) -> bool {
    timeout(Duration::from_secs(10), async {
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

async fn uds_rpc(socket: &Path, id: i64, method: &str, params: Value) -> Value {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let stream = UnixStream::connect(socket).await.expect("connect uds");
    let (read_half, mut write_half) = stream.into_split();
    let mut line = serde_json::to_string(
        &json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }),
    )
    .unwrap();
    line.push('\n');
    write_half.write_all(line.as_bytes()).await.unwrap();
    write_half.flush().await.unwrap();
    let mut reader = BufReader::new(read_half);
    let mut buf = String::new();
    timeout(Duration::from_secs(5), reader.read_line(&mut buf))
        .await
        .expect("uds rpc timed out")
        .expect("read uds response");
    serde_json::from_str(buf.trim_end()).expect("invalid JSON frame")
}

/// TLS certificate verifier that pins the daemon's self-signed cert by
/// SHA-256 fingerprint (matches the WSS-1 harness).
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
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        use sha2::{Digest, Sha256};
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

async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let tcp = TcpStream::connect((Ipv4Addr::LOCALHOST, port))
        .await
        .expect("tcp connect");
    let name = ServerName::try_from("localhost").unwrap();
    let tls = TlsConnector::from(cfg)
        .connect(name, tcp)
        .await
        .expect("tls connect");
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    let (ws, _resp) = tokio_tungstenite::client_async(url, tls)
        .await
        .expect("ws handshake");
    ws
}

async fn wss_rpc_envelope<S>(
    ws: &mut WebSocketStream<S>,
    id: i64,
    method: &str,
    params: Value,
) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(Duration::from_secs(15), ws.next())
            .await
            .expect("wss rpc envelope timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["id"] == json!(id) {
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

async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let env = wss_rpc_envelope(ws, id, method, params.clone()).await;
    assert!(env.get("error").is_none(), "rpc errored: {env}");
    env["result"].clone()
}

fn workspace_seed(id: &intent_core::WorkspaceId) -> intent_core::Workspace {
    use intent_core::{now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus};
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "WSS-WOC".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
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
    }
}

/// Seed one workspace + one task note (via `mark_as_task`), returning
/// `(workspace_id, task_note_id)`. Runs BEFORE the daemon boots so the daemon
/// picks up the seeded rows on start.
async fn seed_workspace_and_task(data_dir: &Path, title: &str) -> (String, String) {
    use intent_core::{NoteCreate, WorkspaceApi, WorkspaceId};
    use intent_services::Services;
    use intent_store::Store;
    let db_path = data_dir.join("intentd.db");
    let store = Store::open(&db_path).await.expect("open store");
    let services = Services::new(store.clone());
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace_seed(&ws))
        .await
        .expect("insert ws");
    let note = services
        .create_note(
            ws.clone(),
            NoteCreate {
                title: title.into(),
                content: Some(format!("# {title}\n")),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create note");
    services
        .mark_as_task(
            ws.clone(),
            note.id.clone(),
            "not_started".into(),
            vec![],
            None,
        )
        .await
        .expect("markAsTask");
    (ws.0, note.id.0)
}

async fn boot_daemon_with_task(title: &str) -> (Daemon, String, String, u16, String) {
    let data_dir = temp_data_dir();
    let (ws_id, note_id) = seed_workspace_and_task(&data_dir, title).await;
    let port_s = free_port().to_string();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", &port_s)];
    let child = spawn_serve(&data_dir, "both", &env);
    let daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    (daemon, ws_id, note_id, port, fingerprint)
}

/// C1d-10a: exercise the four wire-contract slices of the widened
/// `agent.wakeOrCreate` over one pinned WSS connection, all on a single
/// daemon boot. Bundled to keep the (expensive) daemon spawn under one test.
#[tokio::test]
async fn wake_or_create_widened_wire_contract_over_wss() {
    let (_daemon, ws_id, task_note_id, port, fp) = boot_daemon_with_task("WOC Task").await;
    let cfg = client_config(&fp);
    let mut rpc = connect_ws(port, cfg.clone()).await;

    // (1) Backward-compat 3-required-params call: `taskNoteId` + `contextMessage`
    //     (+ optional `model`) is still accepted; the widened response carries
    //     `action: created_new`, `agentName`, and `taskTitle` for zero
    //     round-trip clients.
    let create_res = wss_rpc(
        &mut rpc,
        1,
        "agent.wakeOrCreate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": task_note_id,
            "contextMessage": "kickoff",
        }),
    )
    .await;
    assert_eq!(create_res["ok"], true);
    assert_eq!(create_res["created"], true);
    assert_eq!(create_res["action"], "created_new");
    assert_eq!(create_res["taskTitle"], "WOC Task");
    assert_eq!(create_res["agentName"], "Task: WOC Task");
    let agent_id = create_res["agentId"].as_str().expect("agentId").to_string();
    assert!(agent_id.starts_with("agent-"));
    assert!(create_res.get("cleanedUpAgentIds").is_none());

    // (2) Wake branch: a subsequent call finds the just-created agent as the
    //     newest resumable assignment. `created: false` and the response
    //     echoes the same agent id / task title so clients don't need a
    //     follow-up `agent.get`.
    //
    //     `action` is `woke_existing` when the assignee's in-flight slot has
    //     been released, or `message_queued_to_active_agent` when the earlier
    //     wake's runtime worker is still holding the slot (DELIV-1: the wake
    //     now routes through the `AgentManager` so a same-tick follow-up
    //     legitimately queues while the drain loop is running). Both action
    //     codes represent successful delivery.
    let wake_res = wss_rpc(
        &mut rpc,
        2,
        "agent.wakeOrCreate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": task_note_id,
            "contextMessage": "resume",
        }),
    )
    .await;
    assert_eq!(wake_res["ok"], true);
    assert_eq!(wake_res["created"], false);
    let wake_action = wake_res["action"].as_str().unwrap_or_default();
    assert!(
        wake_action == "woke_existing" || wake_action == "message_queued_to_active_agent",
        "wake action must be a live-assignee code (DELIV-1): {wake_res}"
    );
    assert_eq!(wake_res["agentId"], agent_id);
    assert_eq!(wake_res["taskTitle"], "WOC Task");

    // (3) Stale cleanup: delete the assigned agent so its assignment goes
    //     stale, then wake again. The response contains `cleanedUpAgentIds`
    //     with the stale id (byte-for-byte) and `action: created_new` for the
    //     replacement.
    wss_rpc(
        &mut rpc,
        3,
        "agent.delete",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let recreate_res = wss_rpc(
        &mut rpc,
        4,
        "agent.wakeOrCreate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": task_note_id,
            "contextMessage": "reboot",
            "create": {
                "metadata": { "custom": "field" },
                "skipAutoCommit": true,
            },
            "messageMetadata": { "type": "task_wake", "source": "wake" },
        }),
    )
    .await;
    assert_eq!(recreate_res["ok"], true);
    assert_eq!(recreate_res["created"], true);
    assert_eq!(recreate_res["action"], "created_new");
    assert_eq!(recreate_res["cleanedUpAgentIds"], json!([agent_id]));
    let new_agent_id = recreate_res["agentId"].as_str().unwrap().to_string();
    assert_ne!(new_agent_id, agent_id);

    // (4) Depth-guard rejection: `delegationDepth == MAX_DELEGATION_DEPTH`
    //     surfaces the JSON-RPC `-32602` error envelope (PROTOCOL §9).
    let err_env = wss_rpc_envelope(
        &mut rpc,
        5,
        "agent.wakeOrCreate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": task_note_id,
            "contextMessage": "too deep",
            "delegationDepth": 2,
        }),
    )
    .await;
    assert_eq!(err_env["jsonrpc"], "2.0");
    assert_eq!(err_env["id"], 5);
    assert!(err_env.get("result").is_none(), "must be error envelope");
    let err = &err_env["error"];
    assert_eq!(err["code"], -32602);
    let msg = err["message"].as_str().unwrap();
    assert!(
        msg.contains("MAX_DELEGATION_DEPTH"),
        "error message must reference the guard constant: {msg}"
    );

    // (5) The metadata payload the (3) create step supplied is persisted on
    //     the new session — read it back via `agent.get` and assert the
    //     folded provenance keys (source / isBackground / skipAutoCommit).
    let got = wss_rpc(
        &mut rpc,
        6,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": new_agent_id }),
    )
    .await;
    assert_eq!(got["agent"]["id"], new_agent_id);
}

/// TASK-C2 (follow-up to #104): `agent.delegate` with `taskNoteId` APPENDS
/// the reference `DelegateTaskTool` "Your Task Note" preamble after the
/// delegated child's FIRST message with a `---` separator, so the child sees
/// the body first followed by the note ID/title and the single-task scope
/// contract byte-for-byte. Exercised over the real WSS wire (not just the
/// `intent-services` unit tests) per the repo's e2e requirement. Hermetic —
/// no ACP provider is spawned; the child's persisted `metadata.initialMessage`
/// carries the preamble bytes at delegate time.
#[tokio::test]
async fn delegate_with_task_note_id_appends_preamble_over_wss() {
    const TITLE: &str = "TASK-C preamble task";
    let (_daemon, ws_id, task_note_id, port, fp) = boot_daemon_with_task(TITLE).await;
    let cfg = client_config(&fp);
    let mut rpc = connect_ws(port, cfg.clone()).await;

    let delegated = wss_rpc(
        &mut rpc,
        1,
        "agent.delegate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": task_note_id,
            "agentInstructions": "do the delegated body",
            "model": "mock:default",
        }),
    )
    .await;
    assert_eq!(delegated["ok"], true, "delegate ok: {delegated}");
    let child_id = delegated["agentId"]
        .as_str()
        .expect("child agent id")
        .to_string();
    assert!(!child_id.is_empty(), "non-empty child agentId");

    // `agent.get` returns `metadata.initialMessage` — the resolved first
    // message the child sees. Byte-exact match against the reference
    // composition `${msg}\n\n---\n${preamble}${commitInstruction}` from
    // `agent-interaction-tools.ts`. `skipAutoCommit` unset => empty tail.
    let got = wss_rpc(
        &mut rpc,
        2,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": child_id }),
    )
    .await;
    let initial = got["agent"]["metadata"]["initialMessage"]
        .as_str()
        .expect("initialMessage string");
    let expected = format!(
        "do the delegated body\n\
\n\
---\n\
**Your Task Note:** \"{TITLE}\" (ID: {task_note_id})\n\
This note is your workspace for this task. Update it with your progress, findings, and deliverables.\n\
\n\
**SCOPE: Complete THIS task only.** When done, mark it complete and end your session. Do not pick up other tasks."
    );
    assert_eq!(
        initial, expected,
        "child first message must be byte-exact: {initial:?}"
    );
}

/// TASK-C2: `agent.delegate` with `taskNoteId` + `skipAutoCommit=true` appends
/// the reference `**Auto-commit is OFF.**` instruction after the scope
/// directive, byte-for-byte, over the real WSS wire.
#[tokio::test]
async fn delegate_with_skip_auto_commit_appends_commit_instruction_over_wss() {
    const TITLE: &str = "TASK-C skipAutoCommit task";
    let (_daemon, ws_id, task_note_id, port, fp) = boot_daemon_with_task(TITLE).await;
    let cfg = client_config(&fp);
    let mut rpc = connect_ws(port, cfg.clone()).await;

    let delegated = wss_rpc(
        &mut rpc,
        1,
        "agent.delegate",
        json!({
            "workspaceId": ws_id,
            "taskNoteId": task_note_id,
            "agentInstructions": "do the delegated body",
            "skipAutoCommit": true,
            "model": "mock:default",
        }),
    )
    .await;
    assert_eq!(delegated["ok"], true, "delegate ok: {delegated}");
    let child_id = delegated["agentId"]
        .as_str()
        .expect("child agent id")
        .to_string();

    let got = wss_rpc(
        &mut rpc,
        2,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": child_id }),
    )
    .await;
    let initial = got["agent"]["metadata"]["initialMessage"]
        .as_str()
        .expect("initialMessage string");
    let expected = format!(
        "do the delegated body\n\
\n\
---\n\
**Your Task Note:** \"{TITLE}\" (ID: {task_note_id})\n\
This note is your workspace for this task. Update it with your progress, findings, and deliverables.\n\
\n\
**SCOPE: Complete THIS task only.** When done, mark it complete and end your session. Do not pick up other tasks.\n\
\n\
**Auto-commit is OFF.** Do not commit unless the user explicitly asks. If asked, use `agent_commit_changes` with `userRequested: true`."
    );
    assert_eq!(
        initial, expected,
        "child first message must be byte-exact when skipAutoCommit=true: {initial:?}"
    );
}
