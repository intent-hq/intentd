//! WSS end-to-end conversation rehydration (P3-1.2): a multi-message
//! conversation (user / assistant-with-tool-blocks / tool) persisted in the
//! daemon's SQLite store BEFORE the daemon boots must round-trip through
//! `agent.get` / `agent.getConversation` over a real pinned-TLS WebSocket with
//! exact ordering, roles, ids, content blocks and timestamps — the FE
//! rehydration path once `UnifiedPersistence` reads move to the daemon
//! (PROTOCOL §5.5, TA-2 pagination).
//!
//! Seeds the store directly (the daemon opens the same data dir on launch),
//! mirroring the seed pattern of `e2e_wss_agent_lifecycle.rs`. Needs neither
//! the mock ACP provider nor node, so it always runs.

#![cfg(unix)]

mod common;
use common::test_timeout;

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
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

/// Fixed 64-hex token, adopted by the daemon via the `INTENTD_AUTH_TOKEN` seam.
const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

/// Live `intentd serve` process; killed and its data dir removed on drop.
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

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-rehyd-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    if listen != "uds" {
        common::enable_ws_api(data_dir);
    }
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
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
async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
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
        let next = timeout(test_timeout(Duration::from_secs(15)), ws.next())
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

fn workspace_seed(id: &intent_core::WorkspaceId) -> intent_core::Workspace {
    use intent_core::{now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus};
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "Rehydration-E2E".to_string(),
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
        execution_environment: None,
        disk_usage: None,
        pending_delete_at: None,
    }
}

/// Pre-seed the daemon's SQLite store with a workspace, a settled agent
/// session, and a 5-message conversation exercising every persisted role
/// (`user`, `assistant`, `tool`) and block type (`text`, `tool_use`,
/// `tool_result`). Returns `(ws_id, agent_id, expected_wire_messages)` where
/// the expected messages are the exact `agent.getConversation` wire objects
/// (`id` / `agentId` / `seq` / `role` / `contentBlocks` / `timestamp`).
/// The store handle is dropped before the daemon process starts.
async fn seed_conversation(data_dir: &Path) -> (String, String, Vec<Value>) {
    use intent_core::{now_iso, AgentId, AgentSession, AgentStatus, WorkspaceId};
    use intent_store::Store;
    let db_path = data_dir.join("intentd.db");
    let store = Store::open(&db_path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace_seed(&ws))
        .await
        .expect("insert ws");

    let agent_id = AgentId::from(format!("agent-{}", Uuid::new_v4()).as_str());
    let ts = now_iso();
    // `RuntimeIdle` (wire `"idle"`) is a settled status: the daemon-startup
    // stale-session heal only rewrites Active/Processing/Waiting, so the
    // seeded session must come back byte-identical. Built via serde (the wire
    // shape) rather than a struct literal so additive `AgentSession` fields
    // never break this seed.
    let session: AgentSession = serde_json::from_value(json!({
        "id": agent_id.0,
        "workspaceId": ws.0,
        "name": "Rehydrated",
        "nameExplicitlySet": true,
        "model": "mock:default",
        "systemPrompt": "be helpful",
        "status": "idle",
        "isActive": false,
        "createdAt": ts,
        "updatedAt": ts,
    }))
    .expect("seed session from wire shape");
    assert_eq!(session.status, AgentStatus::RuntimeIdle);
    store
        .insert_agent_session(&session)
        .await
        .expect("insert session");

    let m: Vec<String> = (0..5).map(|_| Uuid::now_v7().to_string()).collect();
    let turns: Vec<(&str, Value)> = vec![
        (
            "user",
            json!([{ "type": "text", "id": format!("{}:0", m[0]), "text": "Run the tests" }]),
        ),
        (
            "assistant",
            json!([
                { "type": "text", "id": format!("{}:0", m[1]), "text": "I'll run the tests." },
                { "type": "tool_use", "id": format!("{}:1", m[1]), "name": "run_tests",
                  "input": { "path": "." }, "toolCallId": "call_rehydrate",
                  "metadata": { "toolKind": "terminal", "status": "completed" } },
                { "type": "tool_result", "id": format!("{}:2", m[1]),
                  "tool_use_id": "call_rehydrate", "output": "12 passed", "is_error": false },
                { "type": "text", "id": format!("{}:3", m[1]), "text": "Done." },
            ]),
        ),
        (
            "tool",
            json!([
                { "type": "tool_result", "id": format!("{}:0", m[2]),
                  "tool_use_id": "call_rehydrate", "output": "12 passed", "is_error": false },
            ]),
        ),
        (
            "user",
            json!([{ "type": "text", "id": format!("{}:0", m[3]), "text": "Ship it" }]),
        ),
        (
            "assistant",
            json!([{ "type": "text", "id": format!("{}:0", m[4]), "text": "All done." }]),
        ),
    ];
    let mut expected = Vec::with_capacity(turns.len());
    for (seq, (role, blocks)) in turns.iter().enumerate() {
        let ts = now_iso();
        store
            .append_agent_message_with_id(&agent_id, &m[seq], role, blocks, None, &ts)
            .await
            .expect("append message");
        expected.push(json!({
            "id": m[seq],
            "agentId": agent_id.0,
            "seq": seq as i64,
            "role": role,
            "contentBlocks": blocks,
            "timestamp": ts,
        }));
    }
    (ws.0, agent_id.0, expected)
}

/// Boot a fresh daemon over a pre-seeded store and assert the conversation
/// rehydrates over WSS: `agent.get` reflects the seeded transcript in its
/// `AgentLite` projection (derived fields, no `messages`/`systemPrompt`), the
/// full `agent.getConversation` snapshot equals the seeded wire objects
/// byte-for-byte (ordering, roles, ids, content blocks, timestamps), and the
/// TA-2 backward pagination walks newest→oldest via opaque `nextToken`s.
#[tokio::test]
async fn seeded_conversation_rehydrates_over_wss() {
    let data_dir = temp_data_dir();
    let (ws_id, agent_id, expected) = seed_conversation(&data_dir).await;

    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
    let child = spawn_serve(&data_dir, "both", &env);
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
    let mut rpc = connect_ws(port, cfg).await;

    // agent.get — the AgentLite projection is derived from the seeded
    // transcript; the transcript itself is NOT inlined (PROTOCOL §5.5).
    let got = wss_rpc(&mut rpc, 10, "agent.get", json!({ "agentId": agent_id })).await;
    let lite = &got["agent"];
    assert_eq!(lite["id"], json!(agent_id));
    assert_eq!(lite["workspaceId"], json!(ws_id));
    assert_eq!(lite["name"], "Rehydrated");
    assert_eq!(lite["nameExplicitlySet"], true);
    assert_eq!(lite["status"], "idle", "settled status survives the heal");
    assert_eq!(lite["isActive"], false);
    assert_eq!(lite["messageCount"], 5);
    assert!(lite.get("messages").is_none(), "AgentLite strips messages");
    assert!(
        lite.get("systemPrompt").is_none(),
        "AgentLite strips systemPrompt"
    );
    assert_eq!(lite["lastAgentResponse"], "All done.");
    assert_eq!(lite["lastUserMessage"], "Ship it");
    assert_eq!(lite["metadata"]["isBackground"], false);

    // Full snapshot — byte-for-byte: ordering (seq 0..4 oldest→newest), roles
    // (user / assistant / tool), ids, content blocks, timestamps.
    let conv = wss_rpc(
        &mut rpc,
        11,
        "agent.getConversation",
        json!({ "agentId": agent_id }),
    )
    .await;
    assert_eq!(conv["agentId"], json!(agent_id));
    assert_eq!(conv["totalMessages"], 5);
    assert_eq!(conv["truncated"], false);
    assert!(conv["nextToken"].is_null(), "single page ⇒ no token");
    assert_eq!(
        conv["messages"],
        Value::Array(expected.clone()),
        "rehydrated conversation equals the seeded wire shape"
    );

    // TA-2 pagination — newest page first, opaque token walks older pages,
    // each page stays oldest→newest internally.
    let p1 = wss_rpc(
        &mut rpc,
        12,
        "agent.getConversation",
        json!({ "agentId": agent_id, "limit": 2 }),
    )
    .await;
    assert_eq!(p1["messages"], Value::Array(expected[3..5].to_vec()));
    assert_eq!(p1["truncated"], true);
    assert_eq!(p1["totalMessages"], 5);
    let t1 = p1["nextToken"].as_str().expect("page-1 token").to_string();

    let p2 = wss_rpc(
        &mut rpc,
        13,
        "agent.getConversation",
        json!({ "agentId": agent_id, "limit": 2, "nextToken": t1 }),
    )
    .await;
    assert_eq!(p2["messages"], Value::Array(expected[1..3].to_vec()));
    assert_eq!(p2["truncated"], true);
    let t2 = p2["nextToken"].as_str().expect("page-2 token").to_string();

    let p3 = wss_rpc(
        &mut rpc,
        14,
        "agent.getConversation",
        json!({ "agentId": agent_id, "limit": 2, "nextToken": t2 }),
    )
    .await;
    assert_eq!(p3["messages"], Value::Array(expected[0..1].to_vec()));
    assert_eq!(p3["truncated"], false);
    assert!(p3["nextToken"].is_null(), "oldest page ⇒ no token");
}
