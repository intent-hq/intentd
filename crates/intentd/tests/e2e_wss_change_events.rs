//! WSS end-to-end change-event emissions for the workspace-lifecycle
//! mutations (FIX 2 parity): drives a real pinned-TLS WebSocket against a
//! live `intentd serve --listen both` and asserts that `workspace.update`
//! and `workspace.delete` publish `workspace:updated` / `workspace:deleted`
//! (PROTOCOL.md §6.5) so a subscribed client sees the mutation without a
//! follow-up read. The `git:commit` emission is exercised over UDS in
//! `uds_events.rs` and via unit tests where a git worktree is cheaper to
//! materialise; this file focuses on the pure-daemon lifecycle where no
//! git binary is required.

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
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

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

fn scratch_dir(prefix: &str) -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-events-{prefix}-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir scratch dir");
    dir
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .arg("--listen")
        .arg("both")
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

async fn tls_connect(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    let tcp = TcpStream::connect((Ipv4Addr::LOCALHOST, port))
        .await
        .expect("tcp connect");
    let name = ServerName::try_from("localhost").unwrap();
    TlsConnector::from(cfg)
        .connect(name, tcp)
        .await
        .expect("tls connect")
}

async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let tls = tls_connect(port, cfg).await;
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    let (ws, _resp) = tokio_tungstenite::client_async(url, tls)
        .await
        .expect("ws handshake");
    ws
}

async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
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

/// Wait up to `secs` for the next `events.event` notification whose payload
/// `type` matches one of `types`; ignore other frames. Returns the event
/// object (the `params.event` sub-object).
async fn next_event<S>(ws: &mut WebSocketStream<S>, types: &[&str], secs: u64) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "timed out waiting for {types:?}");
        let next = timeout(remaining, ws.next())
            .await
            .expect("timeout elapsed");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = match serde_json::from_str(&text) {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                if v["method"] == json!("events.event") {
                    let evt = &v["params"]["event"];
                    let ty = evt["type"].as_str().unwrap_or("");
                    if types.contains(&ty) {
                        return evt.clone();
                    }
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

async fn boot() -> (Daemon, u16, Arc<ClientConfig>) {
    let data_dir = scratch_dir("data");
    let port_s = free_port().to_string();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", &port_s)];
    let child = spawn_serve(&data_dir, &env);
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
    (daemon, port, client_config(&fingerprint))
}

/// End-to-end: `workspace.update` over WSS publishes `workspace:updated` with
/// the applied `WorkspaceUpdate` delta as `changes` (§6.5). A previously
/// subscribed client sees the event without a follow-up `workspace.get`
/// round-trip.
#[tokio::test]
async fn workspace_update_emits_workspace_updated_over_wss() {
    let (daemon, port, cfg) = boot().await;
    let _ = &daemon;

    // Bootstrap a workspace off the UDS to avoid noise on the WSS event stream.
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "Original", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // Subscribe over WSS before mutating so the emission is guaranteed to be
    // observed (subscribers created after publish miss the event).
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:updated"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // Drive workspace.update over a separate WSS RPC connection.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    wss_rpc(
        &mut rpc,
        2,
        "workspace.update",
        json!({ "workspaceId": ws_id, "title": "Renamed", "tags": ["a", "b"] }),
    )
    .await;

    let evt = next_event(&mut sub, &["workspace:updated"], 10).await;
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    assert!(evt["id"].is_string(), "event id: {evt}");
    assert!(evt["timestamp"].is_string(), "timestamp: {evt}");
    assert_eq!(
        evt["actor"],
        json!({ "type": "system", "id": "system", "name": "System" })
    );
    // `changes` is the applied delta only; `skip_serializing_if = "Option::is_none"`
    // keeps un-supplied fields out of the payload (reference-parity emitter).
    assert_eq!(
        evt["data"],
        json!({
            "workspaceId": ws_id,
            "changes": { "title": "Renamed", "tags": ["a", "b"] },
        })
    );
}

/// End-to-end: `workspace.delete` over WSS publishes `workspace:deleted` with
/// the minimal `{ workspaceId }` payload (§6.5). The event fires only after
/// the store row is actually removed.
#[tokio::test]
async fn workspace_delete_emits_workspace_deleted_over_wss() {
    let (daemon, port, cfg) = boot().await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "ToDelete", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:deleted"], "workspaceId": ws_id }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    let mut rpc = connect_ws(port, cfg.clone()).await;
    wss_rpc(
        &mut rpc,
        2,
        "workspace.delete",
        json!({ "workspaceId": ws_id }),
    )
    .await;

    let evt = next_event(&mut sub, &["workspace:deleted"], 10).await;
    assert_eq!(evt["workspaceId"], ws_id.as_str());
    assert_eq!(evt["data"], json!({ "workspaceId": ws_id }));
}

/// End-to-end TASKFLOW-1 flow over WSS: authoring a note whose content holds
/// an `@@@task` block auto-converts the fence into a linked child task note
/// (fence-free parent + `note:created` for the child + `note:updated` for the
/// rewritten parent), `note.listTasks` surfaces the linked task, and
/// `task.assignAgent` flips the `not_started` task to `in_progress` with a
/// `task:status-changed` emission (PROTOCOL.md §6.5) — author → list →
/// delegate → in-progress without any follow-up reads.
#[tokio::test]
async fn task_block_author_list_assign_flow_over_wss() {
    let (daemon, port, cfg) = boot().await;

    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "Taskflow", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // Subscribe before authoring so all conversion emissions are observed.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({
            "eventTypes": ["note:created", "note:updated", "task:status-changed"],
            "workspaceId": ws_id,
        }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    // Author: `note.create` with an `@@@task` fence auto-converts on write.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        2,
        "note.create",
        json!({
            "workspaceId": ws_id,
            "title": "Plan",
            "content": "intro\n@@@task\n# Ship It\nbody\n@@@\ntail",
        }),
    )
    .await;
    let parent_id = created["note"]["id"].as_str().expect("note id").to_string();
    let content = created["note"]["content"].as_str().expect("content");
    assert!(!content.contains("@@@task"), "fence not removed: {content}");
    assert!(
        content.contains("- [ ] [Ship It](intent://local/task/"),
        "no task link: {content}"
    );

    // List: the converted block is a linked task row.
    let tasks = wss_rpc(
        &mut rpc,
        3,
        "note.listTasks",
        json!({ "workspaceId": ws_id, "noteId": parent_id }),
    )
    .await;
    let rows = tasks.as_array().expect("bare array");
    assert_eq!(rows.len(), 1, "rows: {tasks}");
    assert_eq!(rows[0]["text"], json!("Ship It"));
    assert_eq!(rows[0]["status"], json!("todo"));
    let task_id = rows[0]["taskNoteId"]
        .as_str()
        .expect("linked task note id")
        .to_string();
    assert_eq!(rows[0]["linkedTaskNoteId"], json!(task_id));

    // Authoring emitted `note:created` for the spawned child, `note:updated`
    // for the rewritten parent, and `note:created` for the parent itself.
    let mut saw_child_created = false;
    let mut saw_parent_updated = false;
    let mut saw_parent_created = false;
    for _ in 0..3 {
        let evt = next_event(&mut sub, &["note:created", "note:updated"], 10).await;
        let ty = evt["type"].as_str().unwrap_or("");
        let nid = evt["data"]["noteId"].as_str().unwrap_or("");
        match (ty, nid) {
            ("note:created", id) if id == task_id => {
                assert_eq!(evt["data"]["title"], json!("Ship It"));
                saw_child_created = true;
            }
            ("note:updated", id) if id == parent_id => saw_parent_updated = true,
            ("note:created", id) if id == parent_id => saw_parent_created = true,
            other => panic!("unexpected event {other:?}: {evt}"),
        }
    }
    assert!(saw_child_created && saw_parent_updated && saw_parent_created);

    // Delegate: assigning an agent to the `not_started` task flips it to
    // `in_progress` and publishes `task:status-changed`.
    let agent = "agent-b0a8044a-5eac-4b52-8456-15d3b784decb";
    let assign = wss_rpc(
        &mut rpc,
        4,
        "task.assignAgent",
        json!({ "workspaceId": ws_id, "noteId": task_id, "agentId": agent }),
    )
    .await;
    assert_eq!(assign["ok"], json!(true), "assign: {assign}");
    assert_eq!(assign["noteId"], json!(task_id));
    assert_eq!(assign["agentId"], json!(agent));

    let evt = next_event(&mut sub, &["task:status-changed"], 10).await;
    assert_eq!(evt["data"]["noteId"], json!(task_id));
    assert_eq!(evt["data"]["previousStatus"], json!("not_started"));
    assert_eq!(evt["data"]["newStatus"], json!("in_progress"));
    assert!(evt["data"]["changedAt"].is_string(), "changedAt: {evt}");

    // In-progress: the task note reflects the transition and the assignment.
    let got = wss_rpc(
        &mut rpc,
        5,
        "task.get",
        json!({ "workspaceId": ws_id, "taskNoteId": task_id }),
    )
    .await;
    assert_eq!(got["task"]["status"], json!("in_progress"), "task: {got}");
}
