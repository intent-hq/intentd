//! WSS end-to-end host-services (AUDIT-P2-1 / -P2-4): drives the additive
//! `host.*` detection methods — `host.findBinary`, `host.toolAvailability`,
//! `host.env`, `host.findApp`, and `host.listInstalledEditors` — over a real
//! pinned-TLS WebSocket against a live `intentd serve --listen both`. These
//! methods resolve binaries / PATH / environment / GUI apps on the daemon host
//! so a remote client sees what actually lives where workspaces run; this
//! suite proves the §5.14 wire contract end-to-end (HTTPS upgrade → JSON-RPC
//! 2.0 over WebSocket → host fast-path → response).
//!
//! Unlike the agent-lifecycle suite, host-services need neither a workspace nor
//! the mock ACP provider, so this test is self-contained and always runs.

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

/// Fixed 64-hex token, adopted by the daemon via the `INTENTD_AUTH_TOKEN` seam.
const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

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

fn free_port() -> u16 {
    StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-host-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .arg("--listen")
        .arg(listen)
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

/// One UDS JSON-RPC round-trip (used only to discover bound port + fingerprint).
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

/// Open an authenticated WSS connection (token in the query string).
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

/// Send one JSON-RPC frame and return the result whose id matches.
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

/// Boot a daemon over `--listen both` and return the live handle + a pinned WSS
/// client config plus the bound TCP port (discovered via UDS `system.status`).
async fn boot() -> (Daemon, u16, Arc<ClientConfig>) {
    let data_dir = temp_data_dir();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
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
    (daemon, port, client_config(&fingerprint))
}

/// host.findBinary / host.toolAvailability / host.env over the real WSS wire.
#[tokio::test]
async fn host_detection_services_over_wss() {
    let (_daemon, port, cfg) = boot().await;
    let mut ws = connect_ws(port, cfg).await;

    // §5.14 sanity: WSS connections report remote locality.
    let status = wss_rpc(&mut ws, 1, "host.status", json!({})).await;
    assert_eq!(status["locality"], "remote", "WSS ⇒ remote (§5.14)");

    // host.findBinary requires a `name` — missing ⇒ -32602 (PROTOCOL §9).
    {
        let frame = json!({ "jsonrpc": "2.0", "id": 2, "method": "host.findBinary", "params": {} });
        ws.send(Message::Text(frame.to_string())).await.unwrap();
        let err = loop {
            let next = timeout(Duration::from_secs(15), ws.next())
                .await
                .expect("timed out")
                .unwrap()
                .unwrap();
            if let Message::Text(t) = next {
                let v: Value = serde_json::from_str(&t).unwrap();
                if v["id"] == json!(2) {
                    break v;
                }
            }
        };
        assert_eq!(err["error"]["code"], -32602, "missing name ⇒ -32602: {err}");
    }

    // host.findBinary { name } ⇒ { available, path?, version? }. `git` ships on
    // the CI/host image, so assert the resolved shape rather than just a boolean.
    let git = wss_rpc(&mut ws, 3, "host.findBinary", json!({ "name": "git" })).await;
    assert!(
        git["available"].is_boolean(),
        "available always present: {git}"
    );
    if git["available"] == json!(true) {
        assert!(git["path"].is_string(), "available ⇒ path present: {git}");
    }

    // An unsafe binary name never errors — it resolves to available:false.
    let unsafe_name = wss_rpc(
        &mut ws,
        4,
        "host.findBinary",
        json!({ "name": "../../bin/sh" }),
    )
    .await;
    assert_eq!(unsafe_name["available"], false, "unsafe name ⇒ unavailable");

    // host.toolAvailability (default set) ⇒ { tools: { <name>: { available } } }.
    let tools = wss_rpc(&mut ws, 5, "host.toolAvailability", json!({})).await;
    let map = tools["tools"].as_object().expect("tools object");
    for name in ["claude", "codex", "cortex", "opencode", "git", "code"] {
        assert!(
            map.contains_key(name),
            "default set includes {name}: {tools}"
        );
        assert!(map[name]["available"].is_boolean());
    }

    // host.toolAvailability with an explicit list returns exactly those keys.
    let explicit = wss_rpc(
        &mut ws,
        6,
        "host.toolAvailability",
        json!({ "tools": ["git", "definitely-not-installed-xyzzy"] }),
    )
    .await;
    let explicit_map = explicit["tools"].as_object().unwrap();
    assert_eq!(explicit_map.len(), 2, "explicit list honoured: {explicit}");
    assert_eq!(
        explicit_map["definitely-not-installed-xyzzy"]["available"],
        false
    );

    // host.env ⇒ secret-safe PATH/env probe: path + entries + enhancedPath +
    // varNames (names only, no arbitrary values).
    let env = wss_rpc(&mut ws, 7, "host.env", json!({})).await;
    assert!(env["path"].is_string(), "path present: {env}");
    assert!(env["pathEntries"].is_array(), "pathEntries present: {env}");
    assert!(
        env["enhancedPath"].is_string(),
        "enhancedPath present: {env}"
    );
    assert!(env["varNames"].is_array(), "varNames present: {env}");
    // The auth token is injected as INTENTD_AUTH_TOKEN — its NAME may appear but
    // its VALUE must never cross the wire.
    assert!(
        !env.to_string().contains(TOKEN),
        "host.env must not leak secret env values"
    );
}

/// host.findApp / host.listInstalledEditors over the real WSS wire.
#[tokio::test]
async fn host_app_detection_services_over_wss() {
    let (_daemon, port, cfg) = boot().await;
    let mut ws = connect_ws(port, cfg).await;

    // host.findApp requires a `name` — missing ⇒ -32602 (PROTOCOL §9).
    {
        let frame = json!({ "jsonrpc": "2.0", "id": 100, "method": "host.findApp", "params": {} });
        ws.send(Message::Text(frame.to_string())).await.unwrap();
        let err = loop {
            let next = timeout(Duration::from_secs(15), ws.next())
                .await
                .expect("timed out")
                .unwrap()
                .unwrap();
            if let Message::Text(t) = next {
                let v: Value = serde_json::from_str(&t).unwrap();
                if v["id"] == json!(100) {
                    break v;
                }
            }
        };
        assert_eq!(err["error"]["code"], -32602, "missing name ⇒ -32602: {err}");
    }

    // host.findApp { name } ⇒ { installed, path?, source? }. A bogus but
    // syntactically-safe name resolves to `installed:false` on every host.
    let bogus = wss_rpc(
        &mut ws,
        101,
        "host.findApp",
        json!({ "name": "DefinitelyNotInstalledXyzzy" }),
    )
    .await;
    assert!(
        bogus["installed"].is_boolean(),
        "installed always present: {bogus}"
    );
    assert_eq!(bogus["installed"], false, "bogus app is not installed");

    // An unsafe name never errors — it resolves to installed:false.
    let unsafe_name = wss_rpc(
        &mut ws,
        102,
        "host.findApp",
        json!({ "name": "../../etc/passwd" }),
    )
    .await;
    assert_eq!(
        unsafe_name["installed"], false,
        "unsafe app name ⇒ uninstalled"
    );

    // host.listInstalledEditors ⇒ { editors: [{ id, installed, path?, source?,
    // flatpakId? }] }. Always replies; every entry carries id + installed.
    let editors_result = wss_rpc(&mut ws, 103, "host.listInstalledEditors", json!({})).await;
    let editors = editors_result["editors"].as_array().expect("editors array");
    assert!(!editors.is_empty(), "default catalog is non-empty");
    let ids: std::collections::HashSet<&str> = editors
        .iter()
        .map(|e| e["id"].as_str().expect("id"))
        .collect();
    for expected in ["vscode", "cursor", "zed"] {
        assert!(
            ids.contains(expected),
            "catalog includes {expected}: {editors_result}"
        );
    }
    for entry in editors {
        assert!(
            entry["installed"].is_boolean(),
            "installed boolean: {entry}"
        );
        if entry["installed"] == json!(true) {
            assert!(
                entry["source"].is_string(),
                "installed entries carry a source: {entry}"
            );
        }
    }
}

/// Seed one workspace with a filesystem root so `host.exec` can enforce the
/// within-workspace containment guard on `cwd`. Returns `(workspace_id, root)`.
async fn seed_workspace_with_path(data_dir: &Path, root: &Path) -> String {
    use intent_core::{
        now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
    };
    use intent_store::Store;
    let db_path = data_dir.join("intentd.db");
    let store = Store::open(&db_path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    let ts = now_iso();
    let ws = Workspace {
        id: ws_id.clone(),
        title: "WSS-HOST-EXEC".to_string(),
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
        path: Some(root.to_string_lossy().into_owned()),
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: Some(root.to_string_lossy().into_owned()),
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
    };
    store.insert_workspace(&ws).await.expect("insert ws");
    ws_id.0
}

/// Read the id-matched error frame after sending `frame`. Handles server
/// heartbeats by replying to `Ping` with `Pong` (matches the other WSS
/// helpers in this file); otherwise a mid-wait heartbeat could close the
/// connection and flake the test.
async fn wss_expect_error<S>(ws: &mut WebSocketStream<S>, id: i64) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let next = timeout(Duration::from_secs(15), ws.next())
            .await
            .expect("timed out")
            .unwrap()
            .unwrap();
        match next {
            Message::Text(t) => {
                let v: Value = serde_json::from_str(&t).unwrap();
                if v["id"] == json!(id) {
                    return v;
                }
            }
            Message::Ping(p) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            _ => {}
        }
    }
}

/// host.exec: happy-path round-trip + timeout + cwd-outside-workspace rejection.
#[tokio::test]
async fn host_exec_over_wss() {
    let (daemon, port, cfg) = boot().await;
    // Real filesystem root the daemon can `cd` into; kept alive until the
    // daemon drops (its `Drop` removes the whole data dir; the workspace root
    // is a sibling temp dir, cleaned up here).
    let root = std::env::temp_dir().join(format!("itd-wss-exec-root-{}", Uuid::new_v4().simple()));
    std::fs::create_dir_all(&root).expect("mkdir workspace root");
    let ws_id = seed_workspace_with_path(&daemon.data_dir, &root).await;
    let mut ws = connect_ws(port, cfg).await;

    // 1) Happy path — echo returns stdout + exitCode 0 without cwd validation.
    let out = wss_rpc(
        &mut ws,
        200,
        "host.exec",
        json!({ "command": "echo", "args": ["hello", "world"], "timeoutMs": 5000 }),
    )
    .await;
    assert_eq!(out["exitCode"], 0, "exitCode 0: {out}");
    assert_eq!(
        out["stdout"].as_str().unwrap().trim(),
        "hello world",
        "stdout carries argv payload: {out}"
    );
    assert!(
        out.get("timedOut").is_none(),
        "no timedOut on the happy path: {out}"
    );

    // 2) cwd inside the workspace succeeds — /bin/sh -c is intentionally NOT
    // used; the daemon spawns argv only. `pwd` prints the resolved cwd.
    let inside = wss_rpc(
        &mut ws,
        201,
        "host.exec",
        json!({
            "command": "pwd",
            "cwd": ".",
            "workspaceId": ws_id,
            "timeoutMs": 5000,
        }),
    )
    .await;
    assert_eq!(inside["exitCode"], 0, "cwd inside ⇒ ok: {inside}");
    let printed = inside["stdout"].as_str().unwrap().trim();
    // macOS `/tmp` resolves through a `/private` symlink; the daemon's lexical
    // guard operates on the resolved path so we accept either prefix here.
    let canonical = std::fs::canonicalize(&root)
        .unwrap_or_else(|_| root.clone())
        .to_string_lossy()
        .into_owned();
    assert!(
        printed == root.to_string_lossy() || printed == canonical,
        "pwd prints the workspace root ({} or {}): {printed}",
        root.display(),
        canonical
    );

    // 3) cwd OUTSIDE the workspace ⇒ -32603 with a clear containment message.
    let frame = json!({
        "jsonrpc": "2.0", "id": 202, "method": "host.exec",
        "params": {
            "command": "pwd",
            "cwd": "/etc",
            "workspaceId": ws_id,
            "timeoutMs": 5000,
        }
    });
    ws.send(Message::Text(frame.to_string())).await.unwrap();
    let err = wss_expect_error(&mut ws, 202).await;
    assert_eq!(err["error"]["code"], -32603, "cwd outside ⇒ -32603: {err}");
    assert!(
        err["error"]["message"]
            .as_str()
            .unwrap_or("")
            .contains("cwd outside workspace"),
        "clear containment message: {err}"
    );

    // 4) Timeout ⇒ result carries `timedOut: true` and the child is reaped
    // (SIGTERM → grace → SIGKILL on unix). Use `sleep 30` capped at 500ms.
    let timed_out = wss_rpc(
        &mut ws,
        203,
        "host.exec",
        json!({ "command": "sleep", "args": ["30"], "timeoutMs": 500 }),
    )
    .await;
    assert_eq!(
        timed_out["timedOut"], true,
        "timedOut flag set: {timed_out}"
    );

    // 5) Missing `command` ⇒ -32602 (PROTOCOL §9).
    let frame = json!({ "jsonrpc": "2.0", "id": 204, "method": "host.exec", "params": {} });
    ws.send(Message::Text(frame.to_string())).await.unwrap();
    let err = wss_expect_error(&mut ws, 204).await;
    assert_eq!(
        err["error"]["code"], -32602,
        "missing command ⇒ -32602: {err}"
    );

    let _ = std::fs::remove_dir_all(&root);
}

/// Read one `events.event` frame whose `event.type` matches `type_filter` AND
/// whose `event.data.requestId` equals `request_id`; ignore anything else.
async fn wss_next_stream_event<S>(
    ws: &mut WebSocketStream<S>,
    request_id: &str,
    type_filter: &[&str],
    secs: u64,
) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let next = timeout(Duration::from_secs(secs), ws.next())
            .await
            .expect("wss stream event timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] != "events.event" {
                    continue;
                }
                let event = &v["params"]["event"];
                let ty = event["type"].as_str().unwrap_or("");
                if !type_filter.contains(&ty) {
                    continue;
                }
                if event["data"]["requestId"].as_str() != Some(request_id) {
                    continue;
                }
                return v;
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => continue,
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// host.execStream: happy-path streaming (`cat` echoes an initial stdin payload
/// then a follow-up write closes stdin so the child exits) + cancel path
/// (`sleep 30` reaped by `host.execStream.cancel`) + `-32602` on missing
/// `command`. Exercises the full §5.14 streaming wire: `{ requestId }` on the
/// request, `host:exec:stdout` bus frames (base64 chunks), stdin write with
/// `eof=true`, and terminal `host:exec:exit`.
#[tokio::test]
async fn host_exec_stream_over_wss() {
    use base64::Engine as _;

    let (_daemon, port, cfg) = boot().await;

    // SUBSCRIBER conn — subscribe BEFORE starting the stream so no chunk is
    // missed. `workspaceId` is intentionally omitted: `host.execStream` without
    // a workspace context publishes under the empty-workspace id, and the
    // events fast-path routes matching frames to global (workspace-less)
    // subscribers on the same connection.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        300,
        "events.subscribe",
        json!({ "eventTypes": ["host:exec:stdout", "host:exec:stderr", "host:exec:exit"] }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // RPC conn — kick off a `cat` streaming exec with an initial stdin payload
    // that MUST be echoed back on stdout, correlated by the returned
    // requestId. `cat` reads to EOF, so the follow-up `write { eof:true }`
    // closes stdin and lets the child exit cleanly (exitCode=0).
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let started = wss_rpc(
        &mut rpc,
        301,
        "host.execStream",
        json!({ "command": "cat", "stdin": "hello world\n" }),
    )
    .await;
    let request_id = started["requestId"]
        .as_str()
        .expect("requestId in host.execStream result")
        .to_string();

    // Collect stdout chunks (base64) until the marker appears; stop on exit.
    let mut acc: Vec<u8> = Vec::new();
    let mut saw_exit = false;
    let mut exit_ok: Option<bool> = None;

    // First: watch for the initial stdin's echo.
    for _ in 0..40 {
        let v = wss_next_stream_event(
            &mut sub,
            &request_id,
            &["host:exec:stdout", "host:exec:exit"],
            15,
        )
        .await;
        let event = &v["params"]["event"];
        match event["type"].as_str() {
            Some("host:exec:stdout") => {
                if let Some(chunk) = event["data"]["chunk"].as_str() {
                    let bytes = base64::engine::general_purpose::STANDARD
                        .decode(chunk)
                        .expect("valid base64 in host:exec:stdout.chunk");
                    acc.extend_from_slice(&bytes);
                }
            }
            Some("host:exec:exit") => {
                saw_exit = true;
                exit_ok = event["data"]["ok"].as_bool();
                break;
            }
            _ => continue,
        }
        if String::from_utf8_lossy(&acc).contains("hello world") {
            break;
        }
    }
    assert!(
        String::from_utf8_lossy(&acc).contains("hello world"),
        "initial stdin was echoed on host:exec:stdout: {:?}",
        String::from_utf8_lossy(&acc)
    );

    // Send a follow-up stdin chunk + close so `cat` exits (unless it already
    // exited above via some other race — the write is idempotent-safe).
    if !saw_exit {
        let write_resp = wss_rpc(
            &mut rpc,
            302,
            "host.execStream.write",
            json!({ "requestId": &request_id, "stdin": "goodbye\n", "eof": true }),
        )
        .await;
        assert_eq!(write_resp["ok"], true, "write ok: {write_resp}");
        // Drain until we see the terminal exit frame.
        for _ in 0..40 {
            let v = wss_next_stream_event(
                &mut sub,
                &request_id,
                &["host:exec:stdout", "host:exec:exit"],
                15,
            )
            .await;
            let event = &v["params"]["event"];
            match event["type"].as_str() {
                Some("host:exec:stdout") => {
                    if let Some(chunk) = event["data"]["chunk"].as_str() {
                        let bytes = base64::engine::general_purpose::STANDARD
                            .decode(chunk)
                            .expect("valid base64 in host:exec:stdout.chunk");
                        acc.extend_from_slice(&bytes);
                    }
                }
                Some("host:exec:exit") => {
                    saw_exit = true;
                    exit_ok = event["data"]["ok"].as_bool();
                    break;
                }
                _ => continue,
            }
        }
    }
    assert!(
        saw_exit,
        "host:exec:exit reached (accumulated stdout so far: {:?})",
        String::from_utf8_lossy(&acc)
    );
    assert_eq!(exit_ok, Some(true), "cat exited cleanly (exitCode=0)");

    // ── Cancel path ────────────────────────────────────────────────────────
    // A long-lived `sleep 30` MUST be reaped by `host.execStream.cancel`
    // (SIGTERM → grace → SIGKILL). Terminal frame carries `cancelled:true`.
    let started = wss_rpc(
        &mut rpc,
        310,
        "host.execStream",
        json!({ "command": "sleep", "args": ["30"] }),
    )
    .await;
    let cancel_id = started["requestId"]
        .as_str()
        .expect("requestId for cancel case")
        .to_string();

    // Give the child a moment to start before cancelling.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let cancel_resp = wss_rpc(
        &mut rpc,
        311,
        "host.execStream.cancel",
        json!({ "requestId": &cancel_id }),
    )
    .await;
    assert_eq!(cancel_resp["ok"], true, "cancel ok: {cancel_resp}");
    assert_eq!(
        cancel_resp["cancelled"], true,
        "cancel flipped live token: {cancel_resp}"
    );

    // The exit frame arrives promptly (SIGTERM plus 500ms grace).
    let exit = wss_next_stream_event(&mut sub, &cancel_id, &["host:exec:exit"], 10).await;
    let data = &exit["params"]["event"]["data"];
    assert_eq!(data["cancelled"], true, "cancelled:true on exit: {exit}");
    assert_eq!(data["ok"], false, "cancelled sleep is not `ok`: {exit}");

    // A repeat cancel on the (now-finished) id is idempotent: `ok:true` still,
    // but `cancelled:false` because no live token remained.
    let repeat = wss_rpc(
        &mut rpc,
        312,
        "host.execStream.cancel",
        json!({ "requestId": &cancel_id }),
    )
    .await;
    assert_eq!(repeat["ok"], true, "idempotent cancel is ok: {repeat}");
    assert_eq!(repeat["cancelled"], false, "no live token: {repeat}");

    // ── -32602 arms ────────────────────────────────────────────────────────
    // Missing `command` on the stream request ⇒ -32602 (PROTOCOL §9).
    let frame = json!({ "jsonrpc": "2.0", "id": 320, "method": "host.execStream", "params": {} });
    rpc.send(Message::Text(frame.to_string())).await.unwrap();
    let err = wss_expect_error(&mut rpc, 320).await;
    assert_eq!(
        err["error"]["code"], -32602,
        "missing command ⇒ -32602: {err}"
    );
    // Missing `requestId` on the write / cancel surfaces likewise.
    let frame = json!({
        "jsonrpc": "2.0", "id": 321, "method": "host.execStream.write", "params": {}
    });
    rpc.send(Message::Text(frame.to_string())).await.unwrap();
    let err = wss_expect_error(&mut rpc, 321).await;
    assert_eq!(err["error"]["code"], -32602, "missing requestId ⇒ -32602");

    let frame = json!({
        "jsonrpc": "2.0", "id": 322, "method": "host.execStream.cancel", "params": {}
    });
    rpc.send(Message::Text(frame.to_string())).await.unwrap();
    let err = wss_expect_error(&mut rpc, 322).await;
    assert_eq!(err["error"]["code"], -32602, "missing requestId ⇒ -32602");
}

/// ACP model/readiness handshake probe over `host.execStream` (§5.14).
///
/// AUDIT-R1c-BE. R1b retired the four bidirectional-stdio ACP probes; the
/// replacement is **not** a net-new `provider.probeAcp` RPC but a thin FE parser
/// on top of the existing streaming exec surface, which already provides every
/// guarantee an ACP probe needs (argv-only spawn, process-group + `kill_on_drop`
/// reap on `timeoutMs`/cancel, PATH enrichment, workspace-cwd containment,
/// secret-safe env, initial `stdin` + streamed base64 stdout + terminal exit).
///
/// This e2e proves that shape end-to-end against the deterministic mock ACP
/// agent (`tests/fixtures/mock-acp-agent.mjs`, which responds to `initialize`
/// with `{ protocolVersion: 1, agentCapabilities: { loadSession: false } }`):
///
/// 1. **Handshake happy path** — subscribe to `host:exec:*`, call
///    `host.execStream` with `command:"node"`, `args:[mock-script]`, and an
///    initial `stdin` carrying the `initialize` JSON-RPC line. Assemble stdout
///    chunks (base64) until a full `\n`-terminated line arrives, parse it, and
///    assert the capability payload. Close stdin via
///    `host.execStream.write { eof:true }` so the mock agent exits cleanly.
/// 2. **Timeout reap** — call `host.execStream` on the same agent with a short
///    `timeoutMs` and **no** initial stdin. The agent blocks reading stdin, so
///    the daemon reaps the process group at the deadline and publishes
///    `host:exec:exit { timedOut:true, ok:false }`.
#[tokio::test]
async fn host_exec_stream_acp_handshake_probe_over_wss() {
    use base64::Engine as _;

    // Gate: skip when `node` or the mock script isn't available (parity with
    // the WSS agent-lifecycle suite's gate).
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping ACP handshake probe: node not on PATH");
        return;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping ACP handshake probe: mock script missing at {script}");
        return;
    }

    let (_daemon, port, cfg) = boot().await;

    // Subscriber conn: subscribe BEFORE spawning so no chunk is missed. No
    // workspaceId is passed on `host.execStream`, so events publish under the
    // empty-workspace id and the events fast-path routes them to global
    // subscribers on the same connection.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        400,
        "events.subscribe",
        json!({ "eventTypes": ["host:exec:stdout", "host:exec:stderr", "host:exec:exit"] }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // ── Handshake happy path ──────────────────────────────────────────────
    // The initialize line is what a real FE probe would send; the mock ACP
    // agent responds with a single `\n`-terminated JSON-RPC result line.
    let init_line =
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":1}}\n";

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let started = wss_rpc(
        &mut rpc,
        401,
        "host.execStream",
        json!({
            "command": "node",
            "args": [&script],
            "stdin": init_line,
            "timeoutMs": 15_000,
        }),
    )
    .await;
    let request_id = started["requestId"]
        .as_str()
        .expect("requestId on handshake exec")
        .to_string();

    // Accumulate stdout base64 chunks until we have at least one full line.
    let mut acc: Vec<u8> = Vec::new();
    let mut parsed: Option<Value> = None;
    for _ in 0..40 {
        let v = wss_next_stream_event(
            &mut sub,
            &request_id,
            &["host:exec:stdout", "host:exec:exit"],
            15,
        )
        .await;
        let event = &v["params"]["event"];
        match event["type"].as_str() {
            Some("host:exec:stdout") => {
                if let Some(chunk) = event["data"]["chunk"].as_str() {
                    let bytes = base64::engine::general_purpose::STANDARD
                        .decode(chunk)
                        .expect("valid base64 in host:exec:stdout.chunk");
                    acc.extend_from_slice(&bytes);
                }
                if let Some(nl) = acc.iter().position(|b| *b == b'\n') {
                    let line = &acc[..nl];
                    parsed =
                        Some(serde_json::from_slice(line).expect("mock agent stdout is JSON-RPC"));
                    break;
                }
            }
            Some("host:exec:exit") => {
                panic!(
                    "child exited before reply (acc={:?})",
                    String::from_utf8_lossy(&acc)
                );
            }
            _ => continue,
        }
    }
    let parsed = parsed.expect("received a JSON-RPC reply line on stdout");

    // Assert the ACP capability payload the mock agent returns for `initialize`.
    assert_eq!(parsed["jsonrpc"], "2.0", "handshake reply: {parsed}");
    assert_eq!(parsed["id"], 1, "handshake reply: {parsed}");
    assert_eq!(
        parsed["result"]["protocolVersion"], 1,
        "handshake reply carries protocolVersion=1: {parsed}"
    );
    assert_eq!(
        parsed["result"]["agentCapabilities"]["loadSession"], false,
        "handshake reply carries capability payload: {parsed}"
    );

    // Close stdin so the mock agent's readline loop drains and the child exits
    // cleanly (exitCode=0). The exit frame surfaces on the bus.
    let write_resp = wss_rpc(
        &mut rpc,
        402,
        "host.execStream.write",
        json!({ "requestId": &request_id, "eof": true }),
    )
    .await;
    assert_eq!(write_resp["ok"], true, "eof write ok: {write_resp}");

    let exit = wss_next_stream_event(&mut sub, &request_id, &["host:exec:exit"], 15).await;
    let data = &exit["params"]["event"]["data"];
    assert_eq!(data["ok"], true, "mock agent exits cleanly: {exit}");
    assert!(
        data.get("timedOut").and_then(Value::as_bool) != Some(true),
        "no timedOut on the happy handshake path: {exit}"
    );

    // ── Timeout reap path ─────────────────────────────────────────────────
    // Spawn the same agent with a short `timeoutMs` and NO initial stdin. The
    // agent's readline loop blocks waiting for input, so the daemon must reap
    // the process group at the deadline and surface `timedOut:true`.
    let started = wss_rpc(
        &mut rpc,
        410,
        "host.execStream",
        json!({
            "command": "node",
            "args": [&script],
            "timeoutMs": 500,
        }),
    )
    .await;
    let timeout_id = started["requestId"]
        .as_str()
        .expect("requestId on timeout-probe exec")
        .to_string();

    let exit = wss_next_stream_event(&mut sub, &timeout_id, &["host:exec:exit"], 10).await;
    let data = &exit["params"]["event"]["data"];
    assert_eq!(
        data["timedOut"], true,
        "timedOut:true on the reap path: {exit}"
    );
    assert_eq!(data["ok"], false, "reaped child is not `ok`: {exit}");
}

/// Verify host.findBinary resolves binaries from login-shell PATH enrichment
/// when the daemon runs with minimal PATH. Spawns intentd with a controlled
/// minimal PATH and a fake $SHELL that outputs a PATH containing a temp dir
/// holding a unique binary; asserts host.findBinary resolves that binary.
#[tokio::test]
async fn host_find_binary_uses_login_shell_path() {
    let data_dir = temp_data_dir();

    // Create a unique temp dir with a fake binary
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let fake_bin_dir = data_dir.join(format!("fake_login_bin_{pid}_{nanos}"));
    std::fs::create_dir_all(&fake_bin_dir).unwrap();

    let bin_name = format!("test-login-bin-{pid}-{nanos}");
    let bin_path = fake_bin_dir.join(&bin_name);
    std::fs::write(&bin_path, "#!/bin/sh\nexit 0\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&bin_path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    // Create a fake shell script that outputs the enriched PATH when invoked with -lc
    let fake_shell_path = data_dir.join("fake_shell.sh");
    let shell_script = format!(
        "#!/bin/sh\nif [ \"$1\" = \"-lc\" ]; then\n  # Execute the command but in an environment where PATH is our fake dir\n  PATH=\"{}\" eval \"$2\"\nfi\n",
        fake_bin_dir.display()
    );
    std::fs::write(&fake_shell_path, shell_script).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&fake_shell_path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    // Spawn daemon with minimal PATH and fake SHELL
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("PATH", "/usr/bin:/bin"), // Minimal PATH that won't find our binary
        ("SHELL", fake_shell_path.to_str().unwrap()),
    ];
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
    let cfg = client_config(&fingerprint);

    let mut ws = connect_ws(port, cfg).await;

    // Call host.findBinary for our unique binary name
    let result = wss_rpc(&mut ws, 2, "host.findBinary", json!({ "name": &bin_name })).await;

    // Should find the binary via login-shell PATH enrichment
    assert_eq!(
        result["available"], true,
        "Binary should be found via login-shell PATH: {result}"
    );
    assert_eq!(
        result["path"].as_str().unwrap(),
        bin_path.to_str().unwrap(),
        "Binary path should match: {result}"
    );

    drop(daemon);
}

/// WSS e2e for host.providerDiscovery: proves the providers + npx wire envelope.
#[tokio::test]
async fn host_provider_discovery_over_wss() {
    let data_dir = temp_data_dir();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
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
    let cfg = client_config(&fingerprint);

    let mut ws = connect_ws(port, cfg).await;

    // Call host.providerDiscovery
    let result = wss_rpc(&mut ws, 2, "host.providerDiscovery", json!({})).await;

    // Assert wire contract shape
    assert!(result.is_object(), "result must be an object: {result}");
    assert!(
        result["providers"].is_array(),
        "providers must be an array: {result}"
    );
    assert!(result["npx"].is_object(), "npx must be an object: {result}");

    // Check npx fields
    let npx = &result["npx"];
    assert!(
        npx.get("resolvedPath").is_some(),
        "npx.resolvedPath must exist: {npx}"
    );
    assert!(
        npx.get("version").is_some(),
        "npx.version must exist: {npx}"
    );
    assert!(
        npx["versionOk"].is_boolean(),
        "npx.versionOk must be boolean: {npx}"
    );

    // Check providers array
    let providers = result["providers"].as_array().unwrap();
    assert!(
        !providers.is_empty(),
        "providers array should not be empty: {result}"
    );

    // Check first provider shape
    let p0 = &providers[0];
    assert!(p0["id"].is_string(), "provider.id must be string: {p0}");
    assert!(
        p0["displayName"].is_string(),
        "provider.displayName must be string: {p0}"
    );
    assert!(
        p0["command"].is_string(),
        "provider.command must be string: {p0}"
    );
    assert!(
        p0["installed"].is_boolean(),
        "provider.installed must be boolean: {p0}"
    );
    assert!(
        p0["hasNpxFallback"].is_boolean(),
        "provider.hasNpxFallback must be boolean: {p0}"
    );

    drop(daemon);
}
