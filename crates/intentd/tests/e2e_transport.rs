//! E2E transport suite (§13.1 E2E row): boot a REAL `intentd serve` and drive
//! it end-to-end over BOTH transports — UDS (via the `intentd` control client)
//! and TCP/TLS (via a pinned WebSocket client) — asserting the full transport +
//! lifecycle surface:
//!
//! - bind on UDS + TCP/TLS (live `system.status` reports both);
//! - bearer auth over WSS (accept valid / reject missing + bad token);
//! - origin allow-list (accept loopback + no-origin, reject cross-origin);
//! - the self-signed TLS fingerprint matches the mDNS-advertised `fp`;
//! - mDNS advertise→resolve round-trip (browse the live service);
//! - `intentd status` / `doctor` against the live daemon;
//! - graceful `intentd stop` with a clean immediate restart (no EADDRINUSE);
//! - idle session reaping (gated on the mock ACP provider, like the M3 E2E).
//!
//! Hermetic + deterministic: each daemon gets a private data dir, an OS-assigned
//! free TCP port (no fixed-5180 contention), and a known auth token + discovery
//! enabled through the flagged test seams in `cmd_serve`
//! (`INTENTD_AUTH_TOKEN` / `INTENTD_TCP_PORT` / `INTENTD_DISCOVERY` /
//! `INTENTD_IDLE_REAP_MS`).

#![cfg(unix)]

use std::net::{Ipv4Addr, TcpListener as StdTcpListener};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use intent_core::{
    now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
};
use intent_store::Store;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

/// A fixed 64-char hex token (valid shape) the daemon adopts via the
/// `INTENTD_AUTH_TOKEN` seam, so the client can authenticate hermetically.
const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

/// A spawned `intentd serve` process; killed and its data dir removed on drop.
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

/// A free localhost TCP port (bound then released to discover the number).
fn free_port() -> u16 {
    StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Create a short data dir (`/tmp/itd-e2e-XXXXXXXX`) so `data_dir/intentd.sock`
/// fits within `SUN_LEN` (~104 bytes).
fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-e2e-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

/// Spawn `intentd serve --listen <listen>` with the given data dir + env. The
/// caller holds the returned [`Daemon`] for the test's lifetime.
fn spawn_serve(data_dir: &PathBuf, listen: &str, env: &[(&str, &str)]) -> Child {
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

/// Run an `intentd <args>` control subcommand against `data_dir` to completion.
fn run_cli(data_dir: &PathBuf, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_intentd"))
        .args(args)
        .env("INTENTD_DATA_DIR", data_dir)
        .output()
        .expect("run intentd subcommand")
}

/// Wait (up to 10s) for the daemon's UDS to accept connections.
async fn await_uds(socket: &PathBuf) -> bool {
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

/// One UDS JSON-RPC round-trip (one connection per call); returns the full frame.
async fn uds_rpc(socket: &PathBuf, id: i64, method: &str, params: Value) -> Value {
    let stream = UnixStream::connect(socket).await.expect("connect uds");
    let (read_half, mut write_half) = stream.into_split();
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    let mut line = serde_json::to_string(&frame).unwrap();
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

/// Client cert verifier pinning the server's SHA-256 fingerprint (colon hex),
/// validating the handshake signature with the ring provider — no PKI/hostname
/// checks (TOFU, the M5 mobile-client posture).
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
        let fp = sha256_fingerprint(end_entity.as_ref());
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

/// Colon-separated UPPERCASE hex SHA-256 over a DER body (PROTOCOL §1.2).
fn sha256_fingerprint(der: &[u8]) -> String {
    Sha256::digest(der)
        .iter()
        .map(|b| format!("{b:02X}"))
        .collect::<Vec<_>>()
        .join(":")
}

/// A pinning [`ClientConfig`] on the ring provider (the only provider compiled
/// in — see the workspace `rustls` feature set).
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

/// Open a pinned TLS stream to the listener at `127.0.0.1:port` (SNI `localhost`).
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

/// Build a WebSocket upgrade request head with optional Origin / query token.
fn upgrade_req(target: &str, origin: Option<&str>) -> String {
    let mut r = format!(
        "GET {target} HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n"
    );
    if let Some(o) = origin {
        r.push_str(&format!("Origin: {o}\r\n"));
    }
    r.push_str("\r\n");
    r
}

/// Send a raw upgrade request over TLS and return the HTTP status code from the
/// first response line. Reads only the status line (bounded by a short timeout),
/// so a successful `101` (socket kept open) is handled the same as a rejection.
async fn http_status(port: u16, cfg: Arc<ClientConfig>, request: &str) -> u16 {
    let mut tls = tls_connect(port, cfg).await;
    tls.write_all(request.as_bytes()).await.expect("write");
    tls.flush().await.expect("flush");
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    let _ = timeout(Duration::from_secs(3), async {
        loop {
            match tls.read(&mut byte).await {
                Ok(0) => break,
                Ok(_) => {
                    buf.push(byte[0]);
                    if buf.ends_with(b"\r\n") {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    })
    .await;
    String::from_utf8_lossy(&buf)
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0)
}

/// One authenticated WSS JSON-RPC round-trip (token in the query string).
async fn wss_call(port: u16, cfg: Arc<ClientConfig>, frame: &str) -> Value {
    let tls = tls_connect(port, cfg).await;
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    let (mut ws, _resp) = tokio_tungstenite::client_async(url, tls)
        .await
        .expect("ws handshake");
    ws.send(Message::Text(frame.to_string()))
        .await
        .expect("send");
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(text))) => return serde_json::from_str(&text).expect("json"),
            Some(Ok(_)) => continue,
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// Browse the daemon's mDNS advertisement and return the resolved service whose
/// SRV port equals `port` (disambiguating it from any other advertiser on the
/// host). Panics on timeout — the advertise→resolve round-trip must complete.
fn resolve_advert(port: u16) -> mdns_sd::ServiceInfo {
    use mdns_sd::{ServiceDaemon, ServiceEvent};
    let daemon = ServiceDaemon::new().expect("create mdns daemon");
    let rx = daemon
        .browse(intent_transport::SERVICE_TYPE)
        .expect("browse service type");
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let resolved = loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out resolving mDNS advert on :{port}"
        );
        match rx.recv_timeout(remaining) {
            Ok(ServiceEvent::ServiceResolved(found)) if found.get_port() == port => break found,
            Ok(_) => continue,
            Err(_) => panic!("timed out resolving mDNS advert on :{port}"),
        }
    };
    let _ = daemon.shutdown();
    resolved
}

#[tokio::test]
async fn e2e_transport_full() {
    let data_dir = temp_data_dir();
    let port_s = free_port().to_string();
    let env: [(&str, &str); 3] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
        ("INTENTD_DISCOVERY", "1"),
    ];
    let mut daemon = Daemon {
        child: spawn_serve(&data_dir, "both", &env),
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    let pidfile = data_dir.join("intentd.pid");
    assert!(
        await_uds(&socket).await,
        "daemon did not start (see {}/daemon.log)",
        data_dir.display()
    );

    // --- bind on UDS + TCP/TLS: the live control RPC reports both transports ---
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let r = &status["result"];
    assert_eq!(r["running"], true, "status: {status}");
    assert_eq!(r["listenMode"], "both");
    assert_eq!(r["transports"], json!(["uds", "tcp"]));
    assert_eq!(r["host"]["locality"], "local", "UDS control ⇒ local");
    let bound_port = r["port"].as_u64().expect("bound tcp port") as u16;
    let fingerprint = r["fingerprint"]
        .as_str()
        .expect("cert fingerprint")
        .to_string();
    assert!(fingerprint.contains(':'), "fp is colon hex: {fingerprint}");

    // The persisted cert inspected off-process carries the same fingerprint.
    match intent_transport::inspect_cert(&data_dir) {
        intent_transport::CertStatus::Valid { fingerprint: disk } => {
            assert_eq!(disk, fingerprint, "on-disk cert fp matches status fp")
        }
        other => panic!("expected a valid persisted cert, got {other:?}"),
    }

    let cfg = client_config(&fingerprint);

    // --- bearer auth over WSS: reject missing + bad token ---
    assert_eq!(
        http_status(bound_port, cfg.clone(), &upgrade_req("/ws", None)).await,
        401,
        "missing token must be rejected"
    );
    assert_eq!(
        http_status(
            bound_port,
            cfg.clone(),
            &upgrade_req("/ws?token=nope", None)
        )
        .await,
        401,
        "bad token must be rejected"
    );

    // --- origin allow-list: reject cross-origin, accept loopback ---
    let cross = upgrade_req(&format!("/ws?token={TOKEN}"), Some("http://evil.example"));
    assert_eq!(
        http_status(bound_port, cfg.clone(), &cross).await,
        403,
        "cross-origin must be rejected"
    );
    let loopback = upgrade_req(&format!("/ws?token={TOKEN}"), Some("http://localhost"));
    assert_eq!(
        http_status(bound_port, cfg.clone(), &loopback).await,
        101,
        "loopback origin must upgrade"
    );

    // --- accept valid token (no origin): a real WSS JSON-RPC round-trip that is
    // byte-identical to the UDS transport (the two listeners share one router) ---
    let frame = r#"{"jsonrpc":"2.0","id":7,"method":"agent.getModels"}"#;
    let wss = wss_call(bound_port, cfg.clone(), frame).await;
    assert_eq!(wss["id"], 7);
    assert!(
        !wss["result"]["models"].as_array().unwrap().is_empty(),
        "models must be non-empty over WSS"
    );
    let uds = uds_rpc(&socket, 7, "agent.getModels", json!({})).await;
    assert_eq!(wss["result"], uds["result"], "WSS result must match UDS");

    // --- mDNS advertise→resolve round-trip + fingerprint match ---
    let advert = tokio::task::spawn_blocking(move || resolve_advert(bound_port))
        .await
        .expect("join mdns browse");
    assert_eq!(advert.get_type(), intent_transport::SERVICE_TYPE);
    assert_eq!(advert.get_port(), bound_port);
    assert_eq!(advert.get_property_val_str("path"), Some("/ws"));
    assert_eq!(
        advert.get_property_val_str("fp"),
        Some(fingerprint.as_str()),
        "advertised mDNS fp must equal the pinned TLS fingerprint"
    );

    // --- `intentd status` against the live daemon ---
    let status_cli = run_cli(&data_dir, &["status"]);
    assert!(
        status_cli.status.success(),
        "status exit: {}",
        String::from_utf8_lossy(&status_cli.stderr)
    );
    let out = String::from_utf8_lossy(&status_cli.stdout);
    assert!(out.contains("intentd: up"), "status stdout: {out}");
    assert!(out.contains("transports: uds, tcp"), "status stdout: {out}");

    // --- `intentd doctor` against the healthy live data dir (⇒ exit 0) ---
    let doctor = run_cli(&data_dir, &["doctor"]);
    let dout = String::from_utf8_lossy(&doctor.stdout);
    assert!(doctor.status.success(), "doctor failed: {dout}");
    assert!(dout.contains("migrations current"), "doctor stdout: {dout}");

    // --- graceful `intentd stop`: clean socket + pidfile, then clean restart ---
    let stop_dir = data_dir.clone();
    let stop = tokio::task::spawn_blocking(move || run_cli(&stop_dir, &["stop"]));
    let reaped = timeout(Duration::from_secs(15), async {
        loop {
            if matches!(daemon.child.try_wait(), Ok(Some(_))) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(reaped, "daemon did not exit after stop");
    let stop = stop.await.expect("join stop");
    assert!(
        stop.status.success(),
        "stop exit: {}",
        String::from_utf8_lossy(&stop.stderr)
    );
    assert!(!socket.exists(), "socket not cleaned after stop");
    assert!(!pidfile.exists(), "pidfile not cleaned after stop");
    // The child already exited and was reaped above; don't let `Drop` remove the
    // data dir before the restart reuses it.
    std::mem::forget(daemon);

    // Immediate restart on the SAME data dir + SAME TCP port: the freed UDS and
    // listen port must rebind with no stale-owner refusal / EADDRINUSE.
    let restart = Daemon {
        child: spawn_serve(&data_dir, "both", &env),
        data_dir: data_dir.clone(),
    };
    assert!(await_uds(&socket).await, "daemon did not restart cleanly");
    let again = uds_rpc(&socket, 2, "system.status", json!({})).await;
    assert_eq!(again["result"]["running"], true, "restart status: {again}");
    assert_eq!(
        again["result"]["transports"],
        json!(["uds", "tcp"]),
        "restart serves both transports"
    );
    drop(restart);
}

/// Write a JSON frame + newline to a persistent UDS connection.
async fn send_frame(write_half: &mut (impl AsyncWriteExt + Unpin), frame: Value) {
    let line = serde_json::to_string(&frame).unwrap();
    write_half.write_all(line.as_bytes()).await.unwrap();
    write_half.write_all(b"\n").await.unwrap();
    write_half.flush().await.unwrap();
}

/// Read one newline-delimited JSON frame (bounded by `secs`).
async fn read_frame(reader: &mut (impl AsyncBufReadExt + Unpin), secs: u64) -> Value {
    let mut line = String::new();
    let n = timeout(Duration::from_secs(secs), reader.read_line(&mut line))
        .await
        .expect("timed out waiting for a frame")
        .expect("read failed");
    assert!(n > 0, "connection closed unexpectedly");
    serde_json::from_str(line.trim_end()).expect("invalid JSON frame")
}

/// Minimal active workspace for the reaping seed.
fn workspace(id: &WorkspaceId) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "Reap".to_string(),
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
        archived: false,
        archived_at: None,
    }
}

/// Idle session reaping (§5.6/§6.7) over the live daemon: drive one mock ACP turn
/// (which spawns a child that goes idle when the turn ends) and assert the TTL
/// sweep evicts it — the live `system.status` agent count returns to zero with no
/// explicit stop. Uses the `INTENTD_IDLE_REAP_MS` seam for a sub-second sweep.
/// Gated on `node` + the mock script (the CI ACP gate); skips cleanly otherwise.
#[tokio::test]
async fn e2e_idle_session_reaping() {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping idle-reaping E2E: node not on PATH");
        return;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping idle-reaping E2E: mock script missing at {script}");
        return;
    }

    let data_dir = temp_data_dir();
    let ws = WorkspaceId::new();
    {
        let store = Store::open(&data_dir.join("intentd.db"))
            .await
            .expect("open store");
        store
            .insert_workspace(&workspace(&ws))
            .await
            .expect("insert ws");
    }
    let behavior = json!({ "response": "going idle" }).to_string();
    let daemon = Daemon {
        child: spawn_serve(
            &data_dir,
            "uds",
            &[
                ("MOCK_AGENT_SCRIPT_PATH", &script),
                ("MOCK_AGENT_BEHAVIOR", &behavior),
                // Sub-second TTL + sweep so reaping is observable without a ≥30s
                // wait (test-only seam; clamped to minutes in production).
                ("INTENTD_IDLE_REAP_MS", "800"),
            ],
        ),
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    // Subscribe BEFORE the turn so the terminal stream:end is never missed.
    let (sub_read, mut sub_write) = UnixStream::connect(&socket)
        .await
        .expect("sub connect")
        .into_split();
    let mut sub_reader = BufReader::new(sub_read);
    send_frame(
        &mut sub_write,
        json!({ "jsonrpc": "2.0", "id": 1, "method": "events.subscribe",
            "params": { "eventTypes": ["agent:*"], "workspaceId": ws.0 } }),
    )
    .await;
    let sub_resp = read_frame(&mut sub_reader, 5).await;
    assert!(
        sub_resp["result"]["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // Create the agent and drive one turn (lazily spawns the child process).
    let created = uds_rpc(
        &socket,
        2,
        "agent.create",
        json!({ "workspaceId": ws.0, "name": "reap", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let sent = uds_rpc(
        &socket,
        3,
        "agent.sendMessage",
        json!({ "workspaceId": ws.0, "agentId": agent_id, "content": "hi" }),
    )
    .await;
    assert_eq!(sent["result"]["success"], true, "sendMessage: {sent}");

    // The turn ends (agent registered + marked idle) at stream:end.
    let mut saw_end = false;
    for _ in 0..80 {
        let frame = read_frame(&mut sub_reader, 30).await;
        if frame["method"] == "events.event"
            && frame["params"]["event"]["type"] == "agent:stream:end"
        {
            saw_end = true;
            break;
        }
    }
    assert!(saw_end, "agent turn reached stream:end");

    // The idle agent must be reaped by the TTL sweep — the live registry count
    // returns to zero with no explicit `agent.stop`.
    let reaped = timeout(Duration::from_secs(15), async {
        loop {
            let st = uds_rpc(&socket, 4, "system.status", json!({})).await;
            if st["result"]["agents"].as_u64() == Some(0) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(reaped, "idle agent was not reaped by the TTL sweep");
    drop(daemon);
}
