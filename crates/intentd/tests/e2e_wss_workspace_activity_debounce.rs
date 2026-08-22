//! WSS end-to-end: `workspace:activity-changed` debounce behavior over the wire.
//!
//! Proves over a real WSS connection that the busy→idle transition is debounced
//! (~3s default, 50ms in test) using the mock ACP agent fixture to drive real
//! agent activity, and that the derived `displayStatus` tracks the same
//! transitions in lockstep: the agent run promotes it to `in_progress`
//! (emitting `workspace:displayStatus-changed`) and the debounced idle flip
//! demotes it back to `idle`.

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
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
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

fn scratch_dir(prefix: &str) -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-actdebounce-{prefix}-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir scratch dir");
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

async fn uds_rpc(socket: &Path, id: u64, method: &str, params: Value) -> Value {
    let mut stream = UnixStream::connect(socket).await.expect("uds connect");
    let req = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
    let msg = format!("{}\n", serde_json::to_string(&req).unwrap());
    stream.write_all(msg.as_bytes()).await.expect("uds write");
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await.expect("uds read");
    serde_json::from_str(&line).expect("parse uds response")
}

#[derive(Debug)]
struct PinnedVerifier {
    fingerprint: String,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinnedVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer,
        _intermediates: &[CertificateDer],
        _server_name: &ServerName,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let hash = Sha256::digest(end_entity.as_ref());
        let hex = hash
            .iter()
            .map(|b| format!("{b:02X}"))
            .collect::<Vec<_>>()
            .join(":");
        if hex == self.fingerprint {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(format!(
                "fingerprint mismatch: expected {} got {}",
                self.fingerprint, hex
            )))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer,
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
        cert: &CertificateDer,
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
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

async fn wss_rpc(
    ws: &mut WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>,
    id: u64,
    method: &str,
    params: Value,
) -> Value {
    let req = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
    ws.send(Message::Text(req.to_string().into()))
        .await
        .expect("ws send");
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(txt))) => {
                let v: Value = serde_json::from_str(&txt).expect("parse ws msg");
                if v.get("id").and_then(serde_json::Value::as_u64) == Some(id) {
                    return v;
                }
            }
            Some(Ok(Message::Ping(p))) => {
                ws.send(Message::Pong(p)).await.expect("pong");
            }
            Some(Ok(_)) => {}
            _ => panic!("ws closed before response"),
        }
    }
}

/// Wait for a specific event type or timeout after `deadline_secs`.
async fn wss_event_opt(
    ws: &mut WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>,
    deadline_secs: u64,
) -> Option<Value> {
    timeout(Duration::from_secs(deadline_secs), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(txt))) => {
                    let v: Value = serde_json::from_str(&txt).ok()?;
                    if v.get("method").and_then(|m| m.as_str()) == Some("events.event") {
                        return Some(v);
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    let _ = ws.send(Message::Pong(p)).await;
                }
                Some(Ok(_)) => {}
                _ => return None,
            }
        }
    })
    .await
    .ok()
    .flatten()
}

async fn boot(mock_script: &str, behavior: &str) -> (Daemon, u16, Arc<ClientConfig>) {
    let data_dir = scratch_dir("data");
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("WORKSPACE_IDLE_DEBOUNCE_TEST_MS", "50"),
        ("MOCK_AGENT_SCRIPT_PATH", mock_script),
        ("MOCK_AGENT_BEHAVIOR", behavior),
    ];
    let child = spawn_serve(&data_dir, &env);
    let daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    (daemon, port, client_config(&fingerprint))
}

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

#[tokio::test]
async fn workspace_activity_changed_debounce() {
    let Some(script) = gate("WSS activity debounce") else {
        return;
    };

    let behavior = json!({ "response": "test activity" }).to_string();
    let (daemon, port, cfg) = boot(&script, &behavior).await;

    // Bootstrap workspace via UDS.
    let socket = daemon.data_dir.join("intentd.sock");
    let create = uds_rpc(
        &socket,
        2,
        "workspace.create",
        json!({ "title": "ActivityDebounceTest", "branch": "main", "skipWorktree": true }),
    )
    .await;
    let ws_id = create["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // Seed the displayStatus last-observed baseline (a first observation
    // never emits) and pin the pre-run wire value: no agent running → idle.
    let got = uds_rpc(&socket, 3, "workspace.get", json!({ "workspaceId": ws_id })).await;
    assert_eq!(
        got["result"]["workspace"]["displayStatus"], "idle",
        "pre-run displayStatus is idle: {got}"
    );

    // Subscribe to workspace:* before any activity, scoped to this workspace.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_res["result"]["subscriptionId"].is_string(),
        "sub id: {sub_res}"
    );

    // Drive activity: create + run an agent.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        2,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "TestAgent", "model": "mock:default" }),
    )
    .await;
    let agent_id = created["result"]["agent"]["id"].as_str().expect("agent id");

    wss_rpc(
        &mut rpc,
        3,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "do work" }),
    )
    .await;

    // Drain events until we see the agent_running workspace activity change
    // plus the displayStatus promotion to in_progress that rides the same
    // transition. The subscription is scoped to this workspace, so we expect
    // only relevant events.
    let mut saw_agent_running = false;
    let mut saw_in_progress = false;
    for _ in 0..40 {
        if saw_agent_running && saw_in_progress {
            break;
        }
        if let Some(ev) = wss_event_opt(&mut sub, 2).await {
            let ev = &ev["params"]["event"];
            if ev["type"] == "workspace:activity-changed"
                && ev["data"]["activity"] == "agent_running"
            {
                saw_agent_running = true;
            }
            if ev["type"] == "workspace:displayStatus-changed"
                && ev["data"]["displayStatus"] == "in_progress"
            {
                saw_in_progress = true;
            }
        }
    }
    assert!(
        saw_agent_running,
        "expected workspace:activity-changed agent_running"
    );
    assert!(
        saw_in_progress,
        "expected workspace:displayStatus-changed in_progress during the run"
    );

    // Agent completes automatically. Wait a bit longer for the debounce + idle emission.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Drain events until we see the idle workspace activity change plus the
    // displayStatus demotion that rides the same debounced flip. The demoted
    // status is `idle`: the completed turn raises the server-owned unread
    // flag (§9.9), but the flag is not a displayStatus axis (§6.5).
    let mut saw_idle = false;
    let mut saw_status_idle = false;
    for _ in 0..40 {
        if saw_idle && saw_status_idle {
            break;
        }
        if let Some(ev) = wss_event_opt(&mut sub, 2).await {
            let ev = &ev["params"]["event"];
            if ev["type"] == "workspace:activity-changed" && ev["data"]["activity"] == "idle" {
                saw_idle = true;
            }
            if ev["type"] == "workspace:displayStatus-changed"
                && ev["data"]["displayStatus"] == "idle"
            {
                saw_status_idle = true;
            }
        }
    }
    assert!(
        saw_idle,
        "expected workspace:activity-changed idle after debounce"
    );
    assert!(
        saw_status_idle,
        "expected workspace:displayStatus-changed idle after debounce"
    );

    // The post-run read path agrees with the event stream; the turn-end
    // unread flag persists on `attention` without moving the rollup.
    let got = uds_rpc(&socket, 4, "workspace.get", json!({ "workspaceId": ws_id })).await;
    assert_eq!(
        got["result"]["workspace"]["displayStatus"], "idle",
        "post-run displayStatus is idle: {got}"
    );
    assert_eq!(
        got["result"]["workspace"]["attention"], "unread",
        "turn-end unread flag persists: {got}"
    );
}
