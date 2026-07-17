//! WSS e2e: reportToParent delivers parent wake immediately, agent:idle does NOT deliver second wake
//!
//! Regression test for report-time parent wake with idle suppression. A parent delegates one child
//! (immediate, ungrouped), the child calls `ws.agent.reportToParent` mid-turn then finishes.
//!
//! Expected behavior (post-fix):
//! - The parent receives EXACTLY ONE wake containing the report
//! - The wake arrives immediately after reportToParent (before child's agent:idle)
//! - The child's subsequent agent:idle does NOT trigger a second wake
//!
//! Current behavior (pre-fix, should FAIL):
//! - reportToParent only persists the report
//! - The ONLY wake arrives at child's agent:idle time
//! - This means the test will FAIL initially, proving the regression

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
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::{sleep, timeout};
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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-report-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let secrets_file = data_dir.join("secrets.json");
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .arg("--listen")
        .arg(listen)
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_SECRETS_FILE", &secrets_file)
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

async fn seed_workspace_only(data_dir: &Path) -> String {
    let socket = data_dir.join("intentd.sock");
    if !await_uds(&socket).await {
        panic!("seed: UDS not ready in time");
    }
    let mut stream = UnixStream::connect(&socket).await.unwrap();
    let req = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "workspace.create",
        "params": { "options": {} }
    });
    stream.write_all(req.to_string().as_bytes()).await.unwrap();
    stream.write_all(b"\n").await.unwrap();
    let mut buf = Vec::new();
    BufReader::new(&mut stream)
        .read_until(b'\n', &mut buf)
        .await
        .unwrap();
    let resp: Value = serde_json::from_slice(&buf).unwrap();
    resp["result"]["id"].as_str().unwrap().to_string()
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
    if !PathBuf::from(&script).exists() {
        eprintln!("skipping {test}: mock script not found at {script}");
        return None;
    }
    Some(script)
}

/// Regression test: reportToParent delivers parent wake immediately, agent:idle does NOT deliver second wake.
///
/// Expected (post-fix):
/// - Parent receives EXACTLY ONE wake message containing the report text
/// - The wake arrives immediately after reportToParent (parent streams BEFORE child's agent:idle)
/// - Child's agent:idle does NOT trigger a second wake to the parent
///
/// Current behavior (pre-fix, should FAIL):
/// - reportToParent only persists metadata, no wake
/// - The ONLY wake comes at child's agent:idle time
/// - This means parent streaming happens AFTER child idle (test FAILS, proving regression)
#[tokio::test]
async fn report_to_parent_delivers_immediate_wake_idle_suppressed() {
    let Some(script) = gate("WSS reportToParent immediate wake E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let ws_id = seed_workspace_only(&data_dir).await;
    const CHILD_TAG: &str = "IMMEDIATE_WAKE_CHILD";
    const REPORT: &str = "IMMEDIATE_WAKE_REPORT completed the work";
    const PARENT_GO: &str = "IMMEDIATE_WAKE_PARENT_GO";

    let report_js = format!("return await ws.agent.reportToParent({});", json!(REPORT));
    let delegate_js = format!(
        "return await ws.agent.delegate({{ agentInstructions: {}, model: 'mock:default' }});",
        json!(CHILD_TAG),
    );
    let behavior = json!({
        "rules": [
            {
                "ifPromptContains": CHILD_TAG,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": report_js, "summary": "child reportToParent" }
                },
                "response": "child finished after reportToParent",
            },
            {
                "ifPromptContains": "[WORKSPACE EVENTS]",
                "response": "parent acknowledged the wake",
            },
            {
                "ifPromptContains": PARENT_GO,
                "toolCall": {
                    "name": "workspace_api",
                    "arguments": { "code": delegate_js, "summary": "parent delegates child" }
                },
                "response": "parent delegated one immediate child",
            },
        ],
    })
    .to_string();
    let port_s = free_port().to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", &port_s),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon { child, data_dir };
    let socket = _daemon.data_dir.join("intentd.sock");
    if !await_uds(&socket).await {
        panic!("daemon UDS not ready");
    }
    let port: u16 = port_s.parse().unwrap();

    let tls_info = uds_rpc(&socket, 1, "server.getTlsInfo", json!({})).await;
    let fingerprint = tls_info["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

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
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "parent", "model": "mock:default" }),
    )
    .await;
    let parent_id = created["agentId"].as_str().expect("parent id").to_string();

    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": parent_id, "content": PARENT_GO }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage ok: {sent}");

    // Track observable event ordering. The NEW (post-fix) behavior:
    // parent idle (after delegating) → child streams → parent wake stream BEFORE child idle → child idle → parent idle again
    //
    // OLD (pre-fix) behavior:
    // parent idle → child streams → child idle → parent wake stream → parent idle again
    let mut parent_idle_after_delegate = false;
    let mut child_id: Option<String> = None;
    let mut child_first_chunk = false;
    let mut parent_wake_stream_before_child_idle = false; // NEW: should be true post-fix
    let mut child_idle = false;
    let mut parent_wake_ends = 0u32;
    let mut parent_idle_after_wake = false;

    for _ in 0..400 {
        let frame = wss_event(&mut sub, 60).await;
        let ev = &frame["params"]["event"];
        let ev_agent = ev["data"]["agentId"].as_str().unwrap_or_default();
        let ev_type = ev["type"].as_str().unwrap_or_default();

        // Capture child id from first child stream event
        if (ev_type == "agent:stream:chunk" || ev_type == "agent:stream:end")
            && !ev_agent.is_empty()
            && ev_agent != parent_id
        {
            child_id = Some(ev_agent.to_string());
        }

        if ev_agent == parent_id && ev_type == "agent:idle" && !parent_idle_after_delegate {
            parent_idle_after_delegate = true;
            continue;
        }

        if let Some(cid) = child_id.as_deref() {
            if ev_agent == cid && ev_type == "agent:stream:chunk" {
                child_first_chunk = true;
            }
            if ev_agent == cid && ev_type == "agent:idle" {
                child_idle = true;
            }
        }

        // KEY ASSERTION: parent wake stream BEFORE child idle means reportToParent fired the wake immediately
        if ev_agent == parent_id
            && ev_type == "agent:stream:chunk"
            && child_first_chunk
            && !child_idle
        {
            parent_wake_stream_before_child_idle = true;
        }

        if ev_agent == parent_id && ev_type == "agent:stream:end" && child_idle {
            parent_wake_ends += 1;
        }
        if ev_agent == parent_id && ev_type == "agent:idle" && child_idle {
            parent_idle_after_wake = true;
        }
        if parent_idle_after_wake && parent_wake_ends >= 1 {
            break;
        }
    }

    assert!(
        parent_idle_after_delegate,
        "parent went idle after delegating"
    );
    assert!(child_id.is_some(), "child agent id observed");
    assert!(child_idle, "child emitted agent:idle");

    // REGRESSION ASSERTION: parent wake stream MUST fire BEFORE child idle (immediate wake at reportToParent time)
    assert!(
        parent_wake_stream_before_child_idle,
        "reportToParent MUST deliver parent wake immediately — parent should stream BEFORE child idles"
    );

    assert_eq!(
        parent_wake_ends, 1,
        "exactly one wake-turn stream:end on the parent"
    );
    assert!(
        parent_idle_after_wake,
        "parent idled again after the wake turn"
    );

    // Parent transcript carries EXACTLY ONE wake message with the report
    let conv = wss_rpc(
        &mut rpc,
        12,
        "agent.getConversation",
        json!({ "agentId": parent_id }),
    )
    .await;
    let messages = conv["messages"].as_array().expect("messages array");
    let texts: Vec<String> = messages
        .iter()
        .filter_map(|m| {
            serde_json::to_string(&m["contentBlocks"])
                .ok()
                .filter(|t| t.contains("[WORKSPACE EVENTS]"))
        })
        .collect();
    assert_eq!(texts.len(), 1, "exactly one wake message: {conv}");
    let wake = &texts[0];
    assert!(
        wake.contains(&format!("Report: {REPORT}")),
        "wake carries the report text: {wake}"
    );
    assert!(
        !wake.contains("Summary:"),
        "wake prefers report over lastResponseSummary: {wake}"
    );
}
