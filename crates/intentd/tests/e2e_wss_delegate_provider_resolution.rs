//! WSS e2e regression for spec Decision D2: `agent.delegate` provider
//! resolution across the real websocket/router path (intentd#910 review).
//!
//! `agent.delegate` has no `provider` param on the wire (PROTOCOL §5.5).
//! `crates/intent-services/src/agent_ops/tests_delegate_provider_resolution.rs`
//! already covers the resolution order at the service-layer seam; this file
//! locks the same behavior through the router + a real WSS connection:
//! - no explicit `model`, no specialist → the configured default
//!   (`providers.active`) is resolved onto the created session, never left
//!   to fall through to the hardcoded default provider (Auggie).
//! - an unavailable configured default fails the RPC with a clear error
//!   naming the configured provider, never silently substituting Auggie.

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
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
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

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-delegprov-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let secrets_file = data_dir.join("secrets.json");
    if listen != "uds" {
        common::enable_ws_api(data_dir);
    }
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
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

/// One WSS JSON-RPC round-trip, returning the raw envelope (caller checks
/// `error`/`result` itself) — used by the unavailable-provider test, which
/// expects an RPC error rather than a result.
async fn wss_rpc_raw<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
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

/// [`wss_rpc_raw`], asserting the call succeeded and returning `result`.
async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let v = wss_rpc_raw(ws, id, method, params).await;
    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
    v["result"].clone()
}

fn gate() -> Option<String> {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping WSS delegate-provider-resolution E2E: node not on PATH");
        return None;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping WSS delegate-provider-resolution E2E: mock script missing at {script}");
        return None;
    }
    Some(script)
}

/// D2 step 2: no explicit `model`, no specialist — `agent.delegate` resolves
/// the configured default (`providers.active`) onto the created session,
/// instead of leaving `provider` unset and falling through to the spawn
/// path's hardcoded default (Auggie).
#[tokio::test]
async fn delegate_resolves_configured_default_provider_over_wss() {
    let Some(script) = gate() else {
        return;
    };

    let data_dir = temp_data_dir();
    let behavior = json!({ "response": "mock response" }).to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
    ];
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

    let mut ws = connect_ws(port, cfg).await;

    let ws_result = wss_rpc(
        &mut ws,
        10,
        "workspace.create",
        json!({ "title": "D2 WSS E2E", "noPrompt": true }),
    )
    .await;
    let ws_id = ws_result["workspace"]["id"].as_str().expect("workspace id");

    wss_rpc(
        &mut ws,
        20,
        "settings.update",
        json!({ "changes": [{ "path": "providers.active", "value": "mock" }] }),
    )
    .await;

    let delegate_result = wss_rpc(
        &mut ws,
        30,
        "agent.delegate",
        json!({
            "workspaceId": ws_id,
            "agentInstructions": "do the thing",
        }),
    )
    .await;
    let agent_id = delegate_result["agentId"].as_str().expect("agentId");
    assert_eq!(
        delegate_result["provider"].as_str(),
        Some("mock"),
        "delegate result surfaces the resolved provider over the wire (PROTOCOL §5.5): {delegate_result}"
    );

    let get_result = wss_rpc(
        &mut ws,
        40,
        "agent.get",
        json!({ "agentId": agent_id, "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(
        get_result["agent"]["provider"].as_str(),
        Some("mock"),
        "configured default provider persisted on the delegated session, not left unset/Auggie: {get_result}"
    );
}

/// D2 step 3 (error path): the configured default (`providers.active`) is
/// unavailable — `agent.delegate` fails the RPC with a clear error naming the
/// configured provider, never silently substituting/spawning the hardcoded
/// default provider (Auggie).
#[tokio::test]
async fn delegate_errors_not_auggie_when_configured_default_unavailable_over_wss() {
    let data_dir = temp_data_dir();
    // Deliberately no MOCK_AGENT_SCRIPT_PATH: "mock" stays gated off/unavailable.
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

    let mut ws = connect_ws(port, cfg).await;

    let ws_result = wss_rpc(
        &mut ws,
        10,
        "workspace.create",
        json!({ "title": "D2 WSS E2E unavailable", "noPrompt": true }),
    )
    .await;
    let ws_id = ws_result["workspace"]["id"].as_str().expect("workspace id");

    wss_rpc(
        &mut ws,
        20,
        "settings.update",
        json!({ "changes": [{ "path": "providers.active", "value": "mock" }] }),
    )
    .await;

    let delegate_resp = wss_rpc_raw(
        &mut ws,
        30,
        "agent.delegate",
        json!({
            "workspaceId": ws_id,
            "agentInstructions": "do the thing",
        }),
    )
    .await;
    let error = delegate_resp.get("error").unwrap_or_else(|| {
        panic!(
            "unavailable configured default must fail, not silently spawn Auggie: {delegate_resp}"
        )
    });
    let message = error["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("mock"),
        "error names the unavailable configured provider: {message}"
    );
    assert!(
        !message.to_ascii_lowercase().contains("auggie"),
        "error must never silently point at the hardcoded default provider: {message}"
    );
}
