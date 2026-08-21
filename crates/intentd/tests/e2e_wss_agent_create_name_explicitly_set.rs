//! WSS e2e for `agent.create`'s optional `nameExplicitlySet` param
//! (PROTOCOL §5.5, agent-rename-persistence Bug 2).
//!
//! Boots a real `intentd serve` (WSS listener enabled via config) and drives
//! `agent.create` → `agent.rename { skipIfExplicitlySet: true }` over the real
//! transport:
//! - a name created with `nameExplicitlySet: false` (an FE placeholder) stays
//!   renameable by the guarded self-rename, and the applied rename emits
//!   `agent:renamed`;
//! - a name without the flag keeps today's behavior (explicit → guarded
//!   rename skipped);
//! - a non-boolean `nameExplicitlySet` is rejected with `-32602`.
//!
//! No provider is spawned (no `agent.sendMessage`), so no node gate.

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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-nameexpl-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let secrets_file = data_dir.join("secrets.json");
    common::enable_ws_api(data_dir);
    common::seed_default_provider(data_dir);
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

/// Send one JSON-RPC request and return the FULL response envelope (so error
/// responses can be asserted too).
async fn wss_rpc_env<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
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
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// Like [`wss_rpc_env`] but asserts success and returns just `result`.
async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let v = wss_rpc_env(ws, id, method, params).await;
    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
    v["result"].clone()
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
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// `agent.create` + `nameExplicitlySet` over WSS (PROTOCOL §5.5):
/// 1. `nameExplicitlySet: false` → the guarded `agent.rename` applies (no
///    `skipped`), persists, and emits `agent:renamed`.
/// 2. name without the flag → today's behavior: guarded rename skipped.
/// 3. non-boolean `nameExplicitlySet` → `-32602` error envelope.
#[tokio::test]
async fn agent_create_name_explicitly_set_controls_guarded_rename_over_wss() {
    let data_dir = temp_data_dir();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
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

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let ws_result = wss_rpc(
        &mut rpc,
        10,
        "workspace.create",
        json!({ "title": "nameExplicitlySet WSS E2E", "noPrompt": true }),
    )
    .await;
    let ws_id = ws_result["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // SUBSCRIBER conn — events.subscribe BEFORE the mutations so the
    // `agent:renamed` emission cannot be missed.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        20,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // 1) Placeholder name: `nameExplicitlySet: false` → the guarded
    //    self-rename applies despite the supplied name.
    let created = wss_rpc(
        &mut rpc,
        30,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Placeholder", "nameExplicitlySet": false }),
    )
    .await;
    assert_eq!(created["agent"]["name"], "Placeholder");
    assert_eq!(
        created["agent"]["nameExplicitlySet"],
        json!(false),
        "persisted flag honors the param: {created}"
    );
    let placeholder_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();

    let renamed = wss_rpc(
        &mut rpc,
        31,
        "agent.rename",
        json!({
            "workspaceId": ws_id,
            "agentId": placeholder_id,
            "name": "Self-Chosen",
            "skipIfExplicitlySet": true,
        }),
    )
    .await;
    assert_eq!(renamed["success"], json!(true));
    assert_eq!(renamed["name"], "Self-Chosen");
    assert!(
        renamed.get("skipped").is_none(),
        "guarded rename must apply, not skip: {renamed}"
    );

    // The applied rename emits `agent:renamed` with { agentId, name }.
    let mut saw_renamed = false;
    for _ in 0..40 {
        let frame = wss_event(&mut sub, 15).await;
        let event = &frame["params"]["event"];
        if event["type"] == "agent:renamed" && event["data"]["agentId"] == json!(placeholder_id) {
            assert_eq!(event["data"]["name"], "Self-Chosen", "event data: {frame}");
            saw_renamed = true;
            break;
        }
    }
    assert!(saw_renamed, "agent:renamed delivered over WSS");

    // The rename persisted: agent.get serves the new name with the explicit
    // flag now set (a later user rename wins).
    let got = wss_rpc(
        &mut rpc,
        32,
        "agent.get",
        json!({ "agentId": placeholder_id, "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(got["agent"]["name"], "Self-Chosen");
    assert_eq!(got["agent"]["nameExplicitlySet"], json!(true));

    // 2) Back-compat: a name WITHOUT the flag stays explicit — the guarded
    //    rename is a no-op echoing the existing name with `skipped: true`.
    let created = wss_rpc(
        &mut rpc,
        40,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Explicit" }),
    )
    .await;
    assert_eq!(created["agent"]["nameExplicitlySet"], json!(true));
    let explicit_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let renamed = wss_rpc(
        &mut rpc,
        41,
        "agent.rename",
        json!({
            "workspaceId": ws_id,
            "agentId": explicit_id,
            "name": "Clobber",
            "skipIfExplicitlySet": true,
        }),
    )
    .await;
    assert_eq!(renamed["success"], json!(true));
    assert_eq!(renamed["skipped"], json!(true));
    assert_eq!(renamed["name"], "Explicit");

    // 3) Non-boolean `nameExplicitlySet` → `-32602` (PROTOCOL §9).
    let envl = wss_rpc_env(
        &mut rpc,
        50,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Bad", "nameExplicitlySet": "nope" }),
    )
    .await;
    assert_eq!(envl["jsonrpc"], "2.0");
    assert_eq!(envl["id"], json!(50));
    assert_eq!(envl["error"]["code"], json!(-32602));
    assert_eq!(
        envl["error"]["message"],
        json!("nameExplicitlySet must be a boolean")
    );
}
