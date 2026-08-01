//! WSS e2e tests for settings wire behavior (per AGENTS.md testing gate
//! requirement):
//! - atomic rollback: a failed mixed batch over WSS fully reverts all settings
//!   to their pre-batch values and returns the failing key in the error
//!   response;
//! - retired `model.workspaceOverrides`: `settings.update` over WSS
//!   tolerates-and-ignores the retired path while `settings.get` rejects it.

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

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-atomic-rb-{}", &id[..8]));
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
    timeout(common::rpc_read_timeout(), reader.read_line(&mut buf))
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

async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
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

/// Atomic rollback over WSS: a failed mixed batch fully reverts all settings
/// and returns the failing key in the error response (per AGENTS.md testing gate).
#[tokio::test]
async fn mixed_batch_rollback_over_wss() {
    let data_dir = temp_data_dir();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
    // Start daemon with both UDS and TCP (server.wsApi.enabled=true in config.toml)
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    // Baseline: git.autoCommit=true, server.port=5181 (default)
    // Note: server.wsApi.enabled=true comes from the seeded config.toml, which boot-starts the listener
    let r = uds_rpc(
        &socket,
        1,
        "settings.get",
        json!({"path": "git.autoCommit"}),
    )
    .await;
    assert_eq!(r["result"]["value"], json!(true));
    let r = uds_rpc(&socket, 2, "settings.get", json!({"path": "server.port"})).await;
    // server.port is stored as a JSON number (float); compare numerically
    assert_eq!(r["result"]["value"], json!(5181.0));

    // Get server fingerprint and port from system.status (WSS listener started at boot via config)
    let status = common::await_wss_status(&socket).await;
    let port = status["result"]["port"]
        .as_u64()
        .expect("port should be set at boot") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint should be set")
        .to_string();
    let cfg = client_config(&fingerprint);

    // Connect to WSS to test batch rollback over WSS transport
    let mut ws = connect_ws(port, cfg).await;

    // Mixed batch: git.autoCommit=false, server.port=6000, server.wsApi.enabled=false
    // The third change (wsApi.enabled=false from a TCP connection) will fail due to the guard
    // that prevents self-termination, triggering rollback of all three changes.
    let batch_resp = wss_rpc(
        &mut ws,
        4,
        "settings.update",
        json!({
            "changes": [
                {"path": "git.autoCommit", "value": false},
                {"path": "server.port", "value": 6000},
                {"path": "server.wsApi.enabled", "value": false}
            ]
        }),
    )
    .await;

    // Assert error response with failing key
    assert!(batch_resp.get("error").is_some(), "expected error response");
    let error = &batch_resp["error"];
    let msg = error["message"].as_str().unwrap();
    assert!(
        msg.contains("TCP connection") || msg.contains("self-terminate"),
        "error should mention TCP connection guard"
    );
    assert!(
        msg.contains("server.wsApi.enabled"),
        "error should include the failing key (server.wsApi.enabled)"
    );

    // Verify rollback: all three settings should be back to baseline via UDS
    // (Note: In this test the hook failure was the TCP self-termination guard, so no
    // listener stop occurred. The WSS connection remains open, but we use UDS to verify
    // rollback to avoid any ambiguity.)
    let r = uds_rpc(
        &socket,
        10,
        "settings.get",
        json!({"path": "git.autoCommit"}),
    )
    .await;
    assert_eq!(
        r["result"]["value"],
        json!(true),
        "git.autoCommit should be rolled back to true"
    );
    let r = uds_rpc(&socket, 11, "settings.get", json!({"path": "server.port"})).await;
    // server.port is stored as a JSON number (float); compare numerically
    assert_eq!(
        r["result"]["value"],
        json!(5181.0),
        "server.port should be rolled back to 5181"
    );
    let r = uds_rpc(
        &socket,
        12,
        "settings.get",
        json!({"path": "server.wsApi.enabled"}),
    )
    .await;
    assert_eq!(
        r["result"]["value"],
        json!(true),
        "server.wsApi.enabled should be rolled back to true (seeded file value)"
    );
}

/// Retired `model.workspaceOverrides` over WSS: `settings.update` writes to the
/// retired path are tolerated-and-ignored (no `-32602`, `applied: []`, mixed
/// batches still apply their live entries) and `settings.get` rejects the path
/// as unknown — same wire contract as UDS (`legacy_workspace_overrides_discards_and_strips_on_boot`).
#[tokio::test]
async fn retired_workspace_overrides_over_wss() {
    let data_dir = temp_data_dir();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    let status = common::await_wss_status(&socket).await;
    let port = status["result"]["port"]
        .as_u64()
        .expect("port should be set at boot") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint should be set")
        .to_string();
    let cfg = client_config(&fingerprint);
    let mut ws = connect_ws(port, cfg).await;

    // Old-client write of the retired path alone: tolerated, nothing applied.
    let resp = wss_rpc(
        &mut ws,
        1,
        "settings.update",
        json!({
            "changes": [
                {"path": "model.workspaceOverrides", "value": {"ws-1": "gpt-5"}}
            ]
        }),
    )
    .await;
    assert!(
        resp.get("error").is_none(),
        "retired-path update must not error: {resp}"
    );
    assert_eq!(
        resp["result"]["applied"],
        json!([]),
        "retired path must not be echoed in applied"
    );

    // Mixed batch: the live entry applies, the retired one is skipped.
    let resp = wss_rpc(
        &mut ws,
        2,
        "settings.update",
        json!({
            "changes": [
                {"path": "model.workspaceOverrides", "value": {"ws-1": "gpt-5"}},
                {"path": "model.default", "value": "claude-sonnet-4"}
            ]
        }),
    )
    .await;
    assert!(
        resp.get("error").is_none(),
        "mixed batch with retired path must not error: {resp}"
    );
    let applied = resp["result"]["applied"].as_array().expect("applied array");
    assert_eq!(applied.len(), 1, "only the live entry applies: {resp}");
    assert_eq!(applied[0]["path"], json!("model.default"));

    let resp = wss_rpc(&mut ws, 3, "settings.get", json!({"path": "model.default"})).await;
    assert_eq!(resp["result"]["value"], json!("claude-sonnet-4"));

    // The retired path is gone from the catalog: settings.get rejects it.
    let resp = wss_rpc(
        &mut ws,
        4,
        "settings.get",
        json!({"path": "model.workspaceOverrides"}),
    )
    .await;
    assert_eq!(
        resp["error"]["code"],
        json!(-32602),
        "settings.get on the retired path must reject as unknown: {resp}"
    );
}

/// `workspaceApi.*` over WSS (per AGENTS.md testing gate): the two
/// TOML-backed workspace_api output knobs appear in `settings.list` with
/// their definitions, round-trip through `settings.update`/`settings.reset`,
/// and out-of-range values reject with `-32602`.
#[tokio::test]
async fn workspace_api_settings_round_trip_over_wss() {
    let data_dir = temp_data_dir();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    let status = common::await_wss_status(&socket).await;
    let port = status["result"]["port"]
        .as_u64()
        .expect("port should be set at boot") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint should be set")
        .to_string();
    let cfg = client_config(&fingerprint);
    let mut ws = connect_ws(port, cfg).await;

    // settings.list — both keys advertised with their definitions + defaults.
    let list = wss_rpc(&mut ws, 1, "settings.list", json!({})).await;
    let settings = list["result"]["settings"]
        .as_array()
        .expect("settings array");
    let chars = settings
        .iter()
        .find(|e| e["path"] == "workspaceApi.maxOutputChars")
        .expect("workspaceApi.maxOutputChars missing from settings.list");
    assert_eq!(chars["type"], json!("number"));
    assert_eq!(chars["value"], json!(100000.0));
    assert_eq!(chars["min"], json!(0.0));
    assert_eq!(chars["max"], json!(10000000.0));
    assert_eq!(chars["origin"], json!("default"));
    let toon = settings
        .iter()
        .find(|e| e["path"] == "workspaceApi.toonOutput")
        .expect("workspaceApi.toonOutput missing from settings.list");
    assert_eq!(toon["type"], json!("boolean"));
    assert_eq!(toon["value"], json!(true));
    assert_eq!(toon["origin"], json!("default"));

    // Update both → applied, get reads back with `file` origin.
    let resp = wss_rpc(
        &mut ws,
        2,
        "settings.update",
        json!({ "changes": [
            {"path": "workspaceApi.maxOutputChars", "value": 250000},
            {"path": "workspaceApi.toonOutput", "value": false}
        ] }),
    )
    .await;
    assert!(resp.get("error").is_none(), "update errored: {resp}");
    let applied = resp["result"]["applied"].as_array().expect("applied array");
    assert_eq!(applied.len(), 2, "{resp}");
    let resp = wss_rpc(
        &mut ws,
        3,
        "settings.get",
        json!({"path": "workspaceApi.maxOutputChars"}),
    )
    .await;
    // Registry-read numbers are reported as floats on the wire (see
    // `wire_value`), matching the numeric shape of the catalog defaults.
    assert_eq!(resp["result"]["value"], json!(250000.0));
    assert_eq!(resp["result"]["origin"], json!("file"));
    let resp = wss_rpc(
        &mut ws,
        4,
        "settings.get",
        json!({"path": "workspaceApi.toonOutput"}),
    )
    .await;
    assert_eq!(resp["result"]["value"], json!(false));
    assert_eq!(resp["result"]["origin"], json!("file"));

    // Sub-1000 non-zero / over-max values reject with -32602.
    for bad in [json!(500), json!(20000000)] {
        let resp = wss_rpc(
            &mut ws,
            5,
            "settings.update",
            json!({ "changes": [{"path": "workspaceApi.maxOutputChars", "value": bad}] }),
        )
        .await;
        assert_eq!(resp["error"]["code"], json!(-32602), "{resp}");
    }

    // 0 = unlimited is accepted.
    let resp = wss_rpc(
        &mut ws,
        6,
        "settings.update",
        json!({ "changes": [{"path": "workspaceApi.maxOutputChars", "value": 0}] }),
    )
    .await;
    assert!(resp.get("error").is_none(), "0 must be accepted: {resp}");

    // Reset both back to their defaults.
    let resp = wss_rpc(
        &mut ws,
        7,
        "settings.reset",
        json!({"path": "workspaceApi.maxOutputChars"}),
    )
    .await;
    assert_eq!(resp["result"]["value"], json!(100000.0));
    let resp = wss_rpc(
        &mut ws,
        8,
        "settings.reset",
        json!({"path": "workspaceApi.toonOutput"}),
    )
    .await;
    assert_eq!(resp["result"]["value"], json!(true));
}
