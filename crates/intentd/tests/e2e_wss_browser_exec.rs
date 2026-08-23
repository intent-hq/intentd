//! WSS end-to-end for the `browser.exec` client-callable trigger + FE-served
//! reverse RPC (PROTOCOL §5.14, §12.4). Drives the real WSS transport against a
//! live `intentd serve` (WSS listener enabled via config): the client sends `browser.exec`, plays
//! the role of the FE by replying to the daemon-initiated reverse RPC, and
//! asserts the shaped response the caller sees.
//!
//! Covers the wire contract every FE binding (WSAPI-6) will depend on:
//!   * envelope validation (missing / empty `actions` ⇒ `-32602` with no
//!     reverse hop),
//!   * single-action reduction (result envelope for a one-action batch),
//!   * multi-action reduction (`results[]` for a multi-action batch),
//!   * FE failure-envelope passthrough (`-32603` carrying the FE's context).

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

const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-browser-{}", &id[..8]));
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

/// Read the next `Message::Text` frame off the socket, answering pings inline.
async fn read_text<S>(ws: &mut WebSocketStream<S>) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let next = timeout(Duration::from_secs(15), ws.next())
            .await
            .expect("wss read timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                return serde_json::from_str(&text).expect("json frame");
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// Drive one `browser.exec` round-trip: send the caller frame, play the FE by
/// replying to the daemon-initiated reverse RPC with `fe_result`, and return
/// the caller-side response frame + the params the daemon forwarded.
async fn drive_browser_exec<S>(
    ws: &mut WebSocketStream<S>,
    id: i64,
    params: Value,
    fe_result: Value,
) -> (Value, Value)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": "browser.exec", "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .unwrap();

    // Read frames until we have both the reverse-RPC request (from the daemon)
    // and the caller-side response (matching our id). Frames can arrive in
    // either order.
    let mut caller_response: Option<Value> = None;
    let mut forwarded_params: Option<Value> = None;
    while caller_response.is_none() || forwarded_params.is_none() {
        let v = read_text(ws).await;
        if v.get("method").and_then(Value::as_str) == Some("browser.exec") {
            let rev_id = v["id"].as_str().expect("rev id is a string").to_string();
            assert!(rev_id.starts_with("rev-"), "reverse id uses rev- prefix");
            forwarded_params = Some(v["params"].clone());
            let mut reply = json!({ "jsonrpc": "2.0", "id": rev_id });
            let obj = reply.as_object_mut().unwrap();
            for (k, val) in fe_result.as_object().unwrap() {
                obj.insert(k.clone(), val.clone());
            }
            ws.send(Message::Text(reply.to_string().into()))
                .await
                .unwrap();
        } else if v["id"] == json!(id) {
            caller_response = Some(v);
        }
    }
    (caller_response.unwrap(), forwarded_params.unwrap())
}

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
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    (daemon, port, client_config(&fingerprint))
}

#[tokio::test]
async fn browser_exec_single_action_over_wss() {
    let (_daemon, port, cfg) = boot().await;
    let mut ws = connect_ws(port, cfg).await;

    let fe = json!({
        "result": { "success": true, "results": [
            { "action": "listTabs", "success": true, "result": [{ "id": "tab-1" }] }
        ]}
    });
    let params = json!({
        "actions": [{ "action": "listTabs" }],
        "tabId": "tab-1",
        "agentId": "agent-1",
        "workspaceId": "ws-1",
    });
    let (resp, forwarded) = drive_browser_exec(&mut ws, 10, params, fe).await;

    assert!(resp.get("error").is_none(), "no error: {resp}");
    assert_eq!(forwarded["actions"][0]["action"], "listTabs");
    assert_eq!(forwarded["tabId"], "tab-1");
    assert_eq!(forwarded["agentId"], "agent-1");
    assert_eq!(forwarded["workspaceId"], "ws-1");
    let result = &resp["result"];
    assert_eq!(result["action"], "listTabs");
    assert_eq!(result["result"][0]["id"], "tab-1");
}

#[tokio::test]
async fn browser_exec_multi_action_over_wss() {
    let (_daemon, port, cfg) = boot().await;
    let mut ws = connect_ws(port, cfg).await;

    let fe = json!({
        "result": { "success": true, "results": [
            { "action": "listTabs", "success": true, "result": [] },
            { "action": "screenshot", "success": true, "result": { "base64": "..." } }
        ]}
    });
    let params = json!({
        "actions": [{ "action": "listTabs" }, { "action": "screenshot" }]
    });
    let (resp, _forwarded) = drive_browser_exec(&mut ws, 11, params, fe).await;

    let arr = resp["result"]["results"]
        .as_array()
        .expect("multi-action ⇒ results[]");
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[1]["action"], "screenshot");
}

#[tokio::test]
async fn browser_exec_missing_actions_is_invalid_params() {
    let (_daemon, port, cfg) = boot().await;
    let mut ws = connect_ws(port, cfg).await;

    let frame = json!({ "jsonrpc": "2.0", "id": 20, "method": "browser.exec", "params": {} });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .unwrap();
    // Validation short-circuits before the reverse hop; the only frame we get
    // back is the caller-side error.
    let v = read_text(&mut ws).await;
    assert_eq!(v["id"], json!(20));
    assert_eq!(v["error"]["code"], -32602, "missing actions ⇒ -32602: {v}");
    assert!(v["error"]["message"].as_str().unwrap().contains("actions"));
}

#[tokio::test]
async fn browser_exec_empty_actions_is_invalid_params() {
    let (_daemon, port, cfg) = boot().await;
    let mut ws = connect_ws(port, cfg).await;

    let frame = json!({
        "jsonrpc": "2.0", "id": 21, "method": "browser.exec",
        "params": { "actions": [] }
    });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .unwrap();
    let v = read_text(&mut ws).await;
    assert_eq!(v["id"], json!(21));
    assert_eq!(v["error"]["code"], -32602, "empty actions ⇒ -32602: {v}");
}

#[tokio::test]
async fn browser_exec_fe_failure_surfaces_as_internal_error() {
    let (_daemon, port, cfg) = boot().await;
    let mut ws = connect_ws(port, cfg).await;

    // FE responds success at the JSON-RPC layer but reports a failure envelope
    // inside the result — daemon reshapes it into `-32603` with FE context.
    let fe = json!({
        "result": { "success": false, "error": "no tab focused", "results": [] }
    });
    let params = json!({ "actions": [{ "action": "screenshot" }] });
    let (resp, _forwarded) = drive_browser_exec(&mut ws, 30, params, fe).await;

    let err = &resp["error"];
    assert_eq!(err["code"], -32603, "FE failure ⇒ -32603: {resp}");
    assert!(err["message"].as_str().unwrap().contains("no tab focused"));
}

#[tokio::test]
async fn browser_exec_fe_json_rpc_error_surfaces_as_internal_error() {
    let (_daemon, port, cfg) = boot().await;
    let mut ws = connect_ws(port, cfg).await;

    // FE responds with a JSON-RPC error object at the reverse layer.
    let fe = json!({
        "error": { "code": -32603, "message": "CDP not attached" }
    });
    let params = json!({ "actions": [{ "action": "screenshot" }] });
    let (resp, _forwarded) = drive_browser_exec(&mut ws, 31, params, fe).await;

    let err = &resp["error"];
    assert_eq!(err["code"], -32603);
    assert!(err["message"]
        .as_str()
        .unwrap()
        .contains("CDP not attached"));
}
