//! WSS end-to-end MCP OAuth **refresh** slice (PROTOCOL §5.22.1/§5.22.2):
//! drives the production WebSocket transport (TLS + fingerprint pinning +
//! bearer auth) to prove the daemon-side token refresh on the
//! `mcp.testConnection` path. Stores an EXPIRED bag via `mcp.oauth.set` whose
//! `token_endpoint` points at the mock token fixture, saves a matching-origin
//! server config, then calls `mcp.testConnection` with `serverName` and
//! asserts (a) the token endpoint received exactly one RFC 6749 §6 refresh
//! grant, (b) the outbound probe carried the REFRESHED bearer token, (c) the
//! persisted bag was rewritten — a second probe reuses the refreshed token
//! (hit count stays 1; a second refresh would mint `refreshed-token-2`).
//! Companion case: an expired bag WITHOUT refresh metadata falls back to the
//! stale token — no refresh attempted, no error. Gated on `node` + the mock
//! fixture; skips cleanly otherwise. No external network: both fixtures live
//! on 127.0.0.1.

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, BufReader, Lines};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

/// Fixed 64-hex token, adopted by the daemon via the `INTENTD_AUTH_TOKEN` seam.
const TOKEN: &str = "cececececececececececececececececececececececececececececececece";
/// The stale access token stored in the expired bag; must never be sent when
/// a refresh succeeds, and must be sent as-is when no refresh metadata exists.
const STALE_TOKEN: &str = "stale-access-token-EXPIRED";
/// The refresh token stored in the expired bag; must appear in the grant POST.
const REFRESH_TOKEN: &str = "refresh-token-abc123";

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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-oar-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let secrets_file = data_dir.join("secrets.json");
    common::enable_ws_api(data_dir);
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

/// Send one JSON-RPC frame and return the `result` whose id matches; any
/// out-of-band notifications (`events.event`) are ignored. Asserts the §1
/// response envelope (jsonrpc/id, no error).
async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(common::rpc_read_timeout(), ws.next())
            .await
            .expect("wss rpc timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["id"] == json!(id) {
                    assert_eq!(v["jsonrpc"], json!("2.0"), "envelope jsonrpc");
                    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
                    return v["result"].clone();
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

fn node_available() -> bool {
    Command::new("node")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// A spawned fixture process plus its buffered stdout line reader (the
/// fixture logs `AUTH=`/`HIT=` observability lines after the `PORT=` line).
struct Fixture {
    _child: tokio::process::Child,
    lines: Lines<BufReader<tokio::process::ChildStdout>>,
    base_url: String,
}

/// Spawn the mock fixture with `args` and read its `PORT=<n>` announcement.
async fn spawn_fixture(script: &str, args: &[&str]) -> Fixture {
    let mut child = tokio::process::Command::new("node")
        .arg(script)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn mock fixture");
    let stdout = child.stdout.take().expect("fixture stdout");
    let mut lines = BufReader::new(stdout).lines();
    let line = timeout(Duration::from_secs(10), lines.next_line())
        .await
        .expect("timed out waiting for fixture PORT line")
        .expect("read fixture stdout")
        .expect("fixture exited before announcing PORT");
    let port = line
        .strip_prefix("PORT=")
        .expect("fixture PORT line")
        .trim()
        .to_string();
    Fixture {
        _child: child,
        lines,
        base_url: format!("http://127.0.0.1:{port}"),
    }
}

impl Fixture {
    /// Read the next observability line off the fixture's stdout.
    async fn next_line(&mut self) -> String {
        timeout(Duration::from_secs(10), self.lines.next_line())
            .await
            .expect("timed out waiting for fixture output line")
            .expect("read fixture stdout")
            .expect("fixture exited unexpectedly")
    }
}

/// Epoch seconds `delta` in the past.
fn epoch_secs_ago(delta: u64) -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_secs()
        .saturating_sub(delta)
}

/// Boot a daemon + WSS connection; returns the daemon guard and the socket.
async fn boot_daemon() -> (
    Daemon,
    WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>,
) {
    let data_dir = temp_data_dir();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
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
    let ws = connect_ws(port, client_config(&fingerprint)).await;
    (daemon, ws)
}

/// The fixture script path, or `None` when the e2e must be skipped.
fn fixture_script() -> Option<&'static str> {
    if !node_available() {
        eprintln!("skipping mcp oauth refresh WSS E2E: node not on PATH");
        return None;
    }
    let script = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/mock-mcp-server.mjs"
    );
    if !PathBuf::from(script).exists() {
        eprintln!("skipping mcp oauth refresh WSS E2E: fixture not found at {script}");
        return None;
    }
    Some(script)
}

/// Save an http server config pointed at `url` and return its generated id.
async fn create_server<S>(ws: &mut WebSocketStream<S>, id: i64, url: &str) -> String
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let created = wss_rpc(
        ws,
        id,
        "mcp.servers.create",
        json!({ "config": {
            "name": "Mock HTTP",
            "transport": "http",
            "url": url,
            "enabled": false,
        } }),
    )
    .await;
    created["server"]["id"].as_str().expect("id").to_string()
}

/// Expired bag + refresh metadata: `mcp.testConnection` refreshes the token
/// via the mock token endpoint, sends the refreshed bearer, and persists the
/// rewritten bag (second probe reuses it — the token endpoint stays at 1 hit).
#[tokio::test]
async fn test_connection_refreshes_expired_bag_and_persists_it() {
    let Some(script) = fixture_script() else {
        return;
    };
    let mut mcp = spawn_fixture(script, &["--http", "--log-auth"]).await;
    let mut token_ep = spawn_fixture(script, &["--token"]).await;

    let (_daemon, mut ws) = boot_daemon().await;
    let server_id = create_server(&mut ws, 1, &mcp.base_url).await;

    // Store an EXPIRED bag carrying full RFC 6749 §6 refresh metadata whose
    // token_endpoint points at the mock token fixture. Response is
    // presence-only (the bag never crosses the wire).
    let set = wss_rpc(
        &mut ws,
        2,
        "mcp.oauth.set",
        json!({ "serverId": server_id, "tokenBag": {
            "access_token": STALE_TOKEN,
            "token_type": "Bearer",
            "expires_at": epoch_secs_ago(600),
            "refresh_token": REFRESH_TOKEN,
            "token_endpoint": format!("{}/token", token_ep.base_url),
            "client_id": "client-abc",
        } }),
    )
    .await;
    assert_eq!(set["serverId"], json!(server_id));
    assert!(
        !set.to_string().contains(STALE_TOKEN),
        "bag leaked in mcp.oauth.set response: {set}"
    );

    // Probe #1: expired bag → refresh grant POST → refreshed bearer outbound.
    let probe = wss_rpc(
        &mut ws,
        3,
        "mcp.testConnection",
        json!({ "url": mcp.base_url, "serverName": server_id }),
    )
    .await;
    assert_eq!(probe["status"], json!("connected"), "probe #1: {probe}");
    assert_eq!(probe["statusCode"], json!(200));

    // (a) The token endpoint received exactly one refresh grant with the
    // stored refresh_token and client_id, form-encoded.
    let hit = token_ep.next_line().await;
    let (hit_no, grant_body) = hit
        .strip_prefix("HIT=")
        .and_then(|rest| rest.split_once(" BODY="))
        .expect("token fixture HIT line");
    assert_eq!(hit_no, "1", "first refresh grant: {hit}");
    assert!(
        grant_body.contains("grant_type=refresh_token")
            && grant_body.contains(&format!("refresh_token={REFRESH_TOKEN}"))
            && grant_body.contains("client_id=client-abc"),
        "refresh grant body: {grant_body}"
    );

    // (b) The outbound initialize POST carried the REFRESHED bearer token.
    let auth = mcp.next_line().await;
    assert_eq!(auth, "AUTH=Bearer refreshed-token-1", "probe #1 header");

    // (c) Persisted-bag rewrite: probe #2 short-circuits on the fresh
    // expires_at and reuses the refreshed token. A second refresh would have
    // minted `refreshed-token-2` (and a second HIT line); a stale bag would
    // resend the stale token.
    let probe = wss_rpc(
        &mut ws,
        4,
        "mcp.testConnection",
        json!({ "url": mcp.base_url, "serverName": server_id }),
    )
    .await;
    assert_eq!(probe["status"], json!("connected"), "probe #2: {probe}");
    let auth = mcp.next_line().await;
    assert_eq!(auth, "AUTH=Bearer refreshed-token-1", "probe #2 header");
}

/// Expired bag WITHOUT refresh metadata: no refresh attempted, no error — the
/// stale stored token is sent as-is (fail-soft §5.22.1 contract).
#[tokio::test]
async fn test_connection_expired_bag_without_refresh_metadata_falls_back() {
    let Some(script) = fixture_script() else {
        return;
    };
    let mut mcp = spawn_fixture(script, &["--http", "--log-auth"]).await;

    let (_daemon, mut ws) = boot_daemon().await;
    let server_id = create_server(&mut ws, 1, &mcp.base_url).await;

    // Expired bag with NO refresh metadata (no refresh_token/token_endpoint).
    wss_rpc(
        &mut ws,
        2,
        "mcp.oauth.set",
        json!({ "serverId": server_id, "tokenBag": {
            "access_token": STALE_TOKEN,
            "token_type": "Bearer",
            "expires_at": epoch_secs_ago(600),
        } }),
    )
    .await;

    // The probe succeeds (fail-soft: never an RPC error) and carries the
    // stale token unchanged.
    let probe = wss_rpc(
        &mut ws,
        3,
        "mcp.testConnection",
        json!({ "url": mcp.base_url, "serverName": server_id }),
    )
    .await;
    assert_eq!(
        probe["status"],
        json!("connected"),
        "fallback probe: {probe}"
    );
    assert_eq!(probe["statusCode"], json!(200));
    let auth = mcp.next_line().await;
    assert_eq!(auth, format!("AUTH=Bearer {STALE_TOKEN}"), "stale fallback");
}
