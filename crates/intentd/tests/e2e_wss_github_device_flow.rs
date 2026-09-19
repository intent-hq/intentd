//! WSS end-to-end for the GitHub device-flow auth surface (PROTOCOL §5.27):
//! `github.connect` → background poll → `github:auth-changed` event →
//! `github.authStatus` / `github.cancelAuth` / `github.revoke`.
//!
//! Boots a real `intentd serve` (WSS listener enabled via config) whose device flow is pointed at
//! a local mock of GitHub's `/login/device/code` + `/login/oauth/access_token`
//! endpoints (via the `INTENTD_GITHUB_LOGIN_BASE_URI` seam), then drives the
//! full connect → poll → authorized → revoke path over a pinned-TLS WebSocket.
//! Hermetic: no live network, secrets land in a temp `INTENTD_SECRETS_FILE`.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::Path;
use std::process::{Child, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

/// The user code the mock hands out and the token it mints on authorize.
const USER_CODE: &str = "WXYZ-4321";
const ACCESS_TOKEN: &str = "gho_e2e_device_flow_token";

struct Daemon {
    child: Child,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn temp_data_dir() -> tempfile::TempDir {
    common::test_tempdir_in("/tmp", "itd-wss-ghdf-")
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    if listen != "uds" {
        common::enable_ws_api(data_dir);
    }
    let mut cmd = common::serve_command();
    cmd.env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        // Reduce token-resolution noise: strip env PATs. The `gh` CLI
        // fallback can still resolve on a developer machine, which is why the
        // tests never assert on `isConfigured` — only on device-flow state
        // and the daemon's own secrets file.
        .env_remove("GITHUB_TOKEN")
        .env_remove("GH_TOKEN")
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

/// One WSS JSON-RPC round-trip returning the full envelope (so callers can
/// assert on `result` OR `error`). Out-of-band notifications are skipped.
async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
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

/// Pump the subscriber connection until a `github:auth-changed` event with the
/// wanted status arrives (bounded).
async fn await_auth_changed<S>(ws: &mut WebSocketStream<S>, status: &str, secs: u64) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .unwrap_or_else(|| panic!("timed out waiting for github:auth-changed {status}"));
        let next = timeout(remaining, ws.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for github:auth-changed {status}"));
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == json!("events.event")
                    && v["params"]["event"]["type"] == json!("github:auth-changed")
                    && v["params"]["event"]["data"]["status"] == json!(status)
                {
                    return v["params"]["event"].clone();
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

// ---------------------------------------------------------------------------
// Mock GitHub host: plain-HTTP `/login/device/code` +
// `/login/oauth/access_token` (login side — answers `authorization_pending`
// until `authorize` is flipped, then answers every subsequent token poll with
// the access token; the daemon's poll loop stops after the first Authorized),
// plus the API-side `GET /user` / `POST /gists` / `GET /gists/{id}` /
// `DELETE /gists/{id}` the gist identity proof needs (`GET /user` and
// `GET /gists/{id}` report the `gist` scope in `X-OAuth-Scopes` while
// `gist_scope` is set; the minted gist reads back as a proof gist, the
// `OTHER_GIST_ID` gist as an unrelated one-file gist).
// ---------------------------------------------------------------------------

/// The gist id the mock mints for every `POST /gists`.
const GIST_ID: &str = "e2e0000000000000000000000000abcd";

/// An unrelated gist of the same account (not a proof gist): the mock would
/// happily `DELETE` it, so the daemon's read-back guard is what protects it.
const OTHER_GIST_ID: &str = "e2e000000000000000000000000other";

struct MockGithub {
    base_uri: String,
    authorize: Arc<AtomicBool>,
    gist_scope: Arc<AtomicBool>,
}

async fn spawn_mock_github() -> MockGithub {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind mock github");
    let port = listener.local_addr().expect("mock addr").port();
    let authorize = Arc::new(AtomicBool::new(false));
    let gist_scope = Arc::new(AtomicBool::new(true));
    let flag = authorize.clone();
    let scope = gist_scope.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let flag = flag.clone();
            let scope = scope.clone();
            tokio::spawn(async move {
                let _ = serve_conn(stream, flag, scope).await;
            });
        }
    });
    MockGithub {
        base_uri: format!("http://127.0.0.1:{port}"),
        authorize,
        gist_scope,
    }
}

/// Minimal HTTP/1.1 handler for the mocked endpoints. Reads one request
/// (headers + content-length body), answers, and closes.
async fn serve_conn(
    mut stream: TcpStream,
    authorize: Arc<AtomicBool>,
    gist_scope: Arc<AtomicBool>,
) -> std::io::Result<()> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
    let (head_end, body_start) = loop {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break (pos, pos + 4);
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let content_length = head
        .lines()
        .find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.trim()
                .eq_ignore_ascii_case("content-length")
                .then(|| v.trim().parse::<usize>().ok())?
        })
        .unwrap_or(0);
    while buf.len() < body_start + content_length {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
    }

    let mut request_line = head.split_whitespace();
    let method = request_line.next().unwrap_or_default();
    let path = request_line.next().unwrap_or_default();
    let mut status = 200;
    let mut extra_headers = String::new();
    let body = if path.starts_with("/login/device/code") {
        json!({
            "device_code": "e2e-device-code-opaque",
            "user_code": USER_CODE,
            "verification_uri": "https://github.com/login/device",
            "expires_in": 900,
            "interval": 1,
        })
    } else if path.starts_with("/login/oauth/access_token") {
        if authorize.load(Ordering::SeqCst) {
            json!({
                "access_token": ACCESS_TOKEN,
                "token_type": "bearer",
                "scope": "repo,read:org,workflow,gist",
            })
        } else {
            json!({ "error": "authorization_pending" })
        }
    } else if method == "GET" && path == "/user" {
        let scopes = if gist_scope.load(Ordering::SeqCst) {
            "repo, read:org, workflow, gist"
        } else {
            "repo, read:org, workflow"
        };
        extra_headers = format!("X-OAuth-Scopes: {scopes}\r\n");
        json!({
            "login": "octocat",
            "id": 1,
            "avatar_url": "https://avatars.githubusercontent.com/u/1",
            "html_url": "https://github.com/octocat",
        })
    } else if method == "POST" && path == "/gists" {
        status = 201;
        json!({ "id": GIST_ID, "public": false })
    } else if method == "GET" && path == format!("/gists/{GIST_ID}") {
        let scopes = if gist_scope.load(Ordering::SeqCst) {
            "repo, read:org, workflow, gist"
        } else {
            "repo, read:org, workflow"
        };
        extra_headers = format!("X-OAuth-Scopes: {scopes}\r\n");
        json!({
            "id": GIST_ID,
            "public": false,
            "files": { "intent-join-proof.txt": { "filename": "intent-join-proof.txt" } },
        })
    } else if method == "GET" && path == format!("/gists/{OTHER_GIST_ID}") {
        extra_headers = "X-OAuth-Scopes: repo, read:org, workflow, gist\r\n".to_string();
        json!({
            "id": OTHER_GIST_ID,
            "public": false,
            "files": { "notes.md": { "filename": "notes.md" } },
        })
    } else if method == "DELETE"
        && (path == format!("/gists/{GIST_ID}") || path == format!("/gists/{OTHER_GIST_ID}"))
    {
        status = 204;
        Value::Null
    } else if path.starts_with("/gists/") {
        status = 404;
        json!({ "message": "Not Found" })
    } else {
        status = 404;
        json!({ "error": "not_found" })
    };
    let payload = if body.is_null() {
        String::new()
    } else {
        body.to_string()
    };
    let response = format!(
        "HTTP/1.1 {status} Status\r\ncontent-type: application/json\r\ncontent-length: {}\r\n{extra_headers}connection: close\r\n\r\n{}",
        payload.len(),
        payload
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

/// Full device-flow lifecycle over WSS: connect returns the mock's codes and
/// is idempotent while pending → authStatus surfaces the pending flow → the
/// mock authorizes → the daemon's background poll persists the token and
/// emits `github:auth-changed { status: "authorized" }` → the secrets file
/// holds the token → revoke deletes it and emits `{ status: "revoked" }` →
/// cancelAuth with nothing in flight is an idempotent no-op.
#[tokio::test]
async fn github_device_flow_full_lifecycle_over_wss() {
    let mock = spawn_mock_github().await;

    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let secrets_file = data_dir.join("secrets.json");
    let secrets_s = secrets_file.to_string_lossy().to_string();
    let env: [(&str, &str); 3] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_SECRETS_FILE", &secrets_s),
        ("INTENTD_GITHUB_LOGIN_BASE_URI", &mock.base_uri),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon { child };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    // SUBSCRIBER conn — global subscription (github:auth-changed carries no
    // workspace id, like settings:changed) BEFORE the flow starts.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let ack = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["github:auth-changed"] }),
    )
    .await;
    assert!(ack.get("error").is_none(), "subscribe failed: {ack}");

    let mut rpc = connect_ws(port, cfg.clone()).await;

    // 1. connect → the mock's codes (§5.27 request/response shape).
    let v = wss_rpc(&mut rpc, 10, "github.connect", json!({})).await;
    assert!(v.get("error").is_none(), "connect errored: {v}");
    let r = &v["result"];
    assert_eq!(r["ok"], json!(true));
    assert_eq!(r["userCode"], json!(USER_CODE));
    assert_eq!(
        r["verificationUri"],
        json!("https://github.com/login/device")
    );
    assert_eq!(r["interval"], json!(1));
    assert!(r["expiresIn"].as_u64().expect("expiresIn") > 0);
    // 🔒 Never the device code or a token on the wire.
    assert!(r.get("deviceCode").is_none());
    assert!(r.get("accessToken").is_none());

    // 2. connect again while pending → the SAME codes (idempotent).
    let v = wss_rpc(&mut rpc, 11, "github.connect", json!({})).await;
    assert_eq!(v["result"]["userCode"], json!(USER_CODE));

    // 3. authStatus while pending → deviceFlow.status == "pending" and the
    //    verification uri doubles as oauthUrl for existing FE consumers.
    //    (`isConfigured` is NOT asserted: the resolution chain can fall back
    //    to a developer machine's authenticated `gh` CLI.)
    let v = wss_rpc(&mut rpc, 12, "github.authStatus", json!({})).await;
    let r = &v["result"];
    assert!(r["isConfigured"].is_boolean(), "isConfigured present: {r}");
    assert_eq!(r["deviceFlow"]["status"], json!("pending"));
    assert_eq!(r["deviceFlow"]["userCode"], json!(USER_CODE));
    assert_eq!(r["oauthUrl"], json!("https://github.com/login/device"));

    // 4. The user authorizes on (mock) github.com; the daemon's background
    //    poll picks it up and pushes github:auth-changed { authorized }.
    mock.authorize.store(true, Ordering::SeqCst);
    let ev = await_auth_changed(&mut sub, "authorized", 30).await;
    assert_eq!(ev["data"]["status"], json!("authorized"));

    // 5. The engine persisted the token into the daemon's secrets file
    //    (server-side only — asserted on disk, never over the wire).
    let secrets = std::fs::read_to_string(&secrets_file).expect("secrets file exists");
    let secrets: Value = serde_json::from_str(&secrets).expect("secrets json");
    assert_eq!(
        secrets["sourceControl.github.token"],
        json!(ACCESS_TOKEN),
        "token persisted under the resolution-chain slot"
    );

    // 6. The authorized transition cleared the flow slot: a cancel now has
    //    nothing to cancel. (`github.authStatus` is deliberately NOT called
    //    while the token is stored — its `GET /user` probe goes to the real
    //    api.github.com, which would make this test network-dependent.)
    let v = wss_rpc(&mut rpc, 13, "github.cancelAuth", json!({})).await;
    assert_eq!(v["result"]["ok"], json!(true));
    assert_eq!(v["result"]["cancelled"], json!(false));

    // 7. revoke → token deleted from the secrets file + revoked event.
    let v = wss_rpc(&mut rpc, 14, "github.revoke", json!({})).await;
    assert_eq!(v["result"]["ok"], json!(true));
    let ev = await_auth_changed(&mut sub, "revoked", 15).await;
    assert_eq!(ev["data"]["status"], json!("revoked"));
    let secrets = std::fs::read_to_string(&secrets_file).expect("secrets file exists");
    let secrets: Value = serde_json::from_str(&secrets).expect("secrets json");
    assert!(
        secrets.get("sourceControl.github.token").is_none(),
        "revoke removed the stored token: {secrets}"
    );

    // 8. cancelAuth with nothing in flight → idempotent no-op.
    let v = wss_rpc(&mut rpc, 15, "github.cancelAuth", json!({})).await;
    assert_eq!(v["result"]["ok"], json!(true));
    assert_eq!(v["result"]["cancelled"], json!(false));
}

/// Cancelling a pending flow stops the background poll: `cancelAuth` reports
/// `cancelled: true`, authStatus drops back to `deviceFlow: null`, and a
/// LATER authorize on the mock must NOT mint a token (the poll task is gone).
#[tokio::test]
async fn github_cancel_auth_stops_the_background_poll_over_wss() {
    let mock = spawn_mock_github().await;

    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let secrets_file = data_dir.join("secrets.json");
    let secrets_s = secrets_file.to_string_lossy().to_string();
    let env: [(&str, &str); 3] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_SECRETS_FILE", &secrets_s),
        ("INTENTD_GITHUB_LOGIN_BASE_URI", &mock.base_uri),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon { child };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);
    let mut rpc = connect_ws(port, cfg.clone()).await;

    let v = wss_rpc(&mut rpc, 10, "github.connect", json!({})).await;
    assert_eq!(v["result"]["ok"], json!(true));

    let v = wss_rpc(&mut rpc, 11, "github.cancelAuth", json!({})).await;
    assert_eq!(v["result"]["ok"], json!(true));
    assert_eq!(v["result"]["cancelled"], json!(true));

    let v = wss_rpc(&mut rpc, 12, "github.authStatus", json!({})).await;
    assert_eq!(v["result"]["deviceFlow"], Value::Null);

    // Authorize AFTER the cancel: the aborted poll task must never mint the
    // token. Give a would-be zombie poller ample time (interval is 1s).
    mock.authorize.store(true, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(3)).await;
    let on_disk = std::fs::read_to_string(&secrets_file).unwrap_or_default();
    assert!(
        !on_disk.contains(ACCESS_TOKEN),
        "cancelled flow must not persist a token"
    );
}

/// Gist identity proof over WSS (`github.identityProof.create` /
/// `github.identityProof.delete`): with the device-flow token pre-seeded in
/// the secrets file and the API host pointed at the mock, create returns the
/// mock's `{ gistId, login }`, delete is idempotent (`{ ok: true }` for a
/// live AND an unknown gist), a token that lacks the `gist` scope is refused
/// with `github-scope-missing` on create and delete alike (on delete even for
/// an unknown gist — the scope decides before the gist's 404), a gist that is
/// not a proof gist is refused with `-32602` and left alone, missing params
/// are `-32602`, and once the token is revoked create is refused with
/// `github-not-connected`.
#[tokio::test]
async fn github_identity_proof_create_and_delete_over_wss() {
    let mock = spawn_mock_github().await;

    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let secrets_file = data_dir.join("secrets.json");
    std::fs::write(
        &secrets_file,
        json!({ "sourceControl.github.token": ACCESS_TOKEN }).to_string(),
    )
    .expect("seed secrets file");
    let secrets_s = secrets_file.to_string_lossy().to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_SECRETS_FILE", &secrets_s),
        ("INTENTD_GITHUB_LOGIN_BASE_URI", &mock.base_uri),
        ("INTENTD_GITHUB_API_BASE_URI", &mock.base_uri),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon { child };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);
    let mut rpc = connect_ws(port, cfg.clone()).await;

    // 1. create → the mock's gist id + the token owner's login, nothing else.
    let v = wss_rpc(
        &mut rpc,
        20,
        "github.identityProof.create",
        json!({ "nonce": "n0nce-e2e", "hostLabel": "Host E2E" }),
    )
    .await;
    assert_eq!(v["jsonrpc"], json!("2.0"));
    assert!(v.get("error").is_none(), "create errored: {v}");
    assert_eq!(
        v["result"],
        json!({ "gistId": GIST_ID, "login": "octocat" }),
        "create result is exactly {{ gistId, login }}"
    );
    // 🔒 Never the token on the wire.
    assert!(!v.to_string().contains(ACCESS_TOKEN));

    // 2. delete the live gist → { ok: true }.
    let v = wss_rpc(
        &mut rpc,
        21,
        "github.identityProof.delete",
        json!({ "gistId": GIST_ID }),
    )
    .await;
    assert!(v.get("error").is_none(), "delete errored: {v}");
    assert_eq!(v["result"], json!({ "ok": true }));

    // 3. delete an unknown gist (mock answers 404) → still { ok: true }.
    let v = wss_rpc(
        &mut rpc,
        22,
        "github.identityProof.delete",
        json!({ "gistId": "0000000000000000000000000000dead" }),
    )
    .await;
    assert!(v.get("error").is_none(), "idempotent delete errored: {v}");
    assert_eq!(v["result"], json!({ "ok": true }));

    // 4. missing params → -32602 on both methods (§9).
    let v = wss_rpc(&mut rpc, 23, "github.identityProof.create", json!({})).await;
    assert_eq!(v["error"]["code"], json!(-32602), "create: {v}");
    let v = wss_rpc(&mut rpc, 24, "github.identityProof.delete", json!({})).await;
    assert_eq!(v["error"]["code"], json!(-32602), "delete: {v}");

    // 5. Token without the `gist` scope → bounded `github-scope-missing`
    //    (checked before any gist is written; the mock's POST /gists is never
    //    needed for this branch).
    mock.gist_scope.store(false, Ordering::SeqCst);
    let v = wss_rpc(
        &mut rpc,
        25,
        "github.identityProof.create",
        json!({ "nonce": "n0nce-e2e", "hostLabel": "Host E2E" }),
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32603), "scope-missing: {v}");
    assert_eq!(v["error"]["data"]["code"], json!("github-scope-missing"));
    //    Delete runs the same `GET /user` preflight before reading the gist
    //    or sending any `DELETE`.
    let v = wss_rpc(
        &mut rpc,
        28,
        "github.identityProof.delete",
        json!({ "gistId": GIST_ID }),
    )
    .await;
    assert_eq!(
        v["error"]["code"],
        json!(-32603),
        "delete scope-missing: {v}"
    );
    assert_eq!(v["error"]["data"]["code"], json!("github-scope-missing"));
    //    …and the scope decides before the gist's `404` can: an unknown gist
    //    without the scope is the same refusal, not the `{ ok: true }` a
    //    scoped 404 means (step 3).
    let v = wss_rpc(
        &mut rpc,
        30,
        "github.identityProof.delete",
        json!({ "gistId": "0000000000000000000000000000dead" }),
    )
    .await;
    assert_eq!(
        v["error"]["code"],
        json!(-32603),
        "delete scope-missing on unknown gist: {v}"
    );
    assert_eq!(v["error"]["data"]["code"], json!("github-scope-missing"));
    mock.gist_scope.store(true, Ordering::SeqCst);

    // 5b. A gist of the account that is not a proof gist is refused with
    //     `-32602` — the mock would have answered the `DELETE` with 204, so
    //     the refusal proves the read-back guard fired first.
    let v = wss_rpc(
        &mut rpc,
        29,
        "github.identityProof.delete",
        json!({ "gistId": OTHER_GIST_ID }),
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32602), "not a proof gist: {v}");
    assert!(
        v["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("nothing deleted")),
        "{v}"
    );

    // 6. revoke drops the stored token → create is `github-not-connected`
    //    (the env / `gh` fallbacks never count for the proof).
    let v = wss_rpc(&mut rpc, 26, "github.revoke", json!({})).await;
    assert_eq!(v["result"]["ok"], json!(true));
    let v = wss_rpc(
        &mut rpc,
        27,
        "github.identityProof.create",
        json!({ "nonce": "n0nce-e2e", "hostLabel": "Host E2E" }),
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32603), "not-connected: {v}");
    assert_eq!(v["error"]["data"]["code"], json!("github-not-connected"));
}
