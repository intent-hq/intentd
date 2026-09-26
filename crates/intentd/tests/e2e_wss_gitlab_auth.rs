//! WSS end-to-end for the provider-generic auth surface with `provider:
//! "gitlab"` (PROTOCOL §5.27): `sourceControl.connect` (device grant and PAT)
//! → `sourceControl:auth-changed` → `sourceControl.authStatus` /
//! `sourceControl.getUser` / `sourceControl.cancelAuth` / `sourceControl.revoke`,
//! plus the typed `device-grant-unsupported` / `source-control-unauthorized`
//! errors and the `-32602` parameter refusals.
//!
//! Boots a real `intentd serve` (WSS listener enabled via config) whose GitLab
//! OAuth + API calls are pointed at a local mock of `/oauth/authorize_device`,
//! `/oauth/token` and `/api/v4/user` (via the `INTENTD_GITLAB_API_BASE_URI`
//! seam), then drives the flows over a pinned-TLS WebSocket. Hermetic: no live
//! network, secrets land in a temp `INTENTD_SECRETS_FILE`.

#![cfg(unix)]

mod common;

#[path = "collaboration_identity/github.rs"]
mod collaboration_github;
#[path = "collaboration_identity/gitlab.rs"]
mod collaboration_identity;

use std::net::Ipv4Addr;
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use intentd_test_support::GuardedChild;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixStream};
use tokio::sync::Notify;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

const TOKEN: &str = "cececececececececececececececececececececececececececececececece";

/// The user code the mock hands out, the token pair it mints on authorize,
/// and the personal access token it accepts.
const USER_CODE: &str = "GLAB-0001";
const ACCESS_TOKEN: &str = "glo_e2e_device_grant_token";
const REFRESH_TOKEN: &str = "glr_e2e_device_grant_refresh";
const ROTATED_ACCESS_TOKEN: &str = "glo_e2e_rotated_access_token";
const ROTATED_REFRESH_TOKEN: &str = "glr_e2e_rotated_refresh";
const PAT_TOKEN: &str = "glpat-e2e-valid-personal-token";
const BAD_PAT: &str = "glpat-e2e-rejected-token";
/// A PAT the mock accepts on `/api/v4/user` but refuses snippet creation
/// for (`403 insufficient_scope`: no `api` scope).
const READ_ONLY_PAT: &str = "glpat-e2e-read-only-token";

/// The bound instance: `sourceControl.gitlab.host` defaults to gitlab.com,
/// whose device grant uses the compiled client id, so no settings are needed.
const HOST: &str = "gitlab.com";

struct Daemon {
    child: GuardedChild,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn temp_data_dir() -> tempfile::TempDir {
    common::test_tempdir_in("/tmp", "itd-wss-glauth-")
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> GuardedChild {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(data_dir);
    // The GitHub resolution chain ends at `gh auth token`; point the CLI at an
    // empty config dir so a developer's own `gh auth login` is never borrowed.
    let gh_config_dir = data_dir.join("gh-config");
    std::fs::create_dir_all(&gh_config_dir).expect("mkdir hermetic gh config dir");
    let mut cmd = common::serve_command();
    cmd.env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        // The GitLab resolution chain falls back to `GITLAB_TOKEN`; strip it
        // so `isConfigured` reflects only the daemon's own secrets file.
        .env_remove("GITLAB_TOKEN")
        .env_remove("GITHUB_TOKEN")
        .env_remove("GH_TOKEN")
        .env("GH_CONFIG_DIR", &gh_config_dir)
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    for (k, v) in env {
        cmd.env(k, v);
    }
    GuardedChild::spawn(&mut cmd).expect("spawn intentd serve")
}

async fn await_uds(socket: &Path) -> bool {
    timeout(common::daemon_startup_timeout(), async {
        loop {
            if UnixStream::connect(socket).await.is_ok() {
                return;
            }
            // timing-guard: poll interval
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

type Ws = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

async fn connect_ws(port: u16, cfg: Arc<ClientConfig>) -> Ws {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// One WSS JSON-RPC round-trip returning the full envelope (so callers can
/// assert on `result` OR `error`). Out-of-band notifications are skipped.
/// The JSON-RPC 2.0 response envelope (PROTOCOL §1) is asserted on the
/// matched frame: `jsonrpc == "2.0"`, the echoed `id`, no `method` member,
/// and exactly one of `result` / `error`.
async fn wss_rpc(ws: &mut Ws, id: i64, method: &str, params: Value) -> Value {
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
                    assert_eq!(
                        v["jsonrpc"],
                        json!("2.0"),
                        "{method}: envelope jsonrpc: {v}"
                    );
                    assert!(
                        v.get("method").is_none(),
                        "{method}: a response carries no method member: {v}"
                    );
                    assert!(
                        v.get("result").is_some() ^ v.get("error").is_some(),
                        "{method}: envelope must carry exactly one of result/error: {v}"
                    );
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

/// Pump the subscriber connection until a `sourceControl:auth-changed` event
/// with the wanted status arrives (bounded) and return its `data`.
async fn await_auth_changed(ws: &mut Ws, status: &str, secs: u64) -> Value {
    await_auth_changed_matching(ws, Some(status), secs).await
}

/// Like [`await_auth_changed`] but returns the NEXT `sourceControl:auth-changed`
/// event of any status when `status` is `None` — the way to prove an event
/// was never emitted: the first one observed is the one a later action
/// deliberately triggered.
async fn await_auth_changed_matching(ws: &mut Ws, status: Option<&str>, secs: u64) -> Value {
    let status = status.unwrap_or("<any>");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .unwrap_or_else(|| panic!("timed out waiting for sourceControl:auth-changed {status}"));
        let next = timeout(remaining, ws.next()).await.unwrap_or_else(|_| {
            panic!("timed out waiting for sourceControl:auth-changed {status}")
        });
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == json!("events.event")
                    && v["params"]["event"]["type"] == json!("sourceControl:auth-changed")
                    && (status == "<any>"
                        || v["params"]["event"]["data"]["status"] == json!(status))
                {
                    return v["params"]["event"]["data"].clone();
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

fn read_secrets(path: &Path) -> Value {
    let raw = std::fs::read_to_string(path).unwrap_or_else(|_| "{}".to_string());
    serde_json::from_str(&raw).expect("secrets json")
}

// ---------------------------------------------------------------------------
// Mock GitLab instance: plain-HTTP `/oauth/authorize_device`, `/oauth/token`
// and `/api/v4/user`. The token endpoint answers `authorization_pending` until
// `authorize` is flipped, then mints the access + refresh token pair (a
// 60 s access token with `short_lived`, inside the daemon's refresh leeway);
// a `grant_type=refresh_token` exchange rotates the pair exactly once — like
// a real instance, `REFRESH_TOKEN` is consumed by its first exchange and any
// later one (with it or with the rotated token) is `invalid_grant`. The user
// endpoint accepts the minted / rotated token or `PAT_TOKEN` as bearer and
// answers 401 for anything else (`reject_rotated` revokes the rotated token
// server-side); `user_requests` counts every hit on it and `user_hit` wakes
// once per hit (as the request arrives). With `unsupported` set, the device
// endpoint answers 404 (a GitLab < 17.1 instance).
//
// Two one-shot latches make a token exchange's response arrive late, so a
// test can put the daemon's completion / rotation in flight and act while it
// is: with `hold_authorize` (`hold_refresh`) set, the NEXT successful
// authorize (refresh) response is parked after `authorize_held`
// (`refresh_held`) wakes, until `release_authorize` (`release_refresh`) is
// notified. The latch clears itself, so later exchanges are prompt.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct MockFlags {
    authorize: AtomicBool,
    unsupported: AtomicBool,
    short_lived: AtomicBool,
    reject_rotated: AtomicBool,
    refresh_exchanges: AtomicUsize,
    user_requests: AtomicUsize,
    user_hit: Notify,
    hold_authorize: AtomicBool,
    authorize_held: Notify,
    release_authorize: Notify,
    hold_refresh: AtomicBool,
    refresh_held: Notify,
    release_refresh: Notify,
    hold_poll: AtomicBool,
    poll_held: Notify,
    release_poll: Notify,
    /// Personal snippets by id: `(author bearer, create body as posted)`.
    /// Ids are handed out from 1 in creation order.
    snippets: Mutex<Vec<(u64, String, Value)>>,
    /// When set, snippet reads require a bearer (the instance restricts
    /// anonymous access) — the host-side fallback scenario.
    private_snippets: AtomicBool,
}

/// The mock's `GET /api/v4/snippets/:id` body for a stored snippet.
fn snippet_json(id: u64, posted: &Value) -> Value {
    let files = posted["files"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|f| json!({ "path": f["file_path"], "raw_url": format!("https://gitlab.com/-/snippets/{id}/raw/main/x") }))
        .collect::<Vec<_>>();
    json!({
        "id": id,
        "title": posted["title"],
        "visibility": posted["visibility"],
        "created_at": "2026-09-21T02:00:00.000Z",
        "author": {
            "id": 4242,
            "username": "glab-octocat",
            "name": "GitLab Octocat",
            "avatar_url": "https://gitlab.com/uploads/avatar.png",
        },
        "file_name": files.first().map_or(Value::Null, |f| f["path"].clone()),
        "files": files,
    })
}

struct MockGitlab {
    base_uri: String,
    flags: Arc<MockFlags>,
}

async fn spawn_mock_gitlab() -> MockGitlab {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind mock gitlab");
    let port = listener.local_addr().expect("mock addr").port();
    let flags = Arc::new(MockFlags::default());
    let shared = flags.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let flags = shared.clone();
            tokio::spawn(async move {
                let _ = serve_conn(stream, flags).await;
            });
        }
    });
    MockGitlab {
        base_uri: format!("http://127.0.0.1:{port}"),
        flags,
    }
}

/// Minimal HTTP/1.1 handler for the mock endpoints. Reads one request
/// (headers + content-length body), answers, and closes.
async fn serve_conn(mut stream: TcpStream, flags: Arc<MockFlags>) -> std::io::Result<()> {
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
    let header = |name: &str| -> Option<String> {
        head.lines().find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.trim()
                .eq_ignore_ascii_case(name)
                .then(|| v.trim().to_string())
        })
    };
    let content_length = header("content-length")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    while buf.len() < body_start + content_length {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
    }

    let mut parts = head.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or_default();
    let bearer = header("authorization")
        .and_then(|v| v.strip_prefix("Bearer ").map(str::to_string))
        .unwrap_or_default();
    let form = String::from_utf8_lossy(&buf[body_start..]).to_string();
    let form_field = |name: &str| -> Option<String> {
        form.split('&')
            .find_map(|kv| kv.split_once('=').filter(|(k, _)| *k == name))
            .map(|(_, v)| v.to_string())
    };
    let is_refresh = form_field("grant_type").as_deref() == Some("refresh_token");
    let bearer_ok = bearer == ACCESS_TOKEN
        || bearer == PAT_TOKEN
        || bearer == READ_ONLY_PAT
        || (bearer == ROTATED_ACCESS_TOKEN && !flags.reject_rotated.load(Ordering::SeqCst));
    let route = path.split('?').next().unwrap_or_default();
    if method == "GET" && route == "/api/v4/user" {
        flags.user_requests.fetch_add(1, Ordering::SeqCst);
        flags.user_hit.notify_one();
    }
    // `/api/v4/snippets/:id[/raw]` → `(id, is_raw)`.
    let snippet_route =
        route
            .strip_prefix("/api/v4/snippets/")
            .map(|rest| match rest.strip_suffix("/raw") {
                Some(id) => (id.parse::<u64>().ok(), true),
                None => (rest.parse::<u64>().ok(), false),
            });
    let snippets_readable = bearer_ok || !flags.private_snippets.load(Ordering::SeqCst);
    let (status, body) = match (method, route) {
        ("POST", "/api/v4/snippets") if bearer == READ_ONLY_PAT => (
            403,
            json!({ "error": "insufficient_scope", "error_description": "api" }),
        ),
        ("POST", "/api/v4/snippets") if bearer_ok => {
            let posted: Value = serde_json::from_str(&form).unwrap_or(Value::Null);
            let mut snippets = flags.snippets.lock().unwrap();
            let id = snippets.len() as u64 + 1;
            snippets.push((id, bearer.clone(), posted.clone()));
            (201, snippet_json(id, &posted))
        }
        ("GET", _) if snippet_route.is_some() && !snippets_readable => {
            (401, json!({ "message": "401 Unauthorized" }))
        }
        ("GET", _) if snippet_route.is_some() => {
            let (id, is_raw) = snippet_route.unwrap();
            let snippets = flags.snippets.lock().unwrap();
            match id.and_then(|id| snippets.iter().find(|(sid, _, _)| *sid == id)) {
                Some((id, _, posted)) if is_raw => (200, posted["files"][0]["content"].clone()),
                Some((id, _, posted)) => (200, snippet_json(*id, posted)),
                None => (404, json!({ "message": "404 Snippet Not Found" })),
            }
        }
        ("DELETE", _) if snippet_route.is_some() && bearer_ok => {
            let (id, _) = snippet_route.unwrap();
            let mut snippets = flags.snippets.lock().unwrap();
            match id.and_then(|id| snippets.iter().position(|(sid, _, _)| *sid == id)) {
                Some(pos) => {
                    snippets.remove(pos);
                    (204, Value::Null)
                }
                None => (404, json!({ "message": "404 Snippet Not Found" })),
            }
        }
        ("DELETE", _) if snippet_route.is_some() => (401, json!({ "message": "401 Unauthorized" })),
        ("POST", "/oauth/authorize_device") if flags.unsupported.load(Ordering::SeqCst) => {
            (404, json!({ "error": "Not Found" }))
        }
        ("POST", "/oauth/authorize_device") => (
            200,
            json!({
                "device_code": "e2e-gitlab-device-code-opaque",
                "user_code": USER_CODE,
                "verification_uri": "https://gitlab.com/oauth/device",
                "verification_uri_complete": "https://gitlab.com/oauth/device?user_code=GLAB-0001",
                "expires_in": 900,
                "interval": 1,
            }),
        ),
        ("POST", "/oauth/token")
            if is_refresh
                && form_field("refresh_token").as_deref() == Some(REFRESH_TOKEN)
                && flags.refresh_exchanges.fetch_add(1, Ordering::SeqCst) == 0 =>
        {
            (
                200,
                json!({
                    "access_token": ROTATED_ACCESS_TOKEN,
                    "token_type": "Bearer",
                    "refresh_token": ROTATED_REFRESH_TOKEN,
                    "expires_in": 7200,
                    "scope": "api",
                }),
            )
        }
        ("POST", "/oauth/token") if is_refresh => (
            400,
            json!({ "error": "invalid_grant", "error_description": "revoked" }),
        ),
        ("POST", "/oauth/token") if flags.authorize.load(Ordering::SeqCst) => (
            200,
            json!({
                "access_token": ACCESS_TOKEN,
                "token_type": "Bearer",
                "refresh_token": REFRESH_TOKEN,
                "expires_in": if flags.short_lived.load(Ordering::SeqCst) { 60 } else { 7200 },
                "scope": "api",
            }),
        ),
        ("POST", "/oauth/token") => (400, json!({ "error": "authorization_pending" })),
        ("GET", "/api/v4/user") if bearer_ok => (
            200,
            json!({
                "id": 4242,
                "username": "glab-octocat",
                "name": "GitLab Octocat",
                "avatar_url": "https://gitlab.com/uploads/avatar.png",
                "email": "hidden@example.com",
            }),
        ),
        ("POST", "/api/v4/snippets") | ("GET", "/api/v4/user") => {
            (401, json!({ "message": "401 Unauthorized" }))
        }
        _ => (404, json!({ "error": "not_found" })),
    };
    if method == "POST" && route == "/oauth/token" && status == 200 {
        if is_refresh {
            if flags.hold_refresh.swap(false, Ordering::SeqCst) {
                flags.refresh_held.notify_one();
                flags.release_refresh.notified().await;
            }
        } else if flags.hold_authorize.swap(false, Ordering::SeqCst) {
            flags.authorize_held.notify_one();
            flags.release_authorize.notified().await;
        }
    } else if method == "POST"
        && route == "/oauth/token"
        && !is_refresh
        && status == 400
        && flags.hold_poll.swap(false, Ordering::SeqCst)
    {
        flags.poll_held.notify_one();
        flags.release_poll.notified().await;
    }
    // A snippet's raw read answers the file text itself; `204` has no body.
    let is_raw_read = matches!(snippet_route, Some((_, true))) && status == 200;
    let payload = match &body {
        Value::String(text) if is_raw_read => text.clone(),
        _ if status == 204 => String::new(),
        other => other.to_string(),
    };
    let response = format!(
        "HTTP/1.1 {status} {}\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        if (200..300).contains(&status) { "OK" } else { "Error" },
        if is_raw_read { "text/plain" } else { "application/json" },
        payload.len(),
        payload
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

/// A booted daemon pointed at `mock`, with the WSS port + pinned client config.
struct Harness {
    _data_dir: tempfile::TempDir,
    _daemon: Daemon,
    secrets_file: std::path::PathBuf,
    /// The daemon's stderr (tracing at `info`).
    log_file: std::path::PathBuf,
    port: u16,
    cfg: Arc<ClientConfig>,
}

async fn boot(mock: &MockGitlab) -> Harness {
    boot_with_env(mock, &[]).await
}

/// [`boot`] with extra daemon environment (applied after the harness's own,
/// so a test can put `GITLAB_TOKEN` back).
async fn boot_with_env(mock: &MockGitlab, extra_env: &[(&str, &str)]) -> Harness {
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let secrets_file = data_dir.join("secrets.json");
    let secrets_s = secrets_file.to_string_lossy().to_string();
    let mut env: Vec<(&str, &str)> = vec![
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_SECRETS_FILE", &secrets_s),
        ("INTENTD_GITLAB_API_BASE_URI", &mock.base_uri),
    ];
    env.extend_from_slice(extra_env);
    let child = spawn_serve(&data_dir, &env);
    let daemon = Daemon { child };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    Harness {
        log_file: data_dir.join("daemon.log"),
        _data_dir: data_dir_guard,
        _daemon: daemon,
        secrets_file,
        port,
        cfg: client_config(&fingerprint),
    }
}

/// Subscribe a fresh connection to the global `sourceControl:auth-changed`
/// stream (no workspace id, like `settings:changed`).
async fn subscriber(h: &Harness) -> Ws {
    let mut sub = connect_ws(h.port, h.cfg.clone()).await;
    let ack = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["sourceControl:auth-changed"] }),
    )
    .await;
    assert!(ack.get("error").is_none(), "subscribe failed: {ack}");
    sub
}

fn expect_typed_error(v: &Value, code: &str) {
    let err = &v["error"];
    assert_eq!(err["code"], json!(-32603), "envelope: {v}");
    assert_eq!(err["data"]["code"], json!(code), "envelope: {v}");
    assert_eq!(err["data"]["provider"], json!("gitlab"), "envelope: {v}");
    assert_eq!(err["data"]["host"], json!(HOST), "envelope: {v}");
}

fn expect_invalid_params(v: &Value) {
    assert_eq!(v["error"]["code"], json!(-32602), "envelope: {v}");
}

/// Device-grant lifecycle over WSS: authStatus reports the unconfigured bound
/// instance → connect returns the mock's codes and is idempotent while
/// pending → authStatus surfaces the pending flow → the mock authorizes → the
/// daemon's background poll persists the token pair and emits
/// `sourceControl:auth-changed { provider, host, status: "authorized" }` →
/// authStatus / getUser resolve the identity through the mock → revoke
/// deletes the pair and emits `revoked` → cancelAuth is an idempotent no-op.
#[tokio::test]
async fn gitlab_device_grant_full_lifecycle_over_wss() {
    let mock = spawn_mock_gitlab().await;
    let h = boot(&mock).await;
    let mut sub = subscriber(&h).await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    let gitlab = json!({ "provider": "gitlab" });

    // 1. Nothing stored: not configured, no user, method null, grant supported.
    let v = wss_rpc(&mut rpc, 10, "sourceControl.authStatus", gitlab.clone()).await;
    assert!(v.get("error").is_none(), "authStatus errored: {v}");
    let r = &v["result"];
    assert_eq!(r["isConfigured"], json!(false));
    assert_eq!(r["provider"], json!("gitlab"));
    assert_eq!(r["host"], json!(HOST));
    assert_eq!(r["method"], Value::Null);
    assert!(
        r.get("user").is_none(),
        "user absent when unconfigured: {r}"
    );
    assert_eq!(r["deviceGrantSupported"], json!(true));
    assert_eq!(r["deviceFlow"], Value::Null);
    assert_eq!(r["oauthUrl"], json!(""));

    // 2. connect (method omitted = device) → the mock's codes; the complete
    //    verification uri wins when the instance reports one.
    let v = wss_rpc(&mut rpc, 11, "sourceControl.connect", gitlab.clone()).await;
    assert!(v.get("error").is_none(), "connect errored: {v}");
    let r = &v["result"];
    assert_eq!(r["ok"], json!(true));
    assert_eq!(r["userCode"], json!(USER_CODE));
    assert_eq!(
        r["verificationUri"],
        json!("https://gitlab.com/oauth/device?user_code=GLAB-0001")
    );
    assert_eq!(r["interval"], json!(1));
    assert!(r["expiresIn"].as_u64().expect("expiresIn") > 0);
    // 🔒 Never the device code or a token on the wire.
    assert!(r.get("deviceCode").is_none());
    assert!(r.get("accessToken").is_none());

    // 3. connect again while pending → the SAME codes (idempotent).
    let v = wss_rpc(&mut rpc, 12, "sourceControl.connect", gitlab.clone()).await;
    assert_eq!(v["result"]["userCode"], json!(USER_CODE));

    // 4. authStatus while pending → deviceFlow.status == "pending".
    let v = wss_rpc(&mut rpc, 13, "sourceControl.authStatus", gitlab.clone()).await;
    let r = &v["result"];
    assert_eq!(r["isConfigured"], json!(false));
    assert_eq!(r["deviceFlow"]["status"], json!("pending"));
    assert_eq!(r["deviceFlow"]["userCode"], json!(USER_CODE));
    assert_eq!(
        r["oauthUrl"],
        json!("https://gitlab.com/oauth/device?user_code=GLAB-0001")
    );

    // 5. The user authorizes on (mock) gitlab.com; the daemon's background
    //    poll picks it up and pushes the provider-tagged event.
    mock.flags.authorize.store(true, Ordering::SeqCst);
    let ev = await_auth_changed(&mut sub, "authorized", 30).await;
    assert_eq!(
        ev,
        json!({ "provider": "gitlab", "host": HOST, "status": "authorized" })
    );

    // 6. The engine persisted the token pair + expiry under the gitlab
    //    secret accounts (server-side only — asserted on disk, never on the
    //    wire) and bound the host.
    let secrets = read_secrets(&h.secrets_file);
    assert_eq!(secrets["sourceControl.gitlab.token"], json!(ACCESS_TOKEN));
    assert_eq!(
        secrets["sourceControl.gitlab.refreshToken"],
        json!(REFRESH_TOKEN)
    );
    let expires_at: u64 = secrets["sourceControl.gitlab.tokenExpiresAt"]
        .as_str()
        .expect("tokenExpiresAt stored")
        .parse()
        .expect("unix seconds");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    assert!(
        expires_at > now + 3600 && expires_at <= now + 7200,
        "expiry derived from expires_in: {expires_at} vs now {now}"
    );
    let v = wss_rpc(
        &mut rpc,
        14,
        "settings.get",
        json!({ "path": "sourceControl.gitlab.host" }),
    )
    .await;
    assert_eq!(v["result"]["value"], json!(HOST), "bound host: {v}");

    // 7. authStatus now probes the mock's `/api/v4/user` with the stored
    //    token: configured, method "device", identity projected without
    //    the email; the authorized transition cleared the flow slot.
    let v = wss_rpc(&mut rpc, 15, "sourceControl.authStatus", gitlab.clone()).await;
    let r = &v["result"];
    assert_eq!(r["isConfigured"], json!(true), "{r}");
    assert_eq!(r["method"], json!("device"));
    assert_eq!(
        r["user"],
        json!({
            "id": "4242",
            "login": "glab-octocat",
            "displayName": "GitLab Octocat",
            "avatarUrl": "https://gitlab.com/uploads/avatar.png",
        })
    );
    assert_eq!(r["deviceFlow"], Value::Null);

    // 8. getUser → the same identity; the github alias and the generic form
    //    of cancelAuth agree that nothing is in flight for github.
    let v = wss_rpc(&mut rpc, 16, "sourceControl.getUser", gitlab.clone()).await;
    assert_eq!(v["result"]["user"]["login"], json!("glab-octocat"));
    let alias = wss_rpc(&mut rpc, 17, "github.cancelAuth", json!({})).await;
    let generic = wss_rpc(
        &mut rpc,
        18,
        "sourceControl.cancelAuth",
        json!({ "provider": "github" }),
    )
    .await;
    assert_eq!(alias["result"], generic["result"]);
    assert_eq!(alias["result"], json!({ "ok": true, "cancelled": false }));

    // 9. revoke → the pair is deleted and `revoked` is emitted.
    let v = wss_rpc(&mut rpc, 19, "sourceControl.revoke", gitlab.clone()).await;
    assert_eq!(v["result"], json!({ "ok": true }));
    let ev = await_auth_changed(&mut sub, "revoked", 15).await;
    assert_eq!(
        ev,
        json!({ "provider": "gitlab", "host": HOST, "status": "revoked" })
    );
    let secrets = read_secrets(&h.secrets_file);
    for account in [
        "sourceControl.gitlab.token",
        "sourceControl.gitlab.refreshToken",
        "sourceControl.gitlab.tokenExpiresAt",
    ] {
        assert!(
            secrets.get(account).is_none(),
            "{account} removed: {secrets}"
        );
    }
    let v = wss_rpc(&mut rpc, 20, "sourceControl.getUser", gitlab.clone()).await;
    assert_eq!(v["result"], json!({ "user": null }));

    // 10. cancelAuth with nothing in flight → idempotent no-op.
    let v = wss_rpc(&mut rpc, 21, "sourceControl.cancelAuth", gitlab).await;
    assert_eq!(v["result"], json!({ "ok": true, "cancelled": false }));
}

/// PAT connect over WSS: a rejected token stores nothing and surfaces as the
/// typed `source-control-unauthorized` error; an accepted one is persisted
/// (no refresh token), emits `authorized` and reports `method: "pat"`.
/// The parameter refusals of §5.27 are `-32602`.
#[tokio::test]
async fn gitlab_pat_connect_over_wss() {
    let mock = spawn_mock_gitlab().await;
    let h = boot(&mock).await;
    let mut sub = subscriber(&h).await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;

    // Rejected token → typed error, nothing on disk, still unconfigured.
    let v = wss_rpc(
        &mut rpc,
        10,
        "sourceControl.connect",
        json!({ "provider": "gitlab", "method": "pat", "token": BAD_PAT }),
    )
    .await;
    expect_typed_error(&v, "source-control-unauthorized");
    assert!(
        !v.to_string().contains(BAD_PAT),
        "🔒 the token never echoes in an error: {v}"
    );
    assert!(read_secrets(&h.secrets_file)
        .get("sourceControl.gitlab.token")
        .is_none());

    // Accepted token → persisted + authorized event.
    let v = wss_rpc(
        &mut rpc,
        11,
        "sourceControl.connect",
        json!({ "provider": "gitlab", "method": "pat", "token": PAT_TOKEN }),
    )
    .await;
    assert_eq!(v["result"], json!({ "ok": true, "method": "pat" }), "{v}");
    let ev = await_auth_changed(&mut sub, "authorized", 15).await;
    assert_eq!(
        ev,
        json!({ "provider": "gitlab", "host": HOST, "status": "authorized" })
    );
    let secrets = read_secrets(&h.secrets_file);
    assert_eq!(secrets["sourceControl.gitlab.token"], json!(PAT_TOKEN));
    assert!(secrets.get("sourceControl.gitlab.refreshToken").is_none());
    assert!(secrets.get("sourceControl.gitlab.tokenExpiresAt").is_none());

    let v = wss_rpc(
        &mut rpc,
        12,
        "sourceControl.authStatus",
        json!({ "provider": "gitlab", "host": HOST }),
    )
    .await;
    let r = &v["result"];
    assert_eq!(r["isConfigured"], json!(true), "{r}");
    assert_eq!(r["method"], json!("pat"));
    assert_eq!(r["user"]["login"], json!("glab-octocat"));
    assert_eq!(r["host"], json!(HOST));

    // Parameter refusals (§5.27): all -32602, none of them touch the store.
    let refusals = [
        json!({ "provider": "gitlab", "method": "device", "token": PAT_TOKEN }),
        json!({ "provider": "gitlab", "method": "pat" }),
        json!({ "provider": "gitlab", "method": "oauth" }),
        json!({ "provider": "github", "method": "pat", "token": PAT_TOKEN }),
        json!({ "provider": "bitbucket" }),
        json!({ "provider": "gitlab", "host": "https://gitlab.com" }),
    ];
    for (id, params) in (20i64..).zip(refusals) {
        let v = wss_rpc(&mut rpc, id, "sourceControl.connect", params.clone()).await;
        expect_invalid_params(&v);
        assert!(!v.to_string().contains(PAT_TOKEN), "🔒 {params}: {v}");
    }
    let v = wss_rpc(
        &mut rpc,
        30,
        "sourceControl.authStatus",
        json!({ "provider": "bitbucket" }),
    )
    .await;
    expect_invalid_params(&v);
    assert_eq!(
        read_secrets(&h.secrets_file)["sourceControl.gitlab.token"],
        json!(PAT_TOKEN),
        "refusals left the stored token alone"
    );
}

/// The GitLab snippet identity proof, guest half, over WSS (protocol 10.8,
/// `sourceControl.identityProof.create` / `delete`): refused with the typed
/// `gitlab-not-connected` before a connection exists (and for a host that is
/// not the bound instance) → param refusals are `-32602` and touch nothing →
/// a connected PAT publishes the nonce as a **public** single-file personal
/// snippet with the stored token, the owner identity coming from the
/// snippet's `author` with no follow-up user call → delete reads the snippet
/// back, deletes it, is idempotent, and refuses a snippet that is not an
/// Intent proof → a token without the `api` scope is `gitlab-scope-missing`
/// → the `github.identityProof.*` aliases still answer `github-not-connected`
/// on this GitHub-less daemon → revoke returns create to `gitlab-not-connected`.
/// 🔒 No response ever carries the token.
#[tokio::test]
async fn gitlab_snippet_identity_proof_over_wss() {
    let mock = spawn_mock_gitlab().await;
    let h = boot(&mock).await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    let create = |nonce: &str| json!({ "provider": "gitlab", "nonce": nonce, "hostLabel": "Clement's Mac Studio" });
    let expect_proof_error = |v: &Value, code: &str| {
        assert_eq!(v["error"]["code"], json!(-32603), "envelope: {v}");
        assert_eq!(v["error"]["data"], json!({ "code": code }), "envelope: {v}");
    };

    // 1. Nothing connected → typed refusal, nothing created.
    let v = wss_rpc(
        &mut rpc,
        1,
        "sourceControl.identityProof.create",
        create("n-0"),
    )
    .await;
    expect_proof_error(&v, "gitlab-not-connected");
    let v = wss_rpc(
        &mut rpc,
        2,
        "sourceControl.identityProof.delete",
        json!({ "provider": "gitlab", "proofId": "1" }),
    )
    .await;
    expect_proof_error(&v, "gitlab-not-connected");

    // 2. Param refusals (all -32602, none reach the forge).
    let refusals = [
        json!({ "provider": "bitbucket", "nonce": "n", "hostLabel": "h" }),
        json!({ "provider": "github", "host": HOST, "nonce": "n", "hostLabel": "h" }),
        json!({ "provider": "gitlab", "host": "https://gitlab.com", "nonce": "n", "hostLabel": "h" }),
        json!({ "provider": "gitlab", "hostLabel": "h" }),
        json!({ "provider": "gitlab", "nonce": "n" }),
        json!({ "provider": "gitlab", "nonce": "line1\nline2", "hostLabel": "h" }),
        json!({ "provider": "gitlab", "nonce": "   ", "hostLabel": "h" }),
    ];
    for (id, params) in (10i64..).zip(refusals) {
        let v = wss_rpc(
            &mut rpc,
            id,
            "sourceControl.identityProof.create",
            params.clone(),
        )
        .await;
        expect_invalid_params(&v);
    }
    for (id, proof_id) in (20i64..).zip(["", "abc", "1/raw", "-1"]) {
        let v = wss_rpc(
            &mut rpc,
            id,
            "sourceControl.identityProof.delete",
            json!({ "provider": "gitlab", "proofId": proof_id }),
        )
        .await;
        expect_invalid_params(&v);
    }
    assert!(mock.flags.snippets.lock().unwrap().is_empty());

    // 3. Connect with a PAT, then prove.
    let v = wss_rpc(
        &mut rpc,
        30,
        "sourceControl.connect",
        json!({ "provider": "gitlab", "method": "pat", "token": PAT_TOKEN }),
    )
    .await;
    assert_eq!(v["result"], json!({ "ok": true, "method": "pat" }), "{v}");

    // A host other than the bound instance has no credential.
    let v = wss_rpc(
        &mut rpc,
        31,
        "sourceControl.identityProof.create",
        json!({ "provider": "gitlab", "host": "gitlab.example.org", "nonce": "n-1", "hostLabel": "h" }),
    )
    .await;
    expect_proof_error(&v, "gitlab-not-connected");

    let user_calls_before = mock.flags.user_requests.load(Ordering::SeqCst);
    let v = wss_rpc(
        &mut rpc,
        32,
        "sourceControl.identityProof.create",
        create(" n-1 "),
    )
    .await;
    assert_eq!(
        v["result"],
        json!({
            "proofId": "1",
            "provider": "gitlab",
            "host": HOST,
            "login": "glab-octocat",
            "externalUserId": "4242",
            "avatarUrl": "https://gitlab.com/uploads/avatar.png",
        }),
        "{v}"
    );
    assert!(!v.to_string().contains(PAT_TOKEN), "🔒 {v}");
    assert_eq!(
        mock.flags.user_requests.load(Ordering::SeqCst),
        user_calls_before,
        "identity comes from the snippet author: no follow-up user call"
    );
    {
        let snippets = mock.flags.snippets.lock().unwrap();
        assert_eq!(snippets.len(), 1);
        let (_, bearer, posted) = &snippets[0];
        assert_eq!(bearer, PAT_TOKEN, "made with the stored token");
        assert_eq!(posted["visibility"], json!("public"));
        assert_eq!(
            posted["title"],
            json!("Intent identity proof for Clement's Mac Studio (safe to delete)")
        );
        let files = posted["files"].as_array().expect("files");
        assert_eq!(files.len(), 1, "exactly one file: {posted}");
        assert_eq!(files[0]["file_path"], json!("intent-join-proof.txt"));
        let content = files[0]["content"].as_str().expect("content");
        assert_eq!(
            content.lines().next(),
            Some("n-1"),
            "nonce is the first line"
        );
    }

    // 4. Delete: reads back, deletes, idempotent, refuses non-proof snippets.
    // A second, non-proof snippet of the account (seeded straight into the mock).
    mock.flags.snippets.lock().unwrap().push((
        2,
        PAT_TOKEN.to_string(),
        json!({
            "title": "my notes",
            "visibility": "private",
            "files": [{ "file_path": "notes.md", "content": "hello" }],
        }),
    ));
    let v = wss_rpc(
        &mut rpc,
        40,
        "sourceControl.identityProof.delete",
        json!({ "provider": "gitlab", "host": HOST, "proofId": "1" }),
    )
    .await;
    assert_eq!(v["result"], json!({ "ok": true }), "{v}");
    let v = wss_rpc(
        &mut rpc,
        41,
        "sourceControl.identityProof.delete",
        json!({ "provider": "gitlab", "proofId": "1" }),
    )
    .await;
    assert_eq!(
        v["result"],
        json!({ "ok": true }),
        "already deleted is ok: {v}"
    );
    let v = wss_rpc(
        &mut rpc,
        42,
        "sourceControl.identityProof.delete",
        json!({ "provider": "gitlab", "proofId": "2" }),
    )
    .await;
    expect_invalid_params(&v);
    assert!(
        v["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("nothing deleted")),
        "{v}"
    );
    {
        let snippets = mock.flags.snippets.lock().unwrap();
        assert_eq!(snippets.len(), 1, "the non-proof snippet survives");
        assert_eq!(snippets[0].0, 2);
    }

    // 5. A token without the `api` scope → `gitlab-scope-missing`.
    let v = wss_rpc(
        &mut rpc,
        50,
        "sourceControl.connect",
        json!({ "provider": "gitlab", "method": "pat", "token": READ_ONLY_PAT }),
    )
    .await;
    assert_eq!(v["result"], json!({ "ok": true, "method": "pat" }), "{v}");
    let v = wss_rpc(
        &mut rpc,
        51,
        "sourceControl.identityProof.create",
        create("n-2"),
    )
    .await;
    expect_proof_error(&v, "gitlab-scope-missing");
    assert!(!v.to_string().contains(READ_ONLY_PAT), "🔒 {v}");

    // 6. The GitHub aliases are untouched by the GitLab connection: this
    // daemon has no GitHub token, and the gitlab path never consulted one.
    let v = wss_rpc(
        &mut rpc,
        60,
        "github.identityProof.create",
        json!({ "nonce": "n", "hostLabel": "h" }),
    )
    .await;
    expect_proof_error(&v, "github-not-connected");
    let v = wss_rpc(
        &mut rpc,
        61,
        "sourceControl.identityProof.create",
        json!({ "provider": "github", "nonce": "n", "hostLabel": "h" }),
    )
    .await;
    expect_proof_error(&v, "github-not-connected");

    // 7. Revoke → back to not connected.
    let v = wss_rpc(
        &mut rpc,
        70,
        "sourceControl.revoke",
        json!({ "provider": "gitlab", "host": HOST }),
    )
    .await;
    assert!(v.get("error").is_none(), "{v}");
    let v = wss_rpc(
        &mut rpc,
        71,
        "sourceControl.identityProof.create",
        create("n-3"),
    )
    .await;
    expect_proof_error(&v, "gitlab-not-connected");
    assert_eq!(mock.flags.snippets.lock().unwrap().len(), 1);
}

/// An instance without the device grant (404 on `/oauth/authorize_device`)
/// → typed `device-grant-unsupported`, remembered per host so authStatus
/// flips `deviceGrantSupported` to false and a repeat connect fails without
/// another request; a self-hosted instance with no configured client id is
/// unsupported up front. A pending flow can be cancelled host-scoped.
#[tokio::test]
async fn gitlab_device_grant_unsupported_and_cancel_over_wss() {
    let mock = spawn_mock_gitlab().await;
    let h = boot(&mock).await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    let gitlab = json!({ "provider": "gitlab" });

    // Cancel path first (the grant is still supported): pending → cancelled.
    let v = wss_rpc(&mut rpc, 10, "sourceControl.connect", gitlab.clone()).await;
    assert_eq!(v["result"]["ok"], json!(true), "{v}");
    let v = wss_rpc(
        &mut rpc,
        11,
        "sourceControl.cancelAuth",
        json!({ "provider": "gitlab", "host": "gitlab.acme.internal" }),
    )
    .await;
    assert_eq!(
        v["result"],
        json!({ "ok": true, "cancelled": false }),
        "another host never cancels the bound instance's flow"
    );
    let v = wss_rpc(&mut rpc, 12, "sourceControl.cancelAuth", gitlab.clone()).await;
    assert_eq!(v["result"], json!({ "ok": true, "cancelled": true }));
    let v = wss_rpc(&mut rpc, 13, "sourceControl.authStatus", gitlab.clone()).await;
    assert_eq!(v["result"]["deviceFlow"], Value::Null);
    assert_eq!(v["result"]["deviceGrantSupported"], json!(true));

    // The instance stops offering the grant.
    mock.flags.unsupported.store(true, Ordering::SeqCst);
    let v = wss_rpc(&mut rpc, 14, "sourceControl.connect", gitlab.clone()).await;
    expect_typed_error(&v, "device-grant-unsupported");
    let v = wss_rpc(&mut rpc, 15, "sourceControl.authStatus", gitlab.clone()).await;
    assert_eq!(v["result"]["deviceGrantSupported"], json!(false), "{v}");
    assert_eq!(v["result"]["isConfigured"], json!(false));
    // Remembered: the grant is not retried even once the mock recovers.
    mock.flags.unsupported.store(false, Ordering::SeqCst);
    let v = wss_rpc(&mut rpc, 16, "sourceControl.connect", gitlab.clone()).await;
    expect_typed_error(&v, "device-grant-unsupported");

    // A self-hosted instance with no `sourceControl.gitlab.oauthClientId`
    // has no client id to run the grant with: unsupported without a request.
    let v = wss_rpc(
        &mut rpc,
        17,
        "sourceControl.connect",
        json!({ "provider": "gitlab", "host": "gitlab.acme.internal" }),
    )
    .await;
    let err = &v["error"];
    assert_eq!(err["code"], json!(-32603), "{v}");
    assert_eq!(err["data"]["code"], json!("device-grant-unsupported"));
    assert_eq!(err["data"]["host"], json!("gitlab.acme.internal"));

    // The PAT route stays open on the bound instance regardless.
    let v = wss_rpc(
        &mut rpc,
        18,
        "sourceControl.connect",
        json!({ "provider": "gitlab", "method": "pat", "token": PAT_TOKEN }),
    )
    .await;
    assert_eq!(v["result"], json!({ "ok": true, "method": "pat" }), "{v}");
}

/// Token refresh over WSS: the grant mints a 60 s access token (inside the
/// daemon's refresh leeway), so the next authStatus proactively exchanges the
/// refresh token, persists the rotated pair and still reports the identity.
/// When the instance later rejects the rotated access token AND refuses the
/// rotated refresh token (`invalid_grant`), the daemon clears the connection
/// and emits `sourceControl:auth-changed { status: "expired" }`.
#[tokio::test]
async fn gitlab_token_refresh_and_expiry_over_wss() {
    let mock = spawn_mock_gitlab().await;
    mock.flags.short_lived.store(true, Ordering::SeqCst);
    let h = boot(&mock).await;
    let mut sub = subscriber(&h).await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    let gitlab = json!({ "provider": "gitlab" });

    let v = wss_rpc(&mut rpc, 10, "sourceControl.connect", gitlab.clone()).await;
    assert_eq!(v["result"]["userCode"], json!(USER_CODE), "{v}");
    mock.flags.authorize.store(true, Ordering::SeqCst);
    let ev = await_auth_changed(&mut sub, "authorized", 30).await;
    assert_eq!(ev["status"], json!("authorized"));
    let secrets = read_secrets(&h.secrets_file);
    assert_eq!(secrets["sourceControl.gitlab.token"], json!(ACCESS_TOKEN));
    assert_eq!(
        secrets["sourceControl.gitlab.refreshToken"],
        json!(REFRESH_TOKEN)
    );

    // Proactive refresh: the probe sees the near-expiry and rotates first.
    let v = wss_rpc(&mut rpc, 11, "sourceControl.authStatus", gitlab.clone()).await;
    let r = &v["result"];
    assert_eq!(r["isConfigured"], json!(true), "{r}");
    assert_eq!(r["method"], json!("device"));
    assert_eq!(r["user"]["login"], json!("glab-octocat"));
    let secrets = read_secrets(&h.secrets_file);
    assert_eq!(
        secrets["sourceControl.gitlab.token"],
        json!(ROTATED_ACCESS_TOKEN),
        "rotated access token persisted: {secrets}"
    );
    assert_eq!(
        secrets["sourceControl.gitlab.refreshToken"],
        json!(ROTATED_REFRESH_TOKEN)
    );
    let expires_at: u64 = secrets["sourceControl.gitlab.tokenExpiresAt"]
        .as_str()
        .expect("tokenExpiresAt stored")
        .parse()
        .expect("unix seconds");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    assert!(
        expires_at > now + 3600,
        "expiry moved out: {expires_at} vs {now}"
    );

    // A second probe needs no refresh and keeps the rotated pair.
    let v = wss_rpc(&mut rpc, 12, "sourceControl.getUser", gitlab.clone()).await;
    assert_eq!(v["result"]["user"]["login"], json!("glab-octocat"));
    assert_eq!(
        read_secrets(&h.secrets_file)["sourceControl.gitlab.refreshToken"],
        json!(ROTATED_REFRESH_TOKEN)
    );

    // The instance revokes the rotated access token; the one retry-refresh
    // is refused (the rotated refresh token is not accepted) → disconnect.
    mock.flags.reject_rotated.store(true, Ordering::SeqCst);
    let v = wss_rpc(&mut rpc, 13, "sourceControl.authStatus", gitlab.clone()).await;
    let r = &v["result"];
    assert_eq!(r["isConfigured"], json!(false), "{r}");
    assert_eq!(r["method"], Value::Null);
    assert!(r.get("user").is_none(), "{r}");
    let ev = await_auth_changed(&mut sub, "expired", 15).await;
    assert_eq!(
        ev,
        json!({ "provider": "gitlab", "host": HOST, "status": "expired" })
    );
    let secrets = read_secrets(&h.secrets_file);
    for account in [
        "sourceControl.gitlab.token",
        "sourceControl.gitlab.refreshToken",
        "sourceControl.gitlab.tokenExpiresAt",
    ] {
        assert!(
            secrets.get(account).is_none(),
            "{account} cleared: {secrets}"
        );
    }
    let v = wss_rpc(&mut rpc, 14, "sourceControl.getUser", gitlab).await;
    assert_eq!(v["result"], json!({ "user": null }));
}

/// Regression (intentd#2037 review): the identity-proof token read runs the
/// same proactive refresh as the auth-status probe. A device grant whose
/// access token is near expiry is rotated by `sourceControl.identityProof.create`
/// itself — no other RPC in between — and the snippet is made with the
/// rotated token; a grant whose refresh the instance refuses clears the
/// connection (`expired`) and the create is `gitlab-not-connected` with
/// nothing published; a PAT is used as is, never refreshed.
#[tokio::test]
async fn gitlab_identity_proof_refreshes_a_near_expiry_device_grant_over_wss() {
    let mock = spawn_mock_gitlab().await;
    mock.flags.short_lived.store(true, Ordering::SeqCst);
    let h = boot(&mock).await;
    let mut sub = subscriber(&h).await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    let gitlab = json!({ "provider": "gitlab" });
    let create = json!({ "provider": "gitlab", "nonce": "n-refresh", "hostLabel": "h" });

    // 1. A near-expiry device grant; nothing has probed it yet.
    let v = wss_rpc(&mut rpc, 10, "sourceControl.connect", gitlab.clone()).await;
    assert_eq!(v["result"]["userCode"], json!(USER_CODE), "{v}");
    mock.flags.authorize.store(true, Ordering::SeqCst);
    await_auth_changed(&mut sub, "authorized", 30).await;
    assert_eq!(
        read_secrets(&h.secrets_file)["sourceControl.gitlab.token"],
        json!(ACCESS_TOKEN)
    );
    assert_eq!(mock.flags.refresh_exchanges.load(Ordering::SeqCst), 0);

    // 2. The proof create rotates the pair first and signs with the rotated
    // token.
    let v = wss_rpc(
        &mut rpc,
        11,
        "sourceControl.identityProof.create",
        create.clone(),
    )
    .await;
    assert_eq!(v["result"]["proofId"], json!("1"), "{v}");
    assert_eq!(v["result"]["login"], json!("glab-octocat"), "{v}");
    assert_eq!(mock.flags.refresh_exchanges.load(Ordering::SeqCst), 1);
    {
        let snippets = mock.flags.snippets.lock().unwrap();
        assert_eq!(snippets.len(), 1);
        assert_eq!(
            snippets[0].1, ROTATED_ACCESS_TOKEN,
            "made with the rotated token"
        );
    }
    let secrets = read_secrets(&h.secrets_file);
    assert_eq!(
        secrets["sourceControl.gitlab.token"],
        json!(ROTATED_ACCESS_TOKEN),
        "rotated access token persisted: {secrets}"
    );
    assert_eq!(
        secrets["sourceControl.gitlab.refreshToken"],
        json!(ROTATED_REFRESH_TOKEN)
    );

    // The rotated grant is not near expiry: delete needs no refresh.
    let v = wss_rpc(
        &mut rpc,
        12,
        "sourceControl.identityProof.delete",
        json!({ "provider": "gitlab", "proofId": "1" }),
    )
    .await;
    assert_eq!(v["result"], json!({ "ok": true }), "{v}");
    assert_eq!(mock.flags.refresh_exchanges.load(Ordering::SeqCst), 1);
    assert!(mock.flags.snippets.lock().unwrap().is_empty());

    // 3. A fresh near-expiry grant whose refresh the instance refuses (the
    // mock rotates exactly once): the create clears the connection and is
    // `gitlab-not-connected`; nothing is published.
    let v = wss_rpc(&mut rpc, 20, "sourceControl.revoke", gitlab.clone()).await;
    assert!(v.get("error").is_none(), "{v}");
    await_auth_changed(&mut sub, "revoked", 15).await;
    let v = wss_rpc(&mut rpc, 21, "sourceControl.connect", gitlab.clone()).await;
    assert_eq!(v["result"]["userCode"], json!(USER_CODE), "{v}");
    await_auth_changed(&mut sub, "authorized", 30).await;
    assert_eq!(
        read_secrets(&h.secrets_file)["sourceControl.gitlab.refreshToken"],
        json!(REFRESH_TOKEN)
    );
    let v = wss_rpc(
        &mut rpc,
        22,
        "sourceControl.identityProof.create",
        create.clone(),
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32603), "envelope: {v}");
    assert_eq!(
        v["error"]["data"],
        json!({ "code": "gitlab-not-connected" }),
        "envelope: {v}"
    );
    let ev = await_auth_changed(&mut sub, "expired", 15).await;
    assert_eq!(
        ev,
        json!({ "provider": "gitlab", "host": HOST, "status": "expired" })
    );
    assert_eq!(mock.flags.refresh_exchanges.load(Ordering::SeqCst), 2);
    assert!(mock.flags.snippets.lock().unwrap().is_empty());
    let secrets = read_secrets(&h.secrets_file);
    for account in [
        "sourceControl.gitlab.token",
        "sourceControl.gitlab.refreshToken",
        "sourceControl.gitlab.tokenExpiresAt",
    ] {
        assert!(
            secrets.get(account).is_none(),
            "{account} cleared: {secrets}"
        );
    }

    // 4. A PAT is returned as is: no refresh exchange, signed with the PAT.
    let v = wss_rpc(
        &mut rpc,
        30,
        "sourceControl.connect",
        json!({ "provider": "gitlab", "method": "pat", "token": PAT_TOKEN }),
    )
    .await;
    assert_eq!(v["result"], json!({ "ok": true, "method": "pat" }), "{v}");
    let v = wss_rpc(&mut rpc, 31, "sourceControl.identityProof.create", create).await;
    assert_eq!(v["result"]["login"], json!("glab-octocat"), "{v}");
    assert_eq!(mock.flags.refresh_exchanges.load(Ordering::SeqCst), 2);
    {
        let snippets = mock.flags.snippets.lock().unwrap();
        assert_eq!(snippets.len(), 1);
        assert_eq!(snippets[0].1, PAT_TOKEN, "made with the pat");
    }
    assert!(!v.to_string().contains(PAT_TOKEN), "🔒 {v}");
}

/// Contract decision (PR Context, 2026-09-20): `sourceControl.revoke` is
/// idempotent and host-scoped. With host A bound, `revoke(gitlab, host B)`
/// is a successful no-op — A's token stays, no `auth-changed` is emitted for
/// B — and it aborts only a device grant pending for exactly B. Only a
/// malformed host is `-32602`; "not connected" never is.
#[tokio::test]
async fn gitlab_revoke_is_host_scoped_and_idempotent_over_wss() {
    const OTHER_HOST: &str = "gitlab.acme.internal";
    let mock = spawn_mock_gitlab().await;
    let h = boot(&mock).await;
    let mut sub = subscriber(&h).await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    let gitlab = json!({ "provider": "gitlab" });
    let other = json!({ "provider": "gitlab", "host": OTHER_HOST });

    // Bind host A (the configured instance) with a PAT.
    let v = wss_rpc(
        &mut rpc,
        10,
        "sourceControl.connect",
        json!({ "provider": "gitlab", "method": "pat", "token": PAT_TOKEN }),
    )
    .await;
    assert_eq!(v["result"], json!({ "ok": true, "method": "pat" }), "{v}");
    let ev = await_auth_changed(&mut sub, "authorized", 15).await;
    assert_eq!(ev["host"], json!(HOST));
    assert_eq!(
        read_secrets(&h.secrets_file)["sourceControl.gitlab.token"],
        json!(PAT_TOKEN)
    );

    // revoke(gitlab, host B): ok, A's token untouched, A still configured.
    let v = wss_rpc(&mut rpc, 11, "sourceControl.revoke", other.clone()).await;
    assert_eq!(v["result"], json!({ "ok": true }), "{v}");
    assert_eq!(
        read_secrets(&h.secrets_file)["sourceControl.gitlab.token"],
        json!(PAT_TOKEN),
        "another host's revoke never deletes the bound instance's token"
    );
    let v = wss_rpc(&mut rpc, 12, "sourceControl.authStatus", gitlab.clone()).await;
    assert_eq!(v["result"]["isConfigured"], json!(true), "{v}");
    assert_eq!(v["result"]["host"], json!(HOST));

    // Start a device grant on A; revoke(host B) leaves that grant pending.
    let v = wss_rpc(&mut rpc, 13, "sourceControl.connect", gitlab.clone()).await;
    assert_eq!(v["result"]["userCode"], json!(USER_CODE), "{v}");
    let v = wss_rpc(&mut rpc, 14, "sourceControl.revoke", other.clone()).await;
    assert_eq!(v["result"], json!({ "ok": true }));
    let v = wss_rpc(&mut rpc, 15, "sourceControl.authStatus", gitlab.clone()).await;
    assert_eq!(
        v["result"]["deviceFlow"]["status"],
        json!("pending"),
        "another host's revoke never aborts the bound instance's grant: {v}"
    );
    // cancelAuth on host B is the same host-scoped no-op.
    let v = wss_rpc(&mut rpc, 16, "sourceControl.cancelAuth", other.clone()).await;
    assert_eq!(v["result"], json!({ "ok": true, "cancelled": false }));

    // Not-connected is never -32602; only a malformed host is.
    let v = wss_rpc(
        &mut rpc,
        17,
        "sourceControl.revoke",
        json!({ "provider": "gitlab", "host": "https://gitlab.acme.internal/x" }),
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32602), "{v}");

    // revoke(host A) aborts the pending grant, deletes the token and emits
    // `revoked` for A — and that is the FIRST auth-changed event since the
    // PAT connect, so nothing was ever emitted for host B.
    let v = wss_rpc(&mut rpc, 18, "sourceControl.revoke", gitlab.clone()).await;
    assert_eq!(v["result"], json!({ "ok": true }));
    let ev = await_auth_changed_matching(&mut sub, None, 15).await;
    assert_eq!(
        ev,
        json!({ "provider": "gitlab", "host": HOST, "status": "revoked" }),
        "no auth-changed may be emitted for a host that was never connected"
    );
    let v = wss_rpc(&mut rpc, 19, "sourceControl.authStatus", gitlab.clone()).await;
    assert_eq!(v["result"]["isConfigured"], json!(false), "{v}");
    assert_eq!(v["result"]["deviceFlow"], Value::Null);
    assert!(read_secrets(&h.secrets_file)
        .get("sourceControl.gitlab.token")
        .is_none());

    // Idempotent: revoking again with nothing bound is still ok.
    let v = wss_rpc(&mut rpc, 20, "sourceControl.revoke", other).await;
    assert_eq!(v["result"], json!({ "ok": true }));
    let v = wss_rpc(&mut rpc, 21, "sourceControl.cancelAuth", gitlab).await;
    assert_eq!(v["result"], json!({ "ok": true, "cancelled": false }));
}

/// Every credential the daemon holds belongs to the bound instance — the
/// `GITLAB_TOKEN` fallback included. A probe of another host (a typo, an
/// instance switch) resolves no credential at all and sends no request:
/// `authStatus` is a plain not-configured answer, `getUser` is `{ user: null }`,
/// never an error carrying the bound instance's secret off-host.
#[tokio::test]
async fn gitlab_env_token_is_bound_to_the_configured_host_over_wss() {
    const OTHER_HOST: &str = "gitlab.acme.internal";
    let mock = spawn_mock_gitlab().await;
    let h = boot_with_env(&mock, &[("GITLAB_TOKEN", PAT_TOKEN)]).await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    let gitlab = json!({ "provider": "gitlab" });
    let other = json!({ "provider": "gitlab", "host": OTHER_HOST });

    // The bound instance resolves the env credential (provenance "env").
    let v = wss_rpc(&mut rpc, 10, "sourceControl.authStatus", gitlab.clone()).await;
    let r = &v["result"];
    assert_eq!(r["isConfigured"], json!(true), "{r}");
    assert_eq!(r["method"], json!("env"));
    assert_eq!(r["host"], json!(HOST));
    assert_eq!(r["user"]["login"], json!("glab-octocat"));
    let probes = mock.flags.user_requests.load(Ordering::SeqCst);
    assert!(probes >= 1, "the bound probe reached the instance");

    // Another host: not configured, no error, and nothing leaves the daemon
    // (the bound instance's API override is not applied to another host, so
    // the only observable request path is the one that must stay silent).
    let v = wss_rpc(&mut rpc, 11, "sourceControl.authStatus", other.clone()).await;
    let r = &v["result"];
    assert!(v.get("error").is_none(), "{v}");
    assert_eq!(r["isConfigured"], json!(false), "{r}");
    assert_eq!(r["method"], Value::Null);
    assert!(r.get("user").is_none(), "{r}");
    assert_eq!(r["host"], json!(OTHER_HOST));
    let v = wss_rpc(&mut rpc, 12, "sourceControl.getUser", other).await;
    assert_eq!(v["result"], json!({ "user": null }), "{v}");
    assert_eq!(
        mock.flags.user_requests.load(Ordering::SeqCst),
        probes,
        "an unbound host's probe never reaches an instance"
    );

    // The bound instance is untouched by the other host's probes.
    let v = wss_rpc(&mut rpc, 13, "sourceControl.getUser", gitlab).await;
    assert_eq!(v["result"]["user"]["login"], json!("glab-octocat"), "{v}");
}

/// Two probes that overlap on the same near-expiry device credential must
/// not both replay the refresh token: a real instance consumes it on the
/// first exchange, so the loser's `invalid_grant` would tear down a live
/// connection. The mock accepts `REFRESH_TOKEN` exactly once; the daemon
/// serialises the rotation, the second probe re-reads the rotated pair and
/// simply uses it — one exchange, both probes configured, no `expired`.
#[tokio::test]
async fn gitlab_concurrent_probes_rotate_the_pair_once_over_wss() {
    let mock = spawn_mock_gitlab().await;
    mock.flags.short_lived.store(true, Ordering::SeqCst);
    let h = boot(&mock).await;
    let mut sub = subscriber(&h).await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    let gitlab = json!({ "provider": "gitlab" });

    let v = wss_rpc(&mut rpc, 10, "sourceControl.connect", gitlab.clone()).await;
    assert_eq!(v["result"]["userCode"], json!(USER_CODE), "{v}");
    mock.flags.authorize.store(true, Ordering::SeqCst);
    let ev = await_auth_changed(&mut sub, "authorized", 30).await;
    assert_eq!(ev["status"], json!("authorized"));
    assert_eq!(
        read_secrets(&h.secrets_file)["sourceControl.gitlab.refreshToken"],
        json!(REFRESH_TOKEN)
    );

    // Two connections race a probe of the same near-expiry pair.
    let mut a = connect_ws(h.port, h.cfg.clone()).await;
    let mut b = connect_ws(h.port, h.cfg.clone()).await;
    let (va, vb) = tokio::join!(
        wss_rpc(&mut a, 20, "sourceControl.authStatus", gitlab.clone()),
        wss_rpc(&mut b, 21, "sourceControl.getUser", gitlab.clone()),
    );
    assert_eq!(va["result"]["isConfigured"], json!(true), "{va}");
    assert_eq!(va["result"]["method"], json!("device"));
    assert_eq!(va["result"]["user"]["login"], json!("glab-octocat"));
    assert_eq!(vb["result"]["user"]["login"], json!("glab-octocat"), "{vb}");
    assert_eq!(
        mock.flags.refresh_exchanges.load(Ordering::SeqCst),
        1,
        "exactly one refresh exchange for two overlapping probes"
    );
    let secrets = read_secrets(&h.secrets_file);
    assert_eq!(
        secrets["sourceControl.gitlab.token"],
        json!(ROTATED_ACCESS_TOKEN),
        "{secrets}"
    );
    assert_eq!(
        secrets["sourceControl.gitlab.refreshToken"],
        json!(ROTATED_REFRESH_TOKEN)
    );

    // Still connected afterwards, and no `expired` was ever emitted: the
    // next auth-changed observed is the `revoked` this revoke triggers.
    let v = wss_rpc(&mut rpc, 11, "sourceControl.authStatus", gitlab.clone()).await;
    assert_eq!(v["result"]["isConfigured"], json!(true), "{v}");
    let v = wss_rpc(&mut rpc, 12, "sourceControl.revoke", gitlab).await;
    assert_eq!(v["result"], json!({ "ok": true }));
    let ev = await_auth_changed_matching(&mut sub, None, 15).await;
    assert_eq!(
        ev,
        json!({ "provider": "gitlab", "host": HOST, "status": "revoked" }),
        "no expired event may precede the deliberate revoke"
    );
}

/// `sourceControl.revoke` on the bound host with nothing stored — a fresh
/// daemon, a repeat revoke, or only a pending device grant — is a successful
/// no-op that ends no connection: no delete and no `revoked` event. Only a
/// stored credential's removal emits `revoked`, exactly once.
#[tokio::test]
async fn gitlab_revoke_without_a_connection_emits_nothing_over_wss() {
    let mock = spawn_mock_gitlab().await;
    let h = boot(&mock).await;
    let mut sub = subscriber(&h).await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    let gitlab = json!({ "provider": "gitlab" });
    let pat = json!({ "provider": "gitlab", "method": "pat", "token": PAT_TOKEN });

    // Fresh daemon: first and repeated revoke are ok and silent.
    for id in [10, 11] {
        let v = wss_rpc(&mut rpc, id, "sourceControl.revoke", gitlab.clone()).await;
        assert_eq!(v["result"], json!({ "ok": true }), "{v}");
    }
    // A pending grant without a stored credential: revoke aborts it, silently.
    let v = wss_rpc(&mut rpc, 12, "sourceControl.connect", gitlab.clone()).await;
    assert_eq!(v["result"]["userCode"], json!(USER_CODE), "{v}");
    let v = wss_rpc(&mut rpc, 13, "sourceControl.revoke", gitlab.clone()).await;
    assert_eq!(v["result"], json!({ "ok": true }));
    let v = wss_rpc(&mut rpc, 14, "sourceControl.authStatus", gitlab.clone()).await;
    assert_eq!(v["result"]["isConfigured"], json!(false), "{v}");
    assert_eq!(v["result"]["deviceFlow"], Value::Null);
    assert!(read_secrets(&h.secrets_file)
        .get("sourceControl.gitlab.token")
        .is_none());

    // The FIRST auth-changed ever observed is the one this connect triggers.
    let v = wss_rpc(&mut rpc, 15, "sourceControl.connect", pat.clone()).await;
    assert_eq!(v["result"], json!({ "ok": true, "method": "pat" }), "{v}");
    let ev = await_auth_changed_matching(&mut sub, None, 15).await;
    assert_eq!(
        ev,
        json!({ "provider": "gitlab", "host": HOST, "status": "authorized" }),
        "no revoked may be emitted while nothing is stored"
    );

    // A stored credential's revoke emits `revoked` once; the repeat emits
    // nothing — the next event observed is the following connect's.
    let v = wss_rpc(&mut rpc, 16, "sourceControl.revoke", gitlab.clone()).await;
    assert_eq!(v["result"], json!({ "ok": true }));
    let ev = await_auth_changed_matching(&mut sub, None, 15).await;
    assert_eq!(
        ev,
        json!({ "provider": "gitlab", "host": HOST, "status": "revoked" })
    );
    let v = wss_rpc(&mut rpc, 17, "sourceControl.revoke", gitlab).await;
    assert_eq!(v["result"], json!({ "ok": true }));
    let v = wss_rpc(&mut rpc, 18, "sourceControl.connect", pat).await;
    assert_eq!(v["result"], json!({ "ok": true, "method": "pat" }), "{v}");
    let ev = await_auth_changed_matching(&mut sub, None, 15).await;
    assert_eq!(
        ev,
        json!({ "provider": "gitlab", "host": HOST, "status": "authorized" }),
        "a repeated revoke emits nothing"
    );
}

/// Wait (bounded) for one of the mock's latches to wake.
async fn await_latch(latch: &Notify, what: &str) {
    timeout(Duration::from_secs(15), latch.notified())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for the mock to {what}"));
}

/// A PAT connect that lands while the device grant's authorize exchange is
/// in flight (the instance's response is delayed) must be the connection
/// that survives: at the previous head the late completion persisted its
/// pair over the PAT and, finding its slot gone, deleted the credential —
/// leaving nothing stored. Now the exchange, its persist and the slot
/// reconcile are one gated step: the completion lands first (its own
/// `authorized`), the PAT replaces the pair (a second `authorized`), and the
/// final state is the PAT — `method: "pat"`, no refresh / expiry metadata,
/// no late event flipping the connection.
#[tokio::test]
async fn gitlab_pat_connect_during_device_authorize_keeps_the_pat_over_wss() {
    let mock = spawn_mock_gitlab().await;
    let h = boot(&mock).await;
    let mut sub = subscriber(&h).await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    let gitlab = json!({ "provider": "gitlab" });
    let pat = json!({ "provider": "gitlab", "method": "pat", "token": PAT_TOKEN });

    let v = wss_rpc(&mut rpc, 10, "sourceControl.connect", gitlab.clone()).await;
    assert_eq!(v["result"]["userCode"], json!(USER_CODE), "{v}");

    // Authorize, but park the response: the daemon's completion is now in
    // flight, its persist pending on the held exchange.
    mock.flags.hold_authorize.store(true, Ordering::SeqCst);
    mock.flags.authorize.store(true, Ordering::SeqCst);
    await_latch(&mock.flags.authorize_held, "hold the authorize response").await;

    // The PAT connect runs concurrently (awaiting it before the release
    // would wait on the completion that waits on us). Its validation is the
    // first hit on the user endpoint; release the completion once the PAT
    // is that far, then await the PAT result.
    let mut pat_conn = connect_ws(h.port, h.cfg.clone()).await;
    let pat_params = pat.clone();
    let pat_connect = tokio::spawn(async move {
        wss_rpc(&mut pat_conn, 20, "sourceControl.connect", pat_params).await
    });
    await_latch(&mock.flags.user_hit, "receive the PAT validation").await;
    mock.flags.release_authorize.notify_one();
    let v = pat_connect.await.expect("pat connect task");
    assert_eq!(v["result"], json!({ "ok": true, "method": "pat" }), "{v}");

    // Both connections announce themselves, completion first (it held the
    // gate the PAT waited on), and the PAT is what remains.
    for _ in 0..2 {
        let ev = await_auth_changed(&mut sub, "authorized", 15).await;
        assert_eq!(
            ev,
            json!({ "provider": "gitlab", "host": HOST, "status": "authorized" })
        );
    }
    let secrets = read_secrets(&h.secrets_file);
    assert_eq!(
        secrets["sourceControl.gitlab.token"],
        json!(PAT_TOKEN),
        "the PAT survives the late device completion: {secrets}"
    );
    assert!(secrets.get("sourceControl.gitlab.refreshToken").is_none());
    assert!(secrets.get("sourceControl.gitlab.tokenExpiresAt").is_none());
    let v = wss_rpc(&mut rpc, 11, "sourceControl.authStatus", gitlab.clone()).await;
    let r = &v["result"];
    assert_eq!(r["isConfigured"], json!(true), "{r}");
    assert_eq!(r["method"], json!("pat"));
    assert_eq!(r["user"]["login"], json!("glab-octocat"));
    assert_eq!(
        r["deviceFlow"],
        Value::Null,
        "the completion cleared the slot"
    );
    assert_eq!(mock.flags.refresh_exchanges.load(Ordering::SeqCst), 0);

    // No late event flips the connection: the next auth-changed observed is
    // the `revoked` this deliberate revoke triggers, and it ends the PAT.
    let v = wss_rpc(&mut rpc, 12, "sourceControl.revoke", gitlab.clone()).await;
    assert_eq!(v["result"], json!({ "ok": true }));
    let ev = await_auth_changed_matching(&mut sub, None, 15).await;
    assert_eq!(
        ev,
        json!({ "provider": "gitlab", "host": HOST, "status": "revoked" }),
        "nothing but the revoke may follow the two connects"
    );
    assert!(read_secrets(&h.secrets_file)
        .get("sourceControl.gitlab.token")
        .is_none());
}

/// A probe that waited on the credential gate re-reads the host binding
/// before it touches the stored credential: while probe 1 sits in a held
/// refresh exchange, probe 2 queues behind it and `sourceControl.gitlab.host`
/// moves to another instance. Probe 2 must answer "not configured" without
/// sending the (now foreign) credential anywhere — one refresh exchange, one
/// user request in total — and the rotated pair stays stored, so binding the
/// host back reconnects without a further exchange and no `expired` is ever
/// emitted.
#[tokio::test]
async fn gitlab_probe_rereads_the_binding_after_waiting_over_wss() {
    let mock = spawn_mock_gitlab().await;
    mock.flags.short_lived.store(true, Ordering::SeqCst);
    let h = boot(&mock).await;
    let mut sub = subscriber(&h).await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    let gitlab = json!({ "provider": "gitlab" });
    let other_host = "gitlab.acme.internal";

    let v = wss_rpc(&mut rpc, 10, "sourceControl.connect", gitlab.clone()).await;
    assert_eq!(v["result"]["userCode"], json!(USER_CODE), "{v}");
    mock.flags.authorize.store(true, Ordering::SeqCst);
    let ev = await_auth_changed(&mut sub, "authorized", 30).await;
    assert_eq!(ev["status"], json!("authorized"));
    assert_eq!(mock.flags.user_requests.load(Ordering::SeqCst), 0);

    // Probe 1 refreshes the near-expiry pair; the exchange is parked, so it
    // holds the gate mid-rotation.
    mock.flags.hold_refresh.store(true, Ordering::SeqCst);
    let mut conn_a = connect_ws(h.port, h.cfg.clone()).await;
    let params = gitlab.clone();
    let probe1 =
        tokio::spawn(
            async move { wss_rpc(&mut conn_a, 20, "sourceControl.getUser", params).await },
        );
    await_latch(&mock.flags.refresh_held, "hold the refresh response").await;

    // Probe 2 (explicitly for the instance that is about to lose the
    // binding) queues behind the gate; the binding moves while it waits.
    let mut conn_b = connect_ws(h.port, h.cfg.clone()).await;
    let params = json!({ "provider": "gitlab", "host": HOST });
    let probe2 =
        tokio::spawn(
            async move { wss_rpc(&mut conn_b, 21, "sourceControl.authStatus", params).await },
        );
    let v = wss_rpc(
        &mut rpc,
        11,
        "settings.update",
        json!({ "changes": [{ "path": "sourceControl.gitlab.host", "value": other_host }] }),
    )
    .await;
    assert!(v.get("error").is_none(), "rebind: {v}");
    mock.flags.release_refresh.notify_one();

    let v1 = probe1.await.expect("probe 1 task");
    assert_eq!(v1["result"]["user"]["login"], json!("glab-octocat"), "{v1}");
    let v2 = probe2.await.expect("probe 2 task");
    let r = &v2["result"];
    assert_eq!(
        r["isConfigured"],
        json!(false),
        "unbound after the rebind: {r}"
    );
    assert_eq!(r["method"], Value::Null);
    assert!(r.get("user").is_none(), "{r}");
    assert_eq!(r["host"], json!(HOST));
    assert_eq!(mock.flags.refresh_exchanges.load(Ordering::SeqCst), 1);
    assert_eq!(
        mock.flags.user_requests.load(Ordering::SeqCst),
        1,
        "probe 2 sent the credential nowhere"
    );
    let secrets = read_secrets(&h.secrets_file);
    assert_eq!(
        secrets["sourceControl.gitlab.token"],
        json!(ROTATED_ACCESS_TOKEN)
    );
    assert_eq!(
        secrets["sourceControl.gitlab.refreshToken"],
        json!(ROTATED_REFRESH_TOKEN),
        "the rebind deleted nothing: {secrets}"
    );

    // Binding the host back reconnects on the rotated pair — no exchange.
    let v = wss_rpc(
        &mut rpc,
        12,
        "settings.update",
        json!({ "changes": [{ "path": "sourceControl.gitlab.host", "value": HOST }] }),
    )
    .await;
    assert!(v.get("error").is_none(), "rebind back: {v}");
    let v = wss_rpc(&mut rpc, 13, "sourceControl.authStatus", gitlab.clone()).await;
    assert_eq!(v["result"]["isConfigured"], json!(true), "{v}");
    assert_eq!(v["result"]["method"], json!("device"));
    assert_eq!(mock.flags.refresh_exchanges.load(Ordering::SeqCst), 1);

    // No `expired` was ever emitted: the next auth-changed is this revoke's.
    let v = wss_rpc(&mut rpc, 14, "sourceControl.revoke", gitlab).await;
    assert_eq!(v["result"], json!({ "ok": true }));
    let ev = await_auth_changed_matching(&mut sub, None, 15).await;
    assert_eq!(
        ev,
        json!({ "provider": "gitlab", "host": HOST, "status": "revoked" }),
        "no expired may precede the deliberate revoke"
    );
}

/// A revoke that waited on the credential gate re-reads the host binding
/// before it deletes: while the device completion for `gitlab.com` sits in a
/// held authorize exchange, `sourceControl.gitlab.host` moves to another
/// instance and `revoke(other)` is issued. At the previous head the revoke
/// snapshotted "other is bound" at resolve time, queued behind the gate, and
/// then deleted the pair the completion had just committed (and re-bound to
/// `gitlab.com`). Now the binding is read under the gate: the revoke is a
/// no-op for the unbound host — nothing deleted, no `revoked` — and the
/// device connection to `gitlab.com` survives.
#[tokio::test]
async fn gitlab_revoke_rereads_the_binding_after_waiting_over_wss() {
    let mock = spawn_mock_gitlab().await;
    let h = boot(&mock).await;
    let mut sub = subscriber(&h).await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    let gitlab = json!({ "provider": "gitlab" });
    let other_host = "gitlab.other.internal";

    let v = wss_rpc(&mut rpc, 10, "sourceControl.connect", gitlab.clone()).await;
    assert_eq!(v["result"]["userCode"], json!(USER_CODE), "{v}");

    // Authorize, but park the response: the completion holds the gate.
    mock.flags.hold_authorize.store(true, Ordering::SeqCst);
    mock.flags.authorize.store(true, Ordering::SeqCst);
    await_latch(&mock.flags.authorize_held, "hold the authorize response").await;

    // The binding moves, then a revoke for the newly bound instance queues
    // behind the held completion.
    let v = wss_rpc(
        &mut rpc,
        11,
        "settings.update",
        json!({ "changes": [{ "path": "sourceControl.gitlab.host", "value": other_host }] }),
    )
    .await;
    assert!(v.get("error").is_none(), "rebind: {v}");
    let mut conn = connect_ws(h.port, h.cfg.clone()).await;
    let params = json!({ "provider": "gitlab", "host": other_host });
    let revoke =
        tokio::spawn(async move { wss_rpc(&mut conn, 20, "sourceControl.revoke", params).await });
    mock.flags.release_authorize.notify_one();

    // The completion commits its pair and binds `gitlab.com` back; the
    // queued revoke then finds `other` unbound and deletes nothing.
    let ev = await_auth_changed(&mut sub, "authorized", 15).await;
    assert_eq!(
        ev,
        json!({ "provider": "gitlab", "host": HOST, "status": "authorized" })
    );
    let v = revoke.await.expect("revoke task");
    assert_eq!(v["result"], json!({ "ok": true }), "{v}");
    let secrets = read_secrets(&h.secrets_file);
    assert_eq!(
        secrets["sourceControl.gitlab.token"],
        json!(ACCESS_TOKEN),
        "the revoke of an unbound host deleted nothing: {secrets}"
    );
    let v = wss_rpc(&mut rpc, 12, "sourceControl.authStatus", gitlab.clone()).await;
    let r = &v["result"];
    assert_eq!(
        r["host"],
        json!(HOST),
        "the completion re-bound its host: {r}"
    );
    assert_eq!(r["isConfigured"], json!(true), "{r}");
    assert_eq!(r["method"], json!("device"));
    assert_eq!(r["user"]["login"], json!("glab-octocat"));

    // No `revoked` was emitted for the no-op: the next auth-changed is this
    // deliberate revoke's, for `gitlab.com`.
    let v = wss_rpc(&mut rpc, 13, "sourceControl.revoke", gitlab).await;
    assert_eq!(v["result"], json!({ "ok": true }));
    let ev = await_auth_changed_matching(&mut sub, None, 15).await;
    assert_eq!(
        ev,
        json!({ "provider": "gitlab", "host": HOST, "status": "revoked" }),
        "only the deliberate revoke may follow the authorize"
    );
}

/// A device flow cancelled while its authorize exchange is in flight must not
/// touch a credential connected before it: a PAT is connected, a device grant
/// is started for the same host, the instance's authorize response is parked,
/// `cancelAuth` clears the slot, and the response is released. At the
/// previous head the late completion persisted its pair over the PAT and,
/// finding its slot gone, deleted the credential — leaving nothing stored. Now
/// the grant is committed only by a still-resident flow: the cancelled
/// completion drops it, the PAT stays exactly as it was, and no `authorized`
/// is emitted for the discarded grant.
#[tokio::test]
async fn gitlab_cancel_during_device_authorize_keeps_the_pat_over_wss() {
    let mock = spawn_mock_gitlab().await;
    let h = boot(&mock).await;
    let mut sub = subscriber(&h).await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    let gitlab = json!({ "provider": "gitlab" });

    let v = wss_rpc(
        &mut rpc,
        10,
        "sourceControl.connect",
        json!({ "provider": "gitlab", "method": "pat", "token": PAT_TOKEN }),
    )
    .await;
    assert_eq!(v["result"], json!({ "ok": true, "method": "pat" }), "{v}");
    let ev = await_auth_changed(&mut sub, "authorized", 15).await;
    assert_eq!(ev["status"], json!("authorized"));
    let pat_user_requests = mock.flags.user_requests.load(Ordering::SeqCst);

    // A device grant for the same host, authorized with the response parked.
    let v = wss_rpc(&mut rpc, 11, "sourceControl.connect", gitlab.clone()).await;
    assert_eq!(v["result"]["userCode"], json!(USER_CODE), "{v}");
    mock.flags.hold_authorize.store(true, Ordering::SeqCst);
    mock.flags.authorize.store(true, Ordering::SeqCst);
    await_latch(&mock.flags.authorize_held, "hold the authorize response").await;

    // Cancel while the completion is in flight (cancel does not wait on the
    // gate), then let the instance answer.
    let v = wss_rpc(&mut rpc, 12, "sourceControl.cancelAuth", gitlab.clone()).await;
    assert_eq!(v["result"], json!({ "ok": true, "cancelled": true }), "{v}");
    mock.flags.release_authorize.notify_one();

    // authStatus queues behind the completion's gate hold, so once it
    // answers the completion has finished — and the PAT is what remains.
    let v = wss_rpc(&mut rpc, 13, "sourceControl.authStatus", gitlab.clone()).await;
    let r = &v["result"];
    assert_eq!(r["isConfigured"], json!(true), "{r}");
    assert_eq!(r["method"], json!("pat"));
    assert_eq!(r["user"]["login"], json!("glab-octocat"));
    assert_eq!(r["deviceFlow"], Value::Null, "the cancel cleared the slot");
    assert_eq!(
        mock.flags.user_requests.load(Ordering::SeqCst),
        pat_user_requests + 1,
        "the probe validated the PAT once"
    );
    let secrets = read_secrets(&h.secrets_file);
    assert_eq!(
        secrets["sourceControl.gitlab.token"],
        json!(PAT_TOKEN),
        "the PAT survives the cancelled device completion: {secrets}"
    );
    assert!(secrets.get("sourceControl.gitlab.refreshToken").is_none());
    assert!(secrets.get("sourceControl.gitlab.tokenExpiresAt").is_none());

    // The discarded grant announced nothing: the next auth-changed is the
    // `revoked` this deliberate revoke triggers.
    let v = wss_rpc(&mut rpc, 14, "sourceControl.revoke", gitlab).await;
    assert_eq!(v["result"], json!({ "ok": true }));
    let ev = await_auth_changed_matching(&mut sub, None, 15).await;
    assert_eq!(
        ev,
        json!({ "provider": "gitlab", "host": HOST, "status": "revoked" }),
        "nothing but the revoke may follow the PAT connect"
    );
}

// ---------------------------------------------------------------------------
// Pairwise interleavings of the gated credential mutations.

/// Another GitLab instance; never reaches the mock unless it is bound (only
/// the bound host gets the API-origin override), so the cells that connect
/// it rebind first.
const OTHER: &str = "gitlab.other.internal";

/// The gated exchange parked (its response held by the mock) while the
/// interleaved actions land — the "first mutation" of a pair, waiting.
#[derive(Clone, Copy, Debug)]
enum Holder {
    /// The device completion for `HOST` is inside its authorize exchange;
    /// nothing is stored yet.
    Authorize,
    /// A probe of the bound `HOST` is inside the refresh exchange rotating
    /// its near-expiry device pair (`ACCESS_TOKEN` stored, its `authorized`
    /// already consumed).
    Refresh,
    /// The device flow for `HOST` is inside a pending poll; nothing is
    /// stored. Once the actions landed the instance authorizes the grant, so
    /// the cell shows whether the flow still acts on it.
    PendingPoll,
}

/// The "second mutation": issued while the holder is parked, in order.
#[derive(Clone, Copy, Debug)]
enum Action {
    /// `sourceControl.connect { method: "pat" }` for the host (gated).
    PatConnect(&'static str),
    /// `sourceControl.revoke` for the host (gated).
    Revoke(&'static str),
    /// `sourceControl.cancelAuth` for the host (slot only; never waits),
    /// expected to answer `cancelled`.
    Cancel(&'static str, bool),
    /// `sourceControl.authStatus` for the host (gated), expected to answer
    /// `isConfigured`.
    Probe(&'static str, bool),
    /// `settings.update sourceControl.gitlab.host` (not gated: applies now).
    Rebind(&'static str),
    /// `settings.update sourceControl.gitlab.token = PAT_TOKEN` (gated: the
    /// PAT and its sibling cleanup land after the holder, never under it).
    SettingsPat,
    /// `settings.reset sourceControl.gitlab.token` (gated: the token and its
    /// siblings are cleared after the holder committed, never under it).
    SettingsReset,
    /// `settings.get` + `settings.update` of `git.autoCommit` (not gated, and
    /// not held up by a gated settings write queued ahead of it: the credential
    /// gate is taken BEFORE the revision gate, so a parked holder only delays
    /// GitLab token batches, never unrelated settings traffic).
    UnrelatedSettings,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stored {
    Nothing,
    Device(&'static str),
    Pat,
}

struct Case {
    name: &'static str,
    holder: Holder,
    actions: &'static [Action],
    /// The credential left in the secrets file.
    stored: Stored,
    /// `sourceControl.gitlab.host` afterwards.
    bound: &'static str,
    /// Every `sourceControl:auth-changed` `(host, status)` emitted after the
    /// holder was parked, in order — and nothing else.
    events: &'static [(&'static str, &'static str)],
    /// The device flow for `HOST` was superseded: its poll task exits (or
    /// drops an authorized grant) without committing, which the daemon logs.
    flow_superseded: bool,
    /// Refresh exchanges the mock served in total.
    refreshes: usize,
}

const AUTHORIZED_HOST: (&str, &str) = (HOST, "authorized");
const AUTHORIZED_OTHER: (&str, &str) = (OTHER, "authorized");

static INTERLEAVINGS: &[Case] = &[
    // --- a device completion holds the gate ---------------------------------
    Case {
        name: "authorize ∥ pat connect (same host): both commit, PAT last",
        holder: Holder::Authorize,
        actions: &[Action::PatConnect(HOST)],
        stored: Stored::Pat,
        bound: HOST,
        events: &[AUTHORIZED_HOST, AUTHORIZED_HOST],
        flow_superseded: false,
        refreshes: 0,
    },
    Case {
        name: "authorize ∥ rebind + pat connect (other): PAT wins, binds other",
        holder: Holder::Authorize,
        actions: &[Action::Rebind(OTHER), Action::PatConnect(OTHER)],
        stored: Stored::Pat,
        bound: OTHER,
        events: &[AUTHORIZED_HOST, AUTHORIZED_OTHER],
        flow_superseded: false,
        refreshes: 0,
    },
    Case {
        name: "authorize ∥ settings pat: completion commits, the settings PAT replaces it",
        holder: Holder::Authorize,
        actions: &[Action::SettingsPat],
        stored: Stored::Pat,
        bound: HOST,
        events: &[AUTHORIZED_HOST],
        flow_superseded: false,
        refreshes: 0,
    },
    Case {
        name: "authorize ∥ settings reset: completion commits, the reset clears the pair",
        holder: Holder::Authorize,
        actions: &[Action::SettingsReset],
        stored: Stored::Nothing,
        bound: HOST,
        events: &[AUTHORIZED_HOST],
        flow_superseded: false,
        refreshes: 0,
    },
    Case {
        name: "authorize ∥ settings pat + unrelated settings: unrelated traffic is not held up",
        holder: Holder::Authorize,
        actions: &[Action::SettingsPat, Action::UnrelatedSettings],
        stored: Stored::Pat,
        bound: HOST,
        events: &[AUTHORIZED_HOST],
        flow_superseded: false,
        refreshes: 0,
    },
    Case {
        name: "authorize ∥ revoke (same host): the committed pair is revoked",
        holder: Holder::Authorize,
        actions: &[Action::Revoke(HOST)],
        stored: Stored::Nothing,
        bound: HOST,
        events: &[AUTHORIZED_HOST, (HOST, "revoked")],
        flow_superseded: false,
        refreshes: 0,
    },
    Case {
        name: "authorize ∥ rebind + revoke (other): completion re-binds, revoke is a no-op",
        holder: Holder::Authorize,
        actions: &[Action::Rebind(OTHER), Action::Revoke(OTHER)],
        stored: Stored::Device(ACCESS_TOKEN),
        bound: HOST,
        events: &[AUTHORIZED_HOST],
        flow_superseded: false,
        refreshes: 0,
    },
    Case {
        name: "authorize ∥ cancel (same host): the grant is dropped uncommitted",
        holder: Holder::Authorize,
        actions: &[Action::Cancel(HOST, true)],
        stored: Stored::Nothing,
        bound: HOST,
        events: &[],
        flow_superseded: true,
        refreshes: 0,
    },
    Case {
        name: "authorize ∥ cancel (other): nothing to cancel, completion commits",
        holder: Holder::Authorize,
        actions: &[Action::Cancel(OTHER, false)],
        stored: Stored::Device(ACCESS_TOKEN),
        bound: HOST,
        events: &[AUTHORIZED_HOST],
        flow_superseded: false,
        refreshes: 0,
    },
    Case {
        name: "authorize ∥ probe (same host): the probe sees the committed pair",
        holder: Holder::Authorize,
        actions: &[Action::Probe(HOST, true)],
        stored: Stored::Device(ACCESS_TOKEN),
        bound: HOST,
        events: &[AUTHORIZED_HOST],
        flow_superseded: false,
        refreshes: 0,
    },
    Case {
        name: "authorize ∥ rebind + probe (other): completion re-binds, other is unbound",
        holder: Holder::Authorize,
        actions: &[Action::Rebind(OTHER), Action::Probe(OTHER, false)],
        stored: Stored::Device(ACCESS_TOKEN),
        bound: HOST,
        events: &[AUTHORIZED_HOST],
        flow_superseded: false,
        refreshes: 0,
    },
    // --- a refresh rotation holds the gate ----------------------------------
    Case {
        name: "refresh ∥ pat connect (same host): rotation commits, PAT replaces it",
        holder: Holder::Refresh,
        actions: &[Action::PatConnect(HOST)],
        stored: Stored::Pat,
        bound: HOST,
        events: &[AUTHORIZED_HOST],
        flow_superseded: false,
        refreshes: 1,
    },
    Case {
        name: "refresh ∥ settings pat: rotation commits, the settings PAT replaces it",
        holder: Holder::Refresh,
        actions: &[Action::SettingsPat],
        stored: Stored::Pat,
        bound: HOST,
        events: &[],
        flow_superseded: false,
        refreshes: 1,
    },
    Case {
        name: "refresh ∥ settings reset: rotation commits, the reset clears the pair",
        holder: Holder::Refresh,
        actions: &[Action::SettingsReset],
        stored: Stored::Nothing,
        bound: HOST,
        events: &[],
        flow_superseded: false,
        refreshes: 1,
    },
    Case {
        name: "refresh ∥ settings reset + unrelated settings: unrelated traffic is not held up",
        holder: Holder::Refresh,
        actions: &[Action::SettingsReset, Action::UnrelatedSettings],
        stored: Stored::Nothing,
        bound: HOST,
        events: &[],
        flow_superseded: false,
        refreshes: 1,
    },
    Case {
        name: "refresh ∥ revoke (same host): the rotated pair is revoked",
        holder: Holder::Refresh,
        actions: &[Action::Revoke(HOST)],
        stored: Stored::Nothing,
        bound: HOST,
        events: &[(HOST, "revoked")],
        flow_superseded: false,
        refreshes: 1,
    },
    Case {
        name: "refresh ∥ cancel (same host): no flow, rotation stands",
        holder: Holder::Refresh,
        actions: &[Action::Cancel(HOST, false)],
        stored: Stored::Device(ROTATED_ACCESS_TOKEN),
        bound: HOST,
        events: &[],
        flow_superseded: false,
        refreshes: 1,
    },
    Case {
        name: "refresh ∥ probe (same host): the second probe reuses the rotation",
        holder: Holder::Refresh,
        actions: &[Action::Probe(HOST, true)],
        stored: Stored::Device(ROTATED_ACCESS_TOKEN),
        bound: HOST,
        events: &[],
        flow_superseded: false,
        refreshes: 1,
    },
    Case {
        name: "refresh ∥ rebind + probe (same host): rotation commits, host now unbound",
        holder: Holder::Refresh,
        actions: &[Action::Rebind(OTHER), Action::Probe(HOST, false)],
        stored: Stored::Device(ROTATED_ACCESS_TOKEN),
        bound: OTHER,
        events: &[],
        flow_superseded: false,
        refreshes: 1,
    },
    // --- a pending poll holds the gate; the grant is authorized afterwards --
    Case {
        name: "pending ∥ pat connect (same host): flow superseded, grant never committed",
        holder: Holder::PendingPoll,
        actions: &[Action::PatConnect(HOST)],
        stored: Stored::Pat,
        bound: HOST,
        events: &[AUTHORIZED_HOST],
        flow_superseded: true,
        refreshes: 0,
    },
    Case {
        name: "pending ∥ rebind + pat connect (other): flow superseded, PAT for other stays",
        holder: Holder::PendingPoll,
        actions: &[Action::Rebind(OTHER), Action::PatConnect(OTHER)],
        stored: Stored::Pat,
        bound: OTHER,
        events: &[AUTHORIZED_OTHER],
        flow_superseded: true,
        refreshes: 0,
    },
    Case {
        name: "pending ∥ revoke (same host): flow aborted, nothing to revoke",
        holder: Holder::PendingPoll,
        actions: &[Action::Revoke(HOST)],
        stored: Stored::Nothing,
        bound: HOST,
        events: &[],
        flow_superseded: true,
        refreshes: 0,
    },
    Case {
        name: "pending ∥ rebind + revoke (other): flow survives and completes",
        holder: Holder::PendingPoll,
        actions: &[Action::Rebind(OTHER), Action::Revoke(OTHER)],
        stored: Stored::Device(ACCESS_TOKEN),
        bound: HOST,
        events: &[AUTHORIZED_HOST],
        flow_superseded: false,
        refreshes: 0,
    },
    Case {
        name: "pending ∥ cancel (same host): flow aborted",
        holder: Holder::PendingPoll,
        actions: &[Action::Cancel(HOST, true)],
        stored: Stored::Nothing,
        bound: HOST,
        events: &[],
        flow_superseded: true,
        refreshes: 0,
    },
    Case {
        name: "pending ∥ cancel (other): flow survives and completes",
        holder: Holder::PendingPoll,
        actions: &[Action::Cancel(OTHER, false)],
        stored: Stored::Device(ACCESS_TOKEN),
        bound: HOST,
        events: &[AUTHORIZED_HOST],
        flow_superseded: false,
        refreshes: 0,
    },
];

/// Poll the daemon's stderr log until it contains `marker`.
async fn await_log_marker(log: &Path, marker: &str, what: &str) {
    timeout(Duration::from_secs(15), async {
        loop {
            if std::fs::read_to_string(log).is_ok_and(|s| s.contains(marker)) {
                return;
            }
            // timing-guard: poll interval
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}: daemon log never reported {marker:?}"));
}

fn stored_in(secrets: &Value) -> Stored {
    let token = secrets["sourceControl.gitlab.token"].as_str();
    let refresh = secrets.get("sourceControl.gitlab.refreshToken").is_some();
    let expiry = secrets.get("sourceControl.gitlab.tokenExpiresAt").is_some();
    match token {
        None => {
            assert!(!refresh && !expiry, "orphaned device keys: {secrets}");
            Stored::Nothing
        }
        Some(PAT_TOKEN) => {
            assert!(!refresh && !expiry, "device keys next to a PAT: {secrets}");
            Stored::Pat
        }
        Some(ACCESS_TOKEN) => {
            assert!(refresh && expiry, "device pair incomplete: {secrets}");
            Stored::Device(ACCESS_TOKEN)
        }
        Some(ROTATED_ACCESS_TOKEN) => {
            assert!(refresh && expiry, "device pair incomplete: {secrets}");
            Stored::Device(ROTATED_ACCESS_TOKEN)
        }
        Some(other) => panic!("unexpected stored token {other:?}: {secrets}"),
    }
}

async fn run_interleaving(case: &'static Case) {
    let name = case.name;
    let mock = spawn_mock_gitlab().await;
    if matches!(case.holder, Holder::Refresh) {
        mock.flags.short_lived.store(true, Ordering::SeqCst);
    }
    let h = boot(&mock).await;
    let mut sub = subscriber(&h).await;
    let mut rpc = connect_ws(h.port, h.cfg.clone()).await;
    let gitlab = json!({ "provider": "gitlab" });

    let v = wss_rpc(&mut rpc, 10, "sourceControl.connect", gitlab.clone()).await;
    assert_eq!(v["result"]["userCode"], json!(USER_CODE), "{name}: {v}");

    // Park the holder inside its gated exchange.
    let mut held_probe = None;
    match case.holder {
        Holder::Authorize => {
            mock.flags.hold_authorize.store(true, Ordering::SeqCst);
            mock.flags.authorize.store(true, Ordering::SeqCst);
            await_latch(&mock.flags.authorize_held, name).await;
        }
        Holder::Refresh => {
            mock.flags.authorize.store(true, Ordering::SeqCst);
            let ev = await_auth_changed(&mut sub, "authorized", 30).await;
            assert_eq!(ev["host"], json!(HOST), "{name}: {ev}");
            mock.flags.hold_refresh.store(true, Ordering::SeqCst);
            let mut conn = connect_ws(h.port, h.cfg.clone()).await;
            let params = gitlab.clone();
            held_probe = Some(tokio::spawn(async move {
                wss_rpc(&mut conn, 30, "sourceControl.authStatus", params).await
            }));
            await_latch(&mock.flags.refresh_held, name).await;
        }
        Holder::PendingPoll => {
            mock.flags.hold_poll.store(true, Ordering::SeqCst);
            await_latch(&mock.flags.poll_held, name).await;
        }
    }

    // Land the actions: gated ones queue behind the holder, so they run on
    // their own connections and are awaited after the release.
    let mut queued = Vec::new();
    for (i, action) in case.actions.iter().enumerate() {
        let id = 40 + i64::try_from(i).expect("small index");
        match *action {
            Action::Rebind(host) => {
                let v = wss_rpc(
                    &mut rpc,
                    id,
                    "settings.update",
                    json!({ "changes": [{ "path": "sourceControl.gitlab.host", "value": host }] }),
                )
                .await;
                assert!(v.get("error").is_none(), "{name}: rebind: {v}");
            }
            Action::Cancel(host, cancelled) => {
                let v = wss_rpc(
                    &mut rpc,
                    id,
                    "sourceControl.cancelAuth",
                    json!({ "provider": "gitlab", "host": host }),
                )
                .await;
                assert_eq!(
                    v["result"],
                    json!({ "ok": true, "cancelled": cancelled }),
                    "{name}: cancel: {v}"
                );
            }
            Action::PatConnect(host) | Action::Revoke(host) | Action::Probe(host, _) => {
                let (method, params) = match *action {
                    Action::PatConnect(_) => (
                        "sourceControl.connect",
                        json!({ "provider": "gitlab", "host": host, "method": "pat", "token": PAT_TOKEN }),
                    ),
                    Action::Revoke(_) => (
                        "sourceControl.revoke",
                        json!({ "provider": "gitlab", "host": host }),
                    ),
                    _ => (
                        "sourceControl.authStatus",
                        json!({ "provider": "gitlab", "host": host }),
                    ),
                };
                let mut conn = connect_ws(h.port, h.cfg.clone()).await;
                queued.push((
                    *action,
                    tokio::spawn(async move { wss_rpc(&mut conn, id, method, params).await }),
                ));
            }
            Action::SettingsPat | Action::SettingsReset => {
                let (method, params) = match *action {
                    Action::SettingsPat => (
                        "settings.update",
                        json!({ "changes": [{ "path": "sourceControl.gitlab.token", "value": PAT_TOKEN }] }),
                    ),
                    _ => (
                        "settings.reset",
                        json!({ "path": "sourceControl.gitlab.token" }),
                    ),
                };
                let mut conn = connect_ws(h.port, h.cfg.clone()).await;
                let mut handle =
                    tokio::spawn(async move { wss_rpc(&mut conn, id, method, params).await });
                // The settings write queues behind the holder: it must not
                // answer while the holder is parked (a bounded wait in the
                // safe direction — the gate never lets it through early), and
                // the PAT must not be in the secrets file.
                assert!(
                    timeout(Duration::from_millis(750), &mut handle)
                        .await
                        .is_err(),
                    "{name}: {action:?} landed while the gate was held"
                );
                assert_ne!(
                    read_secrets(&h.secrets_file)["sourceControl.gitlab.token"],
                    json!(PAT_TOKEN),
                    "{name}: settings PAT stored under a held gate"
                );
                queued.push((*action, handle));
            }
            Action::UnrelatedSettings => {
                // Issued while the holder is parked AND a gated settings
                // write is queued behind it: neither may hold up unrelated
                // settings traffic. A bounded wait in the unsafe direction is
                // the point here — under the inverted lock order the queued
                // write would sit on the revision gate while waiting for the
                // credential gate, and every settings.get / update would hang
                // behind it.
                let unrelated = async {
                    let v = wss_rpc(
                        &mut rpc,
                        id,
                        "settings.get",
                        json!({ "path": "git.autoCommit" }),
                    )
                    .await;
                    assert!(v.get("error").is_none(), "{name}: unrelated get: {v}");
                    let v = wss_rpc(
                        &mut rpc,
                        id + 100,
                        "settings.update",
                        json!({ "changes": [{ "path": "git.autoCommit", "value": true }] }),
                    )
                    .await;
                    assert!(v.get("error").is_none(), "{name}: unrelated update: {v}");
                };
                assert!(
                    timeout(Duration::from_secs(5), unrelated).await.is_ok(),
                    "{name}: unrelated settings traffic waited behind the credential gate"
                );
                assert_ne!(
                    read_secrets(&h.secrets_file)["sourceControl.gitlab.token"],
                    json!(PAT_TOKEN),
                    "{name}: settings PAT stored under a held gate"
                );
            }
        }
    }

    // Let the holder finish, then collect what the queued actions answered.
    match case.holder {
        Holder::Authorize => mock.flags.release_authorize.notify_one(),
        Holder::Refresh => mock.flags.release_refresh.notify_one(),
        Holder::PendingPoll => mock.flags.release_poll.notify_one(),
    }
    for (action, handle) in queued {
        let v = handle.await.expect("queued action task");
        match action {
            Action::PatConnect(_) => {
                assert_eq!(
                    v["result"],
                    json!({ "ok": true, "method": "pat" }),
                    "{name}: {action:?}: {v}"
                );
            }
            Action::Revoke(_) => {
                assert_eq!(
                    v["result"],
                    json!({ "ok": true }),
                    "{name}: {action:?}: {v}"
                );
            }
            Action::Probe(_, configured) => {
                assert_eq!(
                    v["result"]["isConfigured"],
                    json!(configured),
                    "{name}: {action:?}: {v}"
                );
            }
            Action::SettingsPat | Action::SettingsReset => {
                assert!(v.get("error").is_none(), "{name}: {action:?}: {v}");
            }
            Action::Cancel(..) | Action::Rebind(_) | Action::UnrelatedSettings => {
                unreachable!()
            }
        }
    }
    if let Some(handle) = held_probe {
        // The parked probe checked the binding before its exchange and
        // answers for the pair it rotated.
        let v = handle.await.expect("held probe task");
        assert_eq!(v["result"]["isConfigured"], json!(true), "{name}: {v}");
        assert_eq!(v["result"]["method"], json!("device"), "{name}: {v}");
    }
    if matches!(case.holder, Holder::PendingPoll) {
        // The instance authorizes the grant now: a flow still resident
        // commits it at its next tick (its `authorized` is in the expected
        // events); a superseded one exits without an exchange.
        mock.flags.authorize.store(true, Ordering::SeqCst);
    }
    if case.flow_superseded {
        await_log_marker(&h.log_file, "gitlab device grant superseded", name).await;
    }

    // The expected events, in order. Every emitter has either answered its
    // RPC or (a completion) is awaited here, so what follows is settled.
    for (host, status) in case.events {
        let ev = await_auth_changed_matching(&mut sub, None, 15).await;
        assert_eq!(
            ev,
            json!({ "provider": "gitlab", "host": host, "status": status }),
            "{name}: events {:?}",
            case.events
        );
    }

    // A probe of the bound host queues behind whatever is still in flight
    // (the completion after a cancel, which did not wait) and reports the
    // binding; the secrets file is read once it answered.
    let v = wss_rpc(&mut rpc, 60, "sourceControl.authStatus", gitlab.clone()).await;
    let r = &v["result"];
    assert_eq!(r["host"], json!(case.bound), "{name}: bound host: {r}");
    let secrets = read_secrets(&h.secrets_file);
    assert_eq!(
        stored_in(&secrets),
        case.stored,
        "{name}: stored: {secrets}"
    );
    let expected_method = match case.stored {
        Stored::Nothing => Value::Null,
        Stored::Device(_) => json!("device"),
        Stored::Pat => json!("pat"),
    };
    assert_eq!(r["method"], expected_method, "{name}: method: {r}");
    assert_eq!(
        mock.flags.refresh_exchanges.load(Ordering::SeqCst),
        case.refreshes,
        "{name}: refresh exchanges"
    );

    // Nothing beyond the expected events: a sentinel PAT connect on the
    // bound host must be the next auth-changed observed.
    let v = wss_rpc(
        &mut rpc,
        61,
        "sourceControl.connect",
        json!({ "provider": "gitlab", "method": "pat", "token": PAT_TOKEN }),
    )
    .await;
    assert_eq!(
        v["result"],
        json!({ "ok": true, "method": "pat" }),
        "{name}: sentinel: {v}"
    );
    let ev = await_auth_changed_matching(&mut sub, None, 15).await;
    assert_eq!(
        ev,
        json!({ "provider": "gitlab", "host": case.bound, "status": "authorized" }),
        "{name}: an event beyond {:?} was emitted",
        case.events
    );
}

/// Every credential mutation (PAT connect, device completion, cancelAuth,
/// revoke, refresh rotation, a `settings.update` / `settings.reset` of the
/// token — intentd#2042 review) takes the credential gate, re-reads the host
/// binding and the generation it was started for (slot residency, the stored
/// token) inside the hold, acts only while that still holds, and commits the
/// store write, the slot / binding update and the event in the same hold.
/// This table pairs each gated exchange that can be parked mid-flight
/// ([`Holder`]) with every other mutation for the same or another host
/// ([`Action`]) and pins the stored credential, the bound host and the exact
/// event stream each interleaving leaves behind. Cells run a few at a time,
/// each against its own daemon and mock.
#[tokio::test]
async fn gitlab_gated_mutation_interleavings_over_wss() {
    let permits = Arc::new(tokio::sync::Semaphore::new(4));
    let mut handles = Vec::new();
    for case in INTERLEAVINGS {
        let permits = permits.clone();
        handles.push((
            case.name,
            tokio::spawn(async move {
                let _permit = permits.acquire_owned().await.expect("semaphore");
                run_interleaving(case).await;
            }),
        ));
    }
    let mut failed = Vec::new();
    for (name, handle) in handles {
        if let Err(e) = handle.await {
            failed.push(format!("{name}: {e}"));
        }
    }
    assert!(
        failed.is_empty(),
        "failed interleavings:\n{}",
        failed.join("\n")
    );
}
