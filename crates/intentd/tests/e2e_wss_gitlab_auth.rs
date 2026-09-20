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

use std::net::Ipv4Addr;
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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
    let mut cmd = common::serve_command();
    cmd.env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        // The GitLab resolution chain falls back to `GITLAB_TOKEN`; strip it
        // so `isConfigured` reflects only the daemon's own secrets file.
        .env_remove("GITLAB_TOKEN")
        .env_remove("GITHUB_TOKEN")
        .env_remove("GH_TOKEN")
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
// a `grant_type=refresh_token` exchange rotates the pair exactly once (only
// `REFRESH_TOKEN` is accepted; a rotated one is `invalid_grant`). The user
// endpoint accepts the minted / rotated token or `PAT_TOKEN` as bearer and
// answers 401 for anything else (`reject_rotated` revokes the rotated token
// server-side). With `unsupported` set, the device endpoint answers 404 (a
// GitLab < 17.1 instance).
// ---------------------------------------------------------------------------

#[derive(Default)]
struct MockFlags {
    authorize: AtomicBool,
    unsupported: AtomicBool,
    short_lived: AtomicBool,
    reject_rotated: AtomicBool,
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
        || (bearer == ROTATED_ACCESS_TOKEN && !flags.reject_rotated.load(Ordering::SeqCst));
    let (status, body) = match (method, path.split('?').next().unwrap_or_default()) {
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
            if is_refresh && form_field("refresh_token").as_deref() == Some(REFRESH_TOKEN) =>
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
        ("GET", "/api/v4/user") => (401, json!({ "message": "401 Unauthorized" })),
        _ => (404, json!({ "error": "not_found" })),
    };
    let payload = body.to_string();
    let response = format!(
        "HTTP/1.1 {status} {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        if status == 200 { "OK" } else { "Error" },
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
    port: u16,
    cfg: Arc<ClientConfig>,
}

async fn boot(mock: &MockGitlab) -> Harness {
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let secrets_file = data_dir.join("secrets.json");
    let secrets_s = secrets_file.to_string_lossy().to_string();
    let env: [(&str, &str); 3] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_SECRETS_FILE", &secrets_s),
        ("INTENTD_GITLAB_API_BASE_URI", &mock.base_uri),
    ];
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
