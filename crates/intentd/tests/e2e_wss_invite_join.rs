//! WSS end-to-end for invite links and the identity-only join (multiplayer
//! w4): `workspace.invite.create` (pinned to a login) → the invitee joins by
//! gist identity proof over the unauthenticated `/invite` endpoint:
//! `invite.challenge` issues a nonce, the mock GitHub serves a gist carrying
//! it, `invite.prove` verifies it → a proof by the wrong account is refused
//! (`invite-pin-mismatch`) → the pinned account proves and receives its own
//! credential → that credential connects to `/ws`, `principal.me` shows the
//! GitHub identity and `workspace.get` the collaborator role → the same
//! guest previews a second workspace's link with `invite.inspect` and joins
//! it with `invite.accept` on that credential (no GitHub call; the presented
//! credential is rotated out) → a third workspace is joined by gist proof
//! again, exercising every proof refusal → the owner's `github.connect`
//! authorised as a different account is refused (`identity-locked`) while
//! the guest depends on the primary identity → the owner removes the member →
//! `workspace.get` is `NotFound` → `principal.revokeSelf` closes the
//! connection and the credential no longer authenticates.
//!
//! Boots a real `intentd serve` whose GitHub login host AND API host are
//! pointed at one local mock (`INTENTD_GITHUB_LOGIN_BASE_URI` /
//! `INTENTD_GITHUB_API_BASE_URI`). The owner's identity comes from a fake
//! `GITHUB_TOKEN` the mock recognises; the owner's `github.connect` runs the
//! mock's device flow; the invitee never authenticates to GitHub — the proof
//! gists and the accounts they name are scripted by the test
//! (`GET /gists/{id}`, `GET /users/{login}`). Hermetic: no live network, and
//! no guest token exists to leak into the daemon's secrets file.

#![cfg(unix)]

mod common;

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};
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
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

/// Tokens the mock GitHub recognises on `GET /user`, and the accounts they
/// resolve to. The owner's is handed to the daemon as `GITHUB_TOKEN`; the
/// intruder's is minted by the mock's device flow for the owner's
/// `github.connect` in step 6b.
const OWNER_TOKEN: &str = "gho_e2e_owner_token";
const INTRUDER_TOKEN: &str = "gho_e2e_intruder_token";
const OWNER_ID: u64 = 100;
const GUEST_ID: u64 = 200;
const INTRUDER_ID: u64 = 300;
const USER_CODE: &str = "JOIN-0001";

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
    common::test_tempdir_in("/tmp", "itd-wss-invite-")
}

/// A fake `tailcat` sidecar so the daemon reports a tunnel address: the
/// loopback-default bind advertises no LAN host, and an invite link needs at
/// least one dialable route (same seam as the pairing e2e).
fn write_fake_tailcat(dir: &Path) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join("fake-tailcat.sh");
    let script = r#"#!/bin/sh
key=""
for arg in "$@"; do
  case "$arg" in
    --key=*) key="${arg#--key=}" ;;
  esac
done
case "$1" in
  genkey)
    printf 'key-%s' $$ > "$key"
    ;;
  serve)
    printf '{"listenAddr":"tc-%s"}\n' "$(cat "$key")"
    sleep 600
    ;;
esac
"#;
    std::fs::write(&path, script).expect("write fake tailcat");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
        .expect("chmod fake tailcat");
    path
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> GuardedChild {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    std::fs::write(
        data_dir.join("config.toml"),
        "[server.tunnel]\nenabled = true\n",
    )
    .expect("seed config.toml with server.tunnel.enabled");
    common::enable_ws_api(data_dir);
    let mut cmd = common::serve_command();
    cmd.env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
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

async fn connect_ws(port: u16, cfg: Arc<ClientConfig>, token: &str) -> Ws {
    let url = format!("wss://localhost:{port}/ws?token={token}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// The unauthenticated invite endpoint: no token anywhere.
async fn connect_invite(port: u16, cfg: Arc<ClientConfig>) -> Ws {
    let url = format!("wss://localhost:{port}/invite");
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
        let next = timeout(Duration::from_secs(30), ws.next())
            .await
            .unwrap_or_else(|_| panic!("wss rpc {method} timed out"));
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
            other => panic!("{method}: expected text frame, got {other:?}"),
        }
    }
}

/// One `/invite` round-trip that waits out the listener-wide start throttle
/// (§ step 9): a request refused `invite-flow-busy` is retried until it is
/// admitted — the bucket restores one token per 5 s — bounded by a deadline.
/// For the steps whose assertions are about the request itself, not the
/// throttle.
async fn admitted_rpc(ws: &mut Ws, id: i64, method: &str, params: Value) -> Value {
    timeout(Duration::from_secs(90), async {
        loop {
            let v = wss_rpc(ws, id, method, params.clone()).await;
            if v["error"]["data"]["code"] != json!("invite-flow-busy") {
                return v;
            }
            // timing-guard: poll interval (the start throttle refills one token per 5 s)
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{method} (id {id}) was never admitted by the start throttle"))
}

/// An admitted `invite.challenge` for the link, returning its result.
async fn challenge(prover: &mut Ws, id: i64, invite_id: &str, secret: &str) -> Value {
    let v = admitted_rpc(
        prover,
        id,
        "invite.challenge",
        json!({ "inviteId": invite_id, "secret": secret }),
    )
    .await;
    assert!(v.get("error").is_none(), "invite.challenge: {v}");
    v["result"].clone()
}

/// An admitted `invite.prove` claiming the `login` account with `gist_id`,
/// returning the full envelope.
async fn prove(
    prover: &mut Ws,
    id: i64,
    invite_id: &str,
    secret: &str,
    nonce: &str,
    gist_id: &str,
    login: &str,
) -> Value {
    admitted_rpc(
        prover,
        id,
        "invite.prove",
        json!({
            "inviteId": invite_id, "secret": secret,
            "nonce": nonce, "gistId": gist_id, "login": login,
        }),
    )
    .await
}

/// Pump a subscriber until a `workspace:updated` event whose `changes`
/// satisfy `pred` arrives (bounded).
async fn await_workspace_updated(ws: &mut Ws, what: &str, pred: impl Fn(&Value) -> bool) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .unwrap_or_else(|| panic!("timed out waiting for workspace:updated ({what})"));
        let next = timeout(remaining, ws.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for workspace:updated ({what})"));
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == json!("events.event")
                    && v["params"]["event"]["type"] == json!("workspace:updated")
                    && pred(&v["params"]["event"]["data"]["changes"])
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

/// Wait for a `github:auth-changed` event carrying `status`.
async fn await_auth_changed(ws: &mut Ws, status: &str) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .unwrap_or_else(|| panic!("timed out waiting for github:auth-changed ({status})"));
        let next = timeout(remaining, ws.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for github:auth-changed ({status})"));
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

/// Raw HTTPS upgrade attempt on `/ws` with `token`; returns the status line.
async fn upgrade_status_line(port: u16, cfg: Arc<ClientConfig>, token: &str) -> String {
    let mut tls = common::tls_connect_with_retry(port, cfg).await;
    let req = format!(
        "GET /ws?token={token} HTTP/1.1\r\nHost: localhost:{port}\r\nUpgrade: websocket\r\n\
         Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         Sec-WebSocket-Version: 13\r\n\r\n"
    );
    tls.write_all(req.as_bytes()).await.expect("write upgrade");
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
    loop {
        let n = timeout(Duration::from_secs(10), tls.read(&mut tmp))
            .await
            .expect("upgrade response timed out")
            .expect("read upgrade response");
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8_lossy(&buf)
        .lines()
        .next()
        .unwrap_or_default()
        .to_string()
}

// ---------------------------------------------------------------------------
// Mock GitHub: ONE plain-HTTP host serving both the login endpoints the
// owner's `github.connect` uses (`/login/device/code`,
// `/login/oauth/access_token`) and the API reads the join needs
// (`GET /user`, `GET /users/{login}`, `GET /gists/{id}`). Each
// `/login/device/code` call mints device code `e2e-dc-{n}`; the token poll
// for flow `n` answers `authorization_pending` until the test sets
// `grants[n]` to the access token the (mock) user authorised with. Gists are
// scripted by the test: `gists[id]` is the `GET /gists/{id}` body (an
// unknown id is `404`; the id `broken` is always `502`).
// ---------------------------------------------------------------------------

/// The mock's scripted gists, keyed by gist id.
type Gists = Arc<Mutex<HashMap<String, Value>>>;

struct MockGithub {
    base_uri: String,
    flows: Arc<AtomicUsize>,
    grants: Arc<Mutex<Vec<Option<&'static str>>>>,
    gists: Gists,
    /// Every `GET /gists/{id}` the daemon made, in order.
    gist_reads: Arc<Mutex<Vec<String>>>,
}

impl MockGithub {
    /// Script `GET /gists/{id}`: a gist owned by `owner`, created `created_at`,
    /// whose `intent-join-proof.txt` (when `proof` is given) starts with
    /// `proof` — the shape the daemon reads back to verify an identity proof.
    fn script_gist(&self, id: &str, owner: &str, created_at: &str, proof: Option<&str>) {
        let mut files = serde_json::Map::new();
        if let Some(proof) = proof {
            files.insert(
                "intent-join-proof.txt".into(),
                json!({
                    "filename": "intent-join-proof.txt",
                    "content": format!("{proof}\nIntent join proof for e2e host\n"),
                }),
            );
        }
        self.gists.lock().expect("gists").insert(
            id.to_string(),
            json!({
                "id": id,
                "public": false,
                "created_at": created_at,
                "owner": { "login": owner },
                "files": files,
            }),
        );
    }

    /// Authorise device flow `n` as the account behind `token`. The flow
    /// must already have been started (a `/login/device/code` call minted
    /// it), so a stale index is a test bug rather than a silent no-op.
    fn authorize(&self, n: usize, token: &'static str) {
        let started = self.flows.load(Ordering::SeqCst);
        assert!(
            n < started,
            "device flow {n} not started yet ({started} minted)"
        );
        let mut grants = self.grants.lock().expect("grants");
        if grants.len() <= n {
            grants.resize(n + 1, None);
        }
        grants[n] = Some(token);
    }
}

fn user_json(login: &str, id: u64) -> Value {
    json!({
        "login": login,
        "id": id,
        "name": format!("{login} name"),
        "avatar_url": format!("https://avatars.example/u/{id}"),
        "html_url": format!("https://github.com/{login}"),
    })
}

fn user_for_token(token: &str) -> Option<Value> {
    match token {
        OWNER_TOKEN => Some(user_json("owner", OWNER_ID)),
        INTRUDER_TOKEN => Some(user_json("intruder", INTRUDER_ID)),
        _ => None,
    }
}

fn user_for_login(login: &str) -> Option<Value> {
    match login {
        "owner" => Some(user_json("owner", OWNER_ID)),
        "guest" => Some(user_json("guest", GUEST_ID)),
        "intruder" => Some(user_json("intruder", INTRUDER_ID)),
        _ => None,
    }
}

async fn spawn_mock_github() -> MockGithub {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind mock github");
    let port = listener.local_addr().expect("mock addr").port();
    let flows = Arc::new(AtomicUsize::new(0));
    let grants: Arc<Mutex<Vec<Option<&'static str>>>> = Arc::new(Mutex::new(Vec::new()));
    let gists: Gists = Arc::new(Mutex::new(HashMap::new()));
    let gist_reads: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let (f, g, gi, gr) = (
        flows.clone(),
        grants.clone(),
        gists.clone(),
        gist_reads.clone(),
    );
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let (f, g, gi, gr) = (f.clone(), g.clone(), gi.clone(), gr.clone());
            tokio::spawn(async move {
                let _ = serve_conn(stream, f, g, gi, gr).await;
            });
        }
    });
    MockGithub {
        base_uri: format!("http://127.0.0.1:{port}"),
        flows,
        grants,
        gists,
        gist_reads,
    }
}

/// Minimal HTTP/1.1 handler: reads one request (head + content-length body),
/// answers, and closes.
async fn serve_conn(
    mut stream: TcpStream,
    flows: Arc<AtomicUsize>,
    grants: Arc<Mutex<Vec<Option<&'static str>>>>,
    gists: Gists,
    gist_reads: Arc<Mutex<Vec<String>>>,
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
    let body: Value = serde_json::from_slice(&buf[body_start..]).unwrap_or(Value::Null);
    let bearer = header("authorization")
        .and_then(|v| v.strip_prefix("Bearer ").map(str::to_string))
        .unwrap_or_default();

    let path = head
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_string();
    let path_only = path.split('?').next().unwrap_or_default();
    let (status, payload) = if path_only == "/login/device/code" {
        let n = flows.fetch_add(1, Ordering::SeqCst);
        (
            200,
            json!({
                "device_code": format!("e2e-dc-{n}"),
                "user_code": USER_CODE,
                "verification_uri": "https://github.com/login/device",
                "expires_in": 900,
                "interval": 1,
            }),
        )
    } else if path_only == "/login/oauth/access_token" {
        let n = body["device_code"]
            .as_str()
            .and_then(|dc| dc.strip_prefix("e2e-dc-"))
            .and_then(|n| n.parse::<usize>().ok());
        let granted = n.and_then(|n| grants.lock().expect("grants").get(n).copied().flatten());
        match granted {
            Some(token) => (
                200,
                json!({ "access_token": token, "token_type": "bearer", "scope": "" }),
            ),
            None => (200, json!({ "error": "authorization_pending" })),
        }
    } else if path_only == "/user" {
        match user_for_token(&bearer) {
            Some(u) => (200, u),
            None => (401, json!({ "message": "Bad credentials" })),
        }
    } else if let Some(login) = path_only.strip_prefix("/users/") {
        match user_for_login(login) {
            Some(u) => (200, u),
            None => (404, json!({ "message": "Not Found" })),
        }
    } else if let Some(id) = path_only.strip_prefix("/gists/") {
        gist_reads.lock().expect("gist reads").push(id.to_string());
        if id == "broken" {
            (502, json!({ "message": "Bad Gateway" }))
        } else {
            match gists.lock().expect("gists").get(id).cloned() {
                Some(g) => (200, g),
                None => (404, json!({ "message": "Not Found" })),
            }
        }
    } else {
        (404, json!({ "message": "Not Found" }))
    };
    let payload = payload.to_string();
    let reason = match status {
        200 => "OK",
        401 => "Unauthorized",
        502 => "Bad Gateway",
        _ => "Not Found",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        payload.len(),
        payload
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

/// Full invite → join → remove → revoke lifecycle over WSS against the mock
/// GitHub host (see the module docs for the storyline).
#[tokio::test]
async fn invite_link_identity_join_and_removal_over_wss() {
    let mock = spawn_mock_github().await;

    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let secrets_file = data_dir.join("secrets.json");
    let secrets_s = secrets_file.to_string_lossy().to_string();
    let tailcat = write_fake_tailcat(&data_dir).to_string_lossy().to_string();
    let env: [(&str, &str); 7] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("INTENTD_SECRETS_FILE", &secrets_s),
        ("INTENTD_GITHUB_LOGIN_BASE_URI", &mock.base_uri),
        ("INTENTD_GITHUB_API_BASE_URI", &mock.base_uri),
        ("INTENTD_TAILCAT_BIN", &tailcat),
        ("GITHUB_TOKEN", OWNER_TOKEN),
    ];
    let child = spawn_serve(&data_dir, &env);
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

    // OWNER: a workspace, plus a subscriber connection on workspace:updated.
    let mut owner = connect_ws(port, cfg.clone(), TOKEN).await;
    let v = wss_rpc(
        &mut owner,
        1,
        "workspace.create",
        json!({ "title": "Invite E2E" }),
    )
    .await;
    assert!(v.get("error").is_none(), "workspace.create: {v}");
    let ws_id = v["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let mut sub = connect_ws(port, cfg.clone(), TOKEN).await;
    let ack = wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:updated"] }),
    )
    .await;
    assert!(ack.get("error").is_none(), "subscribe failed: {ack}");

    // 1. workspace.invite.create pinned to the guest's login: the pin is
    //    resolved to the account id through the (mock) API host, the link
    //    carries the pair envelope minus the bearer token plus inviteId /
    //    secret, and the secret appears here exactly once.
    let v = wss_rpc(
        &mut owner,
        10,
        "workspace.invite.create",
        json!({ "workspaceId": ws_id, "pinLogin": "guest" }),
    )
    .await;
    assert!(v.get("error").is_none(), "invite.create: {v}");
    let r = &v["result"];
    let invite_id = r["invite"]["id"].as_str().expect("invite id").to_string();
    let secret = r["secret"].as_str().expect("secret").to_string();
    assert_eq!(secret.len(), 64);
    assert_eq!(r["invite"]["workspaceId"], json!(ws_id));
    assert_eq!(r["invite"]["pinLogin"], json!("guest"));
    assert_eq!(r["invite"]["pinGithubUserId"], json!(GUEST_ID));
    assert!(r["invite"].get("secretHash").is_none());
    assert_eq!(r["port"], json!(port));
    assert_eq!(r["fingerprint"], json!(fingerprint));
    assert_eq!(r["version"], json!(1));
    assert_eq!(r["hosts"], json!([]));
    let tc = r["tcAddress"].as_str().expect("tcAddress");
    assert!(tc.starts_with("tc-"), "fake sidecar address: {tc}");
    let url = r["url"].as_str().expect("url");
    assert!(url.starts_with("intent://invite?v=1&host=&port="), "{url}");
    assert!(url.contains(&format!("&inviteId={invite_id}")), "{url}");
    assert!(url.contains(&format!("&secret={secret}")), "{url}");
    assert!(url.contains(&format!("&tc={tc}")), "{url}");
    assert!(
        !url.contains("token="),
        "no bearer token in the link: {url}"
    );
    assert!(!url.contains(TOKEN), "no bearer token in the link: {url}");
    assert_eq!(
        r["invite"]["url"],
        json!(url),
        "the invite row echoes the minted link"
    );

    // 2. invite.list shows it open with the same link rebuilt from the
    //    stored secret — never with the secret itself as a field.
    let v = wss_rpc(
        &mut owner,
        11,
        "workspace.invite.list",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(v["result"]["invites"].as_array().map(Vec::len), Some(1));
    let listed = &v["result"]["invites"][0];
    assert_eq!(listed["id"], json!(invite_id));
    assert!(listed.get("secret").is_none());
    assert!(listed.get("secretHash").is_none());
    assert_eq!(listed["url"], json!(url), "list rebuilds the minted link");
    let mut listed_sans_url = listed.clone();
    listed_sans_url.as_object_mut().unwrap().remove("url");
    assert!(
        !listed_sans_url.to_string().contains(&secret),
        "the secret rides only inside url: {listed}"
    );

    // 3. The unauthenticated /invite endpoint: nothing but the invite
    //    methods is reachable (the retired `invite.redeem` included), a
    //    wrong secret is `invite-not-found`, and a good link answers
    //    `invite.challenge` with the workspace hint plus a nonce — no device
    //    flow is started.
    let mut invitee = connect_invite(port, cfg.clone()).await;
    let v = wss_rpc(&mut invitee, 20, "workspace.list", json!({})).await;
    assert_eq!(v["error"]["code"], json!(-32001), "{v}");
    let v = wss_rpc(
        &mut invitee,
        21,
        "invite.redeem",
        json!({ "inviteId": invite_id, "secret": secret }),
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32001), "retired method: {v}");
    let v = wss_rpc(
        &mut invitee,
        22,
        "invite.challenge",
        json!({ "inviteId": invite_id, "secret": "not-the-secret" }),
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32602), "{v}");
    assert_eq!(v["error"]["data"]["code"], json!("invite-not-found"), "{v}");
    let r = challenge(&mut invitee, 23, &invite_id, &secret).await;
    assert_eq!(r["workspaceId"], json!(ws_id));
    assert_eq!(r["workspaceTitle"], json!("Invite E2E"));
    assert!(
        r["hostname"].as_str().is_some_and(|h| !h.is_empty()),
        "hostname: {r}"
    );
    assert!(
        r["prettyHostname"].as_str().is_some_and(|h| !h.is_empty()),
        "prettyHostname: {r}"
    );
    assert!(r.get("flowId").is_none(), "no device flow: {r}");
    assert!(r.get("userCode").is_none(), "no device flow: {r}");
    assert!(r.get("verificationUri").is_none(), "no device flow: {r}");
    let join_nonce = r["nonce"].as_str().expect("nonce").to_string();
    assert_eq!(mock.flows.load(Ordering::SeqCst), 0, "no device flow");

    // 4. The WRONG GitHub account proves (its own gist carries the nonce) →
    //    pin mismatch; the nonce is spent, the invite stays open and no
    //    principal / credential was minted for the intruder.
    let now = chrono::Utc::now().to_rfc3339();
    mock.script_gist("intruderjoin", "intruder", &now, Some(&join_nonce));
    mock.script_gist("guestjoin", "guest", &now, Some(&join_nonce));
    let v = prove(
        &mut invitee,
        24,
        &invite_id,
        &secret,
        &join_nonce,
        "intruderjoin",
        "intruder",
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32602), "{v}");
    assert_eq!(
        v["error"]["data"]["code"],
        json!("invite-pin-mismatch"),
        "{v}"
    );
    let v = prove(
        &mut invitee,
        25,
        &invite_id,
        &secret,
        &join_nonce,
        "guestjoin",
        "guest",
    )
    .await;
    assert_eq!(
        v["error"]["data"]["code"],
        json!("proof-invalid"),
        "the nonce was spent by the refused attempt: {v}"
    );

    // 5. The pinned account proves on a fresh nonce → the join: a credential
    //    (returned once), the principal, the workspace.
    let r = challenge(&mut invitee, 26, &invite_id, &secret).await;
    let join_nonce_2 = r["nonce"].as_str().expect("nonce").to_string();
    assert_ne!(join_nonce_2, join_nonce);
    let after = chrono::Utc::now().to_rfc3339();
    mock.script_gist("guestjoin2", "guest", &after, Some(&join_nonce_2));
    let v = prove(
        &mut invitee,
        27,
        &invite_id,
        &secret,
        &join_nonce_2,
        "guestjoin2",
        "guest",
    )
    .await;
    assert!(v.get("error").is_none(), "invite.prove: {v}");
    let r = &v["result"];
    assert_eq!(r["status"], json!("authorized"));
    assert_eq!(r["login"], json!("guest"));
    assert_eq!(r["workspaceId"], json!(ws_id));
    let guest_token = r["token"].as_str().expect("credential").to_string();
    assert_eq!(guest_token.len(), 64);
    let guest_id = r["principalId"].as_str().expect("principalId").to_string();

    // The owner's subscriber saw the membership change with the new count.
    let ev = await_workspace_updated(&mut sub, "join", |c| {
        c["addedPrincipalId"] == json!(guest_id)
    })
    .await;
    assert_eq!(ev["data"]["workspaceId"], json!(ws_id));
    assert_eq!(ev["data"]["changes"]["members"], json!(true));
    assert_eq!(ev["data"]["changes"]["memberCount"], json!(2));

    // Single use: the same link is now `invite-redeemed`.
    let v = admitted_rpc(
        &mut invitee,
        28,
        "invite.challenge",
        json!({ "inviteId": invite_id, "secret": secret }),
    )
    .await;
    assert_eq!(v["error"]["data"]["code"], json!("invite-redeemed"), "{v}");
    drop(invitee);
    assert_eq!(
        mock.flows.load(Ordering::SeqCst),
        0,
        "the join never started a device flow"
    );

    // 6. The collaborator credential connects to /ws: principal.me shows the
    //    GitHub identity and workspace.get the collaborator role.
    let mut guest = connect_ws(port, cfg.clone(), &guest_token).await;
    let v = wss_rpc(&mut guest, 30, "principal.me", json!({})).await;
    assert!(v.get("error").is_none(), "principal.me: {v}");
    assert_eq!(v["result"]["id"], json!(guest_id));
    assert_eq!(v["result"]["login"], json!("guest"));
    assert_eq!(v["result"]["displayName"], json!("guest name"));
    assert_eq!(
        v["result"]["avatarUrl"],
        json!(format!("https://avatars.example/u/{GUEST_ID}"))
    );
    assert_eq!(v["result"]["isAdministrator"], json!(false));
    let v = wss_rpc(
        &mut guest,
        31,
        "workspace.get",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert!(v.get("error").is_none(), "workspace.get as guest: {v}");
    assert_eq!(v["result"]["workspace"]["myRole"], json!("collaborator"));
    assert_eq!(v["result"]["workspace"]["memberCount"], json!(2));
    let v = wss_rpc(
        &mut guest,
        32,
        "workspace.invite.create",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(
        v["error"]["code"],
        json!(-32003),
        "collaborator minting: {v}"
    );

    // 6a. Returning guest: the owner shares a SECOND workspace. On `/invite`
    //     the guest previews the link with `invite.inspect` (phase-1
    //     validation + host identity, no device flow) and joins with
    //     `invite.accept` on the credential minted in 5 — no GitHub call at
    //     all: the mock's flow counter stays where step 5 left it. A bogus
    //     credential is `credential-invalid`. The result is the phase-2
    //     shape with a fresh credential for the same principal; the
    //     presented credential is rotated out (it no longer upgrades, while
    //     the connection it already opened stays bound), and the owner's
    //     subscriber sees the membership change on the second workspace.
    let flows_before = mock.flows.load(Ordering::SeqCst);
    let v = wss_rpc(
        &mut owner,
        16,
        "workspace.create",
        json!({ "title": "Second E2E" }),
    )
    .await;
    assert!(v.get("error").is_none(), "workspace.create #2: {v}");
    let second_ws = v["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let v = wss_rpc(
        &mut owner,
        17,
        "workspace.invite.create",
        json!({ "workspaceId": second_ws, "pinLogin": "guest" }),
    )
    .await;
    assert!(v.get("error").is_none(), "invite.create #2: {v}");
    let second_invite = v["result"]["invite"]["id"]
        .as_str()
        .expect("invite id")
        .to_string();
    let second_secret = v["result"]["secret"].as_str().expect("secret").to_string();

    // Every `/invite` request hashes the secret, so each draws from the
    // throttle the join above spent; the assertions here are about the
    // requests, so each waits to be admitted.
    let mut returning = connect_invite(port, cfg.clone()).await;
    let v = admitted_rpc(
        &mut returning,
        60,
        "invite.inspect",
        json!({ "inviteId": second_invite, "secret": second_secret }),
    )
    .await;
    assert!(v.get("error").is_none(), "invite.inspect: {v}");
    let r = &v["result"];
    assert_eq!(r["workspaceId"], json!(second_ws));
    assert_eq!(r["workspaceTitle"], json!("Second E2E"));
    assert!(
        r["hostname"].as_str().is_some_and(|h| !h.is_empty()),
        "hostname: {r}"
    );
    assert!(
        r["prettyHostname"].as_str().is_some_and(|h| !h.is_empty()),
        "prettyHostname: {r}"
    );
    assert!(r.get("flowId").is_none(), "no device flow: {r}");
    assert!(r.get("userCode").is_none(), "no device flow: {r}");
    let v = admitted_rpc(
        &mut returning,
        61,
        "invite.accept",
        json!({ "inviteId": second_invite, "secret": second_secret, "credential": "not-a-credential" }),
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32602), "{v}");
    assert_eq!(
        v["error"]["data"]["code"],
        json!("credential-invalid"),
        "{v}"
    );
    let v = admitted_rpc(
        &mut returning,
        62,
        "invite.accept",
        json!({ "inviteId": second_invite, "secret": second_secret, "credential": guest_token }),
    )
    .await;
    assert!(v.get("error").is_none(), "invite.accept: {v}");
    let r = &v["result"];
    assert_eq!(r["status"], json!("authorized"));
    assert_eq!(r["principalId"], json!(guest_id));
    assert_eq!(r["login"], json!("guest"));
    assert_eq!(r["workspaceId"], json!(second_ws));
    let guest_token_2 = r["token"].as_str().expect("credential").to_string();
    assert_eq!(guest_token_2.len(), 64);
    assert_ne!(guest_token_2, guest_token);
    assert!(r.get("hostname").is_none(), "{r}");
    assert_eq!(
        mock.flows.load(Ordering::SeqCst),
        flows_before,
        "inspect / accept never started a device flow"
    );
    drop(returning);
    let ev = await_workspace_updated(&mut sub, "accept", |c| {
        c["addedPrincipalId"] == json!(guest_id) && c["memberCount"] == json!(2)
    })
    .await;
    assert_eq!(ev["data"]["workspaceId"], json!(second_ws));
    assert_eq!(ev["data"]["changes"]["members"], json!(true));

    // The fresh credential connects; the rotated-out one no longer upgrades,
    // but the connection it already opened is still bound.
    let line = upgrade_status_line(port, cfg.clone(), &guest_token).await;
    assert!(
        line.starts_with("HTTP/1.1 401"),
        "rotated-out credential upgrade: {line}"
    );
    let mut guest2 = connect_ws(port, cfg.clone(), &guest_token_2).await;
    let v = wss_rpc(&mut guest2, 70, "principal.me", json!({})).await;
    assert_eq!(v["result"]["id"], json!(guest_id), "{v}");
    let v = wss_rpc(
        &mut guest2,
        71,
        "workspace.get",
        json!({ "workspaceId": second_ws }),
    )
    .await;
    assert!(v.get("error").is_none(), "workspace.get #2 as guest: {v}");
    assert_eq!(v["result"]["workspace"]["myRole"], json!("collaborator"));
    assert_eq!(v["result"]["workspace"]["memberCount"], json!(2));
    drop(guest2);
    let v = wss_rpc(&mut guest, 36, "workspace.list", json!({})).await;
    assert_eq!(
        v["result"]["workspaces"].as_array().map(Vec::len),
        Some(2),
        "the first connection still lists both workspaces: {v}"
    );

    // 6c. Gist identity proof: the owner shares a THIRD workspace (pinned to
    //     the guest). On `/invite` the guest asks `invite.challenge` for a
    //     nonce (the inspect payload plus `nonce` / `nonceExpiresAt`, no
    //     device flow), then `invite.prove` names a gist: one the mock
    //     answers `502` is `github-unreachable` and leaves the nonce usable;
    //     one owned by another account is `proof-invalid` and spends it
    //     (the matching gist is then too late on that nonce); a fresh
    //     challenge plus a gist owned by the guest whose proof file starts
    //     with the nonce mints a credential — the phase-2 shape for the same
    //     principal. GitHub was read for the gists and the account, never
    //     for a device flow.
    let v = wss_rpc(
        &mut owner,
        18,
        "workspace.create",
        json!({ "title": "Third E2E" }),
    )
    .await;
    assert!(v.get("error").is_none(), "workspace.create #3: {v}");
    let third_ws = v["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let v = wss_rpc(
        &mut owner,
        19,
        "workspace.invite.create",
        json!({ "workspaceId": third_ws, "pinLogin": "guest" }),
    )
    .await;
    assert!(v.get("error").is_none(), "invite.create #3: {v}");
    let third_invite = v["result"]["invite"]["id"]
        .as_str()
        .expect("invite id")
        .to_string();
    let third_secret = v["result"]["secret"].as_str().expect("secret").to_string();

    // Every challenge / prove hashes the secret, so each draws from the
    // start throttle spent above; the assertions here are about the proof,
    // so each request waits to be admitted.
    let mut prover = connect_invite(port, cfg.clone()).await;
    let r = challenge(&mut prover, 80, &third_invite, &third_secret).await;
    assert_eq!(r["workspaceId"], json!(third_ws));
    assert_eq!(r["workspaceTitle"], json!("Third E2E"));
    assert!(
        r["hostname"].as_str().is_some_and(|h| !h.is_empty()),
        "hostname: {r}"
    );
    assert!(r.get("flowId").is_none(), "no device flow: {r}");
    let nonce = r["nonce"].as_str().expect("nonce").to_string();
    assert_eq!(nonce.len(), 43, "32 bytes base64url unpadded: {nonce}");
    let expires =
        chrono::DateTime::parse_from_rfc3339(r["nonceExpiresAt"].as_str().expect("nonceExpiresAt"))
            .expect("nonceExpiresAt is RFC 3339");
    let ttl = expires.signed_duration_since(chrono::Utc::now());
    assert!(
        ttl > chrono::Duration::minutes(9) && ttl <= chrono::Duration::minutes(10),
        "nonce TTL ~10 min: {ttl}"
    );

    let now = chrono::Utc::now().to_rfc3339();
    mock.script_gist("intruderproof", "intruder", &now, Some(&nonce));
    mock.script_gist("guestproof", "guest", &now, Some(&nonce));

    let v = prove(
        &mut prover,
        81,
        &third_invite,
        &third_secret,
        &nonce,
        "broken",
        "guest",
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32603), "{v}");
    assert_eq!(
        v["error"]["data"]["code"],
        json!("github-unreachable"),
        "{v}"
    );
    let v = prove(
        &mut prover,
        82,
        &third_invite,
        &third_secret,
        &nonce,
        "intruderproof",
        "guest",
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32602), "{v}");
    assert_eq!(v["error"]["data"]["code"], json!("proof-invalid"), "{v}");
    let v = prove(
        &mut prover,
        83,
        &third_invite,
        &third_secret,
        &nonce,
        "guestproof",
        "guest",
    )
    .await;
    assert_eq!(
        v["error"]["data"]["code"],
        json!("proof-invalid"),
        "the nonce was spent by the refused attempt: {v}"
    );

    let r = challenge(&mut prover, 84, &third_invite, &third_secret).await;
    let nonce_2 = r["nonce"].as_str().expect("nonce").to_string();
    assert_ne!(nonce_2, nonce);
    // A gist that predates its nonce is `proof-invalid` (the `now` above was
    // taken before this challenge); the one created after it verifies.
    mock.script_gist("stale", "guest", &now, Some(&nonce_2));
    let v = prove(
        &mut prover,
        87,
        &third_invite,
        &third_secret,
        &nonce_2,
        "stale",
        "guest",
    )
    .await;
    assert_eq!(v["error"]["data"]["code"], json!("proof-invalid"), "{v}");
    // The host owner's own account (the primary principal, `GITHUB_TOKEN`
    // resolves to "owner") proving a valid gist is `owner-self-join`: the
    // owner cannot join its own host as a guest, no credential is minted,
    // and the invite stays open (the guest joins it below).
    let r = challenge(&mut prover, 89, &third_invite, &third_secret).await;
    let nonce_owner = r["nonce"].as_str().expect("nonce").to_string();
    let owner_after = chrono::Utc::now().to_rfc3339();
    mock.script_gist("ownerproof", "owner", &owner_after, Some(&nonce_owner));
    let v = prove(
        &mut prover,
        90,
        &third_invite,
        &third_secret,
        &nonce_owner,
        "ownerproof",
        "owner",
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32602), "{v}");
    assert_eq!(v["error"]["data"]["code"], json!("owner-self-join"), "{v}");
    assert!(v["result"].is_null(), "no credential for the owner: {v}");
    let r = challenge(&mut prover, 88, &third_invite, &third_secret).await;
    let nonce_2 = r["nonce"].as_str().expect("nonce").to_string();
    let after = chrono::Utc::now().to_rfc3339();
    mock.script_gist("guestproof2", "guest", &after, Some(&nonce_2));
    let v = prove(
        &mut prover,
        85,
        &third_invite,
        &third_secret,
        &nonce_2,
        "guestproof2",
        "guest",
    )
    .await;
    assert!(v.get("error").is_none(), "invite.prove: {v}");
    let r = &v["result"];
    assert_eq!(r["status"], json!("authorized"));
    assert_eq!(r["principalId"], json!(guest_id));
    assert_eq!(r["login"], json!("guest"));
    assert_eq!(r["workspaceId"], json!(third_ws));
    let guest_token_3 = r["token"].as_str().expect("credential").to_string();
    assert_eq!(guest_token_3.len(), 64);
    assert_ne!(guest_token_3, guest_token_2);
    assert!(r.get("hostname").is_none(), "{r}");
    assert_eq!(
        mock.flows.load(Ordering::SeqCst),
        flows_before,
        "challenge / prove never started a device flow"
    );
    {
        let reads = mock.gist_reads.lock().expect("gist reads");
        let distinct: std::collections::BTreeSet<&str> = reads.iter().map(String::as_str).collect();
        assert_eq!(
            distinct.into_iter().collect::<Vec<_>>(),
            [
                "broken",
                "guestjoin2",
                "guestproof2",
                "intruderjoin",
                "intruderproof",
                "ownerproof",
                "stale",
            ],
            "every named gist was read, the too-late ones never were: {reads:?}"
        );
    }
    // Spent nonce: the same proof does not join again.
    let v = prove(
        &mut prover,
        86,
        &third_invite,
        &third_secret,
        &nonce_2,
        "guestproof2",
        "guest",
    )
    .await;
    assert_eq!(v["error"]["data"]["code"], json!("invite-redeemed"), "{v}");
    drop(prover);
    let ev = await_workspace_updated(&mut sub, "prove", |c| {
        c["addedPrincipalId"] == json!(guest_id) && c["memberCount"] == json!(2)
    })
    .await;
    assert_eq!(ev["data"]["workspaceId"], json!(third_ws));

    let mut guest3 = connect_ws(port, cfg.clone(), &guest_token_3).await;
    let v = wss_rpc(&mut guest3, 72, "principal.me", json!({})).await;
    assert_eq!(v["result"]["id"], json!(guest_id), "{v}");
    let v = wss_rpc(
        &mut guest3,
        73,
        "workspace.get",
        json!({ "workspaceId": third_ws }),
    )
    .await;
    assert!(v.get("error").is_none(), "workspace.get #3 as guest: {v}");
    assert_eq!(v["result"]["workspace"]["myRole"], json!("collaborator"));
    drop(guest3);
    let v = wss_rpc(&mut guest, 37, "workspace.list", json!({})).await;
    assert_eq!(
        v["result"]["workspaces"].as_array().map(Vec::len),
        Some(3),
        "all three workspaces: {v}"
    );

    // 6b. Reconnect guard at the OAuth commit boundary: with the guest a
    //     member, the primary identity is load-bearing. The owner runs
    //     github.connect and a DIFFERENT account (the intruder) authorises →
    //     the grant is refused before the token write: the poll reports
    //     `identity-locked`, nothing lands in the secrets file, and the
    //     primary still resolves as "owner".
    let mut auth_sub = connect_ws(port, cfg.clone(), TOKEN).await;
    let ack = wss_rpc(
        &mut auth_sub,
        3,
        "events.subscribe",
        json!({ "eventTypes": ["github:auth-changed"] }),
    )
    .await;
    assert!(ack.get("error").is_none(), "subscribe failed: {ack}");
    let v = wss_rpc(&mut owner, 14, "github.connect", json!({})).await;
    assert!(v.get("error").is_none(), "github.connect: {v}");
    assert_eq!(v["result"]["userCode"], json!(USER_CODE));
    mock.authorize(0, INTRUDER_TOKEN);
    let ev = await_auth_changed(&mut auth_sub, "identity-locked").await;
    assert_eq!(ev["data"]["status"], json!("identity-locked"));
    if let Ok(secrets) = std::fs::read_to_string(&secrets_file) {
        assert!(
            !secrets.contains(INTRUDER_TOKEN),
            "refused grant persisted: {secrets}"
        );
    }
    let v = wss_rpc(&mut owner, 15, "principal.me", json!({})).await;
    assert_eq!(v["result"]["login"], json!("owner"), "{v}");
    drop(auth_sub);

    // 7. The owner removes the member: the next read by the guest is
    //    NotFound and the owner's subscriber sees the count drop.
    let v = wss_rpc(
        &mut owner,
        12,
        "workspace.members.remove",
        json!({ "workspaceId": ws_id, "principalId": guest_id }),
    )
    .await;
    assert_eq!(v["result"], json!({ "removed": true }), "{v}");
    let ev = await_workspace_updated(&mut sub, "remove", |c| {
        c["removedPrincipalId"] == json!(guest_id)
    })
    .await;
    assert_eq!(ev["data"]["changes"]["memberCount"], json!(1));
    let v = wss_rpc(
        &mut guest,
        33,
        "workspace.get",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(
        v["error"]["code"],
        json!(-32602),
        "removed member reads: {v}"
    );
    let v = wss_rpc(&mut guest, 34, "workspace.list", json!({})).await;
    let mut remaining: Vec<&str> = v["result"]["workspaces"]
        .as_array()
        .map(|ws| ws.iter().filter_map(|w| w["id"].as_str()).collect())
        .unwrap_or_default();
    remaining.sort_unstable();
    let mut expected = vec![second_ws.as_str(), third_ws.as_str()];
    expected.sort_unstable();
    assert_eq!(remaining, expected, "the second and third remain: {v}");

    // 8. principal.revokeSelf: both live credentials (the one `invite.accept`
    //    rotated in and the one `invite.prove` minted in 6c — the one minted
    //    in 5 was rotated out in 6a) are revoked, the remaining memberships (the
    //    second and third workspaces) are left, the connection is closed by
    //    the daemon (policy close), and no token authenticates an upgrade.
    let v = wss_rpc(&mut guest, 35, "principal.revokeSelf", json!({})).await;
    assert_eq!(
        v["result"],
        json!({ "revoked": true, "credentials": 2, "workspaces": 2 }),
        "{v}"
    );
    let closed = timeout(Duration::from_secs(15), async {
        loop {
            match guest.next().await {
                Some(Ok(Message::Close(frame))) => return frame.map(|f| f.code),
                Some(Ok(Message::Ping(p))) => {
                    let _ = guest.send(Message::Pong(p)).await;
                }
                Some(Ok(_)) => {}
                None | Some(Err(_)) => return None,
            }
        }
    })
    .await
    .expect("daemon closed the revoked connection");
    assert_eq!(
        closed,
        Some(CloseCode::Policy),
        "policy close after revokeSelf"
    );
    for token in [&guest_token, &guest_token_2, &guest_token_3] {
        let line = upgrade_status_line(port, cfg.clone(), token).await;
        assert!(
            line.starts_with("HTTP/1.1 401"),
            "revoked credential upgrade: {line}"
        );
    }

    // The owner's session is untouched.
    let v = wss_rpc(&mut owner, 13, "principal.me", json!({})).await;
    assert_eq!(v["result"]["isAdministrator"], json!(true));
    assert_eq!(v["result"]["login"], json!("owner"));

    // 9. Secret guessing is rate-limited across time, not just in flight: a
    //    serial stream of bad-link challenges is answered `invite-not-found`
    //    (the store was consulted) until the listener-wide burst (8, one
    //    token back per 5 s) is spent, then `invite-flow-busy` before any
    //    store work. The bucket belongs to the listener, so a reconnect does
    //    not refill it: a fresh connection gets at most the single token a
    //    refill boundary may have restored meanwhile, never a new burst.
    //    The steps above may have left the bucket empty, so the first request
    //    waits to be admitted; the nine that follow it within the same
    //    refill interval cannot all be (at most 8 + 1 tokens exist).
    let mut flood = connect_invite(port, cfg.clone()).await;
    let mut codes = Vec::new();
    for i in 0..10 {
        let params = json!({ "inviteId": invite_id, "secret": format!("guess-{i}") });
        let v = if i == 0 {
            admitted_rpc(&mut flood, 40, "invite.challenge", params).await
        } else {
            wss_rpc(&mut flood, 40 + i, "invite.challenge", params).await
        };
        codes.push(
            v["error"]["data"]["code"]
                .as_str()
                .unwrap_or("")
                .to_string(),
        );
    }
    assert_eq!(codes[0], "invite-not-found", "{codes:?}");
    assert_eq!(codes[9], "invite-flow-busy", "{codes:?}");
    drop(flood);
    let mut again = connect_invite(port, cfg.clone()).await;
    let mut after = Vec::new();
    for i in 0..2 {
        let v = wss_rpc(
            &mut again,
            50 + i,
            "invite.challenge",
            json!({ "inviteId": invite_id, "secret": format!("again-{i}") }),
        )
        .await;
        after.push(
            v["error"]["data"]["code"]
                .as_str()
                .unwrap_or("")
                .to_string(),
        );
    }
    assert_eq!(
        after[1], "invite-flow-busy",
        "reconnect did not refill the bucket: {after:?}"
    );
}
