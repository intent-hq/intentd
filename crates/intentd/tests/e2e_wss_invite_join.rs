//! WSS end-to-end for invite links and the identity-only join (multiplayer
//! w4): `workspace.invite.create` (pinned to a login) → the invitee redeems
//! over the unauthenticated `/invite` endpoint → a pin mismatch is refused →
//! the pinned account joins and receives its own credential → that credential
//! connects to `/ws`, `principal.me` shows the GitHub identity and
//! `workspace.get` the collaborator role → the owner removes the member →
//! `workspace.get` is `NotFound` → `principal.revokeSelf` closes the
//! connection and the credential no longer authenticates.
//!
//! Boots a real `intentd serve` whose GitHub login host AND API host are
//! pointed at one local mock (`INTENTD_GITHUB_LOGIN_BASE_URI` /
//! `INTENTD_GITHUB_API_BASE_URI`). The owner's identity comes from a fake
//! `GITHUB_TOKEN` the mock recognises; the invitee's `GET /user` runs on the
//! device-flow token the mock mints per flow. Hermetic: no live network, and
//! the invitee's token is asserted never to reach the daemon's secrets file.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
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
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

/// Tokens the mock GitHub recognises on `GET /user`, and the accounts they
/// resolve to. The owner's is handed to the daemon as `GITHUB_TOKEN`; the
/// other two are minted by the mock's device flow, one per redemption.
const OWNER_TOKEN: &str = "gho_e2e_owner_token";
const GUEST_TOKEN: &str = "gho_e2e_guest_token";
const INTRUDER_TOKEN: &str = "gho_e2e_intruder_token";
const OWNER_ID: u64 = 100;
const GUEST_ID: u64 = 200;
const INTRUDER_ID: u64 = 300;
const USER_CODE: &str = "JOIN-0001";

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

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    std::fs::write(
        data_dir.join("config.toml"),
        "[server.tunnel]\nenabled = true\n",
    )
    .expect("seed config.toml with server.tunnel.enabled");
    common::enable_ws_api(data_dir);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
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
// Mock GitHub: ONE plain-HTTP host serving both the login endpoints
// (`/login/device/code`, `/login/oauth/access_token`) and the API reads the
// join needs (`GET /user`, `GET /users/{login}`). Each `/login/device/code`
// call mints device code `e2e-dc-{n}`; the token poll for flow `n` answers
// `authorization_pending` until the test sets `grants[n]` to the access
// token the (mock) user authorised with.
// ---------------------------------------------------------------------------

struct MockGithub {
    base_uri: String,
    flows: Arc<AtomicUsize>,
    grants: Arc<Mutex<Vec<Option<&'static str>>>>,
}

impl MockGithub {
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
        GUEST_TOKEN => Some(user_json("guest", GUEST_ID)),
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
    let (f, g) = (flows.clone(), grants.clone());
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let (f, g) = (f.clone(), g.clone());
            tokio::spawn(async move {
                let _ = serve_conn(stream, f, g).await;
            });
        }
    });
    MockGithub {
        base_uri: format!("http://127.0.0.1:{port}"),
        flows,
        grants,
    }
}

/// Minimal HTTP/1.1 handler: reads one request (head + content-length body),
/// answers, and closes.
async fn serve_conn(
    mut stream: TcpStream,
    flows: Arc<AtomicUsize>,
    grants: Arc<Mutex<Vec<Option<&'static str>>>>,
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
    } else {
        (404, json!({ "message": "Not Found" }))
    };
    let payload = payload.to_string();
    let reason = match status {
        200 => "OK",
        401 => "Unauthorized",
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

    // 2. invite.list shows it open, never with the secret.
    let v = wss_rpc(
        &mut owner,
        11,
        "workspace.invite.list",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(v["result"]["invites"].as_array().map(Vec::len), Some(1));
    assert_eq!(v["result"]["invites"][0]["id"], json!(invite_id));
    assert!(v["result"]["invites"][0].get("secret").is_none());

    // 3. The unauthenticated /invite endpoint: nothing but invite.redeem is
    //    reachable, a wrong secret is `invite-not-found`, and a good link
    //    starts the identity-only device flow (mock codes + workspace hint).
    let mut invitee = connect_invite(port, cfg.clone()).await;
    let v = wss_rpc(&mut invitee, 20, "workspace.list", json!({})).await;
    assert_eq!(v["error"]["code"], json!(-32001), "{v}");
    let v = wss_rpc(
        &mut invitee,
        21,
        "invite.redeem",
        json!({ "inviteId": invite_id, "secret": "not-the-secret" }),
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32602), "{v}");
    assert_eq!(v["error"]["data"]["code"], json!("invite-not-found"), "{v}");
    let v = wss_rpc(
        &mut invitee,
        22,
        "invite.redeem",
        json!({ "inviteId": invite_id, "secret": secret }),
    )
    .await;
    assert!(v.get("error").is_none(), "redeem start: {v}");
    let r = &v["result"];
    let flow_0 = r["flowId"].as_str().expect("flowId").to_string();
    assert_eq!(r["userCode"], json!(USER_CODE));
    assert_eq!(
        r["verificationUri"],
        json!("https://github.com/login/device")
    );
    assert_eq!(r["interval"], json!(1));
    assert_eq!(r["workspaceId"], json!(ws_id));
    assert_eq!(r["workspaceTitle"], json!("Invite E2E"));
    assert!(r.get("deviceCode").is_none());

    // 4. The WRONG GitHub account authorises → pin mismatch; the invite stays
    //    open and no principal / credential was minted for the intruder.
    mock.authorize(0, INTRUDER_TOKEN);
    let v = wss_rpc(
        &mut invitee,
        23,
        "invite.redeem",
        json!({ "flowId": flow_0 }),
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32602), "{v}");
    assert_eq!(
        v["error"]["data"]["code"],
        json!("invite-pin-mismatch"),
        "{v}"
    );
    let v = wss_rpc(
        &mut invitee,
        24,
        "invite.redeem",
        json!({ "flowId": flow_0 }),
    )
    .await;
    assert_eq!(
        v["error"]["data"]["code"],
        json!("invite-flow-not-found"),
        "{v}"
    );

    // 5. The pinned account authorises → the join: a credential (returned
    //    once), the principal, the workspace; the GitHub access token itself
    //    never crosses the wire.
    let v = wss_rpc(
        &mut invitee,
        25,
        "invite.redeem",
        json!({ "inviteId": invite_id, "secret": secret }),
    )
    .await;
    assert!(v.get("error").is_none(), "redeem restart: {v}");
    let flow_1 = v["result"]["flowId"].as_str().expect("flowId").to_string();
    assert_ne!(flow_1, flow_0);
    mock.authorize(1, GUEST_TOKEN);
    let v = wss_rpc(
        &mut invitee,
        26,
        "invite.redeem",
        json!({ "flowId": flow_1 }),
    )
    .await;
    assert!(v.get("error").is_none(), "redeem wait: {v}");
    let r = &v["result"];
    assert_eq!(r["status"], json!("authorized"));
    assert_eq!(r["login"], json!("guest"));
    assert_eq!(r["workspaceId"], json!(ws_id));
    let guest_token = r["token"].as_str().expect("credential").to_string();
    assert_eq!(guest_token.len(), 64);
    assert_ne!(guest_token, GUEST_TOKEN);
    assert!(!v.to_string().contains(GUEST_TOKEN), "{v}");
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
    let v = wss_rpc(
        &mut invitee,
        27,
        "invite.redeem",
        json!({ "inviteId": invite_id, "secret": secret }),
    )
    .await;
    assert_eq!(v["error"]["data"]["code"], json!("invite-redeemed"), "{v}");
    drop(invitee);

    // 🔒 The invitee's GitHub token was spent on one GET /user and dropped:
    //    it is nowhere in the daemon's secrets file (which may not exist at
    //    all — nothing was stored).
    if let Ok(secrets) = std::fs::read_to_string(&secrets_file) {
        assert!(
            !secrets.contains(GUEST_TOKEN),
            "guest token persisted: {secrets}"
        );
        assert!(
            !secrets.contains(INTRUDER_TOKEN),
            "intruder token persisted: {secrets}"
        );
    }

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
    assert_eq!(v["result"]["workspaces"], json!([]), "{v}");

    // 8. principal.revokeSelf: the credential is revoked, the connection is
    //    closed by the daemon (policy close), and the token no longer
    //    authenticates an upgrade.
    let v = wss_rpc(&mut guest, 35, "principal.revokeSelf", json!({})).await;
    assert_eq!(
        v["result"],
        json!({ "revoked": true, "credentials": 1, "workspaces": 0 }),
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
    let line = upgrade_status_line(port, cfg.clone(), &guest_token).await;
    assert!(
        line.starts_with("HTTP/1.1 401"),
        "revoked credential upgrade: {line}"
    );

    // The owner's session is untouched.
    let v = wss_rpc(&mut owner, 13, "principal.me", json!({})).await;
    assert_eq!(v["result"]["isAdministrator"], json!(true));
    assert_eq!(v["result"]["login"], json!("owner"));
}
