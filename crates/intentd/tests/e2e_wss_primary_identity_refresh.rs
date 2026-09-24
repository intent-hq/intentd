//! WSS end-to-end for the primary principal's GitHub identity refresh at
//! daemon startup (intent-hq/intent#5534): a primary row that predates the
//! GitHub connection is served with `login: null` by `workspace.members.list`
//! until something calls `principal.me`. The daemon now refreshes the
//! identity once at boot when the row is still unlinked.
//!
//! Boots a real `intentd serve` whose GitHub API host is pointed at a local
//! mock (`INTENTD_GITHUB_API_BASE_URI`) that counts `GET /user` hits and the
//! bearer each carried. The owner's identity comes from a fake `GITHUB_TOKEN`
//! the mock recognises (the env token source). Two boots:
//!
//! 1. With the token: exactly one refresh's worth of `GET /user`
//!    ([`GET_USER_PER_REFRESH`]) lands after boot with no client having
//!    called `principal.me`, `workspace.members.list` or `principal.list`,
//!    and `workspace.members.list` on a fresh workspace then carries the
//!    owner row with `login` / `displayName` / `avatarUrl` from the mock in
//!    the unchanged envelope shape. The roster reads made afterwards inside
//!    the refresh window leave the total unchanged.
//! 2. Without any token (env removed, empty secrets file, `GH_CONFIG_DIR`
//!    pointed at an empty dir so the `gh` CLI fallback finds no login): the
//!    mock is never hit and `workspace.members.list` serves `login: null`
//!    without error.
//!
//! The `workspace.members.list` / `principal.list` triggers cannot be told
//! apart from the startup one here — there is one refresh per
//! `IDENTITY_REFRESH_INTERVAL` across all trigger sites — so their own
//! triggering is proven at unit level in
//! `intent_services::principal_ops::tests`; this suite proves the wire path
//! and the startup trigger. Hermetic: no live network.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::Path;
use std::pin::Pin;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::{Sink, SinkExt, Stream, StreamExt};
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
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::WebSocketStream;

const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

/// The token the mock GitHub recognises on `GET /user`, handed to the daemon
/// as `GITHUB_TOKEN`, and the account it resolves to.
const OWNER_TOKEN: &str = "gho_e2e_owner_token";
const OWNER_ID: u64 = 100;

/// `GET /user` requests ONE identity refresh makes. `refresh_primary_identity`
/// probes `check_auth` (a `GET /user` in `GitHubSourceControl`) and then
/// `get_user` (another), so one refresh is two requests — pre-existing
/// behaviour of the `principal.me` refresher, not introduced here. The
/// exact-total assertions below are pinned to this so a second refresh
/// inside the window (or a third request per refresh) fails loudly.
const GET_USER_PER_REFRESH: usize = 2;

/// Absolute bound on one `wss_rpc` round-trip — send plus the whole receive
/// loop, however many pings or unrelated notifications arrive in between.
const RPC_DEADLINE: Duration = Duration::from_secs(30);

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
    common::test_tempdir_in("/tmp", "itd-wss-identity-")
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

async fn connect_ws(port: u16, cfg: Arc<ClientConfig>, token: &str) -> Ws {
    let url = format!("wss://localhost:{port}/ws?token={token}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// One WSS JSON-RPC round-trip returning the full envelope (so callers can
/// assert on `result` OR `error`). Out-of-band notifications are skipped.
/// Bounded by one absolute [`RPC_DEADLINE`] (see [`rpc_within`]).
async fn wss_rpc(ws: &mut Ws, id: i64, method: &str, params: Value) -> Value {
    rpc_within(ws, RPC_DEADLINE, id, method, params)
        .await
        .unwrap_or_else(|| panic!("wss rpc {method} timed out after {RPC_DEADLINE:?}"))
}

/// The round-trip behind [`wss_rpc`], generic over the socket so a fake can
/// drive it. ONE `deadline` covers the send and the entire receive loop: a
/// stream of pings or unrelated frames never extends the wait. `None` when
/// the deadline passes without the matching `id`.
async fn rpc_within<S>(
    ws: &mut S,
    deadline: Duration,
    id: i64,
    method: &str,
    params: Value,
) -> Option<Value>
where
    S: Stream<Item = Result<Message, WsError>> + Sink<Message> + Unpin,
    <S as Sink<Message>>::Error: std::fmt::Debug,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    timeout(deadline, async {
        ws.send(Message::Text(frame.to_string().into()))
            .await
            .expect("send rpc frame");
        loop {
            match ws.next().await {
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
    })
    .await
    .ok()
}

/// A socket that never answers: it delivers a ping every tick (closer
/// together than any per-frame read timeout would be) and swallows every
/// send. The shape that let a per-frame timeout wait forever.
struct PingOnlyWs {
    tick: tokio::time::Interval,
}

impl Stream for PingOnlyWs {
    type Item = Result<Message, WsError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.tick
            .poll_tick(cx)
            .map(|_| Some(Ok(Message::Ping(Vec::new().into()))))
    }
}

impl Sink<Message> for PingOnlyWs {
    type Error = WsError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), WsError> {
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        Poll::Ready(Ok(()))
    }
}

/// Regression for the helper's bound: with non-matching frames arriving
/// faster than any read timeout, the round-trip still gives up at its ONE
/// absolute deadline instead of restarting the clock per frame (the
/// pre-fix shape waited forever here and would trip the outer guard).
/// Bounded from both sides: not before the deadline, and not long after it
/// (`SLACK` absorbs scheduler jitter on a loaded host, nothing more).
#[tokio::test]
async fn rpc_helper_gives_up_at_its_absolute_deadline_despite_continuous_pings() {
    const DEADLINE: Duration = Duration::from_millis(500);
    const SLACK: Duration = Duration::from_secs(2);
    let mut ws = PingOnlyWs {
        tick: tokio::time::interval(Duration::from_millis(20)),
    };
    let started = tokio::time::Instant::now();
    let outcome = timeout(
        Duration::from_secs(10),
        rpc_within(&mut ws, DEADLINE, 1, "system.status", json!({})),
    )
    .await
    .expect("the round-trip returned at its own deadline, not the outer guard");
    let elapsed = started.elapsed();
    assert!(
        outcome.is_none(),
        "no reply could have matched: {outcome:?}"
    );
    assert!(
        elapsed >= DEADLINE,
        "gave up before the deadline: {elapsed:?}"
    );
    assert!(
        elapsed < DEADLINE + SLACK,
        "kept waiting well past the deadline: {elapsed:?}"
    );
}

/// The owner row of a `workspace.members.list` result.
fn owner_row(result: &Value) -> &Value {
    result["members"]
        .as_array()
        .and_then(|rows| rows.iter().find(|r| r["role"] == "owner"))
        .unwrap_or_else(|| panic!("owner row in {result}"))
}

/// The documented `workspace.members.list` row keys — the shape this change
/// leaves untouched. The additive `identity` triple (protocol 10.8) is
/// present exactly when the row is linked (`login` is a string) and absent
/// otherwise.
fn assert_member_row_shape(row: &Value) {
    let mut keys: Vec<&str> = row
        .as_object()
        .expect("member row is an object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    let linked = row["login"].is_string();
    let expected: &[&str] = if linked {
        &[
            "addedAt",
            "avatarUrl",
            "displayName",
            "identity",
            "login",
            "principalId",
            "role",
        ]
    } else {
        &[
            "addedAt",
            "avatarUrl",
            "displayName",
            "login",
            "principalId",
            "role",
        ]
    };
    assert_eq!(keys, expected, "{row}");
    if linked {
        assert_eq!(
            row["identity"],
            json!({ "provider": "github", "host": "github.com", "externalUserId": OWNER_ID.to_string() }),
            "{row}"
        );
    }
}

// ---------------------------------------------------------------------------
// Mock GitHub API host: answers `GET /user` for the recognised bearer (401
// otherwise) and records the bearer of every `GET /user` it saw, in order.
// Everything else is 404.
// ---------------------------------------------------------------------------

struct MockGithub {
    base_uri: String,
    /// The bearer token of every `GET /user` the daemon made, in order.
    user_reads: Arc<Mutex<Vec<String>>>,
}

impl MockGithub {
    fn user_hits(&self) -> usize {
        self.user_reads.lock().expect("user reads").len()
    }

    /// Wait until the mock has seen at least `n` `GET /user` hits (bounded).
    async fn await_user_hits(&self, n: usize) {
        timeout(common::daemon_startup_timeout(), async {
            while self.user_hits() < n {
                // timing-guard: poll interval
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "mock GitHub never saw {n} GET /user (saw {})",
                self.user_hits()
            )
        });
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

async fn spawn_mock_github() -> MockGithub {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind mock github");
    let port = listener.local_addr().expect("mock addr").port();
    let user_reads: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let reads = user_reads.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let reads = reads.clone();
            tokio::spawn(async move {
                let _ = serve_conn(stream, reads).await;
            });
        }
    });
    MockGithub {
        base_uri: format!("http://127.0.0.1:{port}"),
        user_reads,
    }
}

/// Minimal HTTP/1.1 handler: reads one request head, answers, and closes.
async fn serve_conn(
    mut stream: TcpStream,
    user_reads: Arc<Mutex<Vec<String>>>,
) -> std::io::Result<()> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
    let head_end = loop {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos;
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let bearer = head
        .lines()
        .find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.trim()
                .eq_ignore_ascii_case("authorization")
                .then(|| v.trim().to_string())
        })
        .and_then(|v| v.strip_prefix("Bearer ").map(str::to_string))
        .unwrap_or_default();
    let path = head
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_string();
    let path_only = path.split('?').next().unwrap_or_default();
    let (status, payload) = if path_only == "/user" {
        user_reads.lock().expect("user reads").push(bearer.clone());
        if bearer == OWNER_TOKEN {
            (200, user_json("owner", OWNER_ID))
        } else {
            (401, json!({ "message": "Bad credentials" }))
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

/// Boot the daemon from `data_dir` with `env` and return the WSS port and
/// pinned client config once `system.status` reports the listener.
async fn boot(data_dir: &Path, env: &[(&str, &str)]) -> (Daemon, u16, Arc<ClientConfig>) {
    let child = spawn_serve(data_dir, env);
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
    (daemon, port, client_config(&fingerprint))
}

/// Boot 1 (see the module docs): the startup refresh links the primary
/// identity with no roster read having been made, and
/// `workspace.members.list` then carries it in the unchanged row shape.
#[tokio::test]
async fn startup_refresh_links_primary_identity_over_wss() {
    let mock = spawn_mock_github().await;
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let secrets_s = data_dir.join("secrets.json").to_string_lossy().to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("INTENTD_SECRETS_FILE", &secrets_s),
        ("INTENTD_GITHUB_API_BASE_URI", &mock.base_uri),
        ("GITHUB_TOKEN", OWNER_TOKEN),
    ];
    let (_daemon, port, cfg) = boot(&data_dir, &env).await;

    // The only RPC so far is the readiness `system.status`: no `principal.me`,
    // `workspace.members.list` or `principal.list` — the boot itself ran
    // exactly one refresh (= `GET_USER_PER_REFRESH` requests).
    mock.await_user_hits(GET_USER_PER_REFRESH).await;
    assert_eq!(
        mock.user_hits(),
        GET_USER_PER_REFRESH,
        "boot ran exactly one identity refresh"
    );
    assert!(
        mock.user_reads
            .lock()
            .expect("user reads")
            .iter()
            .all(|b| b == OWNER_TOKEN),
        "every GET /user carried the env token"
    );

    let mut owner = connect_ws(port, cfg.clone(), TOKEN).await;
    let v = wss_rpc(
        &mut owner,
        1,
        "workspace.create",
        json!({ "title": "Identity refresh E2E" }),
    )
    .await;
    assert!(v.get("error").is_none(), "workspace.create: {v}");
    let ws_id = v["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    // The refresh is off-path: the fetch has happened, the persisted row
    // follows shortly. Poll the wire read until it carries the identity.
    let linked = timeout(common::daemon_startup_timeout(), async {
        let mut id = 10;
        loop {
            let v = wss_rpc(
                &mut owner,
                id,
                "workspace.members.list",
                json!({ "workspaceId": ws_id }),
            )
            .await;
            assert_eq!(v["jsonrpc"], json!("2.0"), "{v}");
            assert_eq!(v["id"], json!(id), "{v}");
            assert!(v.get("error").is_none(), "workspace.members.list: {v}");
            let row = owner_row(&v["result"]);
            assert_member_row_shape(row);
            if row["login"].is_string() {
                return v;
            }
            id += 1;
            // timing-guard: poll interval
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("workspace.members.list carried the refreshed owner identity within the bound");
    let row = owner_row(&linked["result"]);
    assert_eq!(row["login"], json!("owner"), "{linked}");
    assert_eq!(row["displayName"], json!("owner name"), "{linked}");
    assert_eq!(
        row["avatarUrl"],
        json!(format!("https://avatars.example/u/{OWNER_ID}")),
        "{linked}"
    );
    assert_eq!(row["role"], json!("owner"), "{linked}");
    assert!(row["principalId"].is_string(), "{linked}");
    assert!(row["addedAt"].is_string(), "{linked}");
    assert!(linked["result"]["guestCount"].is_number(), "{linked}");
    assert!(linked["result"]["guestLimit"].is_number(), "{linked}");
    // The roster reads polled above (unlinked, then linked) shared the
    // startup refresh's window: still exactly one refresh.
    assert_eq!(
        mock.user_hits(),
        GET_USER_PER_REFRESH,
        "workspace.members.list inside the startup refresh window did not refresh again"
    );

    // Further roster reads inside the window add nothing either: the row is
    // linked and there is one refresh per IDENTITY_REFRESH_INTERVAL across
    // all trigger sites.
    let v = wss_rpc(&mut owner, 30, "principal.list", json!({})).await;
    assert!(v.get("error").is_none(), "principal.list: {v}");
    assert_eq!(v["result"], json!({ "principals": [] }), "{v}");
    let v = wss_rpc(
        &mut owner,
        31,
        "workspace.members.list",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert!(v.get("error").is_none(), "workspace.members.list: {v}");
    assert_eq!(owner_row(&v["result"])["login"], json!("owner"), "{v}");
    assert_eq!(
        mock.user_hits(),
        GET_USER_PER_REFRESH,
        "exactly one refresh in total: no further GET /user from roster reads once linked"
    );
}

/// Boot 2 (see the module docs): with GitHub auth not configured the startup
/// refresh is a no-op — the mock is never hit and `workspace.members.list`
/// serves `login: null` without error. The negative is bounded by the RPC
/// round-trips made after boot; the deterministic proof of the auth gate is
/// `principal_ops::tests::startup_leaves_primary_unlinked_without_github_auth`.
#[tokio::test]
async fn startup_refresh_is_a_no_op_without_github_auth_over_wss() {
    let mock = spawn_mock_github().await;
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let secrets_s = data_dir.join("secrets.json").to_string_lossy().to_string();
    let gh_config_dir = data_dir.join("gh-config");
    std::fs::create_dir_all(&gh_config_dir).expect("mkdir empty gh config dir");
    let gh_config_s = gh_config_dir.to_string_lossy().to_string();
    let env: [(&str, &str); 5] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("INTENTD_SECRETS_FILE", &secrets_s),
        ("INTENTD_GITHUB_API_BASE_URI", &mock.base_uri),
        ("GH_CONFIG_DIR", &gh_config_s),
    ];
    let (_daemon, port, cfg) = boot(&data_dir, &env).await;

    let mut owner = connect_ws(port, cfg.clone(), TOKEN).await;
    let v = wss_rpc(
        &mut owner,
        1,
        "workspace.create",
        json!({ "title": "Identity refresh E2E (no auth)" }),
    )
    .await;
    assert!(v.get("error").is_none(), "workspace.create: {v}");
    let ws_id = v["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();

    let v = wss_rpc(
        &mut owner,
        2,
        "workspace.members.list",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(v["jsonrpc"], json!("2.0"), "{v}");
    assert_eq!(v["id"], json!(2), "{v}");
    assert!(v.get("error").is_none(), "workspace.members.list: {v}");
    let row = owner_row(&v["result"]);
    assert_member_row_shape(row);
    assert_eq!(row["login"], Value::Null, "{v}");
    assert_eq!(row["displayName"], Value::Null, "{v}");
    assert_eq!(row["avatarUrl"], Value::Null, "{v}");
    assert_eq!(row["role"], json!("owner"), "{v}");

    let v = wss_rpc(&mut owner, 3, "principal.list", json!({})).await;
    assert!(v.get("error").is_none(), "principal.list: {v}");
    assert_eq!(v["result"], json!({ "principals": [] }), "{v}");

    assert_eq!(
        mock.user_hits(),
        0,
        "GET /user never attempted without GitHub auth: {:?}",
        mock.user_reads.lock().expect("user reads")
    );
}
