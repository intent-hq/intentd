//! WSS end-to-end for the provider-neutral identity seam (protocol 10.8) with
//! a GitLab identity on both sides of an invite:
//!
//! - **(a)** a host with **no** GitHub credential and a GitLab identity
//!   linked mints an invite (`workspace.invite.create`, its `pinLogin`
//!   resolved on GitLab), `principal.me` carries the gitlab triple, and a
//!   gitlab-only guest completes `invite.challenge` / `invite.prove`
//!   (`provider: "gitlab"`, `proofId` = its snippet id) and appears in
//!   `workspace.members.list` with a `gitlab` identity;
//! - **(c)** the owner's own GitLab account is refused as a guest
//!   (`owner-self-join`) before any credential is minted;
//! - **(b)** on an instance that refuses anonymous reads, the host that is
//!   connected to it verifies the snippet with its own credential, while a
//!   host without a connection to it gets the typed `identity-unverifiable`
//!   refusal (naming the host) and keeps the nonce for a retry; an instance
//!   answering a server error is `github-unreachable` (the code is kept for
//!   both providers) and the same nonce succeeds on retry.
//!
//! Boots real `intentd serve` daemons whose GitHub API/login host and GitLab
//! API host all point at one local mock (`INTENTD_GITHUB_API_BASE_URI`,
//! `INTENTD_GITHUB_LOGIN_BASE_URI`, `INTENTD_GITLAB_API_BASE_URI`). The
//! hosts' identities come from fake `GITLAB_TOKEN` / `GITHUB_TOKEN` values the
//! mock recognises; the guests never authenticate — their proof snippets are
//! scripted straight into the mock, as the GitHub suite scripts gists.
//! Hermetic: no live network, secrets land in a temp `INTENTD_SECRETS_FILE`.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
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
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

const TOKEN: &str = "cfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcf";

/// The GitLab personal access tokens the mock recognises on `/api/v4/user`
/// and the accounts they resolve to; the host's is handed to its daemon as
/// `GITLAB_TOKEN`. The guest's token exists only to author its snippet in
/// the mock — no guest daemon ever holds it.
const HOST_GL_PAT: &str = "glpat-e2e-host-token";
const GUEST_GL_PAT: &str = "glpat-e2e-guest-token";
const INTRUDER_GL_PAT: &str = "glpat-e2e-intruder-token";
const HOST_GL_ID: u64 = 4242;
const HOST_GL_LOGIN: &str = "glab-host";
const GUEST_GL_ID: u64 = 7777;
const GUEST_GL_LOGIN: &str = "glab-guest";
const INTRUDER_GL_ID: u64 = 8888;
const INTRUDER_GL_LOGIN: &str = "glab-intruder";
/// The GitHub token of the second host in (b): a github.com identity and
/// no GitLab connection.
const OWNER_GH_TOKEN: &str = "gho_e2e_gh_owner_token";
const OWNER_GH_ID: u64 = 100;
const OWNER_GH_LOGIN: &str = "gh-owner";

/// The bound instance: `sourceControl.gitlab.host` defaults to gitlab.com.
const HOST: &str = "gitlab.com";
const PROOF_FILE_NAME: &str = "intent-join-proof.txt";

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
    common::test_tempdir_in("/tmp", "itd-wss-glinvite-")
}

/// A fake `tailcat` sidecar so the daemon reports a tunnel address: an
/// invite link is tunnel-only, so `workspace.invite.create` is refused
/// (`tunnel-down`) until the daemon has one (same seam as the GitHub
/// invite suite).
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
    # // timing-guard: the fake sidecar stays alive until the daemon that spawned it dies
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
    // The GitHub resolution chain ends at `gh auth token`; point the CLI at an
    // empty config dir so a developer's own `gh auth login` is never borrowed.
    let gh_config_dir = data_dir.join("gh-config");
    std::fs::create_dir_all(&gh_config_dir).expect("mkdir hermetic gh config dir");
    let mut cmd = common::serve_command();
    cmd.env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        // Each host's identity is exactly the credential its test hands it.
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

/// One `/invite` round-trip that waits out the listener-wide start throttle:
/// a request refused `invite-flow-busy` is retried until admitted, bounded.
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

/// An admitted `invite.challenge` for the link, returning its `nonce`.
async fn challenge_nonce(prover: &mut Ws, id: i64, invite_id: &str, secret: &str) -> String {
    let v = admitted_rpc(
        prover,
        id,
        "invite.challenge",
        json!({ "inviteId": invite_id, "secret": secret }),
    )
    .await;
    assert!(v.get("error").is_none(), "invite.challenge: {v}");
    assert!(v["result"].get("flowId").is_none(), "no device flow: {v}");
    v["result"]["nonce"].as_str().expect("nonce").to_string()
}

/// An admitted `invite.prove` claiming the GitLab account `login` with the
/// snippet `proof_id` (`provider: "gitlab"`, the `proofId` spelling), returning
/// the full envelope.
async fn prove_gitlab(
    prover: &mut Ws,
    id: i64,
    invite_id: &str,
    secret: &str,
    nonce: &str,
    proof_id: &str,
    login: &str,
) -> Value {
    admitted_rpc(
        prover,
        id,
        "invite.prove",
        json!({
            "inviteId": invite_id, "secret": secret, "nonce": nonce,
            "proofId": proof_id, "login": login, "provider": "gitlab",
        }),
    )
    .await
}

/// The mock's scripted snippets, keyed by snippet id: `(metadata, raw body)`.
type Snippets = Arc<Mutex<Vec<(String, Value, String)>>>;

/// One local HTTP mock standing in for github.com's API + login host **and**
/// gitlab.com's API, so every forge call a daemon under test makes lands
/// here.
struct MockForge {
    base_uri: String,
    /// When set, the GitLab snippet routes refuse anonymous reads (`401`),
    /// answering only a bearer the instance knows.
    private_snippets: Arc<AtomicBool>,
    /// When set, the GitLab snippet routes answer `503` to every read.
    snippet_server_error: Arc<AtomicBool>,
    /// The switches scripting github.com's `GET /user`.
    github_user: GithubUser,
    snippets: Snippets,
    /// Snippet reads (metadata or raw) that carried a bearer token.
    authenticated_snippet_reads: Arc<AtomicUsize>,
}

/// The switches scripting github.com's `GET /user` (the second host's
/// identity in (b), and the forge an `identity.provider` write of `github`
/// probes).
#[derive(Clone)]
struct GithubUser {
    /// When set, `GET /user` answers `503` (an outage, not a rejected
    /// credential) — after `grace` more normal answers.
    server_error: Arc<AtomicBool>,
    /// Reads still answered normally once the outage is armed (decremented
    /// per read): scripts "the first read succeeds, the next one fails".
    grace: Arc<AtomicUsize>,
    /// When set, `GET /user` answers `401` to every bearer (the credential
    /// is rejected: not connected).
    unauthorized: Arc<AtomicBool>,
    /// While `true`, every `GET /user` is held after arriving and answers
    /// only once the switch flips back — scripts a probe that outlives the
    /// next `identity.provider` write.
    hold: Arc<tokio::sync::watch::Sender<bool>>,
    /// Reads that arrived while held (cumulative), for waiting until the
    /// probe under test is in flight.
    held: Arc<tokio::sync::watch::Sender<usize>>,
}

impl GithubUser {
    fn new() -> Self {
        Self {
            server_error: Arc::new(AtomicBool::new(false)),
            grace: Arc::new(AtomicUsize::new(0)),
            unauthorized: Arc::new(AtomicBool::new(false)),
            hold: Arc::new(tokio::sync::watch::Sender::new(false)),
            held: Arc::new(tokio::sync::watch::Sender::new(0)),
        }
    }

    /// Wait until `n` reads in total have arrived while held.
    async fn held_reads(&self, n: usize) {
        let mut rx = self.held.subscribe();
        timeout(Duration::from_secs(30), rx.wait_for(|held| *held >= n))
            .await
            .expect("a held GET /user arrives")
            .expect("held counter alive");
    }

    /// Answer one `GET /user` (holding it first while `hold` is set).
    async fn answer(&self, bearer: &str) -> (u16, Body) {
        if *self.hold.borrow() {
            self.held.send_modify(|n| *n += 1);
            let mut rx = self.hold.subscribe();
            rx.wait_for(|held| !*held).await.expect("hold switch alive");
        }
        let outage = self.server_error.load(Ordering::SeqCst)
            && self
                .grace
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |g| g.checked_sub(1))
                .is_err();
        if outage {
            (
                503,
                Body::Json(json!({ "message": "503 Service Unavailable" })),
            )
        } else if bearer == OWNER_GH_TOKEN && !self.unauthorized.load(Ordering::SeqCst) {
            (200, Body::Json(gh_user_json(OWNER_GH_LOGIN, OWNER_GH_ID)))
        } else {
            (401, Body::Json(json!({ "message": "Bad credentials" })))
        }
    }
}

impl MockForge {
    /// Script `GET /api/v4/snippets/{id}` (+ `/raw`): an Intent proof snippet
    /// authored by the GitLab account behind `author_pat`, created
    /// `created_at`, whose first line is `proof`.
    fn script_snippet(&self, id: &str, author_pat: &str, created_at: &str, proof: &str) {
        let author = gl_user_for_token(author_pat).expect("known gitlab author");
        let meta = json!({
            "id": id.parse::<u64>().expect("numeric snippet id"),
            "title": "Intent identity proof (safe to delete)",
            "visibility": if self.private_snippets.load(Ordering::SeqCst) { "private" } else { "public" },
            "created_at": created_at,
            "author": {
                "id": author["id"],
                "username": author["username"],
                "avatar_url": author["avatar_url"],
            },
            "files": [{ "path": PROOF_FILE_NAME, "raw_url": format!("{}/-/snippets/{id}/raw/main/{PROOF_FILE_NAME}", self.base_uri) }],
        });
        let raw =
            format!("{proof}\nProof of GitLab identity for Intent host e2e; safe to delete.\n");
        let mut snippets = self.snippets.lock().expect("snippets");
        snippets.retain(|(sid, _, _)| sid != id);
        snippets.push((id.to_string(), meta, raw));
    }
}

fn gl_user_json(username: &str, id: u64) -> Value {
    json!({
        "id": id,
        "username": username,
        "name": format!("{username} name"),
        "avatar_url": format!("https://gitlab.example/avatar/{id}"),
        "web_url": format!("https://gitlab.com/{username}"),
    })
}

fn gl_user_for_token(token: &str) -> Option<Value> {
    match token {
        HOST_GL_PAT => Some(gl_user_json(HOST_GL_LOGIN, HOST_GL_ID)),
        GUEST_GL_PAT => Some(gl_user_json(GUEST_GL_LOGIN, GUEST_GL_ID)),
        INTRUDER_GL_PAT => Some(gl_user_json(INTRUDER_GL_LOGIN, INTRUDER_GL_ID)),
        _ => None,
    }
}

fn gl_user_for_username(username: &str) -> Option<Value> {
    [
        (HOST_GL_LOGIN, HOST_GL_ID),
        (GUEST_GL_LOGIN, GUEST_GL_ID),
        (INTRUDER_GL_LOGIN, INTRUDER_GL_ID),
    ]
    .into_iter()
    .find(|(login, _)| login.eq_ignore_ascii_case(username))
    .map(|(login, id)| gl_user_json(login, id))
}

fn gh_user_json(login: &str, id: u64) -> Value {
    json!({
        "login": login,
        "id": id,
        "name": format!("{login} name"),
        "avatar_url": format!("https://avatars.example/u/{id}"),
        "html_url": format!("https://github.com/{login}"),
    })
}

async fn spawn_mock_forge() -> MockForge {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind mock forge");
    let port = listener.local_addr().expect("mock addr").port();
    let private_snippets = Arc::new(AtomicBool::new(false));
    let snippet_server_error = Arc::new(AtomicBool::new(false));
    let github_user = GithubUser::new();
    let snippets: Snippets = Arc::new(Mutex::new(Vec::new()));
    let authenticated_snippet_reads = Arc::new(AtomicUsize::new(0));
    let (private, snip_err, gh_user, snips, auth_reads) = (
        private_snippets.clone(),
        snippet_server_error.clone(),
        github_user.clone(),
        snippets.clone(),
        authenticated_snippet_reads.clone(),
    );
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let (private, snip_err, gh_user, snips, auth_reads) = (
                private.clone(),
                snip_err.clone(),
                gh_user.clone(),
                snips.clone(),
                auth_reads.clone(),
            );
            tokio::spawn(async move {
                let _ = serve_conn(stream, private, snip_err, gh_user, snips, auth_reads).await;
            });
        }
    });
    MockForge {
        base_uri: format!("http://127.0.0.1:{port}"),
        private_snippets,
        snippet_server_error,
        github_user,
        snippets,
        authenticated_snippet_reads,
    }
}

enum Body {
    Json(Value),
    Text(String),
}

/// Minimal HTTP/1.1 handler: reads one request head, answers, and closes.
async fn serve_conn(
    mut stream: TcpStream,
    private_snippets: Arc<AtomicBool>,
    snippet_server_error: Arc<AtomicBool>,
    github_user: GithubUser,
    snippets: Snippets,
    authenticated_snippet_reads: Arc<AtomicUsize>,
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
    let (path_only, query) = path.split_once('?').unwrap_or((&path, ""));

    let not_found = || (404, Body::Json(json!({ "message": "404 Not Found" })));
    let unauthorized = || (401, Body::Json(json!({ "message": "401 Unauthorized" })));
    let snippet_read = |id: &str, raw: bool| {
        if !bearer.is_empty() {
            authenticated_snippet_reads.fetch_add(1, Ordering::SeqCst);
        }
        if snippet_server_error.load(Ordering::SeqCst) {
            return (
                503,
                Body::Json(json!({ "message": "503 Service Unavailable" })),
            );
        }
        if private_snippets.load(Ordering::SeqCst) && gl_user_for_token(&bearer).is_none() {
            return unauthorized();
        }
        let snippets = snippets.lock().expect("snippets");
        match snippets.iter().find(|(sid, _, _)| sid == id) {
            Some((_, _, body)) if raw => (200, Body::Text(body.clone())),
            Some((_, meta, _)) => (200, Body::Json(meta.clone())),
            None => not_found(),
        }
    };
    let (status, body) = if path_only == "/user" {
        github_user.answer(&bearer).await
    } else if path_only.starts_with("/users/") {
        // github.com `GET /users/{login}`: nobody the tests pin lives there.
        (404, Body::Json(json!({ "message": "Not Found" })))
    } else if path_only == "/api/v4/user" {
        match gl_user_for_token(&bearer) {
            Some(u) => (200, Body::Json(u)),
            None => unauthorized(),
        }
    } else if path_only == "/api/v4/users" {
        let username = query
            .split('&')
            .find_map(|kv| kv.strip_prefix("username="))
            .unwrap_or_default();
        let users: Vec<Value> = gl_user_for_username(username).into_iter().collect();
        (200, Body::Json(Value::Array(users)))
    } else if let Some(rest) = path_only.strip_prefix("/api/v4/snippets/") {
        match rest.strip_suffix("/raw") {
            Some(id) => snippet_read(id, true),
            None => snippet_read(rest, false),
        }
    } else {
        not_found()
    };
    let (content_type, payload) = match body {
        Body::Json(v) => ("application/json", v.to_string()),
        Body::Text(t) => ("text/plain", t),
    };
    let reason = match status {
        200 => "OK",
        401 => "Unauthorized",
        503 => "Service Unavailable",
        _ => "Not Found",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        payload.len(),
        payload
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

/// A booted daemon reachable over WSS.
struct Host {
    _dir: tempfile::TempDir,
    _daemon: Daemon,
    port: u16,
    cfg: Arc<ClientConfig>,
}

/// Boot one `intentd serve` against `mock`, its forge credential(s) being
/// exactly `credentials` (`GITLAB_TOKEN` / `GITHUB_TOKEN` pairs).
async fn boot(mock: &MockForge, credentials: &[(&str, &str)]) -> Host {
    let dir = temp_data_dir();
    let data_dir = dir.path().to_path_buf();
    let secrets_s = data_dir.join("secrets.json").to_string_lossy().to_string();
    let tailcat = write_fake_tailcat(&data_dir).to_string_lossy().to_string();
    let mut env: Vec<(&str, &str)> = vec![
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("INTENTD_SECRETS_FILE", &secrets_s),
        ("INTENTD_GITHUB_LOGIN_BASE_URI", &mock.base_uri),
        ("INTENTD_GITHUB_API_BASE_URI", &mock.base_uri),
        ("INTENTD_GITLAB_API_BASE_URI", &mock.base_uri),
        ("INTENTD_TAILCAT_BIN", &tailcat),
    ];
    env.extend_from_slice(credentials);
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
    let cfg = client_config(&fingerprint);
    Host {
        _dir: dir,
        _daemon: daemon,
        port,
        cfg,
    }
}

/// The owner's workspace on `host`, plus its id.
async fn create_workspace(owner: &mut Ws, id: i64, title: &str) -> String {
    let v = wss_rpc(owner, id, "workspace.create", json!({ "title": title })).await;
    assert!(v.get("error").is_none(), "workspace.create: {v}");
    v["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string()
}

/// `workspace.invite.create` with `params` merged over `{ workspaceId }`,
/// returning `(inviteId, secret, invite row)`.
async fn create_invite(
    owner: &mut Ws,
    id: i64,
    ws_id: &str,
    mut params: Value,
) -> (String, String, Value) {
    params["workspaceId"] = json!(ws_id);
    let v = wss_rpc(owner, id, "workspace.invite.create", params).await;
    assert!(v.get("error").is_none(), "invite.create: {v}");
    let invite_id = v["result"]["invite"]["id"]
        .as_str()
        .expect("invite id")
        .to_string();
    let secret = v["result"]["secret"].as_str().expect("secret").to_string();
    (invite_id, secret, v["result"]["invite"].clone())
}

fn gitlab_identity(id: u64) -> Value {
    json!({ "provider": "gitlab", "host": HOST, "externalUserId": id.to_string() })
}

fn github_identity(id: u64) -> Value {
    json!({ "provider": "github", "host": "github.com", "externalUserId": id.to_string() })
}

/// Wait up to `secs` for the next `events.event` notification of type
/// `event_type` on `ws` (other frames are skipped); returns the event object.
async fn next_event(ws: &mut Ws, event_type: &str, secs: u64) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(!remaining.is_zero(), "timed out waiting for {event_type}");
        match timeout(remaining, ws.next()).await.expect("event frame") {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == json!("events.event")
                    && v["params"]["event"]["type"] == json!(event_type)
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

/// True when no `events.event` of type `event_type` reaches `ws` within
/// `window_ms` (a negative assertion: the socket stayed quiet).
async fn stays_quiet(ws: &mut Ws, event_type: &str, window_ms: u64) -> bool {
    timeout(Duration::from_millis(window_ms), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).expect("json frame");
                    if v["method"] == json!("events.event")
                        && v["params"]["event"]["type"] == json!(event_type)
                    {
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
    })
    .await
    .is_err()
}

/// Subscribe `ws` to `principal:identity-changed` (global: no workspace).
async fn subscribe_identity_changed(ws: &mut Ws, id: i64) {
    let v = wss_rpc(
        ws,
        id,
        "events.subscribe",
        json!({ "eventTypes": ["principal:identity-changed"] }),
    )
    .await;
    assert!(
        v["result"]["subscriptionId"].is_string(),
        "events.subscribe: {v}"
    );
}

/// `settings.update` of `identity.provider` to `value` over WSS.
async fn set_identity_provider(owner: &mut Ws, id: i64, value: Value) {
    let v = wss_rpc(
        owner,
        id,
        "settings.update",
        json!({ "changes": [{ "path": "identity.provider", "value": value }] }),
    )
    .await;
    assert!(v.get("error").is_none(), "settings.update: {v}");
}

/// (a) + (c): a GitLab-only host mints invites and admits a GitLab guest;
/// its own account is refused as a guest.
#[tokio::test]
async fn gitlab_only_host_mints_invites_and_admits_gitlab_guest_over_wss() {
    let mock = spawn_mock_forge().await;
    let host = boot(&mock, &[("GITLAB_TOKEN", HOST_GL_PAT)]).await;
    let mut owner = connect_ws(host.port, host.cfg.clone(), TOKEN).await;

    // Fresh daemon: the primary principal is not linked yet (`principal.me`
    // serves the cached row and refreshes in the background).
    let v = wss_rpc(&mut owner, 1, "principal.me", json!({})).await;
    assert!(v.get("error").is_none(), "principal.me: {v}");
    assert!(
        v["result"]["isAdministrator"].as_bool().unwrap_or(false),
        "{v}"
    );

    let ws_id = create_workspace(&mut owner, 2, "GitLab Invite E2E").await;

    // 1. `pinLogin` alone resolves on the inviter's forge — GitLab — through
    //    `GET /api/v4/users?username=`: the invite carries the pin triple and
    //    no legacy github id; a `pinProvider: "github"` pin for the same
    //    login is looked up on github.com, where nobody by that name lives.
    let (pinned_id, pinned_secret, pinned) = create_invite(
        &mut owner,
        10,
        &ws_id,
        json!({ "pinLogin": GUEST_GL_LOGIN }),
    )
    .await;
    assert_eq!(pinned["pinLogin"], json!(GUEST_GL_LOGIN), "{pinned}");
    assert_eq!(
        pinned["pinIdentity"],
        gitlab_identity(GUEST_GL_ID),
        "{pinned}"
    );
    assert!(
        pinned.get("pinGithubUserId").is_none(),
        "a gitlab pin has no github projection: {pinned}"
    );
    // Minting resolved the inviter's identity synchronously: with no GitHub
    // credential anywhere, `principal.me` now carries the gitlab triple.
    let v = wss_rpc(&mut owner, 3, "principal.me", json!({})).await;
    assert_eq!(v["result"]["identity"], gitlab_identity(HOST_GL_ID), "{v}");
    assert_eq!(v["result"]["login"], json!(HOST_GL_LOGIN), "{v}");
    let url = pinned["url"].as_str().expect("url");
    assert!(
        url.starts_with(&format!("intent://invite?v=1&port={}&fp=", host.port)),
        "{url}"
    );
    assert!(!url.contains("host="), "tunnel-only link: {url}");
    assert!(url.contains(&format!("&secret={pinned_secret}")), "{url}");
    assert!(!url.contains(TOKEN), "no bearer token in the link: {url}");
    let v = wss_rpc(
        &mut owner,
        11,
        "workspace.invite.create",
        json!({ "workspaceId": ws_id, "pinLogin": GUEST_GL_LOGIN, "pinProvider": "github" }),
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32602), "{v}");
    assert_eq!(
        v["error"]["data"]["code"],
        json!("invite-pin-unknown"),
        "a github pin never resolves on gitlab: {v}"
    );
    // The explicit triple spelling of the same GitLab pin resolves alike.
    let (_, _, explicit) = create_invite(
        &mut owner,
        12,
        &ws_id,
        json!({ "pinLogin": GUEST_GL_LOGIN.to_uppercase(), "pinProvider": "gitlab", "pinHost": HOST }),
    )
    .await;
    assert_eq!(
        explicit["pinIdentity"],
        gitlab_identity(GUEST_GL_ID),
        "{explicit}"
    );

    // 2. An open (unpinned) link too.
    let (open_id, open_secret, open) = create_invite(&mut owner, 13, &ws_id, json!({})).await;
    assert!(open.get("pinIdentity").is_none(), "{open}");
    assert!(open.get("pinLogin").is_none(), "{open}");

    // 3. (c) The owner's own GitLab account proves against the open link →
    //    `owner-self-join`; no member was added.
    let mut prover = connect_invite(host.port, host.cfg.clone()).await;
    let nonce = challenge_nonce(&mut prover, 20, &open_id, &open_secret).await;
    let now = chrono::Utc::now().to_rfc3339();
    mock.script_snippet("4001", HOST_GL_PAT, &now, &nonce);
    let v = prove_gitlab(
        &mut prover,
        21,
        &open_id,
        &open_secret,
        &nonce,
        "4001",
        HOST_GL_LOGIN,
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32602), "{v}");
    assert_eq!(v["error"]["data"]["code"], json!("owner-self-join"), "{v}");
    let v = wss_rpc(
        &mut owner,
        30,
        "workspace.members.list",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert!(v.get("error").is_none(), "members.list: {v}");
    assert_eq!(
        v["result"]["members"].as_array().map(Vec::len),
        Some(1),
        "only the owner: {v}"
    );
    assert_eq!(
        v["result"]["members"][0]["identity"],
        gitlab_identity(HOST_GL_ID),
        "{v}"
    );

    // 4. The wrong GitLab account (neither the owner nor the pinned guest)
    //    against the pinned link → pin mismatch.
    let nonce = challenge_nonce(&mut prover, 22, &pinned_id, &pinned_secret).await;
    let now = chrono::Utc::now().to_rfc3339();
    mock.script_snippet("4002", INTRUDER_GL_PAT, &now, &nonce);
    let v = prove_gitlab(
        &mut prover,
        23,
        &pinned_id,
        &pinned_secret,
        &nonce,
        "4002",
        INTRUDER_GL_LOGIN,
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32602), "{v}");
    assert_eq!(
        v["error"]["data"]["code"],
        json!("invite-pin-mismatch"),
        "{v}"
    );

    // 4b. Input contract (multiplayer.md §invite.prove): an omitted
    //     `provider` is github — never inferred from the GitLab pin — so
    //     the guest's snippet id is looked up as a gist, which does not
    //     exist → `proof-invalid`; and `proofId` + `gistId` together are
    //     `-32602` before any forge call, even spelling the same id.
    let nonce = challenge_nonce(&mut prover, 27, &pinned_id, &pinned_secret).await;
    let now = chrono::Utc::now().to_rfc3339();
    mock.script_snippet("4004", GUEST_GL_PAT, &now, &nonce);
    let v = admitted_rpc(
        &mut prover,
        28,
        "invite.prove",
        json!({
            "inviteId": pinned_id, "secret": pinned_secret, "nonce": nonce,
            "proofId": "4004", "login": GUEST_GL_LOGIN,
        }),
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32602), "omitted provider: {v}");
    assert_eq!(
        v["error"]["data"]["code"],
        json!("proof-invalid"),
        "omitted provider is github, where no gist 4004 lives: {v}"
    );
    let v = admitted_rpc(
        &mut prover,
        29,
        "invite.prove",
        json!({
            "inviteId": pinned_id, "secret": pinned_secret, "nonce": nonce,
            "proofId": "4004", "gistId": "4004", "login": GUEST_GL_LOGIN, "provider": "gitlab",
        }),
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32602), "duplicate ids: {v}");
    assert!(
        v["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("exactly one")),
        "{v}"
    );

    // 5. (a) The pinned GitLab guest proves with its public snippet →
    //    authorized; it appears in members.list with the gitlab triple.
    let nonce = challenge_nonce(&mut prover, 24, &pinned_id, &pinned_secret).await;
    let now = chrono::Utc::now().to_rfc3339();
    mock.script_snippet("4003", GUEST_GL_PAT, &now, &nonce);
    let v = prove_gitlab(
        &mut prover,
        25,
        &pinned_id,
        &pinned_secret,
        &nonce,
        "4003",
        GUEST_GL_LOGIN,
    )
    .await;
    assert!(v.get("error").is_none(), "invite.prove: {v}");
    let r = &v["result"];
    assert_eq!(r["status"], json!("authorized"), "{r}");
    assert_eq!(r["workspaceId"], json!(ws_id), "{r}");
    assert_eq!(r["login"], json!(GUEST_GL_LOGIN), "{r}");
    let guest_token = r["token"].as_str().expect("guest token").to_string();
    let guest_principal = r["principalId"].as_str().expect("principalId").to_string();
    assert_ne!(guest_token, TOKEN);
    assert_eq!(
        mock.authenticated_snippet_reads.load(Ordering::SeqCst),
        0,
        "public snippets are read anonymously"
    );

    let v = wss_rpc(
        &mut owner,
        31,
        "workspace.members.list",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    let members = v["result"]["members"].as_array().expect("members");
    assert_eq!(members.len(), 2, "{v}");
    let guest = members
        .iter()
        .find(|m| m["principalId"] == json!(guest_principal))
        .unwrap_or_else(|| panic!("guest row: {v}"));
    assert_eq!(guest["login"], json!(GUEST_GL_LOGIN), "{guest}");
    assert_eq!(guest["identity"], gitlab_identity(GUEST_GL_ID), "{guest}");
    assert_eq!(guest["role"], json!("collaborator"), "{guest}");

    // The guest's credential opens a session whose `principal.me` is the
    // gitlab identity, and the pinned link is now spent.
    let mut guest_ws = connect_ws(host.port, host.cfg.clone(), &guest_token).await;
    let v = wss_rpc(&mut guest_ws, 40, "principal.me", json!({})).await;
    assert!(v.get("error").is_none(), "guest principal.me: {v}");
    assert_eq!(v["result"]["id"], json!(guest_principal), "{v}");
    assert_eq!(v["result"]["identity"], gitlab_identity(GUEST_GL_ID), "{v}");
    let v = admitted_rpc(
        &mut prover,
        26,
        "invite.challenge",
        json!({ "inviteId": pinned_id, "secret": pinned_secret }),
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32602), "spent pinned link: {v}");
}

/// (b): a snippet the instance serves only to authenticated readers. The
/// host connected to that instance verifies it with its own credential; a
/// host with no connection to it gets `identity-unverifiable` naming the
/// host, keeps the nonce, and succeeds once the snippet is public. An
/// instance answering a server error is `github-unreachable` (the code is
/// kept for both providers) and the same nonce succeeds on retry.
#[tokio::test]
async fn restricted_snippet_needs_the_hosts_own_connection_over_wss() {
    let mock = spawn_mock_forge().await;
    mock.private_snippets.store(true, Ordering::SeqCst);

    // The GitLab-connected host: anonymous read refused → its own PAT reads
    // the snippet → the guest joins.
    let connected = boot(&mock, &[("GITLAB_TOKEN", HOST_GL_PAT)]).await;
    let mut owner = connect_ws(connected.port, connected.cfg.clone(), TOKEN).await;
    let ws_id = create_workspace(&mut owner, 1, "Restricted snippets").await;
    let (invite_id, secret, _) = create_invite(&mut owner, 2, &ws_id, json!({})).await;
    let mut prover = connect_invite(connected.port, connected.cfg.clone()).await;
    let nonce = challenge_nonce(&mut prover, 10, &invite_id, &secret).await;
    let now = chrono::Utc::now().to_rfc3339();
    mock.script_snippet("5001", GUEST_GL_PAT, &now, &nonce);

    // The instance answering `503` while the host reads the snippet is the
    // documented `github-unreachable` — the code the GitHub era fixed for
    // both providers — and leaves the nonce usable.
    mock.snippet_server_error.store(true, Ordering::SeqCst);
    let v = prove_gitlab(
        &mut prover,
        11,
        &invite_id,
        &secret,
        &nonce,
        "5001",
        GUEST_GL_LOGIN,
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32603), "server error: {v}");
    assert_eq!(
        v["error"]["data"],
        json!({ "code": "github-unreachable" }),
        "{v}"
    );
    mock.snippet_server_error.store(false, Ordering::SeqCst);

    let v = prove_gitlab(
        &mut prover,
        12,
        &invite_id,
        &secret,
        &nonce,
        "5001",
        GUEST_GL_LOGIN,
    )
    .await;
    assert!(v.get("error").is_none(), "connected host verifies: {v}");
    assert_eq!(v["result"]["status"], json!("authorized"), "{v}");
    assert!(
        mock.authenticated_snippet_reads.load(Ordering::SeqCst) >= 1,
        "the host read the snippet with its own credential"
    );
    drop(prover);
    drop(owner);
    drop(connected);

    // A github.com host with no GitLab connection: the typed refusal names
    // the instance; the nonce survives for a retry, which succeeds once the
    // instance serves the snippet anonymously.
    let unconnected = boot(&mock, &[("GITHUB_TOKEN", OWNER_GH_TOKEN)]).await;
    let mut owner = connect_ws(unconnected.port, unconnected.cfg.clone(), TOKEN).await;
    let ws_id = create_workspace(&mut owner, 1, "No GitLab here").await;
    let (invite_id, secret, _) = create_invite(&mut owner, 2, &ws_id, json!({})).await;
    let v = wss_rpc(&mut owner, 3, "principal.me", json!({})).await;
    assert_eq!(
        v["result"]["identity"],
        json!({ "provider": "github", "host": "github.com", "externalUserId": OWNER_GH_ID.to_string() }),
        "{v}"
    );
    let mut prover = connect_invite(unconnected.port, unconnected.cfg.clone()).await;
    let nonce = challenge_nonce(&mut prover, 10, &invite_id, &secret).await;
    let now = chrono::Utc::now().to_rfc3339();
    mock.script_snippet("5002", GUEST_GL_PAT, &now, &nonce);
    let before = mock.authenticated_snippet_reads.load(Ordering::SeqCst);
    let v = prove_gitlab(
        &mut prover,
        11,
        &invite_id,
        &secret,
        &nonce,
        "5002",
        GUEST_GL_LOGIN,
    )
    .await;
    assert_eq!(v["error"]["code"], json!(-32603), "{v}");
    assert_eq!(
        v["error"]["data"],
        json!({ "code": "identity-unverifiable", "host": HOST }),
        "{v}"
    );
    assert_eq!(
        mock.authenticated_snippet_reads.load(Ordering::SeqCst),
        before,
        "a host without a GitLab credential sends none"
    );

    mock.private_snippets.store(false, Ordering::SeqCst);
    let v = prove_gitlab(
        &mut prover,
        12,
        &invite_id,
        &secret,
        &nonce,
        "5002",
        GUEST_GL_LOGIN,
    )
    .await;
    assert!(v.get("error").is_none(), "nonce kept, retry succeeds: {v}");
    assert_eq!(v["result"]["status"], json!("authorized"), "{v}");
    assert_eq!(v["result"]["login"], json!(GUEST_GL_LOGIN), "{v}");
    let v = wss_rpc(
        &mut owner,
        20,
        "workspace.members.list",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    let members = v["result"]["members"].as_array().expect("members");
    assert!(
        members
            .iter()
            .any(|m| m["identity"] == gitlab_identity(GUEST_GL_ID)),
        "gitlab guest on a github host: {v}"
    );
}

/// `identity.provider` (settings.md "Identity", §6.5): writing the setting
/// is the explicit re-key of the primary — admitted while an open invite
/// locks the identity — and publishes `principal:identity-changed
/// { principalId, identity }`: the selected forge's triple when it is
/// connected, `null` when it is not (the primary is left unlinked until it
/// connects; selecting a connected forge again re-links it). A forge that
/// is connected but unreachable (its `GET /user` fails with a server
/// error) defers the re-key: the cached identity stays, nothing is
/// published, and the selection succeeds once the forge answers again.
/// Unsetting the value is the implied resolution: with the current account
/// still qualifying nothing changes and nothing is published. A probe that
/// outlives the next write is superseded: neither its `401` (unlink) nor
/// its `200` (link) commits over the identity the newer write applied.
#[tokio::test]
async fn identity_provider_write_rekeys_the_primary_over_wss() {
    let mock = spawn_mock_forge().await;

    // Both forges connected: implied resolution is github (nothing set, no
    // identity yet); the open invite then locks the identity.
    let host = boot(
        &mock,
        &[
            ("GITLAB_TOKEN", HOST_GL_PAT),
            ("GITHUB_TOKEN", OWNER_GH_TOKEN),
        ],
    )
    .await;
    let mut owner = connect_ws(host.port, host.cfg.clone(), TOKEN).await;
    let mut sub = connect_ws(host.port, host.cfg.clone(), TOKEN).await;
    subscribe_identity_changed(&mut sub, 1).await;
    let ws_id = create_workspace(&mut owner, 2, "Identity re-key").await;
    create_invite(&mut owner, 3, &ws_id, json!({})).await;
    let v = wss_rpc(&mut owner, 4, "principal.me", json!({})).await;
    assert_eq!(v["result"]["identity"], github_identity(OWNER_GH_ID), "{v}");
    assert_eq!(v["result"]["login"], json!(OWNER_GH_LOGIN), "{v}");
    let principal_id = v["result"]["id"].clone();

    // 1. Explicit switch to the other connected forge while locked.
    set_identity_provider(&mut owner, 5, json!("gitlab")).await;
    let ev = next_event(&mut sub, "principal:identity-changed", 30).await;
    assert_eq!(
        ev["data"],
        json!({ "principalId": principal_id, "identity": gitlab_identity(HOST_GL_ID) }),
        "{ev}"
    );
    let v = wss_rpc(&mut owner, 6, "principal.me", json!({})).await;
    assert_eq!(v["result"]["identity"], gitlab_identity(HOST_GL_ID), "{v}");
    assert_eq!(v["result"]["login"], json!(HOST_GL_LOGIN), "{v}");

    // 2. Unset → implied: both connected, the current (gitlab) account is
    //    kept; the same account is no re-key and publishes nothing.
    let v = wss_rpc(
        &mut owner,
        7,
        "settings.reset",
        json!({ "path": "identity.provider" }),
    )
    .await;
    assert!(v.get("error").is_none(), "settings.reset: {v}");
    assert!(
        stays_quiet(&mut sub, "principal:identity-changed", 2_000).await,
        "unsetting the provider over the same account is not a re-key"
    );
    let v = wss_rpc(&mut owner, 8, "principal.me", json!({})).await;
    assert_eq!(v["result"]["identity"], gitlab_identity(HOST_GL_ID), "{v}");

    // 3. Select github while github.com answers `GET /user` with 503: an
    //    outage is not a disconnection — the re-key is deferred, the
    //    cached gitlab identity is kept and no `identity: null` (or any
    //    other) event is published.
    mock.github_user.server_error.store(true, Ordering::SeqCst);
    set_identity_provider(&mut owner, 9, json!("github")).await;
    assert!(
        stays_quiet(&mut sub, "principal:identity-changed", 2_000).await,
        "an unreachable forge defers the re-key instead of unlinking"
    );
    let v = wss_rpc(&mut owner, 10, "principal.me", json!({})).await;
    assert_eq!(
        v["result"]["identity"],
        gitlab_identity(HOST_GL_ID),
        "cached identity retained through the outage: {v}"
    );
    assert_eq!(v["result"]["login"], json!(HOST_GL_LOGIN), "{v}");

    // 4. Retry when github.com answers `GET /user` exactly once before
    //    failing again: the single typed read decides, so the selection
    //    connects and publishes the github triple. (A liveness re-check
    //    that folded the following 503 into "not authenticated" would
    //    have unlinked the primary here instead.) The value has to move
    //    for the write hook to fire — an unchanged write applies nothing —
    //    so select gitlab first: the current account, no read of
    //    github.com's `/user`, nothing published.
    set_identity_provider(&mut owner, 11, json!("gitlab")).await;
    assert!(
        stays_quiet(&mut sub, "principal:identity-changed", 2_000).await,
        "re-selecting the current account is not a re-key"
    );
    mock.github_user.grace.store(1, Ordering::SeqCst);
    set_identity_provider(&mut owner, 12, json!("github")).await;
    let ev = next_event(&mut sub, "principal:identity-changed", 30).await;
    assert_eq!(
        ev["data"],
        json!({ "principalId": principal_id, "identity": github_identity(OWNER_GH_ID) }),
        "{ev}"
    );
    assert_eq!(
        mock.github_user.grace.load(Ordering::SeqCst),
        0,
        "the re-key read github.com's `/user` once"
    );
    mock.github_user.server_error.store(false, Ordering::SeqCst);
    let v = wss_rpc(&mut owner, 13, "principal.me", json!({})).await;
    assert_eq!(v["result"]["identity"], github_identity(OWNER_GH_ID), "{v}");
    assert_eq!(v["result"]["login"], json!(OWNER_GH_LOGIN), "{v}");

    // 5. A superseded probe never unlinks. Back on gitlab, select github
    //    while github.com holds `GET /user` (it will answer 401), switch
    //    back to gitlab while that probe is in flight, then let the 401
    //    land: the probe belongs to an earlier write, so it is dropped —
    //    the gitlab identity the newer write applied stays and no
    //    `identity: null` is published.
    set_identity_provider(&mut owner, 14, json!("gitlab")).await;
    let ev = next_event(&mut sub, "principal:identity-changed", 30).await;
    assert_eq!(
        ev["data"],
        json!({ "principalId": principal_id, "identity": gitlab_identity(HOST_GL_ID) }),
        "{ev}"
    );
    mock.github_user.unauthorized.store(true, Ordering::SeqCst);
    mock.github_user.hold.send_replace(true);
    set_identity_provider(&mut owner, 15, json!("github")).await;
    mock.github_user.held_reads(1).await;
    set_identity_provider(&mut owner, 16, json!("gitlab")).await;
    mock.github_user.hold.send_replace(false);
    assert!(
        stays_quiet(&mut sub, "principal:identity-changed", 2_000).await,
        "a stale `not connected` result must not unlink the newer identity"
    );
    let v = wss_rpc(&mut owner, 17, "principal.me", json!({})).await;
    assert_eq!(
        v["result"]["identity"],
        gitlab_identity(HOST_GL_ID),
        "the newer choice survives the superseded probe: {v}"
    );
    assert_eq!(v["result"]["login"], json!(HOST_GL_LOGIN), "{v}");
    mock.github_user.unauthorized.store(false, Ordering::SeqCst);
    drop(sub);
    drop(owner);
    drop(host);

    // A single-user host (nothing locks the identity): a superseded probe
    // that comes back *connected* does not overwrite the newer choice
    // either. Select gitlab (the first explicit link publishes), then github
    // with its `GET /user` held, then gitlab again before the held read
    // answers `200`: the stale github account is dropped, not applied.
    let host = boot(
        &mock,
        &[
            ("GITLAB_TOKEN", HOST_GL_PAT),
            ("GITHUB_TOKEN", OWNER_GH_TOKEN),
        ],
    )
    .await;
    let mut owner = connect_ws(host.port, host.cfg.clone(), TOKEN).await;
    let mut sub = connect_ws(host.port, host.cfg.clone(), TOKEN).await;
    subscribe_identity_changed(&mut sub, 1).await;
    set_identity_provider(&mut owner, 2, json!("gitlab")).await;
    let ev = next_event(&mut sub, "principal:identity-changed", 30).await;
    assert_eq!(ev["data"]["identity"], gitlab_identity(HOST_GL_ID), "{ev}");
    let principal_id = ev["data"]["principalId"].clone();
    let v = wss_rpc(&mut owner, 3, "principal.me", json!({})).await;
    assert_eq!(v["result"]["id"], principal_id, "{v}");
    assert_eq!(v["result"]["identity"], gitlab_identity(HOST_GL_ID), "{v}");
    mock.github_user.hold.send_replace(true);
    set_identity_provider(&mut owner, 4, json!("github")).await;
    mock.github_user.held_reads(2).await;
    set_identity_provider(&mut owner, 5, json!("gitlab")).await;
    mock.github_user.hold.send_replace(false);
    assert!(
        stays_quiet(&mut sub, "principal:identity-changed", 2_000).await,
        "a stale connected result must not overwrite the newer choice"
    );
    let v = wss_rpc(&mut owner, 6, "principal.me", json!({})).await;
    assert_eq!(
        v["result"]["identity"],
        gitlab_identity(HOST_GL_ID),
        "the newer choice survives the superseded probe: {v}"
    );
    assert_eq!(v["result"]["login"], json!(HOST_GL_LOGIN), "{v}");
    drop(sub);
    drop(owner);
    drop(host);

    // A GitLab-only host: selecting github — not connected — unlinks the
    // primary (`identity: null`); selecting gitlab again re-links it.
    let host = boot(&mock, &[("GITLAB_TOKEN", HOST_GL_PAT)]).await;
    let mut owner = connect_ws(host.port, host.cfg.clone(), TOKEN).await;
    let mut sub = connect_ws(host.port, host.cfg.clone(), TOKEN).await;
    subscribe_identity_changed(&mut sub, 1).await;
    let ws_id = create_workspace(&mut owner, 2, "Identity unlink").await;
    create_invite(&mut owner, 3, &ws_id, json!({})).await;
    let v = wss_rpc(&mut owner, 4, "principal.me", json!({})).await;
    assert_eq!(v["result"]["identity"], gitlab_identity(HOST_GL_ID), "{v}");
    let principal_id = v["result"]["id"].clone();

    set_identity_provider(&mut owner, 5, json!("github")).await;
    let ev = next_event(&mut sub, "principal:identity-changed", 30).await;
    assert_eq!(
        ev["data"],
        json!({ "principalId": principal_id, "identity": null }),
        "{ev}"
    );
    let v = wss_rpc(&mut owner, 6, "principal.me", json!({})).await;
    assert!(v.get("error").is_none(), "principal.me: {v}");
    assert!(
        v["result"].get("identity").is_none(),
        "unlinked primary: {v}"
    );
    assert!(v["result"]["login"].is_null(), "unlinked primary: {v}");
    assert_eq!(v["result"]["id"], principal_id, "{v}");

    set_identity_provider(&mut owner, 7, json!("gitlab")).await;
    let ev = next_event(&mut sub, "principal:identity-changed", 30).await;
    assert_eq!(
        ev["data"],
        json!({ "principalId": principal_id, "identity": gitlab_identity(HOST_GL_ID) }),
        "{ev}"
    );
    let v = wss_rpc(&mut owner, 8, "principal.me", json!({})).await;
    assert_eq!(v["result"]["identity"], gitlab_identity(HOST_GL_ID), "{v}");
    assert_eq!(v["result"]["login"], json!(HOST_GL_LOGIN), "{v}");
}
