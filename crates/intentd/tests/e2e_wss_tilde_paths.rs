//! WSS e2e regression for intent-hq/monorepo#822: caller-supplied paths with a
//! leading `~` (`workspace.create` `clonePath`/`repositoryPath`, `git.clone`
//! `parentDir`) must expand to the daemon's `$HOME` instead of being passed
//! through to git literally (which resolves `./~/...` relative to the daemon
//! cwd — a read-only filesystem in the packaged sidecar).
//!
//! Each test spawns a real `intentd serve` with `HOME` overridden to a temp
//! directory and drives the JSON-RPC methods over a pinned-TLS WebSocket.
//! Local `file://` fixture repos keep everything off the network. Gated on
//! `git` being on PATH; skips cleanly otherwise.

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
    home: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
        let _ = std::fs::remove_dir_all(&self.home);
    }
}

fn scratch_dir(prefix: &str) -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-tilde-{prefix}-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir scratch dir");
    dir
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(data_dir);
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

async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(Duration::from_secs(60), ws.next())
            .await
            .expect("wss rpc timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["id"] == json!(id) {
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

/// Read `git:clone:*` frames for `request_id` until the terminal done frame or
/// the deadline. Non-matching frames are ignored.
async fn drain_clone_events<S>(
    ws: &mut WebSocketStream<S>,
    request_id: &str,
    secs: u64,
) -> Vec<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut out: Vec<Value> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return out;
        }
        let next = match timeout(remaining, ws.next()).await {
            Ok(x) => x,
            Err(_) => return out,
        };
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = match serde_json::from_str(&text) {
                    Ok(x) => x,
                    Err(_) => continue,
                };
                if v["method"] == json!("events.event") {
                    let evt = &v["params"]["event"];
                    let ty = evt["type"].as_str().unwrap_or("");
                    if evt["data"]["requestId"].as_str() == Some(request_id)
                        && (ty == "git:clone:progress" || ty == "git:clone:done")
                    {
                        let done = ty == "git:clone:done";
                        out.push(evt.clone());
                        if done {
                            return out;
                        }
                    }
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            None => return out,
            Some(Err(_)) => return out,
        }
    }
}

/// Gate: skip if `git` is not on PATH.
fn gate() -> bool {
    match Command::new("git")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
    {
        Ok(s) if s.success() => true,
        _ => {
            eprintln!("skipping tilde-path WSS e2e: git not on PATH");
            false
        }
    }
}

/// Init a small local repo (one commit on `main`) at `dir` and return it.
fn seed_repo(dir: &Path) -> PathBuf {
    std::fs::create_dir_all(dir).expect("mkdir seed repo");
    let run = |args: &[&str]| {
        let status = Command::new("git")
            .args(args)
            .env("GIT_AUTHOR_NAME", "e2e")
            .env("GIT_AUTHOR_EMAIL", "e2e@example.com")
            .env("GIT_COMMITTER_NAME", "e2e")
            .env("GIT_COMMITTER_EMAIL", "e2e@example.com")
            .current_dir(dir)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("run git");
        assert!(status.success(), "git {args:?} failed");
    };
    run(&["init", "-q", "-b", "main"]);
    std::fs::write(dir.join("README.md"), "hello\n").unwrap();
    run(&["add", "README.md"]);
    run(&["commit", "-q", "-m", "init"]);
    dir.to_path_buf()
}

/// Boot a daemon with `HOME` pointed at a fresh temp directory. Returns the
/// daemon guard, the fake home, the WSS port, and the pinned client config.
async fn boot() -> (Daemon, PathBuf, u16, Arc<ClientConfig>) {
    let data_dir = scratch_dir("data");
    let home = scratch_dir("home");
    let home_str = home.to_string_lossy().into_owned();
    let env: [(&str, &str); 3] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("HOME", &home_str),
    ];
    let child = spawn_serve(&data_dir, &env);
    let daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
        home: home.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port = status["result"]["port"].as_u64().expect("port") as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);
    (daemon, home, port, cfg)
}

/// `workspace.create { githubUrl, clonePath: "~/Developer/repo" }` clones into
/// `$HOME/Developer/repo` and persists the expanded path as `repositoryPath`
/// (the #822 onboarding failure: the literal `~` used to reach `git clone`).
#[tokio::test]
async fn workspace_create_expands_tilde_clone_path_over_wss() {
    if !gate() {
        return;
    }
    let (daemon, home, port, cfg) = boot().await;
    let source = seed_repo(&daemon.data_dir.join("clone-src"));

    let mut rpc = connect_ws(port, cfg).await;
    let created = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({
            "title": "Tilde clone",
            "branch": "feat/tilde-clone",
            "githubUrl": format!("file://{}", source.display()),
            "clonePath": "~/Developer/repo",
        }),
    )
    .await;

    let expanded = home.join("Developer").join("repo");
    assert_eq!(
        created["workspace"]["repositoryPath"],
        expanded.to_string_lossy().as_ref(),
        "repositoryPath persisted expanded: {created}"
    );
    assert!(
        expanded.join(".git").exists(),
        "clone landed under expanded $HOME path"
    );
}

/// `workspace.create { repositoryPath: "~/..." }` expands before the
/// existing-local-repo check and persistence: the workspace provisions from
/// the checkout under `$HOME` and persists the expanded path.
#[tokio::test]
async fn workspace_create_expands_tilde_repository_path_over_wss() {
    if !gate() {
        return;
    }
    let (_daemon, home, port, cfg) = boot().await;
    let repo = seed_repo(&home.join("existing-repo"));

    let mut rpc = connect_ws(port, cfg).await;
    let created = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({
            "title": "Tilde repo path",
            "repositoryPath": "~/existing-repo",
            "baseRef": "main",
            "branch": "feat/tilde-repo",
        }),
    )
    .await;

    assert_eq!(
        created["workspace"]["repositoryPath"],
        repo.to_string_lossy().as_ref(),
        "repositoryPath persisted expanded: {created}"
    );
    let wt = created["workspace"]["worktreePath"]
        .as_str()
        .expect("worktreePath");
    assert!(
        Path::new(wt).exists(),
        "worktree provisioned from the expanded checkout"
    );
}

/// `git.clone { parentDir: "~/clones" }` resolves the target under `$HOME` —
/// the ack's `targetPath` is expanded and the checkout lands there.
#[tokio::test]
async fn git_clone_expands_tilde_parent_dir_over_wss() {
    if !gate() {
        return;
    }
    let (daemon, home, port, cfg) = boot().await;
    let source = seed_repo(&daemon.data_dir.join("clone-src"));

    // Subscribe BEFORE issuing the clone so the terminal frame is not missed.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let sub_res = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["git:clone:progress", "git:clone:done"] }),
    )
    .await;
    assert!(sub_res["subscriptionId"].is_string(), "sub id: {sub_res}");

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let request_id = "tilde-clone-1";
    let ack = wss_rpc(
        &mut rpc,
        2,
        "git.clone",
        json!({
            "url": format!("file://{}", source.display()),
            "parentDir": "~/clones",
            "targetName": "repo",
            "requestId": request_id,
        }),
    )
    .await;
    let expanded = home.join("clones").join("repo");
    assert_eq!(
        ack["targetPath"],
        expanded.to_string_lossy().as_ref(),
        "ack targetPath expanded: {ack}"
    );

    let events = drain_clone_events(&mut sub, request_id, 30).await;
    let done = events
        .iter()
        .find(|e| e["type"] == json!("git:clone:done"))
        .cloned()
        .expect("terminal git:clone:done event");
    assert_eq!(done["data"]["ok"], json!(true), "done ok=true: {done}");
    assert!(
        expanded.join(".git").exists(),
        "clone landed under expanded $HOME path"
    );
}
