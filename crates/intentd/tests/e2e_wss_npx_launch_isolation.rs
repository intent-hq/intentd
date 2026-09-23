//! WSS e2e for npx startup isolation (intent-hq/intent#5738): an npx-only
//! provider (claude-code) launched inside a Bun/pnpm workspace whose
//! `package.json` uses `catalog:` specifiers must still come up, because the
//! daemon runs `npx -y <pinned adapter>` in a neutral, empty directory rather
//! than the workspace — while the ACP `session/new` still names the real
//! workspace (a path containing a space) as its `cwd`.
//!
//! Hermetic setup: the daemon child's `PATH` starts with a scratch `bin/`
//! holding a fake `npx` that behaves like npm inside a `catalog:` workspace —
//! it records its cwd + argv, fails with `EUNSUPPORTEDPROTOCOL` when a
//! `package.json` is present in its cwd, and otherwise execs the deterministic
//! mock ACP fixture under `node`. Nothing is downloaded. The fixture's
//! `MOCK_AGENT_SESSION_LOG` seam records the `session/new` `cwd` param and
//! the child's actual process cwd.
//!
//! Gated on `node` + the mock script; skips cleanly otherwise.

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::Stdio;
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
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

/// Fixed 64-hex token, adopted by the daemon via the `INTENTD_AUTH_TOKEN` seam.
const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

/// Live `intentd serve` process; killed (whole process group) on drop, with
/// the daemon log echoed for post-mortems.
struct Daemon {
    child: GuardedChild,
    data_dir: tempfile::TempDir,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let log_path = self.data_dir.path().join("daemon.log");
        if let Ok(log) = std::fs::read_to_string(&log_path) {
            eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
        }
    }
}

fn temp_data_dir() -> tempfile::TempDir {
    common::test_tempdir_in("/tmp", "itd-wss-npxiso-")
}

/// The hermetic workspaces root — deliberately containing a space so the ACP
/// session cwd shape with whitespace is exercised end to end.
fn workspaces_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("work spaces")
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> GuardedChild {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = workspaces_dir(data_dir);
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(data_dir);
    let mut cmd = common::serve_command();
    cmd.env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        // The mock is a Node child of the daemon; host-injected Node
        // instrumentation would slow every start (intent-hq/intent#5649).
        .env_remove("NODE_OPTIONS")
        .env("DD_TRACE_ENABLED", "false")
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

/// Send one JSON-RPC frame and return the result whose id matches; any
/// out-of-band notifications (`events.event`) are ignored.
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

/// Read one `events.event` notification from a subscriber connection (bounded).
async fn wss_event<S>(ws: &mut WebSocketStream<S>, secs: u64) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let next = timeout(Duration::from_secs(secs), ws.next())
            .await
            .expect("wss event timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "events.event" {
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

/// Drain subscriber events until the turn settles for `agent_id`: returns the
/// terminal event type (`agent:stream:end` on success, `agent:failed` when
/// every spawn attempt died — the pre-fix outcome in a `catalog:` workspace).
async fn await_turn_settled<S>(sub: &mut WebSocketStream<S>, agent_id: &str) -> String
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    for _ in 0..200 {
        let frame = wss_event(sub, 60).await;
        let ev = &frame["params"]["event"];
        if ev["data"]["agentId"].as_str() != Some(agent_id) {
            continue;
        }
        let ty = ev["type"].as_str().unwrap_or_default();
        if ty == "agent:stream:end" || ty == "agent:failed" {
            return ty.to_string();
        }
    }
    panic!("turn never settled for {agent_id}");
}

/// Mock-agent gate (parity with the WSS lifecycle suite).
fn gate(test: &str) -> Option<String> {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping {test}: node not on PATH");
        return None;
    }
    if !Path::new(&script).exists() {
        eprintln!("skipping {test}: mock script missing at {script}");
        return None;
    }
    if intent_providers::resolve_on_path("git").is_none() {
        eprintln!("skipping {test}: git not on PATH");
        return None;
    }
    Some(script)
}

fn run_git(args: &[&str], cwd: &Path) -> String {
    let out = std::process::Command::new("git")
        .args(args)
        .env("GIT_AUTHOR_NAME", "e2e")
        .env("GIT_AUTHOR_EMAIL", "e2e@example.com")
        .env("GIT_COMMITTER_NAME", "e2e")
        .env("GIT_COMMITTER_EMAIL", "e2e@example.com")
        .current_dir(cwd)
        .stderr(Stdio::null())
        .output()
        .expect("run git");
    assert!(out.status.success(), "git {args:?} failed");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// A one-commit source repository the workspace is checked out from: a
/// workspace needs a real checkout (`path` / `worktreePath`) to be the
/// agent's cwd — a repository-less workspace spawns its agent in the temp dir.
fn make_source_repo(dir: &Path) -> PathBuf {
    let repo = dir.join("source-repo");
    std::fs::create_dir_all(&repo).expect("mkdir source repo");
    run_git(&["init", "-q", "-b", "main"], &repo);
    std::fs::write(repo.join("README.md"), "hello\n").unwrap();
    run_git(&["add", "README.md"], &repo);
    run_git(&["commit", "-q", "-m", "init"], &repo);
    repo
}

/// Write the fake `npx` into `bin_dir`: it appends its cwd to `<report>.cwd`
/// and its argv to `<report>.args`, then fails exactly like npm inside a
/// `catalog:` workspace when a `package.json` is present in its cwd
/// (`EUNSUPPORTEDPROTOCOL`), and otherwise execs the mock fixture under `node`.
fn write_fake_npx(bin_dir: &Path, report: &Path, script: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let node = intent_providers::resolve_on_path("node").expect("node on PATH (gated)");
    let npx = bin_dir.join("npx");
    std::fs::write(
        &npx,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$PWD\" >> '{report}.cwd'\nprintf '%s\\n' \"$*\" >> '{report}.args'\n\
             if [ -e package.json ]; then\n  echo 'npm error code EUNSUPPORTEDPROTOCOL' >&2\n  \
             echo 'npm error Unsupported URL Type \"catalog:\": catalog:' >&2\n  exit 1\nfi\n\
             exec '{node}' '{script}'\n",
            report = report.display(),
            node = node.display(),
        ),
    )
    .expect("write fake npx");
    std::fs::set_permissions(&npx, std::fs::Permissions::from_mode(0o755)).expect("chmod npx");
    npx
}

/// Turn `workspace` into the failing fixture: a pnpm/Bun workspace whose
/// manifest uses `catalog:` specifiers (what npm rejects with
/// `EUNSUPPORTEDPROTOCOL` when run from inside it).
fn seed_catalog_workspace(workspace: &Path) {
    std::fs::write(
        workspace.join("package.json"),
        r#"{"name":"catalog-workspace","private":true,"dependencies":{"zod":"catalog:"}}"#,
    )
    .expect("write package.json");
    std::fs::write(
        workspace.join("pnpm-workspace.yaml"),
        "packages:\n  - packages/*\ncatalog:\n  zod: ^3.23.0\n",
    )
    .expect("write pnpm-workspace.yaml");
}

/// Parse the fixture's session log into `(method, cwd, processCwd)` rows.
fn read_session_log(path: &Path) -> Vec<(String, Value, Value)> {
    let raw = std::fs::read_to_string(path).unwrap_or_default();
    raw.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let v: Value = serde_json::from_str(l).expect("session log line json");
            (
                v["method"].as_str().expect("method").to_string(),
                v["cwd"].clone(),
                v["processCwd"].clone(),
            )
        })
        .collect()
}

/// NPX LAUNCH ISOLATION (intent-hq/intent#5738): a claude-code agent in a
/// `catalog:` workspace comes up — npx runs in a neutral empty directory,
/// never the workspace — and its `session/new` still names the workspace as
/// the ACP cwd, spaces included.
#[tokio::test]
async fn npx_launch_runs_outside_the_workspace_while_session_cwd_is_the_workspace() {
    let Some(script) = gate("WSS npx launch isolation E2E") else {
        return;
    };

    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let bin_dir = data_dir.join("bin");
    let home_dir = data_dir.join("home");
    std::fs::create_dir_all(&bin_dir).unwrap();
    std::fs::create_dir_all(&home_dir).unwrap();
    let report = data_dir.join("npx-report");
    write_fake_npx(&bin_dir, &report, &script);
    let source_repo = make_source_repo(&data_dir);
    let session_log = data_dir.join("sessions.txt");
    let session_log_s = session_log.to_string_lossy().into_owned();
    // `bin/` first: `find_npx` scans the inherited PATH ahead of the enriched
    // tool dirs, so the fake npx wins over any real install on the host.
    let path = format!("{}:/usr/bin:/bin", bin_dir.display());
    let behavior = json!({ "response": "NPX_ISOLATION_E2E_REPLY" }).to_string();
    let env: [(&str, &str); 7] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("PATH", path.as_str()),
        ("HOME", home_dir.to_str().unwrap()),
        ("SHELL", "/bin/sh"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_SESSION_LOG", &session_log_s),
    ];
    let child = spawn_serve(&data_dir, &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir_guard,
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let cfg = client_config(
        status["result"]["fingerprint"]
            .as_str()
            .expect("fingerprint"),
    );

    // SUBSCRIBER conn — events.subscribe BEFORE the turn so we miss nothing.
    // The workspace is a real checkout of `source_repo` under the hermetic
    // root, so it (not the temp dir) is the agent's cwd.
    let mut sub = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut sub,
        1,
        "workspace.create",
        json!({
            "title": "Npx Isolation E2E",
            "repositoryPath": source_repo.to_string_lossy(),
            "repositoryName": "source-repo",
            "baseRef": "main",
            "noPrompt": true,
        }),
    )
    .await;
    let ws_id = created["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let workspace = PathBuf::from(
        created["workspace"]["path"]
            .as_str()
            .or_else(|| created["workspace"]["worktreePath"].as_str())
            .unwrap_or_else(|| panic!("workspace checkout path: {created}")),
    );
    assert!(
        workspace.starts_with(workspaces_dir(&data_dir)),
        "workspace {} lives under the hermetic root",
        workspace.display()
    );
    assert!(
        workspace.to_string_lossy().contains(' '),
        "workspace path must contain a space: {}",
        workspace.display()
    );
    seed_catalog_workspace(&workspace);
    let sub_resp = wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(
        sub_resp["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // RPC conn — create the agent on the npx-only claude-code provider.
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({
            "workspaceId": ws_id,
            "name": "Npx Isolation E2E",
            "model": "mock-model", "provider": "claude-code",
        }),
    )
    .await;
    let agent_id = created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "first turn" }),
    )
    .await;
    assert_eq!(sent["success"], true, "sendMessage accepted: {sent}");
    let terminal = await_turn_settled(&mut sub, &agent_id).await;

    // The fake npx ran — with the pinned argv — and never inside the workspace.
    let args = std::fs::read_to_string(format!("{}.args", report.display()))
        .expect("fake npx recorded its argv");
    let first_args = args.lines().next().expect("at least one npx launch");
    assert_eq!(
        first_args,
        format!("-y {}", intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE),
        "claude-code npx argv is the pinned package"
    );
    let cwds = std::fs::read_to_string(format!("{}.cwd", report.display()))
        .expect("fake npx recorded its cwd");
    let npx_cwds: Vec<PathBuf> = cwds.lines().map(PathBuf::from).collect();
    assert!(!npx_cwds.is_empty(), "fake npx recorded at least one cwd");
    let canonical_workspace = std::fs::canonicalize(&workspace).expect("workspace exists");
    for npx_cwd in &npx_cwds {
        let canonical = std::fs::canonicalize(npx_cwd).unwrap_or_else(|_| npx_cwd.clone());
        assert!(
            !canonical.starts_with(&canonical_workspace),
            "npx ran inside the workspace ({}): launches {npx_cwds:?}",
            npx_cwd.display()
        );
    }
    assert_eq!(
        terminal, "agent:stream:end",
        "the turn must complete — a spawn dying on `catalog:` ends in agent:failed"
    );

    // The ACP session still targets the real workspace: `session/new.cwd` is
    // the workspace path, while the child's own process cwd is the neutral
    // npx launch directory.
    let log = read_session_log(&session_log);
    assert_eq!(log.len(), 1, "exactly one session opened: {log:?}");
    let (method, cwd, process_cwd) = &log[0];
    assert_eq!(method, "session/new");
    let session_cwd = PathBuf::from(cwd.as_str().expect("session/new carries cwd"));
    assert_eq!(
        std::fs::canonicalize(&session_cwd).unwrap_or(session_cwd.clone()),
        canonical_workspace,
        "session/new cwd must be the workspace (spaces included): {cwd}"
    );
    let process_cwd = PathBuf::from(process_cwd.as_str().expect("processCwd"));
    assert_ne!(
        std::fs::canonicalize(&process_cwd).unwrap_or(process_cwd.clone()),
        canonical_workspace,
        "the adapter process must not run inside the workspace"
    );
}
