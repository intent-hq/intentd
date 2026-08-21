//! WSS e2e test for setup script methods: workspace.create's setupScript param is
//! execute-only (never persisted), workspace.saveSetupScript writes/reads via repo
//! config, workspace.getSetupScript reads from repo config with DB fallback (per
//! AGENTS.md testing gate requirement).

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use futures_util::{SinkExt, StreamExt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "cfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcfcf";

struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let log_path = self.data_dir.join("daemon.log");
        if let Ok(log) = std::fs::read_to_string(&log_path) {
            eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-setup-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    if listen != "uds" {
        common::enable_ws_api(data_dir);
    }
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

async fn uds_rpc(socket: &Path, id: i64, method: &str, params: Value) -> Value {
    let stream = UnixStream::connect(socket).await.expect("connect uds");
    let (read_half, mut write_half) = stream.into_split();
    let mut line = serde_json::to_string(
        &json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }),
    )
    .unwrap();
    line.push('\n');
    write_half.write_all(line.as_bytes()).await.unwrap();
    write_half.flush().await.unwrap();
    let mut reader = BufReader::new(read_half);
    let mut buf = String::new();
    timeout(common::rpc_read_timeout(), reader.read_line(&mut buf))
        .await
        .expect("uds rpc timed out")
        .expect("read uds response");
    serde_json::from_str(buf.trim_end()).expect("invalid JSON frame")
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

async fn wss_connect(
    port: u16,
    fingerprint: &str,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let cfg = client_config(fingerprint);
    connect_ws(port, cfg).await
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

/// Poll `terminal.list` until the workspace's setup terminal (named "Setup
/// Script") is running, returning its live list entry. Uses a generous
/// deadline so slow CI machines don't flake.
async fn await_running_setup_terminal<S>(
    ws: &mut WebSocketStream<S>,
    base_id: i64,
    workspace_id: &str,
) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut attempt = 0;
    while tokio::time::Instant::now() < deadline {
        let list_resp = wss_rpc(
            ws,
            base_id + attempt,
            "terminal.list",
            json!({ "workspaceId": workspace_id }),
        )
        .await;
        attempt += 1;
        let terminals = list_resp["result"]["terminals"]
            .as_array()
            .expect("terminal.list terminals array");
        if let Some(entry) = terminals
            .iter()
            .find(|t| t["name"] == json!("Setup Script") && t["isExecutingCommand"] == json!(true))
        {
            return entry.clone();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("running setup terminal named \"Setup Script\" never appeared for {workspace_id}");
}

/// Poll `terminal.getBuffer` until the decoded scrollback contains `needle`,
/// returning the buffer. Setup terminals disappear from `terminal.list` on
/// exit, but their retained PTY sessions remain readable by id.
async fn await_buffer_contains<S>(
    ws: &mut WebSocketStream<S>,
    base_id: i64,
    terminal_id: &str,
    needle: &str,
) -> String
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut attempt = 0;
    loop {
        let buffer = terminal_buffer(ws, base_id + attempt, terminal_id).await;
        attempt += 1;
        if buffer.contains(needle) {
            return buffer;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("buffer never contained {needle:?}: {buffer:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Poll `terminal.list` until the exited setup terminal is no longer returned.
async fn await_terminal_omitted<S>(
    ws: &mut WebSocketStream<S>,
    base_id: i64,
    workspace_id: &str,
    terminal_id: &str,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut attempt = 0;
    loop {
        let list_resp = wss_rpc(
            ws,
            base_id + attempt,
            "terminal.list",
            json!({ "workspaceId": workspace_id }),
        )
        .await;
        attempt += 1;
        let terminals = list_resp["result"]["terminals"]
            .as_array()
            .expect("terminal.list terminals array");
        if terminals.iter().all(|entry| entry["id"] != terminal_id) {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("exited setup terminal {terminal_id} remained in terminal.list");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Fetch a terminal's decoded scrollback via `terminal.getBuffer`.
async fn terminal_buffer<S>(ws: &mut WebSocketStream<S>, id: i64, terminal_id: &str) -> String
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let resp = wss_rpc(
        ws,
        id,
        "terminal.getBuffer",
        json!({ "terminalId": terminal_id }),
    )
    .await;
    assert_eq!(resp["result"]["terminalId"], json!(terminal_id));
    let data = resp["result"]["data"].as_str().expect("base64 data");
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data)
        .expect("valid base64 buffer");
    String::from_utf8_lossy(&bytes).into_owned()
}

/// Committed `.intent/config.json` content used by `create_test_repo` — tests
/// assert byte-identity against this exact string after `workspace.create`.
const COMMITTED_CONFIG: &str = r#"{"setupScript": "pnpm install"}"#;

/// Create a git repo without any committed `.intent/config.json`.
fn create_bare_test_repo() -> PathBuf {
    let repo_path = std::env::temp_dir().join(format!("setup-repo-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&repo_path).expect("create temp repo dir");
    let status = std::process::Command::new("git")
        .args(["init", "--initial-branch=main"])
        .current_dir(&repo_path)
        .status()
        .expect("git init spawn");
    assert!(status.success(), "git init failed");
    let status = std::process::Command::new("git")
        .args(["config", "user.email", "test@example.com"])
        .current_dir(&repo_path)
        .status()
        .expect("git config email spawn");
    assert!(status.success(), "git config email failed");
    let status = std::process::Command::new("git")
        .args(["config", "user.name", "Test"])
        .current_dir(&repo_path)
        .status()
        .expect("git config name spawn");
    assert!(status.success(), "git config name failed");
    std::fs::write(repo_path.join("README.md"), "# Test\n").expect("write readme");
    let status = std::process::Command::new("git")
        .args(["add", "."])
        .current_dir(&repo_path)
        .status()
        .expect("git add spawn");
    assert!(status.success(), "git add failed");
    let status = std::process::Command::new("git")
        .args(["commit", "-m", "initial commit"])
        .current_dir(&repo_path)
        .status()
        .expect("git commit spawn");
    assert!(status.success(), "git commit failed");

    repo_path
}

fn create_test_repo() -> PathBuf {
    let repo_path = create_bare_test_repo();

    // Commit a setup script in .intent/config.json to test inheritance
    std::fs::create_dir_all(repo_path.join(".intent")).expect("create .intent dir");
    std::fs::write(repo_path.join(".intent/config.json"), COMMITTED_CONFIG).expect("write config");
    let status = std::process::Command::new("git")
        .args(["add", ".intent/config.json"])
        .current_dir(&repo_path)
        .status()
        .expect("git add config spawn");
    assert!(status.success(), "git add config failed");
    let status = std::process::Command::new("git")
        .args(["commit", "-m", "add setup script"])
        .current_dir(&repo_path)
        .status()
        .expect("git commit config spawn");
    assert!(status.success(), "git commit config failed");

    repo_path
}

/// WSS e2e coverage for setup script methods: workspace.create's setupScript is
/// execute-only (no repo-config write, no DB write), saveSetupScript writes repo config,
/// getSetupScript reads from repo config with legacy DB fallback (§5.1 / §5.25).
#[tokio::test]
async fn setup_script_repo_config_sole_source() {
    let data_dir = temp_data_dir();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    let repo_path = create_test_repo();

    // Create a workspace with an explicit setupScript
    let create_resp = uds_rpc(
        &socket,
        1,
        "workspace.create",
        json!({
            "title": "test-setup",
            "repositoryPath": repo_path.to_string_lossy(),
            "setupScript": "npm install"
        }),
    )
    .await;
    let workspace_id = create_resp["result"]["workspace"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Assert the workspace DB row has NO setupScript (retired field)
    assert_eq!(
        create_resp["result"]["workspace"].get("setupScript"),
        None,
        "workspace DB row should not have setupScript"
    );

    // Regression (monorepo#1870): the explicit setupScript param is execute-only.
    // The committed `.intent/config.json` in the new worktree must be byte-identical
    // to the repo's committed content — workspace.create performs no config write.
    let workspace_path = create_resp["result"]["workspace"]["worktreePath"]
        .as_str()
        .expect("worktreePath should be set");
    let worktree_config_path = PathBuf::from(workspace_path).join(".intent/config.json");
    assert!(
        worktree_config_path.exists(),
        "committed worktree config should exist"
    );
    let config_content = std::fs::read_to_string(&worktree_config_path).expect("read config");
    assert_eq!(
        config_content, COMMITTED_CONFIG,
        "worktree config must be byte-identical to the committed content (no create-path write)"
    );

    // Get server fingerprint and connect via WSS
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().unwrap()).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .unwrap()
        .to_string();
    let cfg = client_config(&fingerprint);
    let mut ws = connect_ws(port, cfg).await;

    // Test workspace.getSetupScript returns the committed repo-config value —
    // NOT the explicit create param, which was execute-only.
    let get_resp = wss_rpc(
        &mut ws,
        10,
        "workspace.getSetupScript",
        json!({"workspaceId": workspace_id}),
    )
    .await;
    // Assert JSON-RPC 2.0 envelope (§5.25 wire contract)
    assert_eq!(get_resp["jsonrpc"], json!("2.0"), "jsonrpc version");
    assert_eq!(get_resp["id"], json!(10), "echoed id");
    assert_eq!(
        get_resp["result"]["setupScript"]["script"],
        json!("pnpm install"),
        "getSetupScript should return the committed repo config value, not the create param"
    );
    assert_eq!(
        get_resp["result"]["setupScript"]["generatedBy"],
        json!("user"),
        "generatedBy should be synthesized"
    );
    assert!(
        get_resp["result"]["setupScript"]["updatedAt"].is_number(),
        "updatedAt should be present as number (file mtime)"
    );

    // Test workspace.saveSetupScript updates the repo config file
    let save_resp = wss_rpc(
        &mut ws,
        11,
        "workspace.saveSetupScript",
        json!({"workspaceId": workspace_id, "script": "yarn install"}),
    )
    .await;
    // Assert JSON-RPC 2.0 envelope
    assert_eq!(save_resp["jsonrpc"], json!("2.0"), "jsonrpc version");
    assert_eq!(save_resp["id"], json!(11), "echoed id");
    assert_eq!(
        save_resp["result"]["setupScript"]["script"],
        json!("yarn install"),
        "saveSetupScript should return updated script"
    );
    assert_eq!(
        save_resp["result"]["setupScript"]["generatedBy"],
        json!("user"),
        "generatedBy should be user for saved scripts"
    );
    assert!(
        save_resp["result"]["setupScript"]["updatedAt"].is_number(),
        "updatedAt should be present as number"
    );

    // Assert the repo config file in the worktree was updated
    let updated_content =
        std::fs::read_to_string(&worktree_config_path).expect("read updated config");
    let updated_json: Value = serde_json::from_str(&updated_content).expect("parse updated config");
    assert_eq!(
        updated_json["setupScript"],
        json!("yarn install"),
        "worktree repo config should be updated"
    );

    // Test getSetupScript now returns the updated value
    let get_resp2 = wss_rpc(
        &mut ws,
        12,
        "workspace.getSetupScript",
        json!({"workspaceId": workspace_id}),
    )
    .await;
    assert_eq!(
        get_resp2["result"]["setupScript"]["script"],
        json!("yarn install"),
        "getSetupScript should return updated value"
    );

    // Create another workspace from the same repo WITHOUT an explicit script.
    // It should inherit the committed setupScript from the repo's main branch.
    let create_resp2 = uds_rpc(
        &socket,
        3,
        "workspace.create",
        json!({
            "title": "test-setup-2",
            "repositoryPath": repo_path.to_string_lossy()
        }),
    )
    .await;
    let workspace_id2 = create_resp2["result"]["workspace"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // The new workspace should inherit the committed repo config script (pnpm install)
    let get_resp3 = wss_rpc(
        &mut ws,
        13,
        "workspace.getSetupScript",
        json!({"workspaceId": workspace_id2}),
    )
    .await;
    assert_eq!(
        get_resp3["result"]["setupScript"]["script"],
        json!("pnpm install"),
        "new workspace should inherit committed repo config script from main branch"
    );

    // Cleanup
    let _ = std::fs::remove_dir_all(&repo_path);
}

/// WSS e2e coverage for setup script execution: workspace.create with setupScript
/// runs it in the worktree (taking precedence over the committed repo-config script)
/// without persisting it, env vars are visible, failing script doesn't fail create.
#[tokio::test]
async fn setup_script_executes_on_create() {
    let data_dir = temp_data_dir();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    // Get fingerprint and actual bound port from daemon
    let status_resp = common::await_wss_status(&socket).await;
    let fingerprint = status_resp["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint");
    let actual_port = u16::try_from(status_resp["result"]["port"].as_u64().expect("port"))
        .expect("value fits in u16");

    let mut wss = wss_connect(actual_port, fingerprint).await;

    let repo_path = create_test_repo();

    // Create hermetic marker paths under the worktree (not /tmp) to avoid parallel collisions
    let test_run_id = Uuid::new_v4().simple().to_string();

    // Create a workspace with a setup script that writes a marker file + env vars
    let marker_script = format!(
        r#"#!/bin/sh
set -e
# Write env vars to a file in the worktree (hermetic, not /tmp)
	while [ ! -f "${{WORKTREE_PATH}}/.setup-release-{test_run_id}" ]; do
	  sleep 0.1
	done
echo "MAIN_CHECKOUT=${{MAIN_CHECKOUT}}" > "${{WORKTREE_PATH}}/setup-env-{test_run_id}.txt"
echo "WORKTREE_PATH=${{WORKTREE_PATH}}" >> "${{WORKTREE_PATH}}/setup-env-{test_run_id}.txt"
echo "BRANCH_NAME=${{BRANCH_NAME}}" >> "${{WORKTREE_PATH}}/setup-env-{test_run_id}.txt"
echo "SOURCE_BRANCH=${{SOURCE_BRANCH}}" >> "${{WORKTREE_PATH}}/setup-env-{test_run_id}.txt"
touch "${{WORKTREE_PATH}}/.setup-ran-{test_run_id}"
"#
    );

    let create_resp = wss_rpc(
        &mut wss,
        1,
        "workspace.create",
        json!({
            "title": "test-setup-exec",
            "repositoryPath": repo_path.to_string_lossy(),
            "setupScript": marker_script
        }),
    )
    .await;

    // Assert workspace.create succeeded
    assert_eq!(create_resp["jsonrpc"], json!("2.0"));
    assert_eq!(create_resp["id"], json!(1));
    assert!(
        create_resp["result"]["workspace"]["id"].is_string(),
        "create should succeed even if script runs"
    );

    let workspace_id = create_resp["result"]["workspace"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let workspace_path = create_resp["result"]["workspace"]["worktreePath"]
        .as_str()
        .expect("worktreePath should be set");

    // Setup Script remains list-visible while running. Hold it behind a marker
    // so even fast CI machines can observe it before allowing it to finish.
    let setup_terminal = await_running_setup_terminal(&mut wss, 100, &workspace_id).await;
    let terminal_id = setup_terminal["id"]
        .as_str()
        .expect("terminal id")
        .to_string();
    std::fs::write(
        PathBuf::from(workspace_path).join(format!(".setup-release-{test_run_id}")),
        "",
    )
    .expect("release setup script");

    // Poll for the marker file (script execution is fire-and-forget, may take a moment).
    // The marker appearing also proves the explicit param took precedence over the
    // committed repo-config script ("pnpm install") — exactly one script executes.
    let marker_path = PathBuf::from(workspace_path).join(format!(".setup-ran-{test_run_id}"));
    let mut found = false;
    for _ in 0..100 {
        if marker_path.exists() {
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(found, "setup script should have created marker file");

    // Regression (monorepo#1870): executing the explicit script must not write it
    // anywhere — the committed worktree config stays byte-identical.
    let config_after_exec =
        std::fs::read_to_string(PathBuf::from(workspace_path).join(".intent/config.json"))
            .expect("read worktree config");
    assert_eq!(
        config_after_exec, COMMITTED_CONFIG,
        "explicit setupScript must be execute-only; committed config must be untouched"
    );

    // Verify env vars were set correctly
    let env_file_path = PathBuf::from(workspace_path).join(format!("setup-env-{test_run_id}.txt"));
    let env_content = std::fs::read_to_string(&env_file_path).expect("read env test file");
    assert!(
        env_content.contains("MAIN_CHECKOUT="),
        "MAIN_CHECKOUT should be set"
    );
    assert!(
        env_content.contains(&format!("WORKTREE_PATH={workspace_path}")),
        "WORKTREE_PATH should match workspace path"
    );
    assert!(
        env_content.contains("BRANCH_NAME="),
        "BRANCH_NAME should be set"
    );
    assert!(
        env_content.contains("SOURCE_BRANCH="),
        "SOURCE_BRANCH should be set"
    );

    // After exit, the terminal is omitted from terminal.list but its scrollback
    // remains readable by the id captured while it was running.
    let buffer =
        await_buffer_contains(&mut wss, 300, &terminal_id, "Setup script completed in ").await;
    assert!(
        buffer.contains("(exit code 0)"),
        "buffer should report exit code 0: {buffer:?}"
    );
    await_terminal_omitted(&mut wss, 800, &workspace_id, &terminal_id).await;

    // Test that a failing script doesn't fail workspace.create
    let failing_run_id = Uuid::new_v4().simple().to_string();
    let failing_script = format!(
        r#"#!/bin/sh
while [ ! -f "${{WORKTREE_PATH}}/.setup-fail-release-{failing_run_id}" ]; do
  sleep 0.1
done
touch "${{WORKTREE_PATH}}/.setup-fail-ran-{failing_run_id}"
exit 1
"#
    );
    let create_resp2 = wss_rpc(
        &mut wss,
        2,
        "workspace.create",
        json!({
            "title": "test-setup-fail",
            "repositoryPath": repo_path.to_string_lossy(),
            "setupScript": failing_script
        }),
    )
    .await;

    // Assert workspace.create succeeded even with failing script
    assert_eq!(create_resp2["jsonrpc"], json!("2.0"));
    assert_eq!(create_resp2["id"], json!(2));
    assert!(
        create_resp2["result"]["workspace"]["id"].is_string(),
        "create should succeed even when setup script fails"
    );

    // The failing script's terminal reports the failure summary with its
    // preserved (non-zero) exit code in the scrollback.
    let workspace_id2 = create_resp2["result"]["workspace"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let workspace_path2 = create_resp2["result"]["workspace"]["worktreePath"]
        .as_str()
        .expect("worktreePath should be set");
    let failed_terminal = await_running_setup_terminal(&mut wss, 400, &workspace_id2).await;
    let failed_terminal_id = failed_terminal["id"]
        .as_str()
        .expect("terminal id")
        .to_string();
    std::fs::write(
        PathBuf::from(workspace_path2).join(format!(".setup-fail-release-{failing_run_id}")),
        "",
    )
    .expect("release failing setup script");
    let failed_marker_path =
        PathBuf::from(workspace_path2).join(format!(".setup-fail-ran-{failing_run_id}"));
    let mut failed_marker_found = false;
    for _ in 0..100 {
        if failed_marker_path.exists() {
            failed_marker_found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        failed_marker_found,
        "failing setup script should have created marker file"
    );
    let failed_buffer = await_buffer_contains(
        &mut wss,
        600,
        &failed_terminal_id,
        "Setup script failed in ",
    )
    .await;
    assert!(
        failed_buffer.contains("(exit code 1)"),
        "buffer should report exit code 1: {failed_buffer:?}"
    );
    await_terminal_omitted(&mut wss, 900, &workspace_id2, &failed_terminal_id).await;

    // Regression (monorepo#1870), file-absent case: creating from a repo with NO
    // committed .intent/config.json and an explicit setupScript must not create
    // the config file — the script still executes.
    let bare_repo_path = create_bare_test_repo();
    let bare_run_id = Uuid::new_v4().simple().to_string();
    let bare_script = format!(
        r#"#!/bin/sh
touch "${{WORKTREE_PATH}}/.setup-bare-ran-{bare_run_id}"
"#
    );
    let create_resp_bare = wss_rpc(
        &mut wss,
        4,
        "workspace.create",
        json!({
            "title": "test-setup-bare",
            "repositoryPath": bare_repo_path.to_string_lossy(),
            "setupScript": bare_script
        }),
    )
    .await;
    assert_eq!(create_resp_bare["jsonrpc"], json!("2.0"));
    assert_eq!(create_resp_bare["id"], json!(4));
    let bare_workspace_path = create_resp_bare["result"]["workspace"]["worktreePath"]
        .as_str()
        .expect("worktreePath should be set");
    let bare_marker_path =
        PathBuf::from(bare_workspace_path).join(format!(".setup-bare-ran-{bare_run_id}"));
    let mut bare_marker_found = false;
    for _ in 0..100 {
        if bare_marker_path.exists() {
            bare_marker_found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        bare_marker_found,
        "explicit setup script should have executed in the bare-config workspace"
    );
    assert!(
        !PathBuf::from(bare_workspace_path)
            .join(".intent/config.json")
            .exists(),
        "workspace.create must not create .intent/config.json for an explicit setupScript"
    );
    let _ = std::fs::remove_dir_all(&bare_repo_path);

    // Test that skipWorktree workspace does not execute the script
    let skip_marker_id = Uuid::new_v4().simple().to_string();
    let skip_script = format!(
        r#"#!/bin/sh
touch "${{MAIN_CHECKOUT}}/.should-not-run-{skip_marker_id}"
"#
    );

    let create_resp3 = wss_rpc(
        &mut wss,
        3,
        "workspace.create",
        json!({
            "title": "test-skip-worktree",
            "repositoryPath": repo_path.to_string_lossy(),
            "setupScript": skip_script,
            "skipWorktree": true
        }),
    )
    .await;

    assert_eq!(create_resp3["jsonrpc"], json!("2.0"));
    assert_eq!(create_resp3["id"], json!(3));
    assert!(
        create_resp3["result"]["workspace"]["id"].is_string(),
        "skipWorktree create should succeed"
    );

    let _skip_workspace_id = create_resp3["result"]["workspace"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Give it time to potentially run (it shouldn't)
    tokio::time::sleep(Duration::from_millis(500)).await;

    // skipWorktree has no worktree, so WORKTREE_PATH would be empty; the script uses
    // MAIN_CHECKOUT instead (repo path). Assert marker does not appear under repo.
    let skip_marker_path = repo_path.join(format!(".should-not-run-{skip_marker_id}"));
    assert!(
        !skip_marker_path.exists(),
        "skipWorktree should not execute setup script (marker not found under repo)"
    );

    // Cleanup
    let _ = std::fs::remove_dir_all(&repo_path);
}
