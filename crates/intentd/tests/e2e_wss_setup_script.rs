//! WSS e2e test for setup script methods: workspace.create writes to repo config,
//! workspace.saveSetupScript writes/reads via repo config, workspace.getSetupScript
//! reads from repo config with DB fallback (per AGENTS.md testing gate requirement).

#![cfg(unix)]

use std::net::{Ipv4Addr, TcpListener as StdTcpListener};
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
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_rustls::TlsConnector;
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
            eprintln!("=== DAEMON LOG ===\n{}\n=== END LOG ===", log);
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn free_port() -> u16 {
    StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
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
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .arg("--listen")
        .arg(listen)
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
    timeout(Duration::from_secs(10), async {
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
    timeout(Duration::from_secs(5), reader.read_line(&mut buf))
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

async fn tls_connect(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    let tcp = TcpStream::connect((Ipv4Addr::LOCALHOST, port))
        .await
        .expect("tcp connect");
    let name = ServerName::try_from("localhost").unwrap();
    TlsConnector::from(cfg)
        .connect(name, tcp)
        .await
        .expect("tls connect")
}

async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let tls = tls_connect(port, cfg).await;
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    let (ws, _resp) = tokio_tungstenite::client_async(url, tls)
        .await
        .expect("ws handshake");
    ws
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
    ws.send(Message::Text(frame.to_string()))
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
            Some(Ok(_)) => continue,
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

fn create_test_repo() -> PathBuf {
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

    // Commit a setup script in .intent/config.json to test inheritance
    std::fs::create_dir_all(repo_path.join(".intent")).expect("create .intent dir");
    std::fs::write(
        repo_path.join(".intent/config.json"),
        r#"{"setupScript": "pnpm install"}"#,
    )
    .expect("write config");
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

/// WSS e2e coverage for setup script methods: workspace.create with setupScript writes
/// to repo config (not DB), saveSetupScript writes repo config, getSetupScript reads from
/// repo config with legacy DB fallback. Verifies the §5.25 "repo config sole source" contract.
#[tokio::test]
async fn setup_script_repo_config_sole_source() {
    let data_dir = temp_data_dir();
    let port_s = free_port().to_string();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", &port_s)];
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

    // Assert the config file exists in the NEW WORKTREE (not repo root).
    // The worktree is provisioned under <data_dir>/workspaces/<workspace_id>/<repo_slug>.
    let workspace_path = create_resp["result"]["workspace"]["worktreePath"]
        .as_str()
        .expect("worktreePath should be set");
    let worktree_config_path = PathBuf::from(workspace_path).join(".intent/config.json");
    assert!(
        worktree_config_path.exists(),
        "worktree config should exist"
    );
    let config_content = std::fs::read_to_string(&worktree_config_path).expect("read config");
    let config_json: Value = serde_json::from_str(&config_content).expect("parse config");
    assert_eq!(
        config_json["setupScript"],
        json!("npm install"),
        "worktree config should have the explicit setupScript"
    );

    // Get server fingerprint and connect via WSS
    let status = uds_rpc(&socket, 2, "system.status", json!({})).await;
    let port = status["result"]["port"].as_u64().unwrap() as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .unwrap()
        .to_string();
    let cfg = client_config(&fingerprint);
    let mut ws = connect_ws(port, cfg).await;

    // Test workspace.getSetupScript returns the repo-config value
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
        json!("npm install"),
        "getSetupScript should return repo config value"
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
/// runs it in the worktree, env vars are visible, failing script doesn't fail create.
#[tokio::test]
async fn setup_script_executes_on_create() {
    let data_dir = temp_data_dir();
    let port = free_port();
    let port_s = port.to_string();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", &port_s)];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    // Get fingerprint from daemon
    let status_resp = uds_rpc(&socket, 0, "system.status", json!({})).await;
    let fingerprint = status_resp["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint");

    let mut wss = wss_connect(port, fingerprint).await;

    let repo_path = create_test_repo();

    // Create hermetic marker paths under the worktree (not /tmp) to avoid parallel collisions
    let test_run_id = Uuid::new_v4().simple().to_string();

    // Create a workspace with a setup script that writes a marker file + env vars
    let marker_script = format!(
        r#"#!/bin/sh
set -e
# Write env vars to a file in the worktree (hermetic, not /tmp)
echo "MAIN_CHECKOUT=${{MAIN_CHECKOUT}}" > "${{WORKTREE_PATH}}/setup-env-{}.txt"
echo "WORKTREE_PATH=${{WORKTREE_PATH}}" >> "${{WORKTREE_PATH}}/setup-env-{}.txt"
echo "BRANCH_NAME=${{BRANCH_NAME}}" >> "${{WORKTREE_PATH}}/setup-env-{}.txt"
echo "SOURCE_BRANCH=${{SOURCE_BRANCH}}" >> "${{WORKTREE_PATH}}/setup-env-{}.txt"
touch "${{WORKTREE_PATH}}/.setup-ran-{}"
"#,
        test_run_id, test_run_id, test_run_id, test_run_id, test_run_id
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

    let _workspace_id = create_resp["result"]["workspace"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let workspace_path = create_resp["result"]["workspace"]["worktreePath"]
        .as_str()
        .expect("worktreePath should be set");

    // Poll for the marker file (script execution is fire-and-forget, may take a moment)
    let marker_path = PathBuf::from(workspace_path).join(format!(".setup-ran-{}", test_run_id));
    let mut found = false;
    for _ in 0..100 {
        if marker_path.exists() {
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(found, "setup script should have created marker file");

    // Verify env vars were set correctly
    let env_file_path =
        PathBuf::from(workspace_path).join(format!("setup-env-{}.txt", test_run_id));
    let env_content = std::fs::read_to_string(&env_file_path).expect("read env test file");
    assert!(
        env_content.contains("MAIN_CHECKOUT="),
        "MAIN_CHECKOUT should be set"
    );
    assert!(
        env_content.contains(&format!("WORKTREE_PATH={}", workspace_path)),
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

    // Test that a failing script doesn't fail workspace.create
    let failing_script = r#"#!/bin/sh
exit 1
"#;
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

    // Test that skipWorktree workspace does not execute the script
    let skip_marker_id = Uuid::new_v4().simple().to_string();
    let skip_script = format!(
        r#"#!/bin/sh
touch "${{WORKTREE_PATH}}/.should-not-run-{}"
"#,
        skip_marker_id
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

    let skip_workspace_id = create_resp3["result"]["workspace"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Give it time to potentially run (it shouldn't)
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The skip-worktree workspace shouldn't have a worktree path, but check workspace dir
    let workspaces_dir = data_dir.join("workspaces").join(skip_workspace_id);
    let skip_marker_path = workspaces_dir.join(format!(".should-not-run-{}", skip_marker_id));
    assert!(
        !skip_marker_path.exists(),
        "skipWorktree should not execute setup script"
    );

    // Cleanup
    let _ = std::fs::remove_dir_all(&repo_path);
}
