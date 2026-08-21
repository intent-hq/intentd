//! WSS e2e test for repoConfig.* methods: get/save/has/ensureDir over real /ws transport
//! (per AGENTS.md testing gate requirement: every new RPC method needs WSS e2e coverage).

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
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

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
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-repo-cfg-{}", &id[..8]));
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

/// WSS e2e coverage for repoConfig.* methods: get/save/has/ensureDir happy paths,
/// unknown workspace errors, and .intent/.gitignore creation.
#[tokio::test]
async fn repo_config_wss_e2e() {
    let data_dir = temp_data_dir();
    let env: [(&str, &str); 2] = [("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");

    // Create a temporary git repo in a guarded dir (swept even on panic).
    let repo_dir = common::test_tempdir("repo-cfg-wss-");
    let repo_path = repo_dir.path().to_path_buf();
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

    // Create a test workspace with the git repo via UDS
    let create_resp = uds_rpc(
        &socket,
        1,
        "workspace.create",
        json!({
            "title": "test-repo-config",
            "repositoryPath": repo_path.to_string_lossy()
        }),
    )
    .await;
    let workspace_id = create_resp["result"]["workspace"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let worktree_path = PathBuf::from(
        create_resp["result"]["workspace"]["worktreePath"]
            .as_str()
            .unwrap(),
    );

    // Get server fingerprint and port
    let status = common::await_wss_status(&socket).await;
    let port = status["result"]["port"].as_u64().unwrap() as u16;
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .unwrap()
        .to_string();
    let cfg = client_config(&fingerprint);
    let mut ws = connect_ws(port, cfg).await;

    // Test repoConfig.has - should be false initially
    let has_resp = wss_rpc(
        &mut ws,
        10,
        "repoConfig.has",
        json!({"workspaceId": workspace_id}),
    )
    .await;
    assert_eq!(
        has_resp["result"]["exists"],
        json!(false),
        "config should not exist initially"
    );

    // Test repoConfig.get - should return empty config
    let get_resp = wss_rpc(
        &mut ws,
        11,
        "repoConfig.get",
        json!({"workspaceId": workspace_id}),
    )
    .await;
    assert!(
        get_resp["result"]["config"].is_object(),
        "should return empty config object"
    );

    // Test repoConfig.ensureDir - should create .intent directory and .gitignore
    let ensure_resp = wss_rpc(
        &mut ws,
        12,
        "repoConfig.ensureDir",
        json!({"workspaceId": workspace_id}),
    )
    .await;
    assert_eq!(
        ensure_resp["result"]["ok"],
        json!(true),
        "ensureDir should succeed"
    );
    let intent_dir = worktree_path.join(".intent");
    assert!(intent_dir.exists(), ".intent directory should be created");
    let gitignore = intent_dir.join(".gitignore");
    assert!(gitignore.exists(), ".intent/.gitignore should be created");

    // Test repoConfig.save - save a config
    let config = json!({
        "branchPrefix": "feature/",
        "setupScript": "npm install",
        "instructions": "Always use TypeScript"
    });
    let save_resp = wss_rpc(
        &mut ws,
        13,
        "repoConfig.save",
        json!({"workspaceId": workspace_id, "config": config}),
    )
    .await;
    assert!(
        save_resp["result"]["config"].is_object(),
        "save should return saved config"
    );
    assert_eq!(
        save_resp["result"]["config"]["branchPrefix"],
        json!("feature/"),
        "saved config should match"
    );

    // Test repoConfig.has - should be true now
    let has_resp2 = wss_rpc(
        &mut ws,
        14,
        "repoConfig.has",
        json!({"workspaceId": workspace_id}),
    )
    .await;
    assert_eq!(
        has_resp2["result"]["exists"],
        json!(true),
        "config should exist after save"
    );

    // Test repoConfig.get - should return saved config
    let get_resp2 = wss_rpc(
        &mut ws,
        15,
        "repoConfig.get",
        json!({"workspaceId": workspace_id}),
    )
    .await;
    assert_eq!(
        get_resp2["result"]["config"]["branchPrefix"],
        json!("feature/"),
        "get should return saved config"
    );
    assert_eq!(
        get_resp2["result"]["config"]["instructions"],
        json!("Always use TypeScript"),
        "instructions should be preserved"
    );

    // Regression: a partial save must merge with the on-disk config, not
    // replace it — saving only branchPrefix must not erase setupScript.
    let partial_save_resp = wss_rpc(
        &mut ws,
        20,
        "repoConfig.save",
        json!({"workspaceId": workspace_id, "config": {"branchPrefix": "x/"}}),
    )
    .await;
    assert_eq!(
        partial_save_resp["result"]["config"]["branchPrefix"],
        json!("x/"),
        "partial save should apply the sent field"
    );
    assert_eq!(
        partial_save_resp["result"]["config"]["setupScript"],
        json!("npm install"),
        "partial save must not clobber fields absent from the payload"
    );
    let get_after_partial = wss_rpc(
        &mut ws,
        21,
        "repoConfig.get",
        json!({"workspaceId": workspace_id}),
    )
    .await;
    assert_eq!(
        get_after_partial["result"]["config"]["branchPrefix"],
        json!("x/"),
        "partial save should persist the sent field"
    );
    assert_eq!(
        get_after_partial["result"]["config"]["setupScript"],
        json!("npm install"),
        "setupScript must survive a partial save on disk"
    );
    assert_eq!(
        get_after_partial["result"]["config"]["instructions"],
        json!("Always use TypeScript"),
        "instructions must survive a partial save on disk"
    );

    // Unknown keys sent by other tools still round-trip through save.
    let unknown_key_save = wss_rpc(
        &mut ws,
        22,
        "repoConfig.save",
        json!({"workspaceId": workspace_id, "config": {"customKey": "customValue"}}),
    )
    .await;
    assert_eq!(
        unknown_key_save["result"]["config"]["customKey"],
        json!("customValue"),
        "unknown keys should be accepted and echoed back"
    );
    let get_after_unknown = wss_rpc(
        &mut ws,
        23,
        "repoConfig.get",
        json!({"workspaceId": workspace_id}),
    )
    .await;
    assert_eq!(
        get_after_unknown["result"]["config"]["customKey"],
        json!("customValue"),
        "unknown keys should round-trip on disk"
    );
    assert_eq!(
        get_after_unknown["result"]["config"]["setupScript"],
        json!("npm install"),
        "known fields must survive an unknown-key save"
    );

    // Explicit null clears a field.
    let null_save_resp = wss_rpc(
        &mut ws,
        24,
        "repoConfig.save",
        json!({"workspaceId": workspace_id, "config": {"setupScript": null}}),
    )
    .await;
    assert!(
        null_save_resp["result"]["config"]["setupScript"].is_null(),
        "explicit null should clear setupScript in the response"
    );
    let get_after_null = wss_rpc(
        &mut ws,
        25,
        "repoConfig.get",
        json!({"workspaceId": workspace_id}),
    )
    .await;
    assert!(
        get_after_null["result"]["config"]["setupScript"].is_null(),
        "explicit null should clear setupScript on disk"
    );
    assert_eq!(
        get_after_null["result"]["config"]["branchPrefix"],
        json!("x/"),
        "other fields must survive a null-clearing save"
    );

    // Test unknown workspace error (-32602)
    let unknown_resp = wss_rpc(
        &mut ws,
        16,
        "repoConfig.get",
        json!({"workspaceId": "ws-unknown"}),
    )
    .await;
    assert!(
        unknown_resp.get("error").is_some(),
        "should return error for unknown workspace"
    );
    assert_eq!(
        unknown_resp["error"]["code"],
        json!(-32602),
        "should use -32602 for unknown workspace"
    );

    // Test missing workspaceId parameter (-32602)
    let missing_resp = wss_rpc(&mut ws, 17, "repoConfig.get", json!({})).await;
    assert!(
        missing_resp.get("error").is_some(),
        "should return error for missing parameter"
    );
    assert_eq!(
        missing_resp["error"]["code"],
        json!(-32602),
        "should use -32602 for missing parameter"
    );
}
