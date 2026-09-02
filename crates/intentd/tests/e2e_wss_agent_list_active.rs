//! Real-WSS contract coverage for daemon-global `agent.listActive`.

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio_tungstenite::tungstenite::Message;

const TOKEN: &str = "abababababababababababababababababababababababababababababababab";

struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        use nix::sys::signal::{self, Signal};
        use nix::unistd::Pid;
        let _ = signal::killpg(
            Pid::from_raw(self.child.id().cast_signed()),
            Signal::SIGKILL,
        );
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
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
        let actual = Sha256::digest(end_entity.as_ref())
            .iter()
            .map(|byte| format!("{byte:02X}"))
            .collect::<Vec<_>>()
            .join(":");
        if actual == self.fingerprint {
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

fn temp_data_dir() -> PathBuf {
    let dir = PathBuf::from("/tmp").join(format!(
        "itd-wss-active-{}",
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    ));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, script: &str, behavior: &str) -> Daemon {
    use std::os::unix::process::CommandExt;
    common::enable_ws_api(data_dir);
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir workspaces dir");
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("daemon log");
    let mut command = Command::new(env!("CARGO_BIN_EXE_intentd"));
    command
        .arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_SECRETS_FILE", data_dir.join("secrets.json"))
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_TCP_PORT", "0")
        .env("MOCK_AGENT_SCRIPT_PATH", script)
        .env("MOCK_AGENT_BEHAVIOR", behavior)
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    command.process_group(0);
    let child = command.spawn().expect("spawn intentd serve");
    Daemon {
        child,
        data_dir: data_dir.to_path_buf(),
    }
}

fn mock_agent_script() -> Option<String> {
    let script = format!(
        "{}/tests/fixtures/mock-acp-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    if intent_providers::resolve_on_path("node").is_none() || !Path::new(&script).exists() {
        eprintln!("skipping WSS agent.listActive e2e: mock agent unavailable");
        return None;
    }
    Some(script)
}

async fn seed_workspace(data_dir: &Path) -> String {
    use intent_core::{
        now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
    };
    let store = intent_store::Store::open(&data_dir.join("intentd.db"))
        .await
        .expect("open store");
    let id = WorkspaceId::new();
    let timestamp = now_iso();
    let workspace = Workspace {
        id: id.clone(),
        title: "List Active E2E".into(),
        branch: "main".into(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: timestamp.clone(),
        updated_at: timestamp,
        last_activity: None,
        tags: vec![],
        path: None,
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: None,
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        pull_requests: None,
        context_links: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
    };
    store
        .insert_workspace(&workspace)
        .await
        .expect("insert workspace");
    id.0
}

async fn rpc_envelope(ws: &mut common::TlsWs, id: i64, method: &str, params: Value) -> Value {
    ws.send(Message::Text(
        json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })
            .to_string()
            .into(),
    ))
    .await
    .expect("send RPC");
    loop {
        let frame = tokio::time::timeout(common::rpc_read_timeout(), ws.next())
            .await
            .expect("RPC timed out");
        match frame {
            Some(Ok(Message::Text(text))) => {
                let value: Value = serde_json::from_str(&text).expect("JSON response");
                if value["id"] == json!(id) {
                    return value;
                }
            }
            Some(Ok(Message::Ping(payload))) => {
                ws.send(Message::Pong(payload)).await.expect("pong");
            }
            Some(Ok(_)) => {}
            other => panic!("expected WSS frame, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn list_active_tracks_only_mid_turn_agents_over_real_wss() {
    let Some(script) = mock_agent_script() else {
        return;
    };
    let data_dir = temp_data_dir();
    let workspace_id = seed_workspace(&data_dir).await;
    let behavior = json!({ "blockUntilCancel": true, "response": "parked" }).to_string();
    let _daemon = spawn_serve(&data_dir, &script, &behavior);
    let status = common::await_wss_status_logged(
        &data_dir.join("intentd.sock"),
        &data_dir.join("daemon.log"),
    )
    .await;
    let port = u16::try_from(status["result"]["port"].as_u64().expect("WSS port"))
        .expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint");
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    let mut ws = common::wss_connect_with_retry(port, client_config(fingerprint), &url).await;

    let empty = rpc_envelope(&mut ws, 1, "agent.listActive", json!({})).await;
    assert_eq!(
        empty,
        json!({ "jsonrpc": "2.0", "id": 1, "result": { "streams": [] } })
    );

    let created = rpc_envelope(
        &mut ws,
        2,
        "agent.create",
        json!({ "workspaceId": workspace_id, "name": "Busy", "model": "default", "provider": "mock" }),
    )
    .await;
    let agent_id = created["result"]["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string();
    let sent = rpc_envelope(
        &mut ws,
        3,
        "agent.sendMessage",
        json!({ "workspaceId": workspace_id, "agentId": agent_id, "content": "park" }),
    )
    .await;
    assert_eq!(sent["result"]["success"], true, "send response: {sent}");

    let active = rpc_envelope(&mut ws, 4, "agent.listActive", json!({})).await;
    assert_eq!(active["jsonrpc"], "2.0");
    assert_eq!(active["id"], 4);
    assert!(
        active.get("error").is_none(),
        "listActive response: {active}"
    );
    let streams = active["result"]["streams"].as_array().expect("streams");
    assert_eq!(streams.len(), 1, "listActive response: {active}");
    assert_eq!(streams[0]["agentId"], agent_id);
    assert_eq!(streams[0]["sessionId"], agent_id);
    assert_eq!(streams[0]["workspaceId"], workspace_id);
    assert!(streams[0]["startTime"].as_i64().is_some_and(|ms| ms > 0));

    let stopped = rpc_envelope(&mut ws, 5, "agent.stop", json!({ "agentId": agent_id })).await;
    assert_eq!(stopped["result"], json!({ "success": true }));
    let idle = rpc_envelope(&mut ws, 6, "agent.listActive", json!({})).await;
    assert_eq!(idle["result"], json!({ "streams": [] }));
}
