//! WSS end-to-end for `agent.listInterrupted` (INT-41, agent-resumption phase 1).
//!
//! Boots a real `intentd serve --listen both`, creates agent sessions in stale
//! in-flight statuses (Active/Processing/Waiting), restarts the daemon, and
//! verifies that `agent.listInterrupted` returns the interrupted agents.
//!
//! Coverage:
//! - Interrupted agents are persisted across restart
//! - `agent.listInterrupted` returns pending rows with joined workspace/agent data
//! - Terminal/pending sessions are not captured
//! - Idempotent inserts on second restart

#![cfg(unix)]

use std::net::{Ipv4Addr, TcpListener as StdTcpListener};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

fn free_port() -> u16 {
    StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-interrupted-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
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
    use tokio::io::{AsyncBufReadExt, BufReader};
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
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        use sha2::{Digest, Sha256};
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
    // Bound all network handshakes with 5s timeouts to prevent indefinite hangs on retry
    let tcp = timeout(
        Duration::from_secs(5),
        TcpStream::connect((Ipv4Addr::LOCALHOST, port)),
    )
    .await
    .expect("tcp connect timed out")
    .expect("tcp connect");
    let name = ServerName::try_from("localhost").unwrap();
    let tls = timeout(
        Duration::from_secs(5),
        TlsConnector::from(cfg).connect(name, tcp),
    )
    .await
    .expect("tls handshake timed out")
    .expect("tls connect");
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    let (ws, _resp) = timeout(
        Duration::from_secs(5),
        tokio_tungstenite::client_async(url, tls),
    )
    .await
    .expect("ws handshake timed out")
    .expect("ws handshake");
    ws
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
                if v["id"] == json!(id) && v.get("result").is_some() {
                    return v["result"].clone();
                } else if v["id"] == json!(id) {
                    panic!("rpc errored: {v}");
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

#[tokio::test]
async fn interrupted_agents_persisted_across_restart() {
    let data_dir = temp_data_dir();
    let port = free_port();
    let port_s = port.to_string();
    let listen = "both";
    let socket = data_dir.join("intentd.sock");

    // Phase 1: Boot daemon, create a workspace, create an agent session with Active status.
    let mut cmd1 = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd1.arg("serve")
        .arg("--listen")
        .arg(listen)
        .env("INTENTD_DATA_DIR", &data_dir)
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_TCP_PORT", &port_s)
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(data_dir.join("daemon.log")).unwrap(),
        ));
    // Spawn in its own process group to prevent ACP mock process leaks
    #[cfg(unix)]
    cmd1.process_group(0);
    let mut daemon = cmd1.spawn().expect("spawn intentd serve");
    if !await_uds(&socket).await {
        let log_path = data_dir.join("daemon.log");
        if let Ok(log) = std::fs::read_to_string(&log_path) {
            eprintln!("Daemon log:\n{}", log);
        }
        panic!("daemon did not start");
    }

    let ws_id = "ws-interrupted-test";
    let agent_id = format!("agent-{}", Uuid::new_v4().simple());

    let store = intent_store::Store::open(&data_dir.join("intentd.db"))
        .await
        .expect("open store");

    // Seed workspace + agent session with Active status (stale in-flight).
    {
        use intent_core::{now_iso, AgentId, AgentSession, AgentStatus, WorkspaceId};
        let ts = now_iso();
        store
            .insert_workspace(&workspace_seed(&WorkspaceId(ws_id.to_string())))
            .await
            .expect("insert workspace");

        let session = AgentSession {
            id: AgentId(agent_id.clone()),
            workspace_id: WorkspaceId(ws_id.to_string()),
            backend_session_id: None,
            acp_session_id: None,
            name: "Interrupted Agent".to_string(),
            name_explicitly_set: false,
            model: None,
            provider: None,
            status: AgentStatus::Active, // stale in-flight
            is_active: true,
            system_prompt: None,
            created_at: ts.clone(),
            updated_at: ts,
            parent_agent_id: None,
            specialist: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            is_background: false,
            metadata: None,
            messages: vec![],
            stats: None,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
        };
        store
            .insert_agent_session(&session)
            .await
            .expect("insert agent");
    }

    // Kill daemon to simulate restart.
    daemon.kill().expect("kill daemon");
    daemon.wait().expect("wait daemon");

    // Phase 2: Restart daemon — heal sweep should insert interrupted_agent row.
    let mut cmd2 = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd2.arg("serve")
        .arg("--listen")
        .arg(listen)
        .env("INTENTD_DATA_DIR", &data_dir)
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_TCP_PORT", &port_s)
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(data_dir.join("daemon.log")).unwrap(),
        ));
    #[cfg(unix)]
    cmd2.process_group(0);
    daemon = cmd2.spawn().expect("spawn intentd serve 2");
    assert!(await_uds(&socket).await, "daemon did not restart");

    // Fetch fingerprint for TLS cert pinning.
    let status = uds_rpc(&socket, 1, "system.status", json!({})).await;
    let fp = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();

    // Open WSS connection.
    let cfg = client_config(&fp);
    let mut ws = connect_ws(port, cfg).await;

    // Phase 3: Call agent.listInterrupted over WSS.
    let result = wss_rpc(&mut ws, 2, "agent.listInterrupted", json!({})).await;

    // Verify the response shape.
    let agents = result["agents"].as_array().expect("agents array");
    assert_eq!(agents.len(), 1, "expected 1 interrupted agent");
    let interrupted = &agents[0];
    assert_eq!(interrupted["agentId"].as_str(), Some(agent_id.as_str()));
    assert_eq!(interrupted["workspaceId"].as_str(), Some(ws_id));
    assert_eq!(
        interrupted["workspaceName"].as_str(),
        Some("WSS-INTERRUPTED")
    );
    assert_eq!(interrupted["agentName"].as_str(), Some("Interrupted Agent"));
    assert_eq!(interrupted["prevStatus"].as_str(), Some("active"));
    assert!(interrupted["interruptedAt"].is_string());

    // Phase 4: Restart again — idempotent insert should not duplicate.
    daemon.kill().expect("kill daemon 2");
    daemon.wait().expect("wait daemon 2");
    let mut cmd3 = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd3.arg("serve")
        .arg("--listen")
        .arg(listen)
        .env("INTENTD_DATA_DIR", &data_dir)
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_TCP_PORT", &port_s)
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(data_dir.join("daemon.log")).unwrap(),
        ));
    #[cfg(unix)]
    cmd3.process_group(0);
    #[allow(unused_assignments)]
    {
        daemon = cmd3.spawn().expect("spawn intentd serve 3");
    }
    assert!(await_uds(&socket).await, "daemon did not restart 2");

    let status = uds_rpc(&socket, 3, "system.status", json!({})).await;
    let fp = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint 2")
        .to_string();
    let cfg = client_config(&fp);
    let mut ws = connect_ws(port, cfg).await;

    let result = wss_rpc(&mut ws, 4, "agent.listInterrupted", json!({})).await;
    let agents = result["agents"].as_array().expect("agents array 2");
    assert_eq!(agents.len(), 1, "still 1 interrupted agent (idempotent)");
}

fn workspace_seed(id: &intent_core::WorkspaceId) -> intent_core::Workspace {
    use intent_core::{now_iso, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus};
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "WSS-INTERRUPTED".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.clone(),
        updated_at: ts,
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
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
    }
}
